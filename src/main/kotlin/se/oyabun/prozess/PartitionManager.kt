/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.oyabun.prozess

import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndUpdate
import kotlin.concurrent.atomics.update

/**
 * Coordinates the safe sequence for partition revocation and assignment during
 * a Kafka consumer rebalance.
 *
 * Both methods are invoked synchronously on the Kafka polling thread as part of
 * [org.apache.kafka.clients.consumer.ConsumerRebalanceListener].  They receive a
 * [RebalanceContext] that provides direct access to [org.apache.kafka.clients.consumer.KafkaConsumer]
 * operations (commitSync, seek, pause, position) -- these **must** be used instead
 * of going through the reactive scheduler to avoid deadlock on the single-threaded
 * Kafka client scheduler.
 *
 * Contract:
 * - [onPartitionsRevoked] reads the latest [Committer.processedOffsets] snapshot,
 *   commits those offsets for the revoked partitions synchronously via
 *   [RebalanceContext.commit] (the last chance before another consumer takes
 *   over), then removes them from the assignment set atomically.
 * - [onPartitionsAssigned] adds the new partitions to the assignment set, applies
 *   any pending seeks, pauses newly assigned partitions if the consumer is paused,
 *   and seeds the committer for partitions whose position is past their end offset
 *   (catch-up completion).
 */
internal interface PartitionManager {

    /**
     * Invoked when partitions are revoked from this consumer.
     *
     * The given [processedOffsets] are filtered for the revoked [partitions] and
     * committed synchronously via [RebalanceContext.commit] -- the last opportunity
     * to commit offsets before another consumer takes over.  The partitions are
     * then removed from the assignment set.
     *
     * @param context          direct-access bridge to the underlying KafkaConsumer.
     * @param partitions       the set of revoked partitions.
     * @param processedOffsets  the latest processed offsets snapshot.
     */
    fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions, processedOffsets: Offsets)

    /**
     * Invoked when partitions are assigned to this consumer.
     *
     * The partitions are added to the assignment set.  Any pending seeks (e.g.
     * EARLIEST or AT_TIMESTAMP start-offsets) are applied for the newly assigned
     * partitions.  If the consumer is in a paused state, the new partitions are
     * paused immediately.  For catch-up mode, partitions whose current position
     * exceeds their end offset are returned so the caller can seed the committer.
     *
     * @param context     direct-access bridge to the underlying KafkaConsumer.
     * @param partitions  the set of newly assigned partitions.
     * @return offsets to seed into the committer (catch-up completion).
     */
    fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions): Offsets
}

/**
 * Standard [PartitionManager] implementation.
 *
 * Owns the coordination logic extracted from [StreamingConsumer].  Owns the
 * [assignments] set; [pendingSeeks] and [ends] are still shared via
 * [AtomicReference] for cross-component visibility.
 *
 * @param pendingSeeks  one-shot seeks (reset to `emptyMap()` on first assign).
 * @param ends          known end-offsets for catch-up detection.
 * @param paused        whether the consumer is externally paused.
 * @param instanceId    label used in log messages.
 * @param log           logger.
 */
@OptIn(ExperimentalAtomicApi::class)
internal class CoordinatingPartitionManager(
    private val pendingSeeks: AtomicReference<Offsets>,
    private val ends: AtomicReference<Positions>,
    private val paused: () -> Boolean,
    private val instanceId: String,
    private val log: Logger,
) : PartitionManager {

    private val assignments = AtomicReference<Partitions>(emptySet())

    fun assignments(): Partitions = assignments.load()

    override fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions, processedOffsets: Offsets) {
        log.kafka.revoked(instanceId, partitions)
        val offsets = processedOffsets.filterKeys { it in partitions }
        if (offsets.isNotEmpty()) {
            context.commit(offsets)
            log.kafka.committed(instanceId, partitions)
        }
        assignments.update { it - partitions }
    }

    override fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions): Offsets {
        log.kafka.assigned(instanceId, partitions)
        assignments.update { it + partitions }
        if (paused() && partitions.isNotEmpty()) context.pause(partitions)
        pendingSeeks.fetchAndUpdate { emptyMap() }
            .filterKeys { it in partitions }
            .takeIf { it.isNotEmpty() }
            ?.let { context.seek(it) }
        val endPositions = ends.load()
        val done = partitions.mapNotNull { p ->
            val endPos = endPositions.find { it.partition == p }
            if (endPos != null && context.position(p) > endPos.offset) p to (endPos.offset + 1)
            else null
        }.toMap()
        return done
    }
}
