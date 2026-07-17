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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.Policy
import se.oyabun.aelv.Sinks
import se.oyabun.aelv.asMany
import se.oyabun.aelv.bufferTimeout
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.doOnError
import se.oyabun.aelv.doOnNext
import se.oyabun.aelv.drain
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.publishOn
import se.oyabun.aelv.retry
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.atomics.AtomicReference as KAtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.update
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Asynchronously commits processed Kafka offsets in batches.
 *
 * Callers report processed records via [markProcessed]. The committer
 * accumulates positions and flushes them to Kafka in configurable batches
 * (by count or time). Offsets for unassigned partitions are filtered before
 * committing to avoid illegal-state errors during rebalances.
 *
 * [start] must be called before [markProcessed]. [stop] drains and commits
 * all pending positions before returning.
 */
interface Committer {
    /**
     * Records that [position] has been fully processed.
     *
     * Updates the internal offset map (taking the highest offset seen per
     * partition) and feeds [position] into the commit pipeline.
     */
    suspend fun markProcessed(position: Position)

    /**
     * Pre-populates the internal offset map with [offsets] without emitting
     * them into the commit pipeline.
     *
     * Used for catch-up completion detection: the consumer seeds end-offsets
     * so the completion check has a reference point even before the commit
     * pipeline fires.
     */
    fun seedOffsets(offsets: Offsets)

    /**
     * Hot stream of every [Position] passed to [markProcessed].
     *
     * Subscribers receive all positions emitted since they subscribed. Used
     * internally for catch-up completion detection.
     */
    val positions: Many<Position>

    /**
     * Snapshot of the highest processed offset per partition seen so far.
     *
     * The value is `offset + 1` (the next expected offset), matching Kafka's
     * commit convention.
     */
    val processedOffsets: Offsets

    /**
     * Hot stream of committed offset maps.
     *
     * Emits once after each successful Kafka commit. Useful for observing
     * commit progress or writing integration tests.
     */
    val committedOffsets: Many<Offsets>

    /**
     * Starts the internal commit pipeline on a dedicated thread.
     *
     * @throws CommitterAlreadyRunning if called on an already-running committer.
     */
    fun start()

    /**
     * Drains remaining positions and stops the commit pipeline.
     *
     * Returns a [None] that completes once the pipeline has fully drained and
     * the background thread has exited.
     */
    fun stop(): None<Unit>
}

@OptIn(ExperimentalAtomicApi::class)
@Suppress("OPT_IN_USAGE")
internal class BufferedCommitter(
    private val client: KafkaClient,
    private val instanceId: String,
    private val assignments: () -> Partitions,
    private val log: Logger,
    private val bufferSize: Int = 500,
    private val maxBatchSize: Int = 25,
    private val maxBatchTime: kotlin.time.Duration = 1.seconds,
) : Committer {

    private val processedOffsetsRef  = KAtomicReference<Offsets>(emptyMap())
    private val positionSink         = Sinks.replay<Position>()
    private val committedOffsetsSink = Sinks.replayLast<Offsets>(1)
    private val doneSink             = Sinks.broadcast<Unit>()
    private val running              = AtomicBoolean(false)
    private val dispatcher           = newSingleThreadContext("$instanceId-committer")
    private val scope                = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val noopJob              = Job().also { it.cancel() }
    private val job                  = AtomicReference<Job>(noopJob)

    override val processedOffsets: Offsets get() = processedOffsetsRef.load()
    override val positions: Many<Position>  get() = positionSink.asMany()
    override val committedOffsets: Many<Offsets> get() = committedOffsetsSink.asMany()

    override fun seedOffsets(offsets: Offsets) {
        processedOffsetsRef.update { current ->
            current + offsets.mapValues { (partition, seed) -> maxOf(current[partition] ?: 0L, seed) }
        }
    }

    override suspend fun markProcessed(position: Position) {
        processedOffsetsRef.update { current ->
            val offset = maxOf(current[position.partition] ?: 0L, position.offset + 1)
            current + (position.partition to offset)
        }
        positionSink.tryEmit(position)
    }

    override fun start() {
        if (!running.compareAndSet(false, true)) throw CommitterAlreadyRunning("$instanceId already running")
        job.set(scope.launch {
            positionSink.asMany()
                .bufferTimeout(maxBatchSize, maxBatchTime)
                .publishOn(dispatcher)
                .concatMap { batch: List<Position> ->
                    val assigned = assignments()
                    val current  = batch.filter { it.partition in assigned }
                    if (current.isEmpty()) return@concatMap Many.empty<Unit>()
                    val offsets = current.groupBy({ it.partition }, { it.offset })
                    val latest  = offsets.mapValues { (_, v) -> v.max() + 1 }
                    client.commit(latest, "$instanceId@${Clock.System.now()}")
                        .doOnError { cause -> log.kafka.commitFailed(instanceId, latest.keys, cause.cause ?: cause) }
                        .doOnNext { committed ->
                            log.kafka.committed(instanceId, latest.keys)
                            committedOffsetsSink.tryEmit(committed)
                        }
                        .withRetries()
                        .flatMapMany { Many.empty<Unit>() }
                }
                .retry(Policy.retry().withBackoff(500.milliseconds, 30.seconds))
                .drain({}, { signalCompletion(it) }, { signalCompletion() })
        })
    }

    override fun stop(): None<Unit> = None.defer {
        if (!running.get()) return@defer
        positionSink.complete()
        None.from(doneSink.asMany()).await()
        job.get().cancelAndJoin()
        running.set(false)
    }

    private fun signalCompletion(cause: Throwable? = null) {
        running.set(false)
        dispatcher.close()
        if (cause != null) log.kafka.terminatedUnexpectedly("$instanceId-committer", cause)
        else log.kafka.completed("$instanceId-committer")
        doneSink.complete()
    }
}
