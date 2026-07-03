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
internal interface RebalanceHandler {

    /**
     * Invoked when partitions are revoked from this consumer.
     *
     * If a [Committer] is available, its [Committer.processedOffsets] are snapshot
     * and committed synchronously via [RebalanceContext.commit] -- this is the last
     * opportunity to commit offsets for these partitions before another consumer
     * takes over.  The partitions are then removed from the assignment set.
     *
     * @param context  direct-access bridge to the underlying KafkaConsumer.
     * @param partitions  the set of revoked partitions.
     */
    fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions)

    /**
     * Invoked when partitions are assigned to this consumer.
     *
     * The partitions are added to the assignment set.  Any pending seeks (e.g.
     * EARLIEST or AT_TIMESTAMP start-offsets) are applied for the newly assigned
     * partitions.  If the consumer is in a paused state, the new partitions are
     * paused immediately.  For catch-up mode, partitions whose current position
     * exceeds their end offset are seeded into the committer as "completed".
     *
     * @param context  direct-access bridge to the underlying KafkaConsumer.
     * @param partitions  the set of newly assigned partitions.
     */
    fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions)
}

/**
 * Standard [RebalanceHandler] implementation.
 *
 * Owns the coordination logic extracted from [StreamingConsumer].  All mutable
 * state ([assignments], [pendingSeeks], [ends]) is passed as [AtomicReference]
 * to guarantee visibility across threads.  The [committerRef] is a lambda so
 * that the handler can be created before the committer pipeline is initialised;
 * the lambda returns `null` until [Committer.start] has been called.
 *
 * @param committerRef  returns the current [Committer] or `null`.
 * @param assignments   the current set of assigned partitions.
 * @param pendingSeeks  one-shot seeks (cleared on first assign).
 * @param ends          known end-offsets for catch-up detection.
 * @param paused        whether the consumer is externally paused.
 * @param instanceId    label used in log messages.
 * @param log           logger.
 */
@OptIn(ExperimentalAtomicApi::class)
internal class CoordinatingRebalanceHandler(
    private val committerRef: () -> Committer?,
    private val assignments: AtomicReference<Partitions>,
    private val pendingSeeks: AtomicReference<Offsets?>,
    private val ends: AtomicReference<Positions>,
    private val paused: () -> Boolean,
    private val instanceId: String,
    private val log: Logger,
) : RebalanceHandler {

    override fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions) {
        log.kafka.revoked(instanceId, partitions)
        val c = committerRef()
        if (c != null) {
            val offsets = c.processedOffsets.filterKeys { it in partitions }
            if (offsets.isNotEmpty()) {
                context.commit(offsets)
                log.kafka.committed(instanceId, partitions)
            }
        }
        assignments.update { it - partitions }
    }

    override fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions) {
        log.kafka.assigned(instanceId, partitions)
        assignments.update { it + partitions }
        if (paused() && partitions.isNotEmpty()) context.pause(partitions)
        pendingSeeks.fetchAndUpdate { null }
            ?.filterKeys { it in partitions }
            ?.takeIf { it.isNotEmpty() }
            ?.let { context.seek(it) }
        val endPositions = ends.load()
        val done = partitions.mapNotNull { p ->
            val endPos = endPositions.find { it.partition == p }
            if (endPos != null && context.position(p) > endPos.offset) p to (endPos.offset + 1)
            else null
        }.toMap()
        if (done.isNotEmpty()) committerRef()?.seedOffsets(done)
    }
}
