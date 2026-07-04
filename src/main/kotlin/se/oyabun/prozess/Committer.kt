package se.oyabun.prozess

import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import se.oyabun.prozess.CommitFailure
import se.oyabun.prozess.CommitterAlreadyRunning
import se.oyabun.prozess.Logger
import se.oyabun.prozess.Offsets
import se.oyabun.prozess.Partitions
import se.oyabun.prozess.Position
import se.oyabun.prozess.Retrying.anyException
import se.oyabun.prozess.Retrying.infiniteRetries
import se.oyabun.prozess.Retrying.withRetries
import java.time.Clock
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndUpdate
import kotlin.concurrent.atomics.update
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Commits processed positions to Kafka in batches.
 *
 * Owns [processedOffsets] state, feeding positions into an internal sink that is
 * drained by a buffered commit pipeline.
 *
 * Lifecycle: [start] → running → [stop].
 * [markProcessed] can be called at any time; positions are batched and committed
 * asynchronously.
 *
 * Positions are exposed via [positions] for external consumers such as
 * catch-up completion detection.
 */
interface Committer {

    /** Record a processed position.  Updates [processedOffsets] and enqueues for batch commit. */
    fun markProcessed(position: Position): Mono<Position>

    /**
     * Pre-populate offsets without going through the position pipeline.
     * Used to mark partitions as "done" during catch-up completion.
     */
    fun seedOffsets(offsets: Offsets)

    /** Emits each position passed to [markProcessed].  External subscribers see positions in order. */
    val positions: Flux<Position>

    /** Snapshot of the latest tracked offset for each partition (offset = lastProcessed + 1). */
    val processedOffsets: Offsets

    /** Start the buffered commit pipeline on its own scheduler. Throws [CommitterAlreadyRunning] if already started. */
    fun start(): Disposable

    /**
     * Gracefully stop: complete the internal sink, await pipeline drain,
     * then dispose the scheduler.  Idempotent.
     */
    fun stop(): Mono<Void>

    /** SAM interface for the commit operation. */
    fun interface Commit {
        /** Commit the given offsets. Returns the committed offsets on success. */
        fun commit(offsets: Offsets): Mono<Offsets>
    }
}

/**
 * Standard [Committer] implementation that batches processed positions and commits
 * them periodically or when a partition is revoked.
 *
 * Positions arriving via [markProcessed] are added to an in-memory buffer and flushed
 * when either [maxBatchSize] records accumulate or [maxBatchTime] elapses
 * (via [reactor.core.publisher.Flux.bufferTimeout]).
 *
 * Each batch is filtered against the current set of assigned partitions to avoid
 * committing offsets for partitions this consumer no longer owns.
 *
 * @param commit           Commits offsets to Kafka.
 * @param assignments      Returns the current set of assigned partitions.
 * @param instanceId       Label used in logging and scheduler thread names.
 * @param topicPartitions  The full set of subscribed topic partitions (for logging).
 * @param log              Logger.
 * @param bufferSize       Capacity of the internal position sink buffer.
 * @param maxBatchSize     Max number of positions per commit batch.
 * @param maxBatchTime     Max time to wait before flushing a partial batch.
 */
@OptIn(ExperimentalAtomicApi::class)
internal class BufferedCommitter(
    private val commit: Committer.Commit,
    private val assignments: () -> Partitions,
    private val instanceId: String,
    private val topicPartitions: Partitions,
    private val log: Logger,
    private val bufferSize: Int = 500,
    private val maxBatchSize: Int = 25,
    private val maxBatchTime: java.time.Duration = 1.seconds.toJavaDuration(),
) : Committer {

    private val processedOffsetsRef = AtomicReference<Offsets>(emptyMap())
    private val processed = Sinks.many().multicast().onBackpressureBuffer<Position>(bufferSize)
    private val done = Sinks.one<Unit>()
    private val schedulerRef = AtomicReference(Schedulers.immediate())
    private val running = AtomicBoolean(false)
    private var disposable: Disposable = Disposable { }

    override val processedOffsets: Offsets get() = processedOffsetsRef.load()

    override val positions: Flux<Position> get() = processed.asFlux()

    override fun seedOffsets(offsets: Offsets) {
        processedOffsetsRef.update { it + offsets }
    }

    override fun markProcessed(position: Position): Mono<Position> {
        processedOffsetsRef.update { current ->
            val offset = maxOf(current[position.partition] ?: 0L, position.offset + 1)
            current + (position.partition to offset)
        }
        val result = processed.tryEmitNext(position)
        return when {
            result === Sinks.EmitResult.OK -> Mono.just(position)
            result === Sinks.EmitResult.FAIL_TERMINATED || result === Sinks.EmitResult.FAIL_CANCELLED -> Mono.just(position)
            result === Sinks.EmitResult.FAIL_OVERFLOW -> {
                log.kafka.commitBackpressure(instanceId, position.partition)
                Mono.delay(100.milliseconds.toJavaDuration()).then(Mono.defer { markProcessed(position) })
            }
            else -> Mono.error(CommitFailure("$instanceId failed to emit position $position to processed sink: $result"))
        }
    }

    override fun start(): Disposable {
        if (!running.compareAndSet(false, true)) throw CommitterAlreadyRunning("$instanceId already running")
        val scheduler = Schedulers.newSingle("$instanceId-committer")
        schedulerRef.store(scheduler)
        val d = processed.asFlux()
            .bufferTimeout(maxBatchSize, maxBatchTime)
            .publishOn(scheduler)
            .concatMap { batch ->
                val assigned = assignments()
                val current = batch.filter { it.partition in assigned }
                if (current.isEmpty()) Mono.empty()
                else {
                    val offsets = current.groupBy({ it.partition }, { it.offset })
                    val latest = offsets.mapValues { it.value.max() + 1 }
                    commit.commit(latest)
                        .doOnError { cause -> log.kafka.commitFailed(instanceId, topicPartitions, cause) }
                        .doOnSuccess { log.kafka.committed(instanceId, topicPartitions) }
                        .withRetries(id = instanceId, retryOn = anyException, maxAttempts = infiniteRetries)
                }
            }
            .retryWhen(restartIndefinitely())
            .ignoreElements()
            .subscribe(
                {},
                { signalCompletion(it) },
                { signalCompletion() },
            )
        disposable = d
        return d
    }

    override fun stop(): Mono<Void> {
        if (!running.get()) {
            disposable.dispose()
            schedulerRef.fetchAndUpdate { Schedulers.immediate() }.dispose()
            return Mono.empty()
        }
        processed.tryEmitComplete()
        return done.asMono()
            .doFinally {
                disposable.dispose()
                schedulerRef.fetchAndUpdate { Schedulers.immediate() }.dispose()
                running.set(false)
            }
            .then()
    }

    private fun restartIndefinitely() =
        Retry.backoff(Long.MAX_VALUE, 500.milliseconds.toJavaDuration())
            .maxBackoff(30.seconds.toJavaDuration())
            .doBeforeRetry { log.kafka.componentRestarting(instanceId, "$instanceId-committer", it.failure()) }

    private fun signalCompletion(cause: Throwable? = null) {
        running.set(false)
        if (cause != null) log.kafka.terminatedUnexpectedly("$instanceId-committer", cause)
        else log.kafka.completed("$instanceId-committer")
        done.tryEmitEmpty()
    }
}
