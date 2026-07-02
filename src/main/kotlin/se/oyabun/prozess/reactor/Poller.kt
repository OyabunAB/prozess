package se.oyabun.prozess.reactor

import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.just
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import se.oyabun.prozess.Logger
import se.oyabun.prozess.Partitions
import se.oyabun.prozess.PollerAlreadyRunning
import se.oyabun.prozess.PollerNotRunning
import se.oyabun.prozess.Received
import se.oyabun.prozess.reactor.Retrying.anyException
import se.oyabun.prozess.reactor.Retrying.infiniteRetries
import se.oyabun.prozess.reactor.Retrying.withRetries
import java.util.Queue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Polls a Kafka consumer on a single thread and pushes [Received] records into a shared buffer.
 *
 * Owns pause/resume of assigned partitions when the buffer is saturated or drained,
 * providing back-pressure between poll and process stages.
 *
 * Lifecycle: [start] → running → [stop] (or pipeline completes via [shutdown] signal).
 * [pause]/[resume] control the underlying Kafka consumer partition assignment state.
 */
interface Poller {

    /** Start the polling loop. Throws [PollerAlreadyRunning] if already started. */
    fun start(): Disposable

    /**
     * Stop the polling loop and release the scheduler.
     * Awaits graceful pipeline completion, then disposes.
     * Returns a [Mono] that completes when cleanup is done.
     * Idempotent and safe to call after pipeline completion.
     */
    fun stop(): Mono<Void>

    /** Pause all assigned partitions. Throws [PollerNotRunning] if not started. */
    fun pause()

    /** Resume all assigned partitions. Throws [PollerNotRunning] if not started. */
    fun resume()

    /** True while the polling loop is active. Becomes false after [stop] or pipeline completion. */
    val isRunning: Boolean

    /** SAM interface for the poll operation. */
    fun interface Poll {
        /** Returns a batch of polled [Received] records. */
        fun poll(): Mono<List<Received>>
    }

    /** SAM interface for the partition pause operation. */
    fun interface Pause {
        /** Pause consumption from the given partitions. */
        fun pause(partitions: Partitions): Mono<Partitions>
    }

    /** SAM interface for the partition resume operation. */
    fun interface Resume {
        /** Resume consumption from the given partitions. */
        fun resume(partitions: Partitions): Mono<Partitions>
    }
}

/**
 * Standard [Poller] implementation that runs a polling loop on a single scheduler thread.
 *
 * Each tick calls [Poller.Poll.poll] and adds received records to an in-memory buffer.
 * When the buffer reaches [highWaterMark] the assigned partitions are paused, preventing
 * the buffer from growing without bound while downstream processing catches up.
 * Partition resume is handled by [BufferedEmitter] when the buffer drains.
 *
 * The poll call is wrapped with [Retrying.withRetries] (infinite). Downstream failures
 * in the pipeline are handled by [retryStrategy] (default: exponential backoff,
 * Long.MAX_VALUE attempts). The pipeline terminates cleanly when [shutdown] emits.
 *
 * @param poll        Polls the Kafka consumer.
 * @param pause       Pauses assigned partitions.
 * @param resume      Resumes assigned partitions.
 * @param buffer      Shared buffer polled records are pushed into.
 * @param assignments Returns the current set of assigned partitions.
 * @param highWaterMark  Buffer size threshold that triggers partition pause.
 * @param instanceId     Label used in logging and scheduler thread names.
 * @param pollInterval   Interval between poll ticks.
 * @param shutdownSink   Signal sink to trigger clean pipeline termination.
 * @param done           Emitted when the pipeline completes (normal or error).
 * @param retryStrategy  Retry spec for pipeline-level errors (default: infinite backoff).
 */
internal class BufferedPoller(
    private val poll: Poller.Poll,
    private val pause: Poller.Pause,
    private val resume: Poller.Resume,
    private val buffer: Queue<Received>,
    private val assignments: () -> Partitions,
    private val highWaterMark: Int,
    private val instanceId: String,
    private val pollInterval: Duration,
    private val shutdownSink: Sinks.One<Unit>,
    private val done: Sinks.One<Unit>,
    private val log: Logger,
    private val retryStrategy: Retry = Retry.backoff(Long.MAX_VALUE, 500.milliseconds.toJavaDuration())
        .maxBackoff(30.seconds.toJavaDuration())
        .doBeforeRetry { log.kafka.componentRestarting(instanceId, "poll", it.failure()) },
) : Poller {

    private val timer = AtomicReference<Scheduler>(Schedulers.immediate())
    private val running = AtomicBoolean(false)
    private var disposable: Disposable? = null

    override fun start(): Disposable {
        if (!running.compareAndSet(false, true)) throw PollerAlreadyRunning("$instanceId already running")
        val scheduler = Schedulers.newSingle("$instanceId-poll")
        timer.set(scheduler)
        val d = pollingPipeline(scheduler)
        disposable = d
        return d
    }

    override fun stop(): Mono<Void> {
        if (!running.get()) {
            disposable?.dispose()
            timer.getAndSet(Schedulers.immediate()).dispose()
            return Mono.empty()
        }
        shutdownSink.tryEmitValue(Unit)
        return done.asMono()
            .doFinally {
                disposable?.dispose()
                timer.getAndSet(Schedulers.immediate()).dispose()
                running.set(false)
            }
            .then()
    }

    override fun pause() {
        if (!running.get()) throw PollerNotRunning("$instanceId is not running")
        val current = assignments()
        if (current.isNotEmpty()) {
            pause.pause(current).block()
        }
    }

    override fun resume() {
        if (!running.get()) throw PollerNotRunning("$instanceId is not running")
        val current = assignments()
        if (current.isNotEmpty()) {
            resume.resume(current).block()
        }
    }

    override val isRunning: Boolean get() = running.get()

    private fun pollingPipeline(timer: Scheduler): Disposable = Flux.interval(pollInterval.toJavaDuration(), timer)
        .onBackpressureDrop()
        .concatMap {
            poll.poll()
                .withRetries(id = instanceId, retryOn = anyException, maxAttempts = infiniteRetries)
        }
        .doOnNext { records -> if (records.isNotEmpty()) log.kafka.polled(instanceId, records.size) }
        .concatMap { records ->
            records.forEach { buffer.add(it) }
            val current = assignments()
            if (buffer.size >= highWaterMark && current.isNotEmpty()) {
                log.kafka.bufferSaturated(instanceId, current, buffer.size)
                pause.pause(current).thenReturn(records)
            } else {
                just(records)
            }
        }
        .flatMapIterable { it }
        .retryWhen(retryStrategy)
        .takeUntilOther(shutdownSink.asMono())
        .subscribe(
            {},
            { signalCompletion(it) },
            { signalCompletion() },
        )

    private fun signalCompletion(cause: Throwable? = null) {
        running.set(false)
        if (cause != null) log.kafka.terminatedUnexpectedly("$instanceId-poll", cause)
        else log.kafka.completed("$instanceId-poll")
        done.tryEmitValue(Unit)
    }
}
