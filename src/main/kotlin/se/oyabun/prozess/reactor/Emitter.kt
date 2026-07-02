package se.oyabun.prozess.reactor

import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.fromCallable
import reactor.core.publisher.Mono.just
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import se.oyabun.prozess.EmitterAlreadyRunning
import se.oyabun.prozess.Logger
import se.oyabun.prozess.Partitions
import se.oyabun.prozess.Received
import java.util.Queue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Drains the shared buffer and pushes [Received] records downstream via [Emit].
 *
 * Runs on its own scheduler, respects downstream demand via [requestedFromDownstream],
 * and resumes Kafka partitions when the buffer falls below [lowWaterMark],
 * providing the resume side of the back-pressure contract with [BufferedPoller].
 *
 * Lifecycle: [start] → running → [stop] (or pipeline completes via [shutdown] signal).
 */
interface Emitter {

    /** Start the emit loop. Throws [EmitterAlreadyRunning] if already started. */
    fun start(): Disposable

    /**
     * Stop the emit loop and release the scheduler.
     * Awaits graceful pipeline completion, then disposes.
     * Idempotent and safe to call after pipeline completion.
     */
    fun stop(): Mono<Void>

    /** SAM interface for emitting a single record downstream. */
    fun interface Emit {
        /** Emit the [Received] record. Returns the same record on success. */
        fun emit(received: Received): Mono<Received>
    }

    /** SAM interface for the partition resume operation. */
    fun interface Resume {
        /** Resume consumption from the given partitions. */
        fun resume(partitions: Partitions): Mono<Partitions>
    }
}

/**
 * Standard [Emitter] implementation that drains a shared buffer on a timer.
 *
 * Each tick checks downstream demand and buffer contents. One record is polled
 * from the buffer, checked against current assignments, and emitted. When the
 * buffer drains below [lowWaterMark] the assigned partitions are resumed,
 * allowing the poller to fill the buffer again.
 *
 * @param emit       Emits a received record downstream.
 * @param resume     Resumes assigned partitions (un-pauses Kafka consumer).
 * @param buffer     Shared buffer that the poller fills and this emitter drains.
 * @param assignments  Returns the current set of assigned partitions.
 * @param requestedFromDownstream  Returns the number of requested elements downstream.
 * @param lowWaterMark   Buffer size threshold that triggers partition resume.
 * @param instanceId     Label used in logging and scheduler thread names.
 * @param shutdownSink   Signal sink to trigger clean pipeline termination.
 * @param done           Emitted when the pipeline completes (normal or error).
 * @param log            Logger.
 * @param period         Interval between emit ticks.
 * @param retryStrategy  Retry spec for pipeline-level errors.
 */
internal class BufferedEmitter(
    private val emit: Emitter.Emit,
    private val resume: Emitter.Resume,
    private val buffer: Queue<Received>,
    private val assignments: () -> Partitions,
    private val requestedFromDownstream: () -> Long,
    private val lowWaterMark: Int,
    private val instanceId: String,
    private val shutdownSink: Sinks.One<Unit>,
    private val done: Sinks.One<Unit>,
    private val log: Logger,
    private val period: Duration = 100.milliseconds,
    private val retryStrategy: Retry = Retry.backoff(Long.MAX_VALUE, 500.milliseconds.toJavaDuration())
        .maxBackoff(30.seconds.toJavaDuration())
        .doBeforeRetry { log.kafka.componentRestarting(instanceId, "emitter", it.failure()) },
) : Emitter {

    private val timer = AtomicReference<Scheduler>(Schedulers.immediate())
    private val running = AtomicBoolean(false)
    private var disposable: Disposable? = null

    override fun start(): Disposable {
        if (!running.compareAndSet(false, true)) throw EmitterAlreadyRunning("$instanceId already running")
        val scheduler = Schedulers.newSingle("$instanceId-emitter")
        timer.set(scheduler)
        val d = emittingPipeline(scheduler)
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

    private fun emittingPipeline(timer: Scheduler): Disposable = Flux.interval(period.toJavaDuration(), timer)
        .filter { requestedFromDownstream() > 0L && !buffer.isEmpty() }
        .concatMap { fromCallable { buffer.poll() } }
        .concatMap { received ->
            val current = assignments()
            if (received.position.partition !in current) just(received)
            else {
                log.kafka.emitted(instanceId, received.position)
                emit.emit(received)
            }
        }
        .concatMap { received ->
            val current = assignments()
            if (buffer.size < lowWaterMark && current.isNotEmpty()) {
                log.kafka.bufferDrained(instanceId, current, buffer.size)
                resume.resume(current).thenReturn(received)
            } else {
                just(received)
            }
        }
        .retryWhen(retryStrategy)
        .takeUntilOther(shutdownSink.asMono())
        .subscribe(
            {},
            { signalCompletion(it) },
            { signalCompletion() },
        )

    private fun signalCompletion(cause: Throwable? = null) {
        running.set(false)
        if (cause != null) log.kafka.terminatedUnexpectedly("$instanceId-emitter", cause)
        else log.kafka.completed("$instanceId-emitter")
        done.tryEmitValue(Unit)
    }
}
