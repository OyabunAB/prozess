package se.oyabun.prozess

import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.just
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import se.oyabun.prozess.EmitterAlreadyRunning
import se.oyabun.prozess.Logger
import se.oyabun.prozess.Partitions
import se.oyabun.prozess.Received
import se.oyabun.prozess.ReceivedBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Drains the shared buffer via [ReceivedBuffer.asFlux] and pushes [Received]
 * records downstream via [Emit].
 *
 * Runs on its own scheduler for downstream processing. Back-pressure and
 * partition pause/resume are owned by [InMemoryReceivedBuffer].
 *
 * Lifecycle: [start] → running → [stop] (or pipeline completes via [shutdown] signal).
 */
interface Emitter {

    /** Start the emit loop. Throws [EmitterAlreadyRunning] if already started. */
    fun start(): Disposable

    fun stop(): Mono<Void>

    /** SAM interface for emitting a single record downstream. */
    fun interface Emit {
        fun emit(received: Received): Mono<Received>
    }

    /** SAM interface for the partition resume operation. */
    fun interface Resume {
        fun resume(partitions: Partitions): Mono<Partitions>
    }
}

internal class BufferedEmitter(
    private val emit: Emitter.Emit,
    private val buffer: ReceivedBuffer,
    private val assignments: () -> Partitions,
    private val instanceId: String,
    private val shutdownSink: Sinks.One<Unit>,
    private val done: Sinks.One<Unit>,
    private val log: Logger,
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

    private fun emittingPipeline(timer: Scheduler): Disposable = buffer.asFlux()
        .publishOn(timer)
        .concatMap { received ->
            val current = assignments()
            if (received.position.partition !in current) just(received)
            else {
                log.kafka.emitted(instanceId, received.position)
                emit.emit(received)
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
