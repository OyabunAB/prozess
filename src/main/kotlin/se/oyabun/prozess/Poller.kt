package se.oyabun.prozess

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
import se.oyabun.prozess.Retrying.anyException
import se.oyabun.prozess.Retrying.infiniteRetries
import se.oyabun.prozess.Retrying.withRetries
import se.oyabun.prozess.ReceivedBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

interface Poller {

    fun start()
    fun stop(): Mono<Void>
    fun pause()
    fun resume()
    val isRunning: Boolean
}

internal class BufferedPoller(
    private val client: KafkaClient,
    private val buffer: ReceivedBuffer,
    private val assignments: () -> Partitions,
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
    private var disposable: Disposable = Disposable { }

    override fun start() {
        if (!running.compareAndSet(false, true)) throw PollerAlreadyRunning("$instanceId already running")
        val scheduler = Schedulers.newSingle("$instanceId-poll")
        timer.set(scheduler)
        disposable = pollingPipeline(scheduler)
    }

    override fun stop(): Mono<Void> {
        if (!running.get()) {
            disposable.dispose()
            timer.getAndSet(Schedulers.immediate()).dispose()
            return Mono.empty()
        }
        shutdownSink.tryEmitValue(Unit)
        return done.asMono()
            .doFinally {
                disposable.dispose()
                timer.getAndSet(Schedulers.immediate()).dispose()
                running.set(false)
            }
            .then()
    }

    override fun pause() {
        if (!running.get()) throw PollerNotRunning("$instanceId is not running")
        val current = assignments()
        if (current.isNotEmpty()) {
            client.pause(current).block()
        }
    }

    override fun resume() {
        if (!running.get()) throw PollerNotRunning("$instanceId is not running")
        val current = assignments()
        if (current.isNotEmpty()) {
            client.resume(current).block()
        }
    }

    override val isRunning: Boolean get() = running.get()

    private fun pollingPipeline(timer: Scheduler): Disposable = Flux.interval(pollInterval.toJavaDuration(), timer)
        .onBackpressureDrop()
        .concatMap {
            client.poll()
                .withRetries(id = instanceId, retryOn = anyException, maxAttempts = infiniteRetries)
        }
        .doOnNext { records -> if (records.isNotEmpty()) log.kafka.polled(instanceId, records.size) }
        .concatMap { records ->
            records.forEach { buffer.offer(it) }
            just(records)
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
