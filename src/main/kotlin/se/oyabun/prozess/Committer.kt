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
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndUpdate
import kotlin.concurrent.atomics.update

interface Committer {

    fun markProcessed(position: Position): Mono<Position>

    fun seedOffsets(offsets: Offsets)

    val positions: Flux<Position>

    val processedOffsets: Offsets

    fun start()

    fun stop(): Mono<Void>
}

@OptIn(ExperimentalAtomicApi::class)
internal class BufferedCommitter(
    private val client: KafkaClient,
    private val instanceId: String,
    private val assignments: () -> Partitions,
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

    override fun start() {
        if (!running.compareAndSet(false, true)) throw CommitterAlreadyRunning("$instanceId already running")
        val scheduler = Schedulers.newSingle("$instanceId-committer")
        schedulerRef.store(scheduler)
        disposable = processed.asFlux()
            .bufferTimeout(maxBatchSize, maxBatchTime)
            .publishOn(scheduler)
            .concatMap { batch ->
                val assigned = assignments()
                val current = batch.filter { it.partition in assigned }
                if (current.isEmpty()) Mono.empty()
                else {
                    val offsets = current.groupBy({ it.partition }, { it.offset })
                    val latest = offsets.mapValues { it.value.max() + 1 }
                    client.commit(latest, "$instanceId@${Clock.System.now()}")
                        .doOnError { cause -> log.kafka.commitFailed(instanceId, latest.keys, cause) }
                        .doOnSuccess { log.kafka.committed(instanceId, latest.keys) }
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
