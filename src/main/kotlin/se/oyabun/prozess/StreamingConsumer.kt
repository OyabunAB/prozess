package se.oyabun.prozess

import se.oyabun.prozess.Prozess.ConsumerFilter
import se.oyabun.prozess.Prozess.ConsumerProcess
import se.oyabun.prozess.StartOffset.AtTimestamp
import se.oyabun.prozess.StartOffset.Earliest
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.defer
import reactor.core.publisher.Mono.delay
import reactor.core.publisher.Mono.empty
import reactor.core.publisher.Mono.error
import reactor.core.publisher.Mono.fromCallable
import reactor.core.publisher.Mono.just
import reactor.core.publisher.Mono.never
import reactor.core.publisher.Mono.`when`
import reactor.core.publisher.Mono.zip
import reactor.core.publisher.Sinks
import reactor.util.concurrent.Queues
import reactor.util.retry.Retry
import se.oyabun.prozess.EndOffset.CatchUp
import se.oyabun.prozess.Prozess
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndUpdate
import kotlin.concurrent.atomics.update
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@OptIn(ExperimentalAtomicApi::class)
class StreamingConsumer<M>(
    val config: ConsumerConfig,
    private val filter: ConsumerFilter = { true },
    private val deserializer: Prozess.Deserializer<M>,
    private val process: ConsumerProcess<M> = { _,_ -> },
    instance: String? = "consumer",
) {
    private val log = Logging.logger { }
    private val instanceId = "[$instance ${config.topics} consumer]"
    private val client = ThreadsafeKafkaClient(config)
    private val assignments = AtomicReference<Partitions>(emptySet())
    private val ends = AtomicReference<Positions>(emptySet())
    private val started = AtomicBoolean(false)
    private val highWaterMark = config.maxPollRecords
    private val lowWaterMark = config.maxPollRecords / 4
    private val pendingSeeks = AtomicReference<Offsets?>(null)
    private val paused = java.util.concurrent.atomic.AtomicBoolean(false)
    private val closeSignal = Sinks.one<Unit>()
    private val disposed = java.util.concurrent.atomic.AtomicBoolean(false)
    private var shutdownDone = Sinks.one<Unit>()
    private var subscription: Disposable? = null
    private var committer: Committer? = null
    private var emitterComponent: Emitter? = null
    private var pipelinesActive = java.util.concurrent.atomic.AtomicBoolean(false)
    private var poller: Poller? = null
    private val rebalanceHandler: RebalanceHandler = CoordinatingRebalanceHandler(
        committerRef = { committer },
        assignments = assignments,
        pendingSeeks = pendingSeeks,
        ends = ends,
        paused = { paused.get() },
        instanceId = instanceId,
        log = log,
    )

    private val shutdownTask: Mono<Void> = defer {
        if (!disposed.get()) empty()
        else fromCallable {
                client.wakeup()
                closeSignal.tryEmitEmpty()
            }
            .then(
                if (pipelinesActive.get())
                    `when`(poller?.stop() ?: empty<Void>(), emitterComponent?.stop() ?: empty<Void>())
                        .then(committer?.stop() ?: empty())
                else empty()
            )
            .then(client.close())
    }.cache()

    fun start(
        from: StartOffset = StartOffset.Latest,
        until: EndOffset = EndOffset.Continuous,
    ) {
        check(started.compareAndSet(false, true)) { "$instanceId start() called more than once" }
        val complete = completionSignal()
        val sync = syncSignal(until, complete)
        shutdownDone = Sinks.one()
        subscription = incomingFeed(sync, closeSignal.asMono(), from, until)
            .groupBy { it.position.partition }
            .flatMap { partition ->
                partition.concatMap { received ->
                    if (filter(received)) processRetrying(received)
                    else just(received.position)
                }
            }
            .flatMap { position -> committer?.markProcessed(position) ?: just(position) }
            .subscribe()
    }

    fun shutdown() {
        if (!disposed.compareAndSet(false, true)) return
        shutdownTask
            .onErrorResume { e -> log.kafka.terminatedUnexpectedly(instanceId, e); empty() }
            .block()
    }

    fun shutdownAsync(): Mono<Void> {
        if (!disposed.compareAndSet(false, true)) return shutdownTask
        return shutdownTask
    }

    val isDisposed: Boolean get() = disposed.get()

    fun pause() {
        paused.set(true)
        poller?.pause()
    }

    fun resume() {
        paused.set(false)
        poller?.resume()
    }

    private fun incomingFeed(
        sync: Mono<Received>,
        closeSignal: Mono<Unit>,
        from: StartOffset,
        until: EndOffset,
    ): Flux<Received> = client.partitionsFor(config.topics)
        .flatMapMany { topicPartitions ->
            val awaitSubscription = client.subscribe(
                config.topics,
                rebalanceHandler,
            )
            val messages = messagesFeed(topicPartitions, sync)
            val synchronizedMessages = Flux.merge(messages, sync)
            initOffsets(topicPartitions, from, until)
                .then(awaitSubscription)
                .thenMany(synchronizedMessages)
        }
        .takeUntilOther(closeSignal)
        .doOnSubscribe { log.kafka.subscribed(instanceId, config.topics) }
        .doOnError { cause -> log.kafka.consumerFailed(instanceId, config.topics, cause) }
        .doOnTerminate { log.kafka.shuttingDown(instanceId, config.topics) }
        .retryWhen(notCancelled)

    private val notCancelled = Retry.backoff(Long.MAX_VALUE, 500.milliseconds.toJavaDuration())
        .maxBackoff(30.seconds.toJavaDuration())
        .doBeforeRetry { log.kafka.listenerRetrying(instanceId, config.topics, it.failure()) }
        .doAfterRetry { log.kafka.listenerRestarted(instanceId, config.topics) }

    private fun initOffsets(topicPartitions: Partitions, from: StartOffset, until: EndOffset): Mono<Positions> {
        val loadEndings = if (until is CatchUp) client.endOffsets(topicPartitions).map { it.also { ends.store(it) } } else just(emptySet())
        val loadBeginnings = when (from) {
            is Earliest -> zip(
                    client.beginningOffsets(topicPartitions).map { it.asOffsets() },
                    client.committed(topicPartitions),
                ) { beginnings, committed ->
                    val targets = beginnings.filterKeys { p ->
                        val known = committed[p]
                        known == null || known <= 0L
                    }
                    if (targets.isNotEmpty()) pendingSeeks.store(targets)
                    emptySet()
                }
            is StartOffset.Latest -> just(emptySet())
            is AtTimestamp -> client.offsetsForTimes(topicPartitions, from.instant)
                .flatMap { positions: Positions ->
                    if (positions.isEmpty()) client.endOffsets(topicPartitions)
                        .map { endOffsets ->
                            val targets = endOffsets.asOffsets()
                            if (targets.isNotEmpty()) pendingSeeks.store(targets)
                            endOffsets
                        }
                    else zip(
                        just(positions.asOffsets()),
                            client.committed(topicPartitions),
                        ) { targets, committed ->
                            val filtered = targets.filterKeys { p ->
                                val known = committed[p]
                                known == null || known <= (targets[p] ?: 0L)
                            }
                            if (filtered.isNotEmpty()) pendingSeeks.store(filtered)
                            positions
                        }
                }
        }
        return when {
            from is Earliest && until is CatchUp -> loadBeginnings.flatMap { loadEndings }
            from is Earliest -> loadBeginnings
            from is AtTimestamp && until is CatchUp -> loadBeginnings.flatMap { loadEndings }
            until is CatchUp -> loadEndings
            from is AtTimestamp -> loadBeginnings
            else -> just(emptySet())
        }
    }

    private fun processRetrying(received: Received, attempt: Long = 0): Mono<Position> =
        fromCallable { deserializer.invoke(received.message) }
            .map { process(received, it); received.position }
            .onErrorResume { e ->
                if (disposed.get()) error(e)
                else {
                    log.kafka.processingRetrying(instanceId, received.position.partition, attempt, e)
                    delay(backoff(attempt))
                        .thenReturn(received)
                        .flatMap { processRetrying(it, attempt + 1) }
                }
            }

    private fun completionSignal(): Mono<Void> = committer?.positions
        ?.map {
            assignments.load().all { partition ->
                val lastPosition = ends.load().find { it.partition == partition }
                lastPosition == null || (committer?.processedOffsets?.get(partition) ?: 0L) >= lastPosition.offset
            }
        }
        ?.filter { it }?.next()?.then()?.cache() ?: never()

    private fun syncSignal(until: EndOffset, complete: Mono<Void>): Mono<Received> = when (until) {
        is CatchUp -> empty<Received>().delaySubscription(complete)
        is EndOffset.Continuous -> never()
    }

    private fun messagesFeed(
        topicPartitions: Partitions,
        sync: Mono<Received>,
    ): Flux<Received> = Flux.create { emitter: FluxSink<Received> ->
        lateinit var buffer: ReceivedBuffer
        val pauseBlock = {
            val current = assignments.load()
            if (current.isNotEmpty()) {
                log.kafka.bufferSaturated(instanceId, current, buffer.size)
                client.pause(current).subscribe()
            }
        }
        val resumeBlock = {
            val current = assignments.load()
            if (current.isNotEmpty()) {
                log.kafka.bufferDrained(instanceId, current, buffer.size)
                client.resume(current).subscribe()
            }
        }
        buffer = InMemoryReceivedBuffer(
            delegate = Queues.unbounded<Received>().get(),
            highWaterMark = highWaterMark,
            lowWaterMark = lowWaterMark,
            onPause = pauseBlock,
            onResume = resumeBlock,
        )
        val pollerShutdownSink = Sinks.one<Unit>()
        val emitterShutdownSink = Sinks.one<Unit>()
        val pollingDone = Sinks.one<Unit>()
        val emittingDone = Sinks.one<Unit>()
        pipelinesActive.set(true)

        val c = BufferedCommitter(
            commit = { offsets -> client.commit(offsets, "$instanceId@${Clock.System.now()}") },
            assignments = { assignments.load() },
            instanceId = instanceId,
            topicPartitions = topicPartitions,
            log = log,
            bufferSize = config.maxPollRecords,
        )
        c.start()
        committer = c

        val e = BufferedEmitter(
            emit = { received -> emitter.next(received); just(received) },
            buffer = buffer,
            assignments = { assignments.load() },
            instanceId = instanceId,
            shutdownSink = emitterShutdownSink,
            done = emittingDone,
            log = log,
        )
        e.start()
        emitterComponent = e

        poller = BufferedPoller(
            poll = { client.poll() },
            pause = { client.pause(it) },
            resume = { client.resume(it) },
            buffer = buffer,
            assignments = { assignments.load() },
            instanceId = instanceId,
            pollInterval = config.pollInterval,
            shutdownSink = pollerShutdownSink,
            done = pollingDone,
            log = log,
        )
        poller?.start()
        emitter.onDispose {
            pollerShutdownSink.tryEmitValue(Unit)
            emitterShutdownSink.tryEmitValue(Unit)
            shutdownDone.tryEmitEmpty()
        }
    }.takeUntilOther(sync)

    fun hasNoAssignments(): Boolean = assignments.load().isEmpty()

    fun position(partition: Partition): Long =
        client.positionOf(partition).block() ?: throw ConsumerNotActive("$instanceId position() failed for $partition")

    fun lag(partition: Partition): Long =
        client.endOffsetOf(partition).zipWith(client.positionOf(partition)).map { it.t1 - it.t2 }.block()
            ?: throw ConsumerNotActive("$instanceId lag() failed for $partition")

    companion object {
        private fun backoff(attempt: Long): java.time.Duration =
            minOf(30.seconds, 1.seconds * (1 shl attempt.toInt().coerceAtMost(30))).toJavaDuration()
    }
}
