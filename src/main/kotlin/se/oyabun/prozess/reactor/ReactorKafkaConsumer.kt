package se.oyabun.prozess.reactor

import se.oyabun.prozess.ConsumerConfig
import se.oyabun.prozess.ConsumerNotActive
import se.oyabun.prozess.EndOffset
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Offsets
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Partitions
import se.oyabun.prozess.Position
import se.oyabun.prozess.Positions
import se.oyabun.prozess.Prozess.ConsumerFilter
import se.oyabun.prozess.Prozess.ConsumerProcess
import se.oyabun.prozess.Received
import se.oyabun.prozess.StartOffset
import se.oyabun.prozess.StartOffset.AtTimestamp
import se.oyabun.prozess.StartOffset.Earliest
import se.oyabun.prozess.asOffsets
import se.oyabun.prozess.reactor.Retrying.anyException
import se.oyabun.prozess.reactor.Retrying.infiniteRetries
import se.oyabun.prozess.reactor.Retrying.withRetries
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
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.util.concurrent.Queues
import reactor.util.retry.Retry
import se.oyabun.prozess.EndOffset.CatchUp
import se.oyabun.prozess.Prozess
import se.oyabun.prozess.RebalanceContext
import java.util.Queue
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndUpdate
import kotlin.concurrent.atomics.update
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@OptIn(ExperimentalAtomicApi::class)
class ReactorKafkaConsumer<M>(
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
    private var pipelinesActive = java.util.concurrent.atomic.AtomicBoolean(false)
    private var shutdownSignal = Sinks.one<Unit>()
    private var emittingDone = Sinks.one<Unit>()
    private val emitScheduler = java.util.concurrent.atomic.AtomicReference(Schedulers.immediate())
    private var poller: Poller? = null

    private val shutdownTask: Mono<Void> = Mono.defer {
        if (!disposed.get()) Mono.empty()
        else fromCallable {
                client.wakeup()
                closeSignal.tryEmitEmpty()
                shutdownSignal.tryEmitValue(Unit)
            }
            .then(
                if (pipelinesActive.get())
                    Mono.`when`(poller?.stop() ?: empty<Void>(), emittingDone.asMono())
                        .then(committer?.stop() ?: empty())
                else Mono.empty()
            )
            .then(client.close())
            .doFinally {
                emitScheduler.get().dispose()
            }
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
                onRevoked = { onPartitionsRevoked(this, it) },
                onAssigned = { onPartitionsAssigned(this, it) },
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

    private fun restartIndefinitely(component: String) =
        Retry.backoff(Long.MAX_VALUE, 500.milliseconds.toJavaDuration())
            .maxBackoff(30.seconds.toJavaDuration())
            .doBeforeRetry { log.kafka.componentRestarting(instanceId, component, it.failure()) }

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
                if (disposed.get()) Mono.error(e)
                else {
                    log.kafka.processingRetrying(instanceId, received.position.partition, attempt, e)
                    delay(backoff(attempt))
                        .thenReturn(received)
                        .flatMap { processRetrying(it, attempt + 1) }
                }
            }

    private fun onPartitionsRevoked(context: RebalanceContext, domain: Partitions) {
        log.kafka.revoked(instanceId, domain)
        committer?.flushForPartitions(domain)?.block()
        assignments.update { it - domain }
    }

    private fun onPartitionsAssigned(context: RebalanceContext, domain: Partitions) {
        log.kafka.assigned(instanceId, domain)
        assignments.update { it + domain }
        if (paused.get()) context.pause(domain)
        pendingSeeks.fetchAndUpdate { null }
            ?.filterKeys { it in domain }
            ?.takeIf { it.isNotEmpty() }
            ?.let { context.seek(it) }
        val endPositions = ends.load()
        val done = domain.mapNotNull { p ->
            val endPos = endPositions.find { it.partition == p }
            if (endPos != null && context.position(p) > endPos.offset) p to (endPos.offset + 1)
            else null
        }.toMap()
        if (done.isNotEmpty()) committer?.seedOffsets(done)
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
        val buffer = Queues.unbounded<Received>().get()
        shutdownSignal = Sinks.one()
        val pollingDone = Sinks.one<Unit>()
        emittingDone = Sinks.one()
        emitScheduler.set(Schedulers.newSingle("$instanceId-$EMITTER"))
        pipelinesActive.set(true)

        val c = BufferedCommitter(
            commit = Committer.Commit { offsets -> client.commit(offsets, "$instanceId@${Clock.System.now()}") },
            assignments = { assignments.load() },
            instanceId = instanceId,
            topicPartitions = topicPartitions,
            log = log,
            bufferSize = config.maxPollRecords,
        )
        c.start()
        committer = c

        emittingPipeline(buffer, emitter, shutdownSignal.asMono(), emittingDone, timer = emitScheduler.get())
        poller = BufferedPoller(
            poll = Poller.Poll { client.poll() },
            pause = Poller.Pause { client.pause(it) },
            resume = Poller.Resume { client.resume(it) },
            buffer = buffer,
            assignments = { assignments.load() },
            highWaterMark = highWaterMark,
            lowWaterMark = lowWaterMark,
            instanceId = instanceId,
            pollInterval = config.pollInterval,
            shutdownSink = shutdownSignal,
            done = pollingDone,
            log = log,
        )
        poller?.start()
        emitter.onDispose {
            shutdownSignal.tryEmitValue(Unit)
            shutdownDone.tryEmitEmpty()
        }
    }.takeUntilOther(sync)

    private fun emittingPipeline(
        buffer: Queue<Received>,
        emitter: FluxSink<Received>,
        shutdown: Mono<Unit>,
        done: Sinks.One<Unit>,
        component: String = "$instanceId-$EMITTER",
        timer: Scheduler = Schedulers.newSingle(component),
        period: Duration = 100.milliseconds,
    ): Disposable = Flux.interval(period.toJavaDuration(), timer)
        .filter { emitter.requestedFromDownstream() > 0 && !buffer.isEmpty() }
        .concatMap { fromCallable { buffer.poll() } }
        .concatMap { received ->
            val current = assignments.load()
            if (received.position.partition !in current) just(received)
            else {
                log.kafka.emitted(instanceId, received.position)
                emitter.next(received)
                just(received)
            }
        }
        .retryWhen(restartIndefinitely(component))
        .takeUntilOther(shutdown)
        .subscribe(
            {},
            { signalCompletion(done, component, it) },
            { signalCompletion(done, component) },
        )


    private fun signalCompletion(done: Sinks.One<Unit>, component: String, cause: Throwable? = null) {
        if (cause != null) log.kafka.terminatedUnexpectedly(component, cause)
        else log.kafka.completed(component)
        done.tryEmitEmpty()
    }

    fun hasNoAssignments(): Boolean = assignments.load().isEmpty()

    fun position(partition: Partition): Long =
        client.positionOf(partition).block() ?: throw ConsumerNotActive("$instanceId position() failed for $partition")

    fun lag(partition: Partition): Long =
        client.endOffsetOf(partition).zipWith(client.positionOf(partition)).map { it.t1 - it.t2 }.block()
            ?: throw ConsumerNotActive("$instanceId lag() failed for $partition")

    companion object {
        const val EMITTER = "emitter"

        private fun backoff(attempt: Long): java.time.Duration =
            minOf(30.seconds, 1.seconds * (1 shl attempt.toInt().coerceAtMost(30))).toJavaDuration()
    }
}
