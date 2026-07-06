package se.oyabun.prozess

import se.oyabun.prozess.Prozess.ConsumerFilter
import se.oyabun.prozess.StartOffset.AtTimestamp
import se.oyabun.prozess.StartOffset.Earliest
import se.oyabun.prozess.StartOffset.Latest
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.empty
import reactor.core.publisher.Mono.just
import reactor.core.publisher.Mono.never
import reactor.core.publisher.Mono.zip
import reactor.core.publisher.Sinks
import reactor.util.concurrent.Queues
import reactor.util.retry.Retry
import se.oyabun.prozess.EndOffset.CatchUp
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@OptIn(ExperimentalAtomicApi::class)
class StreamingConsumer<M : Any>(
    val config: ConsumerConfig,
    private val processor: Processor<M>,
    private val filter: ConsumerFilter = { true },
    instance: String? = "consumer",
    private val client: KafkaClient = ThreadsafeKafkaClient(config),
) {
    private val log = Logging.logger { }
    private val instanceId = "[$instance ${config.topics} consumer]"

    private val ends = AtomicReference<Positions>(emptySet())
    private val started = AtomicBoolean(false)
    private val highWaterMark = config.maxPollRecords
    private val lowWaterMark = config.maxPollRecords / 4
    private val pendingSeeks = AtomicReference<Offsets>(emptyMap())
    private val paused = java.util.concurrent.atomic.AtomicBoolean(false)
    private val closeSignal = Sinks.one<Unit>()
    private val disposed = java.util.concurrent.atomic.AtomicBoolean(false)
    private val pollerShutdown = Sinks.one<Unit>()
    private val pollingDone = Sinks.one<Unit>()
    private val buffer = InMemoryReceivedBuffer(
        delegate = Queues.unbounded<Received>().get(),
        highWaterMark = highWaterMark,
        lowWaterMark = lowWaterMark,
        onPause = {
            val current = partitionManager.assignments()
            if (current.isNotEmpty()) client.pause(current).subscribe()
        },
        onResume = {
            val current = partitionManager.assignments()
            if (current.isNotEmpty()) client.resume(current).subscribe()
        },
    )
    private val partitionManager = CoordinatingPartitionManager(
        pendingSeeks = pendingSeeks,
        ends = ends,
        paused = paused::get,
        instanceId = instanceId,
        log = log,
    )
    private val committer: Committer = BufferedCommitter(
        client = client,
        instanceId = instanceId,
        assignments = partitionManager::assignments,
        log = log,
        bufferSize = config.maxPollRecords,
    )
    private val poller: Poller = BufferedPoller(
        client = client,
        buffer = buffer,
        assignments = partitionManager::assignments,
        instanceId = instanceId,
        pollInterval = config.pollInterval,
        shutdownSink = pollerShutdown,
        done = pollingDone,
        log = log,
    )

    fun start(
        from: StartOffset = config.startOffset,
        until: EndOffset = EndOffset.Continuous,
    ) {
        check(started.compareAndSet(false, true)) { "$instanceId start() called more than once" }
        poller.start()
        committer.start()
        val complete = completionSignal()
        val sync = syncSignal(until, complete)
        incomingFeed(sync, closeSignal.asMono(), from, until)
            .groupBy { it.position.partition }
            .flatMap { partition ->
                partition.groupBy { filter(it) }
                    .flatMap { grouped ->
                        if (grouped.key()) processor.process(grouped)
                        else grouped.map { it.position }
                    }
            }
            .flatMap { position -> committer.markProcessed(position) }
            .subscribe()
    }

    fun shutdown(duration: Duration? = null) {
        if (!disposed.compareAndSet(false, true)) return
        ShutdownCoordinator(
            client = client,
            closeSignal = closeSignal,
            poller = poller,
            committer = committer,
            instanceId = instanceId,
            log = log,
        ).shutdown(duration)
    }

    val isDisposed: Boolean get() = disposed.get()

    fun pause() {
        paused.set(true)
        poller.pause()
    }

    fun resume() {
        paused.set(false)
        poller.resume()
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
                object : RebalanceListener {
                    override fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions) {
                        partitionManager.onPartitionsRevoked(context, partitions, committer.processedOffsets)
                    }
                    override fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions) {
                        val seeds = partitionManager.onPartitionsAssigned(context, partitions)
                        if (seeds.isNotEmpty()) committer.seedOffsets(seeds)
                    }
                },
            )
            val messages = buffer.asFlux()
                .filter { it.position.partition in partitionManager.assignments() }
                .takeUntilOther(sync)
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
            is Earliest -> client.committed(topicPartitions)
                .flatMap { committed ->
                    val unseeded = topicPartitions.filter { it !in committed }.toSet()
                    if (unseeded.isEmpty()) just(emptySet())
                    else client.beginningOffsets(unseeded)
                        .map { beginnings ->
                            val targets = beginnings.asOffsets()
                            if (targets.isNotEmpty()) pendingSeeks.store(targets)
                            beginnings
                        }
                }
            is Latest -> just(emptySet())
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

    private fun completionSignal(): Mono<Void> = committer.positions
        .map {
            partitionManager.assignments().all { partition ->
                val lastPosition = ends.load().find { it.partition == partition }
                lastPosition == null || (committer.processedOffsets[partition] ?: 0L) >= lastPosition.offset
            }
        }
        .filter { it }.next().then().cache()

    private fun syncSignal(until: EndOffset, complete: Mono<Void>): Mono<Received> = when (until) {
        is CatchUp -> empty<Received>().delaySubscription(complete)
        is EndOffset.Continuous -> never()
    }

    fun hasNoAssignments(): Boolean = partitionManager.assignments().isEmpty()

    fun position(partition: Partition): Long =
        client.positionOf(partition).block() ?: throw ConsumerNotActive("$instanceId position() failed for $partition")

    fun lag(partition: Partition): Long =
        client.endOffsetOf(partition).zipWith(client.positionOf(partition)).map { it.t1 - it.t2 }.block()
            ?: throw ConsumerNotActive("$instanceId lag() failed for $partition")
}
