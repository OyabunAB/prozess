package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.Policy
import se.oyabun.aelv.Sinks
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.delaySubscription
import se.oyabun.aelv.doOnError
import se.oyabun.aelv.doOnSubscribe
import se.oyabun.aelv.drain
import se.oyabun.aelv.filter
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.map
import se.oyabun.aelv.merge
import se.oyabun.aelv.retry
import se.oyabun.aelv.take
import se.oyabun.aelv.takeUntilOther
import se.oyabun.aelv.await
import se.oyabun.aelv.Failure
import se.oyabun.aelv.Success
import se.oyabun.aelv.groupBy
import se.oyabun.aelv.zip
import se.oyabun.prozess.EndOffset.CatchUp
import se.oyabun.prozess.StartOffset.AtTimestamp
import se.oyabun.prozess.StartOffset.Earliest
import se.oyabun.prozess.StartOffset.Latest
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndUpdate
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalAtomicApi::class)
class StreamingConsumer<M : Any>(
    val config: ConsumerConfig,
    private val processor: Processor<M>,
    instance: String = shortId(),
    private val client: KafkaClient = ThreadsafeKafkaClient(config),
) {
    private val log        = Logging.logger { }
    private val instanceId = "[$instance ${config.topics} consumer]"

    private val eventSink   = Sinks.replay<ConsumerEvent>()
    private val closeSignal = Sinks.broadcast<Unit>()
    private val disposed    = java.util.concurrent.atomic.AtomicBoolean(false)

    private val ends         = AtomicReference<Positions>(emptySet())
    private val started      = AtomicBoolean(false)
    private val pendingSeeks = AtomicReference<Offsets>(emptyMap())
    private val paused       = java.util.concurrent.atomic.AtomicBoolean(false)

    val events: Many<ConsumerEvent> = eventSink.asMany()

    fun onEvent(callback: (ConsumerEvent) -> Unit) {
        events.drain(onNext = { callback(it) }, onError = {})
    }

    private val buffer = InMemoryReceivedBuffer(
        highWaterMark = config.maxPollRecords,
        lowWaterMark  = config.maxPollRecords / 4,
        onPause  = { val current = partitionManager.assignments(); if (current.isNotEmpty()) runBlocking { client.pause(current).await() } },
        onResume = { val current = partitionManager.assignments(); if (current.isNotEmpty()) runBlocking { client.resume(current).await() } },
    )

    private val partitionManager = CoordinatingPartitionManager(
        pendingSeeks = pendingSeeks,
        ends         = ends,
        paused       = paused::get,
        instanceId   = instanceId,
        log          = log,
    )

    private val committer: Committer = BufferedCommitter(
        client        = client,
        instanceId    = instanceId,
        assignments   = partitionManager::assignments,
        log           = log,
        bufferSize    = config.maxPollRecords,
        maxBatchSize  = config.commitBatchSize,
        maxBatchTime  = config.commitBatchTimeout,
    )

    private val pollerShutdown = Sinks.broadcast<Unit>()
    private val pollingDone    = Sinks.broadcast<Unit>()

    private val poller: Poller = BufferedPoller(
        client       = client,
        buffer       = buffer,
        assignments  = partitionManager::assignments,
        instanceId   = instanceId,
        pollInterval = config.pollInterval,
        shutdownSink = pollerShutdown,
        doneSink     = pollingDone,
        log          = log,
    )

    fun start(
        from: StartOffset = config.startOffset,
        until: EndOffset  = EndOffset.Continuous,
    ) {
        check(started.compareAndSet(false, true)) { "$instanceId start() called more than once" }
        poller.start()
        committer.start()
        committer.committedOffsets.drain({ emitEvent(ConsumerEvent.Committed(it)) }, {})
        emitEvent(ConsumerEvent.Started)
        val complete = completionSignal()
        val sync     = syncSignal(until, complete)
        incomingFeed(sync, closeSignal.asMany(), from, until)
            .groupBy(
                keySelector  = { received: Received<ByteArray> -> received.position.partition },
                groupHandler = { _, group ->
                    processor.process(group).concatMap { position ->
                        One.defer { committer.markProcessed(position) }.flatMapMany { Many.empty() }
                    }
                },
            )
            .drain({}, {})
    }

    fun shutdown(duration: Duration? = null) {
        if (!disposed.compareAndSet(false, true)) return
        emitEvent(ConsumerEvent.Stopped)
        eventSink.complete()
        ShutdownCoordinator(
            client      = client,
            closeSignal = closeSignal,
            poller      = poller,
            committer   = committer,
            instanceId  = instanceId,
            log         = log,
        ).shutdown(duration)
    }

    val isDisposed: Boolean get() = disposed.get()

    fun pause() {
        paused.set(true)
        poller.pause()
        emitEvent(ConsumerEvent.Paused)
    }

    fun resume() {
        paused.set(false)
        poller.resume()
        emitEvent(ConsumerEvent.Resumed)
    }

    private fun emitEvent(event: ConsumerEvent) = eventSink.tryEmit(event)

    private fun incomingFeed(
        sync: Many<Received<ByteArray>>,
        closeSignal: Many<Unit>,
        from: StartOffset,
        until: EndOffset,
    ): Many<Received<ByteArray>> = client.partitionsFor(config.topics)
        .flatMapMany { topicPartitions ->
            val awaitSubscription = client.subscribe(
                config.topics,
                object : RebalanceListener {
                    override fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions) {
                        partitionManager.onPartitionsRevoked(context, partitions, committer.processedOffsets)
                        emitEvent(ConsumerEvent.Revoked(partitions))
                    }
                    override fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions) {
                        val seeds = partitionManager.onPartitionsAssigned(context, partitions)
                        if (seeds.isNotEmpty()) committer.seedOffsets(seeds)
                        emitEvent(ConsumerEvent.Assigned(partitions))
                    }
                },
            )
            val messages = buffer.asMany()
                .filter { it.position.partition in partitionManager.assignments() }
                .takeUntilOther(sync)
            val synchronizedMessages = merge(messages, sync)
            initOffsets(topicPartitions, from, until)
                .flatMapMany { awaitSubscription.flatMapMany { synchronizedMessages } }
        }
        .takeUntilOther(closeSignal)
        .doOnSubscribe { log.kafka.subscribed(instanceId, config.topics) }
        .doOnError { cause -> log.kafka.consumerFailed(instanceId, config.topics, cause.cause ?: cause) }
        .retry(Policy.retry().withBackoff(500.milliseconds, 30.seconds))

    private fun initOffsets(topicPartitions: Partitions, from: StartOffset, until: EndOffset): One<Positions> {
        val loadEndings: One<Positions> = if (until is CatchUp)
            client.endOffsets(topicPartitions).map { it.also { e -> ends.store(e) } }
        else One.single(emptySet())

        val loadBeginnings: One<Positions> = when (from) {
            is Earliest -> client.committed(topicPartitions).flatMap { committed ->
                val unseeded = topicPartitions.filter { it !in committed }.toSet()
                if (unseeded.isEmpty()) One.single(emptySet())
                else client.beginningOffsets(unseeded).map { beginnings ->
                    val targets = beginnings.asOffsets()
                    if (targets.isNotEmpty()) pendingSeeks.store(targets)
                    beginnings
                }
            }
            is Latest -> One.single(emptySet())
            is AtTimestamp -> client.offsetsForTimes(topicPartitions, from.instant).flatMap { positions ->
                if (positions.isEmpty())
                    client.endOffsets(topicPartitions).map { endOffsets ->
                        val targets = endOffsets.asOffsets()
                        if (targets.isNotEmpty()) pendingSeeks.store(targets)
                        endOffsets
                    }
                else zip(One.single(positions.asOffsets()), client.committed(topicPartitions)) { targets, committed ->
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
            from is Earliest && until is CatchUp    -> loadBeginnings.flatMap { loadEndings }
            from is Earliest                        -> loadBeginnings
            from is AtTimestamp && until is CatchUp -> loadBeginnings.flatMap { loadEndings }
            until is CatchUp                        -> loadEndings
            from is AtTimestamp                     -> loadBeginnings
            else                                    -> One.single(emptySet())
        }
    }

    private fun completionSignal(): Many<Position> = committer.positions
        .filter { _ ->
            partitionManager.assignments().all { partition ->
                val lastPosition = ends.load().find { it.partition == partition }
                lastPosition == null || (committer.processedOffsets[partition] ?: 0L) >= lastPosition.offset
            }
        }
        .take(1)

    private fun syncSignal(until: EndOffset, complete: Many<Position>): Many<Received<ByteArray>> = when (until) {
        is CatchUp              -> Many.empty<Received<ByteArray>>().delaySubscription(complete)
        is EndOffset.Continuous -> Many.never()
    }

    fun hasNoAssignments(): Boolean = partitionManager.assignments().isEmpty()

    fun position(partition: Partition): Long =
        when (val result = runBlocking { client.positionOf(partition).await() }) {
            is Success -> result.value
            is Failure -> throw ConsumerNotActive("$instanceId position() failed for $partition")
        }

    fun lag(partition: Partition): Long = runBlocking {
        val end = when (val result = client.endOffsetOf(partition).await()) {
            is Success -> result.value
            is Failure -> throw ConsumerNotActive("$instanceId lag() failed for $partition")
        }
        val pos = when (val result = client.positionOf(partition).await()) {
            is Success -> result.value
            is Failure -> throw ConsumerNotActive("$instanceId lag() failed for $partition")
        }
        end - pos
    }
}
