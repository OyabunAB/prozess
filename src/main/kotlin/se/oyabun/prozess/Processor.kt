package se.oyabun.prozess

import se.oyabun.aelv.Many
import se.oyabun.aelv.One
import se.oyabun.aelv.Policy
import se.oyabun.aelv.asMany
import se.oyabun.aelv.bufferTimeout
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.filter
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.flatMapSequential
import se.oyabun.aelv.groupBy
import se.oyabun.aelv.map
import se.oyabun.aelv.mapNotNull
import se.oyabun.aelv.retry
import se.oyabun.aelv.toList
import se.oyabun.prozess.Prozess.DeserializationResult
import se.oyabun.prozess.Prozess.Deserializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.milliseconds

data class Processable<M>(
    val received: Received,
    val value: M,
)

data class RetryConfig(
    val minBackoff: Duration = 1.seconds,
    val maxBackoff: Duration = 30.seconds,
)

interface Processor<M : Any> {
    fun process(partition: Many<Received>): Many<Position>
}

internal class ContiguousOffsetTracker {
    private val completed = java.util.concurrent.ConcurrentSkipListSet<Long>()
    @Volatile private var watermark = 0L

    @Synchronized
    fun onCompleted(position: Position): Position? {
        val offset = position.offset
        if (offset < watermark) return null
        completed.add(offset)
        var advanced = false
        while (completed.remove(watermark)) { watermark++; advanced = true }
        return if (advanced) Position(position.partition, watermark - 1) else null
    }
}

class DefaultProcessor<M : Any> private constructor(
    private val pipeline: (Many<Received>) -> Many<Position>,
) : Processor<M> {

    override fun process(partition: Many<Received>): Many<Position> = pipeline(partition)

    companion object {

        private fun retryPolicy(retryConfig: RetryConfig): Policy.Retry =
            Policy.retry().withBackoff(retryConfig.minBackoff, retryConfig.maxBackoff).on { true }

        fun <M : Any> each(
            deserializer: Deserializer<M>,
            handler: (Processable<M>) -> Unit,
            retryConfig: RetryConfig = RetryConfig(),
            maxConcurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }
            fun processOne(received: Received): One<Position> = One.defer {
                when (val result = deserializer(received)) {
                    is DeserializationResult.Message    -> { handler(Processable(received, result.value)); received.position }
                    is DeserializationResult.Tombstone  -> { log.processing.tombstone(received.position); received.position }
                    is DeserializationResult.PoisonPill -> { log.processing.poisonPill(received.position, result.reason); received.position }
                }
            }.retry(policy)

            return DefaultProcessor { partition ->
                partition.flatMapSequential(maxConcurrency) { received: Received -> processOne(received).asMany() }
            }
        }

        fun <M : Any> batch(
            deserializer: Deserializer<M>,
            handler: (List<Processable<M>>) -> Unit,
            batchSize: Int,
            batchDuration: Duration = Duration.INFINITE,
            retryConfig: RetryConfig = RetryConfig(),
            maxConcurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }

            fun processBatch(messages: List<Received>): Many<Position> =
                Many.from(messages)
                    .flatMap { received: Received ->
                        One.defer {
                            when (val result = deserializer(received)) {
                                is DeserializationResult.Message    -> BatchItem<M>(Processable(received, result.value))
                                is DeserializationResult.Tombstone  -> { log.processing.tombstone(received.position); BatchItem(ack = received.position) }
                                is DeserializationResult.PoisonPill -> { log.processing.poisonPill(received.position, result.reason); BatchItem(ack = received.position) }
                            }
                        }.retry(policy).asMany()
                    }
                    .toList()
                    .flatMapMany { items: List<BatchItem<M>> ->
                        val processables: List<Processable<M>> = items.mapNotNull { it.processable }
                        val acks: List<Position>               = items.mapNotNull { it.ack }
                        if (processables.isEmpty()) return@flatMapMany Many.from(acks)
                        One.defer {
                            handler(processables)
                            processables.map { it.received.position } + acks
                        }.retry(policy).flatMapMany { positions: List<Position> -> Many.from(positions.sortedBy { p -> p.offset }) }
                    }

            return DefaultProcessor { partition ->
                partition.bufferTimeout(batchSize, batchDuration)
                    .flatMapSequential(maxConcurrency) { messages: List<Received> -> processBatch(messages) }
            }
        }

        fun <K : Any, M : Any> groupedEach(
            deserializer: Deserializer<M>,
            keyExtractor: (Processable<M>) -> K,
            handler: (K, Processable<M>) -> Unit,
            retryConfig: RetryConfig = RetryConfig(),
            concurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }

            fun partitionPipeline(partition: Many<Received>): Many<Position> {
                val tracker = ContiguousOffsetTracker()
                return deserialize(log, deserializer, partition, policy)
                    .groupBy(
                        keySelector = { event: GroupedEvent<M> ->
                            when (event) {
                                is GroupedEvent.Message -> GroupKey.Defined(keyExtractor(event.processable))
                                is GroupedEvent.Ignored -> GroupKey.Undefined
                            }
                        },
                        groupHandler = { key: GroupKey<K>, group: Many<GroupedEvent<M>> ->
                            when (key) {
                                is GroupKey.Undefined -> group.mapNotNull { event ->
                                    when (event) {
                                        is GroupedEvent.Ignored -> event.position
                                        is GroupedEvent.Message -> null
                                    }
                                }
                                is GroupKey.Defined -> group
                                    .mapNotNull { event ->
                                        when (event) {
                                            is GroupedEvent.Message<M> -> event.processable
                                            is GroupedEvent.Ignored    -> null
                                        }
                                    }
                                    .concatMap { p: Processable<M> ->
                                        One.defer { handler(key.key, p); p.received.position }.retry(policy).asMany()
                                    }
                            }
                        },
                    )
                    .mapNotNull { position: Position -> tracker.onCompleted(position) }
            }

            return DefaultProcessor { partition -> partitionPipeline(partition) }
        }

        fun <K : Any, M : Any> groupedBatch(
            deserializer: Deserializer<M>,
            keyExtractor: (Processable<M>) -> K,
            handler: (K, List<Processable<M>>) -> Unit,
            batchSize: Int,
            batchDuration: Duration = Duration.INFINITE,
            retryConfig: RetryConfig = RetryConfig(),
            concurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }

            fun partitionPipeline(partition: Many<Received>): Many<Position> {
                val tracker = ContiguousOffsetTracker()
                return deserialize(log, deserializer, partition, policy)
                    .groupBy(
                        keySelector = { event: GroupedEvent<M> ->
                            when (event) {
                                is GroupedEvent.Message -> GroupKey.Defined(keyExtractor(event.processable))
                                is GroupedEvent.Ignored -> GroupKey.Undefined
                            }
                        },
                        groupHandler = { key: GroupKey<K>, group: Many<GroupedEvent<M>> ->
                            when (key) {
                                is GroupKey.Undefined -> group.mapNotNull { event ->
                                    when (event) {
                                        is GroupedEvent.Ignored -> event.position
                                        is GroupedEvent.Message -> null
                                    }
                                }
                                is GroupKey.Defined -> group
                                    .mapNotNull { event ->
                                        when (event) {
                                            is GroupedEvent.Message<M> -> event.processable
                                            is GroupedEvent.Ignored    -> null
                                        }
                                    }
                                    .bufferTimeout(batchSize, batchDuration)
                                    .filter { it.isNotEmpty() }
                                    .concatMap { batch: List<Processable<M>> ->
                                        One.defer {
                                            handler(key.key, batch)
                                            batch.map { p -> p.received.position }
                                        }.retry(policy).flatMapMany { positions: List<Position> -> Many.from(positions) }
                                    }
                            }
                        },
                    )
                    .mapNotNull { position: Position -> tracker.onCompleted(position) }
            }

            return DefaultProcessor { partition -> partitionPipeline(partition) }
        }

        private fun <M> deserialize(
            log: Logger,
            deserializer: Deserializer<M>,
            partition: Many<Received>,
            policy: Policy.Retry,
        ): Many<GroupedEvent<M>> = partition.flatMap { received: Received ->
            One.defer {
                when (val result = deserializer(received)) {
                    is DeserializationResult.Message    -> GroupedEvent.Message(Processable(received, result.value))
                    is DeserializationResult.Tombstone  -> { log.processing.tombstone(received.position); GroupedEvent.Ignored(received.position) }
                    is DeserializationResult.PoisonPill -> { log.processing.poisonPill(received.position, result.reason); GroupedEvent.Ignored(received.position) }
                }
            }.retry(policy).asMany()
        }
    }
}

private data class BatchItem<M>(
    val processable: Processable<M>? = null,
    val ack: Position? = null,
)

private sealed class GroupedEvent<out M> {
    class Message<M>(val processable: Processable<M>) : GroupedEvent<M>()
    class Ignored(val position: Position) : GroupedEvent<Nothing>()
}

private sealed class GroupKey<out K> {
    data class Defined<K>(val key: K) : GroupKey<K>()
    data object Undefined : GroupKey<Nothing>()
}
