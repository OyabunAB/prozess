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

/**
 * A deserialized Kafka record ready for processing.
 *
 * @param K the Kafka key type; [Nothing] for [Prozess.Consumer.Unkeyed] consumers.
 * @param M the deserialized value type.
 */
data class Processable<out K, M>(
    val key: Key<K>,
    val value: M,
    val position: Position,
    val headers: Headers,
)

data class RetryConfig(
    val minBackoff: Duration = 1.seconds,
    val maxBackoff: Duration = 30.seconds,
)

interface Processor<M : Any> {
    fun process(partition: Many<Received<ByteArray>>): Many<Position>
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
    private val pipeline: (Many<Received<ByteArray>>) -> Many<Position>,
) : Processor<M> {

    override fun process(partition: Many<Received<ByteArray>>): Many<Position> = pipeline(partition)

    companion object {

        private fun retryPolicy(retryConfig: RetryConfig): Policy.Retry =
            Policy.retry().withBackoff(retryConfig.minBackoff, retryConfig.maxBackoff).on { true }

        fun <KafkaKey, M : Any> each(
            keyMapper: (Key<ByteArray>) -> Key<KafkaKey> = { Key.Missing },
            deserializer: Deserializer<M>,
            handler: (Processable<KafkaKey, M>) -> Unit,
            retryConfig: RetryConfig = RetryConfig(),
            maxConcurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }
            fun processOne(received: Received<ByteArray>): One<Position> = One.defer {
                when (val result = deserializer(received)) {
                    is DeserializationResult.Message    -> { handler(Processable(keyMapper(received.key), result.value, received.position, received.headers)); received.position }
                    is DeserializationResult.Tombstone  -> { log.processing.tombstone(received.position); received.position }
                    is DeserializationResult.PoisonPill -> { log.processing.poisonPill(received.position, result.reason); received.position }
                }
            }.retry(policy)

            return DefaultProcessor { partition ->
                partition.flatMapSequential(maxConcurrency) { received -> processOne(received).asMany() }
            }
        }

        fun <KafkaKey, M : Any> batch(
            keyMapper: (Key<ByteArray>) -> Key<KafkaKey> = { Key.Missing },
            deserializer: Deserializer<M>,
            handler: (List<Processable<KafkaKey, M>>) -> Unit,
            batchSize: Int,
            batchDuration: Duration = Duration.INFINITE,
            retryConfig: RetryConfig = RetryConfig(),
            maxConcurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }

            fun processBatch(messages: List<Received<ByteArray>>): Many<Position> =
                Many.from(messages)
                    .flatMap { received ->
                        One.defer {
                            when (val result = deserializer(received)) {
                                is DeserializationResult.Message    -> BatchItem<KafkaKey, M>(Processable(keyMapper(received.key), result.value, received.position, received.headers))
                                is DeserializationResult.Tombstone  -> { log.processing.tombstone(received.position); BatchItem(ack = received.position) }
                                is DeserializationResult.PoisonPill -> { log.processing.poisonPill(received.position, result.reason); BatchItem(ack = received.position) }
                            }
                        }.retry(policy).asMany()
                    }
                    .toList()
                    .flatMapMany { items: List<BatchItem<KafkaKey, M>> ->
                        val processables = items.mapNotNull { it.processable }
                        val acks         = items.mapNotNull { it.ack }
                        if (processables.isEmpty()) return@flatMapMany Many.from(acks)
                        One.defer {
                            handler(processables)
                            processables.map { it.position } + acks
                        }.retry(policy).flatMapMany { positions -> Many.from(positions.sortedBy { it.offset }) }
                    }

            return DefaultProcessor { partition ->
                partition.bufferTimeout(batchSize, batchDuration)
                    .flatMapSequential(maxConcurrency) { messages -> processBatch(messages) }
            }
        }

        fun <GroupingKey : Any, KafkaKey, M : Any> groupedEach(
            keyMapper: (Key<ByteArray>) -> Key<KafkaKey> = { Key.Missing },
            deserializer: Deserializer<M>,
            keyExtractor: (Processable<KafkaKey, M>) -> GroupingKey,
            handler: (GroupingKey, Processable<KafkaKey, M>) -> Unit,
            retryConfig: RetryConfig = RetryConfig(),
            concurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }

            fun partitionPipeline(partition: Many<Received<ByteArray>>): Many<Position> {
                val tracker = ContiguousOffsetTracker()
                return deserialize(log, keyMapper, deserializer, partition, policy)
                    .groupBy(
                        keySelector = { event: GroupedEvent<KafkaKey, M> ->
                            when (event) {
                                is GroupedEvent.Message -> GroupKey.Defined(keyExtractor(event.processable))
                                is GroupedEvent.Ignored -> GroupKey.Undefined
                            }
                        },
                        groupHandler = { key: GroupKey<GroupingKey>, group: Many<GroupedEvent<KafkaKey, M>> ->
                            when (key) {
                                is GroupKey.Undefined   -> group.mapNotNull { event ->
                                    when (event) {
                                        is GroupedEvent.Ignored -> event.position
                                        is GroupedEvent.Message -> null
                                    }
                                }
                                is GroupKey.Defined<GroupingKey> -> group
                                    .mapNotNull { event ->
                                        when (event) {
                                            is GroupedEvent.Message<*, *> -> @Suppress("UNCHECKED_CAST") (event as GroupedEvent.Message<KafkaKey, M>).processable
                                            is GroupedEvent.Ignored       -> null
                                        }
                                    }
                                    .concatMap { p ->
                                        One.defer { handler(key.key, p); p.position }.retry(policy).asMany()
                                    }
                            }
                        },
                    )
                    .mapNotNull { position -> tracker.onCompleted(position) }
            }

            return DefaultProcessor { partition -> partitionPipeline(partition) }
        }

        fun <GroupingKey : Any, KafkaKey, M : Any> groupedBatch(
            keyMapper: (Key<ByteArray>) -> Key<KafkaKey> = { Key.Missing },
            deserializer: Deserializer<M>,
            keyExtractor: (Processable<KafkaKey, M>) -> GroupingKey,
            handler: (GroupingKey, List<Processable<KafkaKey, M>>) -> Unit,
            batchSize: Int,
            batchDuration: Duration = Duration.INFINITE,
            retryConfig: RetryConfig = RetryConfig(),
            concurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }

            fun partitionPipeline(partition: Many<Received<ByteArray>>): Many<Position> {
                val tracker = ContiguousOffsetTracker()
                return deserialize(log, keyMapper, deserializer, partition, policy)
                    .groupBy(
                        keySelector = { event: GroupedEvent<KafkaKey, M> ->
                            when (event) {
                                is GroupedEvent.Message -> GroupKey.Defined(keyExtractor(event.processable))
                                is GroupedEvent.Ignored -> GroupKey.Undefined
                            }
                        },
                        groupHandler = { key: GroupKey<GroupingKey>, group: Many<GroupedEvent<KafkaKey, M>> ->
                            when (key) {
                                is GroupKey.Undefined   -> group.mapNotNull { event ->
                                    when (event) {
                                        is GroupedEvent.Ignored -> event.position
                                        is GroupedEvent.Message -> null
                                    }
                                }
                                is GroupKey.Defined<GroupingKey> -> group
                                    .mapNotNull { event ->
                                        when (event) {
                                            is GroupedEvent.Message<*, *> -> @Suppress("UNCHECKED_CAST") (event as GroupedEvent.Message<KafkaKey, M>).processable
                                            is GroupedEvent.Ignored       -> null
                                        }
                                    }
                                    .bufferTimeout(batchSize, batchDuration)
                                    .filter { it.isNotEmpty() }
                                    .concatMap { batch ->
                                        One.defer {
                                            handler(key.key, batch)
                                            batch.map { it.position }
                                        }.retry(policy).flatMapMany { positions -> Many.from(positions) }
                                    }
                            }
                        },
                    )
                    .mapNotNull { position -> tracker.onCompleted(position) }
            }

            return DefaultProcessor { partition -> partitionPipeline(partition) }
        }

        private fun <KafkaKey, M> deserialize(
            log: Logger,
            keyMapper: (Key<ByteArray>) -> Key<KafkaKey>,
            deserializer: Deserializer<M>,
            partition: Many<Received<ByteArray>>,
            policy: Policy.Retry,
        ): Many<GroupedEvent<KafkaKey, M>> = partition.flatMap { received ->
            One.defer {
                when (val result = deserializer(received)) {
                    is DeserializationResult.Message    -> GroupedEvent.Message(Processable(keyMapper(received.key), result.value, received.position, received.headers))
                    is DeserializationResult.Tombstone  -> { log.processing.tombstone(received.position); GroupedEvent.Ignored(received.position) }
                    is DeserializationResult.PoisonPill -> { log.processing.poisonPill(received.position, result.reason); GroupedEvent.Ignored(received.position) }
                }
            }.retry(policy).asMany()
        }
    }
}

private data class BatchItem<KafkaKey, M>(
    val processable: Processable<KafkaKey, M>? = null,
    val ack: Position? = null,
)

private sealed class GroupedEvent<out KafkaKey, out M> {
    class Message<KafkaKey, M>(val processable: Processable<KafkaKey, M>) : GroupedEvent<KafkaKey, M>()
    class Ignored(val position: Position) : GroupedEvent<Nothing, Nothing>()
}

private sealed class GroupKey<out GroupingKey> {
    data class Defined<GroupingKey>(val key: GroupingKey) : GroupKey<GroupingKey>()
    data object Undefined : GroupKey<Nothing>()
}
