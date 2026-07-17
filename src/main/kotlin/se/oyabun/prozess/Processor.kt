/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import se.oyabun.aelv.doOnError
import se.oyabun.aelv.doOnNext
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
 * @param key      the deserialized record key.
 * @param value    the deserialized message value.
 * @param position the partition and offset of the original record.
 * @param headers  the Kafka record headers.
 */
data class Processable<out K, M>(
    val key: Key<K>,
    val value: M,
    val position: Position,
    val headers: Headers,
)

/**
 * Configures the backoff policy applied when a processing handler throws.
 *
 * @param minBackoff initial backoff before the first retry.
 * @param maxBackoff upper bound for exponential backoff growth.
 */
data class RetryConfig(
    val minBackoff: Duration = 1.seconds,
    val maxBackoff: Duration = 30.seconds,
)

/**
 * Transforms a stream of raw Kafka records into a stream of committed [Position]s.
 *
 * Each [Position] emitted by [process] signals that the record at that offset
 * has been fully handled and is safe to commit.
 */
interface Processor<M : Any> {
    /**
     * Applies this processor's deserialization and handling pipeline to [partition].
     *
     * @param partition a stream of raw records from a single Kafka partition.
     * @return a stream of positions ready to commit.
     */
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

/**
 * Standard [Processor] implementation with four factory modes.
 *
 * All modes apply the same retry contract: if the handler throws, the entire
 * operation is retried with exponential backoff according to [RetryConfig].
 * Tombstone records and poison-pill records (where [DeserializationResult.PoisonPill]
 * is returned) are logged and acknowledged without calling the handler.
 *
 * Create instances via the companion object factory functions rather than the
 * constructor directly.
 */
class DefaultProcessor<M : Any> private constructor(
    private val pipeline: (Many<Received<ByteArray>>) -> Many<Position>,
) : Processor<M> {

    override fun process(partition: Many<Received<ByteArray>>): Many<Position> = pipeline(partition)

    companion object {

        private fun retryPolicy(retryConfig: RetryConfig): Policy.Retry =
            Policy.retry().withBackoff(retryConfig.minBackoff, retryConfig.maxBackoff).on { true }

        /**
         * Processes each record individually.
         *
         * @param keyMapper       maps the raw [ByteArray] key to [KafkaKey]; defaults to [Key.Missing].
         * @param deserializer    converts the raw record to a [DeserializationResult].
         * @param handler         called for each successfully deserialized record.
         * @param retryConfig     backoff policy for handler failures.
         * @param maxConcurrency  maximum number of records processed concurrently within a partition.
         */
        fun <KafkaKey, M : Any> each(
            keyMapper: (Key<ByteArray>) -> Key<KafkaKey> = { Key.Missing },
            deserializer: Deserializer<M>,
            handler: (Processable<KafkaKey, M>) -> Unit,
            retryConfig: RetryConfig = RetryConfig(),
            maxConcurrency: Int = 1,
        ): DefaultProcessor<M> {
            val policy = retryPolicy(retryConfig)
            val log    = Logging.logger { }
            fun processOne(received: Received<ByteArray>): One<Position> {
                var attempts = 0L
                return One.defer {
                    when (val result = deserializer(received)) {
                        is DeserializationResult.Message    -> { handler(Processable(keyMapper(received.key), result.value, received.position, received.headers)); received.position }
                        is DeserializationResult.Tombstone  -> { log.processing.tombstone(received.position); received.position }
                        is DeserializationResult.PoisonPill -> { log.processing.poisonPill(received.position, result.reason); received.position }
                    }
                }
                .doOnError { cause -> log.processing.handlerRetrying(received.position, ++attempts, cause) }
                .doOnNext  { if (attempts > 0) log.processing.handlerRecovered(received.position, attempts) }
                .retry(policy)
            }

            return DefaultProcessor { partition ->
                partition.flatMapSequential(maxConcurrency) { received -> processOne(received).asMany() }
            }
        }

        /**
         * Collects records into time- or size-bounded batches and processes each batch atomically.
         *
         * All records in a batch are deserialized before [handler] is called. Tombstone and
         * poison-pill records within the batch are acknowledged without entering the handler.
         * If the handler throws, the entire batch is retried.
         *
         * @param keyMapper       maps the raw [ByteArray] key to [KafkaKey].
         * @param deserializer    converts each raw record to a [DeserializationResult].
         * @param handler         called with the full deserialized batch.
         * @param batchSize       maximum number of records per batch.
         * @param batchDuration   maximum time to wait before flushing a partial batch.
         * @param retryConfig     backoff policy for handler failures.
         * @param maxConcurrency  maximum number of batches processed concurrently within a partition.
         */
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

        /**
         * Groups records by a user-defined key and processes each key's records
         * sequentially and concurrently across keys.
         *
         * A [ContiguousOffsetTracker] gates offset emission: an offset is only emitted
         * once all lower offsets in the same partition have completed, preventing gaps
         * in committed offsets caused by out-of-order key group completion.
         *
         * @param keyMapper      maps the raw [ByteArray] key to [KafkaKey].
         * @param deserializer   converts each raw record to a [DeserializationResult].
         * @param keyExtractor   extracts the grouping key from each [Processable].
         * @param handler        called per-record with the grouping key and processable.
         * @param retryConfig    backoff policy for handler failures.
         * @param concurrency    maximum number of key groups processed concurrently.
         */
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

        /**
         * Groups records by a user-defined key and processes each key's records in
         * size- or time-bounded batches.
         *
         * Like [groupedEach], a [ContiguousOffsetTracker] ensures safe offset emission.
         *
         * @param keyMapper      maps the raw [ByteArray] key to [KafkaKey].
         * @param deserializer   converts each raw record to a [DeserializationResult].
         * @param keyExtractor   extracts the grouping key from each [Processable].
         * @param handler        called per-batch with the grouping key and list of processables.
         * @param batchSize      maximum records per batch per key group.
         * @param batchDuration  maximum time before flushing a partial batch.
         * @param retryConfig    backoff policy for handler failures.
         * @param concurrency    maximum number of key groups processed concurrently.
         */
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
