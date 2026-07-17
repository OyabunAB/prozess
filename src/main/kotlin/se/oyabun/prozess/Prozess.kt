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

import kotlinx.coroutines.runBlocking
import kotlin.reflect.KClass
import se.oyabun.aelv.Failure
import se.oyabun.aelv.Many
import se.oyabun.aelv.Success
import se.oyabun.aelv.await
import se.oyabun.aelv.toList
import kotlin.time.Duration

/**
 * Top-level entry point for creating Kafka consumers and producers.
 *
 * All public factory functions are on this object. Consumers are created via
 * the [consumer] overloads and the resulting [ConsumerBuilder] or [GroupedConsumerBuilder].
 * Producers are created via the [producer] overloads.
 *
 * ```kotlin
 * // Simple consumer
 * val consumer = Prozess.consumer(config, { String(it) }) { _, _, value ->
 *     println(value)
 * }
 * consumer.start()
 *
 * // Keyed producer
 * val producer = Prozess.producer(config, { it.userId }, { it.toByteArray() }, { it.toJson() })
 * producer.sendAll(events)
 * producer.close()
 * ```
 */
object Prozess {
    /** Sentinel for [PartitionExtractor] — instructs Kafka to assign a partition via the default partitioner. */
    const val NO_PARTITION: Int = -1

    /** Sentinel for [TimestampExtractor] — instructs Kafka to use the broker ingestion time. */
    const val NO_TIMESTAMP: Long = -1L

    /** Deserializes a raw key [ByteArray] into type [K]. */
    typealias KeyDeserializer<K>     = (ByteArray) -> K
    /** Extracts the key of type [K] from a message of type [M]. Used for [Producer.Keyed]. */
    typealias KeyExtraction<M, K>    = (M) -> K
    /** Serializes an extracted key of type [K] to [ByteArray]. */
    typealias KeySerializer<K>       = (K) -> ByteArray
    /** Serializes a message of type [M] to [ByteArray] for transmission to Kafka. */
    typealias Serializer<M>          = (M) -> ByteArray
    /** Returns the list of [Header]s to attach to a produced record. */
    typealias HeaderEnricher<M>      = (M) -> Headers
    /** Returns the target partition for a produced record, or [NO_PARTITION] for default partitioning. */
    typealias PartitionExtractor<M>  = (M) -> Int
    /** Returns the record timestamp in epoch milliseconds, or [NO_TIMESTAMP] to use broker time. */
    typealias TimestampExtractor<M>  = (M) -> Long
    /** Converts a raw [Received] record to a [DeserializationResult]. */
    typealias Deserializer<M>        = (Received<ByteArray>) -> DeserializationResult<M>

    /**
     * Result of deserializing a raw Kafka record.
     *
     * Return [Message] for normal records, [Tombstone] for null-value records
     * (Kafka delete markers), and [PoisonPill] for records that cannot be processed
     * (malformed data). Tombstones and poison pills are logged and acknowledged
     * without calling the handler.
     */
    sealed interface DeserializationResult<out M> {
        /** A successfully deserialized record with [value]. */
        data class Message<M>(val value: M) : DeserializationResult<M>
        /** A Kafka tombstone — the record had a null value. */
        data object Tombstone : DeserializationResult<Nothing>
        /**
         * A record that cannot be processed.
         *
         * The record will be acknowledged and skipped. [reason] is logged at `WARN` level.
         */
        data class PoisonPill(val reason: String? = null) : DeserializationResult<Nothing>
    }

    /**
     * A synchronous Kafka producer interface.
     *
     * Use [Keyed] when records have a meaningful key that controls per-key ordering.
     * Use [Unkeyed] for round-robin partitioned producers.
     *
     * Create instances via [Prozess.producer].
     */
    sealed interface Producer<M> {
        /** Initialises the producer for transactional use. Must be called before [beginTransaction]. */
        fun initTransactions()
        /** Begins a Kafka transaction. */
        fun beginTransaction()
        /** Commits the current Kafka transaction. */
        fun commitTransaction()
        /** Aborts the current Kafka transaction. */
        fun abortTransaction()
        /**
         * Sends consumer [offsets] to the current transaction, associating them with [member].
         *
         * Used to implement read-process-write exactly-once semantics.
         */
        fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember)
        /** Flushes and closes the underlying Kafka producer. Idempotent. */
        fun close()

        /** A producer with a configured key extractor and serializer. Per-key ordering is guaranteed in [sendAll]. */
        sealed interface Keyed<M> : Producer<M> {
            /** Sends [value] to Kafka and blocks until acknowledged. Returns the written offset. */
            fun send(value: M): Long

            /**
             * Sends all [messages] to Kafka and blocks until every message has been acknowledged.
             * Messages are grouped by extracted key and sent in order within each key group.
             * Headers, partition, and timestamp are derived from each message via the configured extractors.
             * Returns the sent messages as a list (pass-through).
             * @throws SendFailure if any message fails to send.
             */
            fun sendAll(messages: Collection<M>): List<M>

            /** Convenience vararg overload — delegates to [sendAll]. */
            fun sendAll(vararg messages: M): List<M> = sendAll(messages.toList())
        }

        /** A producer with no key — records are sent with a null key and Kafka assigns partitions via round-robin. */
        sealed interface Unkeyed<M> : Producer<M> {
            /** Sends [value] to Kafka and blocks until acknowledged. Returns the written offset. */
            fun send(value: M): Long

            /**
             * Sends all [messages] to Kafka and blocks until every message has been acknowledged.
             * Headers, partition, and timestamp are derived from each message via the configured extractors.
             * Returns the sent messages as a list (pass-through).
             * @throws SendFailure if any message fails to send.
             */
            fun sendAll(messages: Collection<M>): List<M>

            /** Convenience vararg overload — delegates to [sendAll]. */
            fun sendAll(vararg messages: M): List<M> = sendAll(messages.toList())
        }
    }

    /**
     * A running Kafka consumer.
     *
     * Create instances via the [Prozess.consumer] factory functions and builder chain.
     * Call [start] to begin consuming. Call [shutdown] to stop gracefully.
     */
    interface Consumer<K, M : Any> {
        /**
         * Starts consuming from Kafka.
         *
         * @param from  offset reset strategy; defaults to [ConsumerConfig.startOffset].
         * @param until controls when the consumer stops; defaults to [EndOffset.Continuous].
         */
        fun start(from: StartOffset = StartOffset.Latest, until: EndOffset = EndOffset.Continuous)

        /** Initiates graceful shutdown: drains in-flight records, commits final offsets, closes the client. */
        fun shutdown()

        /** `true` after [shutdown] has been called. */
        val isDisposed: Boolean

        /** Pauses fetching on all currently assigned partitions. */
        fun pause()

        /** Resumes fetching on all currently assigned partitions. */
        fun resume()

        /** `true` when the consumer currently has no assigned partitions. */
        fun hasNoAssignments(): Boolean

        /** Returns the current fetch position for [partition]. */
        fun position(partition: Partition): Long

        /** Returns the number of uncommitted records for [partition] (end offset minus current position). */
        fun lag(partition: Partition): Long

        /** Registers [callback] to be invoked for every [ConsumerEvent]. */
        fun onEvent(callback: (ConsumerEvent) -> Unit)

        /** Registers [callback] to be invoked only for events of type [T]. */
        fun <T : ConsumerEvent> onEvent(type: KClass<T>, callback: (T) -> Unit) =
            onEvent { if (type.isInstance(it)) @Suppress("UNCHECKED_CAST") callback(it as T) }
    }

    private fun <M : Any> simpleDeserializer(messageDeserializer: (ByteArray) -> M): Deserializer<M> = { received ->
        when (val message = received.message) {
            is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
            is ReceivedMessage.Data      -> DeserializationResult.Message(messageDeserializer(message.bytes))
        }
    }

    private fun <K> keyMapperFor(keyDeserializer: KeyDeserializer<K>): (Key<ByteArray>) -> Key<K> = { k ->
        when (k) {
            is Key.Present -> Key.Present(keyDeserializer(k.value))
            is Key.Missing -> Key.Missing
        }
    }

    /** Convenience factory — deserializes values with [messageDeserializer], keys remain raw [ByteArray]. */
    fun <M : Any> consumer(
        config: ConsumerConfig,
        messageDeserializer: (ByteArray) -> M,
        process: (Headers, Key<ByteArray>, M) -> Unit,
        instance: String = shortId(),
    ): Consumer<ByteArray, M> = ConsumerBuilder<ByteArray, M>(config, { it }, simpleDeserializer(messageDeserializer), instance)
        .each { p -> process(p.headers, p.key, p.value) }

    /** Convenience factory — deserializes keys with [keyDeserializer] and values with [messageDeserializer]. */
    fun <K : Any, M : Any> consumer(
        config: ConsumerConfig,
        keyDeserializer: KeyDeserializer<K>,
        messageDeserializer: (ByteArray) -> M,
        process: (Headers, Key<K>, M) -> Unit,
        instance: String = shortId(),
    ): Consumer<K, M> = ConsumerBuilder(config, keyMapperFor(keyDeserializer), simpleDeserializer(messageDeserializer), instance)
        .each { p -> process(p.headers, p.key, p.value) }

    /** Builder factory — deserializes values with [deserializer], keys remain raw [ByteArray]. */
    fun <M : Any> consumer(
        config: ConsumerConfig,
        deserializer: Deserializer<M>,
        instance: String = shortId(),
    ): ConsumerBuilder<ByteArray, M> = ConsumerBuilder(config, { it }, deserializer, instance)

    /** Builder factory — deserializes both keys with [keyDeserializer] and values with [deserializer]. */
    fun <K : Any, M : Any> consumer(
        config: ConsumerConfig,
        keyDeserializer: KeyDeserializer<K>,
        deserializer: Deserializer<M>,
        instance: String = shortId(),
    ): ConsumerBuilder<K, M> = ConsumerBuilder(config, keyMapperFor(keyDeserializer), deserializer, instance)

    private fun <K, M : Any> wrap(config: ConsumerConfig, processor: Processor<M>, instance: String): Consumer<K, M> =
        object : Consumer<K, M> {
            private val delegate = StreamingConsumer(config, processor, instance)
            override fun start(from: StartOffset, until: EndOffset) = delegate.start(from, until)
            override fun shutdown()                                  = delegate.shutdown()
            override val isDisposed: Boolean                         get() = delegate.isDisposed
            override fun pause()                                     = delegate.pause()
            override fun resume()                                    = delegate.resume()
            override fun hasNoAssignments(): Boolean                 = delegate.hasNoAssignments()
            override fun position(partition: Partition): Long        = delegate.position(partition)
            override fun lag(partition: Partition): Long             = delegate.lag(partition)
            override fun onEvent(callback: (ConsumerEvent) -> Unit)  = delegate.onEvent(callback)
        }

    private fun <T> Any.valueOrThrow(instance: String): T {
        @Suppress("UNCHECKED_CAST")
        return when (this) {
            is Success<*> -> value as T
            is Failure<*> -> throw SendFailure("$instance send failed", value as? Throwable ?: RuntimeException("unknown cause"))
            else          -> throw IllegalStateException("Unexpected result type: $this")
        }
    }

    /**
     * Builder for configuring and starting a consumer.
     *
     * Obtained from [Prozess.consumer]. Call one of [each], [batch], or [groupBy]
     * to wire a processing mode and receive a [Consumer].
     *
     * @param K the Kafka key type.
     * @param M the deserialized message type.
     */
    class ConsumerBuilder<K, M : Any>(
        private val config: ConsumerConfig,
        private val keyMapper: (Key<ByteArray>) -> Key<K>,
        private val deserializer: Deserializer<M>,
        private val instance: String = shortId(),
    ) {
        /**
         * Processes each record individually.
         *
         * @param maxConcurrency maximum records handled concurrently within a single partition.
         * @param handler        called for each deserialized record.
         */
        fun each(
            maxConcurrency: Int = 1,
            handler: (Processable<K, M>) -> Unit,
        ): Consumer<K, M> = wrapProcessor(
            DefaultProcessor.each(keyMapper, deserializer, handler, maxConcurrency = maxConcurrency)
        )

        /**
         * Collects records into time- or size-bounded batches and processes each batch atomically.
         *
         * @param size           maximum records per batch (defaults to [ConsumerConfig.maxPollRecords]).
         * @param duration       maximum time before a partial batch is flushed.
         * @param maxConcurrency maximum batches processed concurrently within a single partition.
         * @param handler        called with each complete batch.
         */
        fun batch(
            size: Int = config.maxPollRecords,
            duration: Duration = Duration.INFINITE,
            maxConcurrency: Int = 1,
            handler: (List<Processable<K, M>>) -> Unit,
        ): Consumer<K, M> = wrapProcessor(
            DefaultProcessor.batch(keyMapper, deserializer, handler, size, duration, maxConcurrency = maxConcurrency)
        )

        /**
         * Groups records by the key returned by [extractor] and returns a [GroupedConsumerBuilder]
         * for configuring per-key processing.
         */
        fun <GK : Any> groupBy(extractor: (Processable<K, M>) -> GK): GroupedConsumerBuilder<GK, K, M> =
            GroupedConsumerBuilder(config, keyMapper, deserializer, extractor, instance)

        /** Uses a custom [Processor] implementation. */
        fun processor(custom: Processor<M>): Consumer<K, M> = wrapProcessor(custom)

        private fun wrapProcessor(processor: Processor<M>): Consumer<K, M> =
            wrap(config, processor, instance)
    }

    /**
     * Builder for configuring grouped (per-key) processing.
     *
     * Obtained from [ConsumerBuilder.groupBy]. Call [each] or [batch] to finalise
     * the configuration and receive a [Consumer].
     *
     * @param GK the grouping key type.
     * @param K  the Kafka key type.
     * @param M  the deserialized message type.
     */
    class GroupedConsumerBuilder<GK : Any, K, M : Any>(
        private val config: ConsumerConfig,
        private val keyMapper: (Key<ByteArray>) -> Key<K>,
        private val deserializer: Deserializer<M>,
        private val keyExtractor: (Processable<K, M>) -> GK,
        private val instance: String = shortId(),
    ) {
        /**
         * Processes each record individually within its key group.
         *
         * Records with the same key are processed sequentially; different keys may
         * be processed concurrently up to [concurrency].
         *
         * @param concurrency maximum number of key groups processed concurrently.
         * @param handler     called with the grouping key and each deserialized record.
         */
        fun each(
            concurrency: Int = 1,
            handler: (GK, Processable<K, M>) -> Unit,
        ): Consumer<K, M> = wrapProcessor(
            DefaultProcessor.groupedEach(keyMapper, deserializer, keyExtractor, handler, concurrency = concurrency)
        )

        /**
         * Collects records per key group into time- or size-bounded batches.
         *
         * @param size        maximum records per batch per key group.
         * @param duration    maximum time before a partial batch is flushed.
         * @param concurrency maximum number of key groups processed concurrently.
         * @param handler     called with the grouping key and each complete batch.
         */
        fun batch(
            size: Int = config.maxPollRecords,
            duration: Duration = Duration.INFINITE,
            concurrency: Int = 1,
            handler: (GK, List<Processable<K, M>>) -> Unit,
        ): Consumer<K, M> = wrapProcessor(
            DefaultProcessor.groupedBatch(keyMapper, deserializer, keyExtractor, handler, size, duration, concurrency = concurrency)
        )

        private fun wrapProcessor(processor: Processor<M>): Consumer<K, M> =
            wrap(config, processor, instance)
    }

    /**
     * Creates an unkeyed producer that sends records with a null key.
     *
     * @param config             producer configuration.
     * @param serializer         converts messages to [ByteArray].
     * @param headerEnricher     produces [Headers] for each message; defaults to empty.
     * @param partitionExtractor selects a target partition; defaults to [NO_PARTITION] (default partitioner).
     * @param timestampExtractor returns the record timestamp; defaults to [NO_TIMESTAMP] (broker time).
     * @param instance           label for logging and thread naming.
     */
    fun <M : Any> producer(
        config: ProducerConfig,
        serializer: Serializer<M>,
        headerEnricher: HeaderEnricher<M> = { emptyList() },
        partitionExtractor: PartitionExtractor<M> = { NO_PARTITION },
        timestampExtractor: TimestampExtractor<M> = { NO_TIMESTAMP },
        instance: String = shortId(),
    ): Producer.Unkeyed<M> = UnkeyedProducerImpl(
        StreamingProducer(config, instance, null, null, headerEnricher, partitionExtractor, timestampExtractor, serializer),
        instance,
    )

    /**
     * Creates a keyed producer that groups records by key for per-key ordering guarantees.
     *
     * @param config             producer configuration.
     * @param keyExtractor       extracts the key from each message.
     * @param keySerializer      serializes the extracted key to [ByteArray].
     * @param serializer         converts messages to [ByteArray].
     * @param headerEnricher     produces [Headers] for each message; defaults to empty.
     * @param partitionExtractor selects a target partition; defaults to [NO_PARTITION].
     * @param timestampExtractor returns the record timestamp; defaults to [NO_TIMESTAMP].
     * @param instance           label for logging and thread naming.
     */
    fun <K : Any, M : Any> producer(
        config: ProducerConfig,
        keyExtractor: KeyExtraction<M, K>,
        keySerializer: KeySerializer<K>,
        serializer: Serializer<M>,
        headerEnricher: HeaderEnricher<M> = { emptyList() },
        partitionExtractor: PartitionExtractor<M> = { NO_PARTITION },
        timestampExtractor: TimestampExtractor<M> = { NO_TIMESTAMP },
        instance: String = shortId(),
    ): Producer.Keyed<M> = KeyedProducerImpl(
        StreamingProducer(config, instance, keyExtractor, { m -> keySerializer(keyExtractor(m)) }, headerEnricher, partitionExtractor, timestampExtractor, serializer),
        instance,
    )

    private class UnkeyedProducerImpl<M : Any>(
        private val delegate: StreamingProducer<M>,
        private val instance: String,
    ) : Producer.Unkeyed<M> {
        override fun send(value: M): Long =
            runBlocking { delegate.send(value).await() }.valueOrThrow(instance)
        override fun sendAll(messages: Collection<M>): List<M> =
            runBlocking { delegate.sendAll(Many.from(messages)).toList().await() }.valueOrThrow(instance)
        override fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember) { runBlocking { delegate.sendOffsetsToTransaction(offsets, member).await() } }
        override fun initTransactions()  { runBlocking { delegate.initTransactions().await() } }
        override fun beginTransaction()  { runBlocking { delegate.beginTransaction().await() } }
        override fun commitTransaction() { runBlocking { delegate.commitTransaction().await() } }
        override fun abortTransaction()  { runBlocking { delegate.abortTransaction().await() } }
        override fun close()             { runBlocking { delegate.close().await() } }
    }

    private class KeyedProducerImpl<M : Any>(
        private val delegate: StreamingProducer<M>,
        private val instance: String,
    ) : Producer.Keyed<M> {
        override fun send(value: M): Long =
            runBlocking { delegate.send(value).await() }.valueOrThrow(instance)
        override fun sendAll(messages: Collection<M>): List<M> =
            runBlocking { delegate.sendAll(Many.from(messages)).toList().await() }.valueOrThrow(instance)
        override fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember) { runBlocking { delegate.sendOffsetsToTransaction(offsets, member).await() } }
        override fun initTransactions()  { runBlocking { delegate.initTransactions().await() } }
        override fun beginTransaction()  { runBlocking { delegate.beginTransaction().await() } }
        override fun commitTransaction() { runBlocking { delegate.commitTransaction().await() } }
        override fun abortTransaction()  { runBlocking { delegate.abortTransaction().await() } }
        override fun close()             { runBlocking { delegate.close().await() } }
    }
}

/**
 * Registers [callback] to be invoked only for [ConsumerEvent]s of the reified type [T].
 *
 * Inline convenience alternative to the `KClass` overload:
 * ```kotlin
 * consumer.onEvent<ConsumerEvent.Assigned> { println(it.partitions) }
 * ```
 */
inline fun <reified T : ConsumerEvent> Prozess.Consumer<*, *>.onEvent(noinline callback: (T) -> Unit) =
    onEvent { if (it is T) callback(it) }
