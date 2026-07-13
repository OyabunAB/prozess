package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import kotlin.reflect.KClass
import se.oyabun.aelv.Failure
import se.oyabun.aelv.Many
import se.oyabun.aelv.Success
import se.oyabun.aelv.await
import se.oyabun.aelv.toList
import kotlin.time.Duration

object Prozess {
    /** Sentinel for [PartitionExtractor] — instructs Kafka to assign a partition via the default partitioner. */
    const val NO_PARTITION: Int = -1

    /** Sentinel for [TimestampExtractor] — instructs Kafka to use the broker ingestion time. */
    const val NO_TIMESTAMP: Long = -1L

    typealias KeyDeserializer<K>     = (ByteArray) -> K
    typealias KeyExtraction<M, K>    = (M) -> K
    typealias KeySerializer<K>       = (K) -> ByteArray
    typealias Serializer<M>          = (M) -> ByteArray
    typealias HeaderEnricher<M>      = (M) -> Headers
    typealias PartitionExtractor<M>  = (M) -> Int
    typealias TimestampExtractor<M>  = (M) -> Long
    typealias Deserializer<M>        = (Received<ByteArray>) -> DeserializationResult<M>

    sealed interface DeserializationResult<out M> {
        data class Message<M>(val value: M) : DeserializationResult<M>
        data object Tombstone : DeserializationResult<Nothing>
        data class PoisonPill(val reason: String? = null) : DeserializationResult<Nothing>
    }

    sealed interface Producer<M> {
        fun initTransactions()
        fun beginTransaction()
        fun commitTransaction()
        fun abortTransaction()
        fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember)
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

    interface Consumer<K, M : Any> {
        fun start(from: StartOffset = StartOffset.Latest, until: EndOffset = EndOffset.Continuous)
        fun shutdown()
        val isDisposed: Boolean
        fun pause()
        fun resume()
        fun hasNoAssignments(): Boolean
        fun position(partition: Partition): Long
        fun lag(partition: Partition): Long
        fun onEvent(callback: (ConsumerEvent) -> Unit)
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

    class ConsumerBuilder<K, M : Any>(
        private val config: ConsumerConfig,
        private val keyMapper: (Key<ByteArray>) -> Key<K>,
        private val deserializer: Deserializer<M>,
        private val instance: String = shortId(),
    ) {
        fun each(
            maxConcurrency: Int = 1,
            handler: (Processable<K, M>) -> Unit,
        ): Consumer<K, M> = wrapProcessor(
            DefaultProcessor.each(keyMapper, deserializer, handler, maxConcurrency = maxConcurrency)
        )

        fun batch(
            size: Int = config.maxPollRecords,
            duration: Duration = Duration.INFINITE,
            maxConcurrency: Int = 1,
            handler: (List<Processable<K, M>>) -> Unit,
        ): Consumer<K, M> = wrapProcessor(
            DefaultProcessor.batch(keyMapper, deserializer, handler, size, duration, maxConcurrency = maxConcurrency)
        )

        fun <GK : Any> groupBy(extractor: (Processable<K, M>) -> GK): GroupedConsumerBuilder<GK, K, M> =
            GroupedConsumerBuilder(config, keyMapper, deserializer, extractor, instance)

        fun processor(custom: Processor<M>): Consumer<K, M> = wrapProcessor(custom)

        private fun wrapProcessor(processor: Processor<M>): Consumer<K, M> =
            wrap(config, processor, instance)
    }

    class GroupedConsumerBuilder<GK : Any, K, M : Any>(
        private val config: ConsumerConfig,
        private val keyMapper: (Key<ByteArray>) -> Key<K>,
        private val deserializer: Deserializer<M>,
        private val keyExtractor: (Processable<K, M>) -> GK,
        private val instance: String = shortId(),
    ) {
        fun each(
            concurrency: Int = 1,
            handler: (GK, Processable<K, M>) -> Unit,
        ): Consumer<K, M> = wrapProcessor(
            DefaultProcessor.groupedEach(keyMapper, deserializer, keyExtractor, handler, concurrency = concurrency)
        )

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

inline fun <reified T : ConsumerEvent> Prozess.Consumer<*, *>.onEvent(noinline callback: (T) -> Unit) =
    onEvent { if (it is T) callback(it) }
