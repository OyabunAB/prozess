package se.oyabun.prozess

import kotlin.time.Duration

object Prozess {
    typealias KeyExtraction<M> = (M) -> String
    typealias Serializer<M> = (M) -> ByteArray
    typealias HeadersProvider<M> = (M) -> Headers

    sealed interface DeserializationResult<out M> {
        data class Message<M>(val value: M) : DeserializationResult<M>
        data object Tombstone : DeserializationResult<Nothing>
        data class PoisonPill(val reason: String? = null) : DeserializationResult<Nothing>
    }

    typealias Deserializer<M> = (Received) -> DeserializationResult<M>

    interface Producer<M> {
        fun send(key: String, value: M, partition: Int? = null, timestamp: Long? = null, headers: Headers = emptyList()): Long
        fun initTransactions()
        fun beginTransaction()
        fun commitTransaction()
        fun abortTransaction()
        fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember)
        fun close()
    }

    interface Consumer<M : Any> {
        fun start(from: StartOffset = StartOffset.Latest, until: EndOffset = EndOffset.Continuous)
        fun shutdown()
        val isDisposed: Boolean
        fun pause()
        fun resume()
        fun hasNoAssignments(): Boolean
        fun position(partition: Partition): Long
        fun lag(partition: Partition): Long
        fun onEvent(callback: (ConsumerEvent) -> Unit)
    }

    private fun <M : Any> simpleDeserializer(deserializeBytes: (ByteArray) -> M): Deserializer<M> = { received ->
        when (val message = received.message) {
            is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
            is ReceivedMessage.Data -> DeserializationResult.Message(deserializeBytes(message.bytes))
        }
    }

    fun <M : Any> consumer(
        config: ConsumerConfig,
        deserializeBytes: (ByteArray) -> M,
        process: (Received, M) -> Unit,
        instance: String? = "consumer",
    ): Consumer<M> = ConsumerBuilder(config, simpleDeserializer(deserializeBytes), instance)
        .each { p -> process(p.received, p.value) }

    fun <M : Any> consumer(
        config: ConsumerConfig,
        deserializer: Deserializer<M>,
    ): ConsumerBuilder<M> = ConsumerBuilder(config, deserializer)

    private fun <M : Any> wrap(
        config: ConsumerConfig,
        processor: Processor<M>,
        instance: String?,
    ): Consumer<M> = object : Consumer<M> {
        private val delegate = StreamingConsumer(config, processor, instance)
        override fun start(from: StartOffset, until: EndOffset) = delegate.start(from, until)
        override fun shutdown() = delegate.shutdown()
        override val isDisposed: Boolean get() = delegate.isDisposed
        override fun pause() = delegate.pause()
        override fun resume() = delegate.resume()
        override fun hasNoAssignments(): Boolean = delegate.hasNoAssignments()
        override fun position(partition: Partition): Long = delegate.position(partition)
        override fun lag(partition: Partition): Long = delegate.lag(partition)
        override fun onEvent(callback: (ConsumerEvent) -> Unit) = delegate.onEvent(callback)
    }

    fun <M : Any> producer(
        config: ProducerConfig,
        serializer: Serializer<M>,
        instance: String? = "producer",
    ): Producer<M> = object : Producer<M> {
        val delegate = StreamingProducer(config, instance, serializer)
        override fun send(key: String, value: M, partition: Int?, timestamp: Long?, headers: Headers): Long { return delegate.send(key, value, partition, timestamp, headers).blockOptional().orElseThrow() }
        override fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember) { delegate.sendOffsetsToTransaction(offsets, member).block() }
        override fun initTransactions() { delegate.initTransactions().block() }
        override fun beginTransaction() { delegate.beginTransaction().block() }
        override fun commitTransaction() { delegate.commitTransaction().block() }
        override fun abortTransaction() { delegate.abortTransaction().block() }
        override fun close() { delegate.close().block() }
    }

    /** Builds a [Consumer] by configuring the processing pipeline. */
    class ConsumerBuilder<M : Any>(
        private val config: ConsumerConfig,
        private val deserializer: Deserializer<M>,
        private val instance: String? = "consumer",
    ) {
        fun each(
            maxConcurrency: Int = 1,
            handler: (Processable<M>) -> Unit,
        ): Consumer<M> = wrapProcessor(
            DefaultProcessor.each(
                deserializer = deserializer,
                handler = handler,
                maxConcurrency = maxConcurrency,
            )
        )

        /** Fetches messages into atomic batches of [size] or [duration] timeout, then processes each batch. */
        fun batch(
            size: Int = config.maxPollRecords,
            duration: Duration = Duration.INFINITE,
            maxConcurrency: Int = 1,
            handler: (List<Processable<M>>) -> Unit,
        ): Consumer<M> = wrapProcessor(
            DefaultProcessor.batch(
                deserializer = deserializer,
                handler = handler,
                batchSize = size,
                batchDuration = duration,
                maxConcurrency = maxConcurrency,
            )
        )

        /** Groups messages by an extracted key, enabling per-key ordered processing. */
        fun <TKey : Any> groupBy(extractor: (Processable<M>) -> TKey): GroupedConsumerBuilder<TKey, M> =
            GroupedConsumerBuilder(config, deserializer, extractor, instance)

        /** Uses a custom [Processor] implementation instead of the built-in strategies. */
        fun processor(custom: Processor<M>): Consumer<M> = wrapProcessor(custom)

        private fun wrapProcessor(processor: Processor<M>): Consumer<M> =
            wrap(config, processor, instance)
    }

    /** Builds a [Consumer] that groups messages by a key for per-key ordered processing. */
    class GroupedConsumerBuilder<K : Any, M : Any>(
        private val config: ConsumerConfig,
        private val deserializer: Deserializer<M>,
        private val keyExtractor: (Processable<M>) -> K,
        private val instance: String? = "consumer",
    ) {
        /** Processes each message individually, grouped by key. Per-key ordering is preserved. */
        fun each(
            concurrency: Int = 1,
            handler: (K, Processable<M>) -> Unit,
        ): Consumer<M> = wrapProcessor(
            DefaultProcessor.groupedEach(
                deserializer = deserializer,
                keyExtractor = keyExtractor,
                handler = handler,
                concurrency = concurrency,
            )
        )

        /**
         * Buffers messages into atomic batches per key group of [size] or [duration] timeout,
         * then processes each batch as a unit. Per-key ordering is preserved.
         */
        fun batch(
            size: Int = config.maxPollRecords,
            duration: Duration = Duration.INFINITE,
            concurrency: Int = 1,
            handler: (K, List<Processable<M>>) -> Unit,
        ): Consumer<M> = wrapProcessor(
            DefaultProcessor.groupedBatch(
                deserializer = deserializer,
                keyExtractor = keyExtractor,
                handler = handler,
                batchSize = size,
                batchDuration = duration,
                concurrency = concurrency,
            )
        )

        private fun wrapProcessor(processor: Processor<M>): Consumer<M> =
            wrap(config, processor, instance)
    }
}
