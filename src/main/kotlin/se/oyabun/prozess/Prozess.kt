package se.oyabun.prozess

import se.oyabun.prozess.reactor.ReactorKafkaConsumer
import se.oyabun.prozess.reactor.ReactorKafkaProducer

object Prozess {
    typealias KeyExtraction<M> = (M) -> String
    typealias Serializer<M> = (M) -> ByteArray
    typealias Deserializer<M> = (ByteArray) -> M
    typealias ConsumerFilter = (Received) -> Boolean
    typealias ConsumerProcess<M> = (Received, M) -> Unit

    interface Producer<M> {
        fun send(key: String, value: M, partition: Int? = null, timestamp: Long? = null): Long
        fun initTransactions()
        fun beginTransaction()
        fun commitTransaction()
        fun abortTransaction()
        fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember)
        fun close()
    }

    interface Consumer<M> {
        fun start(from: StartOffset = StartOffset.Latest, until: EndOffset = EndOffset.Continuous)
        fun shutdown()
        val isDisposed: Boolean
        fun pause()
        fun resume()
        fun hasNoAssignments(): Boolean
        fun position(partition: Partition): Long
        fun lag(partition: Partition): Long
    }

    fun <M> consumer(
        config: ConsumerConfig,
        filter: ConsumerFilter = { true },
        deserializer: Deserializer<M>,
        process: ConsumerProcess<M> = { _,_ -> },
        instance: String? = "consumer",
    ): Consumer<M> = object : Consumer<M> {
        private val delegate = ReactorKafkaConsumer(config, filter, deserializer, process, instance)
        override fun start(from: StartOffset, until: EndOffset) = delegate.start(from, until)
        override fun shutdown() = delegate.shutdown()
        override val isDisposed: Boolean get() = delegate.isDisposed
        override fun pause() = delegate.pause()
        override fun resume() = delegate.resume()
        override fun hasNoAssignments(): Boolean = delegate.hasNoAssignments()
        override fun position(partition: Partition): Long = delegate.position(partition)
        override fun lag(partition: Partition): Long = delegate.lag(partition)
    }

    fun <M : Any> producer(
        config: ProducerConfig,
        serializer: Serializer<M>,
        instance: String? = "producer",
    ): Producer<M> = object : Producer<M> {
        val delegate = ReactorKafkaProducer(config, instance, serializer)
        override fun send(key: String, value: M, partition: Int?, timestamp: Long?): Long { return delegate.send(key, value, partition, timestamp).blockOptional().orElseThrow() }
        override fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember) { delegate.sendOffsetsToTransaction(offsets, member).block() }
        override fun initTransactions() { delegate.initTransactions().block() }
        override fun beginTransaction() { delegate.beginTransaction().block() }
        override fun commitTransaction() { delegate.commitTransaction().block() }
        override fun abortTransaction() { delegate.abortTransaction().block() }
        override fun close() { delegate.close().block() }
    }
}
