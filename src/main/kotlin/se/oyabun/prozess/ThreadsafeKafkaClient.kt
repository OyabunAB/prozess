package se.oyabun.prozess

import kotlinx.coroutines.newSingleThreadContext
import org.apache.kafka.clients.consumer.ConsumerConfig as ApacheConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.errors.WakeupException
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import kotlin.time.Duration
import kotlin.time.Instant
import kotlin.time.toJavaDuration

@Suppress("OPT_IN_USAGE")
internal class ThreadsafeKafkaClient(config: ConsumerConfig) : KafkaClient {

    override val pollInterval: Duration = config.pollInterval

    private val delegate = KafkaConsumer<ByteArray?, ByteArray>(
        mapOf(
            ApacheConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   to ByteArrayDeserializer::class.java.name,
            ApacheConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
            ApacheConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       to false,
        ) + config.toKafkaProperties(),
    )

    private val dispatcher = newSingleThreadContext("kafka-client-${delegate.hashCode()}")

    override fun partitionsFor(topics: Topics): One<Partitions> = One.defer(dispatcher) {
        topics.flatMap { delegate.partitionsFor(it) }.map { it.toPartition() }.toSet()
    }

    override fun beginningOffsets(partitions: Partitions): One<Positions> = One.defer(dispatcher) {
        delegate.beginningOffsets(partitions.map { it.toApache() }).map { it.toPosition() }.toSet()
    }

    override fun endOffsets(partitions: Partitions): One<Positions> = One.defer(dispatcher) {
        delegate.endOffsets(partitions.map { it.toApache() })
            .mapNotNull { if (it.value > 0) it.toPosition() else null }.toSet()
    }

    override fun subscribe(topics: Topics, listener: RebalanceListener): One<Topics> = One.defer(dispatcher) {
        delegate.subscribe(topics, object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
                listener.onPartitionsRevoked(PollContext(), partitions.map { it.toPartition() }.toSet())
            }
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
                listener.onPartitionsAssigned(PollContext(), partitions.map { it.toPartition() }.toSet())
            }
        })
        topics
    }

    override fun offsetsForTimes(partitions: Partitions, timestamp: Instant): One<Positions> = One.defer(dispatcher) {
        delegate.offsetsForTimes(partitions.associate { it.toApache() to timestamp.toEpochMilliseconds() })
            .mapNotNull { (tp, oat) -> oat?.let { Position(tp.toPartition(), it.offset()) } }
            .toSet()
    }

    override fun positionOf(partition: Partition): One<Long> =
        One.defer(dispatcher) { delegate.position(partition.toApache()) }

    override fun endOffsetOf(partition: Partition): One<Long> =
        One.defer(dispatcher) {
            delegate.endOffsets(listOf(partition.toApache()))[partition.toApache()]
                ?: throw IllegalStateException("No end offset returned for $partition")
        }

    override fun poll(timeout: Duration): One<List<Received<ByteArray>>> = One.defer(dispatcher) {
        try {
            delegate.poll(timeout.toJavaDuration()).map { it.toReceived() }
        } catch (_: WakeupException) {
            emptyList()
        }
    }

    override fun seek(offsets: Offsets): None<Offsets> = None.defer(dispatcher) {
        offsets.forEach { (partition, target) -> delegate.seek(partition.toApache(), target) }
    }

    override fun pause(partitions: Partitions): One<Partitions> =
        One.defer(dispatcher) { delegate.pause(partitions.map { it.toApache() }); partitions }

    override fun resume(partitions: Partitions): One<Partitions> =
        One.defer(dispatcher) { delegate.resume(partitions.map { it.toApache() }); partitions }

    override fun commit(offsets: Offsets, metadata: String): One<Offsets> = One.defer(dispatcher) {
        val apache = offsets.mapKeys { it.key.toApache() }.mapValues { OffsetAndMetadata(it.value, metadata) }
        delegate.commitSync(apache)
        offsets
    }

    override fun committed(partitions: Partitions): One<Offsets> = One.defer(dispatcher) {
        delegate.committed(partitions.map { it.toApache() }.toSet())
            .entries.mapNotNull { (key, value) -> value?.let { key.toPartition() to it.offset() } }
            .toMap()
    }

    override fun wakeup() = delegate.wakeup()

    override fun close(timeout: Duration): None<Unit> = None.defer(dispatcher) {
        delegate.close(timeout.toJavaDuration())
        dispatcher.close()
    }

    inner class PollContext : RebalanceContext {
        override fun position(partition: Partition): Long = delegate.position(partition.toApache())
        override fun pause(partitions: Partitions)        = delegate.pause(partitions.map { it.toApache() })
        override fun commit(offsets: Offsets)             = delegate.commitSync(offsets.mapKeys { it.key.toApache() }.mapValues { OffsetAndMetadata(it.value, "") })
        override fun seek(targets: Offsets)               = targets.forEach { (partition, offset) -> delegate.seek(partition.toApache(), offset) }
    }

    private fun Partition.toApache()                                   = TopicPartition(topic.name, id)
    private fun TopicPartition.toPartition()                           = Partition(partition(), Topic(topic()))
    private fun org.apache.kafka.common.PartitionInfo.toPartition()   = Partition(partition(), Topic(topic()))
    private fun Map.Entry<TopicPartition, Long>.toPosition()          = Position(Partition(key.partition(), Topic(key.topic())), value)
    private fun ConsumerRecord<ByteArray?, ByteArray>.toReceived() = Received(
        key()?.let { Key.Present(it) } ?: Key.Missing,
        value()?.let { ReceivedMessage.Data(it) } ?: ReceivedMessage.Tombstone,
        Position(Partition(partition(), Topic(topic())), offset()),
        headers().iterator().asSequence().map { Header(it.key(), it.value()) }.toList(),
    )
}
