package se.oyabun.prozess

import org.apache.kafka.clients.consumer.ConsumerConfig as ApacheConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import reactor.core.publisher.Flux
import reactor.core.publisher.Flux.fromIterable
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.fromCallable
import reactor.core.scheduler.Schedulers
import org.apache.kafka.common.errors.WakeupException
import kotlin.time.Duration
import kotlin.time.Instant
import kotlin.time.toJavaDuration

internal class ThreadsafeKafkaClient(config: ConsumerConfig) : KafkaClient {
    override val pollInterval: Duration = config.pollInterval
    private val delegate = KafkaConsumer<String, ByteArray>(
        mapOf(
            ApacheConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ApacheConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
            ApacheConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ) + config.toKafkaProperties(),
    )
    private val scheduler = Schedulers.newSingle("single-thread-kafka-client-" + delegate.hashCode())

    override fun partitionsFor(topics: Topics): Mono<Partitions> = fromIterable(topics)
        .flatMapIterable { delegate.partitionsFor(it) }
        .map { it.toPartition() }.collectSet().subscribeOn(scheduler)

    override fun beginningOffsets(partitions: Partitions): Mono<Positions> = fromIterable(partitions)
        .map { it.toApache() }.collectList().map { delegate.beginningOffsets(it) }
        .map { result -> result.map { it.toPosition() }.toSet() }.subscribeOn(scheduler)

    override fun endOffsets(partitions: Partitions): Mono<Positions> = fromIterable(partitions)
        .map { it.toApache() }.collectList().map { delegate.endOffsets(it) }
        .flatMapIterable { result -> result.mapNotNull { if (it.value > 0) it.toPosition() else null } }
        .collectSet().subscribeOn(scheduler)

    override fun subscribe(
        topics: Topics,
        listener: RebalanceListener,
    ): Mono<Topics> = fromCallable {
        delegate.subscribe(topics, object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
                listener.onPartitionsRevoked(
                    context = PollContext(),
                    partitions = partitions.map { it.toPartition() }.toSet(),
                )
            }
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
                listener.onPartitionsAssigned(
                    context = PollContext(),
                    partitions = partitions.map { it.toPartition() }.toSet(),
                )
            }
        })
        topics
    }.subscribeOn(scheduler)

    override fun offsetsForTimes(partitions: Partitions, timestamp: Instant): Mono<Positions> =
        fromCallable {
            delegate.offsetsForTimes(
                partitions.associate { it.toApache() to timestamp.toEpochMilliseconds() }
            )
        }.map { result ->
            result.mapNotNull { (tp, oat) ->
                oat?.let { Position(tp.toPartition(), it.offset()) }
            }.toSet()
        }.subscribeOn(scheduler)

    override fun positionOf(partition: Partition): Mono<Long> =
        fromCallable { delegate.position(partition.toApache()) }
            .subscribeOn(scheduler)

    override fun endOffsetOf(partition: Partition): Mono<Long> = Flux.just(partition.toApache())
        .collectList().map { delegate.endOffsets(it)[partition.toApache()]!! }
        .subscribeOn(scheduler)

    override fun poll(timeout: Duration): Mono<List<Received>> =
        fromCallable {
            try {
                delegate.poll(timeout.toJavaDuration())
                    .map { it.toReceived() }
            } catch (_: WakeupException) {
                emptyList()
            }
        }.subscribeOn(scheduler)

    override fun seek(offsets: Offsets) = fromCallable {
        offsets.forEach { (partition, target) ->
            delegate.seek(partition.toApache(), target)
        }
    }.then().subscribeOn(scheduler)

    inner class PollContext : RebalanceContext {
        override fun position(partition: Partition): Long = delegate.position(partition.toApache())

        override fun pause(partitions: Partitions) = delegate.pause(partitions.map { it.toApache() })

        override fun commit(offsets: Offsets) = delegate.commitSync(offsets.mapKeys { it.key.toApache() }.mapValues { OffsetAndMetadata(it.value, "") })

        override fun seek(targets: Offsets) = targets.forEach { (partition, offset) -> delegate.seek(partition.toApache(), offset) }

    }

    override fun pause(partitions: Partitions): Mono<Partitions> =
        fromCallable { delegate.pause(partitions.map { it.toApache() }); partitions }
            .subscribeOn(scheduler)

    override fun resume(partitions: Partitions): Mono<Partitions> =
        fromCallable { delegate.resume(partitions.map { it.toApache() }); partitions }
            .subscribeOn(scheduler)

    override fun commit(offsets: Offsets, metadata: String): Mono<Offsets> = fromCallable {
        val apache = offsets.mapKeys { it.key.toApache() }.mapValues { OffsetAndMetadata(it.value, metadata) }
        delegate.commitSync(apache)
    }.thenReturn(offsets).subscribeOn(scheduler)

    override fun committed(partitions: Partitions): Mono<Offsets> = fromCallable {
        delegate.committed(partitions.map { it.toApache() }.toSet())
            .entries
            .mapNotNull { (key, value) -> value?.let { key.toPartition() to it.offset() } }
            .toMap()
    }.subscribeOn(scheduler)

    override fun wakeup() = delegate.wakeup()

    override fun close(timeout: Duration): Mono<Void> = fromCallable {
        delegate.close(timeout.toJavaDuration())
        scheduler.dispose()
    }.then().subscribeOn(scheduler)

    private fun Partition.toApache() = TopicPartition(topic.name, id)

    private fun TopicPartition.toPartition() = Partition(partition(), Topic(topic()))

    private fun PartitionInfo.toPartition() = Partition(partition(), Topic(topic()))

    private fun ConsumerRecord<String, ByteArray>.toReceived() = Received(
        key()?.let { ReceivedKey.Value(it) } ?: ReceivedKey.None,
        value()?.let { ReceivedMessage.Data(it) } ?: ReceivedMessage.Tombstone,
        Position(Partition(partition(), Topic(topic())), offset()),
        headers().iterator().asSequence().map { Header(it.key(), it.value()) }.toList(),
    )

    private fun Map.Entry<TopicPartition, Long>.toPosition() =
        Position(Partition(key.partition(), Topic(key.topic())), value)

    fun <T : Any> Flux<T>.collectSet(): Mono<Set<T>> = collectList().map { it.toSet() }
}
