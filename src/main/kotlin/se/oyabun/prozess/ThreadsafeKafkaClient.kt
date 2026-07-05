package se.oyabun.prozess

import se.oyabun.prozess.ConsumerConfig
import se.oyabun.prozess.Offsets
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Partitions
import se.oyabun.prozess.Position
import se.oyabun.prozess.Positions
import se.oyabun.prozess.Received
import se.oyabun.prozess.Topic
import se.oyabun.prozess.Topics
import se.oyabun.prozess.toKafkaProperties
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
import se.oyabun.prozess.RebalanceContext
import org.apache.kafka.common.errors.WakeupException
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
internal class ThreadsafeKafkaClient(config: ConsumerConfig) : ShutdownableClient {
    private val delegate = KafkaConsumer<String, ByteArray>(
        mapOf(
            ApacheConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ApacheConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
            ApacheConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ApacheConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ) + config.toKafkaProperties(),
    )
    private val scheduler = Schedulers.newSingle("single-thread-kafka-client-" + delegate.hashCode())

    fun partitionsFor(topics: Topics): Mono<Partitions> = fromIterable(topics)
        .flatMapIterable { delegate.partitionsFor(it) }
        .map { it.toPartition() }.collectSet().subscribeOn(scheduler)

    fun beginningOffsets(partitions: Partitions): Mono<Positions> = fromIterable(partitions)
        .map { it.toApache() }.collectList().map { delegate.beginningOffsets(it) }
        .map { result -> result.map { it.toPosition() }.toSet() }.subscribeOn(scheduler)

    fun endOffsets(partitions: Partitions): Mono<Positions> = fromIterable(partitions)
        .map { it.toApache() }.collectList().map { delegate.endOffsets(it) }
        .flatMapIterable { result -> result.mapNotNull { if (it.value > 0) it.toPosition() else null } }
        .collectSet().subscribeOn(scheduler)

    fun rebalanceContext(): RebalanceContext = PollContext(delegate)

    fun subscribe(
        topics: Topics,
        listener: ConsumerRebalanceListener,
    ): Mono<Topics> = fromCallable {
        delegate.subscribe(topics, listener)
        topics
    }.subscribeOn(scheduler)

    fun offsetsForTimes(partitions: Partitions, timestamp: Instant): Mono<Positions> =
        fromCallable {
            delegate.offsetsForTimes(
                partitions.associate { it.toApache() to timestamp.toEpochMilli() }
            )
        }.map { result ->
            result.mapNotNull { (tp, oat) ->
                oat?.let { Position(tp.toPartition(), it.offset()) }
            }.toSet()
        }.subscribeOn(scheduler)

    fun positionOf(partition: Partition): Mono<Long> =
        fromCallable { delegate.position(partition.toApache()) }
            .subscribeOn(scheduler)

    fun endOffsetOf(partition: Partition): Mono<Long> = Flux.just(partition.toApache())
        .collectList().map { delegate.endOffsets(it)[partition.toApache()]!! }
        .subscribeOn(scheduler)

    fun poll(timeout: Duration = 100.milliseconds): Mono<List<Received>> =
        fromCallable {
            try {
                delegate.poll(timeout.toJavaDuration())
                    .map { it.toReceived() }
            } catch (_: WakeupException) {
                emptyList()
            }
        }.subscribeOn(scheduler)

    inner class PollContext(val delegate: KafkaConsumer<*,*>) : RebalanceContext {
        override fun position(partition: Partition): Long = delegate.position(partition.toApache())
        override fun pause(partitions: Partitions) = delegate.pause(partitions.map { it.toApache() })
        fun resume(partitions: Partitions) = delegate.resume(partitions.map { it.toApache() })
        override fun commit(offsets: Offsets) = offsets.mapKeys { it.key.toApache() }
            .mapValues { OffsetAndMetadata(it.value, "") }
            .let { delegate.commitSync(it) }
        override fun seek(targets: Offsets) = targets.forEach { (partition, target) ->
            delegate.seek(partition.toApache(), target)
        }
        fun toPartitions(partitions: Collection<TopicPartition>): Partitions =
            partitions.map { it.toPartition() }.toSet()
    }

    fun pause(partitions: Partitions): Mono<Partitions> =
        fromCallable { delegate.pause(partitions.map { it.toApache() }); partitions }
            .subscribeOn(scheduler)

    fun resume(partitions: Partitions): Mono<Partitions> =
        fromCallable { delegate.resume(partitions.map { it.toApache() }); partitions }
            .subscribeOn(scheduler)

    fun commit(offsets: Offsets, metadata: String): Mono<Offsets> = fromCallable {
        val apache = offsets.mapKeys { it.key.toApache() }
            .mapValues { org.apache.kafka.clients.consumer.OffsetAndMetadata(it.value, metadata) }
        delegate.commitSync(apache)
        offsets
    }.subscribeOn(scheduler)

    fun committed(partitions: Partitions): Mono<Offsets> =
        fromCallable {
            delegate.committed(partitions.map { it.toApache() }.toSet())
                .mapKeys { it.key.toPartition() }
                .mapValues { it.value?.offset() ?: 0L }
        }.subscribeOn(scheduler)

    override fun wakeup() = delegate.wakeup()

    override fun close(): Mono<Void> = fromCallable {
        delegate.close(3.seconds.toJavaDuration())
        scheduler.dispose()
    }.then()
        .subscribeOn(scheduler)

    private fun Partition.toApache() = org.apache.kafka.common.TopicPartition(topic.name, id)


    private fun TopicPartition.toPartition() = Partition(partition(), Topic(topic()))

    private fun PartitionInfo.toPartition() = Partition(partition(), Topic(topic()))

    private fun ConsumerRecord<String, ByteArray>.toReceived() = Received(
        key() ?: "",
        value() ?: ByteArray(0),
        Position(Partition(partition(), Topic(topic())), offset()),
    )

    private fun Map.Entry<TopicPartition, Long>.toPosition() =
        Position(Partition(key.partition(), Topic(key.topic())), value)

    fun <T : Any> Flux<T>.collectSet(): Mono<Set<T>> = collectList().map { it.toSet() }
}
