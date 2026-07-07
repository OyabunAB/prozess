package se.oyabun.prozess

import se.oyabun.prozess.GroupMember
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Offsets
import se.oyabun.prozess.ProducerConfig
import se.oyabun.prozess.Prozess.HeadersProvider
import se.oyabun.prozess.Prozess.KeyExtraction
import se.oyabun.prozess.Prozess.Serializer
import se.oyabun.prozess.Retrying.anyException
import se.oyabun.prozess.Retrying.infiniteRetries
import se.oyabun.prozess.Retrying.withRetries
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

class StreamingProducer<M : Any>(
    val config: ProducerConfig,
    instance: String? = "producer",
    private val serializer: Serializer<M>,
) {
    private val log = Logging.logger { }
    private val instanceId = "[$instance ${config.topic} producer]"
    private val delegate = KafkaProducer<String, ByteArray>(config.toKafkaProperties())

    fun send(key: String, value: M, partition: Int? = null, timestamp: Long? = null, headers: Headers = emptyList()): Mono<Long> =
        Mono.create { sink ->
            val kafkaHeaders = headers.map { RecordHeader(it.key, it.value) }
            delegate.send(
                org.apache.kafka.clients.producer.ProducerRecord(config.topic.name, partition, timestamp, key, serializer(value), kafkaHeaders),
            ) { metadata, exception ->
                if (exception != null) sink.error(SendFailure("$instanceId send failed", exception))
                else sink.success(metadata.offset())
            }
        }.withRetries(id = instanceId, retryOn = anyException, maxAttempts = infiniteRetries)

    fun sendAll(
        source: Flux<M>,
        key: KeyExtraction<M>,
        headersProvider: HeadersProvider<M> = { emptyList() },
    ): Flux<M> = source
        .groupBy { key(it) }
        .flatMap { group -> group.concatMap { element -> send(group.key(), element, headers = headersProvider(element)).thenReturn(element) } }

    fun initTransactions(): Mono<Void> = Mono.fromCallable { delegate.initTransactions() }.then()
        .subscribeOn(Schedulers.boundedElastic())

    fun beginTransaction(): Mono<Void> = Mono.fromCallable { delegate.beginTransaction() }.then()
        .subscribeOn(Schedulers.boundedElastic())

    fun commitTransaction(): Mono<Void> = Mono.fromCallable { delegate.commitTransaction() }.then()
        .subscribeOn(Schedulers.boundedElastic())

    fun abortTransaction(): Mono<Void> = Mono.fromCallable { delegate.abortTransaction() }.then()
        .subscribeOn(Schedulers.boundedElastic())

    fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember): Mono<Void> = Mono.fromCallable {
        delegate.sendOffsetsToTransaction(
            offsets.entries.associate { (partition, offset) ->
                TopicPartition(partition.topic.name, partition.id) to OffsetAndMetadata(offset, "")
            },
            org.apache.kafka.clients.consumer.ConsumerGroupMetadata(
                member.groupId,
                member.generationId,
                member.memberId,
                java.util.Optional.ofNullable(member.groupInstanceId),
            ),
        )
    }.then().subscribeOn(Schedulers.boundedElastic())

    fun close(): Mono<Void> = Mono.fromRunnable { delegate.close(java.time.Duration.ofSeconds(3)) }
}
