package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.groupBy
import java.util.concurrent.atomic.AtomicBoolean

@Suppress("OPT_IN_USAGE")
class StreamingProducer<M : Any>(
    val config: ProducerConfig,
    instance: String? = "producer",
    private val serializer: Prozess.Serializer<M>,
) {
    private val log        = Logging.logger { }
    private val instanceId = "[$instance ${config.topic} producer]"
    private val delegate   = KafkaProducer<String, ByteArray>(config.toKafkaProperties())
    private val dispatcher = kotlinx.coroutines.newSingleThreadContext("$instanceId-producer")
    private val closed     = AtomicBoolean(false)

    fun send(
        key: String,
        value: M,
        partition: Int? = null,
        timestamp: Long? = null,
        headers: Headers = emptyList(),
    ): One<Long> = One.create<Long> { success, failure ->
        val kafkaHeaders = headers.map { RecordHeader(it.key, it.value) }
        delegate.send(
            org.apache.kafka.clients.producer.ProducerRecord(
                config.topic.name, partition, timestamp, key, serializer(value), kafkaHeaders,
            ),
        ) { metadata, exception ->
            if (exception != null) failure(SendFailure("$instanceId send failed", exception))
            else success(metadata.offset())
        }
    }.withRetries(instanceId, log)

    fun sendAll(
        source: Many<M>,
        key: Prozess.KeyExtraction<M>,
        headersProvider: Prozess.HeadersProvider<M> = { emptyList() },
    ): Many<M> = source.groupBy(
        keySelector  = { m: M -> key(m) },
        groupHandler = { _, group: Many<M> ->
            group.concatMap { element: M ->
                send(key(element), element, headers = headersProvider(element))
                    .flatMapMany { Many.of(element) }
            }
        },
    )

    fun initTransactions(): None<Unit>  = None.defer(dispatcher) { delegate.initTransactions() }
    fun beginTransaction(): None<Unit>  = None.defer(dispatcher) { delegate.beginTransaction() }
    fun commitTransaction(): None<Unit> = None.defer(dispatcher) { delegate.commitTransaction() }
    fun abortTransaction(): None<Unit>  = None.defer(dispatcher) { delegate.abortTransaction() }

    fun sendOffsetsToTransaction(offsets: Offsets, member: GroupMember): None<Unit> = None.defer(dispatcher) {
        delegate.sendOffsetsToTransaction(
            offsets.entries.associate { (partition, offset) ->
                TopicPartition(partition.topic.name, partition.id) to OffsetAndMetadata(offset, "")
            },
            org.apache.kafka.clients.consumer.ConsumerGroupMetadata(
                member.groupId, member.generationId, member.memberId,
                java.util.Optional.ofNullable(member.groupInstanceId),
            ),
        )
    }

    fun close(): None<Unit> = if (!closed.compareAndSet(false, true)) None.complete() else None.defer(dispatcher) {
        delegate.close(java.time.Duration.ofSeconds(3))
        dispatcher.close()
    }
}
