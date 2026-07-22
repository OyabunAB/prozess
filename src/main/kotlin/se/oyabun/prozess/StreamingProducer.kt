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

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.groupBy
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Reactive Kafka producer.
 *
 * Wraps [KafkaProducer] on a single-thread dispatcher to ensure thread safety.
 * [send] returns a [One] that resolves to the written offset. [sendAll] preserves
 * per-key ordering by grouping messages and sending each key group sequentially.
 *
 * Use [Prozess.producer] to obtain a synchronous [Prozess.Producer] facade rather
 * than instantiating this class directly.
 *
 * @param config             producer configuration.
 * @param instance           label for logging and thread naming.
 * @param keyExtractor       extracts the grouping key for [sendAll]; null for unkeyed producers.
 * @param keyBytes           serializes the extracted key to bytes; null for unkeyed producers.
 * @param headerEnricher     produces headers for each record.
 * @param partitionExtractor selects the target partition; [Prozess.NO_PARTITION] uses default partitioner.
 * @param timestampExtractor produces the record timestamp; [Prozess.NO_TIMESTAMP] uses broker time.
 * @param serializer         serializes messages to bytes.
 */
@Suppress("OPT_IN_USAGE")
class StreamingProducer<M : Any>(
    val config: ProducerConfig,
    instance: String = shortId(),
    private val keyExtractor: ((M) -> Any)?,
    private val keyBytes: ((M) -> ByteArray)?,
    private val headerEnricher: Prozess.HeaderEnricher<M> = { emptyList() },
    private val partitionExtractor: Prozess.PartitionExtractor<M> = { Prozess.NO_PARTITION },
    private val timestampExtractor: Prozess.TimestampExtractor<M> = { Prozess.NO_TIMESTAMP },
    private val serializer: Prozess.Serializer<M>,
) {
    private val log        = Logging.logger { }
    private val instanceId = "[$instance ${config.topic.name} producer]"
    private val delegate   = KafkaProducer<ByteArray?, ByteArray>(config.toKafkaProperties())
    private val dispatcher = kotlinx.coroutines.newSingleThreadContext("$instanceId-producer")
    private val closed     = AtomicBoolean(false)

    /**
     * Sends a single [value] to Kafka.
     *
     * Returns a [One] that resolves to the written offset on success, or fails
     * with [SendFailure] on error. Retries with exponential backoff on transient failures.
     */
    fun send(value: M): One<Long> = One.create<Long> { success, failure ->
        val key: ByteArray?  = keyBytes?.invoke(value)
        val partition: Int?  = partitionExtractor(value).takeUnless { it == Prozess.NO_PARTITION }
        val timestamp: Long? = timestampExtractor(value).takeUnless { it == Prozess.NO_TIMESTAMP }
        val kafkaHeaders     = headerEnricher(value).map { RecordHeader(it.key, it.value) }
        delegate.send(
            org.apache.kafka.clients.producer.ProducerRecord(
                config.topic.name, partition, timestamp, key, serializer(value), kafkaHeaders,
            ),
        ) { metadata, exception ->
            if (exception != null) failure(SendFailure("$instanceId send failed", exception))
            else success(metadata.offset())
        }
    }.withRetries()

    /**
     * Sends all records in [source] to Kafka.
     *
     * When a [keyExtractor] is configured, messages are grouped by key and each key group
     * is sent sequentially — guaranteeing per-key ordering. Without a key extractor, all
     * messages are sent sequentially. Returns the source messages as pass-through.
     */
    fun sendAll(source: Many<M>): Many<M> = if (keyExtractor != null) {
        source.groupBy(
            keySelector  = { m: M -> keyExtractor(m) },
            groupHandler = { _, group: Many<M> ->
                group.concatMap { element: M ->
                    send(element).flatMapMany { Many.items(element) }
                }
            },
        )
    } else {
        source.flatMap { element: M ->
            send(element).flatMapMany { Many.items(element) }
        }
    }

    /** Initialises the producer for transactional use. Must be called once before [beginTransaction]. */
    fun initTransactions(): None<Unit>  = None.defer(dispatcher) { delegate.initTransactions() }
    /** Begins a Kafka transaction. */
    fun beginTransaction(): None<Unit>  = None.defer(dispatcher) { delegate.beginTransaction() }
    /** Commits the current Kafka transaction. */
    fun commitTransaction(): None<Unit> = None.defer(dispatcher) { delegate.commitTransaction() }
    /** Aborts the current Kafka transaction. */
    fun abortTransaction(): None<Unit>  = None.defer(dispatcher) { delegate.abortTransaction() }

    /**
     * Sends consumer [offsets] to the current transaction, associating them with [member].
     *
     * Used to implement read-process-write exactly-once semantics in conjunction with
     * a transactional consumer.
     */
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

    /**
     * Flushes and closes the underlying Kafka producer.
     *
     * Waits up to 3 seconds for in-flight sends to complete. Idempotent — subsequent
     * calls return immediately.
     */
    fun close(): None<Unit> = if (!closed.compareAndSet(false, true)) None.complete() else None.defer(dispatcher) {
        delegate.close(java.time.Duration.ofSeconds(3))
        dispatcher.close()
    }
}
