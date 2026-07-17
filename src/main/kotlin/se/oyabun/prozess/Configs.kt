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

import org.apache.kafka.clients.consumer.ConsumerConfig as ApacheConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig as ApacheProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Controls whether the producer participates in Kafka transactions.
 *
 * Use [Disabled] for non-transactional producers (default). Use [Enabled]
 * to activate exactly-once semantics by assigning a stable [Enabled.id].
 * When enabled, acks are automatically forced to `all`.
 */
sealed interface TransactionalConfig {
    /** Non-transactional producer. [acks] controls durability. */
    data class Disabled(val acks: ProducerConfig.Acks = ProducerConfig.Acks.Leader) : TransactionalConfig

    /**
     * Transactional producer with the given stable [id].
     *
     * The [id] must be unique across all active producer instances in the
     * same application to avoid fencing.
     */
    data class Enabled(val id: String) : TransactionalConfig {
        init {
            require(id.isNotBlank()) { "transactional.id must not be blank" }
        }
    }
}

/**
 * Typed configuration for a Kafka producer.
 *
 * All tuning knobs map directly to their Kafka client property equivalents.
 * The internal [toKafkaProperties] method translates this to the raw map
 * Apache's [org.apache.kafka.clients.producer.KafkaProducer] expects.
 *
 * @param bootstrapServers comma-separated `host:port` list.
 * @param topic            the default target topic for [Prozess.Producer.send].
 * @param compression      codec applied to record batches before transmission.
 * @param linger           artificial delay to allow batching; `ZERO` means send immediately.
 * @param batchSize        maximum bytes per record batch.
 * @param maxInFlight      maximum in-flight requests per connection.
 * @param bufferMemory     total bytes of memory the producer may use for buffering.
 * @param requestTimeout   timeout for broker acknowledgement.
 * @param transactional    transaction mode — [TransactionalConfig.Disabled] by default.
 * @param security         security protocol; [SecurityProtocol.Plaintext] by default.
 */
data class ProducerConfig(
    val bootstrapServers: String,
    val topic: Topic,
    val compression: ProducerConfig.Compression = ProducerConfig.Compression.None,
    val linger: Duration = Duration.ZERO,
    val batchSize: Int = 16384,
    val maxInFlight: Int = 5,
    val bufferMemory: Long = 32_000_000,
    val requestTimeout: Duration = 30.seconds,
    val transactional: TransactionalConfig = TransactionalConfig.Disabled(),
    val security: SecurityProtocol = SecurityProtocol.Plaintext,
) {
    /** Durability guarantee for produced records. */
    enum class Acks(val value: String) {
        /** Fire-and-forget — no acknowledgement required. */
        None("0"),
        /** Leader acknowledgement only. */
        Leader("1"),
        /** All in-sync replicas must acknowledge. Required for transactions. */
        All("all"),
    }

    /** Compression codec applied to record batches. */
    enum class Compression(val value: String) {
        None("none"), Gzip("gzip"), Snappy("snappy"), Lz4("lz4"), Zstd("zstd")
    }

    internal fun toKafkaProperties(): Map<String, Any> = buildMap {
        put(ApacheProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ApacheProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
        put(ApacheProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
        when (transactional) {
            is TransactionalConfig.Disabled -> put(ApacheProducerConfig.ACKS_CONFIG, transactional.acks.value)
            is TransactionalConfig.Enabled -> {
                put(ApacheProducerConfig.ACKS_CONFIG, Acks.All.value)
                put(ApacheProducerConfig.TRANSACTIONAL_ID_CONFIG, transactional.id)
            }
        }
        put(ApacheProducerConfig.COMPRESSION_TYPE_CONFIG, compression.value)
        put(ApacheProducerConfig.LINGER_MS_CONFIG, linger.inWholeMilliseconds.toInt())
        put(ApacheProducerConfig.BATCH_SIZE_CONFIG, batchSize)
        put(ApacheProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight)
        put(ApacheProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory)
        put(ApacheProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.inWholeMilliseconds.toInt())
        putAll(security.toProperties())
    }
}

/**
 * Typed configuration for a Kafka consumer.
 *
 * All tuning knobs map directly to their Kafka client property equivalents.
 * The internal [toKafkaProperties] extension translates this to the raw map
 * Apache's [org.apache.kafka.clients.consumer.KafkaConsumer] expects.
 *
 * @param bootstrapServers   comma-separated `host:port` list.
 * @param groupId            consumer group identifier.
 * @param topics             set of topic names to subscribe to.
 * @param clientId           optional client identifier shown in Kafka metrics.
 * @param groupInstanceId    optional static member id for cooperative rebalancing.
 * @param startOffset        default offset reset strategy; overridden per [Prozess.Consumer.start] call.
 * @param pollInterval       time between [KafkaClient.poll] calls.
 * @param sessionTimeout     Kafka broker session timeout.
 * @param heartbeatInterval  interval between heartbeats to the group coordinator.
 * @param maxPollInterval    maximum time between polls before the consumer is considered dead.
 * @param maxPollRecords     maximum records returned per poll; also sets buffer high-water mark.
 * @param fetchMinBytes      minimum bytes the broker should accumulate before responding.
 * @param fetchMaxBytes      maximum bytes the broker may return per fetch.
 * @param fetchMaxWait       maximum time the broker waits before returning a fetch response.
 * @param maxPartitionFetchBytes maximum bytes fetched per partition per request.
 * @param isolationLevel     read-uncommitted or read-committed (for transactional producers).
 * @param security           security protocol; [SecurityProtocol.Plaintext] by default.
 * @param commitBatchSize    maximum positions per commit batch.
 * @param commitBatchTimeout maximum time before a partial batch is flushed.
 */
data class ConsumerConfig(
    val bootstrapServers: String,
    val groupId: String,
    val topics: Topics,
    val clientId: String? = null,
    val groupInstanceId: String? = null,
    val startOffset: StartOffset = StartOffset.Latest,
    val pollInterval: Duration = 100.milliseconds,
    val sessionTimeout: Duration = 45.seconds,
    val heartbeatInterval: Duration = 3.seconds,
    val maxPollInterval: Duration = 5.minutes,
    val maxPollRecords: Int = 500,
    val fetchMinBytes: Int = 1,
    val fetchMaxBytes: Int = 50_000_000,
    val fetchMaxWait: Duration = 500.milliseconds,
    val maxPartitionFetchBytes: Int = 10_000_000,
    val isolationLevel: ConsumerConfig.IsolationLevel = ConsumerConfig.IsolationLevel.ReadUncommitted,
    val security: SecurityProtocol = SecurityProtocol.Plaintext,
    val commitBatchSize: Int = 25,
    val commitBatchTimeout: Duration = 1.seconds,
) {
    /** Convenience constructor that accepts topic names as a vararg. */
    constructor(bootstrapServers: String, groupId: String, vararg topics: String) : this(
        bootstrapServers = bootstrapServers,
        groupId = groupId,
        topics = setOf(*topics),
    )

    /** Controls visibility of transactional messages. */
    enum class IsolationLevel(val value: String) {
        /** Default — reads all records, including uncommitted transactional ones. */
        ReadUncommitted("read_uncommitted"),
        /** Only reads records from committed transactions. */
        ReadCommitted("read_committed"),
    }
}

internal fun ConsumerConfig.toKafkaProperties(): Map<String, Any> = buildMap {
    put(ApacheConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    put(ApacheConsumerConfig.GROUP_ID_CONFIG, groupId)
    clientId?.let { put(ApacheConsumerConfig.CLIENT_ID_CONFIG, it) }
    groupInstanceId?.let { put(ApacheConsumerConfig.GROUP_INSTANCE_ID_CONFIG, it) }
    put(ApacheConsumerConfig.AUTO_OFFSET_RESET_CONFIG, startOffset.autoOffsetReset())
    put(ApacheConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout.inWholeMilliseconds.toInt())
    put(ApacheConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval.inWholeMilliseconds.toInt())
    put(ApacheConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval.inWholeMilliseconds.toInt())
    put(ApacheConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
    put(ApacheConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes)
    put(ApacheConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes)
    put(ApacheConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait.inWholeMilliseconds.toInt())
    put(ApacheConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes)
    put(ApacheConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel.value)
    putAll(security.toProperties())
}

private fun StartOffset.autoOffsetReset(): String = when (this) {
    is StartOffset.Earliest -> "earliest"
    is StartOffset.Latest -> "latest"
    is StartOffset.AtTimestamp -> "none"
}
