package se.oyabun.prozess

import org.apache.kafka.clients.consumer.ConsumerConfig as ApacheConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig as ApacheProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

data class ProducerConfig(
    val bootstrapServers: String,
    val topic: Topic,
    val acks: ProducerConfig.Acks = ProducerConfig.Acks.Leader,
    val compression: ProducerConfig.Compression = ProducerConfig.Compression.None,
    val linger: Duration = Duration.ZERO,
    val batchSize: Int = 16384,
    val maxInFlight: Int = 5,
    val bufferMemory: Long = 32_000_000,
    val requestTimeout: Duration = 30.seconds,
    val enableIdempotence: Boolean = false,
) {
    enum class Acks(val value: String) {
        None("0"), Leader("1"), All("all")
    }
    enum class Compression(val value: String) {
        None("none"), Gzip("gzip"), Snappy("snappy"), Lz4("lz4"), Zstd("zstd")
    }

    internal fun toKafkaProperties(): Map<String, Any> = mapOf(
        ApacheProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ApacheProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
        ApacheProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
        ApacheProducerConfig.ACKS_CONFIG to acks.value,
        ApacheProducerConfig.COMPRESSION_TYPE_CONFIG to compression.value,
        ApacheProducerConfig.LINGER_MS_CONFIG to linger.inWholeMilliseconds.toInt(),
        ApacheProducerConfig.BATCH_SIZE_CONFIG to batchSize,
        ApacheProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to maxInFlight,
        ApacheProducerConfig.BUFFER_MEMORY_CONFIG to bufferMemory,
        ApacheProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to requestTimeout.inWholeMilliseconds.toInt(),
        ApacheProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to enableIdempotence,
    )
}

data class ConsumerConfig(
    val bootstrapServers: String,
    val groupId: String,
    val topics: Topics,
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
) {
    constructor(bootstrapServers: String, groupId: String, vararg topics: String) : this(
        bootstrapServers = bootstrapServers,
        groupId = groupId,
        topics = setOf(*topics),
    )

    enum class IsolationLevel(val value: String) {
        ReadUncommitted("read_uncommitted"),
        ReadCommitted("read_committed"),
    }
}

internal fun ConsumerConfig.toKafkaProperties(): Map<String, Any> = mapOf(
    ApacheConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
    ApacheConsumerConfig.GROUP_ID_CONFIG to groupId,
    ApacheConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to sessionTimeout.inWholeMilliseconds.toInt(),
    ApacheConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG to heartbeatInterval.inWholeMilliseconds.toInt(),
    ApacheConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to maxPollInterval.inWholeMilliseconds.toInt(),
    ApacheConsumerConfig.MAX_POLL_RECORDS_CONFIG to maxPollRecords,
    ApacheConsumerConfig.FETCH_MIN_BYTES_CONFIG to fetchMinBytes,
    ApacheConsumerConfig.FETCH_MAX_BYTES_CONFIG to fetchMaxBytes,
    ApacheConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to fetchMaxWait.inWholeMilliseconds.toInt(),
    ApacheConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG to maxPartitionFetchBytes,
    ApacheConsumerConfig.ISOLATION_LEVEL_CONFIG to isolationLevel.value,
)
