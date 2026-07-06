package se.oyabun.prozess

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig as ApacheProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.UUID
import kotlin.time.toJavaDuration
import kotlin.time.Duration.Companion.seconds

fun <T : Any> Sinks.Many<T>.drainAll(timeout: Duration = 30.seconds.toJavaDuration()): List<T> =
    asFlux().collectList().timeout(timeout).block()!!

fun <T : Any> Sinks.Many<T>.drain(count: Int, timeout: Duration = 30.seconds.toJavaDuration()): List<T> =
    asFlux().take(count.toLong()).collectList().timeout(timeout).block()!!

fun <T : Any> Sinks.Many<T>.drain(duration: kotlin.time.Duration): List<T> =
    asFlux().take(duration.toJavaDuration()).collectList().block()!!

fun topic(bootstrapServers: String, partitions: Int = 1): String = UUID.randomUUID().toString().also { name ->
    AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers))
        .use { it.createTopics(listOf(NewTopic(name, partitions, 1))).all().get() }
}

fun publishAt(bootstrapServers: String, topic: String, messages: List<String>, timestamp: Long? = null) {
    KafkaProducer<String, ByteArray>(
        mapOf(
            ApacheProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ApacheProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ApacheProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
        ),
    ).use { producer ->
        messages.forEach { msg ->
            val record = if (timestamp != null) {
                ProducerRecord(topic, null, timestamp, msg, msg.toByteArray())
            } else {
                ProducerRecord(topic, msg, msg.toByteArray())
            }
            producer.send(record).get()
        }
    }
}

fun groupId(): String = UUID.randomUUID().toString()

fun publish(bootstrapServers: String, topic: String, count: Int): List<String> {
    val messages = (1..count).map { UUID.randomUUID().toString() }
    publishAt(bootstrapServers, topic, messages)
    return messages
}

fun publishToPartition(bootstrapServers: String, topic: String, partition: Int, count: Int): List<String> {
    val messages = (1..count).map { UUID.randomUUID().toString() }
    KafkaProducer<String, ByteArray>(
        mapOf(
            ApacheProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ApacheProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ApacheProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
        ),
    ).use { producer ->
        messages.forEach { msg ->
            producer.send(ProducerRecord(topic, partition, msg, msg.toByteArray())).get()
        }
    }
    return messages
}

fun addPartitions(bootstrapServers: String, topic: String, totalPartitions: Int) {
    AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers))
        .use { it.createPartitions(mapOf(topic to org.apache.kafka.clients.admin.NewPartitions.increaseTo(totalPartitions))).all().get() }
}
