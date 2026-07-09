package se.oyabun.prozess

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig as ApacheProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

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

/**
 * Registers assignment callbacks and returns a latch that counts down when each consumer
 * receives [ConsumerEvent.Assigned]. Call this BEFORE [Prozess.Consumer.start], then
 * call [Prozess.Consumer.start], then [awaitLatch] to block until all are assigned.
 */
fun onAssigned(vararg consumers: Prozess.Consumer<*>): CountDownLatch {
    val latch = CountDownLatch(consumers.size)
    consumers.forEach { consumer ->
        val counted = AtomicBoolean(false)
        consumer.onEvent { event ->
            if (event is ConsumerEvent.Assigned && counted.compareAndSet(false, true)) {
                latch.countDown()
            }
        }
    }
    return latch
}

/**
 * Registers a stopped callback and returns a latch that counts down when the consumer
 * emits [ConsumerEvent.Stopped]. Call this BEFORE [Prozess.Consumer.start], then
 * call [awaitLatch] to block until the consumer has fully stopped.
 */
fun onStopped(consumer: Prozess.Consumer<*>): CountDownLatch {
    val latch = CountDownLatch(1)
    val counted = AtomicBoolean(false)
    consumer.onEvent { event ->
        if (event is ConsumerEvent.Stopped && counted.compareAndSet(false, true)) {
            latch.countDown()
        }
    }
    return latch
}

/**
 * Blocks until [latch] reaches zero. Use with [onAssigned] or [onStopped].
 */
fun awaitLatch(latch: CountDownLatch, timeoutSeconds: Long = 5) {
    check(latch.await(timeoutSeconds, TimeUnit.SECONDS)) {
        "Timed out waiting for latch (${latch.count} remaining)"
    }
}
