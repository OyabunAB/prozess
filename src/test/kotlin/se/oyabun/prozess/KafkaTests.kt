package se.oyabun.prozess

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.assertThrows
import org.testcontainers.kafka.KafkaContainer
import se.oyabun.prozess.Prozess.ConsumerFilter
import se.oyabun.prozess.Prozess.ConsumerProcess
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(Lifecycle.PER_CLASS)
class KafkaTests {

    private val kafka = KafkaContainer("apache/kafka-native:3.9.2")
    private val bootstrapServers get() = kafka.bootstrapServers

    @BeforeAll
    fun startKafka() = kafka.start()

    @AfterAll
    fun stopKafka() = kafka.stop()

    @Nested
    inner class ConsumerTest {

        @Test
        fun `start called twice throws`() {
            val groupId = groupId()
            val config = ConsumerConfig(bootstrapServers, groupId, setOf(topic(bootstrapServers)))
            val consumer = stringConsumer(config)
            consumer.start()
            assertThrows<IllegalStateException> { consumer.start() }
            consumer.shutdown()
        }

        @Test
        fun `can consume from beginning`() {
            val topic = topic(bootstrapServers)
            val groupId = groupId()
            val count = 5
            val published = publish(bootstrapServers, topic, count = count)
            val received = mutableListOf<String>()
            val latch = CountDownLatch(count)
            val config = ConsumerConfig(bootstrapServers, groupId, topic)
            val consumer = stringConsumer(config) { _, message ->
                received.add(message)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(10, TimeUnit.SECONDS), "timed out waiting for $count messages")
            assertEquals(published.sorted(), received.sorted())
            consumer.shutdown()
        }

        @Test
        fun `can consume high volume from multiple partitions`() {
            val topicName = topic(bootstrapServers, partitions = 3)
            val groupId = groupId()
            val count = 100
            val published = publish(bootstrapServers, topicName, count = count)
            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(count)
            val config = ConsumerConfig(bootstrapServers, groupId, setOf(topicName))
            val consumer = stringConsumer(config) { _, message ->
                received.add(message)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(20, TimeUnit.SECONDS), "timed out waiting for $count messages, got ${received.size}")
            assertEquals(published.sorted(), received.sorted().toList())
            consumer.shutdown()
        }

        @Test
        fun `close terminates the consumer`() {
            val topic = topic(bootstrapServers)
            publish(bootstrapServers, topic, count = 5)
            val config = ConsumerConfig(bootstrapServers, groupId(), topic)
            val consumer = stringConsumer(config)
            consumer.start()
            consumer.shutdown()
            assertTrue(consumer.isDisposed)
        }

        @Test
        fun `close is idempotent`() {
            val topic = topic(bootstrapServers)
            publish(bootstrapServers, topic, count = 3)
            val config = ConsumerConfig(bootstrapServers, groupId(), topic)
            val consumer = stringConsumer(config)
            consumer.start()
            consumer.shutdown()
            consumer.shutdown()
        }

        @Test
        fun `pause is idempotent`() {
            val topic = topic(bootstrapServers)
            val config = ConsumerConfig(bootstrapServers, groupId(), setOf(topic))
            val consumer = stringConsumer(config)
            consumer.start()
            consumer.pause()
            consumer.pause()
            consumer.resume()
            consumer.shutdown()
        }

        @Test
        fun `shutdown during active processing is clean`() {
            val topic = topic(bootstrapServers)
            publish(bootstrapServers, topic, count = 200)
            val config = ConsumerConfig(bootstrapServers, groupId(), topic)
            val consumer = stringConsumer(config)
            consumer.start(from = StartOffset.Earliest)
            consumer.shutdown()
            assertTrue(consumer.isDisposed)
        }

        @Test
        fun `restart from committed offsets`() {
            val topicName = topic(bootstrapServers, partitions = 3)
            val groupId = groupId()
            val count = 100
            val published = publish(bootstrapServers, topicName, count = count)

            val first = ConcurrentLinkedQueue<String>()
            val firstLatch = CountDownLatch(count)
            val firstConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, m ->
                first.add(m)
                firstLatch.countDown()
            }
            firstConsumer.start(from = StartOffset.Earliest)
            assertTrue(firstLatch.await(30, TimeUnit.SECONDS), "first consumer timed out, got ${first.size}")
            firstConsumer.shutdown()
            assertEquals(published.sorted(), first.sorted().toList())

            val second = ConcurrentLinkedQueue<String>()
            val secondLatch = CountDownLatch(1)
            val secondConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, m ->
                second.add(m)
                secondLatch.countDown()
            }
            secondConsumer.start(from = StartOffset.Earliest)
            val noReplay = !secondLatch.await(3, TimeUnit.SECONDS)
            secondConsumer.shutdown()

            assertTrue(noReplay, "restart replayed ${second.size} messages — offsets were lost")
        }
    }

    @Nested
    inner class ProducerTest {

        @Test
        fun `close is idempotent`() {
            val producer = Prozess.producer<String>(ProducerConfig(bootstrapServers, Topic("unused")), { it.toByteArray() })
            producer.close()
            producer.close()
        }
    }

    private fun stringConsumer(
        config: ConsumerConfig,
        filter: ConsumerFilter = { true },
        process: ConsumerProcess<String> = { _, _ -> },
    ) = Prozess.consumer(
        config = config,
        filter = filter,
        deserializer = { String(it) },
        process = process,
    )
}
