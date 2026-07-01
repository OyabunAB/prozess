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
            val published = publish(bootstrapServers, topic, count = 5)
            val received = mutableListOf<String>()
            val config = ConsumerConfig(bootstrapServers, groupId, topic)
            val consumer = stringConsumer(config) { _, message -> received.add(message) }
            consumer.start(from = StartOffset.Earliest)
            Thread.sleep(3000)
            assertEquals(published.sorted(), received.sorted())
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
