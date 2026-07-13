package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.testcontainers.kafka.KafkaContainer
import se.oyabun.aelv.Many
import se.oyabun.aelv.last
import se.oyabun.aelv.toList
import se.oyabun.aelv.Verify
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(Lifecycle.PER_CLASS)
class StreamingKafkaTests {

    private val kafka = KafkaContainer("apache/kafka-native:3.9.2")
    private val bootstrapServers get() = kafka.bootstrapServers

    @BeforeAll
    fun startKafka() = kafka.start()

    @AfterAll
    fun stopKafka() = kafka.stop()

    @Nested
    inner class ProducerTest {

        @Test
        fun `sendAll passes through original elements`() {
            val topic = topic(bootstrapServers)
            val config = ProducerConfig(bootstrapServers, Topic(topic))
            val producer = StreamingProducer<String>(
                config = config,
                keyExtractor = { it },
                keyBytes = { it.toByteArray() },
                serializer = { it.toByteArray() },
            )
            val messages = (1..10).map { "msg-$it" }
            Verify.that(producer.sendAll(Many.from(messages)).toList())
                .matchesNext { assertEquals(messages.sorted(), it.sorted()) }
                .completesNormally()
            runBlocking { producer.close().await() }
        }

        @Test
        fun `sendAll with headersProvider attaches headers`() {
            val topicName = topic(bootstrapServers)
            val config = ProducerConfig(bootstrapServers, Topic(topicName))
            val producer = StreamingProducer<String>(
                config = config,
                keyExtractor = { it },
                keyBytes = { it.toByteArray() },
                headerEnricher = { listOf(Header("trace-id", "${it}-123".toByteArray())) },
                serializer = { it.toByteArray() },
            )
            val payload = "hello"
            runBlocking { producer.sendAll(Many.items(payload)).last() }
            runBlocking { producer.close().await() }

            val receivedHeaders = mutableListOf<Headers>()
            val latch = java.util.concurrent.CountDownLatch(1)
            val consumer = Prozess.consumer(
                config = ConsumerConfig(bootstrapServers, "groupId", setOf(topicName)),
                messageDeserializer = { String(it) },
                process = { headers, _, _ ->
                    receivedHeaders.add(headers)
                    latch.countDown()
                },
            )
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, java.util.concurrent.TimeUnit.SECONDS), "timed out")
            consumer.shutdown()

            assertEquals(1, receivedHeaders.size)
            val headers = receivedHeaders[0]
            assertEquals(1, headers.size)
            assertEquals("trace-id", headers[0].key)
            assertTrue(headers[0].value.contentEquals("hello-123".toByteArray()))
        }
    }
}
