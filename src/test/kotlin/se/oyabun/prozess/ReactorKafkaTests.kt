package se.oyabun.prozess

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.testcontainers.kafka.KafkaContainer
import reactor.core.publisher.Flux
import reactor.test.StepVerifier.create
import se.oyabun.prozess.reactor.ReactorKafkaProducer
import kotlin.test.assertTrue

@TestInstance(Lifecycle.PER_CLASS)
class ReactorKafkaTests {

    private val kafka = KafkaContainer("apache/kafka-native:3.8.0")
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
            val producer = ReactorKafkaProducer<String>(config) { it.toByteArray() }
            val messages = (1..10).map { "msg-$it" }
            val result = producer.sendAll(Flux.fromIterable(messages)) { it }.collectList()
            create(result).assertNext { assertTrue { it.containsAll(messages) } }.verifyComplete()
            producer.close().block()
        }
    }
}
