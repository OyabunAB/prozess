package se.oyabun.prozess

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.testcontainers.kafka.KafkaContainer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(Lifecycle.PER_CLASS)
class RebalanceKafkaTests {

    private val kafka = KafkaContainer("apache/kafka-native:3.9.2")
    private val bootstrapServers get() = kafka.bootstrapServers

    @BeforeAll
    fun startKafka() = kafka.start()

    @AfterAll
    fun stopKafka() = kafka.stop()

    @Nested
    inner class TwoConsumerGroupTest {

        @Test
        fun `two consumers in same group split partitions without duplication`() {
            val topicName = topic(bootstrapServers, partitions = 2)
            val groupId = groupId()
            val count = 20
            val published = publish(bootstrapServers, topicName, count = count)

            val messagesA = ConcurrentLinkedQueue<String>()
            val messagesB = ConcurrentLinkedQueue<String>()
            val allDone = CountDownLatch(count)

            val consumerA = stringConsumer(
                ConsumerConfig(bootstrapServers, groupId, setOf(topicName))
            ) { _, msg ->
                messagesA.add(msg)
                allDone.countDown()
            }

            val consumerB = stringConsumer(
                ConsumerConfig(bootstrapServers, groupId, setOf(topicName))
            ) { _, msg ->
                messagesB.add(msg)
                allDone.countDown()
            }

            consumerA.start(from = StartOffset.Earliest)
            consumerB.start(from = StartOffset.Earliest)

            assertTrue(
                allDone.await(30, TimeUnit.SECONDS),
                "timed out waiting for $count messages, got A=${messagesA.size} B=${messagesB.size}",
            )

            consumerA.shutdown()
            consumerB.shutdown()

            val allConsumed = messagesA + messagesB
            assertEquals(published.sorted(), allConsumed.sorted(), "All messages must be consumed at least once")

            for (msg in messagesA) {
                assertTrue(msg !in messagesB, "Message $msg was processed by both consumers (duplication)")
            }
        }

        @Test
        fun `rebalance commits processed offsets preventing replay`() {
            val topicName = topic(bootstrapServers, partitions = 2)
            val groupId = groupId()
            val firstBatch = publish(bootstrapServers, topicName, count = 10)
            val firstDone = CountDownLatch(10)

            val messagesB = ConcurrentLinkedQueue<String>()
            val allDone = CountDownLatch(20)

            val consumerA = stringConsumer(
                ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
            ) { _, _ ->
                firstDone.countDown()
                allDone.countDown()
            }
            consumerA.start(from = StartOffset.Earliest)
            assertTrue(
                firstDone.await(30, TimeUnit.SECONDS),
                "Consumer A did not process first batch",
            )

            val consumerB = stringConsumer(
                ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
            ) { _, msg ->
                messagesB.add(msg)
                allDone.countDown()
            }
            consumerB.start(from = StartOffset.Earliest)

            val secondBatch = publish(bootstrapServers, topicName, count = 10)

            assertTrue(
                allDone.await(30, TimeUnit.SECONDS),
                "Not all 20 messages processed, Consumer B got ${messagesB.size}",
            )

            consumerA.shutdown()
            consumerB.shutdown()

            val firstSet = firstBatch.toSet()
            for (msg in messagesB) {
                assertTrue(
                    msg !in firstSet,
                    "Replay detected: Consumer B processed '$msg' from first batch",
                )
            }
        }

        @Test
        fun `pause and resume lifecycle with two consumers`() {
            val topicName = topic(bootstrapServers, partitions = 2)
            val groupId = groupId()
            val count = 20
            val published = publish(bootstrapServers, topicName, count = count)
            val allDone = CountDownLatch(count)

            val messagesA = ConcurrentLinkedQueue<String>()
            val messagesB = ConcurrentLinkedQueue<String>()

            val consumerA = stringConsumer(
                ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
            ) { _, msg ->
                messagesA.add(msg)
                allDone.countDown()
            }

            val consumerB = stringConsumer(
                ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
            ) { _, msg ->
                messagesB.add(msg)
                allDone.countDown()
            }

            consumerA.start(from = StartOffset.Earliest)
            consumerB.start(from = StartOffset.Earliest)

            consumerA.pause()
            consumerA.pause()
            consumerA.resume()

            assertTrue(
                allDone.await(30, TimeUnit.SECONDS),
                "timed out waiting for $count messages, got A=${messagesA.size} B=${messagesB.size}",
            )

            consumerA.shutdown()
            consumerB.shutdown()

            val allConsumed = messagesA + messagesB
            assertEquals(published.sorted(), allConsumed.sorted(), "All messages must be consumed despite pause/resume cycle")

            assertTrue(messagesA.isNotEmpty(), "Consumer A should have processed some messages")
            assertTrue(messagesB.isNotEmpty(), "Consumer B should have processed some messages")
        }
    }

    private fun stringConsumer(
        config: ConsumerConfig,
        process: (Received, String) -> Unit = { _, _ -> },
    ) = Prozess.consumer(
        config = config,
        deserializeBytes = { String(it) },
        process = process,
    )
}
