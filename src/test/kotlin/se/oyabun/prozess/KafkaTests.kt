package se.oyabun.prozess

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.testcontainers.kafka.KafkaContainer
import se.oyabun.prozess.Prozess.ConsumerFilter
import se.oyabun.prozess.Prozess.ConsumerProcess
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@TestInstance(Lifecycle.PER_CLASS)
@Timeout(30)
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
            val config = ConsumerConfig(bootstrapServers, groupId, setOf(topic))
            val consumer = stringConsumer(config) { _, message ->
                received.add(message)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for $count messages")
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
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for $count messages, got ${received.size}")
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
            assertTrue(firstLatch.await(5, TimeUnit.SECONDS), "first consumer timed out, got ${first.size}")
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

    @Nested
    inner class OffsetSeekTest {

        @Test
        fun `earliest seeks to beginning only for partitions without committed offsets`() {
            val topicName = topic(bootstrapServers, partitions = 2)
            val groupId = groupId()
            val count = 10
            val published = publish(bootstrapServers, topicName, count = count)

            // First consumer: consume and commit all messages from both partitions
            val firstLatch = CountDownLatch(count)
            val firstConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _ ->
                firstLatch.countDown()
            }
            firstConsumer.start(from = StartOffset.Earliest)
            assertTrue(firstLatch.await(5, TimeUnit.SECONDS), "first consumer timed out")
            firstConsumer.shutdown()

            // Add a third partition (no committed offsets for it)
            addPartitions(bootstrapServers, topicName, totalPartitions = 3)

            // Publish to the new partition only
            val newPartitionMessages = publishToPartition(bootstrapServers, topicName, partition = 2, count = 5)

            // Second consumer: Earliest should seek partition 2 to beginning, resume 0 and 1 from committed
            val received = ConcurrentLinkedQueue<String>()
            val secondLatch = CountDownLatch(5)
            val secondConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, msg ->
                received.add(msg)
                secondLatch.countDown()
            }
            secondConsumer.start(from = StartOffset.Earliest)
            assertTrue(secondLatch.await(5, TimeUnit.SECONDS), "second consumer timed out, got ${received.size}")
            secondConsumer.shutdown()

            assertEquals(
                newPartitionMessages.sorted(),
                received.sorted().toList(),
                "Should only receive messages from the unseeded partition",
            )
        }

        @Test
        fun `config startOffset used as default when from is not specified`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val count = 5
            val published = publish(bootstrapServers, topicName, count = count)

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(count)
            val config = ConsumerConfig(
                bootstrapServers = bootstrapServers,
                groupId = groupId,
                topics = setOf(topicName),
                startOffset = StartOffset.Earliest,
            )
            val consumer = StreamingConsumer(
                config = config,
                processor = DefaultProcessor.each(
                    deserializer = { String(it) },
                    handler = { p ->
                        received.add(p.value)
                        latch.countDown()
                    },
                ),
            )
            consumer.start() // no from parameter — should use config.startOffset
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for $count messages, got ${received.size}")
            assertEquals(published.sorted(), received.sorted().toList())
            consumer.shutdown()
        }

        @Test
        fun `latest skips pre-existing messages`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val preExisting = publish(bootstrapServers, topicName, count = 10)

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(5)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Latest)

            // Wait for assignment to stabilize before publishing new messages
            Thread.sleep(2000)
            val newMessages = publish(bootstrapServers, topicName, count = 5)

            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for new messages, got ${received.size}")
            consumer.shutdown()

            val preExistingSet = preExisting.toSet()
            for (msg in received) {
                assertTrue(msg !in preExistingSet, "Received pre-existing message: $msg")
            }
            assertEquals(newMessages.sorted(), received.sorted().toList())
        }

        @Test
        fun `at timestamp consumes only messages after given time`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()

            val beforeTimestamp = System.currentTimeMillis()
            val earlyMessages = listOf("early-1", "early-2", "early-3")
            publishAt(bootstrapServers, topicName, earlyMessages, timestamp = beforeTimestamp - 10_000)

            Thread.sleep(100)
            val targetTimestamp = System.currentTimeMillis()
            Thread.sleep(100)

            val lateMessages = listOf("late-1", "late-2", "late-3")
            publishAt(bootstrapServers, topicName, lateMessages, timestamp = targetTimestamp + 1000)

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(3)
            val config = ConsumerConfig(
                bootstrapServers = bootstrapServers,
                groupId = groupId,
                topics = setOf(topicName),
                startOffset = StartOffset.AtTimestamp(kotlin.time.Instant.fromEpochMilliseconds(targetTimestamp)),
            )
            val consumer = stringConsumer(config) { _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.AtTimestamp(kotlin.time.Instant.fromEpochMilliseconds(targetTimestamp)))
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out, got ${received.size}")
            consumer.shutdown()

            val earlySet = earlyMessages.toSet()
            for (msg in received) {
                assertTrue(msg !in earlySet, "Received early message: $msg")
            }
            assertTrue(received.containsAll(lateMessages), "Should contain all late messages, got $received")
        }

        @Test
        fun `at timestamp with committed offsets ahead does not seek backwards`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val count = 10

            // Publish and consume all with consumer A
            val published = publish(bootstrapServers, topicName, count = count)
            val firstLatch = CountDownLatch(count)
            val consumerA = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _ ->
                firstLatch.countDown()
            }
            consumerA.start(from = StartOffset.Earliest)
            assertTrue(firstLatch.await(5, TimeUnit.SECONDS), "consumer A timed out")
            consumerA.shutdown()

            // Restart with AtTimestamp pointing to the beginning — committed offsets are ahead
            val pastTimestamp = kotlin.time.Instant.fromEpochMilliseconds(System.currentTimeMillis() - 60_000)
            val received = ConcurrentLinkedQueue<String>()
            val replayLatch = CountDownLatch(1)
            val consumerB = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, msg ->
                received.add(msg)
                replayLatch.countDown()
            }
            consumerB.start(from = StartOffset.AtTimestamp(pastTimestamp))
            val replayed = replayLatch.await(3, TimeUnit.SECONDS)
            consumerB.shutdown()

            assertTrue(!replayed, "Should not seek backwards past committed offsets, but got ${received.size} messages")
        }
    }

    @Nested
    inner class CatchUpTest {

        @Test
        fun `catch up consumes to end then terminates`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val count = 20
            val published = publish(bootstrapServers, topicName, count = count)

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(count)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest, until = EndOffset.CatchUp)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out, got ${received.size}")

            // Publish more after catch-up — should not be received
            val afterMessages = publish(bootstrapServers, topicName, count = 5)
            Thread.sleep(500)
            consumer.shutdown()

            assertEquals(published.sorted(), received.sorted().toList(), "Should receive exactly the original messages")
            val afterSet = afterMessages.toSet()
            for (msg in received) {
                assertTrue(msg !in afterSet, "Should not receive messages published after catch-up")
            }
        }

        @Test
        fun `catch up multi-partition completes only after all partitions reach end`() {
            val topicName = topic(bootstrapServers, partitions = 3)
            val groupId = groupId()

            // Publish different amounts per partition to create uneven load
            val p0 = publishToPartition(bootstrapServers, topicName, partition = 0, count = 5)
            val p1 = publishToPartition(bootstrapServers, topicName, partition = 1, count = 10)
            val p2 = publishToPartition(bootstrapServers, topicName, partition = 2, count = 3)
            val totalCount = 18
            val allPublished = (p0 + p1 + p2).sorted()

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(totalCount)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest, until = EndOffset.CatchUp)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out, got ${received.size}")
            consumer.shutdown()

            assertEquals(allPublished, received.sorted().toList(), "Should receive all messages from all partitions")
        }

        @Test
        fun `at timestamp with catch up consumes range then terminates`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()

            val earlyMessages = listOf("early-1", "early-2")
            publishAt(bootstrapServers, topicName, earlyMessages, timestamp = System.currentTimeMillis() - 10_000)

            Thread.sleep(100)
            val targetTimestamp = System.currentTimeMillis()
            Thread.sleep(100)

            val lateMessages = listOf("late-1", "late-2", "late-3")
            publishAt(bootstrapServers, topicName, lateMessages, timestamp = targetTimestamp + 1000)

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(lateMessages.size)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(
                from = StartOffset.AtTimestamp(kotlin.time.Instant.fromEpochMilliseconds(targetTimestamp)),
                until = EndOffset.CatchUp,
            )
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out, got ${received.size}")
            consumer.shutdown()

            val earlySet = earlyMessages.toSet()
            for (msg in received) {
                assertTrue(msg !in earlySet, "Received early message: $msg")
            }
            assertTrue(received.containsAll(lateMessages), "Should contain all late messages")
        }
    }

    @Nested
    inner class FilterTest {

        @Test
        fun `filtered messages are skipped but offsets still advance`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val count = 20
            publish(bootstrapServers, topicName, count = count)

            val received = ConcurrentLinkedQueue<String>()
            val processedCount = java.util.concurrent.atomic.AtomicInteger(0)
            val allSeen = CountDownLatch(count)

            // Filter: only process messages with even index (every other message)
            val messageIndex = java.util.concurrent.atomic.AtomicInteger(0)
            val consumer = Prozess.consumer(
                config = ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
                deserializer = { String(it) },
                filter = {
                    allSeen.countDown()
                    messageIndex.getAndIncrement() % 2 == 0
                },
                process = { _, msg ->
                    received.add(msg)
                    processedCount.incrementAndGet()
                },
            )
            consumer.start(from = StartOffset.Earliest)
            assertTrue(allSeen.await(5, TimeUnit.SECONDS), "not all messages passed through filter")
            consumer.shutdown()

            assertTrue(processedCount.get() < count, "Filter should have prevented some messages from processing")
            assertTrue(processedCount.get() > 0, "Some messages should have been processed")

            // Restart in same group — should NOT replay filtered messages
            val replayed = ConcurrentLinkedQueue<String>()
            val replayLatch = CountDownLatch(1)
            val consumer2 = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, msg ->
                replayed.add(msg)
                replayLatch.countDown()
            }
            consumer2.start(from = StartOffset.Earliest)
            val hadReplay = replayLatch.await(2, TimeUnit.SECONDS)
            consumer2.shutdown()

            assertTrue(!hadReplay, "Filtered messages should not be replayed, offsets should have advanced. Got ${replayed.size} messages")
        }
    }

    @Nested
    inner class AtLeastOnceTest {

        @Test
        fun `messages not yet committed are redelivered after rebalance`() {
            val topicName = topic(bootstrapServers, partitions = 1)
            val groupId = groupId()
            val count = 5
            val published = publish(bootstrapServers, topicName, count = count)

            // Consumer A: processes slowly, gets some messages but commits are delayed
            val receivedA = ConcurrentLinkedQueue<String>()
            val firstArrived = CountDownLatch(1)
            val consumerA = stringConsumer(
                ConsumerConfig(
                    bootstrapServers = bootstrapServers,
                    groupId = groupId,
                    topics = setOf(topicName),
                    sessionTimeout = 6.seconds,
                    heartbeatInterval = 2.seconds,
                ),
            ) { _, msg ->
                receivedA.add(msg)
                firstArrived.countDown()
            }
            consumerA.start(from = StartOffset.Earliest)
            assertTrue(firstArrived.await(5, TimeUnit.SECONDS), "consumer A never received messages")

            // Graceful shutdown — processed messages get committed
            consumerA.shutdown()
            val committedCount = receivedA.size

            // Consumer B: same group, should resume from committed offset
            val receivedB = ConcurrentLinkedQueue<String>()
            val allDone = CountDownLatch(count - committedCount)
            val consumerB = stringConsumer(
                ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
            ) { _, msg ->
                receivedB.add(msg)
                allDone.countDown()
            }
            consumerB.start(from = StartOffset.Earliest)

            if (committedCount < count) {
                assertTrue(allDone.await(5, TimeUnit.SECONDS), "consumer B didn't get remaining messages")
            }
            consumerB.shutdown()

            // Union of A and B should cover all published messages (at-least-once)
            val allReceived = (receivedA + receivedB).toSet()
            assertTrue(
                allReceived.containsAll(published.toSet()),
                "At-least-once: all messages must be delivered across consumers. Missing: ${published.toSet() - allReceived}",
            )
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
