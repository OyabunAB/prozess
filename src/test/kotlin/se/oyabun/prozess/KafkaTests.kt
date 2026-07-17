package se.oyabun.prozess

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.testcontainers.kafka.KafkaContainer
import se.oyabun.prozess.Prozess.DeserializationResult
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

@Tag("integration")
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
            val consumer = stringConsumer(config) { _, _, message ->
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
            val consumer = stringConsumer(config) { _, _, message ->
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
            val firstConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, m ->
                first.add(m)
                firstLatch.countDown()
            }
            firstConsumer.start(from = StartOffset.Earliest)
            assertTrue(firstLatch.await(5, TimeUnit.SECONDS), "first consumer timed out, got ${first.size}")
            firstConsumer.shutdown()
            assertEquals(published.sorted(), first.sorted().toList())

            val second = ConcurrentLinkedQueue<String>()
            val secondLatch = CountDownLatch(1)
            val secondConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, m ->
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

        @Test
        fun `transactional commit is consumed with read committed isolation`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val payload = "tx-committed"

            val producer = Prozess.producer<String>(
                ProducerConfig(
                    bootstrapServers = bootstrapServers,
                    topic = Topic(topicName),
                    transactional = TransactionalConfig.Enabled("test-tx-${groupId}"),
                ),
                { it.toByteArray() },
            )
            producer.initTransactions()
            producer.beginTransaction()
            producer.send(payload)
            producer.commitTransaction()
            producer.close()

            val received = mutableListOf<String>()
            val latch = CountDownLatch(1)
            val config = ConsumerConfig(
                bootstrapServers = bootstrapServers,
                groupId = groupId,
                topics = setOf(topicName),
                isolationLevel = ConsumerConfig.IsolationLevel.ReadCommitted,
            )
            val consumer = stringConsumer(config) { _, _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for committed message")
            consumer.shutdown()

            assertEquals(listOf(payload), received)
        }

        @Test
        fun `transactional abort is not visible with read committed isolation`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val committedPayload = "tx-committed"
            val abortedPayload = "tx-aborted"

            val producer = Prozess.producer<String>(
                ProducerConfig(
                    bootstrapServers = bootstrapServers,
                    topic = Topic(topicName),
                    transactional = TransactionalConfig.Enabled("test-tx-${groupId}"),
                ),
                { it.toByteArray() },
            )
            producer.initTransactions()

            // Send and commit
            producer.beginTransaction()
            producer.send(committedPayload)
            producer.commitTransaction()

            // Send and abort
            producer.beginTransaction()
            producer.send(abortedPayload)
            producer.abortTransaction()
            producer.close()

            val received = mutableListOf<String>()
            val latch = CountDownLatch(1)
            val config = ConsumerConfig(
                bootstrapServers = bootstrapServers,
                groupId = groupId,
                topics = setOf(topicName),
                isolationLevel = ConsumerConfig.IsolationLevel.ReadCommitted,
            )
            val consumer = stringConsumer(config) { _, _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for committed message")
            consumer.shutdown()

            assertEquals(listOf(committedPayload), received, "aborted message should not be visible")
        }

        @Test
        fun `send with headers and consume`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val payload = "hello-headers"

            val producer = Prozess.producer<String>(
                config = ProducerConfig(bootstrapServers, Topic(topicName)),
                serializer = { it.toByteArray() },
                headerEnricher = { listOf(Header("trace-id", "abc123".toByteArray()), Header("content-type", "json".toByteArray())) },
            )
            producer.send(payload)
            producer.close()

            val receivedHeaders = mutableListOf<Headers>()
            val latch = CountDownLatch(1)
            val config = ConsumerConfig(bootstrapServers, groupId, setOf(topicName))
            val consumer = stringConsumer(config) { headers, _, _ ->
                receivedHeaders.add(headers)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for message")
            consumer.shutdown()

            assertEquals(1, receivedHeaders.size)
            val headers = receivedHeaders[0]
            assertEquals(2, headers.size)
            assertEquals("trace-id", headers[0].key)
            assertTrue(headers[0].value.contentEquals("abc123".toByteArray()))
            assertEquals("content-type", headers[1].key)
            assertTrue(headers[1].value.contentEquals("json".toByteArray()))
        }

        @Test
        fun `sendAll collection sends all messages and returns them`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val messages = (1..5).map { "batch-$it" }

            val producer = Prozess.producer<String, String>(
                config = ProducerConfig(bootstrapServers, Topic(topicName)),
                keyExtractor = { it },
                keySerializer = { it.toByteArray() },
                serializer = { it.toByteArray() },
            )
            val returned = producer.sendAll(messages)
            producer.close()

            assertEquals(messages.sorted(), returned.sorted())

            val received = mutableListOf<String>()
            val latch = CountDownLatch(messages.size)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for batch messages")
            consumer.shutdown()

            assertEquals(messages.sorted(), received.sorted())
        }

        @Test
        fun `sendAll vararg sends all messages and returns them`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()

            val producer = Prozess.producer<String, String>(
                config = ProducerConfig(bootstrapServers, Topic(topicName)),
                keyExtractor = { it },
                keySerializer = { it.toByteArray() },
                serializer = { it.toByteArray() },
            )
            val returned = producer.sendAll("alpha", "beta", "gamma")
            producer.close()

            assertEquals(listOf("alpha", "beta", "gamma").sorted(), returned.sorted())

            val received = mutableListOf<String>()
            val latch = CountDownLatch(3)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for vararg messages")
            consumer.shutdown()

            assertEquals(listOf("alpha", "beta", "gamma").sorted(), received.sorted())
        }

        @Test
        fun `sendAll attaches headers to all messages`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val messages = listOf("x", "y")

            val producer = Prozess.producer<String, String>(
                config = ProducerConfig(bootstrapServers, Topic(topicName)),
                keyExtractor = { it },
                keySerializer = { it.toByteArray() },
                serializer = { it.toByteArray() },
                headerEnricher = { listOf(Header("src", it.toByteArray())) },
            )
            producer.sendAll(messages)
            producer.close()

            val receivedHeaders = mutableListOf<Pair<String, Headers>>()
            val latch = CountDownLatch(messages.size)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { headers, _, msg ->
                receivedHeaders.add(msg to headers)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for messages with headers")
            consumer.shutdown()

            assertEquals(2, receivedHeaders.size)
            receivedHeaders.forEach { (msg, headers) ->
                assertEquals(1, headers.size)
                assertEquals("src", headers[0].key)
                assertTrue(headers[0].value.contentEquals(msg.toByteArray()))
            }
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
            val firstConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, _ ->
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
            val secondConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
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
                    keyMapper = { Key.Missing },
                    deserializer = { r ->
                        when (val msg = r.message) {
                            is ReceivedMessage.Data -> Prozess.DeserializationResult.Message(String(msg.bytes))
                            is ReceivedMessage.Tombstone -> Prozess.DeserializationResult.Tombstone
                        }
                    },
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
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
                received.add(msg)
                latch.countDown()
            }

            val assigned = onAssigned(consumer)
            consumer.start(from = StartOffset.Latest)
            awaitLatch(assigned)

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

            val earlyMessages = listOf("early-1", "early-2", "early-3")
            publishAt(bootstrapServers, topicName, earlyMessages, timestamp = System.currentTimeMillis() - 10_000)

            val targetTimestamp = System.currentTimeMillis()

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
            val consumer = stringConsumer(config) { _, _, msg ->
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
            val consumerA = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, _ ->
                firstLatch.countDown()
            }
            consumerA.start(from = StartOffset.Earliest)
            assertTrue(firstLatch.await(5, TimeUnit.SECONDS), "consumer A timed out")
            consumerA.shutdown()

            // Restart with AtTimestamp pointing to the beginning — committed offsets are ahead
            val pastTimestamp = kotlin.time.Instant.fromEpochMilliseconds(System.currentTimeMillis() - 60_000)
            val received = ConcurrentLinkedQueue<String>()
            val replayLatch = CountDownLatch(1)
            val consumerB = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
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
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
                received.add(msg)
                latch.countDown()
            }
            consumer.start(from = StartOffset.Earliest, until = EndOffset.CatchUp)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out, got ${received.size}")
            consumer.shutdown()

            // Publish more after catch-up — should not be received
            val afterMessages = publish(bootstrapServers, topicName, count = 5)

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
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
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

            val targetTimestamp = System.currentTimeMillis()

            val lateMessages = listOf("late-1", "late-2", "late-3")
            publishAt(bootstrapServers, topicName, lateMessages, timestamp = targetTimestamp + 1000)

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(lateMessages.size)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
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
        fun `skipped messages are not replayed after restart`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val count = 20
            publish(bootstrapServers, topicName, count = count)

            val received = ConcurrentLinkedQueue<String>()
            val processedCount = java.util.concurrent.atomic.AtomicInteger(0)
            val allSeen = CountDownLatch(count)

            val messageIndex = java.util.concurrent.atomic.AtomicInteger(0)
            val consumer = Prozess.consumer(
                config = ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
                deserializer = { received ->
                    allSeen.countDown()
                    if (messageIndex.getAndIncrement() % 2 == 0) {
                        when (val msg = received.message) {
                            is ReceivedMessage.Data -> Prozess.DeserializationResult.Message(String(msg.bytes))
                            is ReceivedMessage.Tombstone -> Prozess.DeserializationResult.Tombstone
                        }
                    } else {
                        Prozess.DeserializationResult.PoisonPill("skipped")
                    }
                },
            ).each { p ->
                received.add(p.value)
                processedCount.incrementAndGet()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(allSeen.await(5, TimeUnit.SECONDS), "not all messages passed through filter")
            consumer.shutdown()

            assertTrue(processedCount.get() < count, "Filter should have prevented some messages from processing")
            assertTrue(processedCount.get() > 0, "Some messages should have been processed")

            // Restart in same group — should NOT replay filtered messages
            val replayed = ConcurrentLinkedQueue<String>()
            val replayLatch = CountDownLatch(1)
            val consumer2 = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
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
            ) { _, _, msg ->
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
            ) { _, _, msg ->
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
        process: (Headers, Key<ByteArray>, String) -> Unit = { _, _, _ -> },
    ) = Prozess.consumer(
        config = config,
        messageDeserializer = { String(it) },
        process = process,
    )

    @Nested
    inner class ProcessingModeTest {

        @Test
        fun `batch mode commits offsets and does not replay on restart`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val count = 20
            val published = publish(bootstrapServers, topicName, count = count)

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(count)
            val consumer = Prozess.consumer(
                config = ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
                deserializer = { r ->
                    when (val msg = r.message) {
                        is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                        is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                    }
                },
            ).batch(size = 5) { batch ->
                batch.forEach { received.add(it.value); latch.countDown() }
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out, got ${received.size}")
            consumer.shutdown()
            assertEquals(published.sorted(), received.sorted().toList())

            val replayLatch = CountDownLatch(1)
            val replayConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, _ ->
                replayLatch.countDown()
            }
            replayConsumer.start(from = StartOffset.Earliest)
            val replayed = replayLatch.await(3, TimeUnit.SECONDS)
            replayConsumer.shutdown()
            assertTrue(!replayed, "batch mode must commit offsets — no replay expected")
        }

        @Test
        fun `groupBy each commits offsets and does not replay on restart`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val count = 15
            val published = publish(bootstrapServers, topicName, count = count)

            val received = ConcurrentLinkedQueue<String>()
            val latch = CountDownLatch(count)
            val consumer = Prozess.consumer(
                config = ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
                deserializer = { r ->
                    when (val msg = r.message) {
                        is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                        is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                    }
                },
            ).groupBy { p -> p.value.take(4) }
             .each { _, p -> received.add(p.value); latch.countDown() }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out, got ${received.size}")
            consumer.shutdown()
            assertEquals(published.sorted(), received.sorted().toList())

            val replayLatch = CountDownLatch(1)
            val replayConsumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, _ ->
                replayLatch.countDown()
            }
            replayConsumer.start(from = StartOffset.Earliest)
            val replayed = replayLatch.await(3, TimeUnit.SECONDS)
            replayConsumer.shutdown()
            assertTrue(!replayed, "groupBy each must commit offsets — no replay expected")
        }

        @Test
        fun `consumer with key deserializer receives Key Present with correct value`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()

            val producer = Prozess.producer<String, String>(
                config = ProducerConfig(bootstrapServers, Topic(topicName)),
                keyExtractor = { it },
                keySerializer = { it.toByteArray() },
                serializer = { it.toByteArray() },
            )
            producer.send("hello")
            producer.close()

            val receivedKeys = mutableListOf<Key<String>>()
            val latch = CountDownLatch(1)
            val consumer = Prozess.consumer(
                config = ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
                keyDeserializer = { String(it) },
                messageDeserializer = { String(it) },
                process = { _, key, _ -> receivedKeys.add(key); latch.countDown() },
            )
            consumer.start(from = StartOffset.Earliest)
            assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for keyed message")
            consumer.shutdown()

            assertEquals(1, receivedKeys.size)
            val key = receivedKeys[0]
            assertTrue(key is Key.Present, "key must be Key.Present, got $key")
            assertEquals("hello", (key as Key.Present<String>).value)
        }
    }

    @Nested
    inner class CatchUpEdgeCasesTest {

        @Test
        fun `catchup on empty topic completes immediately without processing any records`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()

            val processed = AtomicInteger(0)
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, _ ->
                processed.incrementAndGet()
            }
            val stopped = onStopped(consumer)
            consumer.start(from = StartOffset.Earliest, until = EndOffset.CatchUp)
            assertTrue(stopped.await(5, TimeUnit.SECONDS), "consumer must self-terminate on empty topic with CatchUp")
            assertEquals(0, processed.get(), "no records should be processed on an empty topic")
        }

        @Test
        fun `atTimestamp with no matching records seeks to end and receives nothing`() {
            val topicName = topic(bootstrapServers)
            val groupId = groupId()
            val pastMessages = listOf("old-1", "old-2", "old-3")
            publishAt(bootstrapServers, topicName, pastMessages, timestamp = System.currentTimeMillis() - 60_000)

            val futureTimestamp = Instant.fromEpochMilliseconds(System.currentTimeMillis() + 3_600_000)
            val received = ConcurrentLinkedQueue<String>()
            val consumer = stringConsumer(ConsumerConfig(bootstrapServers, groupId, setOf(topicName))) { _, _, msg ->
                received.add(msg)
            }
            val assigned = onAssigned(consumer)
            consumer.start(from = StartOffset.AtTimestamp(futureTimestamp))
            awaitLatch(assigned)
            Thread.sleep(1000)
            consumer.shutdown()
            assertTrue(received.isEmpty(), "no messages should be received when timestamp is in the future, got: $received")
        }
    }

    @Nested
    inner class LagTest {

        @Test
        fun `lag decreases as messages are consumed`() {
            val topicName = topic(bootstrapServers, partitions = 1)
            val groupId = groupId()
            val count = 20
            publish(bootstrapServers, topicName, count = count)

            val partition = Partition(0, Topic(topicName))
            val processedLatch = CountDownLatch(count)
            val firstProcessed = CountDownLatch(1)
            val consumer = StreamingConsumer(
                config = ConsumerConfig(bootstrapServers, groupId, setOf(topicName)),
                processor = DefaultProcessor.each<Nothing, String>(
                    deserializer = { r ->
                        when (val msg = r.message) {
                            is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                            is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                        }
                    },
                    handler = { firstProcessed.countDown(); processedLatch.countDown() },
                ),
            )
            consumer.start(from = StartOffset.Earliest)

            // Wait until at least one record is processed — position is now set
            assertTrue(firstProcessed.await(5, TimeUnit.SECONDS), "timed out waiting for first record")
            val lagMidway = consumer.lag(partition)
            assertTrue(lagMidway >= 0, "lag must be non-negative, got $lagMidway")

            // Wait for all records to be processed and committed
            assertTrue(processedLatch.await(5, TimeUnit.SECONDS), "timed out processing all records")
            Thread.sleep(300) // allow commit to flush
            val lagAfter = consumer.lag(partition)
            assertEquals(0L, lagAfter, "lag must be 0 after all messages processed and committed")
            consumer.shutdown()
        }
    }

    @Nested
    inner class ExactlyOnceTest {

        @Test
        fun `sendOffsetsToTransaction completes without error and output is committed`() {
            val inputTopic = topic(bootstrapServers)
            val outputTopic = topic(bootstrapServers)
            val groupId = groupId()
            val txId = "test-eos-${groupId.take(8)}"
            val messages = listOf("a", "b", "c")
            publishAt(bootstrapServers, inputTopic, messages)

            val producer = Prozess.producer<String>(
                config = ProducerConfig(
                    bootstrapServers = bootstrapServers,
                    topic = Topic(outputTopic),
                    transactional = TransactionalConfig.Enabled(txId),
                ),
                serializer = { it.toByteArray() },
            )
            producer.initTransactions()

            // Consume input and produce transactionally to output
            val processedLatch = CountDownLatch(messages.size)
            val consumer = stringConsumer(
                ConsumerConfig(bootstrapServers, groupId, setOf(inputTopic)),
            ) { _, _, msg ->
                producer.beginTransaction()
                producer.send("transformed-$msg")
                // sendOffsetsToTransaction with a stub GroupMember — verifies the API path completes
                // without error (Kafka accepts syntactically valid calls even if generationId is stale)
                producer.sendOffsetsToTransaction(
                    emptyMap(),
                    GroupMember(groupId = groupId, generationId = -1, memberId = ""),
                )
                producer.commitTransaction()
                processedLatch.countDown()
            }
            consumer.start(from = StartOffset.Earliest)
            assertTrue(processedLatch.await(10, TimeUnit.SECONDS), "timed out processing input messages")
            consumer.shutdown()
            producer.close()

            // All transformed messages must be visible under read_committed
            val outputReceived = ConcurrentLinkedQueue<String>()
            val outputLatch = CountDownLatch(messages.size)
            val outputConsumer = stringConsumer(
                ConsumerConfig(
                    bootstrapServers = bootstrapServers,
                    groupId = groupId(),
                    topics = setOf(outputTopic),
                    isolationLevel = ConsumerConfig.IsolationLevel.ReadCommitted,
                ),
            ) { _, _, msg -> outputReceived.add(msg); outputLatch.countDown() }
            outputConsumer.start(from = StartOffset.Earliest)
            assertTrue(outputLatch.await(5, TimeUnit.SECONDS), "timed out reading output topic")
            outputConsumer.shutdown()

            assertEquals(messages.map { "transformed-$it" }.sorted(), outputReceived.sorted().toList())
        }
    }
}
