package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import se.oyabun.aelv.Sink
import se.oyabun.aelv.get
import se.oyabun.aelv.Verify
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@Timeout(value = 5, unit = TimeUnit.SECONDS)
@OptIn(ExperimentalAtomicApi::class)
class FakeKafkaClientTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)
    private val p1 = Partition(1, topic)

    // ---- FakeKafkaClient contract ----

    @Test
    fun `queued poll results returned in order`() {
        val client = FakeKafkaClient()
        val r1 = received(p0, 0L, "a")
        val r2 = received(p0, 1L, "b")

        client.queuePollResults(listOf(r1), listOf(r2), emptyList())

        Verify.that(client.poll(100.milliseconds))
            .emitsNext(listOf(r1))
            .completesNormally()
        Verify.that(client.poll(100.milliseconds))
            .emitsNext(listOf(r2))
            .completesNormally()
    }

    @Test
    fun `empty poll when queue drained`() {
        val client = FakeKafkaClient()

        Verify.that(client.poll(100.milliseconds))
            .emitsNext(emptyList<Received>())
            .completesNormally()
    }

    @Test
    fun `commit records offsets and metadata`() {
        val client = FakeKafkaClient()
        val offsets = mapOf(p0 to 10L)

        runBlocking { client.commit(offsets, "test-meta").get() }

        assertEquals(1, client.commits.size)
        assertEquals(offsets, client.commits.peek().first)
        assertEquals("test-meta", client.commits.peek().second)
    }

    @Test
    fun `subscribe triggers initial partition assign`() {
        val client = FakeKafkaClient()
        client.setPartitionsFor("test", setOf(p0, p1))
        val assigned = mutableListOf<Partitions>()
        val listener = object : RebalanceListener {
            override fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions) {}
            override fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions) { assigned.add(partitions) }
        }

        runBlocking { client.subscribe(setOf("test"), listener).get() }

        assertEquals(setOf("test"), client.subscribedTopics)
        assertEquals(1, assigned.size)
        assertEquals(setOf(p0, p1), assigned[0])
    }

    @Test
    fun `rebalanceContext returns stored positions`() {
        val client = FakeKafkaClient()
        client.setStoredPosition(p0, 42L)

        val ctx = client.rebalanceContext()

        assertEquals(42L, ctx.position(p0))
        assertEquals(0L, ctx.position(p1))
    }

    @Test
    fun `rebalanceContext seek updates positions`() {
        val client = FakeKafkaClient()
        val ctx = client.rebalanceContext()

        ctx.seek(mapOf(p0 to 100L, p1 to 200L))

        assertEquals(100L, client.storedPosition(p0))
        assertEquals(200L, client.storedPosition(p1))
    }

    @Test
    fun `rebalanceContext pause records partitions`() {
        val client = FakeKafkaClient()
        val ctx = client.rebalanceContext()

        ctx.pause(setOf(p0, p1))

        assertEquals(setOf(p0, p1), client.pausedPartitions.peek())
    }

    @Test
    fun `wakeup and close tracked`() {
        val client = FakeKafkaClient()

        client.wakeup()
        runBlocking { client.close().await() }

        assertEquals(1, client.wakeupCount)
        assertTrue(client.closed)
    }

    @Test
    fun `partitionsFor returns configured partitions`() {
        val client = FakeKafkaClient()
        client.setPartitionsFor("test", setOf(p0, p1))

        val result = runBlocking { client.partitionsFor(setOf("test")).get().leftOrNull()!! }

        assertEquals(setOf(p0, p1), result)
    }

    // ---- Component wiring using FakeKafkaClient ----

    @Test
    fun `committer commits through fake client`() {
        val fake = FakeKafkaClient()

        val committer = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test",
            log = Logging.logger { },
            maxBatchSize = 1,
            maxBatchTime = 1.seconds,
        )

        committer.start()
        runBlocking {
            committer.markProcessed(Position(p0, 5L))
            committer.stop().await()
        }

        assertTrue(fake.commits.isNotEmpty(), "expected commit")
        assertEquals(mapOf(p0 to 6L), fake.commits.peek().first)
    }

    @Test
    fun `partition manager uses fake rebalance context`() {
        val fake = FakeKafkaClient()
        val pm = CoordinatingPartitionManager(
            pendingSeeks = kotlin.concurrent.atomics.AtomicReference(emptyMap()),
            ends = kotlin.concurrent.atomics.AtomicReference(emptySet()),
            paused = { false },
            instanceId = "test",
            log = Logging.logger { },
        )

        pm.onPartitionsAssigned(fake.rebalanceContext(), setOf(p0))

        assertEquals(setOf(p0), pm.assignments())
    }

    @Test
    fun `partition manager seek via fake rebalance context`() {
        val fake = FakeKafkaClient()
        val pendingSeeks = kotlin.concurrent.atomics.AtomicReference<Offsets>(mapOf(p0 to 100L))
        val pm = CoordinatingPartitionManager(
            pendingSeeks = pendingSeeks,
            ends = kotlin.concurrent.atomics.AtomicReference(emptySet()),
            paused = { false },
            instanceId = "test",
            log = Logging.logger { },
        )

        pm.onPartitionsAssigned(fake.rebalanceContext(), setOf(p0))

        assertEquals(100L, fake.storedPosition(p0))
        assertTrue(pendingSeeks.load().isEmpty())
    }

    @Test
    fun `partition manager pause via fake rebalance context`() {
        val fake = FakeKafkaClient()
        val pm = CoordinatingPartitionManager(
            pendingSeeks = kotlin.concurrent.atomics.AtomicReference(emptyMap()),
            ends = kotlin.concurrent.atomics.AtomicReference(emptySet()),
            paused = { true },
            instanceId = "test",
            log = Logging.logger { },
        )

        pm.onPartitionsAssigned(fake.rebalanceContext(), setOf(p0))

        assertTrue(fake.pausedPartitions.isNotEmpty())
        assertEquals(setOf(p0), fake.pausedPartitions.peek())
    }

    // ---- StreamingConsumer shutdown integration ----

    @Test
    fun `consumer shutdown triggers close and wakeup`() {
        val fake = FakeKafkaClient()
        fake.setPartitionsFor("test", setOf(p0))

        val processor = DefaultProcessor.each<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> Prozess.DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> Prozess.DeserializationResult.Tombstone
                }
            },
            handler = {},
        )
        val consumer = StreamingConsumer(
            config = ConsumerConfig("bootstrap", "group", "test"),
            processor = processor,
            client = fake,
        )

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        consumer.shutdown()

        assertTrue(consumer.isDisposed)
        assertTrue(fake.closed)
        assertTrue(fake.wakeupCount >= 1)
    }

    @Test
    fun `consumer position delegates to fake`() {
        val fake = FakeKafkaClient()
        fake.setPartitionsFor("test", setOf(p0))
        fake.setPositionOfResult { 50L }
        fake.setEndOffsetOfResult { 100L }

        val processor = DefaultProcessor.each<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> Prozess.DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> Prozess.DeserializationResult.Tombstone
                }
            },
            handler = {},
        )
        val consumer = StreamingConsumer(
            config = ConsumerConfig("bootstrap", "group", "test"),
            processor = processor,
            client = fake,
        )

        consumer.start(StartOffset.Latest, EndOffset.Continuous)

        assertEquals(50L, consumer.position(p0))
        assertEquals(50L, consumer.lag(p0))

        consumer.shutdown()
    }

    // ---- PRINCIPLE: NEVER LOSE ----

    @Test
    fun `committed offset equals last processed offset plus one`() {
        val fake = FakeKafkaClient()
        val committer = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test",
            log = Logging.logger { },
            maxBatchSize = 1,
            maxBatchTime = 5.seconds,
        )

        committer.start()
        runBlocking {
            committer.markProcessed(Position(p0, 7L))
            committer.stop().await()
        }

        assertEquals(1, fake.commits.size)
        assertEquals(mapOf(p0 to 8L), fake.commits.peek().first,
            "offset must be lastProcessed + 1")
    }

    @Test
    fun `revoke commits only processed offsets via context`() {
        val fake = FakeKafkaClient()
        val pm = CoordinatingPartitionManager(
            pendingSeeks = kotlin.concurrent.atomics.AtomicReference(emptyMap()),
            ends = kotlin.concurrent.atomics.AtomicReference(emptySet()),
            paused = { false },
            instanceId = "test",
            log = Logging.logger { },
        )
        pm.onPartitionsAssigned(fake.rebalanceContext(), setOf(p0, p1))

        pm.onPartitionsRevoked(
            fake.rebalanceContext(),
            setOf(p0),
            mapOf(p0 to 6L, p1 to 11L),
        )

        val revokeCommits = fake.commits.filter { it.second == "rebalance-context" }
        assertTrue(revokeCommits.isNotEmpty(), "expected commit via rebalance context")
        assertEquals(mapOf(p0 to 6L), revokeCommits[0].first,
            "only revoked partition p0 should be committed")
    }

    // ---- PRINCIPLE: NEVER SKIP ----

    @Test
    fun `same partition offsets committed in order`() {
        val fake = FakeKafkaClient()

        val committer = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test",
            log = Logging.logger { },
            maxBatchSize = 1,
            maxBatchTime = 5.seconds,
        )

        committer.start()
        runBlocking {
            committer.markProcessed(Position(p0, 1L))
            committer.markProcessed(Position(p0, 2L))
            committer.markProcessed(Position(p0, 3L))
            committer.stop().await()
        }

        val offsets = fake.commits.map { (offsets, _) -> offsets[p0]!! }
        assertEquals(listOf(2L, 3L, 4L), offsets,
            "each batch commits the high-water mark + 1 in order")
    }

    // ---- PRINCIPLE: NEVER DIE ----

    @Test
    fun `commit retries on failure and preserves correct offset`() {
        val fake = FakeKafkaClient()
        fake.failNextCommit()
        fake.failNextCommit()

        val committer = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test",
            log = Logging.logger { },
            maxBatchSize = 1,
            maxBatchTime = 5.seconds,
        )

        committer.start()
        runBlocking {
            committer.markProcessed(Position(p0, 5L))
            committer.stop().await()
        }

        assertTrue(fake.commits.isNotEmpty(), "commit must eventually succeed after retries")
        assertEquals(mapOf(p0 to 6L), fake.commits.peek().first,
            "correct offset despite retries")
    }

    @Test
    fun `poller stays running when poll errors`() {
        val fake = FakeKafkaClient()
        val buffer = InMemoryReceivedBuffer()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val shutdownSink = Sink.broadcast<Unit>()
        val doneSink = Sink.broadcast<Unit>()

        fake.failNextPoll(RuntimeException("transient error"))

        val poller = BufferedPoller(
            client = fake,
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test",
            pollInterval = 50.milliseconds,
            shutdownSink = shutdownSink,
            doneSink = doneSink,
            log = Logging.logger { },
        )

        poller.start()
        assertTrue(poller.isRunning, "poller must stay running despite poll error")
        shutdownSink.complete()
        runBlocking { poller.stop().await() }
    }

    @Test
    fun `shutdown is safe during active processing`() {
        val fake = FakeKafkaClient()
        fake.setPartitionsFor("test", setOf(p0))

        val processor = DefaultProcessor.each<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> Prozess.DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> Prozess.DeserializationResult.Tombstone
                }
            },
            handler = {},
        )
        val consumer = StreamingConsumer(
            config = ConsumerConfig("bootstrap", "group", "test"),
            processor = processor,
            client = fake,
        )

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        consumer.shutdown()

        assertTrue(consumer.isDisposed, "consumer must become disposed after shutdown")
        assertTrue(fake.closed, "client must be closed during shutdown")
    }

    // ---- helpers ----

    private fun received(partition: Partition, offset: Long, message: String) =
        Received(ReceivedKey.Value("k-$offset"), ReceivedMessage.Data(message.toByteArray()), Position(partition, offset))

    private fun received(partition: Partition, offset: Long): Received =
        Received(ReceivedKey.Value("k-$offset"), ReceivedMessage.Data(ByteArray(0)), Position(partition, offset))
}
