package se.oyabun.prozess

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import reactor.test.StepVerifier
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Offsets
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Position
import se.oyabun.prozess.Topic
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class CommitterTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)
    private val p1 = Partition(1, topic)

    @Test
    fun `markProcessed updates processedOffsets`() {
        val c = committer()
        c.markProcessed(Position(p0, 5L)).block()
        assertEquals(mapOf(p0 to 6L), c.processedOffsets)
        c.markProcessed(Position(p1, 10L)).block()
        assertEquals(mapOf(p0 to 6L, p1 to 11L), c.processedOffsets)
    }

    @Test
    fun `markProcessed tracks highest offset per partition`() {
        val c = committer()
        c.markProcessed(Position(p0, 5L)).block()
        c.markProcessed(Position(p0, 3L)).block()
        assertEquals(mapOf(p0 to 6L), c.processedOffsets)
    }

    @Test
    fun `seedOffsets populates processedOffsets`() {
        val c = committer()
        c.seedOffsets(mapOf(p0 to 100L))
        assertEquals(mapOf(p0 to 100L), c.processedOffsets)
        c.seedOffsets(mapOf(p1 to 200L))
        assertEquals(mapOf(p0 to 100L, p1 to 200L), c.processedOffsets)
    }

    @Test
    fun `batch commit by size`() {
        val fake = FakeKafkaClient()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-batch-size",
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 3,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.markProcessed(Position(p0, 2L)).block()
        c.markProcessed(Position(p0, 3L)).block()
        c.stop().block()
        assertFalse(fake.commits.isEmpty(), "Expected at least one commit")
        assertEquals(mapOf(p0 to 4L), fake.commits.peek().first)
    }

    @Test
    fun `batch commit by time`() {
        val fake = FakeKafkaClient()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-batch-time",
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 100,
            maxBatchTime = 200.milliseconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        assertFalse(fake.commits.isEmpty(), "Expected at least one commit")
        assertEquals(mapOf(p0 to 2L), fake.commits.peek().first)
    }

    @Test
    fun `batch commit filters out unassigned partitions`() {
        val fake = FakeKafkaClient()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-filter",
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 2,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p1, 1L)).block()
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        assertFalse(fake.commits.isEmpty(), "Expected at least one commit")
        assertEquals(mapOf(p0 to 2L), fake.commits.peek().first)
    }

    @Test
    fun `retry on transient commit failure`() {
        val fake = FakeKafkaClient()
        fake.failNextCommit()
        fake.failNextCommit()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-retry",
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 1,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        assertFalse(fake.commits.isEmpty(), "Expected retry to recover")
        assertEquals(mapOf(p0 to 2L), fake.commits.peek().first)
    }

    @Test
    fun `stop does not commit unassigned partitions`() {
        val fake = FakeKafkaClient()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-graceful",
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 100,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.seedOffsets(mapOf(p1 to 50L))
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        assertFalse(fake.commits.isEmpty(), "Expected commit after stop")
        assertEquals(mapOf(p0 to 2L), fake.commits.peek().first)
    }

    @Test
    fun `positions flux emits marked positions`() {
        val c = committer()
        val verifier = StepVerifier.create(c.positions)
            .expectSubscription()
            .then { c.markProcessed(Position(p0, 5L)).block() }
            .expectNext(Position(p0, 5L))
            .then { c.markProcessed(Position(p1, 10L)).block() }
            .expectNext(Position(p1, 10L))
            .thenCancel()
            .verify()
    }

    @Test
    fun `stop completes all pending commits`() {
        val fake = FakeKafkaClient()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-flush-stop",
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 100,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.markProcessed(Position(p0, 2L)).block()
        c.stop().block()
        assertFalse(fake.commits.isEmpty(), "Expected commit after stop")
        assertEquals(mapOf(p0 to 3L), fake.commits.peek().first)
    }

    @Test
    fun `stop is idempotent`() {
        val fake = FakeKafkaClient()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-idempotent",
            log = Logging.logger { },
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        c.stop().block()
    }

    @Test
    fun `multiple sequential batches`() {
        val fake = FakeKafkaClient()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-seq",
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 3,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        (1..9).forEach { c.markProcessed(Position(p0, it.toLong())).block() }
        c.stop().block()
        val committed = fake.commits.mapNotNull { it.first[p0] }.sorted()
        assertEquals(listOf(4L, 7L, 10L), committed, "Expected per-batch high-water offsets")
    }

    @Test
    fun `stop after pipeline naturally completed is safe`() {
        val fake = FakeKafkaClient()
        val c = BufferedCommitter(
            client = fake,
            assignments = { setOf(p0) },
            instanceId = "test-double-stop",
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 100,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        c.stop().block()
        assertFalse(fake.commits.isEmpty(), "Expected commit from first stop")
    }

    @Test
    fun `processedOffsets is only mutated by markProcessed and seedOffsets`() {
        val c = BufferedCommitter(
            client = FakeKafkaClient(),
            assignments = { setOf(p0) },
            instanceId = "test-invariant",
            log = Logging.logger { },
        )
        assertEquals(emptyMap(), c.processedOffsets)

        c.seedOffsets(mapOf(p0 to 42L))
        assertEquals(mapOf(p0 to 42L), c.processedOffsets)

        c.markProcessed(Position(p0, 50L)).block()
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)

        c.start()
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
        c.stop().block()
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
    }

    @Test
    fun `start throws when already running`() {
        val c = committer()
        c.start()
        try {
            assertThrows<CommitterAlreadyRunning> { c.start() }
        } finally {
            c.stop().block()
        }
    }

    private fun committer(): Committer = BufferedCommitter(
        client = FakeKafkaClient(),
        assignments = { setOf(p0, p1) },
        instanceId = "test",
        log = Logging.logger { },
    )
}
