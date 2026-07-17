package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import se.oyabun.aelv.first
import se.oyabun.aelv.merge
import se.oyabun.aelv.take
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.toMany
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import se.oyabun.aelv.Verify
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Timeout(value = 5, unit = TimeUnit.SECONDS)
class CommitterTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)
    private val p1 = Partition(1, topic)

    @Test
    fun `markProcessed updates processedOffsets`() {
        val c = committer()
        runBlocking { c.markProcessed(Position(p0, 5L)) }
        assertEquals(mapOf(p0 to 6L), c.processedOffsets)
        runBlocking { c.markProcessed(Position(p1, 10L)) }
        assertEquals(mapOf(p0 to 6L, p1 to 11L), c.processedOffsets)
    }

    @Test
    fun `markProcessed tracks highest offset per partition`() {
        val c = committer()
        runBlocking { c.markProcessed(Position(p0, 5L)) }
        runBlocking { c.markProcessed(Position(p0, 3L)) }
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
            maxBatchTime = 5.seconds,
        )
        c.start()
        val committed = runBlocking {
            c.markProcessed(Position(p0, 1L))
            c.markProcessed(Position(p0, 2L))
            c.markProcessed(Position(p0, 3L))
            val offsets = c.committedOffsets.first()
            c.stop().await()
            offsets
        }
        assertTrue(committed.isRight(), "Expected a committed offsets event")
        assertEquals(mapOf(p0 to 4L), committed.rightOrNull())
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
            maxBatchTime = 200.milliseconds,
        )
        c.start()
        runBlocking {
            c.markProcessed(Position(p0, 1L))
            c.stop().await()
        }
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
            maxBatchTime = 5.seconds,
        )
        c.start()
        runBlocking {
            c.markProcessed(Position(p1, 1L))
            c.markProcessed(Position(p0, 1L))
            c.stop().await()
        }
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
            maxBatchTime = 5.seconds,
        )
        c.start()
        runBlocking {
            c.markProcessed(Position(p0, 1L))
            c.stop().await()
        }
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
            maxBatchTime = 5.seconds,
        )
        c.start()
        c.seedOffsets(mapOf(p1 to 50L))
        runBlocking {
            c.markProcessed(Position(p0, 1L))
            c.stop().await()
        }
        assertFalse(fake.commits.isEmpty(), "Expected commit after stop")
        assertEquals(mapOf(p0 to 2L), fake.commits.peek().first)
    }

    @Test
    fun `positions emits marked positions`() {
        val c = committer()
        val driver: Many<Position> = None.defer<Position> {
            c.markProcessed(Position(p0, 5L))
            c.markProcessed(Position(p1, 10L))
        }.toMany()
        Verify.that(merge(c.positions.take(2), driver))
            .emitsNext(Position(p0, 5L), Position(p1, 10L))
            .thenCancels()
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
            maxBatchTime = 5.seconds,
        )
        c.start()
        runBlocking {
            c.markProcessed(Position(p0, 1L))
            c.markProcessed(Position(p0, 2L))
            c.stop().await()
        }
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
        runBlocking {
            c.markProcessed(Position(p0, 1L))
            c.stop().await()
            c.stop().await()
        }
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
            maxBatchTime = 5.seconds,
        )
        c.start()
        runBlocking {
            (1..9).forEach { c.markProcessed(Position(p0, it.toLong())) }
            c.stop().await()
        }
        val committed = fake.commits.mapNotNull { it.first[p0] }
        assertFalse(committed.isEmpty(), "Expected at least one commit")
        assertEquals(10L, committed.last(), "Expected final committed offset to be 10")
        assertEquals(committed.sorted(), committed, "Expected commits to be monotonically increasing")
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
            maxBatchTime = 5.seconds,
        )
        c.start()
        runBlocking {
            c.markProcessed(Position(p0, 1L))
            c.stop().await()
            c.stop().await()
        }
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

        runBlocking { c.markProcessed(Position(p0, 50L)) }
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)

        c.start()
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
        runBlocking { c.stop().await() }
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
    }

    @Test
    fun `seedOffsets does not overwrite higher offset already recorded by markProcessed`() {
        val c = committer()
        runBlocking { c.markProcessed(Position(p0, 50L)) }
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
        c.seedOffsets(mapOf(p0 to 10L))
        assertEquals(mapOf(p0 to 51L), c.processedOffsets, "seedOffsets must not regress a higher processed offset")
    }

    @Test
    fun `start throws when already running`() {
        val c = committer()
        c.start()
        try {
            assertThrows<CommitterAlreadyRunning> { c.start() }
        } finally {
            runBlocking { c.stop().await() }
        }
    }

    private fun committer(): Committer = BufferedCommitter(
        client = FakeKafkaClient(),
        assignments = { setOf(p0, p1) },
        instanceId = "test",
        log = Logging.logger { },
    )
}
