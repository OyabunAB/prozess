package se.oyabun.prozess

import se.oyabun.aelv.take
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.merge
import se.oyabun.aelv.mergeWith
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import se.oyabun.aelv.Verify
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class CommitterTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)
    private val p1 = Partition(1, topic)

    @Test
    fun `markProcessed updates processedOffsets`() {
        val c = committer()
        Verify.that(None.defer<Unit> { c.markProcessed(Position(p0, 5L)) }).completes()
        assertEquals(mapOf(p0 to 6L), c.processedOffsets)
        Verify.that(None.defer<Unit> { c.markProcessed(Position(p1, 10L)) }).completes()
        assertEquals(mapOf(p0 to 6L, p1 to 11L), c.processedOffsets)
    }

    @Test
    fun `markProcessed tracks highest offset per partition`() {
        val c = committer()
        Verify.that(None.defer<Unit> { c.markProcessed(Position(p0, 5L)) }).completes()
        Verify.that(None.defer<Unit> { c.markProcessed(Position(p0, 3L)) }).completes()
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
        val c = committer(maxBatchSize = 3)
        c.start()
        Verify.that(
            c.committedOffsets.take(1).doOnSubscribe {
                c.markProcessed(Position(p0, 1L))
                c.markProcessed(Position(p0, 2L))
                c.markProcessed(Position(p0, 3L))
            }
        )
            .assertNext { assertEquals(mapOf(p0 to 4L), it) }
            .completes()
        Verify.that(c.stop()).completes()
    }

    @Test
    fun `batch commit by time`() {
        val c = committer(maxBatchTime = 200.milliseconds)
        c.start()
        Verify.that(
            c.committedOffsets.take(1).doOnSubscribe { c.markProcessed(Position(p0, 1L)) }
        )
            .assertNext { assertEquals(mapOf(p0 to 2L), it) }
            .completes()
        Verify.that(c.stop()).completes()
    }

    @Test
    fun `batch commit filters out unassigned partitions`() {
        val c = committer(assignments = { setOf(p0) }, maxBatchSize = 2)
        c.start()
        Verify.that(
            c.committedOffsets.take(1).doOnSubscribe {
                c.markProcessed(Position(p1, 1L))
                c.markProcessed(Position(p0, 1L))
            }
        )
            .assertNext { committed ->
                assertEquals(setOf(p0), committed.keys, "only assigned partition committed")
                assertEquals(mapOf(p0 to 2L), committed)
            }
            .completes()
        Verify.that(c.stop()).completes()
    }

    @Test
    fun `retry on transient commit failure`() {
        val fake = FakeKafkaClient()
        fake.failNextCommit()
        fake.failNextCommit()
        val c = committer(client = fake, maxBatchSize = 1)
        c.start()
        Verify.that(
            c.committedOffsets.take(1).doOnSubscribe { c.markProcessed(Position(p0, 1L)) }
        )
            .assertNext { assertEquals(mapOf(p0 to 2L), it) }
            .completes()
        Verify.that(c.stop()).completes()
    }

    @Test
    fun `stop does not commit unassigned partitions`() {
        val c = committer(assignments = { setOf(p0) })
        c.start()
        c.seedOffsets(mapOf(p1 to 50L))
        Verify.that(
            c.committedOffsets.take(1).doOnSubscribe {
                c.markProcessed(Position(p0, 1L))
                c.stop().await()
            }
        )
            .assertNext { committed ->
                assertEquals(setOf(p0), committed.keys, "only assigned partition committed")
                assertEquals(mapOf(p0 to 2L), committed)
            }
            .completes()
    }

    @Test
    fun `positions emits marked positions`() {
        val c = committer()
        Verify.that(
            c.positions.take(2).mergeWith(
                None.defer<Position> {
                    c.markProcessed(Position(p0, 5L))
                    c.markProcessed(Position(p1, 10L))
                }.toMany().delaySubscription(10.milliseconds)
            )
        )
            .emitsNext(Position(p0, 5L), Position(p1, 10L))
            .completes()
    }

    @Test
    fun `stop completes all pending commits`() {
        val fake = FakeKafkaClient()
        val c = committer(client = fake)
        c.start()
        Verify.that(None.defer<Unit> {
            c.markProcessed(Position(p0, 1L))
            c.markProcessed(Position(p0, 2L))
            c.stop().await()
        }).completes()
        assertTrue(fake.commits.isNotEmpty(), "expected at least one commit")
        assertEquals(3L, fake.commits.map { it.first[p0]!! }.max(), "highest committed offset must be 3")
    }

    @Test
    fun `stop is idempotent`() {
        val c = committer()
        c.start()
        Verify.that(None.defer<Unit> { c.markProcessed(Position(p0, 1L)); c.stop().await() }).completes()
        Verify.that(c.stop()).completes()
    }

    @Test
    fun `multiple sequential batches`() {
        val c = committer(maxBatchSize = 3)
        c.start()
        val commits = mutableListOf<Long>()
        Verify.that(
            c.committedOffsets.take(3).doOnSubscribe {
                (1..9).forEach { c.markProcessed(Position(p0, it.toLong())) }
            }.doOnNext { commits.add(it[p0]!!) }
        )
            .emitsCount(3)
            .completes()
        Verify.that(c.stop()).completes()
        assertTrue(commits.isNotEmpty(), "Expected at least one commit")
        assertEquals(commits.sorted(), commits, "Commits must be monotonically increasing")
        assertEquals(10L, commits.last(), "Final committed offset must be 10")
    }

    @Test
    fun `stop after pipeline naturally completed is safe`() {
        val c = committer()
        c.start()
        Verify.that(None.defer<Unit> { c.markProcessed(Position(p0, 1L)); c.stop().await() }).completes()
        Verify.that(c.stop()).completes()
    }

    @Test
    fun `processedOffsets is only mutated by markProcessed and seedOffsets`() {
        val c = committer()
        assertEquals(emptyMap(), c.processedOffsets)
        c.seedOffsets(mapOf(p0 to 42L))
        assertEquals(mapOf(p0 to 42L), c.processedOffsets)
        Verify.that(None.defer<Unit> { c.markProcessed(Position(p0, 50L)) }).completes()
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
        c.start()
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
        Verify.that(c.stop()).completes()
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
    }

    @Test
    fun `seedOffsets does not overwrite higher offset already recorded by markProcessed`() {
        val c = committer()
        Verify.that(None.defer<Unit> { c.markProcessed(Position(p0, 50L)) }).completes()
        assertEquals(mapOf(p0 to 51L), c.processedOffsets)
        c.seedOffsets(mapOf(p0 to 10L))
        assertEquals(mapOf(p0 to 51L), c.processedOffsets, "seedOffsets must not regress a higher processed offset")
    }

    @Test
    fun `start throws when already running`() {
        val c = committer()
        c.start()
        assertThrows<CommitterAlreadyRunning> { c.start() }
        Verify.that(c.stop()).completes()
    }

    private fun committer(
        client: KafkaClient = FakeKafkaClient(),
        assignments: () -> Set<Partition> = { setOf(p0, p1) },
        maxBatchSize: Int = 100,
        maxBatchTime: kotlin.time.Duration = 500.milliseconds,
    ): Committer = BufferedCommitter(
        client = client,
        assignments = assignments,
        instanceId = "test",
        log = Logging.logger { },
        bufferSize = 500,
        maxBatchSize = maxBatchSize,
        maxBatchTime = maxBatchTime,
    )
}
