package se.oyabun.prozess

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Offsets
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Position
import se.oyabun.prozess.Topic
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
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
    fun `flushForPartitions commits pending offsets for specified partitions`() {
        val commitLog = mutableListOf<Offsets>()
        val c = committer(commitLog)
        c.markProcessed(Position(p0, 5L)).block()
        c.markProcessed(Position(p1, 10L)).block()
        c.flushForPartitions(setOf(p0)).block()
        assertEquals(1, commitLog.size)
        assertEquals(mapOf(p0 to 6L), commitLog[0])
    }

    @Test
    fun `flushForPartitions returns empty when no offsets for partitions`() {
        val commitLog = mutableListOf<Offsets>()
        val c = committer(commitLog)
        c.markProcessed(Position(p0, 5L)).block()
        c.flushForPartitions(setOf(p1)).block()
        assertTrue(commitLog.isEmpty())
    }

    @Test
    fun `flushForPartitions commits multiple partitions`() {
        val commitLog = mutableListOf<Offsets>()
        val c = committer(commitLog)
        c.markProcessed(Position(p0, 5L)).block()
        c.markProcessed(Position(p1, 10L)).block()
        c.flushForPartitions(setOf(p0, p1)).block()
        assertEquals(1, commitLog.size)
        assertEquals(mapOf(p0 to 6L, p1 to 11L), commitLog[0])
    }

    @Test
    fun `flushForPartitions propagates commit error`() {
        val c = BufferedCommitter(
            commit = Committer.Commit { Mono.error(RuntimeException("commit failed")) },
            assignments = { setOf(p0) },
            instanceId = "test",
            topicPartitions = setOf(p0),
            log = Logging.logger { },
        )
        c.markProcessed(Position(p0, 5L)).block()
        org.junit.jupiter.api.assertThrows<RuntimeException> {
            c.flushForPartitions(setOf(p0)).block()
        }
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
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val latch = CountDownLatch(1)
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                commitLog.add(offsets)
                latch.countDown()
                Mono.just(offsets)
            },
            assignments = { setOf(p0) },
            instanceId = "test-batch-size",
            topicPartitions = setOf(p0),
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 3,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.markProcessed(Position(p0, 2L)).block()
        c.markProcessed(Position(p0, 3L)).block()
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Expected batch commit within timeout")
        assertFalse(commitLog.isEmpty(), "Expected at least one commit")
        assertEquals(mapOf(p0 to 4L), commitLog.poll())
        c.stop().block()
    }

    @Test
    fun `batch commit by time`() {
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val latch = CountDownLatch(1)
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                commitLog.add(offsets)
                latch.countDown()
                Mono.just(offsets)
            },
            assignments = { setOf(p0) },
            instanceId = "test-batch-time",
            topicPartitions = setOf(p0),
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 100,
            maxBatchTime = 200.milliseconds.toJavaDuration(),
        )
c.start()
        c.markProcessed(Position(p0, 1L)).block()
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Expected time-based batch commit within timeout")
        assertFalse(commitLog.isEmpty(), "Expected at least one commit")
        assertEquals(mapOf(p0 to 2L), commitLog.poll())
        c.stop().block()
    }

    @Test
    fun `batch commit filters out unassigned partitions`() {
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val latch = CountDownLatch(1)
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                commitLog.add(offsets)
                latch.countDown()
                Mono.just(offsets)
            },
            assignments = { setOf(p0) },
            instanceId = "test-filter",
            topicPartitions = setOf(p0, p1),
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 2,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p1, 1L)).block() // p1 not in assignments
        c.markProcessed(Position(p0, 1L)).block() // p0 is assigned
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Expected batch commit within timeout")
        assertFalse(commitLog.isEmpty(), "Expected at least one commit")
        assertEquals(mapOf(p0 to 2L), commitLog.poll()) // only p0 committed
        c.stop().block()
    }

    @Test
    fun `retry on transient commit failure`() {
        val attempts = java.util.concurrent.atomic.AtomicInteger(0)
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val recoveryLatch = CountDownLatch(1)
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                Mono.defer {
                    val n = attempts.incrementAndGet()
                    if (n <= 2) {
                        Mono.error(RuntimeException("commit failure $n"))
                    } else {
                        commitLog.add(offsets)
                        recoveryLatch.countDown()
                        Mono.just(offsets)
                    }
                }
            },
            assignments = { setOf(p0) },
            instanceId = "test-retry",
            topicPartitions = setOf(p0),
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 1,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        assertTrue(recoveryLatch.await(15, TimeUnit.SECONDS), "Expected retry to recover")
        assertEquals(mapOf(p0 to 2L), commitLog.poll())
        assertTrue(attempts.get() >= 3, "Expected at least 3 attempts, got ${attempts.get()}")
        c.stop().block()
    }

    @Test
    fun `stop does not commit unassigned partitions`() {
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                commitLog.add(offsets)
                Mono.just(offsets)
            },
            assignments = { setOf(p0) },
            instanceId = "test-graceful",
            topicPartitions = setOf(p0, p1),
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 100,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.seedOffsets(mapOf(p1 to 50L))
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        assertFalse(commitLog.isEmpty(), "Expected commit after stop")
        assertEquals(mapOf(p0 to 2L), commitLog.poll())
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
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                commitLog.add(offsets)
                Mono.just(offsets)
            },
            assignments = { setOf(p0) },
            instanceId = "test-flush-stop",
            topicPartitions = setOf(p0),
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 100,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.markProcessed(Position(p0, 2L)).block()
        c.stop().block()
        assertFalse(commitLog.isEmpty(), "Expected commit after stop")
        assertEquals(mapOf(p0 to 3L), commitLog.poll())
    }

    @Test
    fun `stop is idempotent`() {
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                commitLog.add(offsets)
                Mono.just(offsets)
            },
            assignments = { setOf(p0) },
            instanceId = "test-idempotent",
            topicPartitions = setOf(p0),
            log = Logging.logger { },
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        c.stop().block()
    }

    @Test
    fun `multiple sequential batches`() {
        val batchCount = java.util.concurrent.atomic.AtomicInteger(0)
        val batchLatch = CountDownLatch(3)
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                commitLog.add(offsets)
                batchCount.incrementAndGet()
                batchLatch.countDown()
                Mono.just(offsets)
            },
            assignments = { setOf(p0) },
            instanceId = "test-seq",
            topicPartitions = setOf(p0),
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 3,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        (1..9).forEach { c.markProcessed(Position(p0, it.toLong())).block() }
        assertTrue(batchLatch.await(2, TimeUnit.SECONDS), "Expected 3 batch commits within timeout")
        assertEquals(3, batchCount.get(), "Expected exactly 3 batches")
        val committed = commitLog.mapNotNull { it[p0] }.sorted()
        assertEquals(listOf(4L, 7L, 10L), committed, "Expected per-batch high-water offsets")
        c.stop().block()
    }

    @Test
    fun `flushForPartitions with empty partition set`() {
        val commitLog = mutableListOf<Offsets>()
        val c = committer(commitLog)
        c.markProcessed(Position(p0, 5L)).block()
        c.flushForPartitions(emptySet()).block()
        assertTrue(commitLog.isEmpty())
    }

    @Test
    fun `multiple flushForPartitions calls accumulate offsets`() {
        val commitLog = mutableListOf<Offsets>()
        val c = committer(commitLog)
        c.markProcessed(Position(p0, 5L)).block()
        c.flushForPartitions(setOf(p0)).block()
        assertEquals(1, commitLog.size)
        assertEquals(mapOf(p0 to 6L), commitLog[0])
        c.markProcessed(Position(p1, 10L)).block()
        c.flushForPartitions(setOf(p0, p1)).block()
        assertEquals(2, commitLog.size)
        assertEquals(mapOf(p0 to 6L, p1 to 11L), commitLog[1])
    }

    @Test
    fun `stop after pipeline naturally completed is safe`() {
        val commitLog = ConcurrentLinkedQueue<Offsets>()
        val c = BufferedCommitter(
            commit = Committer.Commit { offsets ->
                commitLog.add(offsets)
                Mono.just(offsets)
            },
            assignments = { setOf(p0) },
            instanceId = "test-double-stop",
            topicPartitions = setOf(p0),
            log = Logging.logger { },
            bufferSize = 500,
            maxBatchSize = 100,
            maxBatchTime = 5.seconds.toJavaDuration(),
        )
        c.start()
        c.markProcessed(Position(p0, 1L)).block()
        c.stop().block()
        c.stop().block()
        assertFalse(commitLog.isEmpty(), "Expected commit from first stop")
    }

    @Test
    fun `start throws when already running`() {
        val c = committer()
        c.start()
        try {
            assertThrows<se.oyabun.prozess.CommitterAlreadyRunning> { c.start() }
        } finally {
            c.stop().block()
        }
    }

    private fun committer(
        commitLog: MutableList<Offsets> = mutableListOf(),
    ): Committer = BufferedCommitter(
        commit = Committer.Commit { offsets ->
            commitLog.add(offsets)
            Mono.just(offsets)
        },
        assignments = { setOf(p0, p1) },
        instanceId = "test",
        topicPartitions = setOf(p0, p1),
        log = Logging.logger { },
    )
}
