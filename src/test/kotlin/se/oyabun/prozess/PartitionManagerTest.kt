package se.oyabun.prozess

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

@Timeout(value = 5, unit = TimeUnit.SECONDS)
@OptIn(ExperimentalAtomicApi::class)
class PartitionManagerTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)
    private val p1 = Partition(1, topic)
    private val p2 = Partition(2, topic)
    private val log = Logging.logger { }

    @Test
    fun `onPartitionsRevoked commits processed offsets via context and removes from assignments`() {
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            client = FakeKafkaClient(),
            assignments = { setOf(p0, p1) },
            instanceId = "test",
            log = log,
        )
        runBlocking {
            committer.markProcessed(Position(p0, 5L))
            committer.markProcessed(Position(p1, 10L))
        }

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler()
        handler.onPartitionsAssigned(mockContext(), setOf(p0, p1))

        handler.onPartitionsRevoked(context, setOf(p0), committer.processedOffsets)

        assertEquals(setOf(p1), handler.assignments(), "p0 should be removed from assignments")
        assertEquals(1, committed.size)
        assertEquals(mapOf(p0 to 6L), committed[0], "Should commit offset for p0 only")
    }

    @Test
    fun `onPartitionsRevoked with multiple revoked partitions commits each`() {
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            client = FakeKafkaClient(),
            assignments = { setOf(p0, p1, p2) },
            instanceId = "test",
            log = log,
        )
        runBlocking {
            committer.markProcessed(Position(p0, 5L))
            committer.markProcessed(Position(p1, 10L))
            committer.markProcessed(Position(p2, 15L))
        }

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler()
        handler.onPartitionsAssigned(mockContext(), setOf(p0, p1, p2))
        handler.onPartitionsRevoked(context, setOf(p0, p2), committer.processedOffsets)

        assertEquals(setOf(p1), handler.assignments())
        assertEquals(1, committed.size)
        assertEquals(mapOf(p0 to 6L, p2 to 16L), committed[0])
    }

    @Test
    fun `onPartitionsAssigned adds partitions to assignments`() {
        val handler = handler()

        handler.onPartitionsAssigned(mockContext(), setOf(p0, p1))

        assertEquals(setOf(p0, p1), handler.assignments())
    }

    @Test
    fun `onPartitionsAssigned adds to existing assignments`() {
        val handler = handler()
        handler.onPartitionsAssigned(mockContext(), setOf(p0))

        handler.onPartitionsAssigned(mockContext(), setOf(p1))

        assertEquals(setOf(p0, p1), handler.assignments())
    }

    @Test
    fun `onPartitionsAssigned applies pending seeks for assigned partitions`() {
        val pendingSeeks = AtomicReference<Offsets>(mapOf(p0 to 100L, p1 to 200L, p2 to 300L))
        val seeks = mutableMapOf<Partition, Long>()
        val context = mockContext(onSeek = { partition, offset -> seeks[partition] = offset })

        val handler = handler(
            pendingSeeks = pendingSeeks,
        )
        handler.onPartitionsAssigned(context, setOf(p0, p1))

        assertEquals(setOf(p0, p1), handler.assignments())
        assertEquals(mapOf(p0 to 100L, p1 to 200L), seeks, "Should seek only assigned partitions")
        assertTrue(pendingSeeks.load().isEmpty(), "pendingSeeks should be cleared")
    }

    @Test
    fun `onPartitionsAssigned pauses if consumer is paused`() {
        val pausedPartitions = mutableSetOf<Partition>()
        val context = mockContext(onPause = { pausedPartitions.addAll(it) })

        val handler = handler(paused = { true })
        handler.onPartitionsAssigned(context, setOf(p0, p1))

        assertEquals(setOf(p0, p1), pausedPartitions)
    }

    @Test
    fun `onPartitionsAssigned does not pause if consumer is not paused`() {
        var pauseCalled = false
        val context = mockContext(onPause = { pauseCalled = true })

        val handler = handler(paused = { false })
        handler.onPartitionsAssigned(context, setOf(p0))

        assertEquals(false, pauseCalled)
    }

    @Test
    fun `onPartitionsAssigned seeds committer for past-end partitions`() {
        val ends = AtomicReference<Positions>(setOf(Position(p0, 10L), Position(p1, 100L)))
        val context = mockContext(onPosition = { p -> if (p == p0) 15L else 5L })

        val handler = handler(ends = ends)
        val seeds = handler.onPartitionsAssigned(context, setOf(p0, p1))

        assertEquals(mapOf(p0 to 11L), seeds, "p0 is past end offset (15 > 10), should seed offset+1")
    }

    @Test
    fun `onPartitionsRevoked does not commit when no offsets for revoked partitions`() {
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            client = FakeKafkaClient(),
            assignments = { setOf(p0, p1) },
            instanceId = "test",
            log = log,
        )
        runBlocking { committer.markProcessed(Position(p0, 5L)) }

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler()
        handler.onPartitionsAssigned(mockContext(), setOf(p0, p1))
        handler.onPartitionsRevoked(context, setOf(p1), committer.processedOffsets)

        assertTrue(committed.isEmpty(), "Should not commit for partitions without processed offsets")
        assertEquals(setOf(p0), handler.assignments(), "p1 should be removed, p0 remains")
    }

    @Test
    fun `onPartitionsRevoked with partial overlap commits only matching offsets`() {
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            client = FakeKafkaClient(),
            assignments = { setOf(p0, p1, p2) },
            instanceId = "test",
            log = log,
        )
        runBlocking {
            committer.markProcessed(Position(p0, 5L))
            committer.markProcessed(Position(p2, 15L))
        }

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler()
        handler.onPartitionsAssigned(mockContext(), setOf(p0, p1, p2))
        handler.onPartitionsRevoked(context, setOf(p0, p1), committer.processedOffsets)

        assertEquals(setOf(p2), handler.assignments(), "p0 and p1 removed")
        assertEquals(1, committed.size, "One commit for partitions with offsets")
        assertEquals(mapOf(p0 to 6L), committed[0], "Only p0 has processed offsets")
    }

    @Test
    fun `onPartitionsAssigned with no pending seeks does not seek`() {
        var seekCalled = false
        val context = mockContext(onSeek = { _, _ -> seekCalled = true })
        val handler = handler()
        handler.onPartitionsAssigned(context, setOf(p0))
        assertEquals(false, seekCalled, "Should not seek when pendingSeeks is empty")
    }

    @Test
    fun `onPartitionsAssigned with pending seeks not overlapping does not seek`() {
        val pendingSeeks = AtomicReference<Offsets>(mapOf(p2 to 100L))
        var seekCalled = false
        val context = mockContext(onSeek = { _, _ -> seekCalled = true })
        val handler = handler(pendingSeeks = pendingSeeks)
        handler.onPartitionsAssigned(context, setOf(p0, p1))
        assertEquals(false, seekCalled, "Should not seek when pendingSeeks has no overlap")
    }

    @Test
    fun `onPartitionsAssigned does not seed when position below end offset`() {
        val ends = AtomicReference<Positions>(setOf(Position(p0, 100L)))
        val context = mockContext(onPosition = { 50L })
        val handler = handler(ends = ends)
        val seeds = handler.onPartitionsAssigned(context, setOf(p0))
        assertTrue(seeds.isEmpty(), "Should not seed when position is below end offset")
    }

    @Test
    fun `onPartitionsAssigned with no ends loaded does not seed`() {
        val context = mockContext()
        val handler = handler()
        val seeds = handler.onPartitionsAssigned(context, setOf(p0))
        assertTrue(seeds.isEmpty(), "Should not seed when ends is empty")
    }

    @Test
    fun `onPartitionsAssigned with empty partition set is a no-op`() {
        val context = mockContext(
            onPause = { fail("should not pause") },
            onSeek = { _, _ -> fail("should not seek") },
        )
        val handler = handler(paused = { true })
        handler.onPartitionsAssigned(mockContext(), setOf(p0))
        handler.onPartitionsAssigned(context, emptySet())
        assertEquals(setOf(p0), handler.assignments(), "Assignments unchanged")
    }

    @Test
    fun `processing then revoke commits processed offsets via context`() {
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            client = FakeKafkaClient(),
            assignments = { setOf(p0, p1) },
            instanceId = "test-concurrent",
            log = log,
        )
        committer.start()
        val handler = handler()
        handler.onPartitionsAssigned(mockContext(), setOf(p0, p1))

        val context = mockContext(onCommit = { committed.add(it) })

        runBlocking {
            committer.markProcessed(Position(p0, 5L))
            committer.markProcessed(Position(p1, 10L))
        }

        handler.onPartitionsRevoked(context, setOf(p0), committer.processedOffsets)

        assertEquals(setOf(p1), handler.assignments(), "p0 should be removed from assignments")
        assertEquals(1, committed.size, "Should have one commit via context")
        val flushCommit = committed[0]
        assertEquals(setOf(p0), flushCommit.keys, "flush should include p0")
        assertEquals(6L, flushCommit[p0], "Flushed offset for p0 should be 6 (5 + 1)")

        runBlocking { committer.stop().await() }
    }

    @Test
    fun `concurrent processing with full revoke commits all offsets via context`() {
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            client = FakeKafkaClient(),
            assignments = { setOf(p0, p1, p2) },
            instanceId = "test-concurrent-full",
            log = log,
        )
        committer.start()
        val handler = handler()
        handler.onPartitionsAssigned(mockContext(), setOf(p0, p1, p2))

        val context = mockContext(onCommit = { committed.add(it) })

        val processingStarted = CountDownLatch(1)
        val processingActive = AtomicBoolean(true)

        runBlocking {
            launch {
                committer.markProcessed(Position(p0, 0L))
                committer.markProcessed(Position(p1, 0L))
                committer.markProcessed(Position(p2, 0L))
                var i = 1
                processingStarted.countDown()
                while (processingActive.get()) {
                    committer.markProcessed(Position(p0, i.toLong()))
                    committer.markProcessed(Position(p1, i.toLong()))
                    committer.markProcessed(Position(p2, i.toLong()))
                    i++
                    kotlinx.coroutines.yield()
                }
            }

            kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
                processingStarted.await(5, TimeUnit.SECONDS)
            }

            handler.onPartitionsRevoked(context, setOf(p0, p1, p2), committer.processedOffsets)

            processingActive.set(false)
        }

        assertTrue(handler.assignments().isEmpty(), "All partitions should be removed")
        assertEquals(1, committed.size, "Should have one commit via context")
        val flushCommit = committed[0]
        assertEquals(setOf(p0, p1, p2), flushCommit.keys,
            "All revoked partitions should be committed")
        assertTrue(flushCommit.values.all { it > 0 },
            "Each revoked partition should have committed offset > 0")

        runBlocking { committer.stop().await() }
    }

    private fun handler(
        pendingSeeks: AtomicReference<Offsets> = AtomicReference(emptyMap()),
        ends: AtomicReference<Positions> = AtomicReference(emptySet()),
        paused: () -> Boolean = { false },
    ) = CoordinatingPartitionManager(
        pendingSeeks = pendingSeeks,
        ends = ends,
        paused = paused,
        instanceId = "test",
        log = log,
    )

    private fun mockContext(
        onSeek: (Partition, Long) -> Unit = { _, _ -> },
        onPause: (Partitions) -> Unit = {},
        onPosition: (Partition) -> Long = { 0L },
        onCommit: (Offsets) -> Unit = {},
    ) = object : RebalanceContext {
        override fun position(partition: Partition): Long = onPosition(partition)
        override fun pause(partitions: Partitions) = onPause(partitions)
        override fun commit(offsets: Offsets) = onCommit(offsets)
        override fun seek(targets: Offsets) = targets.forEach { (p, o) -> onSeek(p, o) }
    }
}
