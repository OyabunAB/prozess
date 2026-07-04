package se.oyabun.prozess

import org.junit.jupiter.api.Test
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Offsets
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Partitions
import se.oyabun.prozess.Position
import se.oyabun.prozess.Topic
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.update
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

@OptIn(ExperimentalAtomicApi::class)
class RebalanceHandlerTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)
    private val p1 = Partition(1, topic)
    private val p2 = Partition(2, topic)
    private val log = Logging.logger { }

    @Test
    fun `onPartitionsRevoked commits processed offsets via context and removes from assignments`() {
        val assignments = AtomicReference<Partitions>(setOf(p0, p1))
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            commit = Committer.Commit { Mono.just(it) },
            assignments = { setOf(p0, p1) },
            instanceId = "test",
            topicPartitions = setOf(p0, p1),
            log = log,
        )
        committer.markProcessed(Position(p0, 5L)).block()
        committer.markProcessed(Position(p1, 10L)).block()

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler(assignments = assignments, committerRef = { committer })

        handler.onPartitionsRevoked(context, setOf(p0))

        assertEquals(setOf(p1), assignments.load(), "p0 should be removed from assignments")
        assertEquals(1, committed.size)
        assertEquals(mapOf(p0 to 6L), committed[0], "Should commit offset for p0 only")
    }

    @Test
    fun `onPartitionsRevoked with multiple revoked partitions commits each`() {
        val assignments = AtomicReference<Partitions>(setOf(p0, p1, p2))
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            commit = Committer.Commit { Mono.just(it) },
            assignments = { setOf(p0, p1, p2) },
            instanceId = "test",
            topicPartitions = setOf(p0, p1, p2),
            log = log,
        )
        committer.markProcessed(Position(p0, 5L)).block()
        committer.markProcessed(Position(p1, 10L)).block()
        committer.markProcessed(Position(p2, 15L)).block()

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler(assignments = assignments, committerRef = { committer })
        handler.onPartitionsRevoked(context, setOf(p0, p2))

        assertEquals(setOf(p1), assignments.load())
        assertEquals(1, committed.size)
        assertEquals(mapOf(p0 to 6L, p2 to 16L), committed[0])
    }

    @Test
    fun `onPartitionsAssigned adds partitions to assignments`() {
        val assignments = AtomicReference<Partitions>(emptySet())
        val handler = handler(assignments = assignments)

        handler.onPartitionsAssigned(mockContext(), setOf(p0, p1))

        assertEquals(setOf(p0, p1), assignments.load())
    }

    @Test
    fun `onPartitionsAssigned adds to existing assignments`() {
        val assignments = AtomicReference<Partitions>(setOf(p0))
        val handler = handler(assignments = assignments)

        handler.onPartitionsAssigned(mockContext(), setOf(p1))

        assertEquals(setOf(p0, p1), assignments.load())
    }

    @Test
    fun `onPartitionsAssigned applies pending seeks for assigned partitions`() {
        val assignments = AtomicReference<Partitions>(emptySet())
        val pendingSeeks = AtomicReference<Offsets>(mapOf(p0 to 100L, p1 to 200L, p2 to 300L))
        val seeks = mutableMapOf<Partition, Long>()
        val context = mockContext(onSeek = { partition, offset -> seeks[partition] = offset })

        val handler = handler(
            assignments = assignments,
            pendingSeeks = pendingSeeks,
        )
        handler.onPartitionsAssigned(context, setOf(p0, p1))

        assertEquals(setOf(p0, p1), assignments.load())
        assertEquals(mapOf(p0 to 100L, p1 to 200L), seeks, "Should seek only assigned partitions")
        assertTrue(pendingSeeks.load().isEmpty(), "pendingSeeks should be cleared")
    }

    @Test
    fun `onPartitionsAssigned pauses if consumer is paused`() {
        val assignments = AtomicReference<Partitions>(emptySet())
        val pausedPartitions = mutableSetOf<Partition>()
        val context = mockContext(onPause = { pausedPartitions.addAll(it) })

        val handler = handler(assignments = assignments, paused = { true })
        handler.onPartitionsAssigned(context, setOf(p0, p1))

        assertEquals(setOf(p0, p1), pausedPartitions)
    }

    @Test
    fun `onPartitionsAssigned does not pause if consumer is not paused`() {
        val assignments = AtomicReference<Partitions>(emptySet())
        var pauseCalled = false
        val context = mockContext(onPause = { pauseCalled = true })

        val handler = handler(assignments = assignments, paused = { false })
        handler.onPartitionsAssigned(context, setOf(p0))

        assertEquals(false, pauseCalled)
    }

    @Test
    fun `onPartitionsAssigned seeds committer for past-end partitions`() {
        val assignments = AtomicReference<Partitions>(emptySet())
        val ends = AtomicReference<Positions>(setOf(Position(p0, 10L), Position(p1, 100L)))
        val seeds = mutableMapOf<Partition, Long>()
        val context = mockContext(onPosition = { p -> if (p == p0) 15L else 5L })

        val handler = handler(
            assignments = assignments,
            ends = ends,
            committerRef = { mockCommitter(onSeed = { seeds.putAll(it) }) },
        )
        handler.onPartitionsAssigned(context, setOf(p0, p1))

        assertEquals(mapOf(p0 to 11L), seeds, "p0 is past end offset (15 > 10), should seed offset+1")
    }

    @Test
    fun `onPartitionsRevoked does not commit when no offsets for revoked partitions`() {
        val assignments = AtomicReference<Partitions>(setOf(p0, p1))
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            commit = Committer.Commit { Mono.just(it) },
            assignments = { setOf(p0, p1) },
            instanceId = "test",
            topicPartitions = setOf(p0, p1),
            log = log,
        )
        committer.markProcessed(Position(p0, 5L)).block()

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler(assignments = assignments, committerRef = { committer })
        handler.onPartitionsRevoked(context, setOf(p1))

        assertTrue(committed.isEmpty(), "Should not commit for partitions without processed offsets")
        assertEquals(setOf(p0), assignments.load(), "p1 should be removed, p0 remains")
    }

    @Test
    fun `onPartitionsRevoked with partial overlap commits only matching offsets`() {
        val assignments = AtomicReference<Partitions>(setOf(p0, p1, p2))
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            commit = Committer.Commit { Mono.just(it) },
            assignments = { setOf(p0, p1, p2) },
            instanceId = "test",
            topicPartitions = setOf(p0, p1, p2),
            log = log,
        )
        committer.markProcessed(Position(p0, 5L)).block()
        committer.markProcessed(Position(p2, 15L)).block()

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler(assignments = assignments, committerRef = { committer })
        handler.onPartitionsRevoked(context, setOf(p0, p1))

        assertEquals(setOf(p2), assignments.load(), "p0 and p1 removed")
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
        var seedCalled = false
        val context = mockContext(onPosition = { 50L })
        val handler = handler(
            ends = ends,
            committerRef = { mockCommitter(onSeed = { seedCalled = true }) },
        )
        handler.onPartitionsAssigned(context, setOf(p0))
        assertEquals(false, seedCalled, "Should not seed when position is below end offset")
    }

    @Test
    fun `onPartitionsAssigned with no ends loaded does not seed`() {
        var seedCalled = false
        val context = mockContext()
        val handler = handler(
            committerRef = { mockCommitter(onSeed = { seedCalled = true }) },
        )
        handler.onPartitionsAssigned(context, setOf(p0))
        assertEquals(false, seedCalled, "Should not seed when ends is empty")
    }

    @Test
    fun `onPartitionsAssigned with empty partition set is a no-op`() {
        val assignments = AtomicReference<Partitions>(setOf(p0))
        val context = mockContext(
            onPause = { fail("should not pause") },
            onSeek = { _, _ -> fail("should not seek") },
        )
        val handler = handler(assignments = assignments, paused = { true })
        handler.onPartitionsAssigned(context, emptySet())
        assertEquals(setOf(p0), assignments.load(), "Assignments unchanged")
    }

    @Test
    fun `processing then revoke commits processed offsets via context`() {
        val assignments = AtomicReference<Partitions>(setOf(p0, p1))
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            commit = Committer.Commit { Mono.just(it) },
            assignments = { assignments.load() },
            instanceId = "test-concurrent",
            topicPartitions = setOf(p0, p1),
            log = log,
        )
        committer.start()

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler(
            assignments = assignments,
            committerRef = { committer },
        )

        committer.markProcessed(Position(p0, 5L)).block()
        committer.markProcessed(Position(p1, 10L)).block()

        handler.onPartitionsRevoked(context, setOf(p0))

        assertEquals(setOf(p1), assignments.load(), "p0 should be removed from assignments")
        assertEquals(1, committed.size, "Should have one commit via context")
        val flushCommit = committed[0]
        assertEquals(setOf(p0), flushCommit.keys, "flush should include p0")
        assertEquals(6L, flushCommit[p0], "Flushed offset for p0 should be 6 (5 + 1)")

        committer.stop().block()
    }

    @Test
    fun `concurrent processing with full revoke commits all offsets via context`() {
        val assignments = AtomicReference<Partitions>(setOf(p0, p1, p2))
        val committed = mutableListOf<Offsets>()
        val committer = BufferedCommitter(
            commit = Committer.Commit { Mono.just(it) },
            assignments = { assignments.load() },
            instanceId = "test-concurrent-full",
            topicPartitions = setOf(p0, p1, p2),
            log = log,
        )
        committer.start()

        val context = mockContext(onCommit = { committed.add(it) })
        val handler = handler(
            assignments = assignments,
            committerRef = { committer },
        )

        val processingStarted = CountDownLatch(1)
        val processingActive = AtomicBoolean(true)

        val processingThread = Thread {
            var i = 0
            processingStarted.countDown()
            while (processingActive.get()) {
                committer.markProcessed(Position(p0, i.toLong())).block()
                committer.markProcessed(Position(p1, i.toLong())).block()
                committer.markProcessed(Position(p2, i.toLong())).block()
                i++
            }
        }
        processingThread.start()
        processingStarted.await()

        handler.onPartitionsRevoked(context, setOf(p0, p1, p2))

        processingActive.set(false)
        processingThread.join(2000)

        assertTrue(assignments.load().isEmpty(), "All partitions should be removed")
        assertEquals(1, committed.size, "Should have one commit via context")
        val flushCommit = committed[0]
        assertEquals(setOf(p0, p1, p2), flushCommit.keys,
            "All revoked partitions should be committed")
        assertTrue(flushCommit.values.all { it > 0 },
            "Each revoked partition should have committed offset > 0")

        committer.stop().block()
    }

    private fun handler(
        assignments: AtomicReference<Partitions> = AtomicReference(emptySet()),
        pendingSeeks: AtomicReference<Offsets> = AtomicReference(emptyMap()),
        ends: AtomicReference<Positions> = AtomicReference(emptySet()),
        paused: () -> Boolean = { false },
        committerRef: () -> Committer = { mockCommitter() },
    ) = CoordinatingRebalanceHandler(
        committerRef = committerRef,
        assignments = assignments,
        pendingSeeks = pendingSeeks,
        ends = ends,
        paused = paused,
        instanceId = "test",
        log = log,
    )

    private fun mockCommitter(
        onSeed: (Offsets) -> Unit = {},
    ): Committer = object : Committer {
        override fun markProcessed(position: Position): Mono<Position> = Mono.just(position)
        override fun seedOffsets(offsets: Offsets) = onSeed(offsets)
        override val positions = Flux.empty<Position>()
        override val processedOffsets: Offsets get() = emptyMap()
        override fun start(): Disposable = throw UnsupportedOperationException()
        override fun stop(): Mono<Void> = Mono.empty()
    }

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
