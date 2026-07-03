package se.oyabun.prozess

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.util.retry.Retry
import se.oyabun.prozess.EmitterAlreadyRunning
import se.oyabun.prozess.InMemoryReceivedBuffer
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Position
import se.oyabun.prozess.Received
import se.oyabun.prozess.Topic
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class EmitterTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)
    private val p1 = Partition(1, topic)

    @Test
    fun `emits records from buffer when downstream has demand`() {
        val buffer = InMemoryReceivedBuffer()
        val emitted = ConcurrentLinkedQueue<Received>()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val shutdownSink = Sinks.one<Unit>()
        val done = Sinks.one<Unit>()
        val emitLatch = CountDownLatch(3)

        val e = BufferedEmitter(
            emit = Emitter.Emit { received ->
                emitted.add(received)
                emitLatch.countDown()
                Mono.just(received)
            },
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test-demand",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )

        buffer.offer(received(p0, 0L))
        buffer.offer(received(p0, 1L))
        buffer.offer(received(p0, 2L))

        try {
            e.start()
            assertTrue(emitLatch.await(3, TimeUnit.SECONDS), "Expected 3 emits within timeout")
            assertEquals(3, emitted.size)
        } finally {
            e.stop().block()
        }
    }

    @Test
    fun `emits records via reactive drain`() {
        val buffer = InMemoryReceivedBuffer()
        val emitted = ConcurrentLinkedQueue<Received>()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val shutdownSink = Sinks.one<Unit>()
        val done = Sinks.one<Unit>()
        val emitLatch = CountDownLatch(2)

        val e = BufferedEmitter(
            emit = Emitter.Emit { received ->
                emitted.add(received)
                emitLatch.countDown()
                Mono.just(received)
            },
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test-drain",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )

        buffer.offer(received(p0, 0L))
        buffer.offer(received(p0, 1L))

        try {
            e.start()
            assertTrue(emitLatch.await(3, TimeUnit.SECONDS), "Expected 2 emits via reactive drain")
            assertEquals(2, emitted.size)
        } finally {
            e.stop().block()
        }
    }

    @Test
    fun `resumes partitions when buffer falls below lowWaterMark`() {
        val resumeLatch = CountDownLatch(1)
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val shutdownSink = Sinks.one<Unit>()
        val done = Sinks.one<Unit>()

        val buffer = InMemoryReceivedBuffer(
            highWaterMark = 10,
            lowWaterMark = 10,
            onResume = { resumeLatch.countDown() },
        )

        repeat(15) { buffer.offer(received(p0, it.toLong())) }

        val e = BufferedEmitter(
            emit = Emitter.Emit { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test-resume",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )

        try {
            e.start()
            assertTrue(resumeLatch.await(3, TimeUnit.SECONDS), "Expected resume when buffer < lowWaterMark")
        } finally {
            e.stop().block()
        }
    }

    @Test
    fun `does not emit records from unassigned partitions`() {
        val buffer = InMemoryReceivedBuffer()
        val emitted = ConcurrentLinkedQueue<Received>()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val shutdownSink = Sinks.one<Unit>()
        val done = Sinks.one<Unit>()
        val emitLatch = CountDownLatch(1)

        buffer.offer(received(p1, 0L))
        buffer.offer(received(p0, 1L))

        val e = BufferedEmitter(
            emit = Emitter.Emit { received ->
                emitted.add(received)
                emitLatch.countDown()
                Mono.just(received)
            },
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test-assignments",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )

        try {
            e.start()
            assertTrue(emitLatch.await(3, TimeUnit.SECONDS), "Expected emit for assigned partition")
            assertEquals(1, emitted.size, "Expected only one record from assigned partition")
            assertEquals(1L, emitted.first().position.offset)
        } finally {
            e.stop().block()
        }
    }

    @Test
    fun `start throws when already running`() {
        val e = createEmitter()
        e.start()
        try {
            assertThrows<EmitterAlreadyRunning> { e.start() }
        } finally {
            e.stop().block()
        }
    }

    @Test
    fun `stop is idempotent when not running`() {
        val e = createEmitter()
        e.stop().block()
        e.stop().block()
    }

    @Test
    fun `signals done on pipeline completion`() {
        val done = Sinks.one<Unit>()
        val shutdownSink = Sinks.one<Unit>()
        val buffer = InMemoryReceivedBuffer()
        val assignments = AtomicReference(setOf(p0))

        val e = BufferedEmitter(
            emit = Emitter.Emit { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test-done",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )

        e.start()
        shutdownSink.tryEmitValue(Unit)
        val signalled = done.asMono().timeout(3.seconds.toJavaDuration()).hasElement().block() ?: false
        assertTrue(signalled, "Expected done signal after shutdown")
        e.stop().block()
    }

    @Test
    fun `restarts pipeline on error`() {
        val errorAttempts = AtomicInteger(0)
        val recoveryLatch = CountDownLatch(1)
        val done = Sinks.one<Unit>()
        val shutdownSink = Sinks.one<Unit>()
        val buffer = InMemoryReceivedBuffer()
        val assigned = java.util.concurrent.atomic.AtomicReference(setOf(p0))

        val e = BufferedEmitter(
            emit = Emitter.Emit {
                val n = errorAttempts.incrementAndGet()
                if (n <= 2) Mono.error(RuntimeException("emit failure $n"))
                else {
                    recoveryLatch.countDown()
                    Mono.just(it)
                }
            },
            buffer = buffer,
            assignments = { assigned.get() },
            instanceId = "test-retry",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.fixedDelay(5, 10.milliseconds.toJavaDuration()),
        )

        e.start()
        buffer.offer(received(p0, 0))
        Thread.sleep(30)
        buffer.offer(received(p0, 1))
        Thread.sleep(30)
        buffer.offer(received(p0, 2))
        assertTrue(recoveryLatch.await(3, TimeUnit.SECONDS), "Expected pipeline to retry and recover")
        assertTrue(errorAttempts.get() > 2, "Expected pipeline to retry after errors, got ${errorAttempts.get()}")
        e.stop().block()
    }

    @Test
    fun `start stop lifecycle`() {
        val e = createEmitter()
        e.start()
        e.stop().block()
    }

    private fun createEmitter(): BufferedEmitter {
        val buffer = InMemoryReceivedBuffer()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        return BufferedEmitter(
            emit = Emitter.Emit { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test",
            shutdownSink = Sinks.one(),
            done = Sinks.one(),
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )
    }

    private fun received(partition: Partition = p0, offset: Long = 0L): Received =
        Received("key", ByteArray(0), Position(partition, offset))
}
