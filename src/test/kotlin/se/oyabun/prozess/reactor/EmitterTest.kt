package se.oyabun.prozess.reactor

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.util.concurrent.Queues
import reactor.util.retry.Retry
import se.oyabun.prozess.EmitterAlreadyRunning
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Position
import se.oyabun.prozess.Received
import se.oyabun.prozess.Topic
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
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
        val buffer = Queues.unbounded<Received>().get()
        val emitted = ConcurrentLinkedQueue<Received>()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val demand = AtomicLong(5)
        val shutdownSink = Sinks.one<Unit>()
        val done = Sinks.one<Unit>()
        val emitLatch = CountDownLatch(3)

        val e = BufferedEmitter(
            emit = Emitter.Emit { received ->
                emitted.add(received)
                emitLatch.countDown()
                Mono.just(received)
            },
            resume = Emitter.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            requestedFromDownstream = { demand.get() },
            lowWaterMark = 10,
            instanceId = "test-demand",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            period = 10.milliseconds,
            retryStrategy = Retry.max(1),
        )

        buffer.add(received(p0, 0L))
        buffer.add(received(p0, 1L))
        buffer.add(received(p0, 2L))

        try {
            e.start()
            assertTrue(emitLatch.await(3, TimeUnit.SECONDS), "Expected 3 emits within timeout")
            assertEquals(3, emitted.size)
        } finally {
            e.stop().block()
        }
    }

    @Test
    fun `does not emit when downstream has no demand`() {
        val buffer = Queues.unbounded<Received>().get()
        val emitted = ConcurrentLinkedQueue<Received>()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val demand = AtomicLong(0)
        val shutdownSink = Sinks.one<Unit>()
        val done = Sinks.one<Unit>()

        val e = BufferedEmitter(
            emit = Emitter.Emit { received ->
                emitted.add(received)
                Mono.just(received)
            },
            resume = Emitter.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            requestedFromDownstream = { demand.get() },
            lowWaterMark = 10,
            instanceId = "test-no-demand",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            period = 10.milliseconds,
            retryStrategy = Retry.max(1),
        )

        buffer.add(received(p0, 0L))
        buffer.add(received(p0, 1L))

        try {
            e.start()
            Thread.sleep(300)
            assertTrue(emitted.isEmpty(), "Expected no emits when demand is 0")
        } finally {
            e.stop().block()
        }
    }

    @Test
    fun `resumes partitions when buffer falls below lowWaterMark`() {
        val buffer = Queues.unbounded<Received>().get()
        val resumeCalls = mutableListOf<se.oyabun.prozess.Partitions>()
        val resumeLatch = CountDownLatch(1)
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val demand = AtomicLong(10)
        val shutdownSink = Sinks.one<Unit>()
        val done = Sinks.one<Unit>()

        repeat(15) { buffer.add(received(p0, it.toLong())) }

        val e = BufferedEmitter(
            emit = Emitter.Emit { Mono.just(it) },
            resume = Emitter.Resume { p ->
                resumeCalls.add(p)
                resumeLatch.countDown()
                Mono.just(p)
            },
            buffer = buffer,
            assignments = { assignments.get() },
            requestedFromDownstream = { demand.get() },
            lowWaterMark = 10,
            instanceId = "test-resume",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            period = 10.milliseconds,
            retryStrategy = Retry.max(1),
        )

        try {
            e.start()
            assertTrue(resumeLatch.await(3, TimeUnit.SECONDS), "Expected resume when buffer < lowWaterMark")
            assertTrue(resumeCalls.isNotEmpty(), "Expected at least one resume call")
        } finally {
            e.stop().block()
        }
    }

    @Test
    fun `does not emit records from unassigned partitions`() {
        val buffer = Queues.unbounded<Received>().get()
        val emitted = ConcurrentLinkedQueue<Received>()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        val demand = AtomicLong(10)
        val shutdownSink = Sinks.one<Unit>()
        val done = Sinks.one<Unit>()
        val emitLatch = CountDownLatch(1)

        buffer.add(received(p1, 0L))
        buffer.add(received(p0, 1L))

        val e = BufferedEmitter(
            emit = Emitter.Emit { received ->
                emitted.add(received)
                emitLatch.countDown()
                Mono.just(received)
            },
            resume = Emitter.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            requestedFromDownstream = { demand.get() },
            lowWaterMark = 10,
            instanceId = "test-assignments",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            period = 10.milliseconds,
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
        val buffer = Queues.unbounded<Received>().get()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))

        val e = BufferedEmitter(
            emit = Emitter.Emit { Mono.just(it) },
            resume = Emitter.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            requestedFromDownstream = { 1L },
            lowWaterMark = 10,
            instanceId = "test-done",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            period = 10.milliseconds,
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
        val buffer = Queues.unbounded<Received>().get()
        val assigned = java.util.concurrent.atomic.AtomicReference(setOf(p0))

        repeat(5) { buffer.add(received(p0, it.toLong())) }

        val e = BufferedEmitter(
            emit = Emitter.Emit {
                val n = errorAttempts.incrementAndGet()
                if (n <= 2) Mono.error(RuntimeException("emit failure $n"))
                else {
                    recoveryLatch.countDown()
                    Mono.just(it)
                }
            },
            resume = Emitter.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = { assigned.get() },
            requestedFromDownstream = { 1L },
            lowWaterMark = 10,
            instanceId = "test-retry",
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            period = 10.milliseconds,
            retryStrategy = Retry.fixedDelay(5, 10.milliseconds.toJavaDuration()),
        )

        e.start()
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
        val buffer = Queues.unbounded<Received>().get()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(p0))
        return BufferedEmitter(
            emit = Emitter.Emit { Mono.just(it) },
            resume = Emitter.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            requestedFromDownstream = { 1L },
            lowWaterMark = 10,
            instanceId = "test",
            shutdownSink = Sinks.one(),
            done = Sinks.one(),
            log = Logging.logger { },
            period = 100.milliseconds,
            retryStrategy = Retry.max(1),
        )
    }

    private fun received(partition: Partition = p0, offset: Long = 0L): Received =
        Received("key", ByteArray(0), Position(partition, offset))
}
