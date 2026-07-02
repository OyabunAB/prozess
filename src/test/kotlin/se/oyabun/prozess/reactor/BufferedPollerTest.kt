package se.oyabun.prozess.reactor

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.util.concurrent.Queues
import reactor.util.retry.Retry
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Partitions
import se.oyabun.prozess.PollerAlreadyRunning
import se.oyabun.prozess.PollerNotRunning
import se.oyabun.prozess.Position
import se.oyabun.prozess.Received
import se.oyabun.prozess.Topic
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class BufferedPollerTest {

    @Test
    fun `pauses on buffer saturation and resumes on drain`() {
        val pauseLatch = CountDownLatch(1)
        val resumeLatch = CountDownLatch(1)
        val pauseCalls = mutableListOf<Partitions>()
        val resumeCalls = mutableListOf<Partitions>()
        val pollQueue = java.util.ArrayDeque<List<Received>>()
        val buffer = Queues.unbounded<Received>().get()
        val topicPartition = Partition(0, Topic("test"))
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(topicPartition))
        val done = Sinks.one<Unit>()
        val shutdownSink = Sinks.one<Unit>()

        repeat(495) { buffer.add(received(topicPartition)) }
        pollQueue.addLast((1..10).map { received(topicPartition) })

        val poller = BufferedPoller(
            poll = Poller.Poll { Mono.just(pollQueue.pollFirst() ?: emptyList()) },
            pause = Poller.Pause { p -> pauseCalls.add(p); pauseLatch.countDown(); Mono.just(p) },
            resume = Poller.Resume { p -> resumeCalls.add(p); resumeLatch.countDown(); Mono.just(p) },
            buffer = buffer,
            assignments = { assignments.get() },
            highWaterMark = 500,
            lowWaterMark = 125,
            instanceId = "test-poller",
            pollInterval = 10.milliseconds,
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )

        try {
            poller.start()

            assertTrue(pauseLatch.await(3, TimeUnit.SECONDS), "Expected pause on buffer saturation")
            assertTrue(pauseCalls.isNotEmpty(), "Expected pause on buffer saturation")

            buffer.clear()
            assertTrue(resumeLatch.await(3, TimeUnit.SECONDS), "Expected resume on buffer drain")
            assertTrue(resumeCalls.isNotEmpty(), "Expected resume on buffer drain")
        } finally {
            poller.stop().block()
        }
    }

    @Test
    fun `start throws when already running`() {
        val poller = createPoller()
        poller.start()
        try {
            assertThrows<PollerAlreadyRunning> { poller.start() }
        } finally {
            poller.stop().block()
        }
    }

    @Test
    fun `pause throws when not running`() {
        val poller = createPoller()
        assertThrows<PollerNotRunning> { poller.pause() }
    }

    @Test
    fun `resume throws when not running`() {
        val poller = createPoller()
        assertThrows<PollerNotRunning> { poller.resume() }
    }

    @Test
    fun `stop is idempotent when not running`() {
        val poller = createPoller()
        poller.stop().block()
        poller.stop().block()
    }

    @Test
    fun `signals done on pipeline completion`() {
        val done = Sinks.one<Unit>()
        val shutdownSink = Sinks.one<Unit>()
        val buffer = Queues.unbounded<Received>().get()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(Partition(0, Topic("test"))))

        val poller = BufferedPoller(
            poll = Poller.Poll { Mono.just(emptyList()) },
            pause = Poller.Pause { Mono.just(it) },
            resume = Poller.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            highWaterMark = 1_000_000,
            lowWaterMark = 250_000,
            instanceId = "test-done",
            pollInterval = 10.milliseconds,
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )

        poller.start()
        shutdownSink.tryEmitValue(Unit)
        val signalled = done.asMono().timeout(3.seconds.toJavaDuration()).hasElement().block() ?: false
        assertTrue(signalled, "Expected done signal after shutdown")
        poller.stop().block()
    }

    @Test
    fun `restarts pipeline on processing error`() {
        val attempts = AtomicInteger(0)
        val recoveryLatch = CountDownLatch(1)
        val done = Sinks.one<Unit>()
        val shutdownSink = Sinks.one<Unit>()
        val buffer = Queues.unbounded<Received>().get()
        val assigned = java.util.concurrent.atomic.AtomicReference(setOf(Partition(0, Topic("test"))))

        val poller = BufferedPoller(
            poll = Poller.Poll { Mono.just(emptyList()) },
            pause = Poller.Pause { Mono.just(it) },
            resume = Poller.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = {
                val count = attempts.incrementAndGet()
                if (count <= 2) throw RuntimeException("assignments error $count")
                else { recoveryLatch.countDown(); assigned.get() }
            },
            highWaterMark = 1_000_000,
            lowWaterMark = 250_000,
            instanceId = "test-retry",
            pollInterval = 10.milliseconds,
            shutdownSink = shutdownSink,
            done = done,
            log = Logging.logger { },
            retryStrategy = Retry.fixedDelay(5, 10.milliseconds.toJavaDuration()),
        )

        poller.start()
        assertTrue(recoveryLatch.await(3, TimeUnit.SECONDS), "Expected pipeline to retry and recover")
        assertTrue(attempts.get() > 2, "Expected pipeline to retry after errors, got ${attempts.get()}")
        poller.stop().block()
    }

    @Test
    fun `pause and resume lifecycle`() {
        val poller = createPoller()
        try {
            assertTrue(!poller.isRunning, "Should not be running before start")
            poller.start()
            assertTrue(poller.isRunning, "Should be running after start")
            poller.stop().block()
            assertTrue(!poller.isRunning, "Should not be running after stop")
        } finally {
            poller.stop().block()
        }
    }

    @Test
    fun `consecutive start stop cycles with fresh instances`() {
        val p1 = createPoller()
        p1.start()
        assertTrue(p1.isRunning)
        p1.stop().block()
        assertTrue(!p1.isRunning)

        val p2 = createPoller()
        p2.start()
        assertTrue(p2.isRunning)
        p2.stop().block()
        assertTrue(!p2.isRunning)
    }

    private fun createPoller(): BufferedPoller {
        val buffer = Queues.unbounded<Received>().get()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(Partition(0, Topic("test"))))
        return BufferedPoller(
            poll = Poller.Poll { Mono.just(emptyList()) },
            pause = Poller.Pause { Mono.just(it) },
            resume = Poller.Resume { Mono.just(it) },
            buffer = buffer,
            assignments = { assignments.get() },
            highWaterMark = 1_000_000,
            lowWaterMark = 250_000,
            instanceId = "test",
            pollInterval = 100.milliseconds,
            shutdownSink = Sinks.one(),
            done = Sinks.one(),
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )
    }

    private fun received(partition: Partition = Partition(0, Topic("test"))): Received =
        Received("key", ByteArray(0), Position(partition, 0L))
}
