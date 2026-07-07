package se.oyabun.prozess

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import reactor.core.publisher.Sinks
import reactor.util.retry.Retry
import se.oyabun.prozess.Logging
import se.oyabun.prozess.Partition
import se.oyabun.prozess.PollerAlreadyRunning
import se.oyabun.prozess.PollerNotRunning
import se.oyabun.prozess.Position
import se.oyabun.prozess.Received
import se.oyabun.prozess.Topic
import se.oyabun.prozess.InMemoryReceivedBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class BufferedPollerTest {

    @Test
    fun `pauses on buffer saturation`() {
        val client = FakeKafkaClient()
        val pauseLatch = CountDownLatch(1)
        val buffer = InMemoryReceivedBuffer(highWaterMark = 500, onPause = { pauseLatch.countDown() })
        val topicPartition = Partition(0, Topic("test"))
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(topicPartition))
        val done = Sinks.one<Unit>()
        val shutdownSink = Sinks.one<Unit>()

        repeat(495) { buffer.offer(received(topicPartition)) }
        client.queuePollResults((1..10).map { received(topicPartition) })

        val poller = BufferedPoller(
            client = client,
            buffer = buffer,
            assignments = { assignments.get() },
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
        val client = FakeKafkaClient()
        val done = Sinks.one<Unit>()
        val shutdownSink = Sinks.one<Unit>()
        val buffer = InMemoryReceivedBuffer()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(Partition(0, Topic("test"))))

        val poller = BufferedPoller(
            client = client,
            buffer = buffer,
            assignments = { assignments.get() },
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
        val client = FakeKafkaClient()
        val buffer = InMemoryReceivedBuffer()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(Partition(0, Topic("test"))))
        return BufferedPoller(
            client = client,
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test",
            pollInterval = 100.milliseconds,
            shutdownSink = Sinks.one(),
            done = Sinks.one(),
            log = Logging.logger { },
            retryStrategy = Retry.max(1),
        )
    }

    private fun received(partition: Partition = Partition(0, Topic("test"))): Received =
        Received(ReceivedKey.Value("key"), ReceivedMessage.Data(ByteArray(0)), Position(partition, 0L))
}
