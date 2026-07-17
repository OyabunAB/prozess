package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import se.oyabun.aelv.drain
import se.oyabun.aelv.Sinks
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

@Timeout(value = 5, unit = TimeUnit.SECONDS)
class BufferedPollerTest {

    @Test
    fun `pauses on buffer saturation`() {
        val client = FakeKafkaClient()
        val pauseLatch = CountDownLatch(1)
        val buffer = InMemoryReceivedBuffer(highWaterMark = 500, onPause = { pauseLatch.countDown() })
        val topicPartition = Partition(0, Topic("test"))
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(topicPartition))
        val shutdownSink = Sinks.broadcast<Unit>()
        val doneSink = Sinks.broadcast<Unit>()

        runBlocking { repeat(495) { buffer.offer(received(topicPartition)) } }
        client.queuePollResults((1..10).map { received(topicPartition) })

        val poller = BufferedPoller(
            client = client,
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test-poller",
            pollInterval = 10.milliseconds,
            shutdownSink = shutdownSink,
            doneSink = doneSink,
            log = Logging.logger { },
        )

        try {
            poller.start()
            assertTrue(pauseLatch.await(3, TimeUnit.SECONDS), "Expected pause on buffer saturation")
        } finally {
            runBlocking { poller.stop().await() }
        }
    }

    @Test
    fun `start throws when already running`() {
        val poller = createPoller()
        poller.start()
        try {
            assertThrows<PollerAlreadyRunning> { poller.start() }
        } finally {
            runBlocking { poller.stop().await() }
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
        runBlocking {
            poller.stop().await()
            poller.stop().await()
        }
    }

    @Test
    fun `signals done on pipeline completion`() {
        val client = FakeKafkaClient()
        val shutdownSink = Sinks.broadcast<Unit>()
        val doneSink = Sinks.broadcast<Unit>()
        val buffer = InMemoryReceivedBuffer()
        val assignments = java.util.concurrent.atomic.AtomicReference(setOf(Partition(0, Topic("test"))))

        val poller = BufferedPoller(
            client = client,
            buffer = buffer,
            assignments = { assignments.get() },
            instanceId = "test-done",
            pollInterval = 10.milliseconds,
            shutdownSink = shutdownSink,
            doneSink = doneSink,
            log = Logging.logger { },
        )

        val signalledLatch = CountDownLatch(1)
        doneSink.asMany().drain(
            onNext = { signalledLatch.countDown() },
            onError = {},
            onComplete = { signalledLatch.countDown() },
        )

        poller.start()
        shutdownSink.complete()
        val signalled = signalledLatch.await(3, TimeUnit.SECONDS)
        assertTrue(signalled, "Expected done signal after shutdown")
        runBlocking { poller.stop().await() }
    }

    @Test
    fun `pause and resume lifecycle`() {
        val poller = createPoller()
        try {
            assertTrue(!poller.isRunning, "Should not be running before start")
            poller.start()
            assertTrue(poller.isRunning, "Should be running after start")
            runBlocking { poller.stop().await() }
            assertTrue(!poller.isRunning, "Should not be running after stop")
        } finally {
            runBlocking { poller.stop().await() }
        }
    }

    @Test
    fun `consecutive start stop cycles with fresh instances`() {
        val p1 = createPoller()
        p1.start()
        assertTrue(p1.isRunning)
        runBlocking { p1.stop().await() }
        assertTrue(!p1.isRunning)

        val p2 = createPoller()
        p2.start()
        assertTrue(p2.isRunning)
        runBlocking { p2.stop().await() }
        assertTrue(!p2.isRunning)
    }

    @Test
    fun `stop after start and stop on same instance is a no-op`() {
        val poller = createPoller()
        poller.start()
        runBlocking { poller.stop().await() }
        assertTrue(!poller.isRunning)
        runBlocking { poller.stop().await() }
        assertTrue(!poller.isRunning, "second stop must not throw or block")
    }

    @Test
    fun `pause and resume with empty assignments does not call client`() {
        val client = FakeKafkaClient()
        val buffer = InMemoryReceivedBuffer()
        val poller = BufferedPoller(
            client = client,
            buffer = buffer,
            assignments = { emptySet() },
            instanceId = "test-empty",
            pollInterval = 100.milliseconds,
            shutdownSink = se.oyabun.aelv.Sinks.broadcast(),
            doneSink = se.oyabun.aelv.Sinks.broadcast(),
            log = Logging.logger { },
        )
        poller.start()
        try {
            poller.pause()
            poller.resume()
            assertTrue(client.pausedPartitions.isEmpty(), "pause with empty assignments must not call client.pause")
            assertTrue(client.resumedPartitions.isEmpty(), "resume with empty assignments must not call client.resume")
        } finally {
            runBlocking { poller.stop().await() }
        }
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
            shutdownSink = Sinks.broadcast(),
            doneSink = Sinks.broadcast(),
            log = Logging.logger { },
        )
    }

    private fun received(partition: Partition = Partition(0, Topic("test"))): Received<ByteArray> =
        Received(Key.Present("key".toByteArray()), ReceivedMessage.Data(ByteArray(0)), Position(partition, 0L))
}
