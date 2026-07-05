package se.oyabun.prozess

import org.junit.jupiter.api.Test
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ShutdownCoordinatorTest {

    private val log = Logging.logger { }

    @Test
    fun `shutdown stops poller and committer then closes client`() {
        val order = mutableListOf<String>()
        val coordinator = ShutdownCoordinator(
            client = testClient(
                onWakeup = { order.add("wakeup") },
                onClose = { order.add("close"); Mono.empty() },
            ),
            closeSignal = closeSignal(),
            poller = testPoller { order.add("poller") },
            committer = testCommitter { order.add("committer") },
            instanceId = "test",
            log = log,
        )

        coordinator.shutdown()

        assertEquals(listOf("wakeup", "poller", "committer", "close"), order)
    }

    @Test
    fun `shutdown with duration does not throw when components complete in time`() {
        val coordinator = ShutdownCoordinator(
            client = testClient(),
            closeSignal = closeSignal(),
            poller = testPoller { },
            committer = testCommitter { },
            instanceId = "test",
            log = log,
        )

        coordinator.shutdown(5.seconds)
    }

    @Test
    fun `shutdown with duration force closes when components hang`() {
        val order = mutableListOf<String>()
        val poller = object : Poller {
            private val stopped = AtomicBoolean(false)
            override fun start() = error("unexpected")
            override fun stop(): Mono<Void> = if (!stopped.getAndSet(true)) Mono.never() else Mono.empty()
            override fun pause() = error("unexpected")
            override fun resume() = error("unexpected")
            override val isRunning: Boolean get() = !stopped.get()
        }
        val coordinator = ShutdownCoordinator(
            client = testClient(
                onWakeup = { order.add("wakeup") },
                onClose = { order.add("close"); Mono.empty() },
            ),
            closeSignal = closeSignal(),
            poller = poller,
            committer = testCommitter { order.add("committer") },
            instanceId = "test",
            log = log,
        )

        coordinator.shutdown(100.milliseconds)

        assertTrue(order.contains("close"), "forceful close should have been attempted")
    }

    @Test
    fun `shutdown without duration blocks until components complete`() {
        val completed = AtomicBoolean(false)
        val latch = CountDownLatch(1)
        val coordinator = ShutdownCoordinator(
            client = testClient(onClose = { completed.set(true); Mono.empty() }),
            closeSignal = closeSignal(),
            poller = testPoller { latch.countDown() },
            committer = testCommitter { },
            instanceId = "test",
            log = log,
        )

        val thread = Thread { coordinator.shutdown() }
        thread.start()
        assertTrue(latch.await(5, TimeUnit.SECONDS), "poller stop should have been called")
        thread.join(5000)

        assertTrue(completed.get(), "client close should have completed")
    }

    @Test
    fun `shutdown stops components before client`() {
        val order = mutableListOf<String>()
        val coordinator = ShutdownCoordinator(
            client = testClient(
                onWakeup = { order.add("wakeup") },
                onClose = { order.add("close"); Mono.empty() },
            ),
            closeSignal = closeSignal(),
            poller = testPoller { order.add("poller") },
            committer = testCommitter { order.add("committer") },
            instanceId = "test",
            log = log,
        )

        coordinator.shutdown()

        assertTrue(order.indexOf("poller") < order.indexOf("close"), "poller should stop before client")
        assertTrue(order.indexOf("committer") < order.indexOf("close"), "committer should stop before client")
    }

    private fun testClient(
        onWakeup: () -> Unit = {},
        onClose: () -> Mono<Void> = { Mono.empty() },
    ) = object : ShutdownableClient {
        override fun wakeup() = onWakeup()
        override fun close() = onClose()
    }

    private fun closeSignal(): Sinks.One<Unit> = Sinks.one()

    companion object {
        fun testPoller(onStop: () -> Unit = {}): Poller = object : Poller {
            private val stopped = AtomicBoolean(false)
            override fun start() = error("unexpected")
            override fun stop(): Mono<Void> = Mono.fromRunnable {
                if (!stopped.getAndSet(true)) onStop()
            }
            override fun pause() = error("unexpected")
            override fun resume() = error("unexpected")
            override val isRunning: Boolean get() = !stopped.get()
        }

        fun testCommitter(onStop: () -> Unit = {}): Committer = object : Committer {
            override fun markProcessed(position: Position): Mono<Position> = error("unexpected")
            override fun seedOffsets(offsets: Offsets) = error("unexpected")
            override val positions: Flux<Position> get() = error("unexpected")
            override val processedOffsets: Offsets get() = error("unexpected")
            override fun start() = error("unexpected")
            override fun stop(): Mono<Void> = Mono.fromRunnable { onStop() }
        }
    }
}
