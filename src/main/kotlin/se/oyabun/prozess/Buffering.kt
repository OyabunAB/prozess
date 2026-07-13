package se.oyabun.prozess

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import se.oyabun.aelv.Many
import se.oyabun.aelv.asMany
import se.oyabun.aelv.doOnNext
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

interface ReceivedBuffer {
    suspend fun offer(received: Received<ByteArray>)
    val size: Int
    fun asMany(): Many<Received<ByteArray>>
}

internal class InMemoryReceivedBuffer(
    private val highWaterMark: Int = Int.MAX_VALUE,
    private val lowWaterMark: Int = 0,
    private val onPause: () -> Unit = {},
    private val onResume: () -> Unit = {},
) : ReceivedBuffer {

    private val queue  = ConcurrentLinkedQueue<Received<ByteArray>>()
    private val count  = AtomicInteger(0)
    // Capacity 1 — signals wake the collector; extras are dropped since the collector always drains fully.
    private val signal = Channel<Unit>(1)
    @Volatile private var paused = false

    override suspend fun offer(received: Received<ByteArray>) {
        queue.add(received)
        val n = count.incrementAndGet()
        if (!paused && n >= highWaterMark) { paused = true; onPause() }
        signal.trySend(Unit)
    }

    override val size: Int get() = count.get()

    override fun asMany(): Many<Received<ByteArray>> = flow<Received<ByteArray>> {
        while (currentCoroutineContext().isActive) {
            generateSequence(queue::poll).forEach { emit(it) }
            signal.receive()
        }
    }.asMany().doOnNext { _: Received<ByteArray> ->
        val n = count.decrementAndGet()
        if (paused && n < lowWaterMark) { paused = false; onResume() }
    }
}
