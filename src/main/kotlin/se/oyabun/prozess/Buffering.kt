package se.oyabun.prozess

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.flow
import se.oyabun.aelv.Many
import se.oyabun.aelv.asMany
import se.oyabun.aelv.doOnNext
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

interface ReceivedBuffer {
    suspend fun offer(received: Received)
    val size: Int
    fun asMany(): Many<Received>
}

internal class InMemoryReceivedBuffer(
    private val highWaterMark: Int = Int.MAX_VALUE,
    private val lowWaterMark: Int = 0,
    private val onPause: () -> Unit = {},
    private val onResume: () -> Unit = {},
) : ReceivedBuffer {

    private val queue   = ConcurrentLinkedQueue<Received>()
    private val count   = AtomicInteger(0)
    // Signal channel: capacity 1, offers are dropped if a signal is already pending.
    // Used only to wake a suspended collector; data lives in queue.
    private val signal  = Channel<Unit>(1)
    @Volatile private var paused = false

    override suspend fun offer(received: Received) {
        queue.add(received)
        val n = count.incrementAndGet()
        if (!paused && n >= highWaterMark) {
            paused = true
            onPause()
        }
        signal.trySend(Unit) // best-effort wakeup; collector loops anyway
    }

    override val size: Int get() = count.get()

    override fun asMany(): Many<Received> = flow<Received> {
        while (true) {
            // Drain everything currently in the queue
            while (true) {
                val record = queue.poll() ?: break
                emit(record)
            }
            // Wait for next offer() to signal more data
            signal.receive()
        }
    }.asMany().doOnNext { _: Received ->
        val n = count.decrementAndGet()
        if (paused && n < lowWaterMark) {
            paused = false
            onResume()
        }
    }
}
