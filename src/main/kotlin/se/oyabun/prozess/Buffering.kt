package se.oyabun.prozess

import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import se.oyabun.prozess.Received
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Abstraction for a buffer that holds [Received] records.
 *
 * Records are written via [offer] and read reactively via [asFlux].
 * The buffer owns the back-pressure contract via high/low watermarks,
 * calling [onPause] when the buffer fills past [highWaterMark] and
 * [onResume] when it drains below [lowWaterMark].
 */
interface ReceivedBuffer {

    fun offer(received: Received): Boolean
    val size: Int
    fun asFlux(): Flux<Received>
}

internal class InMemoryReceivedBuffer(
    private val delegate: Queue<Received> = ConcurrentLinkedQueue(),
    private val highWaterMark: Int = Int.MAX_VALUE,
    private val lowWaterMark: Int = 0,
    private val onPause: () -> Unit = {},
    private val onResume: () -> Unit = {},
) : ReceivedBuffer {

    private val sink = AtomicReference<FluxSink<Received>>()
    @Volatile
    private var paused = false
    private val count = AtomicInteger(0)

    override fun offer(received: Received): Boolean {
        val result = delegate.add(received)
        val n = count.incrementAndGet()
        if (!paused && n >= highWaterMark) {
            paused = true
            onPause()
        }
        sink.get()?.let { drain(it) }
        return result
    }

    override val size: Int get() = count.get()

    override fun asFlux(): Flux<Received> = Flux.create(
        { s ->
            sink.set(s)
            s.onDispose { sink.compareAndSet(s, null) }
            drain(s)
        },
        FluxSink.OverflowStrategy.IGNORE,
    )

    /**
     * Drains all queued records into the sink while downstream has demand.
     */
    private fun drain(s: FluxSink<Received>) {
        while (s.requestedFromDownstream() > 0) {
            val record = delegate.poll() ?: break
            s.next(record)
            val n = count.decrementAndGet()
            if (paused && n < lowWaterMark) {
                paused = false
                onResume()
            }
        }
    }
}
