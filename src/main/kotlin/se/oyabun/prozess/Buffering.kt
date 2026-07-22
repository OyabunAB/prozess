/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.oyabun.prozess

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import se.oyabun.aelv.Many
import se.oyabun.aelv.asMany
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * A backpressure-aware in-memory buffer that sits between the Kafka poll loop and
 * the processing pipeline.
 *
 * The buffer decouples the two pipelines: the poller can continue polling while
 * downstream processing catches up. When the buffer reaches [highWaterMark] the
 * [onPause] callback fires to pause Kafka partitions; when it drains to [lowWaterMark]
 * the [onResume] callback fires to resume them.
 */
interface ReceivedBuffer {
    /**
     * Enqueues [received] for delivery to the processing pipeline.
     *
     * Triggers [onPause] when the buffer crosses [highWaterMark].
     */
    suspend fun offer(received: Received<ByteArray>)

    /** Current number of records waiting in the buffer. */
    val size: Int

    /**
     * Returns a [Many] that emits records from this buffer in FIFO order.
     *
     * Each call creates a new independent subscription. The stream never completes
     * on its own — it runs until cancelled.
     */
    fun asMany(): Many<Received<ByteArray>>
}

internal class InMemoryReceivedBuffer(
    private val highWaterMark: Int = Int.MAX_VALUE,
    private val lowWaterMark: Int = 0,
    private val onPause: suspend () -> Unit = {},
    private val onResume: suspend () -> Unit = {},
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
