package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.launch
import kotlinx.coroutines.coroutineScope
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import se.oyabun.aelv.drain
import se.oyabun.aelv.take
import se.oyabun.aelv.Verify
import se.oyabun.aelv.None
import se.oyabun.aelv.merge
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Timeout(value = 5, unit = TimeUnit.SECONDS)
class ReceivedBufferTest {

    @Test
    fun `offer adds record to buffer`() {
        val buffer = InMemoryReceivedBuffer()
        val received = Received(Key.Present("key".toByteArray()), ReceivedMessage.Data("value".toByteArray()), Position(Partition(0, Topic("topic")), 0))
        runBlocking { buffer.offer(received) }
        assertEquals(1, buffer.size)
    }

    @Test
    fun `size returns number of records in buffer`() {
        val buffer = InMemoryReceivedBuffer()
        assertEquals(0, buffer.size)
        val p = Partition(0, Topic("topic"))
        runBlocking {
            buffer.offer(Received(Key.Present("k1".toByteArray()), ReceivedMessage.Data("v1".toByteArray()), Position(p, 0)))
            buffer.offer(Received(Key.Present("k2".toByteArray()), ReceivedMessage.Data("v2".toByteArray()), Position(p, 1)))
        }
        assertEquals(2, buffer.size)
    }

    @Test
    fun `size reports zero when buffer is empty`() {
        val buffer = InMemoryReceivedBuffer()
        assertEquals(0, buffer.size)
        val p = Partition(0, Topic("topic"))
        runBlocking { buffer.offer(Received(Key.Present("k".toByteArray()), ReceivedMessage.Data("v".toByteArray()), Position(p, 0))) }
        assertTrue(buffer.size > 0)
    }

    @Test
    fun `asMany emits records in FIFO order`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val r1 = Received(Key.Present("k1".toByteArray()), ReceivedMessage.Data("v1".toByteArray()), Position(p, 0))
        val r2 = Received(Key.Present("k2".toByteArray()), ReceivedMessage.Data("v2".toByteArray()), Position(p, 1))
        runBlocking {
            buffer.offer(r1)
            buffer.offer(r2)
        }
        Verify.that(buffer.asMany().take(2))
            .emitsNext(r1, r2).completes()
    }

    @Test
    fun `asMany drains on subscriber demand`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val record = Received(Key.Present("k".toByteArray()), ReceivedMessage.Data("v".toByteArray()), Position(p, 0))
        Verify.that(merge(buffer.asMany().take(1), None.defer<Received<ByteArray>> { buffer.offer(record) }.toMany()))
            .emitsNext(record)
            .completes()
    }


    @Test
    fun `multiple asMany subscribers each drain from the same queue`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val a = Received(Key.Present("a".toByteArray()), ReceivedMessage.Data("a".toByteArray()), Position(p, 0))
        val b = Received(Key.Present("b".toByteArray()), ReceivedMessage.Data("b".toByteArray()), Position(p, 1))
        Verify.that(merge(buffer.asMany().take(2), None.defer<Received<ByteArray>> { buffer.offer(a); buffer.offer(b) }.toMany()))
            .emitsNext(a, b).completes()
        val c = Received(Key.Present("c".toByteArray()), ReceivedMessage.Data("c".toByteArray()), Position(p, 2))
        Verify.that(merge(buffer.asMany().take(1), None.defer<Received<ByteArray>> { buffer.offer(c) }.toMany()))
            .emitsNext(c).completes()
    }

    @Test
    fun `large batch maintains size and drains via asMany`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val count = 10_000
        runBlocking {
            repeat(count) { i -> buffer.offer(Received(Key.Present("k$i".toByteArray()), ReceivedMessage.Data("v$i".toByteArray()), Position(p, i.toLong()))) }
        }
        assertEquals(count, buffer.size)
        Verify.that(buffer.asMany().take(count.toLong()))
            .emitsCount(count.toLong()).completes()
        assertEquals(0, buffer.size)
    }

    @Test
    fun `thread safety for concurrent offers and drain`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        runBlocking {
            coroutineScope {
                repeat(10) {
                    launch {
                        repeat(100) {
                            buffer.offer(Received(Key.Present("k".toByteArray()), ReceivedMessage.Data("v".toByteArray()), Position(p, 0)))
                        }
                    }
                }
            }
        }
        assertEquals(1000, buffer.size)
    }

    @Test
    fun `onPause fires exactly once even when many offers exceed highWaterMark`() {
        val pauseCount = AtomicInteger(0)
        val buffer = InMemoryReceivedBuffer(highWaterMark = 3, onPause = { pauseCount.incrementAndGet() })
        val p = Partition(0, Topic("topic"))
        runBlocking {
            repeat(10) { i -> buffer.offer(Received(Key.Present("k".toByteArray()), ReceivedMessage.Data("v".toByteArray()), Position(p, i.toLong()))) }
        }
        assertEquals(1, pauseCount.get(), "onPause must fire exactly once per saturation event")
    }

    @Test
    fun `onResume fires exactly once when buffer drains below lowWaterMark`() {
        val resumeCount = AtomicInteger(0)
        val resumeLatch = CountDownLatch(1)
        val buffer = InMemoryReceivedBuffer(
            highWaterMark = 5,
            lowWaterMark  = 2,
            onResume = { resumeCount.incrementAndGet(); resumeLatch.countDown() },
        )
        val p = Partition(0, Topic("topic"))
        runBlocking {
            repeat(6) { i -> buffer.offer(Received(Key.Present("k".toByteArray()), ReceivedMessage.Data("v".toByteArray()), Position(p, i.toLong()))) }
        }
        // Drain until resume fires
        buffer.asMany().take(5).drain({}, {})
        assertTrue(resumeLatch.await(3, TimeUnit.SECONDS), "onResume must fire after draining below lowWaterMark")
        assertEquals(1, resumeCount.get(), "onResume must fire exactly once")
    }
}
