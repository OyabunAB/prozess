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
import java.util.concurrent.TimeUnit

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
            .emitsNext(r1).emitsNext(r2).completesNormally()
    }

    @Test
    fun `asMany drains on subscriber demand`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val record = Received(Key.Present("k".toByteArray()), ReceivedMessage.Data("v".toByteArray()), Position(p, 0))
        Verify.that(buffer.asMany().take(1))
            .runs { runBlocking { buffer.offer(record) } }
            .emitsNext(record)
            .completesNormally()
    }


    @Test
    fun `multiple asMany subscribers each drain from the same queue`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val a = Received(Key.Present("a".toByteArray()), ReceivedMessage.Data("a".toByteArray()), Position(p, 0))
        val b = Received(Key.Present("b".toByteArray()), ReceivedMessage.Data("b".toByteArray()), Position(p, 1))
        Verify.that(buffer.asMany().take(2))
            .runs { runBlocking { buffer.offer(a); buffer.offer(b) } }
            .emitsNext(a).emitsNext(b).completesNormally()
        val c = Received(Key.Present("c".toByteArray()), ReceivedMessage.Data("c".toByteArray()), Position(p, 2))
        Verify.that(buffer.asMany().take(1))
            .runs { runBlocking { buffer.offer(c) } }
            .emitsNext(c).completesNormally()
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
            .emitsCount(count.toLong()).completesNormally()
        assertEquals(0, buffer.size)
    }

    @Test
    fun `thread safety for concurrent offers and flux drain`() {
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
}
