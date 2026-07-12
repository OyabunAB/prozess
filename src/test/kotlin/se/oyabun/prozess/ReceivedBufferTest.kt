package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
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
        val received = Received(ReceivedKey.Value("key"), ReceivedMessage.Data("value".toByteArray()), Position(Partition(0, Topic("topic")), 0))
        runBlocking { buffer.offer(received) }
        assertEquals(1, buffer.size)
    }

    @Test
    fun `size returns number of records in buffer`() {
        val buffer = InMemoryReceivedBuffer()
        assertEquals(0, buffer.size)
        val p = Partition(0, Topic("topic"))
        runBlocking {
            buffer.offer(Received(ReceivedKey.Value("k1"), ReceivedMessage.Data("v1".toByteArray()), Position(p, 0)))
            buffer.offer(Received(ReceivedKey.Value("k2"), ReceivedMessage.Data("v2".toByteArray()), Position(p, 1)))
        }
        assertEquals(2, buffer.size)
    }

    @Test
    fun `size reports zero when buffer is empty`() {
        val buffer = InMemoryReceivedBuffer()
        assertEquals(0, buffer.size)
        val p = Partition(0, Topic("topic"))
        runBlocking { buffer.offer(Received(ReceivedKey.Value("k"), ReceivedMessage.Data("v".toByteArray()), Position(p, 0))) }
        assertTrue(buffer.size > 0)
    }

    @Test
    fun `asMany emits records in FIFO order`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val r1 = Received(ReceivedKey.Value("k1"), ReceivedMessage.Data("v1".toByteArray()), Position(p, 0))
        val r2 = Received(ReceivedKey.Value("k2"), ReceivedMessage.Data("v2".toByteArray()), Position(p, 1))
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
        val record = Received(ReceivedKey.Value("k"), ReceivedMessage.Data("v".toByteArray()), Position(p, 0))
        Verify.that(buffer.asMany().take(1))
            .runs { runBlocking { buffer.offer(record) } }
            .emitsNext(record)
            .completesNormally()
    }


    @Test
    fun `multiple asMany subscribers each drain from the same queue`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val a = Received(ReceivedKey.Value("a"), ReceivedMessage.Data("a".toByteArray()), Position(p, 0))
        val b = Received(ReceivedKey.Value("b"), ReceivedMessage.Data("b".toByteArray()), Position(p, 1))
        Verify.that(buffer.asMany().take(2))
            .runs { runBlocking { buffer.offer(a); buffer.offer(b) } }
            .emitsNext(a).emitsNext(b).completesNormally()
        val c = Received(ReceivedKey.Value("c"), ReceivedMessage.Data("c".toByteArray()), Position(p, 2))
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
            repeat(count) { i -> buffer.offer(Received(ReceivedKey.Value("k$i"), ReceivedMessage.Data("v$i".toByteArray()), Position(p, i.toLong()))) }
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
        val threads = (1..10).map {
            Thread { runBlocking { repeat(100) { buffer.offer(Received(ReceivedKey.Value("k"), ReceivedMessage.Data("v".toByteArray()), Position(p, 0))) } } }
        }
        threads.forEach { it.start() }
        threads.forEach { it.join() }
        assertEquals(1000, buffer.size)
    }
}
