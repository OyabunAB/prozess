package se.oyabun.prozess

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier
import se.oyabun.prozess.Partition
import se.oyabun.prozess.Position
import se.oyabun.prozess.Received
import se.oyabun.prozess.Topic

class ReceivedBufferTest {

    @Test
    fun `offer adds record to buffer`() {
        val buffer = InMemoryReceivedBuffer()
        val received = Received("key", "value".toByteArray(), Position(Partition(0, Topic("topic")), 0))
        assertTrue(buffer.offer(received))
        assertEquals(1, buffer.size)
    }

    @Test
    fun `size returns number of records in buffer`() {
        val buffer = InMemoryReceivedBuffer()
        assertEquals(0, buffer.size)
        val p = Partition(0, Topic("topic"))
        buffer.offer(Received("k1", "v1".toByteArray(), Position(p, 0)))
        buffer.offer(Received("k2", "v2".toByteArray(), Position(p, 1)))
        assertEquals(2, buffer.size)
    }

    @Test
    fun `size reports zero when buffer is empty`() {
        val buffer = InMemoryReceivedBuffer()
        assertEquals(0, buffer.size)
        val p = Partition(0, Topic("topic"))
        buffer.offer(Received("k", "v".toByteArray(), Position(p, 0)))
        assertTrue(buffer.size > 0)
    }

    @Test
    fun `asFlux emits records in FIFO order`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val r1 = Received("k1", "v1".toByteArray(), Position(p, 0))
        val r2 = Received("k2", "v2".toByteArray(), Position(p, 1))
        buffer.offer(r1)
        buffer.offer(r2)
        StepVerifier.create(buffer.asFlux().take(2))
            .expectNext(r1).expectNext(r2).verifyComplete()
    }

    @Test
    fun `asFlux drains on subscriber demand`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val record = Received("k", "v".toByteArray(), Position(p, 0))
        val sub = buffer.asFlux().subscribe { r -> assertEquals(record, r) }
        buffer.offer(record)
        sub.dispose()
    }

    @Test
    fun `multiple asFlux subscribers each drain from the same queue`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val a = Received("a", "a".toByteArray(), Position(p, 0))
        val b = Received("b", "b".toByteArray(), Position(p, 1))
        StepVerifier.create(buffer.asFlux().take(2))
            .then { buffer.offer(a); buffer.offer(b) }
            .expectNext(a).expectNext(b).verifyComplete()
        val c = Received("c", "c".toByteArray(), Position(p, 2))
        StepVerifier.create(buffer.asFlux().take(1))
            .then { buffer.offer(c) }
            .expectNext(c).verifyComplete()
    }

    @Test
    fun `large batch maintains size and drains via asFlux`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val count = 10_000
        repeat(count) { i -> buffer.offer(Received("k$i", "v$i".toByteArray(), Position(p, i.toLong()))) }
        assertEquals(count, buffer.size)
        StepVerifier.create(buffer.asFlux().take(count.toLong()))
            .expectNextCount(count.toLong()).verifyComplete()
        assertEquals(0, buffer.size)
    }

    @Test
    fun `thread safety for concurrent offers and flux drain`() {
        val buffer = InMemoryReceivedBuffer()
        val p = Partition(0, Topic("topic"))
        val threads = (1..10).map {
            Thread { repeat(100) { buffer.offer(Received("k", "v".toByteArray(), Position(p, 0))) } }
        }
        threads.forEach { it.start() }
        threads.forEach { it.join() }
        assertEquals(1000, buffer.size)
    }
}
