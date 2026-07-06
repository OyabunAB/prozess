package se.oyabun.prozess

import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class ProcessorTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)

    private fun received(partition: Partition, offset: Long, message: String = "msg-$offset") =
        Received("k-$offset", message.toByteArray(), Position(partition, offset))

    @Test
    fun `each processor emits position for each message`() {
        val processor = DefaultProcessor.each<String>(
            deserializer = { String(it) },
            handler = {},
        )
        val messages = Flux.just(received(p0, 0), received(p0, 1), received(p0, 2))

        StepVerifier.create(processor.process(messages))
            .expectNext(Position(p0, 0))
            .expectNext(Position(p0, 1))
            .expectNext(Position(p0, 2))
            .verifyComplete()
    }

    @Test
    fun `each processor invokes handler with deserialized value`() {
        val processed = mutableListOf<String>()
        val processor = DefaultProcessor.each<String>(
            deserializer = { String(it) },
            handler = { p -> processed.add(p.value) },
        )
        val messages = Flux.just(received(p0, 0, "hello"), received(p0, 1, "world"))

        processor.process(messages).blockLast()

        assertEquals(listOf("hello", "world"), processed)
    }

    @Test
    fun `each processor retries on transient failure then succeeds`() {
        var attempts = 0
        val processor = DefaultProcessor.each<String>(
            deserializer = { String(it) },
            handler = {
                attempts++
                if (attempts < 3) throw RuntimeException("transient")
            },
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        StepVerifier.create(processor.process(Flux.just(received(p0, 0))))
            .expectNext(Position(p0, 0))
            .verifyComplete()

        assertEquals(3, attempts)
    }

    @Test
    fun `batch processor emits positions for each message in batch`() {
        val processor = DefaultProcessor.batch<String>(
            deserializer = { String(it) },
            handler = {},
            batchSize = 3,
        )
        val messages = Flux.just(
            received(p0, 0), received(p0, 1), received(p0, 2),
            received(p0, 3), received(p0, 4),
        )

        StepVerifier.create(processor.process(messages))
            .expectNext(Position(p0, 0), Position(p0, 1), Position(p0, 2))
            .expectNext(Position(p0, 3), Position(p0, 4))
            .verifyComplete()
    }

    @Test
    fun `batch processor invokes handler with deserialized batch`() {
        val batches = mutableListOf<List<String>>()
        val processor = DefaultProcessor.batch<String>(
            deserializer = { String(it) },
            handler = { batch -> batches.add(batch.map { it.value }) },
            batchSize = 2,
        )
        val messages = Flux.just(received(p0, 0, "a"), received(p0, 1, "b"), received(p0, 2, "c"))

        processor.process(messages).blockLast()

        assertEquals(listOf(listOf("a", "b"), listOf("c")), batches)
    }

    @Test
    fun `batch processor with duration flushes on timeout`() {
        val batches = mutableListOf<List<String>>()
        val processor = DefaultProcessor.batch<String>(
            deserializer = { String(it) },
            handler = { batch -> batches.add(batch.map { it.value }) },
            batchSize = 10,
            batchDuration = 50.milliseconds,
        )
        val messages = Flux.just(received(p0, 0, "a"))

        StepVerifier.create(processor.process(messages))
            .expectNext(Position(p0, 0))
            .verifyComplete()

        assertEquals(1, batches.size)
        assertEquals(listOf("a"), batches[0])
    }

    @Test
    fun `batch retries on transient failure then succeeds`() {
        var attempts = 0
        val processor = DefaultProcessor.batch<String>(
            deserializer = { String(it) },
            handler = {
                attempts++
                if (attempts < 2) throw RuntimeException("transient")
            },
            batchSize = 2,
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        StepVerifier.create(processor.process(Flux.just(received(p0, 0, "a"), received(p0, 1, "b"))))
            .expectNext(Position(p0, 0), Position(p0, 1))
            .verifyComplete()
        assertEquals(2, attempts)
    }

    @Test
    fun `tracker emits highest contiguous offset when prefix fills`() {
        val tracker = ContiguousOffsetTracker()
        assertNull(tracker.onCompleted(Position(p0, 2)))
        assertNull(tracker.onCompleted(Position(p0, 1)))
        assertEquals(Position(p0, 2), tracker.onCompleted(Position(p0, 0)))
    }

    @Test
    fun `tracker emits nothing until offset zero completes`() {
        val tracker = ContiguousOffsetTracker()
        assertNull(tracker.onCompleted(Position(p0, 2)))
        assertNull(tracker.onCompleted(Position(p0, 3)))
        assertEquals(Position(p0, 0), tracker.onCompleted(Position(p0, 0)))
    }

    @Test
    fun `tracker skips already processed offsets`() {
        val tracker = ContiguousOffsetTracker()
        assertEquals(Position(p0, 0), tracker.onCompleted(Position(p0, 0)))
        assertNull(tracker.onCompleted(Position(p0, 0)))
        assertEquals(Position(p0, 1), tracker.onCompleted(Position(p0, 1)))
        assertNull(tracker.onCompleted(Position(p0, 1)))
    }

    @Test
    fun `tracker emits highest contiguous after gap filled`() {
        val tracker = ContiguousOffsetTracker()
        assertEquals(Position(p0, 0), tracker.onCompleted(Position(p0, 0)))
        assertNull(tracker.onCompleted(Position(p0, 2)))
        assertEquals(Position(p0, 2), tracker.onCompleted(Position(p0, 1)))
    }

    @Test
    fun `tracker never emits past a gap to prevent unsafe commits`() {
        val tracker = ContiguousOffsetTracker()
        assertNull(tracker.onCompleted(Position(p0, 3)))
        assertNull(tracker.onCompleted(Position(p0, 4)))
        assertNull(tracker.onCompleted(Position(p0, 6)))
        assertEquals(Position(p0, 0), tracker.onCompleted(Position(p0, 0)))
        assertNull(tracker.onCompleted(Position(p0, 6)))
        assertEquals(Position(p0, 1), tracker.onCompleted(Position(p0, 1)))
        assertNull(tracker.onCompleted(Position(p0, 5)))
        assertNull(tracker.onCompleted(Position(p0, 6)))
        assertEquals(Position(p0, 6), tracker.onCompleted(Position(p0, 2)))
        assertNull(tracker.onCompleted(Position(p0, 2)))
        assertNull(tracker.onCompleted(Position(p0, 5)))
    }

    @Test
    fun `tracker does not emit beyond gap even when higher offsets arrive later`() {
        val tracker = ContiguousOffsetTracker()
        assertEquals(Position(p0, 0), tracker.onCompleted(Position(p0, 0)))
        assertNull(tracker.onCompleted(Position(p0, 2)))
        assertNull(tracker.onCompleted(Position(p0, 3)))
        assertNull(tracker.onCompleted(Position(p0, 4)))
        assertEquals(Position(p0, 4), tracker.onCompleted(Position(p0, 1)))
    }

    @Test
    fun `groupedEach processes messages grouped by key`() {
        val processed = mutableListOf<String>()
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = { String(it) },
            keyExtractor = { p -> p.value },
            handler = { key, p -> processed.add("$key:${p.value}") },
        )
        val messages = Flux.just(
            received(p0, 0, "a"),
            received(p0, 1, "b"),
            received(p0, 2, "a"),
        )

        processor.process(messages).blockLast()

        assertEquals(3, processed.size)
        assertTrue(processed.any { it == "a:a" }, "key 'a' processed")
        assertTrue(processed.any { it == "b:b" }, "key 'b' processed")
    }

    @Test
    fun `groupedEach with contiguous offset tracking emits final position`() {
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = { String(it) },
            keyExtractor = { p -> p.value },
            handler = { _, _ -> },
        )
        val messages = Flux.just(
            received(p0, 0, "a"),
            received(p0, 1, "b"),
            received(p0, 2, "a"),
            received(p0, 3, "b"),
        )

        val lastPosition = processor.process(messages).blockLast()

        assertEquals(Position(p0, 3), lastPosition)
    }

    @Test
    fun `groupedEach retries on transient failure then succeeds`() {
        var attempts = 0
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = { String(it) },
            keyExtractor = { p -> p.value },
            handler = { _, _ ->
                attempts++
                if (attempts < 2) throw RuntimeException("transient")
            },
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        StepVerifier.create(processor.process(Flux.just(received(p0, 0, "a"))).take(1))
            .expectNextCount(1)
            .verifyComplete()
        assertEquals(2, attempts)
    }

    @Test
    fun `groupedBatch processes messages in key-grouped batches`() {
        val batches = mutableListOf<List<String>>()
        val processor = DefaultProcessor.groupedBatch<String, String>(
            deserializer = { String(it) },
            keyExtractor = { p -> p.value },
            handler = { key, batch -> batches.add(batch.map { "$key:${it.value}" }) },
            batchSize = 2,
        )
        val messages = Flux.just(
            received(p0, 0, "a"),
            received(p0, 1, "a"),
            received(p0, 2, "b"),
            received(p0, 3, "b"),
        )

        processor.process(messages).blockLast()

        assertEquals(2, batches.size)
    }

    @Test
    fun `groupedBatch emits final position via tracker`() {
        val processor = DefaultProcessor.groupedBatch<String, String>(
            deserializer = { String(it) },
            keyExtractor = { p -> p.value },
            handler = { _, _ -> },
            batchSize = 2,
        )
        val messages = Flux.just(
            received(p0, 0, "a"),
            received(p0, 1, "a"),
            received(p0, 2, "b"),
            received(p0, 3, "b"),
        )

        val lastPosition = processor.process(messages).blockLast()

        assertEquals(Position(p0, 3), lastPosition)
    }

    @Test
    fun `groupedBatch with duration flushes on timeout`() {
        val batches = mutableListOf<List<String>>()
        val processor = DefaultProcessor.groupedBatch<String, String>(
            deserializer = { String(it) },
            keyExtractor = { p -> p.value },
            handler = { _, batch -> batches.add(batch.map { it.value }) },
            batchSize = 10,
            batchDuration = 50.milliseconds,
        )
        StepVerifier.create(processor.process(Flux.just(received(p0, 0, "a"))))
            .expectNext(Position(p0, 0))
            .verifyComplete()
        assertEquals(1, batches.size)
        assertEquals(listOf("a"), batches[0])
    }

    @Test
    fun `each retries deserialization failure`() {
        var attempts = 0
        val processor = DefaultProcessor.each<String>(
            deserializer = {
                attempts++
                if (attempts < 2) throw RuntimeException("deserialize fail")
                String(it)
            },
            handler = {},
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        StepVerifier.create(processor.process(Flux.just(received(p0, 0))))
            .expectNext(Position(p0, 0))
            .verifyComplete()
        assertEquals(2, attempts)
    }

    @Test
    fun `groupedEach retries deserialization failure`() {
        var attempts = 0
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = {
                attempts++
                if (attempts < 2) throw RuntimeException("deserialize fail")
                String(it)
            },
            keyExtractor = { p -> p.value },
            handler = { _, _ -> },
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        StepVerifier.create(processor.process(Flux.just(received(p0, 0, "a"))).take(1))
            .expectNextCount(1)
            .verifyComplete()
        assertEquals(2, attempts, "deserialization is retried inside retry scope")
    }

    @Test
    fun `each completes on empty flux`() {
        val processor = DefaultProcessor.each<String>(
            deserializer = { String(it) },
            handler = {},
        )
        StepVerifier.create(processor.process(Flux.empty()))
            .verifyComplete()
    }

    @Test
    fun `batch completes on empty flux`() {
        val processor = DefaultProcessor.batch<String>(
            deserializer = { String(it) },
            handler = {},
            batchSize = 2,
        )
        StepVerifier.create(processor.process(Flux.empty()))
            .verifyComplete()
    }

    @Test
    fun `groupedEach completes on empty flux`() {
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = { String(it) },
            keyExtractor = { p -> p.value },
            handler = { _, _ -> },
        )
        StepVerifier.create(processor.process(Flux.empty()))
            .verifyComplete()
    }

    @Test
    fun `each accepts maxConcurrency parameter`() {
        val processor = DefaultProcessor.each<String>(
            deserializer = { String(it) },
            handler = {},
            maxConcurrency = 4,
        )
        StepVerifier.create(processor.process(Flux.just(received(p0, 0), received(p0, 1))))
            .expectNext(Position(p0, 0), Position(p0, 1))
            .verifyComplete()
    }
}
