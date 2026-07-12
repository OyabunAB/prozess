package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import se.oyabun.aelv.Many
import java.util.concurrent.TimeUnit
import se.oyabun.aelv.Success
import se.oyabun.aelv.Failure
import se.oyabun.aelv.Verify
import se.oyabun.aelv.await
import se.oyabun.aelv.last
import se.oyabun.aelv.take
import se.oyabun.aelv.toList
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import se.oyabun.prozess.Prozess.DeserializationResult
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Timeout(value = 5, unit = TimeUnit.SECONDS)
class ProcessorTest {

    private val topic = Topic("test")
    private val p0 = Partition(0, topic)

    private fun received(partition: Partition, offset: Long, message: String = "msg-$offset") =
        Received(ReceivedKey.Value("k-$offset"), ReceivedMessage.Data(message.toByteArray()), Position(partition, offset))

    @Test
    fun `each processor emits position for each message`() {
        val processor = DefaultProcessor.each<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = {},
        )
        val messages = Many.items(received(p0, 0), received(p0, 1), received(p0, 2))

        Verify.that(processor.process(messages))
            .emitsNext(Position(p0, 0))
            .emitsNext(Position(p0, 1))
            .emitsNext(Position(p0, 2))
            .completesNormally()
    }

    @Test
    fun `each processor invokes handler with deserialized value`() {
        val processed = mutableListOf<String>()
        val processor = DefaultProcessor.each<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = { p -> processed.add(p.value) },
        )
        val messages = Many.items(received(p0, 0, "hello"), received(p0, 1, "world"))

        runBlocking { processor.process(messages).last() }

        assertEquals(listOf("hello", "world"), processed)
    }

    @Test
    fun `each processor retries on transient failure then succeeds`() {
        var attempts = 0
        val processor = DefaultProcessor.each<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = {
                attempts++
                if (attempts < 3) throw RuntimeException("transient")
            },
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        Verify.that(processor.process(Many.items(received(p0, 0))))
            .emitsNext(Position(p0, 0))
            .completesNormally()

        assertEquals(3, attempts)
    }

    @Test
    fun `batch processor emits positions for each message in batch`() {
        val processor = DefaultProcessor.batch<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = {},
            batchSize = 3,
            batchDuration = 10.seconds,
        )
        val messages = Many.items(
            received(p0, 0), received(p0, 1), received(p0, 2),
            received(p0, 3), received(p0, 4),
        )

        Verify.that(processor.process(messages))
            .emitsNext(Position(p0, 0), Position(p0, 1), Position(p0, 2))
            .emitsNext(Position(p0, 3), Position(p0, 4))
            .completesNormally()
    }

    @Test
    fun `batch processor invokes handler with deserialized batch`() {
        val batches = mutableListOf<List<String>>()
        val processor = DefaultProcessor.batch<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = { batch -> batches.add(batch.map { it.value }) },
            batchSize = 2,
            batchDuration = 10.seconds,
        )
        val messages = Many.items(received(p0, 0, "a"), received(p0, 1, "b"), received(p0, 2, "c"))

        runBlocking { processor.process(messages).last() }

        assertEquals(listOf(listOf("a", "b"), listOf("c")), batches)
    }

    @Test
    fun `batch processor with duration flushes on timeout`() {
        val batches = mutableListOf<List<String>>()
        val processor = DefaultProcessor.batch<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = { batch -> batches.add(batch.map { it.value }) },
            batchSize = 10,
            batchDuration = 50.milliseconds,
        )
        val messages = Many.items(received(p0, 0, "a"))

        Verify.that(processor.process(messages))
            .emitsNext(Position(p0, 0))
            .completesNormally()

        assertEquals(1, batches.size)
        assertEquals(listOf("a"), batches[0])
    }

    @Test
    fun `batch retries on transient failure then succeeds`() {
        var attempts = 0
        val processor = DefaultProcessor.batch<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = {
                attempts++
                if (attempts < 2) throw RuntimeException("transient")
            },
            batchSize = 2,
            batchDuration = 10.seconds,
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        Verify.that(processor.process(Many.items(received(p0, 0, "a"), received(p0, 1, "b"))))
            .emitsNext(Position(p0, 0), Position(p0, 1))
            .completesNormally()
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
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { key, p -> processed.add("$key:${p.value}") },
        )
        val messages = Many.items(
            received(p0, 0, "a"),
            received(p0, 1, "b"),
            received(p0, 2, "a"),
        )

        runBlocking { processor.process(messages).last() }

        assertEquals(3, processed.size)
        assertTrue(processed.any { it == "a:a" }, "key 'a' processed")
        assertTrue(processed.any { it == "b:b" }, "key 'b' processed")
    }

    @Test
    fun `groupedEach with contiguous offset tracking emits final position`() {
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { _, _ -> },
        )
        val messages = Many.items(
            received(p0, 0, "a"),
            received(p0, 1, "b"),
            received(p0, 2, "a"),
            received(p0, 3, "b"),
        )

        val lastPosition = runBlocking { processor.process(messages).last().rightOrNull() }

        assertEquals(Position(p0, 3), lastPosition)
    }

    @Test
    fun `groupedEach retries on transient failure then succeeds`() {
        var attempts = 0
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { _, _ ->
                attempts++
                if (attempts < 2) throw RuntimeException("transient")
            },
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        Verify.that(processor.process(Many.items(received(p0, 0, "a"))))
            .emitsCount(1)
            .completesNormally()
        assertEquals(2, attempts)
    }

    @Test
    fun `groupedBatch processes messages in key-grouped batches`() {
        val batches = mutableListOf<List<String>>()
        val processor = DefaultProcessor.groupedBatch<String, String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { key, batch -> batches.add(batch.map { "$key:${it.value}" }) },
            batchSize = 2,
            batchDuration = 10.seconds,
        )
        val messages = Many.items(
            received(p0, 0, "a"),
            received(p0, 1, "a"),
            received(p0, 2, "b"),
            received(p0, 3, "b"),
        )

        runBlocking { processor.process(messages).last() }

        assertEquals(2, batches.size)
    }

    @Test
    fun `groupedBatch emits final position via tracker`() {
        val processor = DefaultProcessor.groupedBatch<String, String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { _, _ -> },
            batchSize = 2,
            batchDuration = 10.seconds,
        )
        val messages = Many.items(
            received(p0, 0, "a"),
            received(p0, 1, "a"),
            received(p0, 2, "b"),
            received(p0, 3, "b"),
        )

        val lastPosition = runBlocking { processor.process(messages).last().rightOrNull() }

        assertEquals(Position(p0, 3), lastPosition)
    }

    @Test
    fun `groupedBatch with duration flushes on timeout`() {
        val batches = mutableListOf<List<String>>()
        val processor = DefaultProcessor.groupedBatch<String, String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { _, batch -> batches.add(batch.map { it.value }) },
            batchSize = 10,
            batchDuration = 50.milliseconds,
        )
        Verify.that(processor.process(Many.items(received(p0, 0, "a"))))
            .emitsNext(Position(p0, 0))
            .completesNormally()
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
                when (val msg = it.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = {},
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        Verify.that(processor.process(Many.items(received(p0, 0))))
            .emitsNext(Position(p0, 0))
            .completesNormally()
        assertEquals(2, attempts)
    }

    @Test
    fun `groupedEach retries deserialization failure`() {
        var attempts = 0
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = {
                attempts++
                if (attempts < 2) throw RuntimeException("deserialize fail")
                when (val msg = it.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { _, _ -> },
            retryConfig = RetryConfig(minBackoff = 10.milliseconds),
        )
        Verify.that(processor.process(Many.items(received(p0, 0, "a"))))
            .emitsCount(1)
            .completesNormally()
        assertEquals(2, attempts, "deserialization is retried inside retry scope")
    }

    @Test
    fun `each completes on empty flux`() {
        val processor = DefaultProcessor.each<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = {},
        )
        Verify.that(processor.process(Many.empty()))
            .completesNormally()
    }

    @Test
    fun `batch completes on empty flux`() {
        val processor = DefaultProcessor.batch<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = {},
            batchSize = 2,
            batchDuration = 10.seconds,
        )
        Verify.that(processor.process(Many.empty()))
            .completesNormally()
    }

    @Test
    fun `groupedEach completes on empty flux`() {
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { _, _ -> },
        )
        Verify.that(processor.process(Many.empty()))
            .completesNormally()
    }

    @Test
    fun `each accepts maxConcurrency parameter`() {
        val processor = DefaultProcessor.each<String>(
            deserializer = { r ->
                when (val msg = r.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            handler = {},
            maxConcurrency = 4,
        )
        Verify.that(processor.process(Many.items(received(p0, 0), received(p0, 1))))
            .emitsNext(Position(p0, 0), Position(p0, 1))
            .completesNormally()
    }

    @Test
    fun `each acknowledges tombstone without calling handler`() {
        val processed = mutableListOf<String>()
        val processor = DefaultProcessor.each<String>(
            deserializer = { DeserializationResult.Tombstone },
            handler = { processed.add(it.value) },
        )
        Verify.that(processor.process(Many.items(received(p0, 0))))
            .emitsNext(Position(p0, 0))
            .completesNormally()
        assertEquals(emptyList(), processed)
    }

    @Test
    fun `each acknowledges poison pill without calling handler`() {
        val processed = mutableListOf<String>()
        val processor = DefaultProcessor.each<String>(
            deserializer = { DeserializationResult.PoisonPill("bad data") },
            handler = { processed.add(it.value) },
        )
        Verify.that(processor.process(Many.items(received(p0, 0))))
            .emitsNext(Position(p0, 0))
            .completesNormally()
        assertEquals(emptyList(), processed)
    }

    @Test
    fun `each tombstone and poison pill positions forward through batch`() {
        val processor = DefaultProcessor.batch<String>(
            deserializer = {
                when {
                    it.position.offset == 1L -> DeserializationResult.Tombstone
                    it.position.offset == 2L -> DeserializationResult.PoisonPill("bad")
                    else -> when (val msg = it.message) {
                        is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                        is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                    }
                }
            },
            handler = {},
            batchSize = 4,
            batchDuration = 10.seconds,
        )
        Verify.that(processor.process(Many.items(
            received(p0, 0, "a"),
            received(p0, 1, "b"),
            received(p0, 2, "c"),
            received(p0, 3, "d"),
        )))
            .emitsNext(Position(p0, 0), Position(p0, 1), Position(p0, 2), Position(p0, 3))
            .completesNormally()
    }

    @Test
    fun `groupedEach acknowledges tombstones without entering groupBy`() {
        val processed = mutableListOf<String>()
        val processor = DefaultProcessor.groupedEach<String, String>(
            deserializer = {
                if (it.position.offset == 1L) DeserializationResult.Tombstone
                else when (val msg = it.message) {
                    is ReceivedMessage.Data -> DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> DeserializationResult.Tombstone
                }
            },
            keyExtractor = { p -> p.value },
            handler = { _, p -> processed.add(p.value) },
        )
        val positions = runBlocking {
            when (val result = processor.process(Many.items(
                received(p0, 0, "a"),
                received(p0, 1, "b"),
                received(p0, 2, "c"),
            )).toList().await()) {
                is Success -> result.value
                is Failure -> fail("expected success but got failure: ${result.value}")
            }
        }
        assertTrue(positions.isNotEmpty())
        assertEquals(Position(p0, 2), positions.last())
        assertEquals(listOf("a", "c"), processed)
    }
}
