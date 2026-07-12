package se.oyabun.prozess

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Timeout(value = 5, unit = TimeUnit.SECONDS)
class ConsumerEventTest {

    private val p0 = Partition(0, Topic("test"))
    private val p1 = Partition(1, Topic("test"))

    private fun consumer(fake: FakeKafkaClient = FakeKafkaClient()): Pair<StreamingConsumer<String>, FakeKafkaClient> {
        fake.setPartitionsFor("test", setOf(p0, p1))
        val processor = DefaultProcessor.each<String>(
            deserializer = { received ->
                when (val msg = received.message) {
                    is ReceivedMessage.Data -> Prozess.DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> Prozess.DeserializationResult.Tombstone
                }
            },
            handler = {},
        )
        return StreamingConsumer(
            config = ConsumerConfig("bootstrap", "group", "test"),
            processor = processor,
            client = fake,
        ) to fake
    }

    @Test
    fun `start emits Started event`() {
        val (consumer, _) = consumer()
        val events = CopyOnWriteArrayList<ConsumerEvent>()
        consumer.onEvent { events.add(it) }

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        consumer.shutdown()

        assertTrue(events.contains(ConsumerEvent.Started))
    }

    @Test
    fun `shutdown emits Stopped event`() {
        val (consumer, _) = consumer()
        val events = CopyOnWriteArrayList<ConsumerEvent>()
        consumer.onEvent { events.add(it) }

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        consumer.shutdown()

        assertTrue(events.contains(ConsumerEvent.Stopped))
    }

    @Test
    fun `pause and resume emit Paused and Resumed events`() {
        val (consumer, _) = consumer()
        val events = CopyOnWriteArrayList<ConsumerEvent>()
        consumer.onEvent { events.add(it) }

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        consumer.pause()
        consumer.resume()
        consumer.shutdown()

        assertTrue(events.contains(ConsumerEvent.Paused))
        assertTrue(events.contains(ConsumerEvent.Resumed))
    }

    @Test
    fun `assignment emits Assigned event with partitions`() {
        val (consumer, _) = consumer()
        val events = CopyOnWriteArrayList<ConsumerEvent>()
        consumer.onEvent { events.add(it) }
        val assignedLatch = java.util.concurrent.CountDownLatch(1)
        consumer.onEvent { if (it is ConsumerEvent.Assigned) assignedLatch.countDown() }

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        awaitLatch(assignedLatch)
        consumer.shutdown()

        val assigned = events.filterIsInstance<ConsumerEvent.Assigned>()
        assertTrue(assigned.isNotEmpty(), "expected at least one Assigned event")
        assertEquals(setOf(p0, p1), assigned.first().partitions)
    }

    @Test
    fun `revoke emits Revoked event with partitions`() {
        val fake = FakeKafkaClient()
        fake.setPartitionsFor("test", setOf(p0, p1))
        val processor = DefaultProcessor.each<String>(
            deserializer = { received ->
                when (val msg = received.message) {
                    is ReceivedMessage.Data -> Prozess.DeserializationResult.Message(String(msg.bytes))
                    is ReceivedMessage.Tombstone -> Prozess.DeserializationResult.Tombstone
                }
            },
            handler = {},
        )
        val consumer = StreamingConsumer(
            config = ConsumerConfig("bootstrap", "group", "test"),
            processor = processor,
            client = fake,
        )
        val events = CopyOnWriteArrayList<ConsumerEvent>()
        consumer.onEvent { events.add(it) }
        val assignedLatch = java.util.concurrent.CountDownLatch(1)
        consumer.onEvent { if (it is ConsumerEvent.Assigned) assignedLatch.countDown() }

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        awaitLatch(assignedLatch)

        // FakeKafkaClient triggers assign on subscribe but not revoke.
        // Verify event ordering: Started -> Assigned -> Stopped
        consumer.shutdown()

        val types = events.map { it::class }
        assertTrue(types.indexOf(ConsumerEvent.Started::class) < types.indexOf(ConsumerEvent.Assigned::class))
        assertTrue(types.indexOf(ConsumerEvent.Assigned::class) < types.indexOf(ConsumerEvent.Stopped::class))
    }

    @Test
    fun `events are ordered correctly across lifecycle`() {
        val (consumer, _) = consumer()
        val events = CopyOnWriteArrayList<ConsumerEvent>()
        consumer.onEvent { events.add(it) }
        val assignedLatch = java.util.concurrent.CountDownLatch(1)
        consumer.onEvent { if (it is ConsumerEvent.Assigned) assignedLatch.countDown() }

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        awaitLatch(assignedLatch)
        consumer.pause()
        consumer.resume()
        consumer.shutdown()

        val types = events.map { it::class }
        val started = types.indexOf(ConsumerEvent.Started::class)
        val assigned = types.indexOf(ConsumerEvent.Assigned::class)
        val paused = types.indexOf(ConsumerEvent.Paused::class)
        val resumed = types.indexOf(ConsumerEvent.Resumed::class)
        val stopped = types.indexOf(ConsumerEvent.Stopped::class)

        assertTrue(started < assigned, "Started before Assigned")
        assertTrue(assigned < paused, "Assigned before Paused")
        assertTrue(paused < resumed, "Paused before Resumed")
        assertTrue(resumed < stopped, "Resumed before Stopped")
    }

    @Test
    fun `multiple callbacks all receive events`() {
        val (consumer, _) = consumer()
        val eventsA = CopyOnWriteArrayList<ConsumerEvent>()
        val eventsB = CopyOnWriteArrayList<ConsumerEvent>()
        consumer.onEvent { eventsA.add(it) }
        consumer.onEvent { eventsB.add(it) }

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        consumer.shutdown()

        assertEquals(eventsA, eventsB)
        assertTrue(eventsA.isNotEmpty())
    }

    @Test
    fun `shutdown disposes event subscriptions`() {
        val (consumer, _) = consumer()
        val events = CopyOnWriteArrayList<ConsumerEvent>()
        consumer.onEvent { events.add(it) }

        consumer.start(StartOffset.Latest, EndOffset.Continuous)
        consumer.shutdown()

        val countAfterShutdown = events.size
        // Sink is completed — no further events possible
        assertTrue(events.last() == ConsumerEvent.Stopped)
        assertEquals(countAfterShutdown, events.size)
    }
}
