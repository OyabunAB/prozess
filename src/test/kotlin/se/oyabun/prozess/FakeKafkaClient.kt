package se.oyabun.prozess

import se.oyabun.aelv.None
import se.oyabun.aelv.One
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

internal class FakeKafkaClient : KafkaClient {

    override val pollInterval: Duration = 100.milliseconds

    private val pollQueue = ConcurrentLinkedQueue<List<Received>>()
    private var pollError = RuntimeException()
    private var pollErrorRemaining = 0
    private val topicsToPartitions = mutableMapOf<String, Partitions>()
    private var positionOfResult: (Partition) -> Long = { 0L }
    private var endOffsetOfResult: (Partition) -> Long = { 0L }

    var commitFailuresRemaining = 0

    val commits = ConcurrentLinkedQueue<Pair<Offsets, String>>()
    val pausedPartitions = ConcurrentLinkedQueue<Partitions>()
    val resumedPartitions = ConcurrentLinkedQueue<Partitions>()

    var wakeupCount = 0
    var closed = false
    var subscribedTopics: Topics = emptySet()
    var initialAssignTriggered = false

    private val positions = mutableMapOf<Partition, Long>()

    fun queuePollResults(vararg results: List<Received>) {
        results.forEach { pollQueue.add(it) }
    }

    fun setPartitionsFor(topic: String, partitions: Partitions) {
        topicsToPartitions[topic] = partitions
    }

    fun setPositionOfResult(block: (Partition) -> Long) {
        positionOfResult = block
    }

    fun setEndOffsetOfResult(block: (Partition) -> Long) {
        endOffsetOfResult = block
    }

    fun storedPosition(partition: Partition): Long = positions.getOrDefault(partition, 0L)

    fun setStoredPosition(partition: Partition, offset: Long) {
        positions[partition] = offset
    }

    fun failNextPoll(cause: RuntimeException = RuntimeException("poll failed")) {
        pollError = cause
        pollErrorRemaining = 1
    }

    fun failNextCommit() {
        commitFailuresRemaining++
    }

    override fun partitionsFor(topics: Topics): One<Partitions> = One.of(
        topics.flatMap { t -> topicsToPartitions.getOrDefault(t, emptySet()) }.toSet()
    )

    override fun beginningOffsets(partitions: Partitions): One<Positions> =
        One.of(emptySet())

    override fun endOffsets(partitions: Partitions): One<Positions> =
        One.of(emptySet())

    override fun subscribe(topics: Topics, listener: RebalanceListener): One<Topics> {
        subscribedTopics = topics
        val partitions = topics.flatMap { t ->
            topicsToPartitions.getOrDefault(t, emptySet())
        }.toSet()
        if (partitions.isNotEmpty()) {
            listener.onPartitionsAssigned(FakeRebalanceContext(), partitions)
            initialAssignTriggered = true
        }
        return One.of(topics)
    }

    override fun offsetsForTimes(partitions: Partitions, timestamp: Instant): One<Positions> =
        One.of(emptySet())

    override fun positionOf(partition: Partition): One<Long> =
        One.of(positionOfResult(partition))

    override fun endOffsetOf(partition: Partition): One<Long> =
        One.of(endOffsetOfResult(partition))

    override fun poll(timeout: Duration): One<List<Received>> = One.defer {
        if (pollErrorRemaining > 0) {
            pollErrorRemaining--
            throw pollError
        } else {
            val entry = pollQueue.poll()
            entry ?: emptyList()
        }
    }

    override fun pause(partitions: Partitions): One<Partitions> {
        pausedPartitions.add(partitions)
        return One.of(partitions)
    }

    override fun resume(partitions: Partitions): One<Partitions> {
        resumedPartitions.add(partitions)
        return One.of(partitions)
    }

    override fun commit(offsets: Offsets, metadata: String): One<Offsets> = One.defer {
        if (commitFailuresRemaining > 0) {
            commitFailuresRemaining--
            throw RuntimeException("fake commit failure")
        } else {
            commits.add(offsets to metadata)
            offsets
        }
    }

    override fun committed(partitions: Partitions): One<Offsets> =
        One.of(emptyMap())

    override fun seek(offsets: Offsets): None<Offsets> = None.defer {
        positions.putAll(offsets)
    }

    override fun wakeup() {
        wakeupCount++
    }

    override fun close(timeout: Duration): None<Unit> = None.defer {
        closed = true
    }

    fun rebalanceContext(): RebalanceContext = FakeRebalanceContext()

    inner class FakeRebalanceContext : RebalanceContext {
        override fun position(partition: Partition): Long = positions.getOrDefault(partition, 0L)

        override fun pause(partitions: Partitions) {
            pausedPartitions.add(partitions)
        }

        override fun commit(offsets: Offsets) {
            commits.add(offsets to "rebalance-context")
        }

        override fun seek(targets: Offsets) {
            targets.forEach { (p, o) ->
                positions[p] = o
            }
        }
    }
}
