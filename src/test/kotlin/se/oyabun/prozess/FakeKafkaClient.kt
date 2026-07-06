package se.oyabun.prozess

import reactor.core.publisher.Mono
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

    override fun partitionsFor(topics: Topics): Mono<Partitions> = Mono.just(
        topics.flatMap { t -> topicsToPartitions.getOrDefault(t, emptySet()) }.toSet()
    )

    override fun beginningOffsets(partitions: Partitions): Mono<Positions> =
        Mono.just(emptySet())

    override fun endOffsets(partitions: Partitions): Mono<Positions> =
        Mono.just(emptySet())

    override fun subscribe(topics: Topics, listener: RebalanceListener): Mono<Topics> {
        subscribedTopics = topics
        val partitions = topics.flatMap { t ->
            topicsToPartitions.getOrDefault(t, emptySet())
        }.toSet()
        if (partitions.isNotEmpty()) {
            listener.onPartitionsAssigned(FakeRebalanceContext(), partitions)
            initialAssignTriggered = true
        }
        return Mono.just(topics)
    }

    override fun offsetsForTimes(partitions: Partitions, timestamp: Instant): Mono<Positions> =
        Mono.just(emptySet())

    override fun positionOf(partition: Partition): Mono<Long> =
        Mono.just(positionOfResult(partition))

    override fun endOffsetOf(partition: Partition): Mono<Long> =
        Mono.just(endOffsetOfResult(partition))

    override fun poll(timeout: Duration): Mono<List<Received>> = Mono.defer {
        if (pollErrorRemaining > 0) {
            pollErrorRemaining--
            Mono.error(pollError)
        } else {
            val entry = pollQueue.poll()
            if (entry != null) Mono.just(entry) else Mono.just(emptyList())
        }
    }

    override fun pause(partitions: Partitions): Mono<Partitions> {
        pausedPartitions.add(partitions)
        return Mono.just(partitions)
    }

    override fun resume(partitions: Partitions): Mono<Partitions> {
        resumedPartitions.add(partitions)
        return Mono.just(partitions)
    }

    override fun commit(offsets: Offsets, metadata: String): Mono<Offsets> = Mono.defer {
        if (commitFailuresRemaining > 0) {
            commitFailuresRemaining--
            Mono.error(RuntimeException("fake commit failure"))
        } else {
            commits.add(offsets to metadata)
            Mono.just(offsets)
        }
    }

    override fun committed(partitions: Partitions): Mono<Offsets> =
        Mono.just(emptyMap())

    override fun seek(offsets: Offsets): Mono<Void> {
        positions.putAll(offsets)
        return Mono.empty()
    }

    override fun wakeup() {
        wakeupCount++
    }

    override fun close(timeout: Duration): Mono<Void> {
        closed = true
        return Mono.empty()
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
