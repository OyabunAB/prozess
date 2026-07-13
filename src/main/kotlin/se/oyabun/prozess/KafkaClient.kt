package se.oyabun.prozess

import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

interface KafkaClient : ShutdownableClient {

    val pollInterval: Duration

    fun partitionsFor(topics: Topics): One<Partitions>

    fun beginningOffsets(partitions: Partitions): One<Positions>

    fun endOffsets(partitions: Partitions): One<Positions>

    fun subscribe(topics: Topics, listener: RebalanceListener): One<Topics>

    fun offsetsForTimes(partitions: Partitions, timestamp: Instant): One<Positions>

    fun positionOf(partition: Partition): One<Long>

    fun seek(offsets: Offsets): None<Offsets>

    fun endOffsetOf(partition: Partition): One<Long>

    fun poll(timeout: Duration = 100.milliseconds): One<List<Received<ByteArray>>>

    fun pause(partitions: Partitions): One<Partitions>

    fun resume(partitions: Partitions): One<Partitions>

    fun commit(offsets: Offsets, metadata: String = ""): One<Offsets>

    fun committed(partitions: Partitions): One<Offsets>
}
