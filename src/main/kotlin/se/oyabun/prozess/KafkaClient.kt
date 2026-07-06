package se.oyabun.prozess

import reactor.core.publisher.Mono
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

interface KafkaClient : ShutdownableClient {

    val pollInterval: Duration

    fun partitionsFor(topics: Topics): Mono<Partitions>

    fun beginningOffsets(partitions: Partitions): Mono<Positions>

    fun endOffsets(partitions: Partitions): Mono<Positions>

    fun subscribe(topics: Topics, listener: RebalanceListener): Mono<Topics>

    fun offsetsForTimes(partitions: Partitions, timestamp: Instant): Mono<Positions>

    fun positionOf(partition: Partition): Mono<Long>

    fun seek(offsets: Offsets): Mono<Void>

    fun endOffsetOf(partition: Partition): Mono<Long>

    fun poll(timeout: Duration = 100.milliseconds): Mono<List<Received>>

    fun pause(partitions: Partitions): Mono<Partitions>

    fun resume(partitions: Partitions): Mono<Partitions>

    fun commit(offsets: Offsets, metadata: String = ""): Mono<Offsets>

    fun committed(partitions: Partitions): Mono<Offsets>
}
