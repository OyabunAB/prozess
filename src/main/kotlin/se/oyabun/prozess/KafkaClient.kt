/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.oyabun.prozess

import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

/**
 * Reactive interface contract for all Kafka consumer operations.
 *
 * Every method returns a typed aelv publisher ([One] for single-result operations,
 * [None] for void operations). This keeps all Kafka I/O composable within the
 * aelv pipeline and allows the production implementation to be replaced with a
 * fake in tests.
 *
 * All implementations must serialise calls to the underlying [org.apache.kafka.clients.consumer.KafkaConsumer]
 * because Kafka's consumer is not thread-safe.
 */
interface KafkaClient : ShutdownableClient {

    /** The configured poll interval passed to each [poll] invocation. */
    val pollInterval: Duration

    /** Returns the set of [Partition]s for the given [topics]. */
    fun partitionsFor(topics: Topics): One<Partitions>

    /** Returns the beginning (earliest available) offset for each of the given [partitions]. */
    fun beginningOffsets(partitions: Partitions): One<Positions>

    /** Returns the end (next-to-be-written) offset for each of the given [partitions]. */
    fun endOffsets(partitions: Partitions): One<Positions>

    /**
     * Subscribes to [topics] and registers [listener] for partition rebalance callbacks.
     *
     * Returns the subscribed topic set on success.
     */
    fun subscribe(topics: Topics, listener: RebalanceListener): One<Topics>

    /**
     * Returns the offset for each partition that corresponds to the given [timestamp].
     *
     * Partitions with no message at or after [timestamp] are omitted from the result.
     */
    fun offsetsForTimes(partitions: Partitions, timestamp: Instant): One<Positions>

    /** Returns the current fetch position (next offset to be returned by poll) for [partition]. */
    fun positionOf(partition: Partition): One<Long>

    /** Seeks the fetch position for each partition in [offsets] to the given offset. */
    fun seek(offsets: Offsets): None<Offsets>

    /** Returns the end offset (next-to-be-written) for a single [partition]. */
    fun endOffsetOf(partition: Partition): One<Long>

    /**
     * Polls Kafka for new records with the given [timeout].
     *
     * Returns an empty list when [org.apache.kafka.common.errors.WakeupException] is raised
     * (i.e. during shutdown).
     */
    fun poll(timeout: Duration = 100.milliseconds): One<List<Received<ByteArray>>>

    /** Pauses fetching for the given [partitions]. Returns the paused set. */
    fun pause(partitions: Partitions): One<Partitions>

    /** Resumes fetching for the given [partitions]. Returns the resumed set. */
    fun resume(partitions: Partitions): One<Partitions>

    /**
     * Commits [offsets] synchronously to Kafka with optional [metadata].
     *
     * Returns the committed offsets map on success.
     */
    fun commit(offsets: Offsets, metadata: String = ""): One<Offsets>

    /**
     * Returns the last committed offset for each of the given [partitions].
     *
     * Partitions with no committed offset are omitted from the result.
     */
    fun committed(partitions: Partitions): One<Offsets>
}
