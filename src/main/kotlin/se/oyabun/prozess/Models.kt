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

import kotlin.time.Instant

/** Wraps a Kafka topic name. */
@JvmInline
value class Topic(val name: String)

/** A set of topic name strings. */
typealias Topics = Set<String>

/** A set of [Partition]s. */
typealias Partitions = Set<Partition>

/** A set of [Position]s (partition + offset pairs). */
typealias Positions = Set<Position>

/** Converts this set of positions to a map of partition → offset. */
fun Positions.asOffsets(): Offsets = associate { it.partition to it.offset }

/** A map of [Partition] to the committed/processed offset value. */
typealias Offsets = Map<Partition, Long>

/** A list of [Header] key/value pairs attached to a Kafka record. */
typealias Headers = List<Header>

/**
 * The deserialized key of a Kafka record.
 *
 * Kafka records may arrive without a key. This type makes that distinction
 * explicit at the type level so handlers never receive a nullable key.
 */
sealed interface Key<out K> {
    /** The record arrived without a key. */
    data object Missing : Key<Nothing>
    /** The record arrived with a key that was successfully deserialized to [value]. */
    data class Present<out K>(val value: K) : Key<K>
}

/** A single Kafka record header consisting of a string [key] and raw byte [value]. */
data class Header(
    val key: String,
    val value: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Header) return false
        return key == other.key && value.contentEquals(other.value)
    }
    override fun hashCode(): Int = 31 * key.hashCode() + value.contentHashCode()
}

/**
 * The payload of a received Kafka record.
 *
 * A record with a null value is a tombstone (deletion marker). A record with
 * a non-null value is a data record.
 */
sealed interface ReceivedMessage {
    /** A record with a non-null value containing raw [bytes]. */
    class Data(val bytes: ByteArray) : ReceivedMessage {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Data) return false
            return bytes.contentEquals(other.bytes)
        }
        override fun hashCode(): Int = bytes.contentHashCode()
    }
    /** A record with a null value — Kafka tombstone / deletion marker. */
    data object Tombstone : ReceivedMessage
}

/**
 * A raw Kafka record as received from the broker, before deserialization.
 *
 * @param K       the type of the deserialized key; [ByteArray] until a [Prozess.KeyDeserializer] is applied.
 * @param key     the record key, or [Key.Missing] if the Kafka key was null.
 * @param message the record payload — [ReceivedMessage.Data] or [ReceivedMessage.Tombstone].
 * @param position the partition and offset of this record.
 * @param headers the list of Kafka record headers.
 */
data class Received<out K>(
    val key: Key<K>,
    val message: ReceivedMessage,
    val position: Position,
    val headers: Headers = emptyList(),
)

/**
 * Controls when a consumer stops consuming.
 *
 * [Continuous] means the consumer runs indefinitely. [CatchUp] means the consumer
 * reads up to the end offsets that were current at subscription time and then stops.
 */
sealed interface EndOffset {
    /** The consumer runs indefinitely, processing new records as they arrive. */
    data object Continuous : EndOffset
    /**
     * The consumer reads up to the end offsets recorded at subscription time and
     * then completes normally. Useful for backfill or one-shot processing jobs.
     */
    data object CatchUp : EndOffset
}

/** Identifies a specific partition within a [Topic]. */
data class Partition(val id: Int, val topic: Topic)

/** The position of a record within a [Partition]: the partition and its zero-based [offset]. */
data class Position(val partition: Partition, val offset: Long)

/**
 * Kafka consumer group membership metadata.
 *
 * Required by [Prozess.Producer.sendOffsetsToTransaction] for exactly-once
 * transactional offset commits.
 */
data class GroupMember(
    val groupId: String,
    val generationId: Int,
    val memberId: String,
    val groupInstanceId: String? = null,
)

/**
 * Direct-access bridge to the underlying [org.apache.kafka.clients.consumer.KafkaConsumer]
 * for use inside rebalance callbacks.
 *
 * Rebalance callbacks fire synchronously on the Kafka polling thread. Operations here
 * bypass the single-threaded scheduler and call the consumer directly — they **must not**
 * go through the reactive [KafkaClient] to avoid deadlock.
 */
interface RebalanceContext {
    /** Returns the current fetch position for [partition]. */
    fun position(partition: Partition): Long
    /** Pauses fetching for [partitions] on the polling thread. */
    fun pause(partitions: Partitions)
    /** Commits [offsets] synchronously on the polling thread (last-chance commit on revocation). */
    fun commit(offsets: Offsets)
    /** Seeks the fetch position for each partition in [targets] on the polling thread. */
    fun seek(targets: Offsets)
}

/**
 * Callback interface for Kafka partition rebalance events.
 *
 * Implement this when you need to react to assignment changes — for example,
 * to commit offsets on revocation or to apply seeks on assignment.
 * Provided to [KafkaClient.subscribe].
 */
interface RebalanceListener {
    /**
     * Invoked when [partitions] are being revoked from this consumer.
     *
     * Called synchronously on the Kafka polling thread before the rebalance
     * completes. Use [context] to commit offsets synchronously.
     */
    fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions)

    /**
     * Invoked when [partitions] have been assigned to this consumer.
     *
     * Called synchronously on the Kafka polling thread after the rebalance
     * completes. Use [context] to seek to a specific offset if needed.
     */
    fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions)
}

/**
 * Controls where a consumer starts reading when it has no committed offset
 * (or when explicitly overriding the default start position).
 */
sealed interface StartOffset {
    /** Read from the earliest available offset. */
    data object Earliest : StartOffset
    /** Read only new records written after the consumer starts. */
    data object Latest : StartOffset
    /**
     * Read records written at or after [instant].
     *
     * If no record exists at or after the given timestamp, the consumer
     * seeks to the end of the partition.
     */
    data class AtTimestamp(val instant: Instant) : StartOffset
}
