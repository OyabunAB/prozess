package se.oyabun.prozess

import kotlin.time.Instant


@JvmInline
value class Topic(val name: String)

typealias Topics = Set<String>

typealias Partitions = Set<Partition>

typealias Positions = Set<Position>

fun Positions.asOffsets(): Offsets = associate { it.partition to it.offset }

typealias Offsets = Map<Partition, Long>
typealias Headers = List<Header>

sealed interface ReceivedKey {
    data class Value(val key: String) : ReceivedKey
    data object None : ReceivedKey
}

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

sealed interface ReceivedMessage {
    class Data(val bytes: ByteArray) : ReceivedMessage {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Data) return false
            return bytes.contentEquals(other.bytes)
        }
        override fun hashCode(): Int = bytes.contentHashCode()
    }
    data object Tombstone : ReceivedMessage
}

data class Received(
    val key: ReceivedKey,
    val message: ReceivedMessage,
    val position: Position,
    val headers: Headers = emptyList(),
)

sealed interface EndOffset {
    data object Continuous : EndOffset
    data object CatchUp : EndOffset
}

data class Partition(val id: Int, val topic: Topic)

data class Position(val partition: Partition, val offset: Long)

data class GroupMember(
    val groupId: String,
    val generationId: Int,
    val memberId: String,
    val groupInstanceId: String? = null,
)

interface RebalanceContext {
    fun position(partition: Partition): Long
    fun pause(partitions: Partitions)
    fun commit(offsets: Offsets)
    fun seek(targets: Offsets)
}

interface RebalanceListener {
    fun onPartitionsRevoked(context: RebalanceContext, partitions: Partitions)
    fun onPartitionsAssigned(context: RebalanceContext, partitions: Partitions)
}

sealed interface StartOffset {
    data object Earliest : StartOffset
    data object Latest : StartOffset
    data class AtTimestamp(val instant: Instant) : StartOffset
}
