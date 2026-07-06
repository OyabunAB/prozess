package se.oyabun.prozess

import kotlin.time.Instant


@JvmInline
value class Topic(val name: String)

typealias Topics = Set<String>

typealias Partitions = Set<Partition>

typealias Positions = Set<Position>

fun Positions.asOffsets(): Offsets = associate { it.partition to it.offset }

typealias Offsets = Map<Partition, Long>

data class Received(val key: String, val message: ByteArray, val position: Position) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as Received
        if (key != other.key) return false
        if (!message.contentEquals(other.message)) return false
        if (position != other.position) return false
        return true
    }
    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + message.contentHashCode()
        result = 31 * result + position.hashCode()
        return result
    }
}

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
