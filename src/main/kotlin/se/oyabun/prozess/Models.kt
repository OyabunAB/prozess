package se.oyabun.prozess

import java.time.Instant

@JvmInline
value class Topic(val name: String)

typealias Topics = Set<String>

typealias Partitions = Set<Partition>

typealias Positions = Set<Position>

fun Positions.asOffsets(): Offsets = associate { it.partition to it.offset }

typealias Offsets = Map<Partition, Long>

data class Received(val key: String, val message: ByteArray, val position: Position)

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

internal interface RebalanceContext {
    fun position(partition: Partition): Long
    fun pause(partitions: Partitions)
    fun commit(offsets: Offsets)
    fun seek(targets: Offsets)
}

sealed interface StartOffset {
    data object Earliest : StartOffset
    data object Latest : StartOffset
    data class AtTimestamp(val instant: Instant) : StartOffset
}
