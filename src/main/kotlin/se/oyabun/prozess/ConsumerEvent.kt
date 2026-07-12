package se.oyabun.prozess

/**
 * Events emitted by a consumer as it transitions through Kafka lifecycle states.
 *
 * Subscribe to [StreamingConsumer.events] (reactive) or register a callback via
 * [Prozess.Consumer.onEvent] (blocking) to observe these transitions.
 */
sealed interface ConsumerEvent {
    data class Assigned(val partitions: Partitions)  : ConsumerEvent
    data class Revoked(val partitions: Partitions)   : ConsumerEvent
    data class Committed(val offsets: Offsets)        : ConsumerEvent
    data object Started  : ConsumerEvent
    data object Paused   : ConsumerEvent
    data object Resumed  : ConsumerEvent
    data object Stopped  : ConsumerEvent
}
