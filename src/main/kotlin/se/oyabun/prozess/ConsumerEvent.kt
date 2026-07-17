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

/**
 * Events emitted by a consumer as it transitions through Kafka lifecycle states.
 *
 * Subscribe to [StreamingConsumer.events] (reactive) or register a callback via
 * [Prozess.Consumer.onEvent] (blocking) to observe these transitions.
 */
sealed interface ConsumerEvent {
    /** Emitted when Kafka assigns [partitions] to this consumer. */
    data class Assigned(val partitions: Partitions)  : ConsumerEvent
    /** Emitted when Kafka revokes [partitions] from this consumer. */
    data class Revoked(val partitions: Partitions)   : ConsumerEvent
    /** Emitted after a successful Kafka offset commit with the committed [offsets]. */
    data class Committed(val offsets: Offsets)        : ConsumerEvent
    /** Emitted once when the consumer has started polling. */
    data object Started  : ConsumerEvent
    /** Emitted when the consumer is externally paused. */
    data object Paused   : ConsumerEvent
    /** Emitted when the consumer is externally resumed. */
    data object Resumed  : ConsumerEvent
    /** Emitted when [Prozess.Consumer.shutdown] has completed. */
    data object Stopped  : ConsumerEvent
}
