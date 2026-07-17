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

import org.slf4j.LoggerFactory

/**
 * Structured logging facade for all Prozess components.
 *
 * Log levels:
 * - `trace` — high-frequency events (polls, commits, sends)
 * - `debug` — lower-frequency diagnostic events (buffer drain, restart, recovery)
 * - `info`  — lifecycle transitions (subscribe, assign, revoke, shutdown)
 * - `warn`  — degraded but recoverable state (commit failure, handler retrying, buffer saturated)
 * - `error` — component failure requiring operator attention
 */
class Logger(private val delegate: org.slf4j.Logger) {
    val kafka      = Kafka()
    val producer   = Producer()
    val processing = Processing()

    inner class Kafka {
        fun subscribed(instance: String, topics: Any)                              = delegate.info("{} subscribed to {}", instance, topics)
        fun shuttingDown(instance: String, topics: Any)                            = delegate.info("{} shutting down subscription to {}", instance, topics)
        fun assigned(instance: String, partitions: Any)                            = delegate.info("{} was assigned to {}", instance, partitions)
        fun revoked(instance: String, partitions: Any)                             = delegate.info("{} was revoked from {}", instance, partitions)
        fun completed(component: String)                                           = delegate.debug("{} completed", component)
        fun terminatedUnexpectedly(component: String, cause: Throwable)            = delegate.error("$component terminated unexpectedly", cause)
        /** Consumer pipeline restarted after a failure. */
        fun listenerRestarted(instance: String, topics: Any)                       = delegate.info("{} restarted subscription to {}", instance, topics)
        fun polled(instance: String, count: Int)                                   = delegate.trace("{} polled {} records (buffering)", instance, count)
        /** Buffer hit high-water mark — partitions paused until it drains. */
        fun bufferSaturated(instance: String, assignments: Any, size: Int)         = delegate.warn("{} buffer saturated at {} records — pausing {}", instance, size, assignments)
        /** Buffer drained to low-water mark — partitions resumed. */
        fun bufferDrained(instance: String, assignments: Any, size: Int)           = delegate.debug("{} buffer drained to {} records — resuming {}", instance, size, assignments)
        fun committed(instance: String, partitions: Any)                           = delegate.trace("{} committed {} successfully", instance, partitions)
        fun commitFailed(instance: String, partitions: Any, cause: Throwable)      = delegate.warn("$instance failed to commit $partitions", cause)
        fun consumerFailed(instance: String, topics: Any, cause: Throwable)        = delegate.warn("$instance consumer error on {}, will retry", topics, cause)
    }

    inner class Producer {
        fun sent(instance: String, offset: Long)                                   = delegate.trace("{} sent record at offset {}", instance, offset)
        fun sendFailed(instance: String, cause: Throwable)                         = delegate.warn("$instance failed to send record", cause)
        fun sendRetrying(instance: String, attempt: Long, cause: Throwable) {
            when {
                attempt < 3L -> delegate.warn("$instance send retrying (attempt $attempt) due to ${cause.javaClass.simpleName}")
                else         -> delegate.error("$instance send stuck at attempt $attempt", cause)
            }
        }
        fun sendRecovered(instance: String, attempts: Long)                        = delegate.info("{} send recovered after {} retries", instance, attempts)
    }

    inner class Processing {
        /** Record had a null Kafka value (tombstone / delete marker). */
        fun tombstone(position: Position)                    = delegate.info("tombstone at {}", position)
        /** Record skipped — deserialization returned PoisonPill. */
        fun poisonPill(position: Position, reason: String?) = delegate.warn("skipping poison pill at {}{}", position, reason?.let { ": $it" } ?: "")
        /**
         * Handler threw and the record is being retried.
         * Escalates from warn → error at attempt 10, then error every 100 attempts.
         */
        fun handlerRetrying(position: Position, attempt: Long, cause: Throwable) {
            when {
                attempt < 10L        -> delegate.warn("handler retrying at {} (attempt {}): {}", position, attempt, cause.javaClass.simpleName)
                attempt == 10L       -> delegate.error("handler stuck at {} after {} attempts", position, attempt, cause)
                attempt % 100L == 0L -> delegate.error("handler critical at {} — {} retries", position, attempt, cause)
            }
        }
        fun handlerRecovered(position: Position, attempts: Long) = delegate.info("handler recovered at {} after {} retries", position, attempts)
    }
}

object Logging {
    fun logger(func: () -> Unit): Logger {
        val clazz = func.javaClass.enclosingClass ?: func.javaClass
        return Logger(LoggerFactory.getLogger(clazz))
    }
}

internal fun shortId(): String = java.util.UUID.randomUUID().toString().replace("-", "").take(8)
