package se.oyabun.prozess

import org.slf4j.LoggerFactory

class Logger(private val delegate: org.slf4j.Logger) {
    val kafka      = Kafka()
    val retry      = Retry()
    val processing = Processing()

    inner class Kafka {
        fun subscribed(instance: String, topics: Any)                             = delegate.info("{} subscribed to {}", instance, topics)
        fun shuttingDown(instance: String, topics: Any)                           = delegate.info("{} shutting down subscription to {}", instance, topics)
        fun consumerFailed(instance: String, topics: Any, cause: Throwable)       = delegate.warn("$instance failed while consuming $topics", cause)
        fun assigned(instance: String, partitions: Any)                           = delegate.info("{} was assigned to {}", instance, partitions)
        fun revoked(instance: String, partitions: Any)                            = delegate.info("{} was revoked from {}", instance, partitions)
        fun emitted(instance: String, position: Any)                              = delegate.trace("{} emitted {}", instance, position)
        fun polled(instance: String, count: Int)                                  = delegate.trace("{} polled {} records (buffering)", instance, count)
        fun committed(instance: String, partitions: Any)                          = delegate.trace("{} committed {} successfully", instance, partitions)
        fun commitFailed(instance: String, partitions: Any, cause: Throwable)     = delegate.warn("$instance failed to commit $partitions", cause)
        fun listenerRetrying(instance: String, topics: Any, cause: Throwable)     = delegate.debug("$instance retrying failed listener on $topics", cause)
        fun listenerRestarted(instance: String, topics: Any)                      = delegate.debug("{} restarted listening to {}", instance, topics)
        fun commitExhausted(instance: String, cause: Throwable)                   = delegate.warn("$instance commit exhausted retries, skipping batch", cause)
        fun commitBackpressure(instance: String, partition: Partition)            = delegate.warn("{} commit pipeline stalled — backpressuring {}", instance, partition)
        fun pollExhausted(instance: String, cause: Throwable)                     = delegate.warn("$instance poll exhausted retries, skipping tick", cause)
        fun bufferDrained(instance: String, assignments: Any, size: Int)          = delegate.trace("{} resuming {} - buffer drained to {}", instance, assignments, size)
        fun bufferSaturated(instance: String, assignments: Any, size: Int)        = delegate.trace("{} pausing {} - buffer saturated at {}", instance, assignments, size)
        fun completed(component: String)                                          = delegate.trace("{} completed", component)
        fun terminatedUnexpectedly(component: String, cause: Throwable)           = delegate.error("$component terminated unexpectedly", cause)
        fun componentRestarting(instance: String, component: String, cause: Throwable) = delegate.warn("$instance $component restarting after error", cause)
        fun processingRetrying(instance: String, partition: Partition, attempt: Long, cause: Throwable) {
            when {
                attempt < 10L        -> delegate.trace("$instance retrying $partition (attempt $attempt)", cause)
                attempt == 10L       -> delegate.warn("$instance partition $partition stuck for $attempt retries", cause)
                attempt % 100L == 0L -> delegate.error("$instance partition $partition critical — $attempt retries", cause)
            }
        }
    }

    inner class Retry {
        fun retrying(id: Any, attempt: String, totalRetries: Long, cause: Throwable) {
            if (delegate.isTraceEnabled) {
                delegate.trace("[$id] Retrying call $attempt", cause)
            } else if (totalRetries == 0L) {
                delegate.debug("[{}] Retrying {} due to {}", id, attempt, cause.javaClass.simpleName)
            }
        }

        fun exhausted(id: Any, totalRetries: Long, cause: Throwable) =
            delegate.debug("[{}] Retries exhausted after {} retries due to {}", id, totalRetries, cause.javaClass.simpleName)

        fun recovered(id: Any, totalRetries: Long) =
            delegate.trace("[{}] Recovered after {} retries.", id, totalRetries)
    }

    inner class Processing {
        fun tombstone(position: Position) =
            delegate.info("tombstone at {}", position)

        fun poisonPill(position: Position, reason: String?) =
            delegate.warn("skipping poison pill at {}{}", position, reason?.let { ": $it" } ?: "")
    }
}

object Logging {
    fun logger(func: () -> Unit): Logger {
        val clazz = func.javaClass.enclosingClass ?: func.javaClass
        return Logger(LoggerFactory.getLogger(clazz))
    }
}

internal fun shortId(): String = java.util.UUID.randomUUID().toString().replace("-", "").take(8)
