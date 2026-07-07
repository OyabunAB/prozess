package se.oyabun.prozess

import mu.KLogger
import mu.KotlinLogging

class Logger(private val delegate: KLogger) {
    val kafka = Kafka()
    val retry = Retry()
    val processing = Processing()

    inner class Kafka {
        fun subscribed(instance: String, topics: Any) = delegate.info { "$instance subscribed to $topics" }
        fun shuttingDown(instance: String, topics: Any) = delegate.info { "$instance shutting down subscription to $topics" }
        fun consumerFailed(instance: String, topics: Any, cause: Throwable) =
            delegate.warn(cause) { "$instance failed while consuming $topics" }
        fun assigned(instance: String, partitions: Any) = delegate.info { "$instance was assigned to $partitions" }
        fun revoked(instance: String, partitions: Any) = delegate.info { "$instance was revoked from $partitions" }
        fun emitted(instance: String, position: Any) = delegate.trace { "$instance emitted $position" }
        fun polled(instance: String, count: Int) = delegate.trace { "$instance polled $count records (buffering)" }
        fun committed(instance: String, partitions: Any) =
            delegate.trace { "$instance committed $partitions successfully" }
        fun commitFailed(instance: String, partitions: Any, cause: Throwable) =
            delegate.warn(cause) { "$instance failed to commit $partitions" }
        fun listenerRetrying(instance: String, topics: Any, cause: Throwable) =
            delegate.debug(cause) { "$instance retrying failed listener on $topics" }
        fun listenerRestarted(instance: String, topics: Any) =
            delegate.debug { "$instance restarted listening to $topics" }
        fun commitExhausted(instance: String, cause: Throwable) =
            delegate.warn(cause) { "$instance commit exhausted retries, skipping batch" }
        fun commitBackpressure(instance: String, partition: Partition) =
            delegate.warn { "$instance commit pipeline stalled — backpressuring $partition" }
        fun pollExhausted(instance: String, cause: Throwable) =
            delegate.warn(cause) { "$instance poll exhausted retries, skipping tick" }
        fun bufferDrained(instance: String, assignments: Any, size: Int) =
            delegate.trace { "$instance resuming $assignments - buffer drained to $size" }
        fun bufferSaturated(instance: String, assignments: Any, size: Int) =
            delegate.trace { "$instance pausing $assignments - buffer saturated at $size" }
        fun completed(component: String) = delegate.trace { "$component completed" }
        fun terminatedUnexpectedly(component: String, cause: Throwable) =
            delegate.error(cause) { "$component terminated unexpectedly" }
        fun componentRestarting(instance: String, component: String, cause: Throwable) =
            delegate.warn(cause) { "$instance $component restarting after error" }
        fun processingRetrying(instance: String, partition: Partition, attempt: Long, cause: Throwable) {
            when {
                attempt < 10L -> delegate.trace(cause) { "$instance retrying $partition (attempt $attempt)" }
                attempt == 10L -> delegate.warn(cause) { "$instance partition $partition stuck for $attempt retries" }
                attempt % 100L == 0L -> delegate.error(cause) { "$instance partition $partition critical — $attempt retries" }
            }
        }
    }

    inner class Retry {
        fun retrying(id: Any, attempt: String, totalRetries: Long, cause: Throwable) {
            if (delegate.isTraceEnabled) {
                delegate.trace(cause) { "[$id] Retrying call $attempt" }
            } else if (totalRetries == 0L) {
                delegate.debug { "[$id] Retrying $attempt due to ${cause.javaClass.simpleName}" }
            }
        }

        fun exhausted(id: Any, totalRetries: Long, cause: Throwable) = delegate.debug {
            "[$id] Retries exhausted after $totalRetries retries due to ${cause.javaClass.simpleName}"
        }

        fun recovered(id: Any, totalRetries: Long) = delegate.trace { "[$id] Recovered after $totalRetries retries." }
    }

    inner class Processing {
        fun tombstone(position: Position) =
            delegate.info { "tombstone at $position" }

        fun poisonPill(position: Position, reason: String?) =
            delegate.warn { "skipping poison pill at $position${reason?.let { ": $it" } ?: ""}" }
    }
}

object Logging {
    fun logger(func: () -> Unit): Logger = Logger(KotlinLogging.logger(func))
}
