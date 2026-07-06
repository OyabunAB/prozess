package se.oyabun.prozess

import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.GroupedFlux
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.fromCallable
import reactor.util.retry.Retry
import se.oyabun.prozess.Prozess.Deserializer
import java.util.function.Function
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/** A deserialized message with its raw Kafka metadata. */
data class Processable<M>(
    val received: Received,
    val value: M,
)

/** Backoff policy for retrying failed message processing. */
data class RetryConfig(
    val minBackoff: Duration = 1.seconds,
    val maxBackoff: Duration = 30.seconds,
)

/** Processing pipeline for a single Kafka partition. */
interface Processor<M : Any> {
    fun process(partition: Flux<Received>): Flux<Position>
}

internal class ContiguousOffsetTracker {
    private val completed = mutableSetOf<Long>()
    private var watermark = 0L

    fun onCompleted(position: Position): Position? {
        val offset = position.offset
        if (offset < watermark) return null

        completed.add(offset)

        var advanced = false
        while (completed.remove(watermark)) {
            watermark++
            advanced = true
        }

        return if (advanced) Position(position.partition, watermark - 1) else null
    }
}

private fun <T : Any, V : Any> Flux<T>.flatMapSequential(maxConcurrency: Int, mapper: (T) -> Publisher<V>): Flux<V> =
    flatMapSequential(mapper, maxConcurrency)

private fun <T : Any, V : Any> Flux<T>.flatMap(concurrency: Int, mapper: (T) -> Publisher<V>): Flux<V> =
    flatMap(mapper, concurrency)

/**
 * Default [Processor] that deserializes messages, invokes a user handler,
 * and retries failures indefinitely with exponential backoff.
 *
 * Use the companion factory methods to create instances:
 * - [each] — process messages one at a time
 * - [batch] — process messages in atomic batches
 * - [groupedEach] — process each key group sequentially, groups concurrently
 * - [groupedBatch] — process key-grouped batches atomically
 */
class DefaultProcessor<M : Any> private constructor(
    private val pipeline: (Flux<Received>) -> Flux<Position>,
) : Processor<M> {

    override fun process(partition: Flux<Received>): Flux<Position> = pipeline(partition)

    companion object {
        private fun retrySpec(retryConfig: RetryConfig, label: String): reactor.util.retry.RetryBackoffSpec {
            val log = Logging.logger { }
            return Retry.backoff(Long.MAX_VALUE, retryConfig.minBackoff.toJavaDuration())
                .maxBackoff(retryConfig.maxBackoff.toJavaDuration())
                .doBeforeRetry { signal ->
                    log.retry.retrying(label, "", signal.totalRetries(), signal.failure())
                }
        }

        /** Processes each message individually. Messages within a partition preserve Kafka offset order. */
        fun <M : Any> each(
            deserializer: Deserializer<M>,
            handler: (Processable<M>) -> Unit,
            retryConfig: RetryConfig = RetryConfig(),
            maxConcurrency: Int = 1,
        ): DefaultProcessor<M> {
            val spec = retrySpec(retryConfig, "process")

            fun processOne(received: Received): Mono<Position> =
                fromCallable { deserializer(received.message) }
                    .map { Processable(received, it) }
                    .map { p -> handler(p); received.position }
                    .retryWhen(spec)

            return DefaultProcessor { partition ->
                partition.flatMapSequential(maxConcurrency) { received ->
                    processOne(received)
                }
            }
        }

        /**
         * Buffers messages into atomic batches of [batchSize] or [batchDuration] timeout,
         * then processes each batch atomically — all succeed or the entire batch is retried.
         */
        fun <M : Any> batch(
            deserializer: Deserializer<M>,
            handler: (List<Processable<M>>) -> Unit,
            batchSize: Int,
            batchDuration: Duration? = null,
            retryConfig: RetryConfig = RetryConfig(),
            maxConcurrency: Int = 1,
        ): DefaultProcessor<M> {
            val spec = retrySpec(retryConfig, "batch")

            fun processBatch(messages: List<Received>): Flux<Position> =
                fromCallable {
                    val processables = messages.map { Processable(it, deserializer(it.message)) }
                    handler(processables)
                    messages.map { it.position }
                }
                    .flatMapMany { Flux.fromIterable(it) }
                    .retryWhen(spec)

            return DefaultProcessor { partition ->
                val batched = if (batchDuration != null) {
                    partition.bufferTimeout(batchSize, batchDuration.toJavaDuration())
                } else {
                    partition.buffer(batchSize)
                }
                batched.flatMapSequential(maxConcurrency) { messages ->
                    processBatch(messages)
                }
            }
        }

        /**
         * Groups messages by [keyExtractor] and processes each group sequentially (concatMap),
         * with up to [concurrency] groups running in parallel. Emits positions only when
         * contiguous prefix is complete, preventing offset gaps.
         */
        fun <K : Any, M : Any> groupedEach(
            deserializer: Deserializer<M>,
            keyExtractor: (Processable<M>) -> K,
            handler: (K, Processable<M>) -> Unit,
            retryConfig: RetryConfig = RetryConfig(),
            concurrency: Int = 1,
        ): DefaultProcessor<M> {
            val spec = retrySpec(retryConfig, "grouped")

            fun partitionPipeline(partition: Flux<Received>): Flux<Position> {
                val tracker = ContiguousOffsetTracker()
                return partition
                    .flatMap { received: Received ->
                        fromCallable { Processable(received, deserializer(received.message)) }
                            .retryWhen(spec)
                    }
                    .groupBy { processable -> keyExtractor(processable) }
                    .flatMap(concurrency) { grouped: GroupedFlux<K, Processable<M>> ->
                        grouped.concatMap { processable: Processable<M> ->
                            fromCallable { handler(grouped.key(), processable); processable.received.position }
                                .retryWhen(spec)
                        }
                    }
                    .mapNotNull { position -> tracker.onCompleted(position) }
            }

            return DefaultProcessor { partition -> partitionPipeline(partition) }
        }

        /**
         * Groups messages by [keyExtractor], buffers into atomic batches per group, and processes
         * each batch atomically. Up to [concurrency] groups run in parallel. Uses contiguous offset
         * tracking for safe committing.
         */
        fun <K : Any, M : Any> groupedBatch(
            deserializer: Deserializer<M>,
            keyExtractor: (Processable<M>) -> K,
            handler: (K, List<Processable<M>>) -> Unit,
            batchSize: Int,
            batchDuration: Duration? = null,
            retryConfig: RetryConfig = RetryConfig(),
            concurrency: Int = 1,
        ): DefaultProcessor<M> {
            val spec = retrySpec(retryConfig, "grouped-batch")

            fun partitionPipeline(partition: Flux<Received>): Flux<Position> {
                val tracker = ContiguousOffsetTracker()
                return partition
                    .flatMap { received: Received ->
                        fromCallable { Processable(received, deserializer(received.message)) }
                            .retryWhen(spec)
                    }
                    .groupBy { processable -> keyExtractor(processable) }
                    .flatMap(concurrency) { grouped: GroupedFlux<K, Processable<M>> ->
                        val batched: Flux<List<Processable<M>>> = if (batchDuration != null)
                            grouped.bufferTimeout(batchSize, batchDuration.toJavaDuration())
                        else
                            grouped.buffer(batchSize)

                        batched.concatMap { processables: List<Processable<M>> ->
                            fromCallable {
                                handler(grouped.key(), processables)
                                processables.map { it.received.position }
                            }
                                .flatMapMany { Flux.fromIterable(it) }
                                .retryWhen(spec)
                        }
                    }
                    .mapNotNull { position -> tracker.onCompleted(position) }
            }

            return DefaultProcessor { partition -> partitionPipeline(partition) }
        }
    }
}
