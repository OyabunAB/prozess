package se.oyabun.prozess

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.retry.RetryBackoffSpec
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.incrementAndFetch
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

object Retrying {
    val log = Logging.logger { }
    val fixedDelay: Duration = 3.seconds
    val fewRetries: Long = 3L
    val infiniteRetries: Long = Long.MAX_VALUE
    val anyException: (Throwable) -> Boolean = { true }

    @OptIn(ExperimentalAtomicApi::class)
    fun <X : Any> Mono<X>.withRetries(
        id: Any,
        maxAttempts: Long = 100L,
        minBackoff: Duration? = null,
        retryOn: (Throwable) -> Boolean,
        fallback: X? = null,
    ): Mono<X> {
        val retrySpec = backingOff(minBackoff, maxAttempts, retryOn, id)
        val retried = retryWhen(retrySpec).subscribeOn(Schedulers.boundedElastic())
        return fallback?.let { retried.onErrorReturn(retryOn, it) } ?: retried
    }

    @OptIn(ExperimentalAtomicApi::class)
    fun <X : Any> Flux<X>.withRetries(
        id: Any,
        maxAttempts: Long = 100L,
        minBackoff: Duration? = null,
        retryOn: (Throwable) -> Boolean,
        fallback: X? = null,
    ): Flux<X> {
        val retrySpec = backingOff(minBackoff, maxAttempts, retryOn, id)
        val retried = retryWhen(retrySpec).subscribeOn(Schedulers.boundedElastic())
        return fallback?.let { retried.onErrorReturn(retryOn, it) } ?: retried
    }

    @OptIn(ExperimentalAtomicApi::class)
    private fun backingOff(
        minBackoff: Duration?,
        maxAttempts: Long,
        retryOn: (Throwable) -> Boolean,
        id: Any
    ): RetryBackoffSpec {
        val total = AtomicLong(0)
        val recovering = AtomicBoolean(false)
        return when (minBackoff) {
            null -> RetryBackoffSpec.fixedDelay(maxAttempts, fixedDelay.toJavaDuration())
            else -> RetryBackoffSpec.backoff(maxAttempts, minBackoff.toJavaDuration())
        }
            .filter { retryOn.invoke(it) }
            .doBeforeRetry { signal ->
                total.incrementAndFetch()
                recovering.compareAndSet(expectedValue = false, newValue = true)
                val attempt = when {
                    maxAttempts == Long.MAX_VALUE -> "indefinitely"
                    else -> "attempt ${signal.totalRetriesInARow() + 1} of $maxAttempts"
                }
                log.retry.retrying(id, attempt, signal.totalRetries(), signal.failure())
            }
            .onRetryExhaustedThrow { _, signal ->
                val cause = signal.failure()
                log.retry.exhausted(id, signal.totalRetries(), cause)
                ProzessException.classify("$id retries exhausted after ${signal.totalRetries()} attempts", cause)
            }
    }
}
