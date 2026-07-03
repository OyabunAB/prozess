package se.oyabun.prozess

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.retry.RetryBackoffSpec
import se.oyabun.prozess.Logging
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
        val total = AtomicLong(0)
        val recovering = AtomicBoolean(false)
        val spec = when (minBackoff) {
            null -> RetryBackoffSpec.fixedDelay(maxAttempts, fixedDelay.toJavaDuration())
            else -> RetryBackoffSpec.backoff(maxAttempts, minBackoff.toJavaDuration())
        }
        val retried = retryWhen(
            spec.filter { retryOn.invoke(it) }
                .doBeforeRetry { signal ->
                    total.incrementAndFetch()
                    recovering.compareAndSet(expectedValue = false, newValue = true)
                    val attempt = if (maxAttempts == Long.MAX_VALUE) {
                        "indefinitely"
                    } else {
                        "attempt ${signal.totalRetriesInARow() + 1} of $maxAttempts"
                    }
                    log.retry.retrying(id, attempt, signal.totalRetries(), signal.failure())
                }
                .onRetryExhaustedThrow { _, signal ->
                    signal.failure().also { log.retry.exhausted(id, signal.totalRetries(), it) }
                },
        )
            .doOnSuccess { if (recovering.load()) log.retry.recovered(id, total.load()) }
            .subscribeOn(Schedulers.boundedElastic())
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
        val total = AtomicLong(0)
        val recovering = AtomicBoolean(false)
        val spec = when (minBackoff) {
            null -> RetryBackoffSpec.fixedDelay(maxAttempts, fixedDelay.toJavaDuration())
            else -> RetryBackoffSpec.backoff(maxAttempts, minBackoff.toJavaDuration())
        }
        val retried = retryWhen(
            spec.filter { retryOn.invoke(it) }
                .doBeforeRetry { signal ->
                    total.incrementAndFetch()
                    recovering.compareAndSet(expectedValue = false, newValue = true)
                    val attempt = if (maxAttempts == Long.MAX_VALUE) {
                        "indefinitely"
                    } else {
                        "attempt ${signal.totalRetriesInARow() + 1} of $maxAttempts"
                    }
                    log.retry.retrying(id, attempt, signal.totalRetries(), signal.failure())
                }
                .onRetryExhaustedThrow { _, signal ->
                    signal.failure().also { log.retry.exhausted(id, signal.totalRetries(), it) }
                },
        ).subscribeOn(Schedulers.boundedElastic())
        return fallback?.let { retried.onErrorReturn(retryOn, it) } ?: retried
    }
}
