package se.oyabun.prozess

import se.oyabun.aelv.Many
import se.oyabun.aelv.One
import se.oyabun.aelv.Policy
import se.oyabun.aelv.retry
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

internal val infiniteRetries: Long = Long.MAX_VALUE

internal fun retryPolicy(
    id: Any,
    log: Logger,
    minBackoff: Duration = 500.milliseconds,
    maxBackoff: Duration = 30.seconds,
    maxAttempts: Long = infiniteRetries,
    retryOn: (Throwable) -> Boolean = { true },
): Policy.Retry = Policy.retry()
    .on(retryOn)
    .withBackoff(minBackoff, maxBackoff)
    .maxAttempts(maxAttempts)

internal fun <T : Any> One<T>.withRetries(
    id: Any,
    log: Logger,
    minBackoff: Duration = 500.milliseconds,
    maxBackoff: Duration = 30.seconds,
    maxAttempts: Long = infiniteRetries,
    retryOn: (Throwable) -> Boolean = { true },
): One<T> = retry(retryPolicy(id, log, minBackoff, maxBackoff, maxAttempts, retryOn))

internal fun <T : Any> Many<T>.withRetries(
    id: Any,
    log: Logger,
    minBackoff: Duration = 500.milliseconds,
    maxBackoff: Duration = 30.seconds,
    maxAttempts: Long = infiniteRetries,
    retryOn: (Throwable) -> Boolean = { true },
): Many<T> = retry(retryPolicy(id, log, minBackoff, maxBackoff, maxAttempts, retryOn))
