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

import se.oyabun.aelv.Many
import se.oyabun.aelv.One
import se.oyabun.aelv.Policy
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

internal val infiniteRetries: Long = Long.MAX_VALUE

/** Returns true for exceptions that are worth retrying (transient). Permanent failures are not retried. */
internal fun isTransient(t: Throwable): Boolean = when (t) {
    is AuthenticationFailure -> false
    is SerializationFailure  -> false
    else                     -> true
}

internal fun retryPolicy(
    minBackoff: Duration = 500.milliseconds,
    maxBackoff: Duration = 30.seconds,
    maxAttempts: Long = infiniteRetries,
    retryOn: (Throwable) -> Boolean = ::isTransient,
): Policy.Retry = Policy.retry()
    .on(retryOn)
    .withBackoff(minBackoff, maxBackoff)
    .maxAttempts(maxAttempts)

internal fun <T : Any> One<T>.withRetries(
    minBackoff: Duration = 500.milliseconds,
    maxBackoff: Duration = 30.seconds,
    maxAttempts: Long = infiniteRetries,
    retryOn: (Throwable) -> Boolean = ::isTransient,
): One<T> = retry(retryPolicy(minBackoff, maxBackoff, maxAttempts, retryOn))

internal fun <T : Any> Many<T>.withRetries(
    minBackoff: Duration = 500.milliseconds,
    maxBackoff: Duration = 30.seconds,
    maxAttempts: Long = infiniteRetries,
    retryOn: (Throwable) -> Boolean = ::isTransient,
): Many<T> = retry(retryPolicy(minBackoff, maxBackoff, maxAttempts, retryOn))
