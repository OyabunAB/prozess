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

import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException

/**
 * Sealed base for all Prozess exceptions.
 *
 * Users can catch this single type to handle every library-originated failure,
 * then narrow with `when` if specific handling is needed.
 */
sealed class ProzessException(message: String, cause: Throwable? = null) : RuntimeException(message, cause) {
    companion object {
        /**
         * Classifies a raw [Throwable] into the most specific [ProzessException] subtype
         * by walking the cause chain. Falls back to [RetryExhausted] when no specific
         * Kafka error type is recognized.
         */
        fun classify(message: String, cause: Throwable): ProzessException = when (rootCause(cause)) {
            is AuthenticationException -> AuthenticationFailure(message, cause)
            is AuthorizationException -> AuthenticationFailure(message, cause)
            is SerializationException -> SerializationFailure(message, cause)
            is KafkaTimeoutException -> TimeoutExpired(message, cause)
            else -> RetryExhausted(message, cause)
        }

        private fun rootCause(t: Throwable): Throwable {
            var current = t
            while (current.cause != null && current.cause !== current) current = current.cause!!
            return current
        }
    }
}

/** Thrown when a consumer operation is attempted after [Prozess.Consumer.shutdown]. */
class ConsumerNotActive(message: String) : ProzessException(message)

/** Thrown by [Poller.start] when the poller is already running. */
class PollerAlreadyRunning(message: String) : ProzessException(message)

/** Thrown by [Poller.pause] or [Poller.resume] when the poller is not running. */
class PollerNotRunning(message: String) : ProzessException(message)

/** Thrown by [Committer.start] when the committer is already running. */
class CommitterAlreadyRunning(message: String) : ProzessException(message)

/** Thrown when a Kafka offset commit fails and retries are exhausted. */
class CommitFailure(message: String, cause: Throwable) : ProzessException(message, cause)

/** Thrown when an operation fails and all retry attempts are exhausted. */
class RetryExhausted(message: String, cause: Throwable) : ProzessException(message, cause)

/** Thrown when [Prozess.Producer.send] or [Prozess.Producer.sendAll] fails. */
class SendFailure(message: String, cause: Throwable) : ProzessException(message, cause)

/** Thrown when a Kafka operation exceeds its configured timeout. */
class TimeoutExpired(message: String, cause: Throwable) : ProzessException(message, cause)

/** Thrown when SASL authentication or ACL authorization fails. */
class AuthenticationFailure(message: String, cause: Throwable) : ProzessException(message, cause)

/** Thrown when Kafka cannot serialize or deserialize a record. */
class SerializationFailure(message: String, cause: Throwable) : ProzessException(message, cause)
