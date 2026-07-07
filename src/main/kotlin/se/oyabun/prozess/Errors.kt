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

class ConsumerNotActive(message: String) : ProzessException(message)

class PollerAlreadyRunning(message: String) : ProzessException(message)

class PollerNotRunning(message: String) : ProzessException(message)

class CommitterAlreadyRunning(message: String) : ProzessException(message)

class CommitFailure(message: String, cause: Throwable? = null) : ProzessException(message, cause)

class RetryExhausted(message: String, cause: Throwable) : ProzessException(message, cause)

class SendFailure(message: String, cause: Throwable) : ProzessException(message, cause)

class TimeoutExpired(message: String, cause: Throwable? = null) : ProzessException(message, cause)

class AuthenticationFailure(message: String, cause: Throwable) : ProzessException(message, cause)

class SerializationFailure(message: String, cause: Throwable) : ProzessException(message, cause)
