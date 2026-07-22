package se.oyabun.prozess

import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame

@Timeout(value = 5, unit = TimeUnit.SECONDS)
class ErrorsTest {

    @Test
    fun `classify maps AuthenticationException to AuthenticationFailure`() {
        val cause = AuthenticationException("broker rejected credentials")
        val result = ProzessException.classify("msg", cause)

        assertIs<AuthenticationFailure>(result)
        assertEquals("msg", result.message)
        assertSame(cause, result.cause)
    }

    @Test
    fun `classify maps AuthorizationException to AuthenticationFailure`() {
        val cause = AuthorizationException("topic access denied")
        val result = ProzessException.classify("msg", cause)

        assertIs<AuthenticationFailure>(result)
        assertSame(cause, result.cause)
    }

    @Test
    fun `classify maps SaslAuthenticationException to AuthenticationFailure`() {
        val cause = SaslAuthenticationException("SASL handshake failed")
        val result = ProzessException.classify("msg", cause)

        assertIs<AuthenticationFailure>(result)
        assertSame(cause, result.cause)
    }

    @Test
    fun `classify maps SerializationException to SerializationFailure`() {
        val cause = SerializationException("bad payload")
        val result = ProzessException.classify("msg", cause)

        assertIs<SerializationFailure>(result)
        assertSame(cause, result.cause)
    }

    @Test
    fun `classify maps KafkaTimeoutException to TimeoutExpired`() {
        val cause = KafkaTimeoutException("request timed out")
        val result = ProzessException.classify("msg", cause)

        assertIs<TimeoutExpired>(result)
        assertSame(cause, result.cause)
    }

    @Test
    fun `classify falls back to RetryExhausted for unknown exceptions`() {
        val cause = IllegalStateException("something unexpected")
        val result = ProzessException.classify("msg", cause)

        assertIs<RetryExhausted>(result)
        assertSame(cause, result.cause)
    }

    @Test
    fun `classify walks cause chain to find root kafka exception`() {
        val root = KafkaTimeoutException("broker unreachable")
        val wrapped = RuntimeException("poll failed", RuntimeException("inner", root))
        val result = ProzessException.classify("msg", wrapped)

        assertIs<TimeoutExpired>(result)
        assertSame(wrapped, result.cause)
    }

    @Test
    fun `classify walks cause chain for nested auth failure`() {
        val root = SaslAuthenticationException("bad credentials")
        val wrapped = SendFailure("send failed", root)
        val result = ProzessException.classify("msg", wrapped)

        assertIs<AuthenticationFailure>(result)
        assertSame(wrapped, result.cause)
    }

    @Test
    fun `all subtypes are instances of ProzessException`() {
        val errors: List<ProzessException> = listOf(
            ConsumerNotActive("x"),
            PollerAlreadyRunning("x"),
            PollerNotRunning("x"),
            CommitterAlreadyRunning("x"),
            CommitFailure("x", RuntimeException()),
            RetryExhausted("x", RuntimeException()),
            SendFailure("x", RuntimeException()),
            TimeoutExpired("x", RuntimeException()),
            AuthenticationFailure("x", RuntimeException()),
            SerializationFailure("x", RuntimeException()),
        )

        errors.forEach { assertIs<ProzessException>(it) }
        errors.forEach { assertIs<RuntimeException>(it) }
    }
}
