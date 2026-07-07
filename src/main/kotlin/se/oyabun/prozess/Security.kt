package se.oyabun.prozess

import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs

/**
 * Security protocol for Kafka client connections.
 *
 * Each subtype maps to its corresponding `security.protocol` value and produces
 * the full set of Kafka client properties required for that protocol.
 */
sealed class SecurityProtocol {
    internal abstract fun toProperties(): Map<String, Any>

    /** No encryption, no authentication. */
    data object Plaintext : SecurityProtocol() {
        override fun toProperties(): Map<String, Any> = mapOf(
            SECURITY_PROTOCOL to "PLAINTEXT",
        )
    }

    /** TLS encryption with optional mutual TLS (client certificate authentication). */
    data class Ssl(
        val truststore: TruststoreConfig? = null,
        val keystore: KeystoreConfig? = null,
        val endpointIdentification: String = "https",
    ) : SecurityProtocol() {
        override fun toProperties(): Map<String, Any> = buildMap {
            put(SECURITY_PROTOCOL, "SSL")
            putAll(sslProperties())
        }

        internal fun sslProperties(): Map<String, Any> = buildMap {
            put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, endpointIdentification)
            truststore?.let { putAll(it.toProperties()) }
            keystore?.let { putAll(it.toProperties()) }
        }
    }

    /** SASL authentication without TLS encryption. Suitable for development only. */
    data class SaslPlaintext(
        val mechanism: SaslMechanism,
    ) : SecurityProtocol() {
        override fun toProperties(): Map<String, Any> = buildMap {
            put(SECURITY_PROTOCOL, "SASL_PLAINTEXT")
            putAll(mechanism.toProperties())
        }
    }

    /** SASL authentication over TLS. The standard production configuration. */
    data class SaslSsl(
        val mechanism: SaslMechanism,
        val ssl: Ssl = Ssl(),
    ) : SecurityProtocol() {
        override fun toProperties(): Map<String, Any> = buildMap {
            put(SECURITY_PROTOCOL, "SASL_SSL")
            putAll(mechanism.toProperties())
            putAll(ssl.sslProperties())
        }
    }

    internal companion object {
        const val SECURITY_PROTOCOL = "security.protocol"
    }
}

/** TLS truststore configuration (server certificate verification). */
data class TruststoreConfig(
    val location: String,
    val password: String,
    val type: String = "JKS",
) {
    internal fun toProperties(): Map<String, Any> = mapOf(
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to location,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to password,
        SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to type,
    )
}

/** TLS keystore configuration (client certificate for mutual TLS). */
data class KeystoreConfig(
    val location: String,
    val password: String,
    val keyPassword: String = password,
    val type: String = "JKS",
) {
    internal fun toProperties(): Map<String, Any> = mapOf(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to location,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to password,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG to keyPassword,
        SslConfigs.SSL_KEYSTORE_TYPE_CONFIG to type,
    )
}

/** SASL authentication mechanism. */
sealed class SaslMechanism {
    internal abstract fun toProperties(): Map<String, Any>

    /** Simple username/password authentication (SASL/PLAIN). */
    data class Plain(
        val username: String,
        val password: String,
    ) : SaslMechanism() {
        init { validateJaasCredentials(username, password) }

        override fun toProperties(): Map<String, Any> = mapOf(
            SaslConfigs.SASL_MECHANISM to "PLAIN",
            SaslConfigs.SASL_JAAS_CONFIG to
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"$username\" password=\"$password\";",
        )
    }

    /** SCRAM challenge-response authentication. */
    data class Scram(
        val algorithm: Algorithm,
        val username: String,
        val password: String,
    ) : SaslMechanism() {
        init { validateJaasCredentials(username, password) }

        enum class Algorithm(val mechanism: String) {
            Sha256("SCRAM-SHA-256"),
            Sha512("SCRAM-SHA-512"),
        }

        override fun toProperties(): Map<String, Any> = mapOf(
            SaslConfigs.SASL_MECHANISM to algorithm.mechanism,
            SaslConfigs.SASL_JAAS_CONFIG to
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"$username\" password=\"$password\";",
        )
    }

    /** OAuth 2.0 bearer token authentication. */
    data class OAuthBearer(
        val tokenEndpointUrl: String,
        val clientId: String,
        val clientSecret: String,
        val scope: String? = null,
    ) : SaslMechanism() {
        init { validateJaasCredentials(clientId, clientSecret) }

        override fun toProperties(): Map<String, Any> = buildMap {
            put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER")
            put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl)
            put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "clientId=\"$clientId\" clientSecret=\"$clientSecret\"" +
                (scope?.let { " scope=\"$it\"" } ?: "") + ";")
            put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler")
        }
    }

    /** Kerberos (GSSAPI) authentication. */
    data class Kerberos(
        val serviceName: String = "kafka",
        val jaasConfig: String,
    ) : SaslMechanism() {
        override fun toProperties(): Map<String, Any> = mapOf(
            SaslConfigs.SASL_MECHANISM to "GSSAPI",
            SaslConfigs.SASL_KERBEROS_SERVICE_NAME to serviceName,
            SaslConfigs.SASL_JAAS_CONFIG to jaasConfig,
        )
    }

    companion object {
        private val JAAS_UNSAFE = Regex("[\"\\\\;]")

        private fun validateJaasCredentials(vararg values: String) {
            values.forEach { value ->
                require(!JAAS_UNSAFE.containsMatchIn(value)) {
                    "SASL credential contains characters that would break JAAS config syntax: \" \\ ;"
                }
            }
        }
    }
}
