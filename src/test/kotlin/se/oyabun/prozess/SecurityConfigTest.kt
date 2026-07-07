package se.oyabun.prozess

import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class SecurityConfigTest {

    @Test
    fun `plaintext produces only security protocol`() {
        val props = SecurityProtocol.Plaintext.toProperties()
        assertEquals(mapOf("security.protocol" to "PLAINTEXT"), props)
    }

    @Test
    fun `ssl with truststore only`() {
        val props = SecurityProtocol.Ssl(
            truststore = TruststoreConfig("/path/to/truststore.jks", "trustpass"),
        ).toProperties()

        assertEquals("SSL", props["security.protocol"])
        assertEquals("https", props[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG])
        assertEquals("/path/to/truststore.jks", props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG])
        assertEquals("trustpass", props[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG])
        assertEquals("JKS", props[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG])
        assertFalse(props.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    }

    @Test
    fun `ssl with mutual tls`() {
        val props = SecurityProtocol.Ssl(
            truststore = TruststoreConfig("/trust.jks", "tp"),
            keystore = KeystoreConfig("/key.jks", "kp", keyPassword = "kpk", type = "PKCS12"),
        ).toProperties()

        assertEquals("SSL", props["security.protocol"])
        assertEquals("/trust.jks", props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG])
        assertEquals("/key.jks", props[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG])
        assertEquals("kp", props[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG])
        assertEquals("kpk", props[SslConfigs.SSL_KEY_PASSWORD_CONFIG])
        assertEquals("PKCS12", props[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG])
    }

    @Test
    fun `ssl endpoint identification can be disabled`() {
        val props = SecurityProtocol.Ssl(
            endpointIdentification = "",
        ).toProperties()

        assertEquals("", props[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG])
    }

    @Test
    fun `sasl plaintext with plain mechanism`() {
        val props = SecurityProtocol.SaslPlaintext(
            mechanism = SaslMechanism.Plain("user", "pass"),
        ).toProperties()

        assertEquals("SASL_PLAINTEXT", props["security.protocol"])
        assertEquals("PLAIN", props[SaslConfigs.SASL_MECHANISM])
        assertEquals(
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"user\" password=\"pass\";",
            props[SaslConfigs.SASL_JAAS_CONFIG],
        )
    }

    @Test
    fun `sasl ssl with scram-sha-256`() {
        val props = SecurityProtocol.SaslSsl(
            mechanism = SaslMechanism.Scram(SaslMechanism.Scram.Algorithm.Sha256, "admin", "secret"),
            ssl = SecurityProtocol.Ssl(truststore = TruststoreConfig("/ca.jks", "capass")),
        ).toProperties()

        assertEquals("SASL_SSL", props["security.protocol"])
        assertEquals("SCRAM-SHA-256", props[SaslConfigs.SASL_MECHANISM])
        assertEquals(
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"admin\" password=\"secret\";",
            props[SaslConfigs.SASL_JAAS_CONFIG],
        )
        assertEquals("/ca.jks", props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG])
    }

    @Test
    fun `sasl ssl with scram-sha-512`() {
        val props = SecurityProtocol.SaslSsl(
            mechanism = SaslMechanism.Scram(SaslMechanism.Scram.Algorithm.Sha512, "user512", "pw512"),
        ).toProperties()

        assertEquals("SASL_SSL", props["security.protocol"])
        assertEquals("SCRAM-SHA-512", props[SaslConfigs.SASL_MECHANISM])
        assertEquals(
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"user512\" password=\"pw512\";",
            props[SaslConfigs.SASL_JAAS_CONFIG],
        )
    }

    @Test
    fun `sasl ssl with oauthbearer`() {
        val props = SecurityProtocol.SaslSsl(
            mechanism = SaslMechanism.OAuthBearer(
                tokenEndpointUrl = "https://auth.example.com/token",
                clientId = "my-client",
                clientSecret = "my-secret",
                scope = "kafka",
            ),
        ).toProperties()

        assertEquals("SASL_SSL", props["security.protocol"])
        assertEquals("OAUTHBEARER", props[SaslConfigs.SASL_MECHANISM])
        assertEquals("https://auth.example.com/token", props[SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL])
        assertEquals(
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
            props[SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS],
        )
        val jaas = props[SaslConfigs.SASL_JAAS_CONFIG] as String
        assert(jaas.contains("clientId=\"my-client\""))
        assert(jaas.contains("clientSecret=\"my-secret\""))
        assert(jaas.contains("scope=\"kafka\""))
    }

    @Test
    fun `sasl ssl with oauthbearer without scope`() {
        val props = SecurityProtocol.SaslSsl(
            mechanism = SaslMechanism.OAuthBearer(
                tokenEndpointUrl = "https://auth.example.com/token",
                clientId = "c",
                clientSecret = "s",
            ),
        ).toProperties()

        val jaas = props[SaslConfigs.SASL_JAAS_CONFIG] as String
        assertFalse(jaas.contains("scope"))
    }

    @Test
    fun `sasl ssl with kerberos`() {
        val jaasConfig = "com.sun.security.auth.module.Krb5LoginModule required " +
            "useKeyTab=true storeKey=true keyTab=\"/etc/kafka.keytab\" " +
            "principal=\"kafka/host@REALM\";"

        val props = SecurityProtocol.SaslSsl(
            mechanism = SaslMechanism.Kerberos(
                serviceName = "kafka",
                jaasConfig = jaasConfig,
            ),
        ).toProperties()

        assertEquals("SASL_SSL", props["security.protocol"])
        assertEquals("GSSAPI", props[SaslConfigs.SASL_MECHANISM])
        assertEquals("kafka", props[SaslConfigs.SASL_KERBEROS_SERVICE_NAME])
        assertEquals(jaasConfig, props[SaslConfigs.SASL_JAAS_CONFIG])
    }

    @Test
    fun `sasl ssl reuses ssl properties from composed Ssl config`() {
        val props = SecurityProtocol.SaslSsl(
            mechanism = SaslMechanism.Plain("u", "p"),
            ssl = SecurityProtocol.Ssl(
                truststore = TruststoreConfig("/trust.jks", "tp"),
                keystore = KeystoreConfig("/key.jks", "kp"),
                endpointIdentification = "",
            ),
        ).toProperties()

        assertEquals("SASL_SSL", props["security.protocol"])
        assertEquals("/trust.jks", props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG])
        assertEquals("/key.jks", props[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG])
        assertEquals("", props[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG])
    }

    @Test
    fun `producer config includes security properties`() {
        val config = ProducerConfig(
            bootstrapServers = "localhost:9092",
            topic = Topic("test"),
            security = SecurityProtocol.SaslSsl(
                mechanism = SaslMechanism.Plain("u", "p"),
            ),
        )
        val props = config.toKafkaProperties()

        assertEquals("SASL_SSL", props["security.protocol"])
        assertEquals("PLAIN", props[SaslConfigs.SASL_MECHANISM])
    }

    @Test
    fun `consumer config includes security properties`() {
        val config = ConsumerConfig(
            bootstrapServers = "localhost:9092",
            groupId = "test-group",
            topics = setOf("topic"),
            security = SecurityProtocol.Ssl(
                truststore = TruststoreConfig("/trust.jks", "pass"),
            ),
        )
        val props = config.toKafkaProperties()

        assertEquals("SSL", props["security.protocol"])
        assertEquals("/trust.jks", props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG])
    }

    @Test
    fun `default security is plaintext for producer`() {
        val config = ProducerConfig(
            bootstrapServers = "localhost:9092",
            topic = Topic("test"),
        )
        val props = config.toKafkaProperties()

        assertEquals("PLAINTEXT", props["security.protocol"])
    }

    @Test
    fun `default security is plaintext for consumer`() {
        val config = ConsumerConfig(
            bootstrapServers = "localhost:9092",
            groupId = "test-group",
            topics = setOf("topic"),
        )
        val props = config.toKafkaProperties()

        assertEquals("PLAINTEXT", props["security.protocol"])
    }

    @Test
    fun `keystore password defaults to key password`() {
        val ks = KeystoreConfig("/key.jks", "storepass")
        val props = ks.toProperties()

        assertEquals("storepass", props[SslConfigs.SSL_KEY_PASSWORD_CONFIG])
    }

    @Test
    fun `rejects credentials containing quote characters`() {
        assertThrows<IllegalArgumentException> {
            SaslMechanism.Plain("user", "pass\"word")
        }
    }

    @Test
    fun `rejects credentials containing backslash`() {
        assertThrows<IllegalArgumentException> {
            SaslMechanism.Scram(SaslMechanism.Scram.Algorithm.Sha256, "user\\name", "pass")
        }
    }

    @Test
    fun `rejects credentials containing semicolon`() {
        assertThrows<IllegalArgumentException> {
            SaslMechanism.OAuthBearer(
                tokenEndpointUrl = "https://auth.example.com/token",
                clientId = "id;bad",
                clientSecret = "secret",
            )
        }
    }
}
