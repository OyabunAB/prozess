package se.oyabun.prozess

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.Timeout
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.MountableFile
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Integration tests verifying that each [SecurityProtocol] variant can
 * produce and consume messages against a real Kafka broker.
 *
 * Uses the JVM image (`apache/kafka`) rather than the native image because
 * `apache/kafka-native:3.9.2` lacks the security classes needed for SASL
 * authentication (java.security.AccessController and javax.security.auth.Subject.current()
 * are unavailable in the GraalVM native image).
 */
@Timeout(120)
class SecurityIntegrationTest {

    companion object {
        private const val KAFKA_IMAGE = "apache/kafka:3.9.2"

        private const val STORE_PASSWORD = "changeit"
        private const val BROKER_KEYSTORE = "/etc/kafka/secrets/broker.keystore.jks"
        private const val TRUSTSTORE = "/etc/kafka/secrets/truststore.jks"
        private const val CLIENT_KEYSTORE = "/etc/kafka/secrets/client.keystore.jks"

        private fun resourcePath(name: String): String =
            SecurityIntegrationTest::class.java.classLoader.getResource("security/$name")!!.path

        private fun sslKafka(protocolMap: String): KafkaContainer =
            KafkaContainer(KAFKA_IMAGE)
                .withCopyFileToContainer(MountableFile.forClasspathResource("security/broker.keystore.jks"), BROKER_KEYSTORE)
                .withCopyFileToContainer(MountableFile.forClasspathResource("security/truststore.jks"), TRUSTSTORE)
                .withCopyFileToContainer(MountableFile.forClasspathResource("security/client.keystore.jks"), CLIENT_KEYSTORE)
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", protocolMap)
                .withEnv("KAFKA_SSL_KEYSTORE_LOCATION", BROKER_KEYSTORE)
                .withEnv("KAFKA_SSL_KEYSTORE_PASSWORD", STORE_PASSWORD)
                .withEnv("KAFKA_SSL_KEY_PASSWORD", STORE_PASSWORD)
                .withEnv("KAFKA_SSL_TRUSTSTORE_LOCATION", TRUSTSTORE)
                .withEnv("KAFKA_SSL_TRUSTSTORE_PASSWORD", STORE_PASSWORD)
                .withEnv("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "")

        private fun saslKafka(protocolMap: String, jaasConfig: String): KafkaContainer =
            KafkaContainer(KAFKA_IMAGE)
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", protocolMap)
                .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", jaasConfig)
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    inner class SslTest {

        private val kafka = sslKafka("PLAINTEXT:SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")

        @BeforeAll fun start() = kafka.start()
        @AfterAll fun stop() = kafka.stop()

        @Test
        fun `produce and consume over SSL`() {
            val security = SecurityProtocol.Ssl(
                truststore = TruststoreConfig(resourcePath("truststore.jks"), STORE_PASSWORD),
                endpointIdentification = "",
            )
            verifyProduceConsume(kafka.bootstrapServers, security)
        }
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    inner class MutualTlsTest {

        private val kafka = sslKafka("PLAINTEXT:SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .withEnv("KAFKA_SSL_CLIENT_AUTH", "required")

        @BeforeAll fun start() = kafka.start()
        @AfterAll fun stop() = kafka.stop()

        @Test
        fun `produce and consume with mutual TLS`() {
            val security = SecurityProtocol.Ssl(
                truststore = TruststoreConfig(resourcePath("truststore.jks"), STORE_PASSWORD),
                keystore = KeystoreConfig(resourcePath("client.keystore.jks"), STORE_PASSWORD),
                endpointIdentification = "",
            )
            verifyProduceConsume(kafka.bootstrapServers, security)
        }
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    inner class SaslPlaintextTest {

        private val kafka = saslKafka(
            protocolMap = "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
            jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" password=\"admin-secret\" " +
                "user_admin=\"admin-secret\" user_testuser=\"testpass\";",
        )

        @BeforeAll fun start() = kafka.start()
        @AfterAll fun stop() = kafka.stop()

        @Test
        fun `produce and consume with SASL PLAIN over plaintext`() {
            val security = SecurityProtocol.SaslPlaintext(
                mechanism = SaslMechanism.Plain("testuser", "testpass"),
            )
            verifyProduceConsume(kafka.bootstrapServers, security)
        }
    }

    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    inner class SaslSslPlainTest {

        private val kafka = sslKafka("PLAINTEXT:SASL_SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" password=\"admin-secret\" " +
                "user_admin=\"admin-secret\" user_testuser=\"testpass\";")

        @BeforeAll fun start() = kafka.start()
        @AfterAll fun stop() = kafka.stop()

        @Test
        fun `produce and consume with SASL PLAIN over SSL`() {
            val security = SecurityProtocol.SaslSsl(
                mechanism = SaslMechanism.Plain("testuser", "testpass"),
                ssl = SecurityProtocol.Ssl(
                    truststore = TruststoreConfig(resourcePath("truststore.jks"), STORE_PASSWORD),
                    endpointIdentification = "",
                ),
            )
            verifyProduceConsume(kafka.bootstrapServers, security)
        }
    }

    private fun verifyProduceConsume(bootstrapServers: String, security: SecurityProtocol) {
        val topicName = UUID.randomUUID().toString()
        val groupId = UUID.randomUUID().toString()
        val message = "hello-${UUID.randomUUID()}"

        // Create topic using the same security config
        AdminClient.create(
            mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers) + security.toProperties(),
        ).use { admin ->
            admin.createTopics(listOf(NewTopic(topicName, 1, 1))).all().get(10, TimeUnit.SECONDS)
        }

        // Produce
        val producerConfig = ProducerConfig(
            bootstrapServers = bootstrapServers,
            topic = Topic(topicName),
            security = security,
        )
        val producer = StreamingProducer<String>(producerConfig) { it.toByteArray() }
        producer.send("key", message).block()
        producer.close().block()

        // Consume
        val received = mutableListOf<String>()
        val latch = CountDownLatch(1)
        val consumerConfig = ConsumerConfig(
            bootstrapServers = bootstrapServers,
            groupId = groupId,
            topics = setOf(topicName),
            security = security,
        )
        val consumer = Prozess.consumer(
            config = consumerConfig,
            deserializeBytes = { String(it) },
            process = { _, msg ->
                received.add(msg)
                latch.countDown()
            },
        )
        consumer.start(from = StartOffset.Earliest)
        assertTrue(latch.await(15, TimeUnit.SECONDS), "Timed out waiting for message")
        consumer.shutdown()

        assertEquals(listOf(message), received)
    }
}
