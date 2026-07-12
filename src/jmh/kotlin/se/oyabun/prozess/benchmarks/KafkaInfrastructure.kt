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
package se.oyabun.prozess.benchmarks

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.Properties
import java.util.concurrent.TimeUnit

const val BENCHMARK_TOPIC   = "benchmark"
const val RECORD_COUNT       = 10_000
private const val PARTITIONS = 4

@State(Scope.Benchmark)
open class KafkaInfrastructure {

    lateinit var bootstrapServers: String
        private set

    private lateinit var kafka: KafkaContainer

    @Setup
    fun startKafka() {
        kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
        kafka.start()
        bootstrapServers = kafka.bootstrapServers
        createTopic()
        produceRecords()
    }

    @TearDown
    fun stopKafka() {
        kafka.stop()
    }

    private fun createTopic() {
        val props = Properties().apply {
            put("bootstrap.servers", bootstrapServers)
        }
        AdminClient.create(props).use { admin ->
            admin.createTopics(listOf(NewTopic(BENCHMARK_TOPIC, PARTITIONS, 1))).all()
                .get(30, TimeUnit.SECONDS)
        }
    }

    private fun produceRecords() {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
        KafkaProducer<String, String>(props).use { producer ->
            repeat(RECORD_COUNT) { i ->
                producer.send(ProducerRecord(BENCHMARK_TOPIC, "key-$i", "value-$i")).get()
            }
        }
    }
}
