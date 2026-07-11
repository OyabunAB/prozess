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

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Warmup
import java.time.Duration
import java.util.Properties

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@State(Scope.Thread)
open class RawKafkaBenchmark {

    private val infra = KafkaInfrastructure()

    @Setup(Level.Trial)
    fun setup() = infra.startKafka()

    @TearDown(Level.Trial)
    fun tearDown() = infra.stopKafka()

    @Benchmark
    fun consumeAllRecords(): Int {
        val groupId = "raw-${System.nanoTime()}"
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  infra.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG,           groupId)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        }
        var consumed = 0
        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(BENCHMARK_TOPIC))
            while (consumed < RECORD_COUNT) {
                val records = consumer.poll(Duration.ofMillis(500))
                consumed += records.count()
            }
        }
        return consumed
    }
}
