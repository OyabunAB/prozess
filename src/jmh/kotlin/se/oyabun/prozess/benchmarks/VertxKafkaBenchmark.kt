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

import io.vertx.core.Vertx
import io.vertx.kafka.client.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@State(Scope.Thread)
open class VertxKafkaBenchmark {

    private val infra = KafkaInfrastructure()
    private var vertx: Vertx = Vertx.vertx()

    @Setup(Level.Trial)
    fun setup() {
        infra.startKafka()
        vertx = Vertx.vertx()
    }

    @TearDown(Level.Trial)
    fun tearDown() {
        val latch = CountDownLatch(1)
        vertx.close { latch.countDown() }
        latch.await(30, TimeUnit.SECONDS)
        infra.stopKafka()
    }

    @Benchmark
    fun consumeAllRecords(): Int {
        val latch   = CountDownLatch(RECORD_COUNT)
        val groupId = "vertx-${System.nanoTime()}"

        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        to infra.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG                 to groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       to "false",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        )

        val consumer = KafkaConsumer.create<String, String>(vertx, config)
        consumer.handler { _ ->
            consumer.commit()
            latch.countDown()
        }

        val subscribeLatch = CountDownLatch(1)
        consumer.subscribe(BENCHMARK_TOPIC) { subscribeLatch.countDown() }
        subscribeLatch.await(10, TimeUnit.SECONDS)

        latch.await(60, TimeUnit.SECONDS)

        val closeLatch = CountDownLatch(1)
        consumer.close { closeLatch.countDown() }
        closeLatch.await(10, TimeUnit.SECONDS)

        return RECORD_COUNT - latch.count.toInt()
    }
}
