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
import se.oyabun.prozess.ConsumerConfig as ProzessConsumerConfig
import se.oyabun.prozess.DefaultProcessor
import se.oyabun.prozess.Prozess.DeserializationResult.Message
import se.oyabun.prozess.Prozess.DeserializationResult.Tombstone as Skip
import se.oyabun.prozess.ReceivedMessage.Data
import se.oyabun.prozess.ReceivedMessage.Tombstone as ReceivedTombstone
import se.oyabun.prozess.StartOffset
import se.oyabun.prozess.StreamingConsumer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds

/**
 * Demonstrates the value of prozess concurrent handler execution vs raw kafka.
 *
 * Raw kafka blocks the poll thread during handler execution. With a slow handler
 * this risks session timeout and rebalance. The workaround is a manual thread pool
 * but then you lose offset ordering guarantees.
 *
 * prozess runs handlers concurrently via flatMapSequential(maxConcurrency) while
 * continuing to poll, maintaining ordering within partitions.
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@State(Scope.Thread)
open class ConcurrentHandlerBenchmark {

    private val infra = KafkaInfrastructure()

    @Setup(Level.Trial)
    fun setup() = infra.startKafka()

    @TearDown(Level.Trial)
    fun teardown() = infra.stopKafka()

    private val handlerDelayMs = 1L
    private val concurrency    = 4

    @Benchmark
    fun prozess_concurrent(): Int {
        val latch = CountDownLatch(RECORD_COUNT)
        val config = ProzessConsumerConfig(
            bootstrapServers = infra.bootstrapServers,
            groupId          = "prozess-concurrent-${System.nanoTime()}",
            topics           = setOf(BENCHMARK_TOPIC),
            startOffset      = StartOffset.Earliest,
            commitBatchSize  = 500,
            commitBatchTimeout = 100.milliseconds,
        )
        val processor = DefaultProcessor.each<ByteArray>(
            deserializer   = { r -> when (val m = r.message) { is Data -> Message(m.bytes); ReceivedTombstone -> Skip } },
            handler        = { _ -> Thread.sleep(handlerDelayMs); latch.countDown() },
            maxConcurrency = concurrency,
        )
        val consumer = StreamingConsumer(config, processor)
        consumer.start()
        latch.await()
        consumer.shutdown()
        return RECORD_COUNT
    }

    /**
     * Raw kafka naive: block the poll thread during handler.
     * Risks session timeout when handler is slow.
     */
    @Benchmark
    fun raw_blocking_poll_thread(): Int {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,           infra.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG,                    "raw-blocking-${System.nanoTime()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,           "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,          "false")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,      StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,    StringDeserializer::class.java.name)
            put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,        "300000")
        }
        var consumed = 0
        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(BENCHMARK_TOPIC))
            while (consumed < RECORD_COUNT) {
                val records = consumer.poll(Duration.ofMillis(500))
                for (record in records) {
                    Thread.sleep(handlerDelayMs)
                    consumed++
                }
                consumer.commitSync()
            }
        }
        return consumed
    }

    /**
     * Raw kafka with manual thread pool: unblock poll thread but lose ordering guarantees
     * and require manual offset management.
     */
    @Benchmark
    fun raw_manual_thread_pool(): Int {
        val latch    = CountDownLatch(RECORD_COUNT)
        val executor = Executors.newFixedThreadPool(concurrency)
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,           infra.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG,                    "raw-pool-${System.nanoTime()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,           "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,          "true")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,      StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,    StringDeserializer::class.java.name)
        }
        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(BENCHMARK_TOPIC))
            var consumed = 0
            while (consumed < RECORD_COUNT) {
                val records = consumer.poll(Duration.ofMillis(500))
                for (record in records) {
                    consumed++
                    executor.submit {
                        Thread.sleep(handlerDelayMs)
                        latch.countDown()
                    }
                }
            }
        }
        latch.await(60, TimeUnit.SECONDS)
        executor.shutdown()
        return RECORD_COUNT
    }
}
