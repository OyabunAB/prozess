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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds

/**
 * Demonstrates the value of prozess grouped/ordered processing.
 *
 * prozess groupedEach guarantees per-key ordering while processing different keys
 * concurrently. Implementing this correctly with raw kafka requires:
 * - Manual partition-per-key routing
 * - A concurrent map of per-key queues
 * - Careful offset management to only commit when all prior keys have completed
 *
 * The raw implementation here is deliberately simplified and does NOT provide
 * the same correctness guarantees — it shows the complexity and the gap.
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@State(Scope.Thread)
open class GroupedOrderedBenchmark {

    private val infra      = KafkaInfrastructure()
    private val keyCount   = 10
    private val handlerDelayMs = 1L

    @Setup(Level.Trial)
    fun setup() = infra.startKafka()

    @TearDown(Level.Trial)
    fun teardown() = infra.stopKafka()

    @Benchmark
    fun prozess_groupedEach(): Int {
        val latch = CountDownLatch(RECORD_COUNT)
        val config = ProzessConsumerConfig(
            bootstrapServers   = infra.bootstrapServers,
            groupId            = "prozess-grouped-${System.nanoTime()}",
            topics             = setOf(BENCHMARK_TOPIC),
            startOffset        = StartOffset.Earliest,
            commitBatchSize    = 500,
            commitBatchTimeout = 100.milliseconds,
        )
        val processor = DefaultProcessor.groupedEach<String, ByteArray>(
            deserializer = { r -> when (val m = r.message) { is Data -> Message(m.bytes); ReceivedTombstone -> Skip } },
            keyExtractor = { p -> (p.received.key as se.oyabun.prozess.ReceivedKey.Value).key.substringAfterLast('-').toInt().rem(keyCount).toString() },
            handler      = { _, _ -> Thread.sleep(handlerDelayMs); latch.countDown() },
        )
        val consumer = StreamingConsumer(config, processor)
        consumer.start()
        latch.await()
        consumer.shutdown()
        return RECORD_COUNT
    }

    /**
     * Raw kafka manual grouped processing.
     * Simplified — does NOT guarantee per-key ordering across concurrent workers.
     * A correct implementation requires significantly more code.
     */
    @Benchmark
    fun raw_manual_grouped(): Int {
        val latch      = CountDownLatch(RECORD_COUNT)
        val keyQueues  = ConcurrentHashMap<String, ArrayDeque<String>>()
        val processing = ConcurrentHashMap<String, AtomicBoolean>()
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        infra.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG,                 "raw-grouped-${System.nanoTime()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        }
        val executor = java.util.concurrent.Executors.newFixedThreadPool(keyCount)
        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(BENCHMARK_TOPIC))
            var consumed = 0
            while (consumed < RECORD_COUNT) {
                val records = consumer.poll(Duration.ofMillis(500))
                for (record in records) {
                    consumed++
                    val key = (record.key() ?: "null").substringAfterLast('-').toIntOrNull()?.rem(keyCount)?.toString() ?: "0"
                    val queue = keyQueues.getOrPut(key) { ArrayDeque() }
                    val active = processing.getOrPut(key) { AtomicBoolean(false) }
                    synchronized(queue) { queue.addLast(record.value()) }
                    if (active.compareAndSet(false, true)) {
                        executor.submit {
                            try {
                                while (true) {
                                    val item = synchronized(queue) { if (queue.isEmpty()) null else queue.removeFirst() } ?: break
                                    Thread.sleep(handlerDelayMs)
                                    latch.countDown()
                                }
                            } finally {
                                active.set(false)
                            }
                        }
                    }
                }
            }
        }
        latch.await(120, java.util.concurrent.TimeUnit.SECONDS)
        executor.shutdown()
        return RECORD_COUNT
    }
}
