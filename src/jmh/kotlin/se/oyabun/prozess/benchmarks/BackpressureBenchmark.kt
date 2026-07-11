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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import se.oyabun.prozess.ConsumerConfig as ProzessConsumerConfig
import se.oyabun.prozess.DefaultProcessor
import se.oyabun.prozess.Prozess.DeserializationResult.Message
import se.oyabun.prozess.Prozess.DeserializationResult.Tombstone as Skip
import se.oyabun.prozess.ReceivedMessage.Data
import se.oyabun.prozess.ReceivedMessage.Tombstone as ReceivedTombstone
import se.oyabun.prozess.StartOffset
import se.oyabun.prozess.StreamingConsumer
import java.lang.management.ManagementFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Measures heap allocation under a slow handler — the memory pressure benchmark.
 *
 * prozess automatically pauses the poller when the in-memory buffer reaches
 * its high watermark, bounding heap growth regardless of handler speed.
 *
 * Spring Kafka and Vertx Kafka have no automatic backpressure — their internal
 * queues grow unboundedly under a slow handler. The gc profiler reveals this:
 *   run with: -prof gc
 *
 * The metric to watch is gc.alloc.rate.norm (bytes allocated per operation).
 * prozess should show significantly lower allocation than Spring/Vertx.
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@State(Scope.Thread)
open class BackpressureBenchmark {

    private val infra         = KafkaInfrastructure()
    private val handlerDelayMs = 5L
    private lateinit var vertx: Vertx

    @Setup(Level.Trial)
    fun setup() {
        infra.startKafka()
        vertx = Vertx.vertx()
    }

    @TearDown(Level.Trial)
    fun teardown() {
        val latch = CountDownLatch(1)
        vertx.close { latch.countDown() }
        latch.await(30, TimeUnit.SECONDS)
        infra.stopKafka()
    }

    private fun heapUsed(): Long = ManagementFactory.getMemoryMXBean().heapMemoryUsage.used

    @Benchmark
    fun prozess_bounded(): Long {
        val latch     = CountDownLatch(RECORD_COUNT)
        val heapBefore = heapUsed()
        val config = ProzessConsumerConfig(
            bootstrapServers   = infra.bootstrapServers,
            groupId            = "prozess-bp-${System.nanoTime()}",
            topics             = setOf(BENCHMARK_TOPIC),
            startOffset        = StartOffset.Earliest,
            maxPollRecords     = 100,
            commitBatchSize    = 100,
            commitBatchTimeout = 500.milliseconds,
        )
        val processor = DefaultProcessor.each<ByteArray>(
            deserializer = { r -> when (val m = r.message) { is Data -> Message(m.bytes); ReceivedTombstone -> Skip } },
            handler      = { _ -> Thread.sleep(handlerDelayMs); latch.countDown() },
        )
        val consumer = StreamingConsumer(config, processor)
        consumer.start()
        latch.await()
        consumer.shutdown()
        return heapUsed() - heapBefore
    }

    @Benchmark
    fun spring_unbounded(): Long {
        val latch      = CountDownLatch(RECORD_COUNT)
        val heapBefore = heapUsed()
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        to infra.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG                 to "spring-bp-${System.nanoTime()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       to "false",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG         to "100",
        )
        val factory = DefaultKafkaConsumerFactory<String, String>(props)
        val cp = ContainerProperties(BENCHMARK_TOPIC).apply {
            ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
            messageListener = AcknowledgingMessageListener<String, String> { _, ack ->
                Thread.sleep(handlerDelayMs)
                ack!!.acknowledge()
                latch.countDown()
            }
        }
        val container = KafkaMessageListenerContainer(factory, cp)
        container.start()
        latch.await(120, TimeUnit.SECONDS)
        container.stop()
        return heapUsed() - heapBefore
    }

    @Benchmark
    fun vertx_unbounded(): Long {
        val latch      = CountDownLatch(RECORD_COUNT)
        val heapBefore = heapUsed()
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        to infra.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG                 to "vertx-bp-${System.nanoTime()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       to "false",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG         to "100",
        )
        val consumer = KafkaConsumer.create<String, String>(vertx, config)
        consumer.handler { _ ->
            Thread.sleep(handlerDelayMs)
            consumer.commit()
            latch.countDown()
        }
        val subscribeLatch = CountDownLatch(1)
        consumer.subscribe(BENCHMARK_TOPIC) { subscribeLatch.countDown() }
        subscribeLatch.await(10, TimeUnit.SECONDS)
        latch.await(120, TimeUnit.SECONDS)
        val closeLatch = CountDownLatch(1)
        consumer.close { closeLatch.countDown() }
        closeLatch.await(10, TimeUnit.SECONDS)
        return heapUsed() - heapBefore
    }
}
