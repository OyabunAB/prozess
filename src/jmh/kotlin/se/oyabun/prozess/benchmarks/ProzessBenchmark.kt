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
import se.oyabun.prozess.ConsumerConfig
import se.oyabun.prozess.DefaultProcessor
import se.oyabun.prozess.Prozess.DeserializationResult.Message
import se.oyabun.prozess.Prozess.DeserializationResult.Tombstone as Skip
import se.oyabun.prozess.ReceivedMessage.Data
import se.oyabun.prozess.ReceivedMessage.Tombstone as ReceivedTombstone
import se.oyabun.prozess.StartOffset
import se.oyabun.prozess.StreamingConsumer
import java.util.concurrent.CountDownLatch
import kotlin.time.Duration.Companion.milliseconds

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
open class ProzessBenchmark {

    private val infra = KafkaInfrastructure()

    @Setup(Level.Trial)
    fun setup() = infra.startKafka()

    @TearDown(Level.Trial)
    fun teardown() = infra.stopKafka()

    @Benchmark
    fun consumeAllRecords(): Int {
        val latch = CountDownLatch(RECORD_COUNT)
        val config = ConsumerConfig(
            bootstrapServers     = infra.bootstrapServers,
            groupId              = "prozess-${System.nanoTime()}",
            topics               = setOf(BENCHMARK_TOPIC),
            startOffset          = StartOffset.Earliest,
            pollInterval         = 1.milliseconds,
            maxPollRecords       = 1000,
            commitBatchSize      = 1000,
            commitBatchTimeout   = 100.milliseconds,
        )
        val processor = DefaultProcessor.each<ByteArray>(
            deserializer = { received ->
                when (val msg = received.message) {
                    is Data           -> Message(msg.bytes)
                    ReceivedTombstone -> Skip
                }
            },
            handler = { _ -> latch.countDown() },
        )
        val consumer = StreamingConsumer(config, processor)
        consumer.start()
        latch.await()
        consumer.shutdown()
        return RECORD_COUNT
    }
}
