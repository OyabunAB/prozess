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
package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import se.oyabun.aelv.Either
import se.oyabun.aelv.Failure
import se.oyabun.aelv.None
import se.oyabun.aelv.BroadcastSink
import se.oyabun.aelv.await
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A Kafka client that can be interrupted and closed.
 *
 * Implemented by [ThreadsafeKafkaClient]. [wakeup] interrupts a blocking [KafkaClient.poll]
 * call; [close] releases all underlying resources.
 */
interface ShutdownableClient {
    /**
     * Signals the Kafka client to interrupt any in-progress [KafkaClient.poll].
     *
     * Thread-safe. May be called from any thread.
     */
    fun wakeup()

    /**
     * Closes the Kafka client and releases all resources.
     *
     * Waits up to [timeout] for the client to finish in-flight operations.
     */
    fun close(timeout: Duration = 3.seconds): None<Unit>
}

/**
 * Orchestrates graceful consumer shutdown.
 *
 * Stops the poller and committer in order, waits for each to drain, then closes the
 * Kafka client. If [timeout] is exceeded, wakes up the client and force-closes it.
 */
internal class ShutdownCoordinator(
    private val client: ShutdownableClient,
    private val closeSignal: BroadcastSink<Unit>,
    private val poller: Poller,
    private val committer: Committer,
    private val instanceId: String,
    private val log: Logger,
) {
    /**
     * Executes the full shutdown sequence within the given [timeout].
     *
     * Sequence:
     * 1. Wakes up any in-progress poll.
     * 2. Signals the close channel to terminate the consumer pipeline.
     * 3. Stops the poller.
     * 4. Stops the committer (drains and commits remaining positions).
     * 5. Closes the Kafka client.
     *
     * If the sequence does not complete within [timeout] (default 10 seconds),
     * the client is force-woken and closed with a 3-second hard timeout.
     */
    fun shutdown(timeout: Duration? = null) {
        runBlocking {
            Either.catching(timeout ?: 10.seconds) {
                client.wakeup()
                closeSignal.complete()
                poller.stop().await()
                committer.stop().await()
                client.close().await()
            }
        }.onLeft { issue ->
            log.kafka.terminatedUnexpectedly(instanceId, issue)
            client.wakeup()
            runBlocking { client.close(3.seconds).await() }
        }
    }
}
