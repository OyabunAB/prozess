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

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import se.oyabun.aelv.Failure
import se.oyabun.aelv.None
import se.oyabun.aelv.Policy
import se.oyabun.aelv.BroadcastSink
import se.oyabun.aelv.Success
import se.oyabun.aelv.await
import se.oyabun.aelv.first
import se.oyabun.aelv.takeUntilOther
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Drives the Kafka poll loop and feeds records into the [ReceivedBuffer].
 *
 * The poller runs independently of the processing pipeline, pushing records
 * into the buffer as fast as the buffer accepts them. Backpressure is applied
 * externally via [pause] and [resume], which delegate to [KafkaClient.pause]
 * and [KafkaClient.resume] on the currently assigned partitions.
 */
interface Poller {
    /**
     * Starts the poll loop on a background coroutine.
     *
     * @throws PollerAlreadyRunning if called on an already-running poller.
     */
    fun start()

    /**
     * Stops the poll loop and waits for the background coroutine to exit.
     *
     * Returns a [None] that completes once the poller has fully stopped.
     */
    fun stop(): None<Unit>

    /**
     * Pauses fetching on all currently assigned partitions.
     *
     * @throws PollerNotRunning if the poller is not running.
     */
    fun pause()

    /**
     * Resumes fetching on all currently assigned partitions.
     *
     * @throws PollerNotRunning if the poller is not running.
     */
    fun resume()

    /** `true` if the poll loop is currently running. */
    val isRunning: Boolean
}

@Suppress("OPT_IN_USAGE")
internal class BufferedPoller(
    private val client: KafkaClient,
    private val buffer: ReceivedBuffer,
    private val assignments: () -> Partitions,
    private val instanceId: String,
    private val pollInterval: Duration,
    private val shutdownSink: BroadcastSink<Unit>,
    private val doneSink: BroadcastSink<Unit>,
    private val log: Logger,
) : Poller {

    private val running = AtomicBoolean(false)
    private val scope   = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val noopJob = Job().also { it.cancel() }
    private val job     = AtomicReference<Job>(noopJob)

    private val retryPolicy = Policy.retry()
        .withBackoff(500.milliseconds, 30.seconds)
        .on { true }

    override fun start() {
        if (!running.compareAndSet(false, true)) throw PollerAlreadyRunning("$instanceId already running")
        job.set(scope.launch(CoroutineExceptionHandler { _, cause -> signalCompletion(cause) }) {
            while (running.get()) {
                val result = client.poll(pollInterval).retry(retryPolicy).await()
                if (result is Success) {
                    val records = result.value
                    if (records.isNotEmpty()) {
                        log.kafka.polled(instanceId, records.size)
                        records.forEach { buffer.offer(it) }
                    }
                }
            }
            signalCompletion()
        })
        scope.launch {
            shutdownSink.asMany().first().await()
            running.set(false)
            job.get().cancelAndJoin()
            signalCompletion()
        }
    }

    override fun stop(): None<Unit> = None.defer {
        if (!running.get()) return@defer
        shutdownSink.complete()
        None.from(doneSink.asMany()).await()
        job.get().cancelAndJoin()
        running.set(false)
    }

    override fun pause() {
        if (!running.get()) throw PollerNotRunning("$instanceId is not running")
        val current = assignments()
        if (current.isNotEmpty()) runBlocking {
            when (val result = client.pause(current).await()) {
                is Failure -> log.kafka.terminatedUnexpectedly("$instanceId-pause", result.value)
                is Success -> Unit
            }
        }
    }

    override fun resume() {
        if (!running.get()) throw PollerNotRunning("$instanceId is not running")
        val current = assignments()
        if (current.isNotEmpty()) runBlocking {
            when (val result = client.resume(current).await()) {
                is Failure -> log.kafka.terminatedUnexpectedly("$instanceId-resume", result.value)
                is Success -> Unit
            }
        }
    }

    override val isRunning: Boolean get() = running.get()

    private fun signalCompletion(cause: Throwable? = null) {
        running.set(false)
        if (cause != null) log.kafka.terminatedUnexpectedly("$instanceId-poll", cause)
        else log.kafka.completed("$instanceId-poll")
        doneSink.complete()
    }
}
