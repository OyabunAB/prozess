package se.oyabun.prozess

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import se.oyabun.aelv.Many
import se.oyabun.aelv.None
import se.oyabun.aelv.One
import se.oyabun.aelv.Policy
import se.oyabun.aelv.Sink
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.doOnNext
import se.oyabun.aelv.drain
import se.oyabun.aelv.first
import se.oyabun.aelv.get
import se.oyabun.aelv.onBackpressureDrop
import se.oyabun.aelv.retry
import se.oyabun.aelv.takeUntilOther
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

interface Poller {
    fun start()
    fun stop(): None<Unit>
    fun pause()
    fun resume()
    val isRunning: Boolean
}

@Suppress("OPT_IN_USAGE")
internal class BufferedPoller(
    private val client: KafkaClient,
    private val buffer: ReceivedBuffer,
    private val assignments: () -> Partitions,
    private val instanceId: String,
    private val pollInterval: Duration,
    private val shutdownSink: Sink<Unit>,
    private val doneSink: Sink<Unit>,
    private val log: Logger,
) : Poller {

    private val running = AtomicBoolean(false)
    private val scope   = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private var job: Job? = null

    private val retryPolicy = Policy.retry()
        .withBackoff(500.milliseconds, 30.seconds)
        .on { true }

    override fun start() {
        if (!running.compareAndSet(false, true)) throw PollerAlreadyRunning("$instanceId already running")
        job = scope.launch {
            try {
                while (running.get()) {
                    val result = client.poll(pollInterval).retry(retryPolicy).get()
                    if (result is se.oyabun.aelv.Either.Left) {
                        val records = result.value
                        if (records.isNotEmpty()) {
                            log.kafka.polled(instanceId, records.size)
                            records.forEach { buffer.offer(it) }
                        }
                    }
                }
                signalCompletion()
            } catch (e: Exception) {
                signalCompletion(e)
            }
        }
        scope.launch {
            shutdownSink.asMany().first()
            running.set(false)
            job?.cancelAndJoin()
            signalCompletion()
        }
    }

    override fun stop(): None<Unit> = None.defer {
        if (!running.get()) return@defer
        shutdownSink.complete()
        None.from(doneSink.asMany()).await()
        job?.cancelAndJoin()
        running.set(false)
    }

    override fun pause() {
        if (!running.get()) throw PollerNotRunning("$instanceId is not running")
        val current = assignments()
        if (current.isNotEmpty()) runBlocking { client.pause(current).get() }
    }

    override fun resume() {
        if (!running.get()) throw PollerNotRunning("$instanceId is not running")
        val current = assignments()
        if (current.isNotEmpty()) runBlocking { client.resume(current).get() }
    }

    override val isRunning: Boolean get() = running.get()

    private fun signalCompletion(cause: Throwable? = null) {
        running.set(false)
        if (cause != null) log.kafka.terminatedUnexpectedly("$instanceId-poll", cause)
        else log.kafka.completed("$instanceId-poll")
        doneSink.complete()
    }
}
