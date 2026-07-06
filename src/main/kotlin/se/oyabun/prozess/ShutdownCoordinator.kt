package se.oyabun.prozess

import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.defer
import reactor.core.publisher.Mono.fromCallable
import reactor.core.publisher.Mono.`when`
import reactor.core.publisher.Sinks
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/** Client contract for [ShutdownCoordinator] — wakeup + close. */
interface ShutdownableClient {
    fun wakeup()
    fun close(timeout: Duration = 3.seconds): Mono<Void>
}

/**
 * Orchestrates a graceful shutdown sequence: signal close, stop poller,
 * flush committer, then close the client.
 *
 * Constructed once at shutdown time with the live component references — no
 * indirection, no state, disposable.
 *
 * @param client       the underlying Kafka client.
 * @param closeSignal  emitted to terminate the message feed flux.
 * @param poller       the poller to stop.
 * @param committer    the committer to flush.
 * @param instanceId   label for logging.
 * @param log          logger.
 */
class ShutdownCoordinator(
    private val client: ShutdownableClient,
    private val closeSignal: Sinks.One<Unit>,
    private val poller: Poller,
    private val committer: Committer,
    private val instanceId: String,
    private val log: Logger,
) {
    fun shutdown(timeout: Duration? = null) {
        val task = fromCallable { client.wakeup(); closeSignal.tryEmitEmpty() }
            .then(poller.stop().then(committer.stop()))
            .then(defer { client.close() })
        try {
            if (timeout != null) task.block(timeout.toJavaDuration())
            else task.block(10.seconds.toJavaDuration())
        } catch (e: Exception) {
            log.kafka.terminatedUnexpectedly(instanceId, e)
            client.wakeup()
            try { client.close().block(3.seconds.toJavaDuration()) } catch (_: Exception) { }
        }
    }
}
