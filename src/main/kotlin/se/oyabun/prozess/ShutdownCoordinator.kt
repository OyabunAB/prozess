package se.oyabun.prozess

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import se.oyabun.aelv.None
import se.oyabun.aelv.Sink
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

interface ShutdownableClient {
    fun wakeup()
    fun close(timeout: Duration = 3.seconds): None<Unit>
}

class ShutdownCoordinator(
    private val client: ShutdownableClient,
    private val closeSignal: Sink<Unit>,
    private val poller: Poller,
    private val committer: Committer,
    private val instanceId: String,
    private val log: Logger,
) {
    fun shutdown(timeout: Duration? = null) {
        val limit = timeout ?: 10.seconds
        try {
            runBlocking {
                withTimeout(limit) {
                    client.wakeup()
                    closeSignal.complete()
                    poller.stop().await()
                    committer.stop().await()
                    client.close().await()
                }
            }
        } catch (e: TimeoutCancellationException) {
            val wrapped = TimeoutExpired("$instanceId shutdown timed out", e)
            log.kafka.terminatedUnexpectedly(instanceId, wrapped)
            client.wakeup()
            runBlocking { client.close(3.seconds).await() }
        } catch (e: Exception) {
            val wrapped = TimeoutExpired("$instanceId shutdown timed out", e)
            log.kafka.terminatedUnexpectedly(instanceId, wrapped)
            client.wakeup()
            runBlocking { client.close(3.seconds).await() }
        }
    }
}
