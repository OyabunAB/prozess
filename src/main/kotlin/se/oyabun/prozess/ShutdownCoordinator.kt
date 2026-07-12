package se.oyabun.prozess

import kotlinx.coroutines.runBlocking
import se.oyabun.aelv.Either
import se.oyabun.aelv.Failure
import se.oyabun.aelv.None
import se.oyabun.aelv.Sink
import se.oyabun.aelv.await
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
