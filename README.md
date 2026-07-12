# prozess

Reactive Kafka consumer/producer built on [aelv](https://github.com/OyabunAB/aelv) and kotlinx-coroutines.

## Performance

See [BENCHMARKS.md](BENCHMARKS.md) for a full comparison against raw `kafka-clients`,
Spring Kafka, and Vert.x Kafka covering throughput, concurrent handler execution,
grouped/ordered processing, and memory pressure under backpressure.

## Consumer Architecture

The consumer has two independent pipelines sharing an in-memory buffer:

```
   Poller                         Committer
  ┌─────────────────────┐        ┌──────────────────────┐
  │ Flux.interval       │        │ Sinks.Many<Position> │
  │  ─► client.poll()   │──buf──►│  ─► bufferTimeout()  │
  │  ─► buffer.offer()  │ filter │  ─► client.commit()  │
  └─────────┬───────────┘   │    └──────────────────────┘
            │               │
       pause/resume         ▼
  (via buffer callbacks)  Processing chain
                          (groupBy, flatMap,
                           deserialize, process,
                           committer.markProcessed)
```

The poller runs on its own single-thread scheduler. The committer runs its commit pipeline on another. The processing chain between buffer and committer is composed inline in `StreamingConsumer.start()` — no separate scheduler, reactive back-pressure propagates from the processing chain through the buffer to the poller.

Partition assignment filtering happens inline via `.filter { it.position.partition in partitionManager.assignments() }` on `buffer.asFlux()` — unassigned records are silently dropped before reaching the processing chain.

### Processor

The `Processor` abstraction (see [Processor.kt](src/main/kotlin/se/oyabun/prozess/Processor.kt)) encapsulates deserialization, handler invocation, and retry/backoff. The consumer builder API ([Prozess.kt](src/main/kotlin/se/oyabun/prozess/Prozess.kt)) constructs the processing pipeline:

```kotlin
// Per-message processing
Prozess.consumer(config, deserializer)
    .each { p -> handleMessage(p.received, p.value) }
    .start()

// Batched processing (atomic batches)
Prozess.consumer(config, deserializer)
    .batch(size = 10, duration = 1.seconds) { batch ->
        handleBatch(batch.map { it.value })
    }
    .start()

// Key-grouped processing (per-key ordering)
Prozess.consumer(config, deserializer)
    .groupBy { p -> p.value.userId }
    .each { key, p -> handleUser(key, p.value) }
    .start()

// Key-grouped batches (per-key batching)
Prozess.consumer(config, deserializer)
    .groupBy { p -> p.value.userId }
    .batch(size = 10) { key, batch ->
        handleUserBatch(key, batch.map { it.value })
    }
    .start()
```

The retry policy is backoff-only — messages are retried indefinitely (never exhausted). Configure via `RetryConfig`:

```kotlin
Prozess.consumer(config, deserializer)
    .each(
        maxConcurrency = 4,
        retryConfig = RetryConfig(
            minBackoff = 500.milliseconds,
            maxBackoff = 30.seconds,
        ),
    ) { p -> handle(p.value) }
    .start()
```

Key-grouped processing uses a `ContiguousOffsetTracker` to prevent offset gaps when concurrent key groups complete out of order — positions are only emitted when a contiguous prefix is complete.

### ReceivedBuffer

The `ReceivedBuffer` interface (see [Buffering.kt](src/main/kotlin/se/oyabun/prozess/Buffering.kt)) connects the poller to the processing chain:

- `offer()` — Poller writes records into the buffer.
- `size` — O(1) counter used for watermark back-pressure decisions.
- `asFlux()` — reactive stream view consumed by the processing pipeline.

#### Back-pressure

The `InMemoryReceivedBuffer` implementation owns the back-pressure contract via high/low watermarks and callbacks:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `highWaterMark` | `Int.MAX_VALUE` | When `size >= highWaterMark`, `onPause()` is called |
| `lowWaterMark` | 0 | When `size < lowWaterMark` after drain, `onResume()` is called |
| `onPause` | no-op | Pauses Kafka partitions (calls `client.pause()`) |
| `onResume` | no-op | Resumes Kafka partitions (calls `client.resume()`) |

The `highWaterMark` equals `config.maxPollRecords` (default 500). The `lowWaterMark` is `maxPollRecords / 4` (default 125).

When the buffer reaches `highWaterMark`, the `onPause` callback pauses all assigned partitions. Kafka buffers data server-side but stops returning it on `poll()` calls. The poller continues ticking — each tick calls `poll()` (returns empty). The processing chain drains the buffer and the `onResume` callback fires once the buffer drops below `lowWaterMark`.

This prevents the buffer from growing unbounded while downstream processing catches up, without blocking the poller thread. The pause/resume contract lives in one place: the buffer.

### Poller

The Poller (`BufferedPoller`) runs the poll loop on a single scheduler thread, calling `client.poll()` at a fixed interval and pushing received records into the `ReceivedBuffer`.

#### Lifecycle

```
       start()
         │
         ▼
  ┌─────────────┐
  │   RUNNING   │
  │             │◄─────────────────┐
  │  interval   │  retry on error  │
  │  ─► poll()  │──────────────────┘
  │  ─► offer() │   (retryWhen)
  └──────┬──────┘
         │
    shutdown signal  or  stop()
         │
         ▼
  ┌─────────────┐
  │  COMPLETED  │  ─► done signal emitted
  │             │  ─► running = false
  └─────────────┘
```

#### Use Cases

| # | Use Case | Trigger | Behaviour |
|---|----------|---------|-----------|
| 1 | Normal polling | `Flux.interval` tick | Calls `poll()`, pushes records into buffer via `offer()` |
| 2 | External pause | `Poller.pause()` | Calls `pause(assignedPartitions)` on all currently assigned partitions. Throws `PollerNotRunning` if not started |
| 3 | External resume | `Poller.resume()` | Calls `resume(assignedPartitions)` on all currently assigned partitions. Throws `PollerNotRunning` if not started |
| 4 | Graceful shutdown | `shutdown` Mono emits | `takeUntilOther` completes the Flux, subscriber completion handler fires `done` signal |
| 5 | Poll error | `poll.poll()` throws | `withRetries` (infinite, fixed 3s delay) retries the Mono before it reaches the pipeline |
| 6 | Pipeline error | Downstream operator throws | `retryWhen` with exponential backoff (500ms initial, 30s max, infinite attempts) re-subscribes the chain |
| 7 | stop() after completion | Consumer calls `stop()` | Fires internal `shutdownSink`, awaits `done` signal, then disposes subscription and scheduler. Idempotent — if already stopped, returns `Mono.empty()` immediately |
| 8 | Double start | `start()` while running | Throws `PollerAlreadyRunning` |

#### Thread Safety

- `running` flag uses `AtomicBoolean` — `start()`/`stop()` are safe to call from different threads
- `stop()` fires `shutdownSink`, awaits `done` via `done.asMono()`, then disposes subscription and scheduler in `doFinally` — safe to call after pipeline completion (returns `Mono.empty()` if already stopped)
- `pause()`/`resume()` check `running` before calling the SAM operation — the SAM operation itself is not thread-safe, but it runs on the caller's thread, and the underlying Kafka client serialises access via its own single-thread scheduler
- `disposable` is always non-null — default `Disposable { }` before `start()` sets the real one

### Committer

The Committer (`BufferedCommitter`) owns the committed offsets state and runs a buffered commit pipeline on its own scheduler thread.

- Accepts positions via `markProcessed(position)` — updates `processedOffsets` atomically and feeds an internal `Sinks.Many<Position>`
- Batches positions with `bufferTimeout(25, 1s)` and commits the highest offset per partition
- Filters out unassigned partitions before committing (avoids committing offsets for partitions the consumer no longer owns)
- `seedOffsets(offsets)` pre-populates offsets without going through the position pipeline (catch-up completion)
- Exposes `positions: Flux<Position>` for external subscribers (completion detection)
- `stop(): Mono<Void>` fires `tryEmitComplete()` on the internal sink, awaits pipeline drain, then disposes the scheduler
- Retries commits indefinitely on transient failures; the outer `retryWhen` restarts the pipeline on unexpected errors
- Owns `processedOffsets` state — no longer managed by `StreamingConsumer`

## Producer

Create a producer with `Prozess.producer()`. Each instance is bound to a single topic via `ProducerConfig`.

```kotlin
val producer = Prozess.producer<MyEvent>(
    config = ProducerConfig(bootstrapServers = "localhost:9092", topic = Topic("events")),
    serializer = { event -> Json.encodeToByteArray(event) },
)
```

### Sending messages

**Single message** — blocks until the broker acknowledges and returns the written offset:

```kotlin
val offset: Long = producer.send(key = event.id, value = event)
```

**Batch — collection** — sends all messages and blocks until all are acknowledged. Returns the
sent messages as a list (pass-through):

```kotlin
val sent: List<MyEvent> = producer.sendAll(events, key = { it.id })
```

**Batch — vararg** — convenience overload for ad-hoc sends:

```kotlin
producer.sendAll(eventA, eventB, eventC, key = { it.id })
```

Both `sendAll` overloads accept an optional `headersProvider` to attach per-message Kafka headers:

```kotlin
producer.sendAll(events, key = { it.id }, headersProvider = { listOf(Header("trace-id", traceId.toByteArray())) })
```

Messages are grouped by extracted key and sent in order within each key group, preserving
per-key ordering. The underlying `StreamingProducer.sendAll()` is available for reactive
pipelines that already work with `Many<M>`.

### Transactions

```kotlin
producer.initTransactions()
producer.beginTransaction()
try {
    producer.send("k", event)
    producer.commitTransaction()
} catch (e: Exception) {
    producer.abortTransaction()
}
producer.close()
```

Configure with `TransactionalConfig.Enabled(id = "my-transactional-id")` in `ProducerConfig`.
Transactional producers use `acks=all` automatically.

### Multiple topics

`ProducerConfig` is a `data class` — copy it to create producers for different topics without
duplicating connection settings:

```kotlin
val base = ProducerConfig(bootstrapServers = "localhost:9092", topic = Topic("events"))
val dlqProducer = Prozess.producer<MyEvent>(base.copy(topic = Topic("events-dlq")), serializer)
```
