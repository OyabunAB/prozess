# prozess

Reactive Kafka consumer/producer built on Reactor and kotlinx-coroutines.

## Consumer Architecture

The consumer is split into three independent pipelines that share an in-memory buffer:

```
  Poller (polling pipeline)     Emitter (emitting pipeline)     Committer (commit pipeline)
  ┌─────────────────────┐       ┌──────────────────────┐        ┌──────────────────────────┐
  │ Flux.interval       │       │ Flux.interval        │        │ Sinks.Many<Position>     │
  │  → client.poll()    │       │  → buffer.poll()     │        │  → bufferTimeout()       │
  │  → buffer.add()     │──buf→│  → emitter.next()     │──pos──→│  → client.commit()       │
  │  → pause/resume     │       │                      │        │  → flushForPartitions()  │
  └─────────────────────┘       └──────────────────────┘        └──────────────────────────┘
```

Each pipeline runs on its own single-thread scheduler, pinned to respect KafkaConsumer thread confinement.

### Poller

The Poller (`BufferedPoller`) owns the poll loop and buffer back-pressure. It runs on a single scheduler thread, calling `client.poll()` at a fixed interval and pushing received records into a shared `Queue<Received>`.

#### Lifecycle

```
  start()
    │
    ▼
  ┌─────────────┐
  │  RUNNING    │◄────────────────────┐
  │             │                     │
  │  interval   │  retry on error     │
  │  → poll()   │─────────────────────┘
  │  → buffer   │  (retryWhen)
  │  → pause/   │
  │    resume   │
  └──────┬──────┘
         │
    shutdown signal  or  stop()
         │
         ▼
  ┌─────────────┐
  │ COMPLETED   │  → done signal emitted
  │             │  → running = false
  └─────────────┘
```

#### Use Cases

| # | Use Case | Trigger | Behaviour |
|---|----------|---------|-----------|
| 1 | Normal polling | `Flux.interval` tick | Calls `poll()`, pushes records into buffer |
| 2 | Buffer saturated | `buffer.size >= highWaterMark` | Calls `pause(assignedPartitions)` — Kafka stops returning data for these partitions on subsequent polls |
| 3 | Buffer drained | `buffer.size < lowWaterMark` | Calls `resume(assignedPartitions)` — Kafka resumes returning data |
| 4 | External pause | `Poller.pause()` | Calls `pause(assignedPartitions)` on all currently assigned partitions. Throws `PollerNotRunning` if not started |
| 5 | External resume | `Poller.resume()` | Calls `resume(assignedPartitions)` on all currently assigned partitions. Throws `PollerNotRunning` if not started |
| 6 | Graceful shutdown | `shutdown` Mono emits | `takeUntilOther` completes the Flux, subscriber completion handler fires `done` signal |
| 7 | Poll error | `poll.poll()` throws | `withRetries` (infinite, fixed 3s delay) retries the Mono before it reaches the pipeline |
| 8 | Pipeline error | Downstream operator throws (e.g. `assignments()` lambda) | `retryWhen` with exponential backoff (500ms initial, 30s max, infinite attempts) re-subscribes the chain |
| 9 | stop() after completion | Consumer calls `stop()` | Fires internal `shutdownSink` (triggers `takeUntilOther`), awaits `done` signal, then disposes subscription and scheduler. Idempotent — if already stopped, returns `Mono.empty()` immediately |
| 10 | Double start | `start()` while running | Throws `PollerAlreadyRunning` |

#### Back-pressure

The `highWaterMark` equals `config.maxPollRecords` (default 500). The `lowWaterMark` is `maxPollRecords / 4` (default 125).

When the buffer reaches `highWaterMark`, the poller pauses all assigned partitions. Kafka buffers data server-side but stops returning it on `poll()` calls. The poller continues ticking — each tick calls `poll()` (returns empty), checks buffer size, and resumes partitions once the emitter has drained the buffer below `lowWaterMark`.

This prevents the buffer from growing unbounded while downstream processing catches up, without blocking the poller thread.

#### Thread Safety

- `running` flag uses `AtomicBoolean` — `start()`/`stop()` are safe to call from different threads
- `stop()` fires `shutdownSink`, awaits `done` via `done.asMono()`, then disposes subscription and scheduler in `doFinally` — safe to call after pipeline completion (returns `Mono.empty()` if already stopped)
- `pause()`/`resume()` check `running` before calling the SAM operation — the SAM operation itself is not thread-safe, but it runs on the caller's thread (typically the consumer thread), and the underlying Kafka client serialises access via its own single-thread scheduler
- The `disposable` field is null-safe: `?.dispose()` is idempotent

### Emitter

The emitter drains the shared buffer and pushes records into the reactive `FluxSink<Received>` that feeds into the processing chain.

- Runs on its own single-thread scheduler
- Respects downstream demand via `emitter.requestedFromDownstream() > 0`
- Removed partitions are silently skipped (not emitted)
- Does not interact with Kafka client pause/resume (these moved to the Poller)

### Committer

The Committer (`BufferedCommitter`) owns the committed offsets state and runs a buffered commit pipeline on its own scheduler thread.

- Accepts positions via `markProcessed(position)` — updates `processedOffsets` atomically and feeds an internal `Sinks.Many<Position>`
- Batches positions with `bufferTimeout(25, 1s)` and commits the highest offset per partition
- Filters out unassigned partitions before committing (avoids committing offsets for partitions the consumer no longer owns)
- `flushForPartitions(partitions)` performs an immediate synchronous commit for revoked partitions — used in the rebalance callback to ensure offsets are saved before another consumer takes over
- `seedOffsets(offsets)` pre-populates offsets without going through the position pipeline (catch-up completion)
- Exposes `positions: Flux<Position>` for external subscribers (completion detection)
- `stop(): Mono<Void>` fires `tryEmitComplete()` on the internal sink, awaits pipeline drain, then disposes the scheduler — matches the Poller lifecycle pattern
- Retries commits indefinitely on transient failures; the outer `retryWhen` restarts the pipeline on unexpected errors
- Owns `processedOffsets` state — no longer managed by ReactorKafkaConsumer

## Producer Architecture

(Single-path — serialises to Kafka via `KafkaProducer.send()` on bounded-elastic scheduler.)
