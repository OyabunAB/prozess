# prozess

Reactive Kafka consumer/producer built on Reactor and kotlinx-coroutines.

## Consumer Architecture

The consumer is split into three independent pipelines that share an in-memory buffer:

```
   Poller                        Emitter                          Committer
  ┌─────────────────────┐       ┌──────────────────────┐        ┌──────────────────────────┐
  │ Flux.interval       │       │ buffer.asFlux()      │        │ Sinks.Many<Position>     │
  │  ─► client.poll()   │──buf─►│  ─► concatMap(emit)  │──pos──►│  ─► bufferTimeout()      │
  │  ─► buffer.offer()  │       │  ─► filter partition │        │  ─► client.commit()      │
  └─────────┬───────────┘       └──────────────────────┘        │  ─► flushForPartitions() │
            │                                                   └──────────────────────────┘
       pause/resume
  (via buffer callbacks)
```

Each pipeline runs on its own single-thread scheduler, pinned to respect KafkaConsumer thread confinement.

### ReceivedBuffer

The `ReceivedBuffer` interface (see [Buffering.kt](src/main/kotlin/se/oyabun/prozess/Buffering.kt)) is the shared contract between Poller and Emitter:

- `offer()` — Poller writes records into the buffer. If a subscriber has demand, records are drained immediately.
- `size` — O(1) counter used for watermark back-pressure decisions.
- `asFlux()` — reactive stream view; Emitter subscribes here instead of polling.

#### Back-pressure

The `InMemoryReceivedBuffer` implementation owns the back-pressure contract via high/low watermarks and callbacks:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `highWaterMark` | `Int.MAX_VALUE` | When `size >= highWaterMark`, `onPause()` is called |
| `lowWaterMark` | 0 | When `size < lowWaterMark` after drain, `onResume()` is called |
| `onPause` | no-op | Pauses Kafka partitions (calls `client.pause()`) |
| `onResume` | no-op | Resumes Kafka partitions (calls `client.resume()`) |

The `highWaterMark` equals `config.maxPollRecords` (default 500). The `lowWaterMark` is `maxPollRecords / 4` (default 125).

When the buffer reaches `highWaterMark`, the `onPause` callback pauses all assigned partitions. Kafka buffers data server-side but stops returning it on `poll()` calls. The poller continues ticking — each tick calls `poll()` (returns empty). The emitter drains the buffer and the `onResume` callback fires once the buffer drops below `lowWaterMark`.

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
- The `disposable` field is null-safe: `?.dispose()` is idempotent

### Emitter

The Emitter (`BufferedEmitter`) subscribes to `buffer.asFlux()` and pushes received records downstream via the processing chain. It runs on its own scheduler via `publishOn`.

No timer, no manual poll loop, no demand checking — Reactor handles back-pressure through the `asFlux` stream. The buffer's `onResume` callback handles partition resume when the buffer drains.

#### Lifecycle

```
       start()
         │
         ▼
  ┌─────────────┐
  │   RUNNING   │
  │             │◄─────────────────┐
  │  asFlux()   │  retry on error  │
  │  ─► emit()  │──────────────────┘
  │  ─► resume? │   (retryWhen)
  │  (buffer)   │
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
| 1 | Normal emit | `asFlux` emits record via subscriber demand | Checks partition is still assigned, calls `emit.emit(received)` downstream |
| 2 | Unassigned partition | Record's partition not in current assignments | Record is silently skipped (not emitted) |
| 3 | Graceful shutdown | `shutdown` Mono emits | `takeUntilOther` completes the Flux, subscriber completion handler fires `done` signal |
| 4 | Pipeline error | Downstream operator throws (e.g. `emit`, `assignments`) | `retryWhen` with exponential backoff (500ms initial, 30s max, infinite attempts) re-subscribes to `asFlux()` |
| 5 | stop() after completion | Consumer calls `stop()` | Fires internal `shutdownSink`, awaits `done` signal, then disposes subscription and scheduler. Idempotent |
| 6 | Double start | `start()` while running | Throws `EmitterAlreadyRunning` |

#### Thread Safety

- `running` flag uses `AtomicBoolean` — `start()`/`stop()` are safe to call from different threads
- `stop()` fires `shutdownSink`, awaits `done` via `done.asMono()`, then disposes subscription and scheduler in `doFinally` — safe to call after pipeline completion (returns `Mono.empty()` if already stopped)
- The `disposable` field is null-safe: `?.dispose()` is idempotent
- The shared `ReceivedBuffer` is accessed from both Poller (writer via `offer()`) and Emitter (reader via `asFlux()`) — `InMemoryReceivedBuffer` uses `ConcurrentLinkedQueue` and is thread-safe

### Committer

The Committer (`BufferedCommitter`) owns the committed offsets state and runs a buffered commit pipeline on its own scheduler thread.

- Accepts positions via `markProcessed(position)` — updates `processedOffsets` atomically and feeds an internal `Sinks.Many<Position>`
- Batches positions with `bufferTimeout(25, 1s)` and commits the highest offset per partition
- Filters out unassigned partitions before committing (avoids committing offsets for partitions the consumer no longer owns)
- `flushForPartitions(partitions)` performs an immediate synchronous commit for revoked partitions — used in the rebalance callback to ensure offsets are saved before another consumer takes over
- `seedOffsets(offsets)` pre-populates offsets without going through the position pipeline (catch-up completion)
- Exposes `positions: Flux<Position>` for external subscribers (completion detection)
- `stop(): Mono<Void>` fires `tryEmitComplete()` on the internal sink, awaits pipeline drain, then disposes the scheduler
- Retries commits indefinitely on transient failures; the outer `retryWhen` restarts the pipeline on unexpected errors
- Owns `processedOffsets` state — no longer managed by `StreamingConsumer`

## Producer Architecture

(Single-path — serialises to Kafka via `KafkaProducer.send()` on bounded-elastic scheduler.)
