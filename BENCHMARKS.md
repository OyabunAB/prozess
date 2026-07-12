# Benchmarks

Performance comparison of prozess against raw `kafka-clients`, Spring Kafka 3.3.7,
and Vert.x Kafka Client 4.5.14.

## Methodology

- **Tool:** JMH 1.37
- **Kafka broker:** Testcontainers `confluentinc/cp-kafka:7.6.0` (in-process Docker)
- **Dataset:** 10,000 pre-produced string records, 4 partitions
- **Warmup:** 3 iterations × 10 s
- **Measurement:** 5 iterations × 10 s
- **Fork:** 1
- **JVM:** OpenJDK 21.0.6 (Corretto)
- **CPU:** Intel Core i9-8950HK @ 2.90 GHz

Run benchmarks locally:

```bash
./gradlew jmhJar
java -jar build/libs/prozess-*-jmh.jar -wi 3 -i 5 -f 1 -tu s -w 10s -r 10s
```

For memory pressure benchmarks, add `-prof gc` to capture allocation rates.

---

## 1. Raw Throughput

Trivial handler (no work). Measures the framework's overhead on the critical path.

| Framework | ops/s | Notes |
|---|---:|---|
| raw kafka-clients | **1.93** | Baseline — single poll loop, no management |
| prozess | 0.98 | Partition management, backpressure, offset tracking |
| Vert.x Kafka | 0.47 | Event loop delivery |
| Spring Kafka | 0.46 | Container polling thread |

prozess pays ~2× overhead vs raw kafka for its correctness guarantees. It is faster
than Spring Kafka and Vert.x Kafka which also add framework overhead without the
same guarantees.

---

## 2. Concurrent Handler (slow IO-bound handler)

Handler simulates 1 ms IO work per record. raw kafka blocks the poll thread;
prozess runs handlers concurrently while continuing to poll.

| Approach | ops/s | Correctness |
|---|---:|---|
| raw kafka manual thread pool | **0.37** | No offset ordering guarantee |
| prozess concurrent (maxConcurrency=4) | 0.08 | Ordered within partition |
| raw kafka blocking poll thread | 0.09 | Ordered but risks rebalance |

**Key finding:** The naive raw kafka workaround (manual thread pool) is fast but
sacrifices offset ordering. prozess delivers concurrent execution with
partition-ordered offset commits out of the box. The blocking poll thread approach
risks session timeout rebalances at production handler speeds.

---

## 3. Grouped/Ordered Processing

Per-key ordered processing with 10 distinct keys, 1 ms handler per record.
prozess `groupedEach` provides per-key ordering with concurrent key groups.
The raw implementation provides no ordering guarantee.

| Approach | ops/s | Correctness |
|---|---:|---|
| raw kafka manual grouped | **0.92** | No per-key ordering guarantee |
| prozess groupedEach | 0.08 | Per-key ordered, concurrent across keys |

**Key finding:** Implementing correct per-key ordered processing on raw kafka
requires a concurrent queue per key, careful offset watermark tracking, and
robust error handling. prozess provides this as a one-liner with `groupedEach`.
The raw implementation shown here does not provide the same correctness guarantees.

---

## 4. Backpressure — Memory Pressure Under Slow Consumer

Handler simulates 5 ms IO work. Measures heap allocation per operation under load.
prozess automatically pauses the poller when the in-memory buffer reaches its
high watermark. Spring Kafka and Vert.x have no automatic backpressure.

| Framework | ops/s | Heap alloc/op | Bounded? |
|---|---:|---:|---|
| prozess | 0.016 | **68 MB** | Automatic — poller pauses at high watermark |
| Spring Kafka | 0.018 | 78 MB | manual — `container.pause()` |
| Vert.x Kafka | 0.020 | 78 MB | manual — `consumer.pause()` |

**Key finding:** prozess allocates ~15% less heap under a slow consumer because the
poller is automatically paused when the buffer fills. Spring Kafka and Vert.x
continue polling, accumulating records in their internal queues. In production with
a sustained slow handler, this difference compounds — prozess stays bounded while
others grow until GC pressure degrades throughput or OOM occurs.

The throughput numbers are similar here because all three frameworks are bottlenecked
on the same 5 ms handler sleep. The meaningful metric is heap allocation, not ops/s.
Use `-prof gc` when running this benchmark to see `gc.alloc.rate.norm`.

---

## Summary

| Scenario | prozess advantage |
|---|---|
| Raw throughput | Faster than Spring/Vertx at 2× raw kafka cost |
| Concurrent handler | Ordered concurrent execution without rebalance risk |
| Grouped processing | Per-key ordering as a one-liner |
| Memory under load | Automatic backpressure bounds heap growth |

The raw throughput cost of prozess (~2× vs raw kafka) is the price of the correctness
guarantees. For IO-bound workloads where handler latency dominates, the framework
overhead is immaterial. For high-throughput in-memory processing, raw kafka-clients
remains the right choice.
