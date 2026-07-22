# Changelog

## 1.0.0 — upcoming

### Changed

- Upgraded to aelv 1.0.1 (fixes `Sink.asMany()` terminal race that could cause `BufferedCommitter` to miss pending offsets on stop)
- Test suite migrated from `runBlocking` to `Verify` pipelines — eliminates thread pool contention flakes

---

## 1.0.0-rc.2 — 2026-07-18

### Added

- `Producer.sendAll` exposed on the `Producer` interface

---

## 1.0.0-rc.1 — 2026-07-13

### Added

- `ShutdownCoordinator` — clean shutdown sequencing; eliminates unnecessary nullables
- `FakeKafkaClient` and `KafkaClient` interface for unit testing
- `Processor` interface and `DefaultProcessor` with builder API (`each`, `batch`, `groupBy`)
- Explicit offset seek and integration tests for rebalance and commit semantics
- SSL/SASL security config (`Ssl`, `SaslSsl` with `Plain`, `Scram`, `OAuthBearer`, `Kerberos`)
- Kafka header support on `Received` and `Producer`
- Typed error hierarchy (`ProzessException` subtypes) and `consumerId` config
- Transactional producer support (`TransactionalConfig`)
- POM metadata, sources/javadoc jars, signing, and Maven Central deployment scaffolding

### Changed

- Migrated from Reactor to aelv rc.3
- `ConsumerFilter` replaced with typed `DeserializationResult` (`Message`, `Tombstone`, `PoisonPill`)
- Partition and offset state ownership centralised
