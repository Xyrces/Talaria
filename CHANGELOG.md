# Changelog

All notable changes to **Talaria Saga Engine** are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/) as
best it can while still pre-1.0. Until the `1.0.0` release, breaking
changes may occur in minor version bumps; patch bumps are reserved for
backwards-compatible fixes.

This seed is generated from `git log` through the current `HEAD`
(`424ef8d` - *Architecture & security review remediation: outbox,
leases, hardening*, PR #3).

---

## [Unreleased]

_Nothing yet._

## [0.3.0] - 2026-08-08 - Architecture & security review remediation

The headline PR (#3) consolidating Phase 9/10 cleanup, transactional
outbox, lease-based deferral, transport hardening, and OpenTelemetry
relay monitoring.

### Added
- **Transactional outbox** for saga dispatch: state transitions and
  their outbound messages are staged atomically and a leased relay
  publishes them at-least-once. (`14864c0`)
- **Lease-based (visibility-timeout) deferral store**, replacing the
  in-process delayed republish. Crash-safe: a sweeper crash never
  loses a message - the lease expires and another worker re-acquires
  it. (`0a819e9`)
- **OpenTelemetry relay monitoring** for outbox and deferral loops
  (`talaria.outbox.*` / `talaria.deferral.*` counters and histograms
  for published/failed entries, re-acquisitions, active leases, and
  relay lag). (`0582eda`)
- **In-memory redelivery semantics + transport contract suite + docs**
  so the in-memory transport matches Kafka-parity guarantees and the
  contracts are pinned by tests. (`0bd2e0f`)
- **Durable saga deferral store replacing in-process delayed republish.**
  (`55834b7`)
- **Real transactional sessions for Kafka and InMemory transports.**
  (`370d41b`)
- **Unified consumer engines on a shared pipeline; explicit saga
  dispatch topics.** (`15c6c6a`)

### Changed
- **Kafka transport hardening** - thread-safety, lifecycle, logging,
  and a shared producer pool. (`f67c54d`)
- **DI cleanup**, **InMemory Kafka-parity**, and **sample API + CI
  hardening.** (`d0c71c0`)
- **Phase 9/10** - dispatch validation, test backfill, and README
  accuracy alignment. (`e22accf`)
- **Phase 9 cleanup** - removed tautologies, deduplicated tests,
  introduced deterministic waits. (`4006bd6`)
- **Dependency hygiene** - cleared all transitive vulnerability
  findings. (`9e3ffb2`)
- **Critical message-loss fixes**, idempotency fencing,
  type-design hardening, and removal of a dead source generator.
  (`9e61a3a`)

### Fixed
- **Kafka consumer commit loss + subscription churn**
  (CI-found, broker-verified). (`e14f348`)
- **Restore per-enumeration consumer sessions**; fix transactional
  test pattern. (`32ef75b`)

## [0.2.0] - 2026-08-06 - In-memory idempotency & transport options

PR #2.

### Added
- **`InMemoryIdempotencyStore`**, extension methods, and a
  configurable in-memory transport option for the sample API - lets
  lightweight single-process deployments and tests run without Kafka
  or Redis. (`d96cde4`, `766aa9c`)

## [0.1.1] - 2026-08-06 - Engine improvements & bugfixes

PR #1.

### Added
- **OpenTelemetry dashboards via Aspire** - Grafana, Prometheus, and
  Tempo telemetry arrays injected directly into AppHost topology,
  establishing event-node mapping dashboards alongside Aspire
  defaults. (`2a771e9`)

### Changed
- **Engine improvements** - explicit Kafka offset commits, multi-hop
  cycle detection, and Docker-guarded integration test suite.
  (`b8a4e93`)

### Fixed
- **Cache Kafka producers per topic** to prevent the metadata-socket
  stampede, and set an explicit `HttpClient` timeout. (`97607ed`)
- **Use `AppHostDirectory` for absolute bind mounts** in
  `AppHost/Program.cs`. (`db1d216`)
- **Prevent 100% CPU tight spin loop** on `KafkaConsumer` polling
  exceptions and empty poll results. (`6f70027`)
- **Refactor `KafkaConsumer` to use `Channel` and
  `TaskCreationOptions.LongRunning`** to prevent ThreadPool thread
  starvation. (`f5e7154`)
- **Skip AppHost multi-container tests in `GITHUB_ACTIONS` CI** to
  prevent runner resource exhaustion. (`c874bce`)

## [0.1.0] - 2026-04-02 - Initial saga engine

First public cut of Talaria.

### Added
- **Initial commit of Talaria Saga Engine.** (`c478d6d`)
- **MinVer** for semantic versioning. (`1d2b4ea`)
- **Multi-targeting**: `net8.0`, `net9.0`, and `net10.0` across the
  library ecosystem, enabling pipeline executions. (`f026237`)

### Changed
- **Multi-target frameworks** applied; `chore: remove coveragereport`
  from git tracking. (`f26825a`)

### Fixed
- **Replace explicit test race-condition timeouts** mirroring
  exponential-backoff polling, handling GitHub Runner virtualization
  delays naturally. (`af4f250`)
- **Resolve `CS0246` missing compiler directive** reference for
  `RedisValue`, repairing scaled verification integration tests.
  (`b97099d`)
- **Bump default timeout** gracefully and securely map target
  assertion payload matching JSON structurally, eliminating
  integration flakes. (`6e3697c`)
- **Resolve GitHub package deployment failure** mapping static source
  URLs explicitly, avoiding alias-referencing errors. (`2ce26b1`)

---

[Unreleased]: https://github.com/Xyrces/Talaria/compare/424ef8d...HEAD
[0.3.0]: https://github.com/Xyrces/Talaria/compare/1021113...424ef8d
[0.2.0]: https://github.com/Xyrces/Talaria/compare/5ec3947...1021113
[0.1.1]: https://github.com/Xyrces/Talaria/compare/2a771e9...5ec3947
[0.1.0]: https://github.com/Xyrces/Talaria/releases/tag/c478d6d
