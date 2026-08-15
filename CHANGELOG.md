# Changelog

All notable changes to **Talaria Saga Engine** are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/) as
best it can while still pre-1.0. Until the `1.0.0` release, breaking
changes may occur in minor version bumps; patch bumps are reserved for
backwards-compatible fixes.

This seed is generated from `git log` through the current `HEAD`
(`98325ac` - *Merge pull request #46 from Xyrces/feat/class-based-consumers*).

---

## [Unreleased]

### Added
- **Class-based topic consumers** — `ITopicConsumer<T>` / `ConsumeContext<T>` plus `MapTopic<TMessage, TConsumer>()` overloads on `TopicRegistry` and `IServiceProvider`. Class consumers are resolved from a per-message DI scope by their concrete type; the scope is created after the hop-guard and idempotency gate and disposed before retry/DLQ decisions. `TalariaListener` fails fast at `StartAsync` when class consumers are registered without an `IServiceProvider`, and `IServiceProvider` overloads optionally fail-fast when `IServiceProviderIsService` detects the consumer type is not registered. (`src/Talaria.Core/Abstractions/ITopicConsumer.cs`, `src/Talaria.Core/Registration/TopicRegistry.cs`, `src/Talaria.Core/Registration/TopicRegistryExtensions.cs`, `src/Talaria.Core/Registration/TalariaEndpointExtensions.cs`, `src/Talaria.Core/Hosting/TopicConsumerEngine.cs`, `src/Talaria.Core/Hosting/TalariaListener.cs`.)
- **Host-agnostic `TalariaListener`** — extracts all consumption orchestration from the two hosted services into a public `TalariaListener` with explicit `StartAsync` / `StopAsync` / `DisposeAsync`. Non-Generic-Host apps (console apps, custom composition roots) now get the full supervision, hop-guard, idempotency, retry, deferral-sweeper, and outbox-relay pipeline. `TalariaHostedService` is the single thin adapter delegating to the shared singleton listener. (`src/Talaria.Core/Hosting/TalariaListener.cs`, `src/Talaria.Core/Hosting/TalariaListenerStores.cs`, `src/Talaria.Core/Hosting/TopicConsumerEngine.cs`, `src/Talaria.Core/Hosting/SagaConsumerEngine.cs`, `src/Talaria.Core/Hosting/DeferralSweeperEngine.cs`, `src/Talaria.Core/Hosting/OutboxRelayEngine.cs`, `src/Talaria.Core/Hosting/ProducerCache.cs`, `src/Talaria.Core/Hosting/StateStoreAccessor.cs`, `src/Talaria.Core/Hosting/TalariaHostedService.cs`, `src/Talaria.Core/Registration/TalariaServiceExtensions.cs`.)
- **Registry-based registration overloads** — new `TopicRegistryExtensions.MapTopic` / `MapTopicWithEnvelope` and `SagaRegistryExtensions.MapSaga` allow configuring handlers and sagas directly against the registries without an `IServiceProvider`. The existing `IServiceProvider` extension methods in `TalariaEndpointExtensions` now delegate to these. (`src/Talaria.Core/Registration/TopicRegistryExtensions.cs`, `src/Talaria.Core/Registration/SagaRegistryExtensions.cs`, `src/Talaria.Core/Registration/TalariaEndpointExtensions.cs`.)
- **DI interop extensions** — `AddTalaria` registers the shared `TalariaListener` singleton; `IServiceProvider.BuildTalariaListener()` returns it for manual lifecycle management within a DI app. (`src/Talaria.Core/Registration/TalariaServiceExtensions.cs`.)
- **Delayed retries for topic handlers and saga step handlers** — opt-in `RetryPolicy` with fixed or exponential backoff, a configurable max interval, and a global `TalariaOptions.MinRetryDelay` floor. Retry copies are persisted in the existing `IDeferralStore` and republished by the sweeper, preserving the original partition key and tracking attempts via `talaria.retry.attempt` / `talaria.retry.root_message_id`. Exhausted retries dead-letter with reason `retries_exhausted`; missing `IDeferralStore` dead-letters with `retry_unavailable`. `OperationCanceledException` is never retried. New metrics: `talaria.messaging.retry.scheduled`, `talaria.messaging.retry.exhausted`, `talaria.messaging.retry.delay`; consumer activities carry the `talaria.retry.attempt` tag when present. (`src/Talaria.Core/RetryPolicy.cs`, `src/Talaria.Core/Hosting/RetryCoordinator.cs`, `src/Talaria.Core/Hosting/TopicConsumerEngine.cs`, `src/Talaria.Core/Hosting/SagaConsumerEngine.cs`, `src/Talaria.Core/Hosting/TalariaListener.cs`, `src/Talaria.Core/Abstractions/MessageHeaders.cs`, `src/Talaria.Core/Diagnostics/TalariaDiagnostics.cs`, `src/Talaria.Core/Registration/TopicRegistry.cs`, `src/Talaria.Core/Registration/TalariaEndpointExtensions.cs`, `src/Talaria.Core/TalariaOptions.cs`, `src/Talaria.Core/TalariaOptionsValidator.cs`.)
- **Single-enumeration contract for `IConsumer<T>.ConsumeAsync`** — the consumer contract now explicitly requires each `IConsumer<T>` instance to be enumerated exactly once. All transports (InMemory, Kafka, Azure Service Bus) enforce this with an `InvalidOperationException` on a second call, preventing accidental concurrent or repeated iteration that could leave offsets unsettled or messages silently dropped. (`src/Talaria.Core/Abstractions/IConsumer.cs` and per-transport consumer implementations.)
- **Fail-fast registry seal for `MapTopic`/`MapSaga` after host start** — `TopicRegistry` and `SagaRegistry` are now sealed synchronously before the hosted service snapshots its registrations. Late calls to `MapTopic` or `MapSaga` after `host.StartAsync()` throw `InvalidOperationException` with a guidance message instead of being silently ignored or corrupting the consumer plan. Both registries expose `bool IsSealed` so callers can probe state without catching exceptions. (`src/Talaria.Core/Registration/TopicRegistry.cs`, `src/Talaria.Core/Sagas/SagaRegistry.cs`, `src/Talaria.Core/Hosting/TalariaListener.cs`, `src/Talaria.Core/Hosting/TalariaHostedService.cs`.)
- **NuGet package metadata on shipped packages** — all packable projects now carry `<Description>`, `<GenerateDocumentationFile>`, and shared metadata from `Directory.Build.props` (`RepositoryUrl`, `PackageLicenseExpression`, `Authors`, etc.) so produced `.nupkg` files are publishable to GitHub Packages.
- **Azure Service Bus deferral adapter (`Talaria.Transports.AzureServiceBus.Deferral.DeferralAdapter`)** — an `IDeferralStore` that splits saga deferrals between the broker's native `ScheduledEnqueueTime` (short/medium waits within `DeferralAdapterOptions.ShortTermCutoff` and payload sizes within `DeferralAdapterOptions.MaxPayloadBytes`) and the existing lease-based `IDeferralStore` + sweeper (long/deadline). `DeferralAdapterOptions.ShortTermCutoff` defaults to 10 minutes and `DeferralAdapterOptions.MaxPayloadBytes` defaults to 256 KB; they are configured through the options action passed to `UseAzureServiceBusDeferral(...)` and are not validated by `TalariaOptionsValidator`. Extension method `UseAzureServiceBusDeferral(...)` registers the adapter as the engine's `IDeferralStore`. Unit tests in `tests/Talaria.Transports.AzureServiceBus.Tests` cover the short/long/boundary/oversize/forward paths without needing the ASB emulator.
- **Azure Service Bus transport (`Talaria.Transports.AzureServiceBus`)** — full `ITransport` implementation backing the existing onboarding saga sample over an ASB namespace. New `AzureServiceBusTransport` (singleton owning one `ServiceBusClient`), `AzureServiceBusProducer<T>` (engine-owned header stamping mirroring the Kafka producer), `AzureServiceBusConsumer<T>` (event-driven `ServiceBusProcessor` pump marshalled through a bounded `Channel<T>`; commit via `CompleteMessageAsync`, nack via DLQ-suffixed entity + complete), and `AzureServiceBusTransactionalSession` (buffered commit, InMemory-parity semantics — saga state stores do not participate, so step handlers must remain idempotent). Public extension `UseAzureServiceBusTransport(opts => ...)` (and connection-string overload) wires the transport as the engine's `ITransport`; the saga sample's `Messaging:Provider` switch in `src/Talaria.Client.Api/Program.cs` now accepts `ServiceBus` and runs `OnboardingSaga` unchanged over ASB (local Service Bus emulator by default). Helper `AzureServiceBusTransport.EnsureEntityAsync(name, kind, ct)` provisions a queue + matching DLQ-suffixed queue for the saga sample's topology.
- **ASB transport test suite** — six test families in `tests/Talaria.Transports.AzureServiceBus.Tests`: `AzureServiceBusConsumerErrorTests` (unit-level, non-emulator) verifies fatal processor errors complete the consumer channel; `EmulatorFactAttribute` gates end-to-end tests on `TALARIA_RUN_ASB_EMULATOR=1` (default: skip with an actionable message); `TransportOptionsTests` pins the public `AzureServiceBusTransportOptions` defaults and constructor validation; `ProducerHeaderDivergenceTests` exercises ASB-specific header stamping (MessageId synthesis, MessageType key, partition-key → SessionId projection, correlation-id-fallback, hop-count increment, W3C trace-context stamping) via a `RecordingSender : ServiceBusSender` double; `TransactionalSessionDivergenceTests` pins the buffered-producer commit/abort/disposal lifecycle by injecting senders into the transport’s private cache via reflection; `TransportExtensionsDiTests` covers all three `UseAzureServiceBusTransport` overloads. `EmulatorIntegrationTests` exercises roundtrip, two-group fan-out, poison DLQ, nack DLQ, and transactional commit/abort against a real ASB namespace via the local emulator when opted in. The unit tests run without Docker or the emulator and pass on every CI worker (net8.0/9.0/10.0); the emulator tests only run when `TALARIA_RUN_ASB_EMULATOR=1` is set so non-emulator builds remain unaffected.

### Changed
- **BREAKING: Hosted-service constructor shape** — `TalariaHostedService` now receives only a shared `TalariaListener` and `ILogger<TalariaHostedService>`. The previous constructor parameters (`TopicRegistry`/`SagaRegistry`, `TalariaOptions`, stores, and pipeline dependencies) are no longer injected directly into the hosted service; orchestration lives in `TalariaListener` instead. (`src/Talaria.Core/Hosting/TalariaHostedService.cs`.)
- **BREAKING: Listener log category** — consumer-loop, idempotency, retry, deferral, and outbox diagnostics are now logged under the `TalariaListener` category (`ILogger<TalariaListener>`). Filters or sinks previously keyed to `TalariaHostedService`/`SagaHostedService` categories will no longer capture these messages. (`src/Talaria.Core/Hosting/TalariaListener.cs`.)
- **BREAKING: Deferral-sweeper lifecycle** — the deferral sweeper now runs whenever an `IDeferralStore` is registered, even in topic-only hosts with no sagas. Previously it was started only inside `SagaHostedService`, so a topic-only host that registered a deferral store but no sagas will now observe sweeper activity. (`src/Talaria.Core/Hosting/TalariaListener.cs`, `src/Talaria.Core/Hosting/DeferralSweeperEngine.cs`.)
- **BREAKING: `SagaHostedService` removed** — `AddTalaria` now registers only `TalariaHostedService` as the single hosted adapter forwarding Generic Host lifecycle events to the shared `TalariaListener`. The duplicate `SagaHostedService` adapter is deleted; saga orchestration, deferral sweeping, and outbox relaying already live in `TalariaListener`. (`src/Talaria.Core/Hosting/SagaHostedService.cs` removed, `src/Talaria.Core/Hosting/TalariaHostedService.cs`, `src/Talaria.Core/Registration/TalariaServiceExtensions.cs`.)
- **Relicense the project to Apache-2.0** — replaces the root `LICENSE` file with the canonical Apache-2.0 text, updates `Directory.Build.props` `<PackageLicenseExpression>` to `Apache-2.0`, changes every `.cs` SPDX header to `Apache-2.0`, and updates `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, and GitHub issue/PR templates to reflect the new license. `docs/LICENSE-RATIONALE.md` is rewritten with the Apache-2.0 rationale; historical task-audit memos are preserved with a dated note.
- **CI publish now packs the Azure Service Bus transport** — `.github/workflows/ci.yml` `Pack NuGets` step adds `dotnet pack src/Talaria.Transports.AzureServiceBus/Talaria.Transports.AzureServiceBus.csproj` so the ASB transport is published alongside Core, Kafka, Redis, and InMemory.

### Fixed
- **Transport contract-matrix harness** — `TransportContractMatrix` now drains each test consumer exactly once per scenario, matching the new single-enumeration contract. The `__app.dlq` assertion is now gated on `TransportContractRow.SupportsApplicationDeadLetterQueue` so transports that route dead-lettered messages only to the per-topic DLQ no longer fail the shared contract test. (`tests/Talaria.Tests.TransportContract/TransportContractMatrix.cs`.)
- **ASB deferral DI ordering** — `UseAzureServiceBusDeferral()` previously called `RemoveAll(IDeferralStore)` and then re-resolved the long-term store via `sp.GetService<IDeferralStore>()` inside the adapter factory, which is either circular (the adapter being constructed is itself the only `IDeferralStore` registration) or null (the durable was just unregistered). The extension now snapshots the durable `IDeferralStore` descriptor(s) before `RemoveAll` and materialises the long-term instance directly from those captured descriptors. Also flipped the scheduler registration from `AddSingleton` to `TryAddSingleton` so tests can pre-register an `IServiceBusMessageScheduler` fake. New DI tests in `tests/Talaria.Transports.AzureServiceBus.Tests` (`DeferralAdapterDiTests`) verify `UseInMemoryDeferralStore() + UseAzureServiceBusDeferral()` resolves to a `DeferralAdapter` whose long-term path reaches the in-memory backing store, and that calling `UseAzureServiceBusDeferral()` without a prior durable registration throws synchronously.
- **ASB fatal processor errors now fault the consumer** — `AzureServiceBusConsumer<T>` previously logged all `ServiceBusProcessor.ProcessErrorAsync` errors and left the channel waiting, so fatal AMQP link/connection errors hung the consumer loop instead of triggering supervised restart with backoff. Non-transient `ServiceBusException` errors and any non-`ServiceBusException` errors now complete the active channel with the exception so `ConsumerSupervision` restarts the loop; transient errors remain logged-only. (`src/Talaria.Transports.AzureServiceBus/AzureServiceBusConsumer.cs`, `tests/Talaria.Transports.AzureServiceBus.Tests/AzureServiceBusConsumerErrorTests.cs`.)
- **InMemory DLQ backlog is unbounded** — `GetOrCreateDlqBus` was incorrectly using `ChannelCapacity` for the retained DLQ backlog, contradicting the transport's documented "DLQ topics are unbounded" guarantee. DLQ buses are now created without a capacity bound, ensuring dead letters are never dropped. (`src/Talaria.Transports.InMemory/InMemoryTransport.cs`.)
- **InMemory consumer disposal cleanup** — removed a no-op `await ValueTask.CompletedTask` from `InMemoryConsumer<T>.DisposeAsync`. (`src/Talaria.Transports.InMemory/InMemoryConsumer.cs`.)


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
