# Talaria Saga Engine

Talaria is a distributed messaging and saga orchestration library for **.NET** (multi-targeting `net8.0`, `net9.0`, `net10.0`) built on **Confluent Kafka**, **Azure Service Bus**, and **Redis**, with a zero-dependency in-memory provider for lightweight single-process deployments, prototyping, and tests.

## Delivery guarantees — stated precisely

Talaria provides **at-least-once delivery with idempotent processing**:

- When delayed retries are disabled (the default), consumers commit offsets only after successful processing; unhandled handler failures are routed to the DLQ. When retries are enabled, a handler failure commits the original delivery as soon as a retry copy is durably scheduled in the `IDeferralStore`; the sweeper republishes the retry copy later. The original message is therefore acknowledged before the retry runs, and the idempotency store (keyed by `MessageId`) ensures the retry copy — which carries a freshly minted `MessageId` — is processed exactly once.
- A distributed idempotency store (`IIdempotencyStore`) deduplicates by `MessageId` using fencing-token locks, so redeliveries and duplicate publishes are processed exactly once per consumer group.
- Saga outbound messages go through a **transactional outbox**: the state transition and its outbound messages are staged in one atomic store operation (`IStateStore.TransitionAsync`), then a leased relay publishes them at-least-once. Each staged message carries a minted `MessageId`, so a duplicate publish after a relay crash is deduplicated by the downstream idempotency gate. A crash after the atomic transition loses nothing.
- The replay window that remains is between the atomic transition and the offset commit: a crash there replays the message against transitioned state. Starter steps are protected by a built-in replay guard; custom step handlers should be idempotent.
- Deferrals and outbox entries use **lease (visibility-timeout) semantics** — the Azure Service Bus peek-lock analogue: acquiring an entry hides it for the lease duration instead of removing it, so a sweeper/relay crash never loses a message; the lease expires and another worker re-acquires it. Completions are fenced by a monotonic lease token.

## Core Features

- **Decoupled architecture:** provider-agnostic abstractions (`ITransport`, `IStateStore`, `IIdempotencyStore`, `IDeferralStore`, `IOutboxStore`, `IIdempotencyVerifier`, `ITopologyProvisioner`, `IDeadLetterHandler`) with Kafka, Azure Service Bus, Redis, and in-memory implementations.
- **Saga orchestration:** strongly typed state machines via `MapSaga<TState>` with explicit correlation and explicit dispatch routing (`DispatchTo`).
- **Idempotency:** fencing-token locks (`SETNX` on Redis) filter duplicate `MessageId`s across a cluster; a stale lock holder can never release another worker's lock.
- **Transactional outbox:** saga state transitions and their outbound messages are staged atomically (single Lua script on Redis, one lock in-memory); a background relay publishes staged messages with lease + fencing semantics. Registered automatically by `UseRedisStateStore()` / `UseInMemoryStateStore()`.
- **Durable deferral:** out-of-order saga messages (a step arriving before the starter) are persisted in an `IDeferralStore` (Redis sorted set or in-memory) and republished by a background sweeper using visibility-timeout leases — they survive restarts and sweeper crashes, unlike an in-process timer.
- **Delayed retries:** topic handlers and saga step handlers can opt in to a configurable number of fixed or exponential backoff retries before routing to the DLQ. Retry copies are persisted in the same `IDeferralStore` as saga deferrals and republished by the sweeper, preserving the original partition key and tracking attempts via `talaria.retry.attempt` / `talaria.retry.root_message_id` headers.
- **Observability:** OpenTelemetry-native traces and metrics with W3C trace-context propagation across produce/consume boundaries. Includes relay monitoring: `talaria.outbox.*` / `talaria.deferral.*` counters and histograms for published/failed entries, re-acquisitions (lease expiry signal), active leases, and relay lag. Retries emit `talaria.messaging.retry.scheduled`, `talaria.messaging.retry.exhausted`, and `talaria.messaging.retry.delay`.
- **Dead-letter resiliency:** automatic DLQ routing (suffix configurable via `DlqSuffix`, default `.dlq`) for handler exceptions, deserialization failures, missing correlation ids, unmapped dispatches, exceeded hop/deferral thresholds, exhausted retries (`retries_exhausted`), and retry misconfiguration (`retry_unavailable`). Exception detail in DLQ headers is gated behind `TalariaOptions.IncludeExceptionDetailsInDlq` (off by default).

---

## 🚀 Getting Started Locally

This repository uses **.NET Aspire** to emulate the distributed environment. **Docker** (or OrbStack) is required for the integration suites.

### 1. Boot up the Aspire Host Environment

```bash
cd src/Talaria.AppHost
dotnet run
```

The AppHost prompts for a `grafana-admin-password` parameter (or supply it via configuration) and spins up Kafka, Redis, Prometheus, Tempo, Grafana, and three replicas of the sample API.

### 2. Running the Core Integration Suite

The AppHost tests horizontally scale 3 API replicas and bombard them with identical requests to prove exactly-once handler execution.

```bash
cd tests/Talaria.AppHost.Tests
dotnet test
```

---

## 🛠 Usage & Configuration

### Dependency Injection

```csharp
// Distributed production configuration (Kafka + Redis)
builder.Services.AddTalaria()
    .UseKafkaTransport(opts =>
    {
        opts.BootstrapServers = builder.Configuration.GetConnectionString("kafka")!;
        // SASL/SSL: configure opts.BaseProducerConfig / opts.BaseConsumerConfig.
        // The transport logs a startup warning when connecting to non-localhost
        // brokers over PLAINTEXT.
    })
    .UseRedisStateStore(opts =>
    {
        // For production include TLS and auth: "host:6379,ssl=true,password=..."
        opts.Configuration = builder.Configuration.GetConnectionString("redis")!;
        opts.KeyPrefix = "mystate:";
    })
    .UseRedisIdempotencyStore(opts =>
    {
        opts.Configuration = builder.Configuration.GetConnectionString("redis")!;
        opts.KeyPrefix = "mystate:";
    })
    .UseRedisDeferralStore(opts =>
    {
        opts.Configuration = builder.Configuration.GetConnectionString("redis")!;
        opts.KeyPrefix = "mystate:";
    });
```

All `UseRedis*` calls share one options registration — configure callbacks accumulate, so `KeyPrefix` only needs to be set once. `UseRedisStateStore` also registers the Redis transactional outbox used for saga dispatch.

```csharp
// Azure Service Bus transport: same saga code, swap the transport extension.
// Connection string comes from the namespace's "Shared access policies" blade,
// or from the Service Bus emulator ("UseDevelopmentEnvironment=true") for local runs.
builder.Services.AddTalaria()
    .UseAzureServiceBusTransport(opts =>
    {
        opts.ConnectionString = builder.Configuration.GetConnectionString("servicebus")
            ?? "UseDevelopmentEnvironment=true";
    })
    .UseRedisStateStore(opts =>
    {
        opts.Configuration = builder.Configuration.GetConnectionString("redis")!;
        opts.KeyPrefix = "mystate:";
    })
    .UseRedisIdempotencyStore(opts =>
    {
        opts.Configuration = builder.Configuration.GetConnectionString("redis")!;
        opts.KeyPrefix = "mystate:";
    })
    .UseRedisDeferralStore(opts =>
    {
        opts.Configuration = builder.Configuration.GetConnectionString("redis")!;
        opts.KeyPrefix = "mystate:";
    });
```

> The ASB transport implements the same `ITransport` / `IConsumer<T>` / `IProducer<T>` contract as Kafka and InMemory, so existing saga code runs unchanged — see [`src/Talaria.Client.Api/Sagas/OnboardingSaga.cs`](src/Talaria.Client.Api/Sagas/OnboardingSaga.cs) and the `ServiceBus` branch of the `Messaging:Provider` switch in [`src/Talaria.Client.Api/Program.cs`](src/Talaria.Client.Api/Program.cs).
>
> The ASB transport currently provides buffered transactional semantics (mirroring the in-memory transport): produces obtained via `BeginTransactionAsync` are committed atomically when the session commits and discarded on abort. Consumer-offset transactions (KIP-98-style exactly-once with the broker) are not yet implemented — the saga state store + idempotency store provide the same end-to-end exactly-once guarantees as the InMemory transport, and saga step handlers must remain idempotent.
>
> Entity provisioning (queues, topics, subscriptions) is the host's responsibility and is exposed through the `ITopologyProvisioner` abstraction. The transport exposes a convenience `EnsureEntityAsync(name, kind, ct)` helper for the saga sample; production deployments should call `ITopologyProvisioner.ProvisionAsync(declarations, ct)` from their startup code so the host's full topology is declared in one place.
>
> Native-dedup short-circuit: ASB surfaces a built-in `MessageId`-based duplicate detection window on queues and topics. Hosts that opt in via `ITransport` implementations of `IIdempotencyVerifier` let the engine skip deserialization + handler invocation for duplicates — see [`src/Talaria.Core/Abstractions/IIdempotencyVerifier.cs`](src/Talaria.Core/Abstractions/IIdempotencyVerifier.cs).

```csharp
// Zero-dependency configuration (in-memory): lightweight single-process
// deployments, prototyping, and tests — no backing message bus required.
// The in-memory transport mirrors Kafka semantics: consumer-group fan-out,
// backlog replay for late-joining groups, transactional produce buffering,
// and redelivery of uncommitted messages on consumer restart.
builder.Services.AddTalaria()
    .UseInMemoryTransport()
    .UseInMemoryStateStore()
    .UseInMemoryIdempotencyStore()
    .UseInMemoryDeferralStore();
```

> Without an `IDeferralStore`, out-of-order saga messages and delayed retry copies
> are routed to the DLQ (`deferral_unavailable` / `retry_unavailable`) instead of
> being deferred.
>
> Without an `IOutboxStore` (registered automatically by both state stores above),
> saga dispatch falls back to direct transactional produce — the state save and the
> message publish are not atomic in that mode, and a startup warning is logged.

### Simple Stateless Handlers

```csharp
app.Services.MapTopic<SendVerificationEmailCommand>("email-commands", async (msg, ct) =>
{
    await EmailService.Dispatch(msg.Email);
});
```

### Delayed retries

Opt-in per topic (or globally via `TalariaOptions.DefaultRetryPolicy`). Retry copies are scheduled in the configured `IDeferralStore` and republished by the sweeper; attempts are exhausted to the DLQ with reason `retries_exhausted`. If retries are enabled but no `IDeferralStore` is registered, messages dead-letter with reason `retry_unavailable`.

```csharp
// Per-topic policy (passed last to avoid overload ambiguity)
app.Services.MapTopic<FetchExternalReportCommand>("report-requests",
    async (msg, ct) =>
    {
        await ReportService.Fetch(msg.ReportId);
    },
    new RetryPolicy
    {
        MaxRetryAttempts = 5,
        RetryInterval = TimeSpan.FromSeconds(2),
        BackoffType = RetryBackoffType.Exponential,
        MaxRetryInterval = TimeSpan.FromMinutes(1),
    });

// Global default applied to all topics/saga steps that do not declare their own
builder.Services.AddTalaria(opts =>
{
    opts.DefaultRetryPolicy = new RetryPolicy
    {
        MaxRetryAttempts = 3,
        RetryInterval = TimeSpan.FromSeconds(1),
        BackoffType = RetryBackoffType.Fixed,
    };
    opts.MinRetryDelay = TimeSpan.FromMilliseconds(100); // hard floor for any computed delay
});
```

`OperationCanceledException` is never retried — it falls through to the existing DLQ behavior.

### Executing Sagas

```csharp
services.MapSaga<OnboardingState>(saga =>
{
    // Starter: begins a new saga instance, correlated by AccountId.
    saga.StartedBy<CreateAccountCommand>(
        "onboarding-commands",
        async (msg, context) =>
        {
            var state = new OnboardingState { AccountId = msg.AccountId, Email = msg.Email };

            context.Dispatch(new SendVerificationEmailCommand
            {
                AccountId = msg.AccountId,
                Email = msg.Email
            });

            return context.Transition(state); // persist state, await the next event
        },
        correlateBy: msg => msg.AccountId);

    // Follow-up step: correlates onto the existing state.
    saga.On<AccountVerifiedEvent>(
        "account-events",
        async (state, msg, context) =>
        {
            state.VerificationReceived = true;
            return context.Complete(); // finalize and purge state
        },
        correlateBy: msg => msg.AccountId);

    // Explicit dispatch routing: every dispatched message type must declare its topic.
    saga.DispatchTo<SendVerificationEmailCommand>("email-commands");
});
```

Notes:

- `context.Transition(state)` persists the state; `context.Complete()` deletes it; `context.Defer()` reschedules the current message via the deferral store (any dispatches queued in the same handler invocation are discarded).
- Dispatches are staged in the transactional outbox atomically with the state transition and published asynchronously by the relay — expect a small relay-latency delay (poll interval configurable via `TalariaOptions.OutboxRelayInterval`, default 250ms).
- Dispatching a message type with no `DispatchTo` mapping dead-letters the triggering message (`unmapped_dispatch`) without saving state.
- Multiple saga steps (and stateless handlers) may share one topic — messages are fanned out by a `talaria.message_type` header.

### Host-agnostic usage (console apps / custom composition roots)

Talaria can run without `IHost` via `TalariaListener`. It exposes explicit `StartAsync` / `StopAsync` and supports the full supervision, hop-guard, idempotency, retry, deferral-sweeper, and outbox-relay pipeline.

```csharp
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;

var transport = new InMemoryTransport();
var topicRegistry = new TopicRegistry();
var sagaRegistry = new SagaRegistry();

topicRegistry.MapTopic<SendVerificationEmailCommand>("email-commands", async (msg, ct) =>
{
    await EmailService.Dispatch(msg.Email);
});

sagaRegistry.MapSaga<OnboardingState>(saga =>
{
    saga.StartedBy<CreateAccountCommand>("onboarding-commands", async (msg, ctx) =>
    {
        var state = new OnboardingState { AccountId = msg.AccountId };
        ctx.Dispatch(new SendVerificationEmailCommand { AccountId = msg.AccountId });
        return ctx.Transition(state);
    }, correlateBy: msg => msg.AccountId);

    saga.DispatchTo<SendVerificationEmailCommand>("email-commands");
});

// Sagas require an IServiceProvider that can resolve IStateStore<TState>.
var services = new ServiceCollection()
    .AddSingleton<ITransport>(transport)
    .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
    .BuildServiceProvider();

await using var listener = new TalariaListener(
    transport,
    topicRegistry,
    sagaRegistry,
    new TalariaOptions { ApplicationName = "my-console-app" },
    LoggerFactory.Create(b => b.AddConsole()).CreateLogger<TalariaListener>(),
    services);

await listener.StartAsync();
Console.WriteLine("Press enter to stop...");
Console.ReadLine();
await listener.StopAsync();
```

In a DI-based app you can still grab the singleton listener manually:

```csharp
var listener = app.Services.BuildTalariaListener();
await listener.StartAsync();
// ...
await listener.StopAsync();
```

`TalariaListener` is single-cycle: `StartAsync` after `StopAsync` throws `InvalidOperationException`. Double-start and double-stop are idempotent no-ops. Disposing the listener stops it if running but does not dispose caller-owned transports or stores.

---

## ⚠️ Sample API

`src/Talaria.Client.Api` is a demo, not production-hardened: it has no authentication or authorization, and its `/api/diagnostics/*` endpoint exists for integration tests. The `Messaging:Provider` configuration switch selects between the Kafka, Azure Service Bus, and in-memory transports — flip the value to `Kafka`, `ServiceBus`, or `InMemory` and the existing onboarding saga (`src/Talaria.Client.Api/Sagas/OnboardingSaga.cs`) runs over the chosen transport with no code changes. Do not deploy it as-is.

---

## 🔎 Personal-refs guard

The repository ships a vendored copy of the open-source release sweep at
[`scripts/check-personal-refs.sh`](scripts/check-personal-refs.sh). Run it
locally before pushing to catch accidental personal or host-local references
that task-11 scrubbed from the codebase:

```bash
PERSONAL_REFS_GUARD=deny scripts/check-personal-refs.sh
```

`PERSONAL_REFS_GUARD` accepts `allow` (suppress), `warn` (default, emit
GitHub-Actions-style `::warning::` annotations), or `deny` (fail the run).
The script's `--self-test` flag plants known hits in a temp dir and asserts
every pattern fires; `tests/Talaria.Ci.Tests` exercises the script end-to-end.

Wiring the script into `.github/workflows/ci.yml` is owned by the devops
agent and tracked as a follow-up; the script is callable from any CI step
that can run `bash`.

---

## 📄 License

Talaria is released under the **Apache License, Version 2.0** ([`LICENSE`](LICENSE) at the repo root). You are free to use, modify, and redistribute the source, including in proprietary applications and hosted services, subject to the Apache-2.0 attribution and patent-grant terms.

The rationale behind this choice is documented in
[`docs/LICENSE-RATIONALE.md`](docs/LICENSE-RATIONALE.md).

A **separate commercial offering** is available for organizations that need:

- Commercial support, SLAs, and security-fix backports.
- Hosted or managed deployments of Talaria.
- Additional proprietary-relicensing options.

The commercial offering is delivered under its own repository and license terms and is intentionally **out of scope for this repo**. The canonical commercial-licensing terms and contact channels are documented in [`docs/LICENSE-RATIONALE.md`](docs/LICENSE-RATIONALE.md) and `SECURITY.md` §Commercial channels.

---

## 📦 Publishing & CI/CD

The GitHub Actions workflow runs restore, build, the full test suite (Docker-backed), and a `dotnet list package --vulnerable --include-transitive` audit on every push and PR — with `contents: read` only. Pushes to `main` additionally run a separate publish job (the only job holding `packages: write`) that packs the four libraries and pushes them to GitHub Packages via `GITHUB_TOKEN`.
