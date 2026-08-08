# Talaria Saga Engine

Talaria is a distributed messaging and saga orchestration library for **.NET** (multi-targeting `net8.0`, `net9.0`, `net10.0`) built on **Confluent Kafka** and **Redis**, with a zero-dependency in-memory provider for tests and local development.

## Delivery guarantees — stated precisely

Talaria provides **at-least-once delivery with idempotent processing**:

- Consumers commit offsets only after successful processing, so failures redeliver.
- A distributed idempotency store (`IIdempotencyStore`) deduplicates by `MessageId` using fencing-token locks, so redeliveries and duplicate publishes are processed exactly once per consumer group.
- Saga outbound messages and the consumed message's offset commit in a **single Kafka transaction** (exactly-once semantics for the produce + offset boundary).
- Saga state (Redis) **cannot** join that Kafka transaction: a crash between the state save and the transaction commit replays the message against transitioned state. Starter steps are protected by a built-in replay guard; custom step handlers should be idempotent.

## Core Features

- **Decoupled architecture:** provider-agnostic abstractions (`ITransport`, `IStateStore`, `IIdempotencyStore`, `IDeferralStore`) with Kafka/Redis and in-memory implementations.
- **Saga orchestration:** strongly typed state machines via `MapSaga<TState>` with explicit correlation and explicit dispatch routing (`DispatchTo`).
- **Idempotency:** fencing-token locks (`SETNX` on Redis) filter duplicate `MessageId`s across a cluster; a stale lock holder can never release another worker's lock.
- **Durable deferral:** out-of-order saga messages (a step arriving before the starter) are persisted in an `IDeferralStore` (Redis sorted set or in-memory) and republished by a background sweeper — they survive restarts, unlike an in-process timer.
- **Observability:** OpenTelemetry-native traces and metrics with W3C trace-context propagation across produce/consume boundaries.
- **Dead-letter resiliency:** automatic DLQ routing (suffix configurable via `DlqSuffix`, default `.dlq`) for handler exceptions, deserialization failures, missing correlation ids, and exceeded hop/deferral thresholds. Exception detail in DLQ headers is gated behind `TalariaOptions.IncludeExceptionDetailsInDlq` (off by default).

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

All `UseRedis*` calls share one options registration — configure callbacks accumulate, so `KeyPrefix` only needs to be set once.

```csharp
// Zero-dependency local & testing configuration (in-memory)
builder.Services.AddTalaria()
    .UseInMemoryTransport()
    .UseInMemoryStateStore()
    .UseInMemoryIdempotencyStore()
    .UseInMemoryDeferralStore();
```

> Without an `IDeferralStore`, out-of-order saga messages are routed to the DLQ
> (`deferral_unavailable`) instead of being deferred.

### Simple Stateless Handlers

```csharp
app.Services.MapTopic<SendVerificationEmailCommand>("email-commands", async (msg, ct) =>
{
    await EmailService.Dispatch(msg.Email);
});
```

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

- `context.Transition(state)` persists the state; `context.Complete()` deletes it; `context.Defer()` reschedules the current message via the deferral store.
- Dispatching a message type with no `DispatchTo` mapping throws at processing time.
- Multiple saga steps (and stateless handlers) may share one topic — messages are fanned out by a `talaria.message_type` header.

---

## ⚠️ Sample API

`src/Talaria.Client.Api` is a demo, not production-hardened: it has no authentication or authorization, and its `/api/diagnostics/*` endpoint exists for integration tests. Do not deploy it as-is.

---

## 📦 Publishing & CI/CD

The GitHub Actions workflow runs restore, build, the full test suite (Docker-backed), and a `dotnet list package --vulnerable --include-transitive` audit on every push and PR — with `contents: read` only. Pushes to `main` additionally run a separate publish job (the only job holding `packages: write`) that packs the four libraries and pushes them to GitHub Packages via `GITHUB_TOKEN`.
