# Talaria Saga Engine

Talaria is a highly scalable, distributed messaging and saga orchestration engine built tightly on **.NET 8.0 Minimal APIs**, **Confluent Kafka**, and **Redis**. It is specifically designed to handle extreme concurrency across a clustered environment while delivering bulletproof **Exactly-Once Delivery** and strict transactional boundaries.

## Core Features

- **Decoupled Architecture:** Extensible abstractions (`ITransport`, `IStateStore`, `IIdempotencyStore`) allowing components like Kafka or Redis to be isolated or swapped seamlessly.
- **Saga Orchestration:** Define asynchronous workflows via strongly typed state machines (`MapSaga<TState>`) that resolve sequential logic, correlation IDs, and complex persistence bounds elegantly.
- **Exactly-Once Delivery (Idempotency):** A native, framework-level distributed lock footprint (`RedisIdempotencyStore` utilizing `SETNX`) filters `MessageId` collisions globally. Duplicate payloads broadcast by unstable network overlays are halted dynamically before code execution begins!
- **Observability Native:** Embedded telemetry tracking natively leverages OpenTelemetry standards (W3C), tracking identical trace IDs across generic `.AppHost` distributions gracefully bridging Consumer and Producer lifecycles visually.
- **Out-of-Order Deferral Support:** Advanced background deferment tracks attempts on out-of-sequence incoming messages (e.g. step 2 arriving before the Saga starter logic).
- **Dead-Letter Resiliency:** Automatic configurable DLQ routing for exceptions, un-correlated records, or exceeded hop thresholds `DlqSuffix`.

---

## 🚀 Getting Started Locally

This repository uses **.NET Aspire** to natively emulate and bind the required distributed environments effortlessly. **TestContainers** or a standing local equivalent of Docker/OrbStack is required.

### 1. Boot up the Aspire Host Environment
To easily spin up an isolated API alongside ephemeral physical Kafka topics and Redis datastores automatically:

```bash
cd src/Talaria.AppHost
dotnet run
```
You will be greeted with the Aspire dashboard natively proxying identical Kafka streams and Minimal API HTTP requests securely.

### 2. Running the Core Integration Suite
The test architecture simulates real-world load boundaries (e.g., horizontally scaling 3 independent API nodes simultaneously overlapping exactly duplicate identical footprints).

```bash
cd tests/Talaria.AppHost.Tests
dotnet test
```

---

## 🛠 Usage & Configuration

### Dependency Injection
Integrating Talaria natively binds through intuitive extension extensions injected efficiently into the Minimal API request builder pipeline:

```csharp
// Distributed Production Configuration (Kafka + Redis)
builder.Services.AddTalaria()
    .UseKafkaTransport(opts =>
    {
        opts.BootstrapServers = builder.Configuration.GetConnectionString("kafka");
    })
    .UseRedisStateStore(opts =>
    {
        opts.Configuration = builder.Configuration.GetConnectionString("redis");
        opts.KeyPrefix = "mystate:";
    })
    .UseRedisIdempotencyStore(opts => 
    {
        opts.Configuration = builder.Configuration.GetConnectionString("redis");
    });

// Zero-Dependency Local & Testing Configuration (In-Memory)
builder.Services.AddTalaria()
    .UseInMemoryTransport()
    .UseInMemoryStateStore()
    .UseInMemoryIdempotencyStore();
```

### Simple Stateless Handlers
Leverage the framework safely to perform direct interactions without engaging heavy Saga footprint tracking:

```csharp
app.Services.MapTopic<SendVerificationEmailCommand>("email-commands", async (msg, ct) =>
{
    // Execute identical tasks bounded exactly once
    await EmailService.Dispatch(msg.Email);
});
```

### Executing Sagas
Map transitions spanning multiple physical topics over prolonged time lengths efficiently:

```csharp
public static void ConfigureOnboardingSaga(IServiceProvider services)
{
    services.MapSaga<OnboardingState>(saga =>
    {
        // Define standard starter footprint 
        saga.StartedBy<CreateAccountCommand>("onboarding-commands", msg => msg.AccountId)
            .TransitionAsync(async (state, message, context) =>
            {
                state.Status = "Created";
                state.Email = message.Email;
                
                context.Dispatch(new SendVerificationEmailCommand
                {
                    AccountId = message.AccountId,
                    Email = message.Email
                });
                
                return context.Transition(state);
            });

        // Correlate subsequent async system interactions onto the existing state efficiently
        saga.HandledBy<AccountVerifiedEvent>("account-events", msg => msg.AccountId)
            .TransitionAsync((state, message, context) =>
            {
                state.Status = "Verified";
                // Saga is formally complete, deleting native Redis tracked states safely!
                return Task.FromResult(context.Complete());
            });
    });
}
```

---

## 📦 Publishing & CI/CD
This ecosystem leverages a robust **GitHub Actions workflow**.
Pushing to the `main` branch automatically checks regression integrations, performs `Release` packagings on exclusively relevant core elements, and publishes identically-versioned NuGet outputs into the native **GitHub Packages Repository** dynamically tied securely via `GITHUB_TOKEN`.
