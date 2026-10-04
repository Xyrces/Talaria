# Talaria minimal messaging quickstart

This runnable example uses a real Generic Host, Talaria's `IMessageBus`, and the in-memory transport. It needs only the .NET SDK:

```bash
dotnet run --project samples/MinimalMessaging
```

`MapCommand<T>` creates one work queue with competing consumers. Use it for an instruction that should be handled by one consumer group. `MapEvent<T>` creates a topic subscription whose consumer identity is scoped to the application name; use it for facts that independent subscribers may each receive. The no-destination `SendAsync` and `PublishAsync` resolve the matching endpoint from the message type. Destination overloads are available when routing explicitly.

The example is in [`samples/MinimalMessaging/Program.cs`](../samples/MinimalMessaging/Program.cs). The in-memory provider is useful for local work and tests; its data disappears when the process exits.

## Dependency injection handlers

Minimal endpoint delegates bind parameters by type. A handler may receive the message, a `CancellationToken`, a `ConsumeContext<T>`, or services registered in DI. For example:

```csharp
services.AddScoped<OrderHandler>();
app.Services.MapCommand<PlaceOrder>(
    (PlaceOrder command, OrderHandler handler, CancellationToken ct) => handler.Handle(command, ct));
```

Map handlers on the built host or its `IServiceProvider` before starting it. Delegate handlers return `void`, `Task`, or `ValueTask`; `async void` handlers are rejected.

## Retries and transactions

Retries are off by default. `WithRetries(attempts)` or `WithRetryPolicy(...)` persists a retry copy in the configured deferral store before acknowledging the failed delivery. Configure a durable deferral store such as Redis or SQL Server for production retries; the in-memory store is process-local.

`WithTransaction()` wraps handler execution in the registered `IMessageTransaction`. With SQL Server persistence, the endpoint transaction writes the application changes, inbox receipt, and staged outbound messages together. The SQL integration requires the application's `DbContext` to include `modelBuilder.AddTalaria()` and migrations to be applied normally. When application code sends messages through an injected `IMessageBus` outside a transactional endpoint handler, that bus stages messages on the scoped outbox; call `SaveChangesAsync` on the application `DbContext` to commit those staged sends. A message is not dispatched merely because `SendAsync` returned.

## Roadmap boundaries

Scheduled delivery and timeout APIs, richer compensation workflows, and generated Native AOT bindings remain future work. Request/reply is already available through `IRequestClient<TRequest>` and `MapRequest<TRequest,TResponse>`; it is not a planned feature gap.

## Runtime readiness and failed-message operations

`TalariaHealth` reports listener state and endpoint readiness; expose it through the application's health endpoint and use `WaitUntilReadyAsync` when startup depends on consumers being ready. The listener drains active handlers for the configured shutdown window before cancellation. Failed-message listing and replay are available through `IFailedMessages` for an application-owned operator endpoint. Protect that endpoint with the application's authentication and authorization; Talaria does not create an unauthenticated web route.

Resolve `TalariaHealth` from DI, or use `TalariaListener.Health` for a manually hosted listener. Built-in transports report readiness after subscribing; custom transports can implement `IConsumerReadiness`. For custom consumers without that capability, readiness means that the consumer was created and enumeration started. `ShutdownDrainTimeout` defaults to 30 seconds. When it expires, Talaria cancels handlers and allows one further bounded wait for cooperative cleanup; unfinished deliveries remain eligible for redelivery.

`IFailedMessages.ListAsync` returns retained handler failures and malformed stored JSON. `ReplayAsync(id)` sends a new delivery to the original endpoint; `ReplayAsync(id, correctedPayloadJson)` allows an operator to correct invalid JSON first. A confirmed publish precedes deletion of the retained failure, so interrupted replay can produce duplicates. Failed entries have no automatic expiry. Broker deserialization failures remain in the transport's native DLQ. Failure retention is included in `UseInMemory()`, `UseRedisPersistence(...)`, and `UseSqlServerPersistence<TDb>()`.

## RabbitMQ and SQL Server

Use the same mappings while changing the provider setup:

```csharp
builder.Services.AddDbContext<OrdersDb>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("orders")));
builder.Services.AddTalaria(t => t
    .UseRabbitMq(builder.Configuration.GetConnectionString("rabbitmq")!)
    .UseSqlServerPersistence<OrdersDb>());

// In OrdersDb.OnModelCreating:
modelBuilder.AddTalaria();

// After building the host:
app.MapCommand<PlaceOrder>(async (PlaceOrder command, OrdersDb db, IMessageBus bus, CancellationToken ct) =>
{
    db.Orders.Add(new Order { Id = command.OrderId });
    await bus.PublishAsync(new OrderPlaced(command.OrderId), ct);
}).WithTransaction().WithRetries(3);
```

Apply the application's EF migrations before starting Talaria. `WithTransaction()` commits the shared scoped context automatically on success; external HTTP calls and other side effects are outside that database transaction and must tolerate retries. Use one persistence bundle per host. The SQL bundle includes saga state, receive receipts, outbox, durable retry storage, and failure retention.

Mapped broker entities are provisioned at startup by default. With externally managed topology, set `TalariaOptions.AutoProvisionTopology = false` and create the entities before running the host. `TalariaListener.GetTopology()` exports the required queues, topics, and subscriptions, including sagas. `TopicRegistry.GetTopology(options)` exports just the stateless and request mappings. Start receivers before sending to a destination whose broker entities have not yet been created.

Request/reply factories use a dedicated reply address per factory. With automatic topology disabled, provision `RequestClientFactory.InboxTopic` as a queue before issuing requests. Retire old reply queues through the broker's operational tooling when their factory is no longer used.
