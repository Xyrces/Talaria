# Messaging roadmap implementation

This tracks the accepted roadmap. Checked items are implemented and verified; unchecked items remain release work.

- [x] Delivery safety: distinguish busy/completed inbox entries, fence leases, prevent offset gaps, preserve failed relay entries.
- [x] Versioned saga snapshots and atomic state/inbox/outbox transitions.
- [x] Minimal command/event mappings with scoped delegate binding and a message bus.
- [x] Convention-based contracts, endpoint identity, targeted retries, and topology validation.
- [x] Complete local and Redis persistence bundles.
- [x] RabbitMQ transport with confirmed delivery and automatic topology.
- [x] SQL Server/EF Core application inbox, outbox, delayed retries, and saga persistence.
- [x] Azure Service Bus topology, lifecycle, authentication, and settlement fixes.
- [x] Readiness, failure inspection/replay, graceful shutdown, and public test harness.
- [x] Mandatory provider integration matrix, migration guide, and executable quickstarts.

Request/reply remains supported through the existing typed request clients. The new minimal command/event API is additive.

Subsequent releases: a public scheduled-send API and saga timeouts, compensation, and Native AOT generation. Persistence in this release supports delayed retries and saga deferrals; it does not expose a general scheduling API.

## Reproducing validation

Run `dotnet build Talaria.slnx -c Release`, then `dotnet test Talaria.slnx -c Release --no-build -m:1 -p:TestTfmsInParallel=false` with `TALARIA_REQUIRE_DOCKER=1`. This requires Docker, starts the provider fixtures, and treats unavailable infrastructure as a failure. The Service Bus emulator needs host port 5672 free. Running projects sequentially bounds container memory use.

On Windows, set `Logging__EventLog__LogLevel__Default=None` when running the tests without permission to create EventLog sources. Run `dotnet run --project samples/MinimalMessaging -c Release` for the quickstart and `dotnet list Talaria.slnx package --vulnerable --include-transitive` for the dependency audit.
