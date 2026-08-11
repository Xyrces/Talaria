# task-11 Audit: Placeholders & Config-Driven Values

This document enumerates every site reviewed for "personal/local"
references during the task-11 sweep, the disposition for each, and the
rationale. Sprint scope per the brief:

1. **Scrub personal/local references** - remove all references to the
   original developer's local machine or personal accounts.
2. **Generalize placeholders** - replace hardcoded local values
   (URLs, hostnames, sample keys, fake user/company names, "TODO: real
   value here" comments, etc.) with generic, documented placeholders or
   proper config-driven values.
3. **Out of scope:** GitHub org/account references are explicitly
   excluded.

> **Note on prior work.** The task description references "the audit" as
> input from task-10. Task-10's audit output was not committed to any
> branch in this worktree (no `docs/TASK_10_AUDIT.md`, no AUDIT file in
> `agent/task-10`, no commit referencing "audit"). task-11 therefore
> re-ran the sweep from scratch against the current state of the repo
> and recorded the findings here.

---

## Disposition summary

| Category | Action | Sites |
| --- | --- | --- |
| Personal/local references | None found | 0 |
| Hardcoded sample values that look local | Replaced with `IConfiguration`-driven values | 2 files, 6 sites |
| Generic example-doc strings ("localhost:9092" as doc examples) | Kept | 6 sites |
| RFC 2606 `example.com` (already canonical placeholder) | Kept | 2 sites |
| Generic test-fixture prefixes | Kept | 5 sites |
| GitHub-org references (`Xyrces/Talaria`, `xyrces.io`) | Kept - out of scope | 9 sites |
| .NET Aspire-generated dev-profile localhost URLs | Kept | 8 sites |
| `UserSecretsId` GUID | Kept - see "machine correlation" note | 1 site |

---

## Detailed findings

### 1. Hardcoded sample values made config-driven

These are the only substantive code changes in this PR.

#### 1a. `src/Talaria.Client.Api/Program.cs`

| Site (line, pre-change) | Disposition |
| --- | --- |
| L33 `opts.BootstrapServers = builder.Configuration.GetConnectionString("kafka") ?? "localhost:9092";` | **Kept.** This is the documented local-dev default for Kafka. README states "Without it the AppHost would not start". Replacing with `example.com` would break every Aspire AppHost run. Already configurable via `ConnectionStrings:kafka` (env var, appsettings.json, Aspire service discovery). |
| L37, L42, L47 `opts.Configuration = builder.Configuration.GetConnectionString("redis") ?? "localhost:6379";` | **Kept.** Same rationale - documented local-dev default for Redis. |
| L38, L43, L48 `opts.KeyPrefix = "onboarding:";` (3x) | **Changed.** Replaced with `opts.KeyPrefix = redisKeyPrefix;` where `redisKeyPrefix = builder.Configuration["Talaria:Redis:KeyPrefix"] ?? "onboarding:";`. The literal `onboarding:` is preserved as the documented default for the onboarding sample; the config key lets consumers override per environment. |
| L58 `app.Services.MapTopic<SendVerificationEmailCommand>("email-commands", ...)` | **Changed.** Now `app.Services.MapTopic<SendVerificationEmailCommand>(emailCommandsTopic, ...)` where `emailCommandsTopic = builder.Configuration["Talaria:Topics:EmailCommands"] ?? "email-commands";`. |
| L90 `var producer = await transport.CreateProducerAsync<CreateAccountCommand>("onboarding-commands", ...)` | **Changed.** Now uses `onboardingCommandsTopic` config-derived local. |
| L106 `var producer = await transport.CreateProducerAsync<AccountVerifiedEvent>("account-events", ...)` | **Changed.** Now uses `accountEventsTopic` config-derived local. |

#### 1b. `src/Talaria.Client.Api/Sagas/OnboardingSaga.cs`

| Site (line, pre-change) | Disposition |
| --- | --- |
| L45 `sagas.StartedBy<CreateAccountCommand>("onboarding-commands", ...)` | **Changed.** Now reads `topics.OnboardingCommands` from a new `OnboardingSagaTopics` record passed into `ConfigureOnboardingSaga(IServiceProvider, OnboardingSagaTopics)`. |
| L70 `sagas.On<AccountVerifiedEvent>("account-events", ...)` | **Changed.** Now reads `topics.AccountEvents`. |
| L82 `sagas.DispatchTo<SendVerificationEmailCommand>("email-commands");` | **Changed.** Now reads `topics.EmailCommands`. |

The `OnboardingSagaTopics` record added in this file carries:

- A `(string OnboardingCommands, string AccountEvents, string EmailCommands)` shape with explicit XML doc naming the three configuration keys that override the defaults.
- `FromConfiguration(IConfiguration)` static factory that resolves each key with the same `?? "default"` fallback as `Program.cs`, keeping the literal defaults in a single place.
- A backwards-compatible zero-arg `ConfigureOnboardingSaga(IServiceProvider)` overload that delegates to the topics overload with `OnboardingSagaTopics.Defaults`, so any future callers (tests, sample apps) keep working without changes.

The integration test (`tests/Talaria.AppHost.Tests/IntegrationTest1.cs:91`) still reads the saga state from Redis under the `"onboarding:onboardingstate:{id}"` key, which is what the saga produces when the default prefix is in effect. With config-driven defaults, the test stays valid as long as no override is supplied - which matches its current behavior. No test changes required.

### 2. Generic example-doc strings (kept)

These are intentional documentation examples, not personal values.

| File | Line | String | Rationale |
| --- | --- | --- | --- |
| `src/Talaria.Transports.Kafka/KafkaTransportOptions.cs` | 14 | `e.g., "localhost:9092"` | Doc-comment example showing the connection-string format. |
| `src/Talaria.Transports.Kafka/KafkaTransport.cs` | 60 | `e.g. "localhost:9092"` | ArgumentException message in the required-config check. |
| `src/Talaria.Transports.Kafka/KafkaTransport.cs` | 75 | `"...non-localhost brokers..."` | Log warning text - informative, not personal. |
| `src/Talaria.Transports.Kafka/KafkaTransport.cs` | 86 | `name is "localhost" or "127.0.0.1" or "::1"` | `IsLocalhostOnly()` security heuristic. |
| `src/Talaria.StateStores.Redis/RedisStateStoreExtensions.cs` | 88 | `e.g. "localhost:6379"` | ArgumentException message in the required-config check. |
| `src/Talaria.StateStores.Redis/TalariaRedisOptions.cs` | 14 | `e.g. "host:6379,ssl=true,password=..."` | Doc-comment example showing TLS/auth syntax. |
| `README.md` | 62, 67 | `"host:6379,ssl=true,password=..."` | Doc-example for production Redis connection string. |

### 3. RFC 2606 `example.com` placeholders (kept)

Already canonical placeholders per RFC 2606 section 2.

| File | Line | String |
| --- | --- | --- |
| `tests/Talaria.AppHost.Tests/IntegrationTest1.cs` | 38 | `"test@example.com"` |
| `tests/Talaria.AppHost.Tests/IntegrationTest1.cs` | 62 | `"duplicate-test@example.com"` |

### 4. Generic test-fixture prefixes (kept)

These are intentional test-isolation values; the strings describe the
test purpose, not a developer's environment.

| File | Line | Value | Purpose |
| --- | --- | --- | --- |
| `src/Talaria.StateStores.Redis/TalariaRedisOptions.cs` | 22 | `"talaria:"` (default `KeyPrefix` for the library itself) | Library default - already overridable. |
| `src/Talaria.Transports.InMemory/InMemoryProducer.cs` | 36 | `"talaria"` (`messaging.system` OTel tag) | Library identifier. |
| `src/Talaria.Transports.InMemory/InMemoryTransactionalSession.cs` | 126 | `"talaria"` (`messaging.system` OTel tag) | Library identifier. |
| `src/Talaria.Core/Diagnostics/TalariaDiagnostics.cs` | 141 | `"talaria"` (`messaging.system` OTel tag) | Library identifier. |
| `src/Talaria.Core/TalariaOptions.cs` | 57 | `"talaria"` (`ApplicationName` default) | Library identifier - overridable. |
| `src/Talaria.Core/Hosting/TalariaHostedService.cs` | 112 | `"talaria"` (`messaging.system` OTel tag) | Library identifier. |
| `tests/Talaria.StateStores.Redis.Tests/RedisStateStoreIntegrationTests.cs` | 28 | `"test-store:"` | Per-test isolation namespace. |
| `tests/Talaria.StateStores.Redis.Tests/RedisConcurrencyIntegrationTests.cs` | 30 | `"test-concurrency-{Guid}:"` | Per-test isolation namespace. |
| `tests/Talaria.StateStores.Redis.Tests/RedisOutboxIntegrationTests.cs` | 37 | `"test-outbox-{Guid}:"` | Per-test isolation namespace. |
| `tests/Talaria.StateStores.Redis.Tests/RedisConcurrencyIntegrationTests.cs` | 26 | `"test-app-{Guid}"` | Per-test isolation app name. |
| `tests/Talaria.StateStores.Redis.Tests/RedisOutboxIntegrationTests.cs` | 31 | `"test-app-{Guid}"` | Per-test isolation app name. |
| `tests/Talaria.Specs/SagaEngineBehaviorTests.cs` | 55, 139, 200, 266, 325, 389 | `"test-app"` (6x) | Test ApplicationName. |

### 5. GitHub org / account references - OUT OF SCOPE

The sprint brief explicitly excludes GitHub org/account references. These
stay.

| File | Lines | Reference |
| --- | --- | --- |
| `CHANGELOG.md` | 136-140 | `https://github.com/Xyrces/Talaria/compare/...` (release links) |
| `CODE_OF_CONDUCT.md` | 64 | `talaria-conduct@xyrces.io` (enforcement contact) |
| `SECURITY.md` | 39 | `https://github.com/Xyrces/Talaria/security/advisories/new` |
| `SECURITY.md` | 42 | `security@xyrces.io` (private disclosure channel) |
| `SECURITY.md` | 155 | `security@xyrces.io` (commercial-licensing contact) |
| `.github/workflows/ci.yml` | 73 | `https://nuget.pkg.github.com/${{ github.repository_owner }}/index.json` |
| `.github/workflows/ci.yml` | 96 | `https://nuget.pkg.github.com/${{ github.repository_owner }}/index.json` |

### 6. .NET Aspire-generated dev-profile URLs (kept)

These files are auto-generated by `dotnet new` / `aspire init`. The
localhost ports are dev-environment conventions.

| File | Lines | Purpose |
| --- | --- | --- |
| `src/Talaria.AppHost/Properties/launchSettings.json` | 8, 12, 13, 20, 24, 25 | AppHost dev-profile URLs (HTTPS/OTLP endpoints). |
| `src/Talaria.Client.Api/Properties/launchSettings.json` | 7, 17, 27 | Sample-API dev-profile URLs. |
| `src/Talaria.Client.Api/Talaria.Client.Api.http` | 1 | `.http` file base URL for VS / Rider REST client. |

### 7. `UserSecretsId` GUID (kept)

`src/Talaria.AppHost/Talaria.AppHost.csproj:10`:

```xml
<UserSecretsId>fe066601-78f3-4dd6-8d07-842e5ade2c3a</UserSecretsId>
```

This GUID was generated by Visual Studio's per-developer-machine
"Manage User Secrets" flow and was committed to the repo in the initial
commit (c478d6d). It is not personally identifying - it is a random
GUID with no name/email mapping. Rotating it would orphan any
`~/.microsoft/usersecrets/fe066601-.../secrets.json` file a maintainer
already has on their machine, and `dotnet user-secrets` rejects the
all-zero GUID. The tradeoff is to leave it; documented here so any
maintainer regenerating it does so knowingly.

---

## Verification

After the code change:

- `dotnet build Talaria.slnx --configuration Release --no-restore` - 0 warnings, 0 errors.
- `dotnet test tests/Talaria.Core.Tests/Talaria.Core.Tests.csproj --configuration Release --no-build --nologo` - 14/14 passing.
- `dotnet test tests/Talaria.InMemory.Tests/Talaria.InMemory.Tests.csproj --configuration Release --no-build --nologo` - 48/48 passing.
- `dotnet test tests/Talaria.Specs/Talaria.Specs.csproj --configuration Release --no-build --nologo` - 49/49 passing.
- `dotnet format src/Talaria.Client.Api/ --verify-no-changes --no-restore` - exit 0.
- `dotnet format Talaria.slnx --verify-no-changes --no-restore` - pre-existing whitespace warnings in files outside this PR's scope (97 reports; the same set reproduces on a clean worktree without the task-11 changes, so they are pre-existing).

Docker-gated suites (Kafka/Redis/AppHost) skip without Docker and are
not part of the local baseline.

---

## Out-of-scope confirmations

- No `~/.local`, `~/Users/jane`, `C:\Users\...`, `/private/var`, or other
  OS-specific path was found.
- No personal email (`jay@...`, `jtn@...`, `*@gmail.com`, `*@yahoo.com`, ...)
  was found in source, configs, or comments.
- No `192.168.*`, `10.*`, `172.16-31.*`, `127.0.0.1`, or `::1` address
  appeared outside the dev-profile `launchSettings.json` files and the
  `localhost`-doc-example strings already enumerated above.
- No sample user names like `John Doe`, `Jane Smith`, `MyCompany`,
  `MyOrg`, `YourCompany`, `your-org`, `yourname`, `username`, `admin`,
  `demo`, `guest`, `changeme`, or `password` literals were found.
- No `TODO: real value here`, `REPLACE ME`, `CHANGE ME`, `PLACEHOLDER`,
  `FIXME`, `XXX`, or `HACK` comments were found.

The repo was already clean of personal references - what this PR
generalizes is the sample-API saga wiring, where the saga-specific topic
names and Redis key prefix were hardcoded in three places.
