# Contributing to Talaria Saga Engine

Thanks for your interest in contributing to Talaria. This guide explains how
to set up a development environment, run the build and tests, follow the
project's coding conventions, and submit a pull request.

Talaria is an open-source library distributed under the
**Apache License, Version 2.0 (Apache-2.0)** (see [`LICENSE`](LICENSE)). By
contributing, you agree that your contributions will be released under the
same license. The rationale behind this choice and the commercial-licensing
posture are documented in [`docs/LICENSE-RATIONALE.md`](docs/LICENSE-RATIONALE.md);
the commercial offering itself is delivered under a separate repository and
separate terms and is intentionally out of scope for this repo.

---

## Table of contents

- [Code of conduct](#code-of-conduct)
- [Reporting a bug or requesting a feature](#reporting-a-bug-or-requesting-a-feature)
- [Security issues](#security-issues)
- [Development environment](#development-environment)
- [Building](#building)
- [Running tests](#running-tests)
- [Coding conventions](#coding-conventions)
- [Public API surface](#public-api-surface)
- [Pull request process](#pull-request-process)

---

## Code of conduct

All participants in this project — maintainers, contributors, and users — are
expected to follow the [Contributor Covenant](CODE_OF_CONDUCT.md). Be
respectful, assume good faith, and help us keep the community welcoming.

---

## Reporting a bug or requesting a feature

Please use the GitHub issue templates:

- **Bug report** — `.github/ISSUE_TEMPLATE/bug_report.md`
- **Feature request** — `.github/ISSUE_TEMPLATE/feature_request.md`

Before opening a new issue, search the existing issue tracker to avoid
duplicates. Include enough detail (versions, configuration, repro steps,
expected vs. actual behavior) for someone unfamiliar with your setup to
understand and reproduce the problem.

---

## Security issues

**Do not file public issues for security vulnerabilities.** Follow the
disclosure process in [`SECURITY.md`](SECURITY.md) instead — it lists the
private reporting channel and the response SLA.

---

## Development environment

| Tool              | Version                                                  |
| ----------------- | -------------------------------------------------------- |
| .NET SDK          | 8.0.x, 9.0.x, and 10.0.x                                 |
| Docker / OrbStack | Required for the AppHost, Kafka, and Redis suites        |
| Git               | Recent (uses MinVer for versioning)                      |

Talaria targets `net8.0`, `net9.0`, and `net10.0`. The solution
([`Talaria.slnx`](Talaria.slnx)) multi-targets across all three — make sure
all three SDKs are installed before opening the solution in your IDE.

The repository uses **MinVer** to derive the assembly version from Git tags.
A clean working tree off `main` will report a pre-release version. Add an
annotated tag (`git tag v0.1.0`) to produce a stable version.

---

## Building

From the repository root:

```bash
# Restore dependencies
dotnet restore Talaria.slnx

# Build all projects in Release
dotnet build Talaria.slnx --configuration Release --no-restore
```

A clean build should report **0 warnings and 0 errors**. Warnings are not
allowed to accumulate — fix them before opening a PR.

The AppHost project additionally depends on **.NET Aspire** (see
`src/Talaria.AppHost/`). It spins up Kafka, Redis, Prometheus, Tempo,
Grafana, and three API replicas when run.

---

## Running tests

The full suite is Docker-backed — start Docker (or OrbStack) first. Then:

```bash
# Run everything in Release (the CI equivalent)
dotnet test Talaria.slnx --configuration Release --no-build
```

Targeted runs are useful while iterating:

```bash
# Core library unit tests only (no Docker required)
dotnet test tests/Talaria.Core.Tests/Talaria.Core.Tests.csproj --configuration Release

# In-memory transport + state store (no Docker required)
dotnet test tests/Talaria.InMemory.Tests/Talaria.InMemory.Tests.csproj --configuration Release

# Behavior-driven specs (no Docker required)
dotnet test tests/Talaria.Specs/Talaria.Specs.csproj --configuration Release

# Kafka + Redis + AppHost tests (Docker required)
dotnet test tests/Talaria.Transports.Kafka.Tests/Talaria.Transports.Kafka.Tests.csproj --configuration Release
dotnet test tests/Talaria.StateStores.Redis.Tests/Talaria.StateStores.Redis.Tests.csproj --configuration Release
dotnet test tests/Talaria.AppHost.Tests/Talaria.AppHost.Tests.csproj --configuration Release
```

A passing local run should match the CI baseline:

- ~110 tests pass with Docker available.
- ~14 tests are gated behind `[DockerFact]` and skip without Docker
  (covering Redis, Kafka, and AppHost multi-container paths).
- 0 failures, 0 unexpected skips.

Tests under `tests/Talaria.Specs/` use xUnit and SpecFlow-style
Given/When/Then naming. Add new specs there for end-to-end behaviour; add
focused unit tests alongside the production code under `tests/Talaria.*Tests/`.

---

## Coding conventions

- **Language version**: C# 12 / latest stable; nullable reference types are
  enabled project-wide (`<Nullable>enable</Nullable>`).
- **Style**: match the surrounding code. Run `dotnet format Talaria.slnx`
  before committing — CI runs `dotnet format --verify-no-changes` as a gate.
- **XML doc comments**: required on all `public` and `protected` members of
  `Talaria.Core` and the transport / state-store libraries. The project
  ships the XML doc file in its NuGet package. CS1591 is suppressed, but
  adding the comments is still expected for any user-visible API.
- **SPDX headers**: every hand-written `.cs` file under `src/` must start
  with `// SPDX-License-Identifier: Apache-2.0`. Generated files in
  `obj/` and `bin/` are excluded.
- **Allocation discipline**: the saga pipeline is hot-path. Avoid `new` in
  message dispatch, the relay loop, and the deferred-sweeper tick. Reuse
  `ArrayPool`, `Span<T>`, and `ValueTask` where appropriate. The acceptance
  criterion is "no GC pressure regressions in the existing benchmarks" —
  if you change the hot path, include a before/after measurement.
- **Concurrency**: saga state, the outbox relay, and the deferral sweeper
  are designed for concurrent access. Use the existing lease / fencing-token
  helpers (see `IOutboxStore`, `IDeferralStore`) rather than introducing new
  locking primitives.
- **Observability**: new features ship with OpenTelemetry spans and the
  `talaria.*` metrics conventions used elsewhere in the codebase.

---

## Public API surface

Talaria's public surface is small and deliberate:

- `ITransport`, `IStateStore`, `IIdempotencyStore`, `IDeferralStore`,
  `IOutboxStore`
- `MapSaga<TState>`, `DispatchTo`, the `TalariaBuilder` registration
  extensions, and the `TalariaOptions` configuration record.

If your change adds a new public type or method, you are responsible for:

1. XML doc comments (summary, parameter notes, `remarks` where helpful,
   `since` tag noting the version).
2. A SpecFlow spec under `tests/Talaria.Specs/` that demonstrates the
   intended behavior end-to-end.
3. An entry in the migration notes (see "Pull request process" below).

**Breaking changes** to existing signatures, semantics, or wire formats are
disallowed on a `0.x` library without a major-version discussion in an issue.
Additive-only changes (new overloads, new optional parameters with safe
defaults, default interface members) are preferred.

---

## Pull request process

1. **Open or pick an issue first.** For non-trivial changes, describe the
   problem and proposed approach in an issue before writing code. This lets
   maintainers and the community weigh in early.
2. **Branch off `main`.** Use a descriptive branch name
   (`fix/kafka-rebalance-recovery`, `feat/outbox-metrics`, etc.) — but note
   that automation-driven task branches live under `agent/task-<id>` and are
   not opened by humans directly.
3. **Keep commits focused.** One logical change per commit. Use
   [Conventional Commits](https://www.conventionalcommits.org/) style
   prefixes (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`).
4. **Run the local gates before pushing.**
   ```bash
   dotnet build Talaria.slnx --configuration Release --no-restore
   dotnet test Talaria.slnx --configuration Release --no-build
   dotnet list package --vulnerable --include-transitive
   ```
5. **Fill out the PR template** (`.github/PULL_REQUEST_TEMPLATE.md`).
   Link the issue, describe the change, list any public-API additions,
   and check off the verification boxes.
6. **CI must be green** before a review begins. The workflow at
   `.github/workflows/ci.yml` runs restore, build, the full test suite,
   and a NuGet vulnerability audit on every push and PR.
7. **Review turnaround.** A maintainer will either approve, request
   changes, or leave non-blocking comments. Force-pushes are fine; please
   mark conversations as resolved after addressing them.
8. **Merge.** The maintainer merges once approved and CI is green — do
   not merge your own PR.

---

## Commit message examples

```
feat: add OutboxLag histogram to talaria.outbox metrics

Adds a `talaria.outbox.lag` histogram that observes the age (in
milliseconds) of the oldest leased outbox entry. Updated the specs
and README to document the new metric.

Closes #142
```

```
fix: prevent double-publish when outbox relay crashes between lease and commit

Acquire a fencing token before publish; reject the publish if the
token no longer matches the row's current lease owner. Adds a
regression spec and a unit test for the failure path.
```

```
docs: clarify that outbox relay polls every 250ms by default

TalariaOptions.OutboxRelayInterval was undocumented. Add XML
remarks, a README section, and a SpecFlow feature.
```

---

## Questions?

- Open a discussion on the issue tracker.
- For security-sensitive questions, follow [`SECURITY.md`](SECURITY.md).
- For commercial licensing or support, see [`docs/LICENSE-RATIONALE.md`](docs/LICENSE-RATIONALE.md)
  and the **License** section of the [README](README.md#-license).
