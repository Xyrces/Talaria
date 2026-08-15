# License Rationale: Apache-2.0

Talaria is released under the **Apache License, Version 2.0**
([`LICENSE`](../LICENSE)). Every hand-written `.cs` file carries the
header `// SPDX-License-Identifier: Apache-2.0` so the license travels
with the code.

## Why Apache-2.0?

The project was originally released under a copyleft network license to
combine OSI/FSF recognition with a strong copyleft network clause and a
commercial carve-out. After shipping that way, two things became clear:

1. **The primary audience is coming from permissive ecosystems.**
   Talaria targets teams that are evaluating or migrating away from
   MassTransit and similar .NET messaging libraries. Those teams build
   inside permissive-licensed stacks and treat copyleft — especially a
   network-copyleft license — as a procurement blocker, even when the
   library is only embedded as infrastructure.

2. **Adoption is the immediate goal; monetization can follow elsewhere.**
   A permissive license maximizes the number of teams that can adopt
   Talaria, run it in SaaS products, embed it in proprietary
   applications, and contribute fixes and integrations back without
   legal review cycles. Commercial value is deferred to adjacent
   offerings — support, hosted deployments, and managed services — that
   live in their own repositories and are licensed separately.

## What changed

- The [`LICENSE`](../LICENSE) file now contains the canonical Apache-2.0
text.
- `Directory.Build.props` sets `<PackageLicenseExpression>Apache-2.0</PackageLicenseExpression>`.
- Every `.cs` SPDX header was changed to `Apache-2.0`.
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) now states that contributions
  are accepted under Apache-2.0.

## What did not change

- The project is still open source and OSI-approved.
- There is still no CLA or copyright assignment required for
  contributions.
- A separate commercial offering may still be developed and licensed
  independently; this repository's Apache-2.0 grant is not affected by
  that.

## Historical note

Earlier project planning documents (task-11, task-13, task-14, and
task-16) discuss the prior copyleft network license choice as historical
record. They were accurate at the time they were written and are left
intact for audit purposes; the current license posture is Apache-2.0.

---

*Last reviewed: 2026-08-15; relicensed to Apache-2.0.*
