# License Rationale: AGPL-3.0-or-later

This document records the rationale for choosing the **GNU Affero General
Public License, version 3 or later** (AGPL-3.0-or-later) for Talaria,
and why the obvious commercial-source alternatives — the Business Source
License (BUSL-1.1), the Server Side Public License (SSPL-1.0), and
permissive licensing (Apache-2.0 / MIT) paired with a separate commercial
end-user licence agreement (EULA) — were rejected.

The decision is documented separately from the LICENSE file itself so
that downstream contributors, downstream users, and enterprise
procurement reviewers can see *why* this license was chosen without
having to reconstruct the reasoning from scattered commits. The choice
is meant to be the long-term posture of the project; see
[Reversibility](#reversibility) below for what would be required to
change it.

---

## 1. Chosen license: AGPL-3.0-or-later

Talaria is released under the **AGPL-3.0-or-later** ([`LICENSE`](../LICENSE)
at the repo root). The header convention (`// SPDX-License-Identifier:
AGPL-3.0-or-later`) is applied to every hand-written `.cs` file under
[`src/`](../src/) so the license travels with each file as well.

### Why AGPL satisfies our two requirements

The project brief asked for a license that simultaneously:

1. Satisfies the **Open Source Initiative (OSI)** and **Free Software
   Foundation (FSF)** open-source definitions, encouraging adoption
   and contribution; and
2. Preserves an **exclusive commercial path** for support, hosting,
   managed services, and proprietary relicensing.

AGPL-3.0-or-later does both:

- **OSI/FSF recognition.** AGPL-3.0 is on the FSF's list of free
  software licenses and is an OSI-approved license. There is no
  ambiguity about whether the license qualifies as open source; it
  does, unambiguously. This matters for downstream adopters with
  enterprise procurement policies that gate on OSI/FSF recognition
  (Debian, Fedora, several large enterprise IT policies, many US/EU
  public-sector procurement processes).
- **Copyleft + network clause.** AGPL-3.0 is a strong copyleft license
  that adds a network-service clause: anyone who runs a *modified*
  Talaria as a network service must publish the complete corresponding
  source under the same license. This closes the "SaaS loophole" that
  permissive licenses (MIT, Apache-2.0) leave wide open. Downstream
  contributors and end users are protected from a competitor offering
  a closed-source, hosted Talaria clone without contributing back.
- **Contribution terms are the same as usage terms.** Because the
  project license is AGPL and the contribution policy (see
  [`CONTRIBUTING.md`](../CONTRIBUTING.md)) says contributions are
  accepted under the same license, every contributor's contribution
  is automatically offered under AGPL-3.0-or-later. There is no CLA
  re-grant step or copyright-assignment dance to track.
- **Commercial carve-out preserved.** The maintainer retains the
  exclusive right to grant proprietary relicensing, commercial
  support, hosted/managed deployments, and SLAs through a separate
  commercial offering delivered under its own repository and license
  terms (see [Commercial offering boundary](#commercial-offering-boundary)
  below). AGPL explicitly permits this: it does not restrict the
  licensor from granting additional permissions to specific
  recipients.

---

## 2. Why we did not choose the alternatives

The brief listed three non-AGPL candidates. Each was considered in
detail and rejected for the reasons below.

### 2.1 Rejected: Business Source License 1.1 (BUSL-1.1)

**What it is.** BUSL-1.1 is a "source-available" license published by
MariaDB in 2013 and adopted by HashiCorp, Sentry, CockroachDB, and
others. It allows non-production use freely; production use requires
a commercial license from the licensor. After a "change license"
period (typically three years) each release automatically converts to
an OSI-approved license (usually Apache-2.0).

**What it would buy us.** The same commercial exclusivity the brief
asks for, via the production-use restriction. The eventual
conversion to Apache-2.0 also gives adopters a clear "this will
become open source on a known date" signal that some enterprise
buyers find reassuring.

**Why we rejected it: the OSI-uncertainty tradeoff.**

- **OSI / FSF status is contested.** BUSL-1.1 is *not* an
  OSI-approved license and is *not* on the FSF's list of free
  software licenses. OSI's published guidance treats BUSL-style
  source-available licenses as a distinct category from open source
  because they restrict the field of use ("production"). Several
  downstream policies explicitly exclude BUSL-licensed code from
  "open source" procurement categories.
- **Corporate-procurement adoption friction.** Enterprise IT and
  some public-sector procurement policies gate on OSI/FSF
  recognition. Choosing BUSL would force those buyers to take a
  separate legal opinion before adopting Talaria, which materially
  slows adoption. Some large adopters (notably the German
  "Free Software" definition and the FSF's four-essential-freedoms
  test) treat BUSL as non-free outright.
- **Change-license drift.** Each release converts to a different
  license after a multi-year window. This means a long-lived
  enterprise deployment might run on a copy of the code whose
  license terms have shifted underneath it. For a library that
  prides itself on predictable licensing posture (see
  [`SECURITY.md`](../SECURITY.md) and the SPDX header convention),
  that drift is awkward to manage.
- **The commercial-path benefit is real but not unique.** BUSL's
  commercial carve-out is also achievable under AGPL + a separate
  commercial agreement; the AGPL path is unambiguous about OSI/FSF
  status in a way BUSL is not.

### 2.2 Rejected: Server Side Public License 1.0 (SSPL-1.0)

**What it is.** SSPL-1.0 is a copyleft license published by MongoDB
in 2018 as an attempt to close the SaaS loophole more aggressively
than AGPL does. SSPL §13 requires that anyone who offers the
licensed software as a service must release *all* software that
manages and provides that service under SSPL.

**What it would buy us.** The strongest possible commercial
exclusivity, because SSPL's stack-wide copyleft makes it
prohibitively expensive for a competitor to offer a hosted Talaria
service without open-sourcing their entire control plane.

**Why we rejected it: the aggression tradeoff.**

- **SSPL is not OSI-approved.** OSI's board issued a formal
  statement declining to recognize SSPL as open source, citing §13's
  service-level copyleft as a discriminatory restriction. SSPL is
  also not on the FSF's list of free software licenses. Adopting
  SSPL puts Talaria firmly outside the OSI/FSF open-source
  definition, which fails the first half of the brief.
- **Ecosystem-integration friction is severe.** SSPL §13 forces
  every adopter who offers Talaria-as-a-service to release their
  entire surrounding stack. This is widely read as hostile to
  integration: a company that drops Talaria into a larger product
  cannot ship a hosted version of that product without open-sourcing
  everything. That's a much harder sell than AGPL's "publish Talaria
  modifications only" model, and it materially shrinks the universe
  of plausible integrators.
- **Fork-and-relicense risk is higher, not lower.** Because SSPL is
  perceived as aggressive, large contributors may decline to
  contribute (avoiding SSPL-encumbered code in their own stack) and
  may fork under a friendlier license instead. That fragmentation
  risk outweighs the marginal commercial-exclusivity gain over
  AGPL.
- **Adoption patterns in the wild confirm the tradeoff.** Projects
  that adopted SSPL (MongoDB, Elastic) saw measurable friction with
  managed-service providers and cloud marketplaces, and have
  subsequently moved to a dual-license model (SSPL + a more
  conventional open-source license) to ease that friction. Starting
  at SSPL means we'd likely end up at "SSPL + something else"
  anyway.

### 2.3 Rejected: Permissive license (Apache-2.0 / MIT) + commercial EULA

**What it is.** Release Talaria under a permissive license (Apache-2.0
or MIT), then use a separate commercial end-user licence agreement
(EULA) to capture value from support, hosting, and managed services.

**What it would buy us.** Maximum adoption: permissive licenses are
the easiest to integrate into other codebases, have the lowest legal
friction for downstream users, and face no OSI/FSF questions.

**Why we rejected it: the enforceability tradeoff.**

- **Permissive licenses provide no structural barrier to a hosted
  competitor.** A competitor can take Talaria under Apache-2.0, build
  a hosted/managed offering on top of it, and contribute nothing
  back. This is by design for permissive licenses, but it leaves the
  maintainer without a structural lever — commercial exclusivity has
  to be defended contractually.
- **EULAs are expensive and slow to enforce.** A commercial EULA is
  a contract; it requires negotiating, signing, and then enforcing
  on a per-infringer basis. Each infringer becomes a separate
  dispute. Copyleft licenses (AGPL, BUSL, SSPL) give the maintainer
  a structural remedy built into the license itself; an EULA does
  not.
- **Adopters are split between two audiences and two contracts.**
  Apache-2.0 users follow Apache-2.0; commercial customers follow
  the EULA. Bug reports, CVE credits, and contribution flows are
  harder to keep coherent when the code base has two simultaneous
  legal regimes.
- **Most-adopted doesn't mean most-appropriate for this project.**
  Apache-2.0 / MIT are excellent choices for libraries whose primary
  value is in being embedded into other products (e.g., HTTP
  clients, JSON parsers, low-level utilities). Talaria's primary
  value is in the *hosted/managed deployment* story — exactly the
  scenario permissive licenses do not protect.

---

## 3. Comparison matrix

| Dimension                                  | AGPL-3.0-or-later | BUSL-1.1         | SSPL-1.0          | Apache-2.0 + EULA         |
| ------------------------------------------ | ----------------- | ---------------- | ----------------- | ------------------------- |
| OSI-approved                               | ✅                | ❌               | ❌                | ✅                        |
| FSF-recognized                             | ✅                | ❌               | ❌                | ✅                        |
| Copyleft                                   | Strong + network  | Time-delayed     | Stack-wide        | None (permissive)         |
| Network-service clause                     | ✅                | Implicit         | ✅ (whole stack)  | ❌                        |
| Adoption friction                          | Low               | Medium-High      | High              | Lowest                    |
| Procurement friendliness (Debian, Fedora)  | ✅                | ❌               | ❌                | ✅                        |
| Commercial exclusivity preserved           | ✅ (relicense)    | ✅ (prod-only)   | ✅ (most strict)  | ✅ (EULA only)            |
| Commercial exclusivity enforceability       | Structural        | Structural       | Structural        | Per-infringer (contractual) |
| Integration with downstream stacks         | Easy              | Easy             | Hard (hostile §13)| Easiest                   |
| Contributor license clarity                | ✅ (same as use)  | ✅ (same as use) | ✅ (same as use)  | ⚠️ (depends on EULA)     |
| Long-term posture stability                | High              | Drifts (→ Apache)| Drifts (rare)     | Stable, but unprotected   |
| Suitable for a hosted-services product     | ✅                | ✅               | ✅ (overkill)     | ❌ (no structural lever) |

AGPL-3.0-or-later is the only option that scores well on OSI/FSF
recognition, copyleft (network clause), commercial carve-out,
adoption friction, and contributor clarity simultaneously.

---

## 4. Commercial offering boundary

To be unambiguous: the **commercial offering is delivered under a
separate repository and separate license terms**. This repository
remains pure AGPL-3.0-or-later, with the SPDX header convention
already in place across [`src/`](../src/).

What the commercial offering covers (per the project's README §License
and SECURITY §Commercial channels):

- **Commercial support contracts** with SLAs, including security-fix
  backports to older supported versions.
- **Hosted or managed Talaria deployments** with hardening,
  monitoring, and incident response.
- **Proprietary relicensing** that lets a customer ship modifications
  without the AGPL network-source obligation.

What the commercial offering does **not** do:

- It does not change the license of this repository.
- It does not retroactively relicense prior versions of Talaria.
- It does not require any CLA, copyright assignment, or upstream
  coordination from contributors or non-commercial users.

This boundary is restated here so that the AGPL choice is not read as
hostile to commercial adoption — it is the opposite: AGPL + a
commercial carve-out is the standard pattern for projects that want
both OSI recognition and a sustainable commercialization path.

---

## 5. Adoption signals we look for post-release

The choice of license is a bet on adoption signals. We will watch
for, and adapt to:

- **OSI / FSF policy evolution.** AGPL-3.0 has been stable since
  2007; we don't expect churn, but we will track any formal
  recognition changes.
- **Enterprise procurement.** FedRAMP, large-enterprise IT, and
  public-sector procurement frameworks vary on their acceptance of
  copyleft licenses; AGPL-3.0 is generally accepted, and the
  commercial carve-out covers the rest.
- **Contributor posture.** The current contribution policy (see
  [`CONTRIBUTING.md`](../CONTRIBUTING.md)) accepts contributions
  under the same AGPL-3.0-or-later license, with no CLA. This keeps
  the contribution friction low and the legal clarity high. If
  contributions start requiring a CLA for some reason in the future
  (e.g., a large enterprise contributor), the rationale will be
  revisited, not silently.
- **Fork / relicensing pressure.** If a meaningful number of
  downstream users demand a more permissive license, that pressure
  should be addressed via the commercial relicensing path first
  (one-off), not by changing the upstream license. See
  [Reversibility](#reversibility).

---

## 6. Reversibility

Switching a copyleft project's license is non-trivial. Because every
contributor's contribution is offered under AGPL-3.0-or-later, the
maintainer cannot unilaterally change the license for past
contributions. There are two principled paths:

1. **Explicit contributor consent.** Re-license only with explicit
   written consent from every contributor (and, for larger projects,
   this is impractical). Not a near-term lever.
2. **Dual-licence wrapper on a future major version.** Going forward
   from version `N+1`, accept contributions under a contributor
   license agreement (CLA) that permits the maintainer to
   dual-license under AGPL-3.0-or-later *and* a more permissive
   license. This gives the maintainer the option to re-license
   `N+1+` without breaking `1.0`-`N`. This is *not* in place today
   and would be its own large decision.

Neither path is appealing enough to motivate a near-term change. The
intention is that **AGPL-3.0-or-later is the long-term posture** of
this repository.

---

## 7. References

- [`LICENSE`](../LICENSE) at the repo root — full AGPL-3.0 text.
- [`README.md`](../README.md) §License — user-facing summary and
  commercial-offering pointer.
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) §License — contribution
  licensing terms (AGPL-3.0-or-later, no CLA).
- [`SECURITY.md`](../SECURITY.md) §Commercial channels — support,
  hosted, and proprietary-relicensing contact.
- [`.github/ISSUE_TEMPLATE/feature_request.md`](../.github/ISSUE_TEMPLATE/feature_request.md)
  §AGPL/licensing notes — feature-request template that asks
  contributors to flag AGPL-impact concerns upfront.
- SPDX header convention (`// SPDX-License-Identifier:
  AGPL-3.0-or-later`) applied to every hand-written `.cs` file under
  [`src/`](../src/), per the contribution policy.

---

*Last reviewed: initial adoption (task-3, AGPL-3.0-or-later); rationale
documented in task-15.*
