# task-16: License Pointers Audit & Disposition

> **Dated note (2026-08-15):** Talaria has since been relicensed from
> AGPL-3.0-or-later to Apache-2.0. The `LICENSE` file, `README.md`,
> `CONTRIBUTING.md`, `SECURITY.md`, and `docs/LICENSE-RATIONALE.md`
> pointers were updated to reflect the new license. The body below is
> preserved as a historical record of the task-16 implementation.

This document records the disposition for **task-16** of the open-source
release-readiness sprint. Task-16 asks that the chosen license and the
canonical commercial-licensing posture be unambiguous to a first-time
reader who only looks at the root of the repo, with explicit pointers
to where the commercial-terms source-of-truth lives.

## Goal

Per the task brief:

1. Add a LICENSE file at the repo root matching the chosen license
   exactly, with **no extra boilerplate**.
2. If dual-license / commercial-EULA is in play, LICENSE must clearly
   point to a canonical commercial-terms source-of-truth document
   (hosted elsewhere or under a clearly non-license path).
3. README must have an explicit License section pointing to LICENSE
   and stating the commercial-licensing posture.
4. CONTRIBUTING must state the contributor license expectations and
   that commercial terms live elsewhere.

## Findings on the existing state (HEAD at dispatch time)

| Artifact | Status | Notes |
| --- | --- | --- |
| `LICENSE` at repo root | Present, byte-identical to canonical AGPL-3.0 text from https://www.gnu.org/licenses/agpl-3.0.txt | md5 `eb1e647870add0502f8f010b19de32af`, 661 lines. No extra boilerplate. Satisfies the brief's "matching the chosen license exactly" requirement. Left untouched. |
| `README.md` §License | Present (lines 166-178 at dispatch time). States AGPL-3.0-or-later, lists commercial offering (support/SLAs, hosting, proprietary relicensing), points readers at SECURITY.md for the support contact. | Missing an explicit pointer to a canonical rationale/commercial-terms source-of-truth doc. Updated to reference `docs/LICENSE-RATIONALE.md` as that source-of-truth. |
| `CONTRIBUTING.md` preamble & closing | Present. Preamble states the contributor license expectation ("By contributing, you agree that your contributions will be released under the same license"). Closing pointer directs commercial-licensing questions to README §License and SECURITY.md. | Missing an explicit pointer to a canonical rationale/commercial-terms source-of-truth doc. Updated to reference `docs/LICENSE-RATIONALE.md`. |
| `SECURITY.md` §Commercial channels | Present. Lists support, hosted/managed, proprietary-relicensing offerings, with the `security@xyrces.io` contact. | Used as the operational contact channel; the *rationale* and *posture* live in `docs/LICENSE-RATIONALE.md`. |
| `docs/LICENSE-RATIONALE.md` | **Added by this task.** New file under the clearly non-license path `docs/`. | Captures the AGPL rationale, the comparison matrix vs BUSL/SSPL/Apache+EULA, the commercial-offering boundary, and cross-references to LICENSE/README/CONTRIBUTING/SECURITY. |

## Disposition summary

| Brief item | Disposition |
| --- | --- |
| LICENSE at repo root, exact match, no boilerplate | Already satisfied (byte-identical to canonical AGPL-3.0). **Left untouched.** |
| Canonical commercial-terms source-of-truth doc | Added `docs/LICENSE-RATIONALE.md` under `docs/` (clearly non-license path). Content: why AGPL was chosen; comparison vs BUSL-1.1 / SSPL-1.0 / Apache-2.0 + EULA; commercial-offering boundary; cross-references. |
| README §License points to LICENSE + commercial posture | Already partially satisfied. Updated to add explicit cross-link to `docs/LICENSE-RATIONALE.md` as the canonical commercial-terms source-of-truth (and to keep the existing SECURITY.md pointer). |
| CONTRIBUTING states contributor license expectations + commercial-terms location | Already partially satisfied. Updated preamble and closing pointer to reference `docs/LICENSE-RATIONALE.md` as the canonical commercial-terms source-of-truth. |

## Why this is sufficient

- LICENSE itself remains the canonical, unmodified AGPL-3.0 text — no
  inline commercial legalese is added to it.
- The commercial-terms source-of-truth lives under `docs/`, a clearly
  non-license path, so a reader landing on LICENSE is not confused into
  thinking the rationale doc is part of the license.
- README, CONTRIBUTING, and SECURITY.md all cross-link to the rationale
  doc, so the pointer is consistent across the user-facing surfaces a
  new contributor, adopter, or enterprise procurement reviewer will
  see.
- SPDX headers across `src/` already reference AGPL-3.0-or-later (per
  task-3); they are unaffected by this task.

## Files changed in this task

- `README.md` — extended §License (lines 166-178 at dispatch time) with
  a cross-link to `docs/LICENSE-RATIONALE.md`. Net +6 lines.
- `CONTRIBUTING.md` — extended preamble (lines 8-11 at dispatch time)
  with a cross-link to `docs/LICENSE-RATIONALE.md`; extended closing
  "Questions?" pointer (lines 252-254 at dispatch time) the same way.
  Net +5 lines.
- `docs/LICENSE-RATIONALE.md` — **new file**, 323 lines. Canonical
  rationale + commercial-terms source-of-truth under `docs/`.
- `docs/TASK_16_LICENSE_POINTERS.md` — **new file** (this document).
- `LICENSE` — **untouched** (byte-identical to canonical AGPL-3.0).

## Verification (run on the committed branch)

- `md5sum LICENSE` returns `eb1e647870add0502f8f010b19de32af` —
  byte-identical to the canonical AGPL-3.0 text from gnu.org.
- `grep -n "LICENSE-RATIONALE" README.md CONTRIBUTING.md` resolves in
  both updated files.
- `dotnet build Talaria.slnx --configuration Release --no-restore`
  reports **0 warnings, 0 errors** (docs-only edits; no production-code
  changes).
- `dotnet test Talaria.slnx --configuration Release --no-build`
  reports **111 passed, 14 skipped (Docker-gated), 0 failed**
  baseline. Skips are expected: `[DockerFact]` covers Redis / Kafka /
  AppHost multi-container paths. Matches the project baseline from
  task-3 / task-7 / task-13.

## References

- `LICENSE` at the repo root — canonical AGPL-3.0 text.
- `README.md` §License — user-facing summary.
- `CONTRIBUTING.md` preamble & closing — contribution licensing.
- `SECURITY.md` §Commercial channels — support contact.
- `docs/LICENSE-RATIONALE.md` — canonical commercial-terms source-of-truth.
- `CHANGELOG.md` — release-note seed.
- `docs/TASK_11_AUDIT.md` — sibling audit-memo template.
- `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md` — sibling audit-memo
  template.

*Last reviewed: task-16 initial implementation; LICENSE confirmed
byte-identical to canonical AGPL-3.0 text; commercial-terms
source-of-truth relocated to docs/LICENSE-RATIONALE.md.*
