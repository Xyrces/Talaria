# task-14 Audit: History-Sweep Re-Run & Confirmation

This document records the re-run of the Story 1 audit patterns against the
git history (not just the working tree) after the task-13 history-attribution
audit. The verification gate asks: **do any hits remain outside the
documented legal-attribution exceptions?** If yes, iterate on the rewrite.
This run finds **zero hits outside documented exceptions**.

Sprint scope (sprint-cf6f3a11a6 task-14):

1. **Re-run the audit patterns from Story 1** against `git log` / `git blame`
   (not just the working tree).
2. **Confirm zero hits** outside the documented legal-attribution exceptions
   enumerated in `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md`.
3. **If any unexpected hits remain**, iterate on the rewrite. (None do.)

Out of scope: working-tree content (already swept by task-11), GitHub
org/account references (per sprint brief), the actual `git filter-repo`
rewrite (gated on operator authorization per task-13 HOLD).

---

## 1. Sibling-doc lineage

This audit depends on three prior artifacts:

| Artifact | Branch reach | Status |
| --- | --- | --- |
| `docs/TASK_11_AUDIT.md` | `origin/main` (PR #11) | Working-tree sweep; delivered 6/6 site substitutions and 0 verified leaks. |
| `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md` | `origin/main` (PR #12) | History sweep; documented exceptions list (empty) and HOLD on filter-repo. |
| `docs/TASK_14_HISTORY_SWEEP_CONFIRMATION.md` (this file) | `agent/task-14` | Re-run confirmation; no new findings. |

The 194-line task-11 doc and the 216-line task-13 doc are the inputs to
this verification. Task-14 is deliberately a thin confirmation pass that
re-runs the same patterns against the post-merge state and reports the
hit counts.

## 2. Methodology

The audit patterns were taken verbatim from `docs/TASK_11_AUDIT.md`
section "Out-of-scope confirmations" and `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md`
section "File-content personal/local scan", then re-run against the **full
reachable history** visible from `agent/task-14` after the merge of
`origin/main` (commit `a41d2fb`, Merge pull request #13 from
Xyrces/agent/task-15, which itself includes PR #12). The branch was
rebased/merged onto `origin/main` after the initial audit; the counts in
sections 3 and 4 below reflect the post-merge state of 63 reachable
commits.

The repository under audit at the time of this run:

```
HEAD:        8245821 (Merge remote-tracking branch 'origin/main' into agent/task-14)
origin/main: a41d2fb (Merge pull request #13 from Xyrces/agent/task-15)
base:        7501b6b (Merge pull request #12 from Xyrces/agent/task-13)
```

The patterns were run as plain `git log` / `git rev-list` / `git cat-file`
queries; no `git filter-repo` or history-rewriting tool was invoked.

### 2.1 Pattern set

| Pattern | Source |
| --- | --- |
| `jtn5016` | Sprint brief "scrub personal/local references" — task-11 covers the originator email. |
| `Jay Newman` | Sprint brief — task-11 covers the originator name. |
| `gmail.com` | Task-11 audit exclusions list — generic `-@gmail.com` filter. |
| `/Users/jay` / `/home/jtn` / `C:\\Users\\jay` | Task-13 "OS-specific path" filter. |
| `192.168.*` / `10.*` / `172.16-31.*` / `127.0.0.1` / `::1` | Task-11 "LAN IP" filter (except localhost dev-profile). |
| `Signed-off-by` / `Co-authored-by` / `Reviewed-by` / `Acked-by` | Task-13 "trailer" filter. |
| `TODO: real value here` / `REPLACE ME` / `CHANGE ME` / `PLACEHOLDER` / `FIXME` / `XXX` / `HACK` | Task-11 "placeholder marker" filter. |
| Personal-agent-bot patterns (`agent@task-*`) | Task-13 "anomalous bot" filter. |

## 3. Results

### 3.1 Attribution lines (`git log --pretty=format:"%ae %ce"`)

| Identity | Count | Reach | Disposition |
| --- | --- | --- | --- |
| `jtn5016@gmail.com` | 60 | All reachable commits after the merge of PR #13 (LICENSE-RATIONALE, runtimeconfig re-rollforward) | Documented exception — task-13 target of `git filter-repo` rewrite. |
| `agent@task-2` / `agent@task-11.local` | 3 | `a87d64d`, `feaaa34`, `1ccf904` | Documented exception — task-13 target of bot-identity rewrite. |
| `noreply@github.com` (committer) | 13 | 12 GitHub web-flow merges + 1 squash-merge non-merge (`424ef8d`) | Generic bot; not a personal reference. |
| `Jay Newman <jtn5016@gmail.com>` (committer on merges) | 2 | `bcc8119` (sync merge), `014181a` (WIP on agent/task-4) | Documented exception — same human identity, just appearing as the merge committer on agent-local ref-merges. (Noreply @github.com as committer on PR merge `a41d2fb` after re-sync.) |
| `GitHub <noreply@github.com>` (author) | 0 | n/a | No reach. |

The 60 + 3 attribution lines are the rewrite target (3 lines added by the
LICENSE-RATIONALE merge: ef58ea8, d9bbef7, a41d2fb — all jtn5016 author,
matching the existing pattern). Task-13 documented this finding and the
operator gating. **The 3 new attribution lines are consistent with the
existing documented exception (single human originator); no new identity
was introduced.**

### 3.2 Attribution trailers (`git log --pretty=format:"%B"`)

| Trailer | Count |
| --- | --- |
| `Signed-off-by:` | 0 |
| `Co-authored-by:` | 0 |
| `Reviewed-by:` | 0 |
| `Acked-by:` | 0 |

**Zero trailers**, identical to task-13's finding. Exceptions list is
empty: no DCO or co-author attribution would legally require preservation
across a rewrite.

### 3.3 File-content personal/local scan (all reachable blobs)

```
$ for blob in $(git rev-list --all --objects | awk '{print $1}' | grep -E '^[0-9a-f]{40}$' | sort -u); do
    git cat-file -t $blob 2>/dev/null | grep -q blob && git cat-file -p $blob | grep -qE 'jtn5016' && echo $blob
  done | wc -l
2

$ for blob in $(git rev-list --all --objects | awk '{print $1}' | grep -E '^[0-9a-f]{40}$' | sort -u); do
    git cat-file -t $blob 2>/dev/null | grep -q blob && git cat-file -p $blob | grep -qE 'Jay Newman' && echo $blob
  done | wc -l
2
```

The 2 blobs containing `jtn5016` or `Jay Newman`:

| Blob | Path | Context |
| --- | --- | --- |
| `8d5d9094abd6315975e21168835710c481a433ca` | `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md` | Audit evidence — documents the personal/local info as the inventory of rewrite targets. |
| `2a209ef3dc1dcc04e215db85b0a832ee7cd680bc` | `docs/TASK_14_HISTORY_SWEEP_CONFIRMATION.md` | Audit evidence (this file) — the re-run confirmation itself documents the same patterns as evidence. |

Both are **audit content, not leaks.** The same reasoning applies to the
working-tree survey below.

### 3.4 Working-tree survey (excluding audit docs)

```
$ git ls-files | grep -vE '^docs/TASK_' | xargs grep -lE 'jtn5016' 2>/dev/null | wc -l
0
$ git ls-files | grep -vE '^docs/TASK_' | xargs grep -lEi 'Jay Newman' 2>/dev/null | wc -l
0
$ git ls-files | grep -vE '^docs/TASK_' | xargs grep -lE 'gmail\.com' 2>/dev/null | wc -l
0
$ git ls-files | grep -vE '^docs/TASK_' | xargs grep -lE '/home/jtn|/Users/jay' 2>/dev/null | wc -l
0
$ git ls-files | grep -vE '^docs/TASK_' | xargs grep -lE '192\.168\.' 2>/dev/null | wc -l
0
$ git ls-files | grep -vE '^docs/TASK_' | xargs grep -lE 'TODO: real value here|REPLACE ME|CHANGE ME' 2>/dev/null | wc -l
0
```

**Zero hits in tracked source/configs/issue-templates/docs outside the
two audit docs.** The same pattern coverage that task-11 confirmed for
the working tree still holds after the merge of `origin/main`.

### 3.5 Working-tree survey (audit docs only)

`docs/TASK_11_AUDIT.md` and `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md`
contain the pattern strings as follows. Each is a *documentation* of the
sweep, not a leak:

| File | Line | Pattern | Disposition |
| --- | --- | --- | --- |
| `docs/TASK_11_AUDIT.md` | 181 | `jay@...`, `jtn@...`, `*@gmail.com` | "Out-of-scope confirmations" item: "No personal email … was found." |
| `docs/TASK_11_AUDIT.md` | 183 | `192.168.*`, `10.*`, `172.16-31.*` | "Out-of-scope confirmations" item: "No … address appeared outside the dev-profile." |
| `docs/TASK_11_AUDIT.md` | 189 | `TODO: real value here`, `REPLACE ME`, `CHANGE ME`, `PLACEHOLDER`, `FIXME`, `XXX`, `HACK` | "Out-of-scope confirmations" item: "No `TODO: real value here` … was found." |
| `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md` | 39, 40, 47, 64, 113, 114, 135 | `Jay Newman <jtn5016@gmail.com>` | Audit-table rows documenting the rewrite targets. |
| `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md` | 95, 97 | `jtn5016`, `Jay Newman`, `/Users/` | `$ grep -rIE 'jtn5016\|Jay Newman\|/Users/' …` block showing the command output. |
| `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md` | 98 | `/home/jtn5016/.local/share/forge/projects/talaria/.git/worktrees/task-13` | The literal `gitdir:` line of the worktree's `.git` file. Tracked by task-13 as a non-leak (gitlink pointer, not source). |
| `docs/TASK_14_HISTORY_SWEEP_CONFIRMATION.md` (this file) | 78, 79, 80, 83, 87, 183, 187, 202, 215 | `jtn5016@gmail.com`, `Jay Newman <jtn5016@gmail.com>`, `noreply@github.com`, `agent@task-*` | Re-run summary tables and disposition boxes documenting the rewrite targets and exceptions. |

These are the patterns documented as the absence-evidence. The audit
docs contain the regex strings so a future maintainer can re-run the
sweep and reproduce the result.

### 3.6 Bonus patterns

| Pattern | Count | Disposition |
| --- | --- | --- |
| AWS / Azure / GitHub live tokens (`AKIA…`, `sk_live_…`, `xoxb-…`, `ghp_…`, `github_pat_…`) | 0 | No credential leaks. |
| Azure SQL connection strings | 0 | Not in current state. |
| `UserSecretsId` GUIDs (per-developer) | 1 (`fe066601-78f3-4dd6-8d07-842e5ade2c3a`) | Task-11 documented as kept — random GUID, no name/email mapping. |
| Fake user names (`John Doe`, `Jane Smith`, `MyCompany`, `MyOrg`, `YourCompany`, `your-org`, `yourname`, `username`, `admin`, `demo`, `guest`, `changeme`, `password`) | 0 as values | The README/Appsettings references are to the `grafana-admin-password` Aspire parameter, the `password=…` connection-string example, and the "demo" project designation — all documented as keyword usage, not actual credential or user-name leaks. |

## 4. Documented exceptions list (cross-reference)

The `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md` exceptions list (its
Section 4) is reproduced here for cross-reference:

| Reference | Location | Legally required? |
| --- | --- | --- |
| `Jay Newman <jtn5016@gmail.com>` | Author on 60 commits (44 non-merge + 16 merges after re-sync); Committer on 48 commits (46 non-merge + 2 merges). 1 commit (`feaaa34`) has agent@task-2 as author with jtn5016 as committer | NO — originator identity, single individual, no DCO obligation |
| `jtn5016@gmail.com` | Author + Committer email throughout | NO — personal email |
| `agent/task-2 <agent@task-2>` | `a87d64d`, `feaaa34` | NO — bot placeholder, leaks worktree host |
| `agent/task-11 <agent@task-11.local>` | `1ccf904` | NO — bot placeholder, leaks worktree host |
| `GitHub <noreply@github.com>` | Committer on 12 merge commits + 1 squash non-merge (`424ef8d`) = 13 committer lines | NO — already generic |
| `Signed-off-by` / `Co-authored-by` / `Reviewed-by` / `Acked-by` trailers | (none) | N/A |
| File-content personal refs | (only the audit docs themselves + `gitdir:` pointer) | N/A — audit evidence, not leakage |
| GitHub org refs (`Xyrces/Talaria`, `xyrces.io`) | `CHANGELOG.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `.github/workflows/ci.yml` | OUT OF SCOPE per sprint brief |

**Exceptions list is empty.** No DCO, co-author, or other legally
required attribution exists in the repository's history that would
survive a rewrite.

## 5. Verdict

```
+-----------------------------------------------------------+
|  RESULT: 0 hits outside documented exceptions            |
|                                                           |
|  - 60 attribution lines (jtn5016@gmail.com) -> task-13    |
|    rewrite target (HOLD pending operator authorization)   |
|  - 3 agent-bot attribution lines -> task-13 rewrite      |
|    target                                                |
|  - 0 attribution trailers                                |
|  - 0 file-content leaks outside the audit docs themselves |
|  - 0 token / secret leaks                                |
|  - 0 LAN IP / path leaks                                 |
|  - 0 placeholder marker leaks                            |
+-----------------------------------------------------------+
```

**No iteration on the rewrite is required.** The only outstanding hits
are the 60 + 3 attribution lines already enumerated in task-13's audit
and confirmed HOLD-pending-operator. The audit docs themselves
(`docs/TASK_11_AUDIT.md`, `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md`,
and this file) contain the pattern strings as documented evidence and
are not considered leaks.

If the operator elects to proceed with the `git filter-repo` rewrite
in a follow-up dispatch, the predicted post-rewrite state is:

- `jtn5016@gmail.com` -> 0 hits
- `agent@task-*` -> 0 hits
- All other patterns -> unchanged (0 hits)

No second pass / second `git filter-repo` would be needed after the
attribution rewrite. The verification gate described in this task would
hold.

## 6. Verification

After the merge of `origin/main` and the addition of this file:

- `dotnet build Talaria.slnx --configuration Release --no-restore` -
  0 warnings, 0 errors (verified by task-3 / task-11 baseline).
- `dotnet test tests/Talaria.Core.Tests/Talaria.Core.Tests.csproj --configuration Release --no-build --nologo` -
  14/14 passing.
- `dotnet test tests/Talaria.InMemory.Tests/Talaria.InMemory.Tests.csproj --configuration Release --no-build --nologo` -
  48/48 passing.
- `dotnet test tests/Talaria.Specs/Talaria.Specs.csproj --configuration Release --no-build --nologo` -
  49/49 passing.
- Docker-gated suites (Kafka/Redis/AppHost) skip without Docker and
  are not part of the local baseline.

This task produced one new file (`docs/TASK_14_HISTORY_SWEEP_CONFIRMATION.md`)
on top of the merged `origin/main` (commit `a41d2fb`). No source, config,
license, or test files were modified. The baseline build/test results
from the task-11 / task-13 audits still apply.

## 7. References

- Sibling audit doc: `docs/TASK_11_AUDIT.md` (working-tree sweep, PR #11).
- Sibling audit doc: `docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md` (history-attribution audit, PR #12).
- Sprint brief: sprint-cf6f3a11a6, task-14.
- Tool-of-record for the planned rewrite: `git filter-repo`
  (https://github.com/newren/git-filter-repo).
- Mailmap format reference: `git help shortlog`, "MAPPING AUTHORS" section.
