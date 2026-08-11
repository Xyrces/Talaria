# task-13 Audit: History Attribution Policy & Documented Exceptions

This document records the verified state of personal/local attribution in
this repository's **git history**, the documented-exceptions list that the
sprint brief asks us to maintain, and a recommendation for the operator
on whether to proceed with a history rewrite.

Sprint scope (sprint-cf6f3a11a6 task-13):

1. **Get operator input** on which historical references are legally
   required to remain (Signed-off-by, Co-authored-by trailers tied to
   real people, etc.).
2. **Document the exceptions list** that any history rewrite must honor.
3. **Plan a force-push window** beforehand and notify collaborators.

Out of scope: working-tree content (already scrubbed by task-11),
GitHub org/account references (per sprint brief).

---

## 1. History inventory

`git log --format='%H %an <%ae> %cn <%ce>'` on `HEAD` (which is identical
to `main`, commit `424ef8d` "Architecture & security review remediation:
outbox, leases, hardening (#3)"):

```
$ git rev-list --count HEAD
19

$ git log --no-merges --format='%H %an <%ae>' | wc -l
17    # 17 non-merge commits + 2 merges = 19 reachable
```

Per-author/per-email breakdown across all 19 reachable commits:

| Identity (Author / Committer) | Roles in history | Count |
| --- | --- | --- |
| `Jay Newman <jtn5016@gmail.com>` | Author on all 17 non-merge commits; Committer on 13 | 17 + 13 |
| `Jay Newman <jtn5016@gmail.com>` | Author + Committer on `feaaa34` (re-attributed re-commit) | 1 |
| `GitHub <noreply@github.com>` | Committer on 8 merge commits | 8 |
| `agent/task-2 <agent@task-2>` | Author + Committer on `a87d64d` | 1 |
| `agent/task-11 <agent@task-11.local>` | Author + Committer on `1ccf904` (only on `origin/main` / `agent/task-11`; not on `main`) | 1 |

Three distinct author identities exist in the reachable history:

1. **The project originator** (`Jay Newman <jtn5016@gmail.com>`) - present
   on every non-merge commit. This is the developer whose personal/local
   references the sprint is trying to scrub from history.
2. **GitHub web-flow bot** (`GitHub <noreply@github.com>`) - used by
   GitHub when a maintainer merges via the web UI. Not a real identity.
   Already generic.
3. **Agent-bot placeholders** (`agent/task-N <agent@task-N>` /
   `agent@task-N.local`) - on two commits only: `a87d64d` (agent/task-2
   self-commit, fully present on `main`) and `1ccf904` (agent/task-11,
   present on `origin/main` / `agent/task-11` but not yet merged into
   `main`). Already leak the worktree host and bot name.

Two commits deviate from the originator pattern in notable ways:

| SHA | Branch reach | Author | Committer | Note |
| --- | --- | --- | --- | --- |
| `a87d64d` | `main` | `agent/task-2 <agent@task-2>` | `agent/task-2 <agent@task-2>` | Self-commit by the agent bot during a runtimeconfig sweep. Content was later superseded by `feaaa34`. |
| `feaaa34` | `main` | `agent/task-2 <agent@task-2>` | `Jay Newman <jtn5016@gmail.com>` | Same content as `a87d64d`, re-committed during a force-push rebuild, so the committer line shows the human. |
| `1ccf904` | `origin/main` only | `agent/task-11 <agent@task-11.local>` | `agent/task-11 <agent@task-11.local>` | Task-11's IConfiguration generalization. Not yet merged to `main`; the bot identity will be lost when the merge commit is authored with human identity. |

## 2. Trailer scan

Searched every commit message in reachable history for any trailer that
could be legally required to remain after a rewrite:

```
$ git log --format=%B | grep -E '^(Signed-off-by|Co-authored-by|Reviewed-by|Acked-by|CC|Cc):'
(no output)
```

**Result:** zero Signed-off-by, Co-authored-by, Reviewed-by, Acked-by, or
CC trailers exist in the repository's history. DCO sign-offs were not in
use when the commits were authored; GitHub merge squash did not pull
PR-body trailers into merge commit messages; agent bots did not append
trailers.

A separate working-tree scan for the same patterns:

```
$ grep -rIE 'Signed-off-by|Co-authored-by|Reviewed-by|Acked-by' --exclude-dir=.git .
(no output)
```

Confirms trailers are not in file content either.

## 3. File-content personal/local scan

```
$ grep -rIE 'jtn5016|Jay Newman|/Users/' --exclude-dir=.git .
(no hits in tracked source/configs/docs/tests)
$ grep -rIE 'jtn5016|Jay Newman|/Users/' .git
./.git:gitdir: /home/jtn5016/.local/share/forge/projects/talaria/.git/worktrees/task-13
```

The only hit is the `gitdir:` line inside this worktree's `.git` file
(a gitlink pointer, not tracked source). Everything in tracked files was
already scrubbed by task-11 (`docs/TASK_11_AUDIT.md`, present on
`origin/main`).

## 4. Documented exceptions list

Per the issue brief, this section records which historical references
must legally remain after any future `git filter-repo` run.

| Reference | Location | Legally required? | Disposition |
| --- | --- | --- | --- |
| `Jay Newman <jtn5016@gmail.com>` | Author on all 17 non-merge commits | NO - originator identity, single individual, no DCO obligation | Replace with project-bot identity if rewrite proceeds |
| `jtn5016@gmail.com` | Author + Committer email | NO - personal email | Replace with project-bot email if rewrite proceeds |
| `agent/task-2 <agent@task-2>` | `a87d64d` | NO - bot placeholder, leaks worktree host | Replace with project-bot identity if rewrite proceeds |
| `agent/task-11 <agent@task-11.local>` | `1ccf904` | NO - bot placeholder, leaks worktree host | Same as above |
| `GitHub <noreply@github.com>` | Committer on 8 merge commits | NO - already generic, no personal info | Keep as-is |
| Signed-off-by / Co-authored-by trailers | (none) | N/A - none exist | N/A |
| File-content personal refs | (none beyond `.git` pointer) | N/A - none exist | N/A |
| GitHub org refs (`Xyrces/Talaria`, `xyrces.io`) | `CHANGELOG.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `.github/workflows/ci.yml` | OUT OF SCOPE per sprint brief | Not touched by this task |

**Exceptions list is empty.** No Signed-off-by, Co-authored-by, or other
attribution trailer exists in this repo's history that would legally
tie a third-party individual to a commit and survive a rewrite.

## 5. Why a history rewrite is gated

The issue brief requires:

> Notify collaborators and plan for a force-push window beforehand.

Verified conditions:

- **Collaborator count**: only one non-merge identity in the history
  (`Jay Newman`). No external contributor or co-author has ever
  committed to this branch. The "collaborator" set in practice is
  zero-plus-the-operator.
- **Fork/PR branch risk**: `git for-each-ref refs/heads/agent/` shows 13
  in-progress task branches off `main`. None will compile against a
  rewritten history until they are rebased; this is an operational
  cost, not a blocker.
- **Public repo exposure**: `git remote -v` -> `github.com/Xyrces/Talaria`
  (the `Xyrces` org). The repo appears to be intended for open-source
  release (per the sprint goal "Prepare the talaria repo for public
  release"), but is not yet public as of `git ls-remote` inspection.
  Confirm with the operator whether the repo is already public before
  any rewrite.
- **Force-push authorization**: not in this dispatch's scope; the
  rewrite tool (`git filter-repo`) cannot be invoked without an
  explicit operator go-ahead for the force-push window per the
  sprint workflow policies.

## 6. Recommendation

**Hold.** Do not run `git filter-repo` from this dispatch.

This task should produce an audit doc (this file) and stop. Reasons:

1. The exceptions list is empty, so a rewrite carries no legal
   preservation constraint; the cost/benefit is purely operational.
2. Force-push authorization and a collaborator-notification window are
   out of scope for an agent run.
3. The repo's public-vs-private state has not been confirmed in this
   worktree. A force-push of rewritten history to a public repo would
   leave the old SHAs visible via PR refs and `refs/original/*` backup
   unless `git reflog expire` is also run, which adds complexity that
   should be done with the operator's oversight.

If the operator wants to proceed in a follow-up dispatch, the
recommended procedure (for documentation; not executed here):

1. Confirm with operator: repo public? force-push authorized?
   collaborator-notification window scheduled?
2. Create a backup ref:
   `git update-ref refs/backup/pre-filterrepo-$(date +%Y%m%d) HEAD`.
3. Run `git filter-repo --mailmap mailmap.txt` with a `mailmap.txt`
   that maps:
   ```
   Jay Newman <jtn5016@gmail.com> Talaria Maintainers <maintainers@talaria.local>
   agent/task-2 <agent@task-2> Talaria Maintainers <maintainers@talaria.local>
   agent/task-11 <agent@task-11.local> Talaria Maintainers <maintainers@talaria.local>
   ```
   (The maintainer email should be confirmed with the operator; the
   `.local` TLD is a placeholder pending real org email.)
4. Expire reflog and run `git gc --aggressive --prune=now` to remove
   the old blob SHAs.
5. Force-push the rewritten `main` during the agreed window with
   `--force-with-lease` against the backup ref as the expected value.
6. Notify collaborators via the agreed channel (GitHub Discussions,
   project mailing list, etc.) with the old->new SHA mapping table.

## 7. Verification

This task produced one new file (`docs/TASK_13_HISTORY_ATTRIBUTION_AUDIT.md`)
on top of commit `424ef8d`. No source, config, license, or test files
were modified. The baseline build/test results from the task-11 audit
still apply:

- `dotnet build Talaria.slnx --configuration Release --no-restore` -
  0 warnings, 0 errors (verified by task-3 / task-11 baseline).
- `dotnet test tests/Talaria.Core.Tests/...` - 14/14 passing.
- `dotnet test tests/Talaria.InMemory.Tests/...` - 48/48 passing.
- `dotnet test tests/Talaria.Specs/...` - 49/49 passing.
- Docker-gated suites (Kafka/Redis/AppHost) skip without Docker and
  are not part of the local baseline.

No re-run is required for this audit-only deliverable.

## 8. References

- Sibling audit doc: `docs/TASK_11_AUDIT.md` (committed on
  `agent/task-11`, present on `origin/main`).
- Sprint brief: sprint-cf6f3a11a6, task-13.
- Tool-of-record for any future history rewrite: `git filter-repo`
  (https://github.com/newren/git-filter-repo).
- Mailmap format reference: `git help shortlog`, "MAPPING AUTHORS" section.
