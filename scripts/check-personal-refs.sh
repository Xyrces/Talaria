#!/usr/bin/env bash
# scripts/check-personal-refs.sh
#
# Personal-refs guard for the Talaria open-source release.
#
# Scans the working tree for personal or host-local references (names, emails,
# LAN IPs, placeholder markers) that must not ship. Runs against the working
# tree by default (so contributors can invoke it locally before pushing) and
# emits GitHub Actions ::warning:: annotations on stderr when hits are found.
#
# Usage:
#   scripts/check-personal-refs.sh [--self-test] [--quiet]
#
# Environment:
#   PERSONAL_REFS_GUARD=allow  Suppress warnings (exit 0 unconditionally)
#   PERSONAL_REFS_GUARD=deny   Treat any warning as a hard failure (exit 1)
#   PERSONAL_REFS_GUARD=warn   Emit warnings, default exit 0  (default)
#
# Exit codes:
#   0  No warnings, or warnings were suppressed / allowed
#   1  Warnings emitted and PERSONAL_REFS_GUARD=deny
#   2  Setup error (not a git repo, bash too old, etc.)
#
# Self-test:
#   scripts/check-personal-refs.sh --self-test
#     Plants known hits in a temp dir and asserts the script flags them.
#     Returns 0 if every expected hit was flagged, 1 otherwise.
#
# Out of scope for this script:
#   - .github/ workflow wiring (lives in ci.yml)
#   - One-shot scrubbing of existing content (this is a guard, not a scrubber)
#   - Git history (working tree only)
#
# Excludes (intentionally not scanned):
#   .git/                       Git internals
#   .cr/personal                CodeRush personal settings (gitignored)
#   bin/, obj/                  Build output
#   node_modules/               JS dependency dirs (defensive)
#   *.lock.json                 Pinned dependency locks
#   coveragereport/             Coverage output
#   PublishScripts/             Web deploy publish scripts (gitignored)

set -uo pipefail

GUARD_MODE="${PERSONAL_REFS_GUARD:-warn}"
QUIET=0
SELF_TEST=0
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --self-test)
      SELF_TEST=1
      shift
      ;;
    --quiet|-q)
      QUIET=1
      shift
      ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

# ----- guard helpers -----------------------------------------------------

if [[ "$GUARD_MODE" == "allow" ]]; then
  [[ $QUIET -eq 0 ]] && echo "PERSONAL_REFS_GUARD=allow - skipping personal-refs guard" >&2
  exit 0
fi

# ----- self-test ---------------------------------------------------------

if [[ $SELF_TEST -eq 1 ]]; then
  ORIG_PWD="$(pwd)"
  TMP_DIR="$(mktemp -d)"
  trap 'rm -rf "$TMP_DIR"' EXIT
  cd "$TMP_DIR"
  git init -q .
  git config user.email self-test@local
  git config user.name self-test
  mkdir -p src tests node_modules .cr/personal
  : > README.md
  printf '// clean fixture\n' > src/clean.cs
  printf 'namespace Talaria.Core.Tests;\n' >> src/clean.cs

  printf 'author = "jtn5016@gmail.com"\n' > src/hit_email.cs
  printf '// /home/jtn5016/work/repo\n' > src/hit_path.cs
  printf 'ApiKey = "abcd1234"\n' > src/hit_secret.cs
  printf '// TODO: real value here\n' > src/hit_todo.cs
  printf 'x' > node_modules/should_skip.cs
  printf 'author = "jtn5016@gmail.com"\n' > .cr/personal/should_skip.cs

  git add -A
  git commit -q -m self-test 2>/dev/null || true

  STDOUT_FILE="$(mktemp)"
  STDERR_FILE="$(mktemp)"
  PERSONAL_REFS_GUARD=deny bash "$SCRIPT_PATH" >"$STDOUT_FILE" 2>"$STDERR_FILE"
  rc=$?
  if [[ $rc -ne 1 ]]; then
    echo "self-test FAIL: expected exit 1 with PERSONAL_REFS_GUARD=deny, got $rc" >&2
    exit 1
  fi
  expected_hits=("src/hit_email.cs" "src/hit_path.cs" "src/hit_secret.cs" "src/hit_todo.cs")
  for hit in "${expected_hits[@]}"; do
    if ! grep -q "$hit" "$STDERR_FILE"; then
      echo "self-test FAIL: expected stderr to mention $hit" >&2
      echo "--- stderr ---" >&2
      cat "$STDERR_FILE" >&2
      echo "--------------" >&2
      exit 1
    fi
  done
  if grep -q "node_modules/should_skip.cs" "$STDERR_FILE"; then
    echo "self-test FAIL: node_modules/ exclusion not honored" >&2
    exit 1
  fi
  if grep -q ".cr/personal/should_skip.cs" "$STDERR_FILE"; then
    echo "self-test FAIL: .cr/personal/ exclusion not honored" >&2
    exit 1
  fi
  echo "self-test OK" >&2
  cd "$ORIG_PWD"
  exit 0
fi

# ----- preconditions -----------------------------------------------------

if ! command -v git >/dev/null 2>&1; then
  echo "ERROR: git is required but was not found on PATH" >&2
  exit 2
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "ERROR: not inside a git working tree" >&2
  exit 2
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# Pattern -> human-readable description. Patterns are POSIX extended regexes
# (grep -E), case-sensitive. Add new patterns here and the self-test above.
PATTERNS=(
  "jtn5016@|jay\.newman|Jay Newman=personal author identity"
  "/home/[a-zA-Z0-9._-]+/|C:\\\\Users\\\\[a-zA-Z0-9._-]+/=host-local absolute path"
"ApiKey[[:space:]]*=[[:space:]]*\"[^\"]{4,}=hardcoded API key in source"
  "TODO[[:space:]]*:[[:space:]]*real value here|FIXME[[:space:]]*:[[:space:]]*real value here=placeholder TODO/FIXME real value here"
)

EXCLUDES=("^\.git/" "^\.cr/personal/" "^bin/" "^obj/" "^node_modules/" "\.lock\.json$" "^coveragereport/" "^PublishScripts/")

EXTENSIONS_REGEX='\.(md|yml|yaml|json|csproj|props|targets|feature|cs|ps1|sh)$'

warnings=0
emit_warning() {
  local file="$1"
  local line="$2"
  local col="$3"
  local desc="$4"
  warnings=$((warnings + 1))
  if [[ $QUIET -eq 0 ]]; then
    printf "  ::warning file=%s,line=%s,col=%s::%s\n" "$file" "$line" "$col" "$desc" >&2
  fi
}

scan_file() {
  local file="$1"
  local pat desc matches ln
  while IFS='=' read -r pat desc; do
    [[ -z "$pat" ]] && continue
    matches="$(grep -nE "$pat" "$file" 2>/dev/null || true)"
    if [[ -n "$matches" ]]; then
      while IFS= read -r m; do
        ln="${m%%:*}"
        emit_warning "$file" "$ln" "1" "$desc"
      done <<< "$matches"
    fi
  done < <(printf '%s\n' "${PATTERNS[@]}")
}

mapfile -t FILES < <(git ls-files | grep -E "$EXTENSIONS_REGEX" 2>/dev/null || true)

for file in "${FILES[@]}"; do
  skip=0
  for ex in "${EXCLUDES[@]}"; do
    if [[ "$file" =~ $ex ]]; then
      skip=1
      break
    fi
  done
  [[ $skip -eq 1 ]] && continue
  scan_file "$file"
done

if [[ $warnings -eq 0 ]]; then
  [[ $QUIET -eq 0 ]] && echo "PERSONAL_REFS_GUARD OK: no hits in ${#FILES[@]} files" >&2
  exit 0
fi

echo "PERSONAL_REFS_GUARD: $warnings hit(s) across ${#FILES[@]} files" >&2
if [[ "$GUARD_MODE" == "deny" ]]; then
  exit 1
fi
exit 0
