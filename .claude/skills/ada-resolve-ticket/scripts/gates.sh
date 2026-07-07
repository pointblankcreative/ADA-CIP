#!/usr/bin/env bash
# gates.sh -- run the ADA merge gates against a worktree. Non-zero exit on failure.
# These mirror the project's real verification recipe: synchronous tsc, and pytest
# run from /tmp with PYTHONPATH (the repo's .env CORS list breaks pydantic-settings
# if pytest is launched from the repo root).
#
#   gates.sh <worktree_path>
set -uo pipefail

WT="${1:?usage: gates.sh <worktree>}"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${ADA_REPO:-$(cd "$SCRIPTS_DIR/../../../.." && pwd)}"

fail=0

echo "== frontend typecheck: npx tsc --noEmit =="
( cd "$WT/frontend" && npx tsc --noEmit ) || { echo ">> TYPECHECK FAILED"; fail=1; }

echo "== backend pytest =="
pytest_paths=""
[ -d "$WT/tests" ] && pytest_paths="$pytest_paths $WT/tests"
[ -d "$WT/backend/tests" ] && pytest_paths="$pytest_paths $WT/backend/tests"
if [ -n "$pytest_paths" ]; then
  # shellcheck disable=SC2086
  ( cd /tmp && PYTHONPATH="$WT" "$REPO/.venv/bin/python" -m pytest $pytest_paths -q ) \
    || { echo ">> PYTEST FAILED"; fail=1; }
else
  echo "(no test dirs found; skipping pytest)"
fi

if [ "$fail" -eq 0 ]; then echo "== gates GREEN =="; else echo "== gates RED =="; fi
exit "$fail"
