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

# Python interpreter: prefer the repo venv (Frazer's Mac). Fall back to system
# python3 when there's no venv — Claude Code on the web bootstraps deps into the
# system interpreter (pip install -r requirements.txt) rather than a .venv.
PY="$REPO/.venv/bin/python"
[ -x "$PY" ] || PY="$(command -v python3 || true)"
if [ -z "$PY" ]; then echo ">> no python interpreter found"; exit 2; fi

fail=0

echo "== frontend typecheck: npx tsc --noEmit =="
( cd "$WT/frontend" && npx tsc --noEmit ) || { echo ">> TYPECHECK FAILED"; fail=1; }

# Backend gate, in two tiers:
#   1. backend/tests is the authoritative maintained suite (CLAUDE.md) and must
#      be fully green.
#   2. The top-level tests/ tree carries documented stale failures (duplicate
#      pacing/retro/media-plan mocks that predate prod). We still run it for the
#      coverage it adds (e.g. the diagnostics tests live there), but fail the
#      build only on failures OUTSIDE the known-stale baseline, so pre-existing
#      rot doesn't park every clean ticket. Baseline lives in
#      known_stale_tests.txt; empty it once the duplicates are reconciled.
echo "== backend pytest: backend/tests (authoritative — must be green) =="
if [ -d "$WT/backend/tests" ]; then
  ( cd /tmp && PYTHONPATH="$WT" "$PY" -m pytest "$WT/backend/tests" -q ) \
    || { echo ">> backend/tests FAILED"; fail=1; }
else
  echo "(no backend/tests dir found)"
fi

if [ -d "$WT/tests" ]; then
  echo "== backend pytest: tests/ (new failures only; known-stale allowed) =="
  KNOWN_STALE="$SCRIPTS_DIR/known_stale_tests.txt"
  out="$(cd /tmp && PYTHONPATH="$WT" "$PY" -m pytest "$WT/tests" -q --tb=no -p no:cacheprovider 2>&1)"
  echo "$out" | tail -3
  # Normalize each "FAILED <path>::<nodeid> - <msg>" line to tests/<file>::<nodeid>
  # (strip the FAILED prefix, any trailing " - msg", and any path before /tests/)
  # so the comparison is independent of cwd and the absolute repo path.
  failed_norm="$(printf '%s\n' "$out" \
    | grep '^FAILED ' \
    | sed -E 's/^FAILED +//; s/ +-.*$//; s#^.*/tests/#tests/#')"
  # Baseline set: non-comment, non-blank lines of known_stale_tests.txt.
  if [ -f "$KNOWN_STALE" ]; then
    baseline="$(grep -vE '^\s*(#|$)' "$KNOWN_STALE")"
  else
    baseline=""
  fi
  new_fail="$(comm -23 \
    <(printf '%s\n' "$failed_norm" | sort -u | grep -v '^$') \
    <(printf '%s\n' "$baseline"   | sort -u | grep -v '^$'))"
  if [ -n "$new_fail" ]; then
    echo ">> tests/ has NEW failures beyond the known-stale baseline:"
    printf '%s\n' "$new_fail"
    fail=1
  else
    echo "(tests/ clean apart from the documented stale baseline)"
  fi
fi

if [ "$fail" -eq 0 ]; then echo "== gates GREEN =="; else echo "== gates RED =="; fi
exit "$fail"
