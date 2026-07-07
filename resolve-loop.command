#!/usr/bin/env bash
# resolve-loop.command — drain the ADA "Ready For: Agent" queue, one fresh Claude
# Code session per ticket. Double-click in Finder, or run:  ./resolve-loop.command [max]
#
# Each session resolves exactly ONE ticket (see .claude/skills/ada-resolve-ticket),
# so context never overflows; this loop just keeps firing sessions until the queue
# reports NONE — or until the optional max ticket count is reached. Sessions are
# concurrency-safe (atomic claim ledger + per-ticket worktrees), so you can run
# more than one of these at once.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL="$REPO/.claude/skills/ada-resolve-ticket"
MAX="${1:-0}"   # 0 = no cap
PROMPT="Use the ada-resolve-ticket skill to resolve the next ADA ticket."

cd "$REPO"

if [ -z "${ASANA_PAT:-}" ]; then
  echo "ASANA_PAT is not set — export it (the token deploy.sh uses) and re-run." >&2
  exit 1
fi
if ! command -v claude >/dev/null 2>&1; then
  echo "claude CLI not found on PATH." >&2
  exit 1
fi

n=0
while :; do
  # Cheap stop condition: --peek reports AVAILABLE / NONE without claiming.
  status="$(python3 "$SKILL/scripts/claim.py" --peek 2>/dev/null || true)"
  if [ "$status" != "AVAILABLE" ]; then
    echo "== queue drained (peek: ${status:-NONE}) — stopping after $n ticket(s). =="
    break
  fi
  n=$((n + 1))
  echo "== [$n] claimable ticket found — launching a fresh resolver session =="
  claude -p "$PROMPT" || echo ">> session #$n exited non-zero; continuing."
  if [ "$MAX" -gt 0 ] && [ "$n" -ge "$MAX" ]; then
    echo "== reached max=$MAX ticket(s) — stopping. =="
    break
  fi
done
