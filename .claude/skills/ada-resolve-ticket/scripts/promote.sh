#!/usr/bin/env bash
# promote.sh -- auto-promote a frontend-only fix to STAGING.
# Serialised behind a global deploy mutex so only one promote runs at a time;
# rebases onto origin/main so it ships on top of others' work; re-runs the gates;
# re-classifies the ACTUAL diff as a hard safety backstop (must be frontend-only);
# then merges the branch to main, which triggers .github/workflows/deploy.yml to
# deploy cip-backend-staging + cip-frontend-staging.
#
#   promote.sh <branch> <worktree_path>
# Exit codes: 0 promoted | 3 rebase/gates failed (send back for revision)
#             4 could not get deploy lock | 5 merge to main failed (park for manual merge)
#             6 diff touches a must-park path (park; must never auto-deploy)
set -uo pipefail

BRANCH="${1:?branch}"; WT="${2:?worktree}"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${ADA_REPO:-$(cd "$SCRIPTS_DIR/../../../.." && pwd)}"
MAIN="main"
LOCKDIR="${ADA_DEPLOY_LOCK:-/tmp/ada-deploy.lock.d}"
LOCK_TTL="${ADA_DEPLOY_LOCK_TTL:-1800}"

# --- portable global deploy mutex (macOS ships no flock binary; mkdir is atomic) ---
acquired=0; i=0
while [ "$i" -lt 900 ]; do
  if mkdir "$LOCKDIR" 2>/dev/null; then
    printf '%s %s\n' "$$" "$(date +%s)" > "$LOCKDIR/owner"
    acquired=1; break
  fi
  # reap a stale lock whose holder pid is dead or that has exceeded the TTL
  if [ -f "$LOCKDIR/owner" ]; then
    opid=""; ots=""
    read -r opid ots < "$LOCKDIR/owner" 2>/dev/null || true
    now="$(date +%s)"; stale=0
    [ -n "$opid" ] && ! kill -0 "$opid" 2>/dev/null && stale=1
    [ -n "$ots" ] && [ $((now - ots)) -gt "$LOCK_TTL" ] && stale=1
    if [ "$stale" -eq 1 ]; then rm -rf "$LOCKDIR" 2>/dev/null || true; continue; fi
  fi
  sleep 1; i=$((i + 1))
done
[ "$acquired" -eq 1 ] || { echo ">> could not acquire deploy lock ($LOCKDIR)"; exit 4; }
trap 'rm -rf "$LOCKDIR" 2>/dev/null || true' EXIT INT TERM

echo "== re-sync $BRANCH onto origin/$MAIN =="
git -C "$REPO" fetch --quiet origin
if ! git -C "$WT" rebase "origin/$MAIN"; then
  git -C "$WT" rebase --abort 2>/dev/null || true
  echo ">> REBASE CONFLICT onto origin/$MAIN -- send back for revision"; exit 3
fi

echo "== re-run gates after re-sync =="
"$SCRIPTS_DIR/gates.sh" "$WT" || { echo ">> gates failed after re-sync"; exit 3; }

echo "== safety backstop: re-classify the actual diff (must be frontend-only) =="
if ! git -C "$WT" diff --name-only "origin/$MAIN...HEAD" | python3 "$SCRIPTS_DIR/area.py" --stdin >/dev/null; then
  echo ">> diff touches a must-park path (BigQuery/ingestion/schema) -- refusing to auto-promote"; exit 6
fi

HEAD_SHA="$(git -C "$WT" rev-parse HEAD)"
echo "== deliver $BRANCH -> $MAIN =="
git -C "$WT" push --force-with-lease origin "$BRANCH" || { echo ">> branch push failed"; exit 5; }

merged=0
if command -v gh >/dev/null 2>&1; then
  ( cd "$WT" && gh pr create --base "$MAIN" --head "$BRANCH" \
      --title "ADA resolver: $BRANCH" \
      --body "Automated frontend-only fix; gates green; deploys to staging on merge." \
      >/dev/null 2>&1 || true )
  if ( cd "$WT" && gh pr merge "$BRANCH" --merge --admin --delete-branch=false >/dev/null 2>&1 ); then
    merged=1
  fi
fi
if [ "$merged" -eq 0 ]; then
  # fallback: direct push to main (fails cleanly if main is protection-locked)
  if git -C "$WT" push origin "HEAD:$MAIN" 2>/dev/null; then merged=1; fi
fi
[ "$merged" -eq 1 ] || { echo ">> could not merge to $MAIN (branch protection / gh auth?) -- park for manual merge"; exit 5; }

echo "PROMOTED $HEAD_SHA -> staging (deploy.yml will roll cip-*-staging in ~7 min)"
exit 0
