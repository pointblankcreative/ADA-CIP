#!/usr/bin/env bash
# worktree.sh -- create or remove a per-session git worktree for the resolver.
# Physical isolation: each session edits/commits in its own directory + branch,
# sharing one .git store, so concurrent sessions cannot corrupt each other's tree.
#
#   worktree.sh add <branch> <worktree_path> [<base_ref>]
#   worktree.sh remove <worktree_path>
set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${ADA_REPO:-$(cd "$SCRIPTS_DIR/../../../.." && pwd)}"

cmd="${1:-}"
case "$cmd" in
  add)
    branch="${2:?branch}"; wt="${3:?worktree path}"; base="${4:-origin/main}"
    git -C "$REPO" worktree prune 2>/dev/null || true   # clear stale registrations from crashed sessions
    git -C "$REPO" fetch --quiet origin || true
    mkdir -p "$(dirname "$wt")"
    if git -C "$REPO" show-ref --verify --quiet "refs/heads/$branch"; then
      # branch already exists (resume): attach a fresh worktree to it
      git -C "$REPO" worktree add "$wt" "$branch"
    else
      git -C "$REPO" worktree add -b "$branch" "$wt" "$base"
    fi
    # frontend deps: symlink the primary node_modules (fast) when the lockfile
    # matches; otherwise install cleanly.
    if [ -d "$REPO/frontend/node_modules" ] && [ ! -e "$wt/frontend/node_modules" ]; then
      if cmp -s "$REPO/frontend/package-lock.json" "$wt/frontend/package-lock.json"; then
        ln -s "$REPO/frontend/node_modules" "$wt/frontend/node_modules"
      else
        ( cd "$wt/frontend" && npm ci --silent )
      fi
    fi
    echo "$wt"
    ;;
  remove)
    wt="${2:?worktree path}"
    git -C "$REPO" worktree remove --force "$wt" 2>/dev/null || rm -rf "$wt"
    git -C "$REPO" worktree prune || true
    ;;
  *)
    echo "usage: worktree.sh add <branch> <path> [base_ref] | remove <path>" >&2
    exit 2
    ;;
esac
