#!/usr/bin/env python3
"""handoff.py -- Asana + ledger state transitions at the end of a resolver run.

Every subcommand takes --gid. Long messages are read from a file (--message-file)
to avoid shell-quoting multi-line park comments.

  park    --gid G --message-file F [--worktree W --branch B]
                                      Persist WIP (commit + push the branch) so a
                                      fresh session can resume, then Ready For ->
                                      Frazer, Status stays In progress, post the
                                      question, ledger=parked (area stays reserved).
  staged  --gid G [--message-file F]  Move to 'Ready In Staging', Status=Completed,
                                      Ready For -> Frazer (only your prod promote is
                                      left), drop the ledger claim, post a summary.
  comment --gid G --message-file F    Post a comment only.
  release --gid G                     Drop the ledger claim (clean abort / recovery).
"""
from __future__ import annotations

import argparse
import subprocess
import sys

import common
from asana import Asana
from ledger import Ledger


def _msg(path):
    if not path:
        return ""
    with open(path) as f:
        return f.read()


def _persist_wip(worktree, branch):
    """Commit and push any work in progress BEFORE the worktree can be removed, so a
    fresh session can always resume from the branch. Raises if the push fails, so the
    caller must NOT remove the worktree in that case."""
    subprocess.run(["git", "-C", worktree, "add", "-A"], check=False)
    subprocess.run(["git", "-C", worktree, "commit", "-m", "WIP: parked for review"],
                   check=False)  # no-op if there is nothing to commit
    push = subprocess.run(["git", "-C", worktree, "push", "-u", "origin", branch],
                          check=False)
    if push.returncode != 0:
        raise SystemExit(
            "push of parked WIP failed; NOT safe to remove the worktree -- resolve manually"
        )


def main(argv=None):
    ap = argparse.ArgumentParser(description="ADA resolver handoff transitions")
    ap.add_argument("cmd", choices=["park", "staged", "comment", "release"])
    ap.add_argument("--gid", required=True)
    ap.add_argument("--message-file")
    ap.add_argument("--worktree")
    ap.add_argument("--branch")
    args = ap.parse_args(argv)

    cfg = common.load_config()
    fields = cfg["asana"]["fields"]
    secs = cfg["asana"]["sections"]
    a = Asana(common.asana_pat(cfg), cfg["asana"]["base_url"])
    rf, st = fields["ready_for"], fields["status"]

    if args.cmd == "comment":
        a.add_comment(args.gid, _msg(args.message_file))
        print("commented")

    elif args.cmd == "park":
        if args.worktree and args.branch:
            _persist_wip(args.worktree, args.branch)  # persist WIP before any removal
        text = _msg(args.message_file) or (
            "Parked for Frazer: a decision is needed before this can continue. "
            "See the branch for work in progress."
        )
        a.add_comment(args.gid, text)
        a.set_enum_field(args.gid, rf["gid"], rf["frazer"])  # your court now
        # Status intentionally left In progress: WIP exists on the branch.
        with Ledger.locked() as led:
            if led.get(args.gid):
                led.set_status(args.gid, "parked")  # keeps the area reserved
        print("parked")

    elif args.cmd == "staged":
        if args.message_file:
            a.add_comment(args.gid, _msg(args.message_file))
        a.move_to_section(secs["ready_in_staging"], args.gid)
        a.set_enum_field(args.gid, st["gid"], st["completed"])
        a.set_enum_field(args.gid, rf["gid"], rf["frazer"])  # only prod promote remains
        with Ledger.locked() as led:
            led.remove(args.gid)
        print("staged")

    elif args.cmd == "release":
        with Ledger.locked() as led:
            led.remove(args.gid)
        print("released")

    return 0


if __name__ == "__main__":
    sys.exit(main())
