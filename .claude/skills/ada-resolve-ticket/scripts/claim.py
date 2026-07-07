#!/usr/bin/env python3
"""claim.py -- pick the next Ready-For=Agent ticket this session may work, claim
it atomically (ledger + Asana Status=In progress), and print a JSON job
descriptor for the orchestrator. Prints {"status":"NO_TICKET"} when nothing is
claimable right now (queue empty, all busy, or all areas conflicting).

With --peek it reports "AVAILABLE" or "NONE" WITHOUT claiming, so the drain loop
has a cheap stop condition.

The claim is the ONLY step that decides who works what, so it holds the ledger
lock across "find a free ticket -> reserve its area -> mark it In progress". Two
sessions can never grab the same ticket or the same module.

Pure helpers (parse_declared_files, filter_candidates) are unit-tested; the
Asana + ledger writes run live.
"""
from __future__ import annotations

import json
import re
import sys

import area
import common
from asana import Asana
from ledger import Ledger

# repo-relative paths mentioned anywhere in a ticket body
PATH_RE = re.compile(
    r"(?:frontend|backend|ingestion|infrastructure|scripts|tests|docs)/"
    r"[A-Za-z0-9_.\-/]+\.[A-Za-z0-9]+"
)

# Explicit human "stop before staging" flags in a ticket body. Even when every
# touched file is auto-eligible, a ticket that asks for review must still park
# (learning from AI-044: the diagnostics engine is now auto-promotable, but its
# body said "needs Frazer review before STG" — that intent must win). Kept
# deliberately narrow so it only fires on a deliberate flag, not casual prose.
FORCE_PARK_RE = re.compile(
    r"(?:needs?\s+(?:frazer|your|human)\s+review\s+before\s+(?:stg|staging|promot)"
    r"|review\s+before\s+(?:stg|staging)"
    r"|do\s*not\s+auto[-\s]?promote"
    r"|don'?t\s+auto[-\s]?promote"
    r"|park\s+(?:this|for\s+review))",
    re.I,
)


def notes_force_park(notes: str) -> bool:
    """True if the ticket body explicitly asks to stop for review before STG."""
    return bool(FORCE_PARK_RE.search(notes or ""))


def parse_declared_files(notes: str):
    """Extract repo-relative file paths a ticket says it touches: any path-like
    token in the body, plus anything on an explicit 'Touches:' line."""
    notes = notes or ""
    found = set(PATH_RE.findall(notes))
    for line in notes.splitlines():
        m = re.match(r"\s*touches\s*:\s*(.+)$", line, re.I)
        if m:
            for tok in re.split(r"[,\s]+", m.group(1).strip()):
                tok = tok.strip("`'\"()")
                if "/" in tok and "." in tok:
                    found.add(tok)
    return sorted(found)


def _enum_gid(task, field_gid):
    for cf in task.get("custom_fields", []):
        if cf.get("gid") == field_gid:
            return (cf.get("enum_value") or {}).get("gid")
    return None


def filter_candidates(tasks, cfg):
    """Ready-For=Agent, not completed, Status != Completed. Ordered by priority
    (High first), then resumes (In progress) before fresh (Not started) so
    started work clears before new work piles up."""
    f = cfg["asana"]["fields"]
    agent = f["ready_for"]["agent"]
    st, pr = f["status"], f["priority"]
    prio_rank = {pr["high"]: 0, pr["medium"]: 1, pr["low"]: 2}
    status_rank = {st["in_progress"]: 0, st["not_started"]: 1}
    out = []
    for t in tasks:
        if t.get("completed"):
            continue
        if _enum_gid(t, f["ready_for"]["gid"]) != agent:
            continue
        if _enum_gid(t, st["gid"]) == st["completed"]:
            continue
        out.append(t)
    out.sort(
        key=lambda t: (
            prio_rank.get(_enum_gid(t, pr["gid"]), 3),
            status_rank.get(_enum_gid(t, st["gid"]), 2),
        )
    )
    return out


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    peek = "--peek" in argv

    cfg = common.load_config()
    a = Asana(common.asana_pat(cfg), cfg["asana"]["base_url"])
    opt = (
        "name,completed,notes,"
        "custom_fields.gid,custom_fields.name,"
        "custom_fields.enum_value.gid,custom_fields.enum_value.name"
    )
    tasks = a.project_tasks(cfg["asana"]["project_gid"], opt)
    candidates = filter_candidates(tasks, cfg)
    base = cfg["worktree_base"].rstrip("/")

    with Ledger.locked() as led:
        for t in candidates:
            gid = t["gid"]
            files = parse_declared_files(t.get("notes", ""))
            reserve = area.claim_area(files)

            if peek:
                if led.can_claim(gid, reserve):
                    print("AVAILABLE")
                    return 0
                continue

            branch = f"ada/ticket-{gid}"
            worktree = f"{base}/{gid}"
            claim = led.claim_if_free(gid, t.get("name", ""), reserve, branch, worktree)
            if not claim:
                continue  # actively owned, or its module is busy -> try next
            # we won the claim: mark the ticket In progress (idempotent)
            st = cfg["asana"]["fields"]["status"]
            a.set_enum_field(gid, st["gid"], st["in_progress"])
            decision, reasons = area.classify(files)
            # An explicit "review before staging" flag in the body overrides an
            # auto verdict — a human deliberately asked to be the gate.
            if decision == "auto" and notes_force_park(t.get("notes", "")):
                decision = "park"
                reasons = ["ticket body explicitly asks for review before "
                           "staging"] + reasons
            print(json.dumps({
                "status": "CLAIMED",
                "ticket_gid": gid,
                "title": t.get("name", ""),
                "notes": t.get("notes", ""),
                "declared_files": files,
                "reserved_area": reserve,
                "branch": branch,
                "worktree": worktree,
                "promote_decision": decision,   # 'auto' -> may ship to STG; 'park' -> stop for Frazer
                "promote_reasons": reasons,
            }, indent=2))
            return 0

    print("NONE" if peek else json.dumps({"status": "NO_TICKET"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
