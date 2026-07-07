#!/usr/bin/env python3
"""
ledger.py -- shared claim ledger + atomic locking for the ADA ticket-resolver harness.

Every resolver session records what it is working on here so that concurrent
sessions never (a) grab the same ticket or (b) touch the same area of the
codebase. The file lives OUTSIDE the git repo (default ~/.ada-harness/active-work.json)
so it is coordination state, not versioned code.

Concurrency model
-----------------
- One exclusive file lock (flock) guards every read-modify-write of the ledger,
  so "pick the next free ticket and claim it" is atomic. No two sessions can
  claim the same ticket.
- A claim reserves one or more code "areas" (path prefixes or globs). A new
  claim is refused if its area overlaps any *blocking* claim, so two live
  sessions never edit the same module. Disjoint areas run in parallel.
- "parked" claims (waiting on Frazer) keep reserving their area, because a WIP
  branch exists and resuming onto a changed base is exactly the collision we are
  preventing. Only the parked ticket itself may re-claim it (a resume).
- Crashed sessions self-heal: a "claimed" record whose pid is dead (same host)
  or older than ADA_LEDGER_TTL is garbage-collected on the next lock.

Library use
-----------
    from ledger import Ledger
    with Ledger.locked() as led:
        claim = led.claim_if_free(gid, title, area, branch, worktree)
        if claim is None:
            ...   # someone else owns it, or the area is busy -> try next ticket

CLI (glance at what is running / housekeeping)
----------------------------------------------
    python ledger.py list         # status board of active + parked work
    python ledger.py gc           # reap crashed sessions
    python ledger.py remove <gid> # drop a record by ticket id
"""
from __future__ import annotations

import contextlib
import errno
import fcntl
import fnmatch
import json
import os
import socket
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_LEDGER = Path(
    os.environ.get("ADA_LEDGER", Path.home() / ".ada-harness" / "active-work.json")
)
# crashed-session self-heal window (a "claimed" record older than this is reaped)
STALE_TTL_SECONDS = int(os.environ.get("ADA_LEDGER_TTL", 6 * 3600))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pid_alive(pid: int, host: str) -> bool:
    """Best-effort liveness. On another host we cannot check, so assume alive
    and rely on the TTL. Locally, signal 0 probes the pid."""
    if host and host != socket.gethostname():
        return True
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as e:
        return e.errno == errno.EPERM  # exists but owned by someone else
    return True


def _norm(p: str) -> str:
    """Normalize a claimed path/glob to a posix relative form: no backslashes,
    no leading ./, no trailing /."""
    p = p.strip().replace("\\", "/")
    while p.startswith("./"):
        p = p[2:]
    while p.endswith("/"):
        p = p[:-1]
    return p


def _is_glob(p: str) -> bool:
    return any(ch in p for ch in "*?[")


def _glob_hits_path_or_parents(glob_pat: str, path: str) -> bool:
    """True if glob matches path or any of its parent dirs (so 'frontend/**'
    reserves everything under frontend, and 'backend/services/*' overlaps
    'backend/services/media_plan_sync.py')."""
    parts = path.split("/")
    for i in range(1, len(parts) + 1):
        if fnmatch.fnmatch("/".join(parts[:i]), glob_pat):
            return True
    return False


def paths_overlap(a: str, b: str) -> bool:
    """Two area entries overlap if either is a prefix of the other on a path
    boundary, or (for glob entries) either matches the other's literal prefix."""
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return False
    if _is_glob(a) or _is_glob(b):
        return (
            fnmatch.fnmatch(b, a)
            or fnmatch.fnmatch(a, b)
            or _glob_hits_path_or_parents(a, b)
            or _glob_hits_path_or_parents(b, a)
        )
    if a == b:
        return True
    return a.startswith(b + "/") or b.startswith(a + "/")


def areas_overlap(area_a, area_b) -> bool:
    return any(paths_overlap(x, y) for x in area_a for y in area_b)


@dataclass
class Claim:
    ticket_gid: str
    title: str
    area: list
    branch: str
    worktree: str
    status: str = "claimed"  # claimed | parked
    pid: int = field(default_factory=os.getpid)
    host: str = field(default_factory=socket.gethostname)
    claimed_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)


class Ledger:
    def __init__(self, path=DEFAULT_LEDGER, _fh=None):
        self.path = Path(path)
        self._fh = _fh
        self.data = {"claims": []}

    # ---- locking -------------------------------------------------------
    @classmethod
    @contextlib.contextmanager
    def locked(cls, path=DEFAULT_LEDGER, timeout=30.0):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = path.with_suffix(path.suffix + ".lock")
        fh = open(lock_path, "w")
        deadline = time.time() + timeout
        while True:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.time() > deadline:
                    fh.close()
                    raise TimeoutError(
                        f"could not lock {lock_path} within {timeout}s"
                    )
                time.sleep(0.2)
        led = cls(path, _fh=fh)
        led._load()
        led.gc_stale()
        try:
            yield led
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()

    # ---- io ------------------------------------------------------------
    def _load(self):
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text() or '{"claims": []}')
            except json.JSONDecodeError:
                self.data = {"claims": []}
        else:
            self.data = {"claims": []}
        self.data.setdefault("claims", [])

    def _save(self):
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2))
        tmp.replace(self.path)  # atomic on POSIX

    # ---- queries -------------------------------------------------------
    def running(self):
        """Claims with a live session actively working (status == claimed)."""
        return [c for c in self.data["claims"] if c.get("status") == "claimed"]

    def _blocking(self):
        """Claims that reserve their area against new claims (claimed OR parked)."""
        return [
            c for c in self.data["claims"] if c.get("status") in ("claimed", "parked")
        ]

    def get(self, gid):
        return next(
            (c for c in self.data["claims"] if c["ticket_gid"] == gid), None
        )

    def conflicts(self, area, exclude_gid=None):
        out = []
        for c in self._blocking():
            if exclude_gid and c["ticket_gid"] == exclude_gid:
                continue
            if areas_overlap(area, c.get("area", [])):
                out.append(c)
        return out

    # ---- mutations -----------------------------------------------------
    def can_claim(self, ticket_gid, area) -> bool:
        """True if this ticket could be claimed right now: not actively owned by a
        live session, and its area free of blocking claims. Pure -- safe to peek
        without mutating the ledger (used by claim.py --peek for the drain loop)."""
        area = [a for a in area if _norm(a)]
        existing = self.get(ticket_gid)
        if existing and existing.get("status") == "claimed":
            return False
        return not self.conflicts(area, exclude_gid=ticket_gid)

    def claim_if_free(self, ticket_gid, title, area, branch, worktree):
        """Claim a ticket if nobody live owns it and its area is free.

        Returns a Claim on success, or None if the ticket is actively owned
        ('claimed') by another session or its area overlaps a blocking claim.
        A 'parked' record for the same ticket is treated as a resume and is
        allowed to re-claim (its own reservation does not block it)."""
        area = [a for a in area if _norm(a)]
        if not self.can_claim(ticket_gid, area):
            return None
        claim = Claim(
            ticket_gid=ticket_gid,
            title=title,
            area=list(area),
            branch=branch,
            worktree=worktree,
        )
        # replace any prior record for this ticket (e.g. a parked resume)
        self.data["claims"] = [
            c for c in self.data["claims"] if c["ticket_gid"] != ticket_gid
        ]
        self.data["claims"].append(asdict(claim))
        self._save()
        return claim

    def set_status(self, gid, status):
        c = self.get(gid)
        if not c:
            return False
        c["status"] = status
        c["updated_at"] = _now_iso()
        self._save()
        return True

    def remove(self, gid):
        before = len(self.data["claims"])
        self.data["claims"] = [
            c for c in self.data["claims"] if c["ticket_gid"] != gid
        ]
        if len(self.data["claims"]) != before:
            self._save()
            return True
        return False

    def gc_stale(self):
        """Reap 'claimed' records whose session crashed (dead pid on this host)
        or that have gone stale past the TTL. Never touches 'parked' records --
        those are intentionally waiting on a human."""
        now = time.time()
        keep, changed = [], False
        for c in self.data["claims"]:
            if c.get("status") != "claimed":
                keep.append(c)
                continue
            try:
                ts = datetime.fromisoformat(
                    c.get("updated_at") or c["claimed_at"]
                ).timestamp()
            except Exception:
                ts = now
            dead = not _pid_alive(int(c.get("pid", -1)), c.get("host", ""))
            if (dead and (now - ts) > 60) or ((now - ts) > STALE_TTL_SECONDS):
                changed = True
                continue
            keep.append(c)
        if changed:
            self.data["claims"] = keep
            self._save()


# ---- CLI ---------------------------------------------------------------
def _cli(argv) -> int:
    cmd = argv[1] if len(argv) > 1 else "list"
    with Ledger.locked() as led:
        if cmd == "list":
            claims = led.data["claims"]
            if not claims:
                print("(ledger empty)")
                return 0
            for c in claims:
                print(
                    f"[{c['status']:>7}] {c['ticket_gid']:<18} {c.get('branch',''):<26} "
                    f"pid={c.get('pid')}  {c.get('updated_at','')}"
                )
                print(f"          area={c.get('area')}")
                print(f"          {c.get('title','')}")
            return 0
        if cmd == "gc":
            led.gc_stale()
            print("gc complete")
            return 0
        if cmd == "remove" and len(argv) > 2:
            print("removed" if led.remove(argv[2]) else "not found")
            return 0
        print(__doc__)
        return 2


if __name__ == "__main__":
    sys.exit(_cli(sys.argv))
