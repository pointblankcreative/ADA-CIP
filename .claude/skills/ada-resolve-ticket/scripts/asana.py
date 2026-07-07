#!/usr/bin/env python3
"""asana.py -- minimal Asana REST helpers (stdlib only) for the resolver.

Deterministic writes (claim marker, park comment, section move) go through here
rather than an MCP so the skill works in a headless `claude -p` run with no MCP
loaded. Auth is the ASANA_PAT env var -- the same token the repo's deploy.sh and
list_ingest service already use.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


class Asana:
    def __init__(self, pat, base_url="https://app.asana.com/api/1.0"):
        self.pat = pat
        self.base = base_url.rstrip("/")

    def _req(self, method, path, params=None, body=None):
        url = self.base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        data = json.dumps({"data": body}).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {self.pat}")
        req.add_header("Accept", "application/json")
        if data:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")
            raise RuntimeError(f"Asana {method} {path} -> {e.code}: {detail}") from None

    def project_tasks(self, project_gid, opt_fields):
        """Every task in a project (paginated), with the requested opt_fields."""
        out, offset = [], None
        while True:
            params = {"opt_fields": opt_fields, "limit": 100}
            if offset:
                params["offset"] = offset
            resp = self._req("GET", f"/projects/{project_gid}/tasks", params=params)
            out.extend(resp.get("data", []))
            offset = (resp.get("next_page") or {}).get("offset")
            if not offset:
                return out

    def set_enum_field(self, task_gid, field_gid, option_gid):
        return self._req(
            "PUT", f"/tasks/{task_gid}",
            body={"custom_fields": {field_gid: option_gid}},
        )

    def add_comment(self, task_gid, text):
        return self._req("POST", f"/tasks/{task_gid}/stories", body={"text": text})

    def move_to_section(self, section_gid, task_gid):
        return self._req(
            "POST", f"/sections/{section_gid}/addTask", body={"task": task_gid}
        )

    def complete_task(self, task_gid, completed=True):
        return self._req("PUT", f"/tasks/{task_gid}", body={"completed": completed})
