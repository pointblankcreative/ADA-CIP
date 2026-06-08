"""Tests for Slack alert formatting.

Covers the launch-week alert redesign:
  1. all content sits inside the coloured attachment (no top-level `text`)
  2. no leading severity emoji
  3. project header shows code + client + project name
  4. headline carries channel + line name + line code
"""

import json
from unittest.mock import MagicMock, patch

from backend.services import slack_alerts as sa


def _all_text(blocks):
    out = []
    for b in blocks:
        if b.get("type") == "section":
            out.append(b["text"]["text"])
        elif b.get("type") == "context":
            out.extend(e["text"] for e in b["elements"])
    return "\n".join(out)


LINE_7 = {
    "line_id": "plan-26018-line-006",
    "line_code": "#7",
    "channel_category": "Digital",
    "site_network": "Google Search Ads",
    "audience_name": "SEARCH",
}
PROJ = {"26018": {"project_code": "26018",
                  "project_name": "Pre-Bargaining Flight 1",
                  "client_name": "CAPE"}}


def _alert(**kw):
    base = {
        "alert_id": "a1", "project_code": "26018", "alert_type": "budget_exceeded",
        "severity": "critical", "title": "Budget exceeded — #7",
        "message": "Actual spend $250.00 is $11.13 (4.7%) over the $238.87 budget",
        "metadata": json.dumps({"line_id": "plan-26018-line-006", "overage": 11.13}),
    }
    base.update(kw)
    return base


class TestAlertHeadline:
    def test_includes_channel_line_name_and_code(self):
        assert sa._alert_headline(_alert(), LINE_7) == \
            "Budget exceeded - Google Search Ads - SEARCH (#7)"

    def test_falls_back_to_title_without_line(self):
        assert sa._alert_headline(_alert(), None) == "Budget exceeded — #7"

    def test_channel_collapses_newlines(self):
        line = dict(LINE_7, site_network="Meta\nFacebook & Instagram",
                    line_code="#1", audience_name="26018 CAPE Conversion Retargeting EN")
        h = sa._alert_headline(_alert(), line)
        assert "Meta Facebook & Instagram" in h
        assert "\n" not in h


class TestFormatAlertBlocks:
    def _blocks(self, alert=None):
        return sa._format_alert_blocks(alert or _alert(), PROJ,
                                       {"plan-26018-line-006": LINE_7})

    def test_no_severity_emoji_anywhere(self):
        text = _all_text(self._blocks())
        assert ":rotating_light:" not in text
        assert ":warning:" not in text
        assert ":bell:" not in text

    def test_headline_project_label_and_body_present(self):
        text = _all_text(self._blocks())
        assert "*Budget exceeded - Google Search Ads - SEARCH (#7)*" in text
        assert "26018 - CAPE - Pre-Bargaining Flight 1" in text
        assert "$11.13" in text

    def test_system_alert_has_no_project_label(self):
        blocks = sa._format_alert_blocks(
            {"alert_id": "s", "project_code": "__system__", "alert_type": "data_stale",
             "severity": "warning", "title": "Stale data: meta",
             "message": "No data loaded for meta in 40h"},
            PROJ, {},
        )
        text = _all_text(blocks)
        assert "__system__" not in text
        assert "Stale data" in text


class TestDispatchNoTopLevelText:
    def test_posts_attachment_only_no_outside_text(self):
        captured = {}

        def fake_post(**kwargs):
            captured.update(kwargs)
            return {"ts": "1.1"}

        client = MagicMock()
        client.chat_postMessage.side_effect = fake_post

        def fake_run_query(sql, params=None):
            if "media_plan_lines" in sql:
                return [LINE_7]
            if "dim_projects" in sql and "client_name" in sql:
                return [dict(PROJ["26018"])]
            if "slack_channel_id" in sql:
                return []
            if "alert_type, severity, title, message" in sql:
                return [_alert()]
            return []  # _mark_sent UPDATE

        with patch.object(sa, "_get_slack_client", return_value=client), \
             patch.object(sa.bq, "run_query", side_effect=fake_run_query), \
             patch.object(sa.bq, "array_param", return_value=MagicMock()), \
             patch.object(sa.bq, "string_param", return_value=MagicMock()):
            result = sa.dispatch_unsent_alerts()

        assert result["dispatched"] == 1
        # Item 1: nothing renders outside the coloured border → no top-level text.
        assert "text" not in captured
        attach = captured["attachments"][0]
        assert attach["color"] == sa.SEVERITY_COLORS["critical"]
        assert attach["fallback"]  # notification preview still set
