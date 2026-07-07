"""Per-line campaign type classification for the diagnostic engine.

Mixed campaigns are the norm at Point Blank: a single project often contains
both awareness/engagement lines AND conversion/retargeting lines. The engine
must classify each media plan line (and each platform-data row) independently,
then run both diagnostic paths on partitioned data subsets.

Design:
    - `classify_line(line)` → CampaignType for one media plan line
    - `classify_objective_string(s)` → CampaignType for a raw objective string
    - `classify_campaign_name(s)` → CampaignType for a campaign_name (fallback when
      campaign_objective is missing, e.g. fact_adset_daily)
    - `is_persuasion(line)` / `is_conversion(line)` → boolean predicates
    - Conservative default: anything ambiguous → PERSUASION. This matches the
      existing `objective_classifier.classify_objective()` semantics and keeps the
      signal set broader rather than narrower for unclassifiable lines.

Wraps `backend.services.objective_classifier.classify_objective()` so that the
keyword lists (retargeting, remarketing, leads, etc.) stay in one place. We map
its string output {"awareness", "conversion", "mixed"} to `CampaignType`:
    awareness  → PERSUASION
    conversion → CONVERSION
    mixed      → PERSUASION   (see conservative default above)
"""

from __future__ import annotations

import re

from backend.services.diagnostics.models import CampaignType, MediaPlanLine
from backend.services.objective_classifier import classify_objective


# Channel categories that are inherently direct-response, independent of the
# objective/name keywords. A Google Search RSA is a conversion channel even when
# its line objective is just "Search"; grading it on persuasion rules (e.g. the
# D5 "delivering in bursts" signal) is meaningless. `channel_category` is the
# reliable per-line signal — `platform_id` ("google_ads") can't tell Search from
# Display. Values come from `media_plan_sync._channel_category()`.
CONVERSION_CHANNELS = {"search"}

# Text fallback for call sites that lack `channel_category` (daily/adset rows):
# the word "search" in an objective/name marks a conversion channel. `\b` avoids
# matching "research".
_SEARCH_RE = re.compile(r"\bsearch\b", re.IGNORECASE)


def _is_conversion_channel(channel_category: str | None) -> bool:
    return bool(channel_category) and channel_category.strip().lower() in CONVERSION_CHANNELS


# ── Primary API ────────────────────────────────────────────────────


def classify_line(line: MediaPlanLine) -> CampaignType:
    """Classify a single media plan line.

    Search is a direct-response channel regardless of the objective keyword, so
    a line whose `channel_category` is "Search" is CONVERSION even when its
    objective is bare (e.g. "Search"). Otherwise falls back to the shared
    `objective_classifier` keyword logic: if the line's objective is ambiguous
    or missing, the classifier's "mixed" result is coerced to PERSUASION
    (conservative default — persuasion signals don't require conversion tracking
    to run, while conversion signals do).
    """
    if _is_conversion_channel(line.channel_category):
        return CampaignType.CONVERSION
    obj = (line.objective or "").strip() or None
    return classify_objective_string(obj)


def classify_objective_string(
    objective: str | None,
    campaign_name: str | None = None,
) -> CampaignType:
    """Classify a raw objective string (optionally with campaign_name fallback).

    Suitable for classifying `fact_digital_daily.campaign_objective` rows and
    `media_plan_lines.objective` values. Falls back to `campaign_name` keyword
    matching when `objective` is missing — mirrors `classify_objective()`.

    Daily/adset call sites don't carry `channel_category`, so Search is detected
    here from the text: an objective/name containing the word "search" is a
    conversion channel (keeps mixed-project daily rows consistent with the
    per-line partition, which uses `channel_category`).
    """
    if (objective and _SEARCH_RE.search(objective)) or (
        campaign_name and _SEARCH_RE.search(campaign_name)
    ):
        return CampaignType.CONVERSION
    label = classify_objective(objective, campaign_name)
    if label == "conversion":
        return CampaignType.CONVERSION
    # "awareness" and "mixed" both fall through to PERSUASION.
    # "mixed" at the *single-row* grain is almost always a campaign whose
    # name contains both kinds of keywords (e.g. "Lead Gen Video Views");
    # treating it as persuasion keeps the persuasion signals running without
    # silently attributing it to conversion, which has stricter data needs.
    return CampaignType.PERSUASION


def classify_campaign_name(campaign_name: str | None) -> CampaignType:
    """Classify a platform-reported `campaign_name` (no objective available).

    Used for `fact_adset_daily` rows, which don't carry `campaign_objective`.
    Equivalent to `classify_objective_string(None, campaign_name)`.
    """
    return classify_objective_string(None, campaign_name)


# ── Boolean predicates (convenience) ──────────────────────────────


def is_conversion(line: MediaPlanLine) -> bool:
    """True if this media plan line is conversion-oriented."""
    return classify_line(line) == CampaignType.CONVERSION


def is_persuasion(line: MediaPlanLine) -> bool:
    """True if this media plan line is persuasion-oriented (default)."""
    return classify_line(line) == CampaignType.PERSUASION


# ── Utility: split a line list by type ────────────────────────────


def partition_lines(
    lines: list[MediaPlanLine],
) -> dict[CampaignType, list[MediaPlanLine]]:
    """Split a list of media plan lines into {persuasion: [...], conversion: [...]}.

    Both keys are always present in the returned dict (value may be an empty list).
    Useful as the first step in the mixed-campaign engine flow.
    """
    buckets: dict[CampaignType, list[MediaPlanLine]] = {
        CampaignType.PERSUASION: [],
        CampaignType.CONVERSION: [],
    }
    for line in lines:
        buckets[classify_line(line)].append(line)
    return buckets
