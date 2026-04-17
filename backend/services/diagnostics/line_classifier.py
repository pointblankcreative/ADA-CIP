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

from backend.services.diagnostics.models import CampaignType, MediaPlanLine
from backend.services.objective_classifier import classify_objective


# ── Primary API ────────────────────────────────────────────────────


def classify_line(line: MediaPlanLine) -> CampaignType:
    """Classify a single media plan line by its `objective` field.

    Uses the shared `objective_classifier` keyword logic. If the line's
    objective is ambiguous or missing, the classifier's "mixed" result is
    coerced to PERSUASION (conservative default — persuasion signals don't
    require conversion tracking to run, while conversion signals do).
    """
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
    """
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
