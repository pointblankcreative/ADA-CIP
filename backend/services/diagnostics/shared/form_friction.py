"""Form Friction Score (FFS) computation.

FFS is a 0-100 score quantifying how much friction a landing page form
creates. Higher score = more friction = lower expected conversion rate.

Inputs come from a dashboard wizard — the user answers questions about
the form once per landing page, and the FFS is stored on the media plan line.

Components:
    1. Field count (0-30 points)
    2. Required fields ratio (0-15 points)
    3. Field type friction (0-20 points)
    4. Clicks to submit (0-10 points)
    5. Form position relative to fold on mobile (0-15 points)
    6. Autofill availability (0-5 points)
    7. Platform form discount (0-5 points reduction)
"""

from __future__ import annotations

from typing import Any


# ── Field type friction weights ─────────────────────────────────────

FIELD_TYPE_FRICTION = {
    "text_name": 1,
    "text_email": 1,
    "text_phone": 3,
    "text_address": 5,
    "text_freeform": 4,
    "dropdown_simple": 2,         # <5 options
    "dropdown_complex": 3,        # 5+ options
    "radio": 1,
    "checkbox": 1,
    "file_upload": 8,
    "date_picker": 3,
    "multi_step": 5,              # Per additional step
    "captcha": 2,
}


def compute_ffs(inputs: dict[str, Any]) -> float:
    """Compute Form Friction Score from wizard inputs.

    Args:
        inputs: Dict with keys:
            - field_count: int (total form fields)
            - required_fields: int (number of required fields)
            - field_types: list[str] (each field's type from FIELD_TYPE_FRICTION)
            - clicks_to_submit: int (clicks from LP load to form submit)
            - below_fold_mobile: bool (is the form below the fold on mobile?)
            - has_autofill: bool (does the form support browser autofill?)
            - is_platform_form: bool (e.g. Facebook Lead Ad, LinkedIn Lead Gen)

    Returns:
        FFS score 0-100 (higher = more friction).
    """
    score = 0.0

    # 1. Field count (0-30 points)
    field_count = inputs.get("field_count", 0)
    if field_count <= 3:
        score += 5
    elif field_count <= 6:
        score += 10
    elif field_count <= 10:
        score += 18
    elif field_count <= 15:
        score += 24
    else:
        score += 30

    # 2. Required fields ratio (0-15 points)
    required = inputs.get("required_fields", field_count)
    if field_count > 0:
        ratio = required / field_count
        score += ratio * 15

    # 3. Field type friction (0-20 points)
    field_types = inputs.get("field_types", [])
    type_friction = sum(
        FIELD_TYPE_FRICTION.get(ft, 2) for ft in field_types
    )
    # Normalize: max realistic type friction ~40 → scale to 20 points
    score += min(type_friction / 2, 20)

    # 4. Clicks to submit (0-10 points)
    clicks = inputs.get("clicks_to_submit", 1)
    score += min(clicks * 2.5, 10)

    # 5. Form position (0-15 points)
    if inputs.get("below_fold_mobile", False):
        score += 15

    # 6. Autofill discount (0-5 points reduction)
    if inputs.get("has_autofill", False):
        score -= 5

    # 7. Platform form discount (0-5 points reduction)
    # Facebook Lead Ads, LinkedIn Lead Gen forms have less friction
    if inputs.get("is_platform_form", False):
        score -= 5

    return max(0.0, min(100.0, round(score, 1)))


def ffs_to_cvr_adjustment(ffs_score: float) -> float:
    """Convert FFS to a CVR benchmark adjustment multiplier.

    Low friction (FFS < 20) → multiplier ~1.0 (no adjustment)
    Medium friction (FFS 20-50) → multiplier ~0.7
    High friction (FFS > 50) → multiplier ~0.4

    Returns:
        Multiplier in (0, 1] to apply to CVR benchmarks.
    """
    if ffs_score <= 10:
        return 1.0
    if ffs_score <= 30:
        return 1.0 - ((ffs_score - 10) / 20) * 0.2  # 1.0 → 0.8
    if ffs_score <= 60:
        return 0.8 - ((ffs_score - 30) / 30) * 0.3  # 0.8 → 0.5
    return max(0.3, 0.5 - ((ffs_score - 60) / 40) * 0.2)  # 0.5 → 0.3
