"""Audience Temperature Adjustment.

Adjusts CVR benchmarks based on audience warmth. A member-list campaign
targeting known supporters should convert at 3-5× the rate of a cold
prospecting campaign — the same CVR means very different things.

Temperature tiers:
    - member_list (warm): People who already know/support the org.
      Multiplier: 1.0 (benchmarks set for this tier)
    - retargeting (lukewarm): People who've engaged before.
      Multiplier: 0.60
    - prospecting (cold): Brand-new audience.
      Multiplier: 0.30

Usage:
    adjusted_benchmark = base_benchmark * get_temperature_multiplier(audience_type)
"""

from __future__ import annotations

from backend.services.diagnostics.models import AudienceType

# Multipliers relative to member_list baseline
TEMPERATURE_MULTIPLIERS: dict[AudienceType | str, float] = {
    AudienceType.MEMBER_LIST: 1.0,
    AudienceType.RETARGETING: 0.60,
    AudienceType.PROSPECTING: 0.30,
    # String fallbacks for flexibility
    "member_list": 1.0,
    "retargeting": 0.60,
    "prospecting": 0.30,
    "warm": 1.0,
    "lukewarm": 0.60,
    "cold": 0.30,
}

# Conservative default when audience type is unknown
DEFAULT_MULTIPLIER = 0.50


def get_temperature_multiplier(audience_type: AudienceType | str | None) -> float:
    """Get CVR benchmark multiplier for an audience temperature tier.

    Args:
        audience_type: The audience warmth classification.

    Returns:
        Multiplier in (0, 1]. Conservative default (0.50) if unknown.
    """
    if audience_type is None:
        return DEFAULT_MULTIPLIER
    return TEMPERATURE_MULTIPLIERS.get(audience_type, DEFAULT_MULTIPLIER)


def adjust_cvr_benchmark(
    base_cvr: float,
    audience_type: AudienceType | str | None,
) -> float:
    """Apply audience temperature adjustment to a CVR benchmark.

    Args:
        base_cvr: The baseline CVR benchmark (e.g. 0.03 for 3%).
        audience_type: The audience warmth classification.

    Returns:
        Adjusted CVR benchmark.
    """
    return base_cvr * get_temperature_multiplier(audience_type)
