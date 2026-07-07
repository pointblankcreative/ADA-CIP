# Voice/tone guard for UAT #6 (advisory bodies). Asserts the rewritten
# A5 fatigue + F3 discovery bands render cleanly and carry no bare commands.
from __future__ import annotations

from backend.services.diagnostics.persuasion.attention import (
    A5_DRIVER_FREQUENCY,
    A5_DRIVER_IDEA,
    A5_HOLDING_ELEVATED_SUFFIX,
    A5_HOLDING_SATURATED,
    A5_MESSAGES,
)
from backend.services.diagnostics.conversion.funnel import F3_MESSAGES
from backend.services.diagnostics.models import StatusBand

BANNED = [
    "Swap it now",
    "Move the form up the page",
    "Have a refresh ready",
    "Line up a refresh",
    "Refresh the audience",
    "Cap the frequency",
]


def _render_a5(band: str) -> str:
    return A5_MESSAGES[band].format(
        slope=-38.0, days=7, worst_suffix="; StackAdapt is fading fastest at -44.9%/day"
    )


def test_a5_bands_render_without_error():
    for band in ("NONE", "EARLY", "MODERATE", "SEVERE"):
        text = _render_a5(band)
        assert text and "{" not in text


def test_a5_has_no_bare_commands():
    for band in ("EARLY", "MODERATE", "SEVERE"):
        text = _render_a5(band)
        for phrase in BANNED:
            assert phrase not in text, f"{band} still contains banned phrase: {phrase!r}"


def test_a5_severe_stays_observational():
    text = _render_a5("SEVERE")
    assert "looks burnt out" in text
    assert "Swap it now" not in text


# ── A5 frequency overlay copy (AI-044) ──────────────────────────────


def _render_a5_overlay() -> list[str]:
    """Every frequency-overlay string, rendered with realistic values."""
    return [
        A5_HOLDING_SATURATED.format(
            days=7, freq=8.2,
            worst_suffix="; StackAdapt is fading fastest at -44.9%/day",
        ),
        A5_HOLDING_ELEVATED_SUFFIX.format(freq=6.4),
        A5_DRIVER_FREQUENCY.format(freq=8.2),
        A5_DRIVER_IDEA.format(freq=2.1),
    ]


def test_a5_overlay_strings_render_without_error():
    for text in _render_a5_overlay():
        assert text and "{" not in text


def test_a5_overlay_strings_are_observational():
    for text in _render_a5_overlay():
        for phrase in BANNED:
            assert phrase not in text, f"overlay contains banned phrase: {phrase!r}"


def test_a5_overlay_names_the_driver():
    # Frazer's acceptance bar: the read must distinguish overexposure from a
    # spent idea.
    assert "overexposure" in A5_DRIVER_IDEA
    assert "frequency wearing the audience out" in A5_DRIVER_FREQUENCY
    # The saturated-but-holding body must not still claim the creative is fresh.
    saturated = A5_HOLDING_SATURATED.format(days=7, freq=8.2, worst_suffix="")
    assert "fresh creative" in saturated  # "…rather than a fresh creative."
    assert "isn't wearing out" not in saturated


def test_f3_action_renders_and_is_observational():
    text = F3_MESSAGES[StatusBand.ACTION].format(discovery="1.1%", scroll_suffix="")
    assert "{" not in text
    assert "reach the form" in text
    assert "Move the form up the page" not in text
    assert "above the fold" not in text


def test_f3_action_does_not_contradict_scroll_absent_flag():
    # When scroll tracking is absent the body must not assert a cause it
    # cannot measure; it should still read cleanly with the flag appended.
    flag = " Note: GA4 isn't recording scroll events here, so this reads on form discovery alone."
    text = F3_MESSAGES[StatusBand.ACTION].format(discovery="1.1%", scroll_suffix=flag)
    assert "{" not in text
    assert "limiting factor" not in text
