"""Regression tests for _is_traditional_media word-boundary matching.

The keyword set includes "ooh" (short for out-of-home). The old substring test
(`kw in plower`) matched "ooh" inside "dooh", so a DOOH label — now a StackAdapt
self-serve *digital* buy — was mislabelled traditional. The match is now
word-boundary aware, so "ooh" only fires as a whole word.
"""

from backend.services.media_plan_sync import (
    _TRADITIONAL_KEYWORDS,
    _is_traditional_media,
)


def test_dooh_is_not_traditional():
    # "ooh" is a substring of "dooh" but not a whole word — must not match.
    assert _is_traditional_media("DOOH", "stackadapt") is False


def test_multiword_out_of_home_still_matches():
    assert _is_traditional_media("Out of Home Transit", None) is True


def test_direct_mail_still_matches():
    assert _is_traditional_media("Direct Mail", None) is True


def test_radio_still_matches():
    # Confirm the keyword we assert on is actually in the set.
    assert "radio" in _TRADITIONAL_KEYWORDS
    assert _is_traditional_media("Radio", None) is True


def test_digital_platform_is_not_traditional():
    assert _is_traditional_media("Meta", "meta") is False
