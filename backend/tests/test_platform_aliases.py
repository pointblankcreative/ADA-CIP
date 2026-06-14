"""PLATFORM_MAP aliases added for 26023 (Sierra Club FIFA).

The Boosted Impact Media Pan tab labels self-serve buys "Open Web Video" and
"Digital Out Of Home"; without aliases _normalise_platform left them
unrecognised and the rows were dropped from pacing.

DOOH is now bought through StackAdapt (Perion's DOOH supply was retired), so
both "DOOH" and "Digital Out Of Home" route to the StackAdapt feed.
"""
from backend.services.media_plan_sync import _normalise_platform


def test_open_web_resolves_to_stackadapt():
    # Substring match, so the full "Open Web Video" label resolves too.
    assert _normalise_platform("Open Web") == "stackadapt"
    assert _normalise_platform("Open Web Video") == "stackadapt"


def test_digital_out_of_home_resolves_to_stackadapt():
    # DOOH now routes through StackAdapt, not Perion.
    assert _normalise_platform("Digital Out Of Home") == "stackadapt"


def test_dooh_short_label_resolves_to_stackadapt():
    assert _normalise_platform("DOOH") == "stackadapt"


def test_existing_aliases_unchanged():
    assert _normalise_platform("Meta") == "meta"
    assert _normalise_platform("Facebook, Instagram & Threads\nMeta") == "meta"
    assert _normalise_platform("Programmatic (Native)") == "stackadapt"
    assert _normalise_platform("Open Internet") == "stackadapt"
    # The literal Perion/Hivestack aliases are left intact for legacy labels.
    assert _normalise_platform("Perion") == "perion"
    assert _normalise_platform("Hivestack") == "perion"


def test_direct_booking_labels_stay_unrecognised():
    # Direct lines (not self-serve) must NOT collide with a real platform; they
    # pass through normalised (lowercased, spaces -> underscores).
    assert _normalise_platform("Connected TV") == "connected_tv"
    assert _normalise_platform("LED Truck") == "led_truck"
    assert _normalise_platform("Building Projection") == "building_projection"


def test_most_specific_alias_wins():
    # Finding 8: "google ads" contains both the "google ads" alias and the bare
    # "google" substring; most-specific-match-wins keeps the result stable (both
    # map to google_ads). "snapchat" contains the "snap" alias too.
    assert _normalise_platform("Google Ads") == "google_ads"
    assert _normalise_platform("Google") == "google_ads"
    assert _normalise_platform("YouTube") == "google_ads"
    assert _normalise_platform("Snapchat") == "snapchat"
