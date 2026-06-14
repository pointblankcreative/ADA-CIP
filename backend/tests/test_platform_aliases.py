"""PLATFORM_MAP aliases added for 26023 (Sierra Club FIFA).

The Boosted Impact Media Pan tab labels self-serve buys "Open Web Video" and
"Digital Out Of Home"; without aliases _normalise_platform left them
unrecognised and the rows were dropped from pacing.
"""
from backend.services.media_plan_sync import _normalise_platform


def test_open_web_resolves_to_stackadapt():
    # Substring match, so the full "Open Web Video" label resolves too.
    assert _normalise_platform("Open Web") == "stackadapt"
    assert _normalise_platform("Open Web Video") == "stackadapt"


def test_digital_out_of_home_resolves_to_perion():
    assert _normalise_platform("Digital Out Of Home") == "perion"


def test_existing_aliases_unchanged():
    assert _normalise_platform("Meta") == "meta"
    assert _normalise_platform("Facebook, Instagram & Threads\nMeta") == "meta"
    assert _normalise_platform("Programmatic (Native)") == "stackadapt"
    assert _normalise_platform("DOOH") == "perion"
    assert _normalise_platform("Open Internet") == "stackadapt"


def test_direct_booking_labels_stay_unrecognised():
    # Direct lines (not self-serve) must NOT collide with a real platform; they
    # pass through normalised (lowercased, spaces -> underscores).
    assert _normalise_platform("Connected TV") == "connected_tv"
    assert _normalise_platform("LED Truck") == "led_truck"
    assert _normalise_platform("Building Projection") == "building_projection"
