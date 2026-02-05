from routes.events import _prev_ym


def test_prev_ym_same_year():
    assert _prev_ym("2026-03") == "2026-02"


def test_prev_ym_year_boundary():
    assert _prev_ym("2026-01") == "2025-12"


def test_prev_ym_invalid():
    assert _prev_ym("bad") == "bad"
