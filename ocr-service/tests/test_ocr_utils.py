from app import _normalize_reading, _sanitize_type, _plausibility_filter


def test_normalize_reading_basic():
    assert _normalize_reading(" 123,45 ") == 123.45
    assert _normalize_reading("00123.00") == 123.0
    assert _normalize_reading(None) is None
    assert _normalize_reading(" ") is None


def test_sanitize_type():
    assert _sanitize_type("ХВС") == "ХВС"
    assert _sanitize_type("ГВС") == "ГВС"
    assert _sanitize_type("Электро") == "Электро"
    assert _sanitize_type("unknown") == "unknown"
    assert _sanitize_type("invalid") == "unknown"


def test_plausibility_filter_negative():
    reading, conf, note = _plausibility_filter("ХВС", -1, 0.9)
    assert reading is None
    assert conf <= 0.2
    assert note == "negative_reading_filtered"


def test_plausibility_filter_too_large_water():
    reading, conf, note = _plausibility_filter("ХВС", 1e9, 0.8)
    assert reading is None
    assert conf <= 0.2
    assert note == "water_too_large_filtered"


def test_plausibility_filter_ok():
    reading, conf, note = _plausibility_filter("Электро", 12345.6, 0.7)
    assert reading == 12345.6
    assert conf == 0.7
    assert note == ""
