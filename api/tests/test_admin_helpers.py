from core.admin_helpers import norm_phone, _phone_variants, _parse_reading_to_float, _normalize_serial


def test_norm_phone_basic_ru():
    assert norm_phone("8 (999) 123-45-67") == "79991234567"
    assert norm_phone("+7 999 123-45-67") == "79991234567"
    assert norm_phone("9991234567") == "79991234567"


def test_norm_phone_long_tail():
    assert norm_phone("abc 8 999 123 45 67 ext 555") == "71234567555"


def test_phone_variants():
    variants = set(_phone_variants("+7 999 123-45-67"))
    assert "79991234567" in variants
    assert "89991234567" in variants
    assert "9991234567" in variants


def test_parse_reading_to_float():
    assert _parse_reading_to_float("123,45") == 123.45
    assert _parse_reading_to_float("  123.45 ") == 123.45
    assert _parse_reading_to_float(" ") is None
    assert _parse_reading_to_float(None) is None


def test_normalize_serial_digits_and_dash():
    assert _normalize_serial("AB-12 34/56") == "-123456"
    assert _normalize_serial("  ") == ""
    assert _normalize_serial(None) == ""
