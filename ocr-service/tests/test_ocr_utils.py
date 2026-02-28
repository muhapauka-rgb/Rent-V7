from app import (
    _parse_context_serial_hints,
    _serial_hint_tails,
    _pick_water_candidate_by_serial,
    _pick_water_candidate_with_context,
    _pick_best_water_candidate_adaptive,
    _water_hypothesis_candidates_from_response,
    _water_leading_trim_context_fixes,
    _refine_fraction_from_prev,
    _snap_to_same_integer_context,
    _pick_best_series_result,
    _series_result_score,
    _extract_digits_from_cell_sheet_resp,
    _is_ok_water_digits,
    _is_suspicious_water_digits,
    _is_weak_red_digits,
    _variant_image_bytes,
    _is_strict_water_odometer_candidate,
    _normalize_reading,
    _plausibility_filter,
    _reading_from_digits,
    _serial_tail_match_len,
    _sanitize_type,
    _parse_photo_filename_dt,
    _recover_series_missing_with_neighbors,
    _pick_water_integer_consensus_candidate,
    _collect_red_votes_for_integer,
    _pick_red_digits_by_vote,
    _has_red_disagreement_for_integer,
    _water_suspicious_layout_fixes,
)


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


def test_reading_from_digits_ignores_single_red_digit():
    assert _reading_from_digits("01003", "2") == 1003.0
    assert _reading_from_digits("01003", "21") == 1003.21


def test_extract_digits_from_cells_drops_single_red_digit():
    resp = {
        "cells": {"B1": "0", "B2": "1", "B3": "0", "B4": "0", "B5": "3", "R1": "2"},
        "black_digits": "01003",
        "red_digits": "2",
    }
    b, r = _extract_digits_from_cell_sheet_resp(resp, black_len=5, red_len=3)
    assert b == "01003"
    assert r is None


def test_strict_water_candidate_allows_strong_black_without_red():
    item = {
        "variant": "cells_row_counter_row_1",
        "provider": "openai-odo:gpt-4o",
        "black_digits": "01003",
        "red_digits": None,
        "confidence": 0.74,
        "serial": None,
    }
    assert _is_strict_water_odometer_candidate(item) is True


def test_strict_water_candidate_rejects_weak_black_without_red():
    item = {
        "variant": "cells_row_counter_row_1",
        "provider": "openai-odo:gpt-4o",
        "black_digits": "01003",
        "red_digits": None,
        "confidence": 0.51,
        "serial": None,
    }
    assert _is_strict_water_odometer_candidate(item) is False


def test_parse_context_serial_hints():
    hints = _parse_context_serial_hints("13 002714, 13-076128; junk; 1")
    assert hints == ["13002714", "13076128"]


def test_serial_hint_tails():
    tails = _serial_hint_tails(["13002714", "13076128", "13002714"])
    assert tails == ["02714", "76128"]


def test_serial_tail_match_len():
    assert _serial_tail_match_len("13 002714", "13002714") >= 6
    assert _serial_tail_match_len("13076128", "13002714") < 4


def test_pick_water_candidate_with_context_prefers_serial_match():
    candidates = [
        {
            "type": "ХВС",
            "reading": 995.0,
            "serial": "13 076128",
            "confidence": 0.9,
            "black_digits": "00995",
            "red_digits": "00",
            "provider": "openai-odo:gpt-4o",
            "variant": "counter_row_1",
        },
        {
            "type": "ХВС",
            "reading": 1005.0,
            "serial": "13 002714",
            "confidence": 0.7,
            "black_digits": "01005",
            "red_digits": "00",
            "provider": "openai-odo:gpt-4o",
            "variant": "counter_row_2",
        },
    ]
    best = _pick_water_candidate_with_context(
        candidates,
        prev_values=[1000.0],
        serial_hints=["13002714"],
    )
    assert best is not None
    assert str(best.get("serial")).replace(" ", "") == "13002714"


def test_pick_water_candidate_with_context_prefers_closer_value_with_same_serial():
    candidates = [
        {
            "type": "unknown",
            "reading": 967.79,
            "serial": "13 002714",
            "confidence": 0.9,
            "black_digits": "00967",
            "red_digits": "791",
            "provider": "openai-odo:gpt-4o",
            "variant": "counter_row_1",
        },
        {
            "type": "unknown",
            "reading": 987.92,
            "serial": "13 002714",
            "confidence": 0.95,
            "black_digits": "00987",
            "red_digits": "92",
            "provider": "openai-odo-serial-target:gpt-4o",
            "variant": "st_orig",
        },
    ]
    best = _pick_water_candidate_with_context(
        candidates,
        prev_values=[871.52, 987.79, 881.10],
        serial_hints=["13002714", "13076128"],
    )
    assert best is not None
    assert float(best.get("reading")) == 987.92


def test_pick_water_candidate_with_context_prefers_serial_matched_near_value_with_weak_red():
    candidates = [
        {
            "type": "unknown",
            "reading": 827.0,
            "serial": None,
            "confidence": 0.84,
            "black_digits": "827",
            "red_digits": None,
            "provider": "openai-odo:gpt-4o:ctxsub",
            "variant": "odo_pre_orig_ctxsub2_3",
        },
        {
            "type": "unknown",
            "reading": 878.7,
            "serial": "13 076128",
            "confidence": 0.88,
            "black_digits": "878",
            "red_digits": "700",
            "provider": "openai-odo-serial-target:gpt-4o:ctxtrim",
            "variant": "st_focused_crop_ctxtrim2_shiftbr",
        },
    ]
    best = _pick_water_candidate_with_context(
        candidates,
        prev_values=[871.52, 987.79, 881.10, 878.77],
        serial_hints=["13002714", "13076128"],
    )
    assert best is not None
    assert float(best.get("reading")) == 878.7


def test_pick_water_candidate_by_serial():
    candidates = [
        {
            "type": "unknown",
            "reading": 872.72,
            "serial": "13 076128",
            "confidence": 0.65,
            "black_digits": "00872",
            "red_digits": "72",
            "provider": "openai-odo:gpt-4o",
            "variant": "counter_row_1",
        },
        {
            "type": "unknown",
            "reading": 987.92,
            "serial": "13 002714",
            "confidence": 0.55,
            "black_digits": "00987",
            "red_digits": "92",
            "provider": "openai-odo:gpt-4o",
            "variant": "counter_row_2",
        },
    ]
    best = _pick_water_candidate_by_serial(candidates, serial_hints=["13002714"])
    assert best is not None
    assert str(best.get("serial")).replace(" ", "") == "13002714"


def test_water_hypothesis_candidates_from_response():
    resp = {
        "type": "unknown",
        "serial": "13 076128",
        "confidence": 0.55,
        "hypotheses": [
            {"black_digits": "00871", "red_digits": "52", "confidence": 0.74},
            {"black_digits": "04871", "red_digits": "54", "confidence": 0.31},
        ],
    }
    out = _water_hypothesis_candidates_from_response(
        resp,
        variant_prefix="hyp_row1",
        provider="openai-odo-hyp:gpt-4o",
    )
    assert len(out) >= 1
    assert out[0]["reading"] == 871.52
    assert out[0]["provider"].startswith("openai-odo-hyp:")


def test_pick_best_water_candidate_adaptive_prefers_prev_context():
    cands = [
        {
            "type": "unknown",
            "reading": 872.0,
            "serial": "13 002714",
            "confidence": 0.8,
            "black_digits": None,
            "red_digits": None,
            "provider": "openai:gpt-4o",
            "variant": "orig",
        },
        {
            "type": "unknown",
            "reading": 871.52,
            "serial": "13 076128",
            "confidence": 0.65,
            "black_digits": "00871",
            "red_digits": "52",
            "provider": "openai-odo-hyp:gpt-4o",
            "variant": "hyp_row_h1",
        },
    ]
    best = _pick_best_water_candidate_adaptive(
        cands,
        prev_values=[871.51, 1003.21],
        serial_hints=["13076128"],
    )
    assert best is not None
    assert best.get("reading") == 871.52


def test_water_leading_trim_context_fixes():
    item = {
        "type": "unknown",
        "reading": 4871.52,
        "serial": "13 076128",
        "confidence": 0.95,
        "black_digits": "04871",
        "red_digits": "52",
        "provider": "openai-odo:gpt-4o",
        "variant": "odo_pre_det",
        "notes": "base",
    }
    fixed = _water_leading_trim_context_fixes(item, prev_values=[878.42, 1003.21])
    assert fixed
    assert fixed[0]["reading"] == 871.52
    assert fixed[0]["black_digits"] == "871"


def test_is_ok_water_digits_accepts_three_digits():
    assert _is_ok_water_digits({"black_digits": "871"}) is True


def test_suspicious_digits_keeps_three_sig_with_two_red():
    assert _is_suspicious_water_digits({"black_digits": "00987", "red_digits": "92"}) is False


def test_is_weak_red_digits():
    assert _is_weak_red_digits(None) is True
    assert _is_weak_red_digits("7") is True
    assert _is_weak_red_digits("07") is True
    assert _is_weak_red_digits("700") is True
    assert _is_weak_red_digits("000") is True
    assert _is_weak_red_digits("77") is False


def test_water_suspicious_layout_fixes_shift_boundary_for_overlong_black():
    item = {
        "type": "unknown",
        "reading": 10032.19,
        "serial": None,
        "confidence": 0.95,
        "black_digits": "010032",
        "red_digits": "19",
        "provider": "openai-odo:gpt-4o",
        "variant": "odo_global_1",
        "notes": "base",
    }
    fixed = _water_suspicious_layout_fixes(item)
    assert fixed
    assert any(abs(float(x.get("reading")) - 1003.21) < 0.01 for x in fixed)


def test_water_suspicious_layout_fixes_shift_boundary_for_leading_zero_case():
    item = {
        "type": "unknown",
        "reading": 8715.22,
        "serial": None,
        "confidence": 0.95,
        "black_digits": "08715",
        "red_digits": "22",
        "provider": "openai-odo:gpt-4o",
        "variant": "odo_global_2",
        "notes": "base",
    }
    fixed = _water_suspicious_layout_fixes(item)
    assert fixed
    assert any(abs(float(x.get("reading")) - 871.52) < 0.01 for x in fixed)


def test_water_suspicious_layout_fixes_handles_dup_shift_pattern():
    item = {
        "type": "unknown",
        "reading": 4887.71,
        "serial": None,
        "confidence": 0.95,
        "black_digits": "04887",
        "red_digits": "71",
        "provider": "openai-odo:gpt-4o",
        "variant": "odo_global_1",
        "notes": "base",
    }
    fixed = _water_suspicious_layout_fixes(item)
    assert fixed
    assert any(abs(float(x.get("reading")) - 878.71) < 0.01 for x in fixed)


def test_variant_image_bytes_ctxtrim_fallback():
    m = {"st_center_crop_strong": b"abc"}
    got = _variant_image_bytes(m, "st_center_crop_strong_ctxtrim2_shiftbr")
    assert got == b"abc"


def test_refine_fraction_from_prev():
    assert _refine_fraction_from_prev("878", [871.52, 878.77, 987.79]) == 878.77
    assert _refine_fraction_from_prev("878", [871.52, 987.79]) is None


def test_snap_to_same_integer_context():
    assert _snap_to_same_integer_context(987.92, [871.52, 987.79, 881.10]) == 987.79
    assert _snap_to_same_integer_context(987.92, [871.52, 988.01, 881.10]) is None


def test_pick_best_series_result_prefers_supported_reading():
    results = [
        {
            "filename": "a.jpg",
            "type": "unknown",
            "reading": 987.92,
            "confidence": 0.95,
            "notes": "provider=openai-odo-serial-target",
        },
        {
            "filename": "b.jpg",
            "type": "unknown",
            "reading": 987.79,
            "confidence": 0.80,
            "notes": "context_same_int_snap=987.79",
        },
        {
            "filename": "c.jpg",
            "type": "unknown",
            "reading": 987.79,
            "confidence": 0.78,
            "notes": "",
        },
    ]
    idx, best = _pick_best_series_result(results, prev_values=[871.52, 987.79, 881.10])
    assert idx in (1, 2)
    assert float(best.get("reading")) == 987.79


def test_series_result_score_penalizes_far_singleton():
    all_items = [
        {
            "type": "unknown",
            "reading": 1132.28,
            "confidence": 0.9,
            "notes": "water_context_far_singleton(dist=144.33)",
        },
        {
            "type": "unknown",
            "reading": None,
            "confidence": 0.0,
            "notes": "",
        },
    ]
    sc = _series_result_score(all_items[0], all_items, prev_values=[871.52, 987.79, 881.10])
    assert sc < 0.5


def test_parse_photo_filename_dt():
    dt = _parse_photo_filename_dt("photo_2026-02-17 18.16.18.jpeg")
    assert dt is not None
    assert dt.year == 2026 and dt.month == 2 and dt.day == 17
    assert dt.hour == 18 and dt.minute == 16 and dt.second == 18


def test_recover_series_missing_with_neighbors_same_minute():
    rows = [
        {
            "filename": "photo_2026-02-17 18.16.15.jpeg",
            "type": "unknown",
            "reading": None,
            "serial": "13 002714",
            "confidence": 0.45,
            "notes": "water_context_far_singleton",
        },
        {
            "filename": "photo_2026-02-17 18.16.18.jpeg",
            "type": "unknown",
            "reading": 881.10,
            "serial": "13 076128",
            "confidence": 0.95,
            "notes": "ok",
        },
    ]
    fixed = _recover_series_missing_with_neighbors(rows)
    assert _normalize_reading(fixed[0].get("reading")) == 881.10
    assert "series_neighbor_recover" in str(fixed[0].get("notes") or "")


def test_recover_series_missing_prefers_near_time_over_far_serial():
    rows = [
        {
            "filename": "photo_2026-02-17 18.16.15.jpeg",
            "type": "unknown",
            "reading": None,
            "serial": "13 002714",
            "confidence": 0.45,
            "notes": "base",
        },
        {
            "filename": "photo_2026-02-17 18.16.18.jpeg",
            "type": "unknown",
            "reading": 881.10,
            "serial": "13 076128",
            "confidence": 0.95,
            "notes": "near",
        },
        {
            "filename": "photo_2026-02-17 00.03.11.jpeg",
            "type": "unknown",
            "reading": 987.79,
            "serial": "13 002714",
            "confidence": 0.95,
            "notes": "far_serial",
        },
    ]
    fixed = _recover_series_missing_with_neighbors(rows)
    assert _normalize_reading(fixed[0].get("reading")) == 881.10


def test_pick_water_integer_consensus_candidate():
    cands = [
        {
            "type": "unknown",
            "reading": 991.35,
            "serial": "13 002714",
            "confidence": 0.88,
            "black_digits": "991",
            "red_digits": "350",
            "provider": "openai-odo-serial-target:gpt-4o:ctxtrim",
            "variant": "st_middle_band_ctxtrim1_shiftbr",
            "notes": "context_trim_leading_digit",
        },
        {
            "type": "unknown",
            "reading": 999.75,
            "serial": None,
            "confidence": 0.95,
            "black_digits": "00999",
            "red_digits": "75",
            "provider": "openai-odo:gpt-4o",
            "variant": "odo_top_strip_1",
            "notes": "",
        },
        {
            "type": "unknown",
            "reading": 999.243,
            "serial": None,
            "confidence": 0.95,
            "black_digits": "00999",
            "red_digits": "243",
            "provider": "openai-odo:gpt-4o",
            "variant": "odo_global_3",
            "notes": "",
        },
    ]
    best = _pick_water_integer_consensus_candidate(
        cands,
        prev_values=[878.77, 871.52, 987.79, 881.10],
    )
    assert best is not None
    assert int(float(best.get("reading"))) == 999


def test_collect_red_votes_for_integer_and_pick():
    cands = [
        {"reading": 999.75, "red_digits": "75", "confidence": 0.92, "type": "unknown"},
        {"reading": 999.675, "red_digits": "675", "confidence": 0.78, "type": "unknown"},
        {"reading": 999.675, "red_digits": "675", "confidence": 0.81, "type": "unknown"},
        {"reading": 998.12, "red_digits": "12", "confidence": 0.99, "type": "unknown"},
    ]
    votes, counts, best_conf = _collect_red_votes_for_integer(cands, target_int=999)
    picked = _pick_red_digits_by_vote(votes, counts, best_conf)
    assert picked == "675"


def test_has_red_disagreement_for_integer():
    cands = [
        {"reading": 999.75, "red_digits": "75", "confidence": 0.92, "type": "unknown"},
        {"reading": 999.243, "red_digits": "243", "confidence": 0.95, "type": "unknown"},
    ]
    assert _has_red_disagreement_for_integer(cands, 999) is True
