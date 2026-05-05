from routes.events import (
    _choose_single_attempt_result,
    _electric_has_display_support,
    _pick_best_series_local,
    _is_uncorroborated_electric_fullframe,
    _extract_water_visual_type_hint,
    _recover_series_missing_with_neighbors,
    _rebuild_series_best_from_payload,
    _resolve_kind_by_type_and_serial,
    _select_water_context_values,
    _get_stable_water_context_prev_reading,
    _needs_electric_quality_retry,
    _pick_same_file_electric_correction_candidate,
    _pick_same_file_cross_month_reuse_candidate,
    _should_hold_unresolved_water_for_review,
)
from core.meters import _assigned_type_after_water_sort


def test_select_water_context_values_prefers_dense_cluster():
    vals = [1300.0, 2740.214, 878.774, 878.774, 999.675, 878.774, 999.675, 580.0, 255.0]
    out = _select_water_context_values(vals, max_values=3, support_tol=180.0)
    assert len(out) >= 2
    assert abs(out[0] - 878.774) < 0.02
    assert any(abs(v - 999.675) < 0.02 for v in out[:2])


def test_select_water_context_values_without_cluster_keeps_recent():
    vals = [580.0, 255.0]
    out = _select_water_context_values(vals, max_values=3, support_tol=120.0)
    assert out == [580.0, 255.0]


def test_select_water_context_values_filters_invalid():
    vals = [0.0, -1.0, float("inf"), float("nan"), 878.7]
    out = _select_water_context_values(vals, max_values=3)
    assert out == [878.7]


def test_select_water_context_values_cluster_only_if_any():
    vals = [878.774, 999.675, 878.774, 999.675, 1300.0, 2740.214]
    out = _select_water_context_values(vals, max_values=4, support_tol=180.0, cluster_only_if_any=True)
    assert len(out) == 2
    assert any(abs(v - 878.774) < 0.02 for v in out)
    assert any(abs(v - 999.675) < 0.02 for v in out)


class _FakeMappingResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, *_args, **_kwargs):
        return _FakeMappingResult(self._rows)


def test_stable_water_context_skips_decreasing_history():
    value, diag = _get_stable_water_context_prev_reading(
        _FakeConn(
            [
                {"id": 1197, "ym": "2026-03", "value": 580.0, "source": "manual", "ocr_value": None},
                {"id": 1181, "ym": "2026-02", "value": 1003.0, "source": "manual", "ocr_value": None},
            ]
        ),
        13,
        "2026-04",
        "cold",
        1,
    )
    assert value is None
    assert diag["reason"] == "unstable_decreasing_water_history"
    assert diag["candidate"]["value"] == 580.0


def test_stable_water_context_keeps_monotonic_history():
    value, diag = _get_stable_water_context_prev_reading(
        _FakeConn(
            [
                {"id": 2000, "ym": "2026-04", "value": 991.89, "source": "ocr", "ocr_value": 991.89},
                {"id": 1197, "ym": "2026-03", "value": 580.0, "source": "manual", "ocr_value": None},
            ]
        ),
        13,
        "2026-05",
        "cold",
        1,
    )
    assert diag is None
    assert value == 991.89


def test_resolve_kind_serial_is_authoritative_over_raw_type():
    out = _resolve_kind_by_type_and_serial(
        "hot",
        "13 002714",
        cold_serial="13 002714",
        hot_serial="13 076128",
    )
    assert out["policy"] == "serial_authoritative"
    assert out["serial_force_kind"] == "cold"
    assert out["resolved_kind"] == "cold"
    assert out["type_conflict"] is True


def test_resolve_kind_uses_raw_type_when_serial_absent():
    out = _resolve_kind_by_type_and_serial("electric", None)
    assert out["policy"] == "raw_type_without_serial_match"
    assert out["resolved_kind"] == "electric"
    assert out["serial_force_kind"] is None


def test_resolve_kind_uses_water_type_when_serial_absent():
    out = _resolve_kind_by_type_and_serial("hot", None, cold_serial="13 002714", hot_serial="13 076128")
    assert out["policy"] == "raw_type_without_serial_match"
    assert out["resolved_kind"] == "hot"
    assert out["serial_force_kind"] is None


def test_resolve_kind_review_when_type_and_serial_absent():
    out = _resolve_kind_by_type_and_serial(None, None)
    assert out["policy"] == "review_no_type_no_serial"
    assert out["resolved_kind"] is None
    assert out["serial_match"] == "none"


def test_extract_water_visual_type_hint_can_feed_raw_kind_fallback():
    hint = _extract_water_visual_type_hint(
        {
            "type": "unknown",
            "visual_water_type_hint": {
                "type": "ХВС",
                "confidence": 0.77,
                "source": "color_marker_hsv",
                "reason": "dominant_red_blue_water_marker",
            },
        }
    )
    assert hint is not None
    assert hint["kind"] == "cold"
    assert hint["confidence"] == 0.77


def test_unresolved_water_value_without_serial_is_review_only():
    resolution = _resolve_kind_by_type_and_serial(None, None, cold_serial="13 002714", hot_serial="13 076128")
    assert _should_hold_unresolved_water_for_review(
        kind=None,
        ocr_type="unknown",
        serial_norm=None,
        value_float=878.77,
        is_water_context=True,
        serial_resolution=resolution,
    )


def test_unresolved_water_guard_does_not_block_serial_or_raw_type():
    cold_resolution = _resolve_kind_by_type_and_serial(
        None,
        "13 002714",
        cold_serial="13 002714",
        hot_serial="13 076128",
    )
    assert not _should_hold_unresolved_water_for_review(
        kind="cold",
        ocr_type="unknown",
        serial_norm="13002714",
        value_float=991.89,
        is_water_context=True,
        serial_resolution=cold_resolution,
    )
    raw_resolution = _resolve_kind_by_type_and_serial("hot", None)
    assert not _should_hold_unresolved_water_for_review(
        kind="hot",
        ocr_type="ГВС",
        serial_norm=None,
        value_float=881.1,
        is_water_context=True,
        serial_resolution=raw_resolution,
    )


def test_water_sort_assignment_returns_hot_for_min_value():
    assigned = _assigned_type_after_water_sort(
        881.1,
        hot_value=881.1,
        cold_value=999.67,
        fallback_kind="hot",
    )
    assert assigned == "hot"


def test_water_sort_assignment_returns_cold_for_max_value():
    assigned = _assigned_type_after_water_sort(
        999.67,
        hot_value=881.1,
        cold_value=999.67,
        fallback_kind="cold",
    )
    assert assigned == "cold"


def test_pick_best_series_local_prefers_supported_contextual_value():
    results = [
        {"reading": 1132.12, "confidence": 0.95, "type": "unknown", "notes": ""},
        {"reading": 881.10, "confidence": 1.00, "type": "unknown", "notes": ""},
        {"reading": 881.07, "confidence": 0.92, "type": "unknown", "notes": ""},
    ]
    idx, best, score = _pick_best_series_local(results, [878.77, 999.675, 1300.0])
    assert idx in (1, 2)
    assert abs(float(best.get("reading")) - 881.10) < 0.1
    assert score > 0.5


def test_recover_series_missing_with_neighbors_same_serial():
    results = [
        {"reading": 878.77, "confidence": 0.95, "type": "unknown", "serial": "13 076128", "notes": ""},
        {"reading": None, "confidence": 0.45, "type": "unknown", "serial": "13 076128", "notes": "water_no_ok_odometer_winner"},
    ]
    fixed, warnings = _recover_series_missing_with_neighbors(
        results,
        prev_values=[878.77, 881.10],
        serial_hints=["13002714", "13076128"],
    )
    assert fixed[1]["reading"] == 878.77
    assert warnings


def test_rebuild_series_best_from_payload_prefers_recovered_neighbor():
    payload = {
        "results": [
            {"filename": "a.jpg", "reading": 878.77, "confidence": 0.95, "type": "unknown", "serial": "13 076128", "notes": ""},
            {"filename": "b.jpg", "reading": None, "confidence": 0.45, "type": "unknown", "serial": "13 076128", "notes": "water_no_ok_odometer_winner"},
        ]
    }
    rebuilt = _rebuild_series_best_from_payload(
        payload,
        prev_values=[878.77, 881.10],
        serial_hints=["13002714", "13076128"],
    )
    assert rebuilt is not None
    assert rebuilt["best"] is not None
    assert float(rebuilt["best"]["reading"]) == 878.77


def test_choose_single_attempt_result_prefers_majority_vote():
    attempts = [
        {"reading": 878.77, "confidence": 0.71, "type": "unknown", "notes": ""},
        {"reading": 878.79, "confidence": 0.66, "type": "unknown", "notes": ""},
        {"reading": 871.52, "confidence": 0.95, "type": "unknown", "notes": ""},
    ]
    picked, warnings = _choose_single_attempt_result(attempts, [878.77, 881.10, 987.79])
    assert picked is not None
    assert abs(float(picked.get("reading")) - 878.78) < 0.12
    assert "single_vote" in str(picked.get("notes") or "")
    assert warnings


def test_electric_fullframe_without_display_support_needs_retry_and_review():
    payload = {
        "type": "Электро",
        "reading": 2834.0,
        "serial": "34485076",
        "confidence": 0.9,
        "notes": "provider=openai:gpt-4o; variant=orig_fullframe; agree=1/4",
        "debug": [
            {
                "provider": "openai:gpt-4o",
                "variant": "orig_fullframe",
                "type": "Электро",
                "reading": 2834.0,
            }
        ],
    }
    assert not _electric_has_display_support(payload)
    assert _is_uncorroborated_electric_fullframe(payload)
    assert _needs_electric_quality_retry(payload)


def test_electric_display_supported_bridge_does_not_need_retry():
    payload = {
        "type": "Электро",
        "reading": 2634.0,
        "confidence": 0.9,
        "notes": (
            "electric_mercury_bridge_forced; "
            "provider=openai-electric:gpt-4o:display; "
            "variant=electric_ed_mercury_bridge_forced"
        ),
        "debug": [
            {
                "provider": "openai-electric:gpt-4o:display",
                "variant": "electric_ed_mercury_bridge_forced",
                "type": "Электро",
                "reading": 2634.0,
            }
        ],
    }
    assert _electric_has_display_support(payload)
    assert not _is_uncorroborated_electric_fullframe(payload)
    assert not _needs_electric_quality_retry(payload)


def test_same_file_electric_correction_reuses_stale_slot():
    correction = _pick_same_file_electric_correction_candidate(
        previous_events=[
            {"id": 693, "meter_index": 1, "meter_value": 2834.0, "ocr_reading": 2834.0},
        ],
        rows_before=[
            {"meter_index": 1, "value": 2834.0, "source": "ocr"},
        ],
        new_value=2634.0,
    )
    assert correction is not None
    assert correction["assigned_meter_index"] == 1
    assert correction["previous_value"] == 2834.0
    assert correction["new_value"] == 2634.0
    assert correction["duplicate_indices"] == []


def test_same_file_electric_correction_marks_duplicate_from_prior_bad_run():
    correction = _pick_same_file_electric_correction_candidate(
        previous_events=[
            {"id": 699, "meter_index": 2, "meter_value": 2634.0, "ocr_reading": 2634.0},
            {"id": 693, "meter_index": 1, "meter_value": 2834.0, "ocr_reading": 2834.0},
        ],
        rows_before=[
            {"meter_index": 1, "value": 2834.0, "source": "ocr"},
            {"meter_index": 2, "value": 2634.0, "source": "ocr"},
        ],
        new_value=2634.0,
    )
    assert correction is not None
    assert correction["assigned_meter_index"] == 1
    assert correction["duplicate_indices"] == [2]
    assert correction["same_value_event_ids"] == [699]


def test_same_file_electric_correction_never_overwrites_manual_value():
    correction = _pick_same_file_electric_correction_candidate(
        previous_events=[
            {"id": 693, "meter_index": 1, "meter_value": 2834.0, "ocr_reading": 2834.0},
        ],
        rows_before=[
            {"meter_index": 1, "value": 2834.0, "source": "manual"},
        ],
        new_value=2634.0,
    )
    assert correction is None


def test_same_file_cross_month_reuse_goes_to_review():
    reuse = _pick_same_file_cross_month_reuse_candidate(
        previous_events=[
            {"id": 699, "ym": "2026-05", "meter_kind": "electric", "meter_index": 1, "meter_value": 2634.0},
        ],
        ym="2026-06",
    )
    assert reuse is not None
    assert reuse["reason"] == "same_file_cross_month_reuse"
    assert reuse["previous_event_id"] == 699
    assert reuse["previous_ym"] == "2026-05"
    assert reuse["current_ym"] == "2026-06"


def test_same_file_same_month_is_not_cross_month_reuse():
    reuse = _pick_same_file_cross_month_reuse_candidate(
        previous_events=[
            {"id": 699, "ym": "2026-05", "meter_kind": "electric", "meter_index": 1, "meter_value": 2634.0},
        ],
        ym="2026-05",
    )
    assert reuse is None


def test_same_file_same_month_retake_wins_over_older_cross_month_history():
    reuse = _pick_same_file_cross_month_reuse_candidate(
        previous_events=[
            {"id": 705, "ym": "2026-05", "meter_kind": "electric", "meter_index": 1, "meter_value": 2634.0},
            {"id": 689, "ym": "2026-04", "meter_kind": "electric", "meter_index": 3, "meter_value": 2834.0},
        ],
        ym="2026-05",
    )
    assert reuse is None


def test_recover_series_missing_with_neighbors_skips_cross_day():
    results = [
        {
            "filename": "photo_2026-02-05 14.24.41.jpeg",
            "reading": None,
            "confidence": 0.45,
            "type": "unknown",
            "serial": "13 076128",
            "notes": "water_no_ok_odometer_winner",
        },
        {
            "filename": "photo_2026-02-17 00.03.06.jpeg",
            "reading": 871.52,
            "confidence": 0.95,
            "type": "unknown",
            "serial": "13 076128",
            "notes": "",
        },
    ]
    fixed, warnings = _recover_series_missing_with_neighbors(
        results,
        prev_values=[871.52, 878.77],
        serial_hints=["13076128"],
    )
    assert fixed[0]["reading"] is None
    assert not warnings


def test_recover_series_missing_with_neighbors_allows_same_day_close_time():
    results = [
        {
            "filename": "photo_2026-02-05 14.24.41.jpeg",
            "reading": None,
            "confidence": 0.45,
            "type": "unknown",
            "serial": "13 076128",
            "notes": "water_no_ok_odometer_winner",
        },
        {
            "filename": "photo_2026-02-05 14.24.38.jpeg",
            "reading": 878.77,
            "confidence": 0.95,
            "type": "unknown",
            "serial": "13 076128",
            "notes": "",
        },
    ]
    fixed, warnings = _recover_series_missing_with_neighbors(
        results,
        prev_values=[871.52, 878.77],
        serial_hints=["13076128"],
    )
    assert fixed[0]["reading"] == 878.77
    assert warnings
