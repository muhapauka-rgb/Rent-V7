from routes.events import (
    _choose_single_attempt_result,
    _pick_best_series_local,
    _recover_series_missing_with_neighbors,
    _rebuild_series_best_from_payload,
    _select_water_context_values,
)


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
