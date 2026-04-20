#!/usr/bin/env python3
import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


CURRENT_GOLDEN_SET: Dict[str, Dict[str, Dict[str, Any]]] = {
    "5.jpeg": {
        "ocr": {
            "type": "ГВС",
            "reading": 991.89,
            "serial": "13 002714",
            "winner_source": "face_top_strip",
            "winner_serial": "13002714",
            "serial_branch_serial": "13002714",
            "odometer_branch_source": "face_top_strip",
        },
        "api": {
            "meter_kind": "cold",
            "meter_type_label": "ХВС",
            "ocr_reading": 991.89,
            "ocr_serial": "13 002714",
            "winner_source": "face_top_strip",
            "winner_serial": "13002714",
            "serial_branch_serial": "13002714",
            "odometer_branch_source": "face_top_strip",
            "meter_written": True,
        },
    },
    "4.jpeg": {
        "ocr": {
            "type": "unknown",
            "reading": 877.00,
            "serial": "13076128",
            "winner_source": "template",
            "winner_serial": "13076128",
            "serial_branch_serial": "13076128",
            "odometer_branch_source": None,
        },
        "api": {
            "meter_kind": "hot",
            "meter_type_label": "ГВС",
            "ocr_reading": 877.00,
            "ocr_serial": "13076128",
            "winner_source": "template",
            "winner_serial": "13076128",
            "serial_branch_serial": "13076128",
            "odometer_branch_source": None,
            "meter_written": True,
        },
    },
    "31.jpg": {
        "ocr": {
            "type": "Электро",
            "reading": 4737.21,
            "serial": "25564336",
        },
        "api": {
            "meter_kind": "electric",
            "meter_type_label": "Электро T1",
            "ocr_reading": 4737.21,
            "ocr_serial": "25564336",
            "meter_written": True,
        },
    },
    "32.jpg": {
        "ocr": {
            "type": "Электро",
            "reading": 5680.55,
            "serial": "25564336",
        },
        "api": {
            "meter_kind": "electric",
            "meter_type_label": "Электро T3",
            "ocr_reading": 5680.55,
            "ocr_serial": "25564336",
            "meter_written": True,
        },
    },
}


def _top_candidates(water_decision: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked = list((water_decision or {}).get("ranked") or [])
    out: List[Dict[str, Any]] = []
    for item in ranked[:4]:
        out.append(
            {
                "source": item.get("source"),
                "reading": item.get("reading"),
                "score": item.get("candidate_score"),
            }
        )
    return out


def _ocr_row(endpoint: str, path: Path, timeout_sec: float) -> Dict[str, Any]:
    with path.open("rb") as fh:
        resp = requests.post(
            endpoint,
            files={"file": (path.name, fh, "image/jpeg")},
            data={"trace_id": f"regress-ocr-{path.stem}"},
            timeout=timeout_sec,
        )
    resp.raise_for_status()
    payload = resp.json()
    water_decision = payload.get("water_decision") or {}
    winner = water_decision.get("winner") or {}
    serial_branch = water_decision.get("serial_branch") or {}
    serial_branch_winner = serial_branch.get("winner") or {}
    odometer_branch = water_decision.get("odometer_branch") or {}
    odometer_branch_winner = odometer_branch.get("winner") or {}
    return {
        "mode": "ocr",
        "file": path.name,
        "type": payload.get("type"),
        "reading": payload.get("reading"),
        "serial": payload.get("serial"),
        "openai_calls": payload.get("openai_calls"),
        "winner_source": winner.get("source"),
        "winner_serial": winner.get("serial"),
        "serial_branch_serial": serial_branch_winner.get("serial"),
        "odometer_branch_source": odometer_branch_winner.get("source"),
        "winner_score": winner.get("candidate_score"),
        "top_candidates": _top_candidates(water_decision),
        "notes": payload.get("notes"),
    }


def _api_row(endpoint: str, path: Path, timeout_sec: float, chat_id: str, ym: str) -> Dict[str, Any]:
    with path.open("rb") as fh:
        resp = requests.post(
            endpoint,
            files={"file": (path.name, fh, "image/jpeg")},
            data={"trace_id": f"regress-api-{path.stem}", "chat_id": chat_id, "ym": ym},
            timeout=timeout_sec,
        )
    resp.raise_for_status()
    payload = resp.json()
    diag = payload.get("diag") or {}
    water_decision = diag.get("ocr_water_decision") or {}
    winner = water_decision.get("winner") or {}
    serial_branch = water_decision.get("serial_branch") or {}
    serial_branch_winner = serial_branch.get("winner") or {}
    odometer_branch = water_decision.get("odometer_branch") or {}
    odometer_branch_winner = odometer_branch.get("winner") or {}
    ocr = payload.get("ocr") or {}
    return {
        "mode": "api",
        "file": path.name,
        "meter_kind": payload.get("meter_kind"),
        "meter_type_label": payload.get("meter_type_label"),
        "meter_written": payload.get("meter_written"),
        "ocr_failed": payload.get("ocr_failed"),
        "assigned_meter_index": payload.get("assigned_meter_index"),
        "ocr_type": ocr.get("type"),
        "ocr_reading": ocr.get("reading"),
        "ocr_serial": ocr.get("serial"),
        "winner_source": winner.get("source"),
        "winner_serial": winner.get("serial"),
        "serial_branch_serial": serial_branch_winner.get("serial"),
        "odometer_branch_source": odometer_branch_winner.get("source"),
        "winner_score": winner.get("candidate_score"),
        "top_candidates": _top_candidates(water_decision),
        "review_gate": water_decision.get("review_gate"),
        "warnings": diag.get("warnings"),
    }


def _same_value(actual: Any, expected: Any) -> bool:
    if isinstance(expected, float):
        try:
            return math.isclose(float(actual), float(expected), rel_tol=0.0, abs_tol=0.01)
        except Exception:
            return False
    return actual == expected


def _check_golden_row(row: Dict[str, Any], golden_scope: Dict[str, Dict[str, Any]]) -> List[str]:
    mode = str(row.get("mode") or "")
    expected = golden_scope.get(mode) or {}
    mismatches: List[str] = []
    for key, expected_value in expected.items():
        actual_value = row.get(key)
        if not _same_value(actual_value, expected_value):
            mismatches.append(f"{mode}:{key}: expected={expected_value!r} actual={actual_value!r}")
    return mismatches


def main() -> int:
    ap = argparse.ArgumentParser(description="Run focused OCR/API regression checks for meter photos")
    ap.add_argument("paths", nargs="+", help="Absolute or relative image paths to test")
    ap.add_argument("--mode", choices=("ocr", "api", "both"), default="ocr")
    ap.add_argument("--ocr-endpoint", default="http://127.0.0.1:8002/recognize")
    ap.add_argument("--api-endpoint", default="http://127.0.0.1:8001/events/photo")
    ap.add_argument("--chat-id", default="206724330")
    ap.add_argument("--ym", default="2026-04")
    ap.add_argument("--timeout-sec", type=float, default=180.0)
    ap.add_argument("--check-golden", choices=("current",), default=None)
    args = ap.parse_args()

    files = [Path(p).expanduser().resolve() for p in args.paths]
    missing = [str(p) for p in files if not p.exists()]
    if missing:
        raise SystemExit(f"missing files: {missing}")

    all_mismatches: List[str] = []
    for path in files:
        golden_scope = CURRENT_GOLDEN_SET.get(path.name, {}) if args.check_golden == "current" else {}
        if args.mode in ("ocr", "both"):
            row = _ocr_row(args.ocr_endpoint, path, args.timeout_sec)
            print(json.dumps(row, ensure_ascii=False))
            if golden_scope:
                all_mismatches.extend([f"{path.name}: {m}" for m in _check_golden_row(row, golden_scope)])
        if args.mode in ("api", "both"):
            row = _api_row(args.api_endpoint, path, args.timeout_sec, args.chat_id, args.ym)
            print(
                json.dumps(
                    row,
                    ensure_ascii=False,
                )
            )
            if golden_scope:
                all_mismatches.extend([f"{path.name}: {m}" for m in _check_golden_row(row, golden_scope)])
    if all_mismatches:
        for mismatch in all_mismatches:
            print(f"GOLDEN_MISMATCH: {mismatch}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
