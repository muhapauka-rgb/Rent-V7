#!/usr/bin/env python3
import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


TOOLS_DIR = Path(__file__).resolve().parent
DEFAULT_GOLDEN_MANIFEST = TOOLS_DIR / "regression_sets" / "current_golden.json"


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


def _load_golden_manifest(path: Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SystemExit(f"golden manifest not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid golden manifest JSON: {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise SystemExit(f"golden manifest must be a JSON object: {path}")
    cases = payload.get("cases")
    if not isinstance(cases, dict):
        raise SystemExit(f"golden manifest must contain object field 'cases': {path}")
    return cases


def _collect_files(
    cli_paths: List[str],
    golden_cases: Dict[str, Dict[str, Dict[str, Any]]],
    *,
    manifest_path: Optional[Path],
    dataset_root: Optional[Path],
) -> List[Tuple[Path, str]]:
    if cli_paths:
        files = [(Path(p).expanduser().resolve(), Path(p).expanduser().resolve().name) for p in cli_paths]
        return files

    collected: List[Tuple[Path, str]] = []
    for case_name, case in golden_cases.items():
        case_path = case.get("path")
        if not case_path:
            continue
        path = Path(str(case_path))
        if not path.is_absolute():
            if dataset_root is not None:
                path = (dataset_root / path).resolve()
            elif manifest_path is not None:
                path = (manifest_path.parent / path).resolve()
            else:
                path = path.resolve()
        else:
            path = path.resolve()
        collected.append((path, str(case_name)))

    if not collected:
        raise SystemExit("no input paths provided and no runnable cases with 'path' in manifest")
    return collected


def main() -> int:
    ap = argparse.ArgumentParser(description="Run focused OCR/API regression checks for meter photos")
    ap.add_argument("paths", nargs="*", help="Absolute or relative image paths to test")
    ap.add_argument("--mode", choices=("ocr", "api", "both"), default="ocr")
    ap.add_argument("--ocr-endpoint", default="http://127.0.0.1:8002/recognize")
    ap.add_argument("--api-endpoint", default="http://127.0.0.1:8001/events/photo")
    ap.add_argument("--chat-id", default="206724330")
    ap.add_argument("--ym", default="2026-04")
    ap.add_argument("--timeout-sec", type=float, default=180.0)
    ap.add_argument("--check-golden", choices=("current",), default=None)
    ap.add_argument(
        "--golden-manifest",
        default=None,
        help="Path to JSON manifest with golden cases. If omitted and --check-golden=current is set, uses the repo manifest.",
    )
    ap.add_argument(
        "--dataset-root",
        default=None,
        help="Optional root directory for manifest-relative case paths.",
    )
    args = ap.parse_args()

    golden_cases: Dict[str, Dict[str, Dict[str, Any]]] = {}
    manifest_path: Optional[Path] = None
    if args.golden_manifest:
        manifest_path = Path(args.golden_manifest).expanduser().resolve()
        golden_cases = _load_golden_manifest(manifest_path)
    elif args.check_golden == "current":
        manifest_path = DEFAULT_GOLDEN_MANIFEST
        golden_cases = _load_golden_manifest(manifest_path)

    dataset_root = Path(args.dataset_root).expanduser().resolve() if args.dataset_root else None
    file_cases = _collect_files(
        args.paths,
        golden_cases,
        manifest_path=manifest_path,
        dataset_root=dataset_root,
    )
    files = [path for path, _case_name in file_cases]
    missing = [str(p) for p in files if not p.exists()]
    if missing:
        raise SystemExit(f"missing files: {missing}")

    all_mismatches: List[str] = []
    for path, case_name in file_cases:
        golden_scope = golden_cases.get(case_name, {})
        if args.mode in ("ocr", "both"):
            row = _ocr_row(args.ocr_endpoint, path, args.timeout_sec)
            print(json.dumps(row, ensure_ascii=False))
            if golden_scope:
                all_mismatches.extend([f"{case_name}: {m}" for m in _check_golden_row(row, golden_scope)])
        if args.mode in ("api", "both"):
            row = _api_row(args.api_endpoint, path, args.timeout_sec, args.chat_id, args.ym)
            print(
                json.dumps(
                    row,
                    ensure_ascii=False,
                )
            )
            if golden_scope:
                all_mismatches.extend([f"{case_name}: {m}" for m in _check_golden_row(row, golden_scope)])
    if all_mismatches:
        for mismatch in all_mismatches:
            print(f"GOLDEN_MISMATCH: {mismatch}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
