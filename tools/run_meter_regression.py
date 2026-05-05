#!/usr/bin/env python3
import argparse
import hashlib
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


TOOLS_DIR = Path(__file__).resolve().parent
DEFAULT_GOLDEN_MANIFEST = TOOLS_DIR / "regression_sets" / "current_golden.json"
OCR_CONTEXT_KEYS = ("context_prev_water", "context_serial_hint", "context_serial_prev")


def _sha16(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _case_ocr_context(case: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not isinstance(case, dict):
        return {}
    raw_ctx: Dict[str, Any] = {}
    nested = case.get("ocr_context")
    if isinstance(nested, dict):
        raw_ctx.update(nested)
    nested = case.get("context")
    if isinstance(nested, dict):
        raw_ctx.update(nested)
    for key in OCR_CONTEXT_KEYS:
        if case.get(key) is not None:
            raw_ctx[key] = case.get(key)
    return {
        key: str(raw_ctx[key])
        for key in OCR_CONTEXT_KEYS
        if raw_ctx.get(key) not in (None, "")
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


def _local_summary(local_recognizer: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    local = local_recognizer or {}
    winner = local.get("winner") or {}
    zones = list(local.get("zones") or [])
    return {
        "local_version": local.get("version"),
        "local_status": local.get("status"),
        "local_digit_classifier_enabled": local.get("digit_classifier_enabled"),
        "local_digit_classifier_version": local.get("digit_classifier_version"),
        "local_winner_kind": winner.get("kind"),
        "local_winner_reading": winner.get("reading"),
        "local_winner_score": winner.get("candidate_score"),
        "local_top_zone": (zones[0].get("kind_hint") if zones and isinstance(zones[0], dict) else None),
    }


def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _ocr_row(endpoint: str, path: Path, timeout_sec: float, *, case_name: str, case: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    import requests

    post_data = {"trace_id": f"regress-ocr-{case_name}"}
    ocr_context = _case_ocr_context(case)
    post_data.update(ocr_context)
    started = time.monotonic()
    with path.open("rb") as fh:
        resp = requests.post(
            endpoint,
            files={"file": (path.name, fh, "image/jpeg")},
            data=post_data,
            timeout=timeout_sec,
        )
    elapsed_ms = round((time.monotonic() - started) * 1000.0, 1)
    resp.raise_for_status()
    payload = resp.json()
    water_decision = payload.get("water_decision") or {}
    winner = water_decision.get("winner") or {}
    serial_branch = water_decision.get("serial_branch") or {}
    serial_branch_winner = serial_branch.get("winner") or {}
    odometer_branch = water_decision.get("odometer_branch") or {}
    odometer_branch_winner = odometer_branch.get("winner") or {}
    row = {
        "mode": "ocr",
        "case": case_name,
        "file": path.name,
        "sha16": _sha16(path),
        "elapsed_ms": elapsed_ms,
        "ocr_context": ocr_context,
        "type": payload.get("type"),
        "reading": payload.get("reading"),
        "serial": payload.get("serial"),
        "openai_calls": payload.get("openai_calls"),
        "provider_errors": payload.get("provider_errors"),
        "timings_ms": payload.get("timings_ms"),
        "winner_source": winner.get("source"),
        "winner_serial": winner.get("serial"),
        "serial_branch_serial": serial_branch_winner.get("serial"),
        "odometer_branch_source": odometer_branch_winner.get("source"),
        "winner_score": winner.get("candidate_score"),
        "top_candidates": _top_candidates(water_decision),
        "notes": payload.get("notes"),
    }
    row.update(_local_summary(payload.get("local_recognizer") or {}))
    return row


def _api_row(endpoint: str, path: Path, timeout_sec: float, chat_id: str, ym: str, *, case_name: str) -> Dict[str, Any]:
    import requests

    started = time.monotonic()
    with path.open("rb") as fh:
        resp = requests.post(
            endpoint,
            files={"file": (path.name, fh, "image/jpeg")},
            data={"trace_id": f"regress-api-{case_name}", "chat_id": chat_id, "ym": ym},
            timeout=timeout_sec,
        )
    elapsed_ms = round((time.monotonic() - started) * 1000.0, 1)
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
    row = {
        "mode": "api",
        "case": case_name,
        "file": path.name,
        "sha16": _sha16(path),
        "elapsed_ms": elapsed_ms,
        "api_writes_db": True,
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
        "provider_errors": diag.get("ocr_provider_errors"),
        "warnings": diag.get("warnings"),
    }
    row.update(_local_summary(diag.get("ocr_local_recognizer") or {}))
    return row


def _same_value(actual: Any, expected: Any, *, key: str = "") -> bool:
    if "serial" in key:
        return _digits_only(actual) == _digits_only(expected)
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
        if not _same_value(actual_value, expected_value, key=key):
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


def _print_manifest_cases(cases: Dict[str, Dict[str, Dict[str, Any]]]) -> None:
    for case_name, case in cases.items():
        expected_modes = [mode for mode in ("ocr", "api") if isinstance(case.get(mode), dict)]
        tags = case.get("tags") or []
        print(
            json.dumps(
                {
                    "case": case_name,
                    "path": case.get("path"),
                    "modes": expected_modes,
                    "tags": tags,
                    "notes": case.get("notes"),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )


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
    ap.add_argument("--list-cases", action="store_true", help="Print cases from the selected golden manifest and exit")
    ap.add_argument("--allow-missing", action="store_true", help="Skip missing manifest files instead of failing immediately")
    ap.add_argument("--case", action="append", default=None, help="Run only this manifest case name. Can be repeated.")
    ap.add_argument("--max-cases", type=int, default=None, help="Run only the first N selected cases")
    ap.add_argument("--slow-ms", type=float, default=60000.0, help="Print a warning when a row takes longer than this")
    ap.add_argument(
        "--allow-api-writes",
        action="store_true",
        help="Allow API mode. /events/photo writes photo_events and may write meter_readings.",
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

    if args.list_cases:
        if not golden_cases:
            manifest_path = manifest_path or DEFAULT_GOLDEN_MANIFEST
            golden_cases = _load_golden_manifest(manifest_path)
        _print_manifest_cases(golden_cases)
        return 0

    if args.mode in ("api", "both") and not args.allow_api_writes:
        raise SystemExit(
            "API mode calls /events/photo and writes photo_events/meter_readings. "
            "Pass --allow-api-writes when that mutation is intentional."
        )

    dataset_root = Path(args.dataset_root).expanduser().resolve() if args.dataset_root else None
    file_cases = _collect_files(
        args.paths,
        golden_cases,
        manifest_path=manifest_path,
        dataset_root=dataset_root,
    )
    if args.case:
        selected_names = {str(v) for v in args.case}
        file_cases = [(path, case_name) for path, case_name in file_cases if case_name in selected_names]
        missing_names = sorted(selected_names - {case_name for _path, case_name in file_cases})
        if missing_names:
            raise SystemExit(f"selected manifest cases not found: {missing_names}")
    files = [path for path, _case_name in file_cases]
    missing = [str(p) for p in files if not p.exists()]
    if missing:
        if not args.allow_missing:
            raise SystemExit(f"missing files: {missing}")
        for path in missing:
            print(f"SKIPPED_MISSING: {path}", file=sys.stderr)
        file_cases = [(path, case_name) for path, case_name in file_cases if path.exists()]
        if not file_cases:
            raise SystemExit("all selected files are missing")
    if args.max_cases is not None:
        file_cases = file_cases[: max(0, int(args.max_cases))]
        if not file_cases:
            raise SystemExit("--max-cases selected zero runnable cases")

    all_mismatches: List[str] = []
    elapsed_values: List[float] = []
    checked_rows = 0
    for path, case_name in file_cases:
        golden_scope = golden_cases.get(case_name, {})
        if args.mode in ("ocr", "both"):
            row = _ocr_row(args.ocr_endpoint, path, args.timeout_sec, case_name=case_name, case=golden_scope)
            checked_rows += 1
            elapsed_values.append(float(row.get("elapsed_ms") or 0.0))
            print(json.dumps(row, ensure_ascii=False), flush=True)
            if float(row.get("elapsed_ms") or 0.0) > float(args.slow_ms):
                print(
                    f"SLOW_ROW: case={case_name} mode=ocr elapsed_ms={row.get('elapsed_ms')} openai_calls={row.get('openai_calls')}",
                    file=sys.stderr,
                    flush=True,
                )
            if golden_scope:
                all_mismatches.extend([f"{case_name}: {m}" for m in _check_golden_row(row, golden_scope)])
        if args.mode in ("api", "both"):
            row = _api_row(args.api_endpoint, path, args.timeout_sec, args.chat_id, args.ym, case_name=case_name)
            checked_rows += 1
            elapsed_values.append(float(row.get("elapsed_ms") or 0.0))
            print(
                json.dumps(
                    row,
                    ensure_ascii=False,
                ),
                flush=True,
            )
            if float(row.get("elapsed_ms") or 0.0) > float(args.slow_ms):
                print(
                    f"SLOW_ROW: case={case_name} mode=api elapsed_ms={row.get('elapsed_ms')} meter_written={row.get('meter_written')}",
                    file=sys.stderr,
                    flush=True,
                )
            if golden_scope:
                all_mismatches.extend([f"{case_name}: {m}" for m in _check_golden_row(row, golden_scope)])
    if all_mismatches:
        for mismatch in all_mismatches:
            print(f"GOLDEN_MISMATCH: {mismatch}", file=sys.stderr, flush=True)
        return 1
    if golden_cases:
        print(f"GOLDEN_OK: rows={checked_rows} cases={len(file_cases)} mode={args.mode}", file=sys.stderr, flush=True)
    if elapsed_values:
        total_ms = round(sum(elapsed_values), 1)
        max_ms = round(max(elapsed_values), 1)
        avg_ms = round(total_ms / max(1, len(elapsed_values)), 1)
        print(
            f"REGRESSION_TIMING: rows={len(elapsed_values)} total_ms={total_ms} avg_ms={avg_ms} max_ms={max_ms}",
            file=sys.stderr,
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
