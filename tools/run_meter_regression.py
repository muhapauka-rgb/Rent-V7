#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


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
    return {
        "mode": "ocr",
        "file": path.name,
        "type": payload.get("type"),
        "reading": payload.get("reading"),
        "serial": payload.get("serial"),
        "openai_calls": payload.get("openai_calls"),
        "winner_source": winner.get("source"),
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
        "winner_score": winner.get("candidate_score"),
        "top_candidates": _top_candidates(water_decision),
        "review_gate": water_decision.get("review_gate"),
        "warnings": diag.get("warnings"),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run focused OCR/API regression checks for meter photos")
    ap.add_argument("paths", nargs="+", help="Absolute or relative image paths to test")
    ap.add_argument("--mode", choices=("ocr", "api", "both"), default="ocr")
    ap.add_argument("--ocr-endpoint", default="http://127.0.0.1:8002/recognize")
    ap.add_argument("--api-endpoint", default="http://127.0.0.1:8001/events/photo")
    ap.add_argument("--chat-id", default="206724330")
    ap.add_argument("--ym", default="2026-04")
    ap.add_argument("--timeout-sec", type=float, default=180.0)
    args = ap.parse_args()

    files = [Path(p).expanduser().resolve() for p in args.paths]
    missing = [str(p) for p in files if not p.exists()]
    if missing:
        raise SystemExit(f"missing files: {missing}")

    for path in files:
        if args.mode in ("ocr", "both"):
            print(json.dumps(_ocr_row(args.ocr_endpoint, path, args.timeout_sec), ensure_ascii=False))
        if args.mode in ("api", "both"):
            print(
                json.dumps(
                    _api_row(args.api_endpoint, path, args.timeout_sec, args.chat_id, args.ym),
                    ensure_ascii=False,
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
