#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from typing import Any, Dict, List, Optional


CONTAINER_NAME = "rent_api"


def _run_container_query(selector: Dict[str, Any]) -> Dict[str, Any]:
    payload = json.dumps(selector, ensure_ascii=False)
    script = r'''
import json
import os
from sqlalchemy import create_engine, text
from core.config import DATABASE_URL

selector = json.loads(os.environ["TRACE_SELECTOR_JSON"])
engine = create_engine(DATABASE_URL)

where = ""
params = {}
if selector.get("event_id") is not None:
    where = "WHERE pe.id = :event_id"
    params["event_id"] = int(selector["event_id"])
elif selector.get("trace_id"):
    where = "WHERE COALESCE(diag_json->>'trace_id', '') = :trace_id OR COALESCE(diag_json->>'ocr_trace_id', '') = :trace_id OR COALESCE(ocr_json->>'trace_id', '') = :trace_id"
    params["trace_id"] = str(selector["trace_id"])
else:
    limit = int(selector.get("latest") or 1)
    where = ""
    params["limit"] = limit

sql = f"""
SELECT
    pe.id,
    pe.created_at,
    pe.chat_id,
    pe.telegram_username,
    pe.apartment_id,
    a.title AS apartment_title,
    pe.ym,
    pe.stage,
    pe.status,
    pe.original_filename,
    pe.file_sha256,
    pe.ocr_type,
    pe.ocr_reading,
    pe.meter_kind,
    pe.meter_value,
    pe.meter_written,
    pe.meter_index,
    pe.ocr_json,
    pe.diag_json
FROM photo_events pe
LEFT JOIN apartments a ON a.id = pe.apartment_id
{where}
ORDER BY pe.id DESC
"""
if "limit" in params:
    sql += "\nLIMIT :limit"

with engine.connect() as conn:
    rows = [dict(row) for row in conn.execute(text(sql), params).mappings().all()]

print(json.dumps({"rows": rows}, ensure_ascii=False, default=str))
'''
    cmd = [
        "docker",
        "exec",
        "-e",
        f"TRACE_SELECTOR_JSON={payload}",
        CONTAINER_NAME,
        "python",
        "-c",
        script,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or proc.stdout.strip() or f"docker exec failed with code {proc.returncode}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"failed to parse container output: {exc}\n{proc.stdout}") from exc


def _top_ranked(decision: Dict[str, Any], limit: int = 5) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in list(decision.get("ranked") or [])[:limit]:
        out.append(
            {
                "source": item.get("source"),
                "reading": item.get("reading"),
                "serial": item.get("serial"),
                "score": item.get("candidate_score"),
                "flags": item.get("suspicious_flags") or [],
                "penalty": item.get("serial_overlap_penalty"),
            }
        )
    return out


def _compact_row(row: Dict[str, Any]) -> Dict[str, Any]:
    ocr = row.get("ocr_json") or {}
    diag = row.get("diag_json") or {}
    water_decision = ocr.get("water_decision") or {}
    summary = dict((water_decision.get("summary") or {}))
    review_trace = list(diag.get("api_review_trace") or [])
    review_gate = None
    if review_trace:
        for step in reversed(review_trace):
            if isinstance(step, dict) and step.get("phase") in {"review_gate", "final_decision"}:
                review_gate = step
                break
    return {
        "event": {
            "id": row.get("id"),
            "created_at": row.get("created_at"),
            "chat_id": row.get("chat_id"),
            "telegram_username": row.get("telegram_username"),
            "apartment_id": row.get("apartment_id"),
            "apartment_title": row.get("apartment_title"),
            "ym": row.get("ym"),
            "stage": row.get("stage"),
            "status": row.get("status"),
            "original_filename": row.get("original_filename"),
            "file_sha256": row.get("file_sha256"),
            "telegram_message_id": diag.get("telegram_message_id"),
            "telegram_media_group_id": diag.get("telegram_media_group_id"),
            "client_batch": diag.get("client_batch"),
            "selected_file": diag.get("selected_file"),
        },
        "ocr": {
            "trace_id": (diag.get("trace_id") or ocr.get("trace_id")),
            "ocr_trace_id": diag.get("ocr_trace_id"),
            "type": row.get("ocr_type") or ocr.get("type"),
            "reading": row.get("ocr_reading") if row.get("ocr_reading") is not None else ocr.get("reading"),
            "serial": ocr.get("serial"),
            "resolved_kind": ocr.get("resolved_kind"),
            "resolved_type": ocr.get("resolved_type"),
            "meter_kind": row.get("meter_kind"),
            "meter_value": row.get("meter_value"),
            "meter_written": row.get("meter_written"),
            "meter_index": row.get("meter_index"),
            "openai_calls": ocr.get("openai_calls"),
            "provider_errors": ocr.get("provider_errors") or diag.get("ocr_provider_errors"),
        },
        "local_recognizer": diag.get("ocr_local_recognizer") or ocr.get("local_recognizer"),
        "water_decision": {
            "summary": summary or None,
            "winner": water_decision.get("winner"),
            "serial_branch": water_decision.get("serial_branch"),
            "odometer_branch": water_decision.get("odometer_branch"),
            "top_ranked": _top_ranked(water_decision),
        }
        if water_decision
        else None,
        "review": {
            "warnings": diag.get("warnings") or [],
            "errors": diag.get("errors") or [],
            "review_gate": review_gate,
            "api_review_trace": review_trace,
        },
    }


def _print_pretty(item: Dict[str, Any]) -> None:
    event = item["event"]
    ocr = item["ocr"]
    water = item.get("water_decision") or {}
    review = item["review"]
    print(f"photo_event #{event['id']}  apartment={event.get('apartment_title') or event.get('apartment_id')}  ym={event.get('ym')}")
    print(f"trace_id={ocr.get('trace_id')}  ocr_trace_id={ocr.get('ocr_trace_id')}")
    print(
        "ocr: "
        f"type={ocr.get('type')} reading={ocr.get('reading')} serial={ocr.get('serial')} "
        f"resolved={ocr.get('resolved_type') or ocr.get('resolved_kind')} written={ocr.get('meter_written')}"
    )
    print(
        "telegram: "
        f"message_id={event.get('telegram_message_id')} "
        f"media_group_id={event.get('telegram_media_group_id')} "
        f"batch={event.get('client_batch')} "
        f"sha={event.get('file_sha256')}"
    )
    if event.get("selected_file"):
        sf = event.get("selected_file") or {}
        print(
            "selected file: "
            f"index={sf.get('index')} reason={sf.get('selection_reason')} "
            f"filename={sf.get('filename')} sha16={sf.get('sha16')} "
            f"client_sha16={sf.get('client_sha16')} unique={sf.get('client_file_unique_id')}"
        )
    if ocr.get("provider_errors"):
        print(f"provider errors: {ocr.get('provider_errors')}")
    local = item.get("local_recognizer") or {}
    if isinstance(local, dict) and local:
        print(
            "local recognizer: "
            f"status={local.get('status')} "
            f"model={local.get('digit_classifier_enabled')}:{local.get('digit_classifier_version')} "
            f"winner={local.get('winner')} "
            f"top_water={local.get('top_water')} "
            f"top_electric={local.get('top_electric')}"
        )
    summary = water.get("summary") or {}
    if summary:
        print(
            "water summary: "
            f"winner={summary.get('winner')} serial_branch={summary.get('serial_branch_winner')} "
            f"odometer_branch={summary.get('odometer_branch_winner')} "
            f"serial_tail_like={summary.get('serial_tail_like')} "
            f"context_override={summary.get('context_override_applied')}"
        )
    if water.get("top_ranked"):
        print("top ranked:")
        for idx, cand in enumerate(water["top_ranked"], start=1):
            print(
                f"  {idx}. source={cand.get('source')} reading={cand.get('reading')} "
                f"serial={cand.get('serial')} score={cand.get('score')} flags={cand.get('flags')}"
            )
    if review.get("review_gate"):
        phase = review["review_gate"].get("phase")
        decision = review["review_gate"].get("decision")
        print(f"review gate: phase={phase} decision={decision}")
    if review.get("warnings"):
        print(f"warnings: {review['warnings']}")
    if review.get("errors"):
        print(f"errors: {review['errors']}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Show structured OCR/API trace for a meter photo event")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--event-id", type=int, help="Exact photo_events.id")
    group.add_argument("--trace-id", help="trace_id from bot/API/OCR")
    group.add_argument("--latest", type=int, nargs="?", const=1, help="Show latest N photo_events (default: 1)")
    ap.add_argument("--json", action="store_true", help="Print JSON instead of compact text")
    args = ap.parse_args()

    selector = {"event_id": args.event_id, "trace_id": args.trace_id, "latest": args.latest}
    payload = _run_container_query(selector)
    rows = list(payload.get("rows") or [])
    if not rows:
        print("No photo_events matched", file=sys.stderr)
        return 1
    compact = [_compact_row(row) for row in rows]
    if args.json:
        print(json.dumps(compact, ensure_ascii=False, indent=2, default=str))
        return 0
    for idx, item in enumerate(compact, start=1):
        if idx > 1:
            print("\n---")
        _print_pretty(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
