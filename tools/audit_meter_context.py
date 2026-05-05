#!/usr/bin/env python3
import argparse
import json
import math
import subprocess
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional


CONTAINER_NAME = "rent_api"


def _run_container_query(selector: Dict[str, Any]) -> Dict[str, Any]:
    payload = json.dumps(selector, ensure_ascii=False)
    script = r'''
import json
import os
from sqlalchemy import create_engine, text
from core.config import DATABASE_URL

selector = json.loads(os.environ["AUDIT_SELECTOR_JSON"])
engine = create_engine(DATABASE_URL)

chat_id = str(selector.get("chat_id") or "").strip()
apartment_id = selector.get("apartment_id")
ym = str(selector.get("ym") or "").strip()
limit = int(selector.get("limit") or 80)

with engine.connect() as conn:
    apt = None
    if apartment_id is not None:
        apt = conn.execute(
            text("""
                SELECT id, title, address, cold_serial, hot_serial
                FROM apartments
                WHERE id=:aid
                LIMIT 1
            """),
            {"aid": int(apartment_id)},
        ).mappings().first()
    elif chat_id:
        apt = conn.execute(
            text("""
                SELECT a.id, a.title, a.address, a.cold_serial, a.hot_serial
                FROM chat_bindings b
                JOIN apartments a ON a.id=b.apartment_id
                WHERE b.chat_id=:chat_id AND b.is_active=true
                LIMIT 1
            """),
            {"chat_id": chat_id},
        ).mappings().first()

    aid = int(apt["id"]) if apt else None

    readings = []
    if aid is not None:
        where_ym = "AND ym <= :ym" if ym else ""
        params = {"aid": aid, "limit": limit}
        if ym:
            params["ym"] = ym
        readings = [
            dict(row)
            for row in conn.execute(
                text(f"""
                    SELECT id, apartment_id, ym, meter_type, meter_index, value, source, ocr_value, created_at, updated_at
                    FROM meter_readings
                    WHERE apartment_id=:aid
                    {where_ym}
                    ORDER BY meter_type, meter_index, ym, id
                    LIMIT :limit
                """),
                params,
            ).mappings().all()
        ]

    event_where = []
    event_params = {"limit": limit}
    if aid is not None:
        event_where.append("apartment_id=:aid")
        event_params["aid"] = aid
    if chat_id:
        event_where.append("chat_id=:chat_id")
        event_params["chat_id"] = chat_id
    where_sql = ("WHERE " + " OR ".join(event_where)) if event_where else ""
    events = [
        dict(row)
        for row in conn.execute(
            text(f"""
                SELECT
                    id, created_at, chat_id, apartment_id, ym, stage, status,
                    original_filename, ydisk_path, file_sha256, ocr_type, ocr_reading,
                    meter_kind, meter_value, meter_written, meter_index,
                    ocr_json, diag_json
                FROM photo_events
                {where_sql}
                ORDER BY id DESC
                LIMIT :limit
            """),
            event_params,
        ).mappings().all()
    ]

print(json.dumps({"apartment": dict(apt) if apt else None, "readings": readings, "events": events}, ensure_ascii=False, default=str))
'''
    cmd = [
        "docker",
        "exec",
        "-e",
        f"AUDIT_SELECTOR_JSON={payload}",
        str(selector.get("container") or CONTAINER_NAME),
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


def _as_float(value: Any) -> Optional[float]:
    try:
        v = float(value)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


def _summarize_event(row: Dict[str, Any]) -> Dict[str, Any]:
    diag = row.get("diag_json") if isinstance(row.get("diag_json"), dict) else {}
    ocr = row.get("ocr_json") if isinstance(row.get("ocr_json"), dict) else {}
    selected = diag.get("selected_file") if isinstance(diag.get("selected_file"), dict) else {}
    water = ocr.get("water_decision") if isinstance(ocr.get("water_decision"), dict) else {}
    winner = water.get("winner") if isinstance(water.get("winner"), dict) else {}
    warning_codes: List[str] = []
    for warning in list(diag.get("warnings") or []):
        if isinstance(warning, str):
            warning_codes.append(warning)
        elif isinstance(warning, dict):
            for key, value in warning.items():
                warning_codes.append(str(key))
                if isinstance(value, dict) and value.get("reason"):
                    warning_codes.append(f"{key}:{value.get('reason')}")
    warning_codes = sorted(set(warning_codes))
    return {
        "id": row.get("id"),
        "created_at": row.get("created_at"),
        "ym": row.get("ym"),
        "stage": row.get("stage"),
        "status": row.get("status"),
        "telegram_message_id": diag.get("telegram_message_id"),
        "telegram_media_group_id": diag.get("telegram_media_group_id"),
        "client_batch": diag.get("client_batch"),
        "original_filename": row.get("original_filename"),
        "selected_file": selected,
        "file_sha16": (str(row.get("file_sha256") or "")[:16] or None),
        "ocr_type": row.get("ocr_type") or ocr.get("type"),
        "ocr_reading": row.get("ocr_reading") if row.get("ocr_reading") is not None else ocr.get("reading"),
        "ocr_serial": ocr.get("serial"),
        "meter_kind": row.get("meter_kind"),
        "meter_value": row.get("meter_value"),
        "meter_written": row.get("meter_written"),
        "winner_source": winner.get("source"),
        "winner_reading": winner.get("reading"),
        "warning_codes": warning_codes,
        "ydisk_path": row.get("ydisk_path"),
    }


def _reading_lookup_key(row: Dict[str, Any]) -> tuple:
    return (
        str(row.get("meter_type") or row.get("meter_kind") or ""),
        int(row.get("meter_index") or 1),
        str(row.get("ym") or ""),
        round(float(_as_float(row.get("value") if "value" in row else row.get("meter_value")) or 0.0), 3),
    )


def _matching_events_for_reading(reading: Dict[str, Any], events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    value = _as_float(reading.get("value"))
    if value is None:
        return []
    out: List[Dict[str, Any]] = []
    mt = str(reading.get("meter_type") or "")
    mi = int(reading.get("meter_index") or 1)
    ym = str(reading.get("ym") or "")
    for event in events:
        ev_value = _as_float(event.get("meter_value"))
        if ev_value is None or abs(float(ev_value) - float(value)) > 0.005:
            continue
        if str(event.get("meter_kind") or "") != mt:
            continue
        try:
            ev_index = int(event.get("meter_index") or 1)
        except Exception:
            ev_index = 1
        if ev_index != mi:
            continue
        if ym and str(event.get("ym") or "") != ym:
            continue
        out.append(
            {
                "photo_event_id": event.get("id"),
                "created_at": event.get("created_at"),
                "ocr": f"{event.get('ocr_type')}/{event.get('ocr_reading')}",
                "winner_source": event.get("winner_source"),
                "warning_codes": event.get("warning_codes") or [],
                "ydisk_path": event.get("ydisk_path"),
                "sha16": event.get("file_sha16"),
            }
        )
    return out


def _build_cleanup_candidates(
    readings: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
    reading_flags: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_id = {int(row.get("id")): row for row in readings if row.get("id") is not None}
    flagged_ids: Dict[int, List[str]] = defaultdict(list)
    for flag in reading_flags:
        current = flag.get("current") if isinstance(flag.get("current"), dict) else {}
        prev = flag.get("prev") if isinstance(flag.get("prev"), dict) else {}
        if current.get("id") is not None:
            flagged_ids[int(current["id"])].append(str(flag.get("kind") or "sequence_flag"))
        # If a very large decrease follows a previous OCR value, the previous point may be the pollution.
        if str(flag.get("kind")) == "decrease" and prev.get("id") is not None and abs(float(flag.get("delta") or 0.0)) >= 300.0:
            flagged_ids[int(prev["id"])].append("possible_prior_pollution_before_decrease")

    candidates: List[Dict[str, Any]] = []
    high_risk_warning_tokens = {
        "anomaly_jump",
        "anomaly_saved_with_review",
        "water_prev_sanity_saved_with_review",
        "water_serial_prev_saved_with_review",
        "water_prev_sanity_blocked",
        "serial_mismatch",
        "water_type_unresolved_review_only",
        "electric_tariff_index_exceeds_expected_review",
    }
    for reading_id, reasons in sorted(flagged_ids.items()):
        row = by_id.get(reading_id)
        if not row:
            continue
        value = _as_float(row.get("value"))
        matches = _matching_events_for_reading(row, events)
        event_warnings = sorted({w for ev in matches for w in list(ev.get("warning_codes") or [])})
        source = str(row.get("source") or "")
        risk = "review"
        if source == "ocr" and any(any(token in warning for token in high_risk_warning_tokens) for warning in event_warnings):
            risk = "high"
        elif source == "ocr":
            risk = "medium"
        candidates.append(
            {
                "risk": risk,
                "reading_id": reading_id,
                "apartment_id": row.get("apartment_id"),
                "ym": row.get("ym"),
                "meter_type": row.get("meter_type"),
                "meter_index": row.get("meter_index"),
                "value": value,
                "source": source,
                "ocr_value": _as_float(row.get("ocr_value")),
                "reasons": sorted(set(reasons)),
                "matching_photo_events": matches,
                "suggested_action": "manual_review_then_correct_or_delete",
                "safe_select_sql": (
                    "SELECT * FROM meter_readings "
                    f"WHERE id={int(reading_id)};"
                ),
                "commented_delete_sql": (
                    "-- DELETE FROM meter_readings "
                    f"WHERE id={int(reading_id)};"
                ),
            }
        )
    candidates.sort(key=lambda item: ({"high": 0, "medium": 1, "review": 2}.get(str(item.get("risk")), 3), str(item.get("meter_type")), int(item.get("meter_index") or 1), str(item.get("ym"))))
    return candidates


def _build_audit(payload: Dict[str, Any], *, water_jump: float, electric_jump: float) -> Dict[str, Any]:
    readings = list(payload.get("readings") or [])
    by_meter: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for row in readings:
        by_meter[(row.get("meter_type"), row.get("meter_index"))].append(row)

    reading_flags: List[Dict[str, Any]] = []
    for (meter_type, meter_index), rows in by_meter.items():
        prev = None
        for row in sorted(rows, key=lambda r: (str(r.get("ym") or ""), int(r.get("id") or 0))):
            value = _as_float(row.get("value"))
            if value is None:
                continue
            if prev is not None:
                prev_value = _as_float(prev.get("value"))
                if prev_value is not None:
                    delta = value - prev_value
                    threshold = electric_jump if str(meter_type) == "electric" else water_jump
                    if delta < -0.01:
                        reading_flags.append(
                            {
                                "kind": "decrease",
                                "meter_type": meter_type,
                                "meter_index": meter_index,
                                "prev": {"id": prev.get("id"), "ym": prev.get("ym"), "value": prev_value},
                                "current": {"id": row.get("id"), "ym": row.get("ym"), "value": value},
                                "delta": round(delta, 3),
                            }
                        )
                    elif abs(delta) > threshold:
                        reading_flags.append(
                            {
                                "kind": "large_jump",
                                "meter_type": meter_type,
                                "meter_index": meter_index,
                                "prev": {"id": prev.get("id"), "ym": prev.get("ym"), "value": prev_value},
                                "current": {"id": row.get("id"), "ym": row.get("ym"), "value": value},
                                "delta": round(delta, 3),
                                "threshold": threshold,
                            }
                        )
            prev = row

    events = [_summarize_event(row) for row in list(payload.get("events") or [])]
    by_sha: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for event in events:
        sha = str(event.get("file_sha16") or "")
        if sha:
            by_sha[sha].append(event)
    repeated_files = [
        {
            "sha16": sha,
            "count": len(rows),
            "events": [
                {
                    "id": r.get("id"),
                    "message_id": r.get("telegram_message_id"),
                    "batch": r.get("client_batch"),
                    "reading": r.get("ocr_reading"),
                    "kind": r.get("meter_kind"),
                    "filename": r.get("original_filename"),
                }
                for r in rows
            ],
        }
        for sha, rows in sorted(by_sha.items())
        if len(rows) > 1
    ]
    cleanup_candidates = _build_cleanup_candidates(readings, events, reading_flags)

    return {
        "apartment": payload.get("apartment"),
        "reading_flags": reading_flags,
        "cleanup_candidates": cleanup_candidates,
        "repeated_file_sha": repeated_files,
        "latest_events": events,
    }


def _print_pretty(audit: Dict[str, Any]) -> None:
    apt = audit.get("apartment") or {}
    print(
        "apartment: "
        f"id={apt.get('id')} title={apt.get('title')} cold_serial={apt.get('cold_serial')} hot_serial={apt.get('hot_serial')}"
    )
    flags = list(audit.get("reading_flags") or [])
    print(f"reading flags: {len(flags)}")
    for flag in flags[:20]:
        print(
            f"  {flag.get('kind')}: {flag.get('meter_type')}#{flag.get('meter_index')} "
            f"{flag.get('prev')} -> {flag.get('current')} delta={flag.get('delta')}"
        )
    cleanup = list(audit.get("cleanup_candidates") or [])
    print(f"cleanup candidates: {len(cleanup)}")
    for item in cleanup[:20]:
        matches = item.get("matching_photo_events") or []
        match_ids = [m.get("photo_event_id") for m in matches[:4]]
        print(
            f"  [{item.get('risk')}] reading_id={item.get('reading_id')} "
            f"{item.get('meter_type')}#{item.get('meter_index')} ym={item.get('ym')} "
            f"value={item.get('value')} source={item.get('source')} "
            f"reasons={item.get('reasons')} photo_events={match_ids}"
        )
    repeated = list(audit.get("repeated_file_sha") or [])
    print(f"repeated file sha groups: {len(repeated)}")
    for group in repeated[:12]:
        print(f"  sha16={group.get('sha16')} count={group.get('count')} events={group.get('events')}")
    print("latest events:")
    for event in list(audit.get("latest_events") or [])[:20]:
        selected = event.get("selected_file") or {}
        print(
            f"  #{event.get('id')} msg={event.get('telegram_message_id')} batch={event.get('client_batch')} "
            f"sha={event.get('file_sha16')} selected_sha={selected.get('sha16')} "
            f"{event.get('meter_kind')}={event.get('meter_value')} ocr={event.get('ocr_type')}/{event.get('ocr_reading')} "
            f"winner={event.get('winner_source')} file={event.get('original_filename')}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only audit for OCR context pollution and Telegram photo binding")
    ap.add_argument("--chat-id", default=None, help="Telegram chat id")
    ap.add_argument("--apartment-id", type=int, default=None, help="Apartment id")
    ap.add_argument("--ym", default=None, help="Only consider readings up to YYYY-MM")
    ap.add_argument("--limit", type=int, default=80)
    ap.add_argument("--container", default=CONTAINER_NAME)
    ap.add_argument("--water-jump", type=float, default=80.0)
    ap.add_argument("--electric-jump", type=float, default=1000.0)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    if not args.chat_id and args.apartment_id is None:
        print("provide --chat-id or --apartment-id", file=sys.stderr)
        return 2
    payload = _run_container_query(
        {
            "chat_id": args.chat_id,
            "apartment_id": args.apartment_id,
            "ym": args.ym,
            "limit": args.limit,
            "container": args.container,
        }
    )
    audit = _build_audit(payload, water_jump=float(args.water_jump), electric_jump=float(args.electric_jump))
    if args.json:
        print(json.dumps(audit, ensure_ascii=False, indent=2, default=str))
    else:
        _print_pretty(audit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
