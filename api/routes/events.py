import json
import re
import hashlib
import requests
import math
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import JSONResponse
from sqlalchemy import text
from datetime import datetime

from core.config import OCR_URL, engine, logger
from core.db import db_ready, ensure_tables
from core.integrations import ydisk_ready, upload_to_ydisk, _tg_send_message
from core.billing import (
    month_now,
    is_ym,
    _calc_month_bill,
    _get_month_bill_state,
    _set_month_bill_state,
    _same_total,
)
from core.meters import (
    _write_electric_explicit,
    _assign_and_write_electric_sorted,
    _write_electric_overwrite_then_sort,
    _write_water_ocr_with_uncertainty,
    _has_open_water_uncertain_flag,
)
from core.admin_helpers import (
    find_apartment_by_chat,
    find_apartment_by_contact,
    bind_chat,
    _set_contact,
    _upsert_month_statuses,
    _ocr_to_kind,
    _parse_reading_to_float,
    _normalize_serial,
    update_apartment_statuses,
)
from core.schemas import UIStatusesPatch

router = APIRouter()
WATER_TYPE_CONF_MIN = 0.7
WATER_RETAKE_THRESHOLD = 1.0
ELECTRIC_RETAKE_THRESHOLD = 5.0
WATER_ANOMALY_THRESHOLD = 50.0
ELECTRIC_ANOMALY_THRESHOLD = 500.0
WATER_SERIAL_HARD_DELTA = float(os.getenv("WATER_SERIAL_HARD_DELTA", "80.0"))
ENABLE_AGGRESSIVE_OCR_AUTOFIX = os.getenv("ENABLE_AGGRESSIVE_OCR_AUTOFIX", "0").strip().lower() in ("1", "true", "yes", "on")
OCR_HTTP_TIMEOUT_SEC = float(os.getenv("OCR_HTTP_TIMEOUT_SEC", "75"))
OCR_HTTP_TIMEOUT_FLOOR_SEC = float(os.getenv("OCR_HTTP_TIMEOUT_FLOOR_SEC", "70"))
OCR_HTTP_RETRIES = int(os.getenv("OCR_HTTP_RETRIES", "1"))
WATER_INTEGER_ONLY = os.getenv("WATER_INTEGER_ONLY", "0").strip().lower() in ("1", "true", "yes", "on")
OCR_SERIES_HTTP_TIMEOUT_SEC = float(os.getenv("OCR_SERIES_HTTP_TIMEOUT_SEC", "220"))
OCR_SERIES_SINGLE_REPEATS = max(1, min(5, int(os.getenv("OCR_SERIES_SINGLE_REPEATS", "3"))))
PHOTO_EVENT_MAX_FILES = int(os.getenv("PHOTO_EVENT_MAX_FILES", "6"))


def _as_image_upload_tuple(blob: bytes, filename: str | None, mime_type: str | None):
    name = (filename or "").strip().lower()
    mime = (mime_type or "").strip().lower()

    if mime in ("image/jpeg", "image/jpg"):
        return (filename or "photo.jpg", blob, "image/jpeg")
    if mime == "image/png":
        return (filename or "photo.png", blob, "image/png")
    if mime == "image/webp":
        return (filename or "photo.webp", blob, "image/webp")

    if name.endswith((".jpg", ".jpeg")):
        return (filename or "photo.jpg", blob, "image/jpeg")
    if name.endswith(".png"):
        return (filename or "photo.png", blob, "image/png")
    if name.endswith(".webp"):
        return (filename or "photo.webp", blob, "image/webp")

    # sniff magic bytes
    if blob[:2] == b"\xff\xd8":
        return ("photo.jpg", blob, "image/jpeg")
    if blob[:8] == b"\x89PNG\r\n\x1a\n":
        return ("photo.png", blob, "image/png")
    if blob[:4] == b"RIFF" and blob[8:12] == b"WEBP":
        return ("photo.webp", blob, "image/webp")

    # safe fallback
    return ("photo.jpg", blob, "image/jpeg")


def _call_ocr_with_retries(
    blob: bytes,
    *,
    filename: str | None = None,
    mime_type: str | None = None,
    trace_id: str | None = None,
    context_prev_water: str | None = None,
    context_serial_hint: str | None = None,
    read_timeout_override_sec: float | None = None,
):
    last_exc = None
    upload_file = _as_image_upload_tuple(blob, filename, mime_type)
    post_data: dict[str, str] = {}
    if trace_id:
        post_data["trace_id"] = str(trace_id)
    if context_prev_water:
        post_data["context_prev_water"] = str(context_prev_water)
    if context_serial_hint:
        post_data["context_serial_hint"] = str(context_serial_hint)
    if not post_data:
        post_data = None
    for attempt in range(max(1, OCR_HTTP_RETRIES)):
        try:
            if read_timeout_override_sec is not None:
                read_timeout = max(10.0, float(read_timeout_override_sec))
            else:
                read_timeout = max(float(OCR_HTTP_TIMEOUT_SEC), float(OCR_HTTP_TIMEOUT_FLOOR_SEC))
            resp = requests.post(
                OCR_URL,
                data=post_data,
                files={"file": upload_file},
                timeout=(5, read_timeout),
            )
            return resp, None
        except Exception as e:
            last_exc = e
            if attempt < max(1, OCR_HTTP_RETRIES) - 1:
                time.sleep(0.35 * (attempt + 1))
    return None, last_exc


def _ocr_series_url() -> str:
    url = str(OCR_URL or "").strip().rstrip("/")
    if url.endswith("/recognize"):
        return url[: -len("/recognize")] + "/recognize-series"
    return url + "/recognize-series"


def _call_ocr_series_with_retries(
    photos: list[tuple[bytes, str | None, str | None]],
    *,
    trace_id: str | None = None,
    context_prev_water: str | None = None,
    context_serial_hint: str | None = None,
):
    last_exc = None
    post_data: dict[str, str] = {}
    if trace_id:
        post_data["trace_id"] = str(trace_id)
    if context_prev_water:
        post_data["context_prev_water"] = str(context_prev_water)
    if context_serial_hint:
        post_data["context_serial_hint"] = str(context_serial_hint)
    if not post_data:
        post_data = None

    files_payload = []
    for blob, filename, mime_type in photos:
        files_payload.append(("files", _as_image_upload_tuple(blob, filename, mime_type)))

    for attempt in range(max(1, OCR_HTTP_RETRIES)):
        try:
            base_timeout = max(float(OCR_HTTP_TIMEOUT_SEC), float(OCR_HTTP_TIMEOUT_FLOOR_SEC))
            read_timeout = max(base_timeout, float(OCR_SERIES_HTTP_TIMEOUT_SEC))
            # series calls can be much slower than single-image OCR
            read_timeout = max(read_timeout, min(900.0, base_timeout * max(1, len(photos))))
            resp = requests.post(
                _ocr_series_url(),
                data=post_data,
                files=files_payload,
                timeout=(5, read_timeout),
            )
            return resp, None
        except Exception as e:
            last_exc = e
            if attempt < max(1, OCR_HTTP_RETRIES) - 1:
                time.sleep(0.35 * (attempt + 1))
    return None, last_exc


def _prev_ym(ym: str) -> str:
    try:
        dt = datetime.strptime(ym, "%Y-%m")
    except Exception:
        return ym
    if dt.month == 1:
        return f"{dt.year - 1}-12"
    return f"{dt.year:04d}-{dt.month - 1:02d}"


def _get_prev_reading(conn, apartment_id: int, ym: str, meter_type: str, meter_index: int = 1) -> float | None:
    row = conn.execute(
        text(
            """
            SELECT value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=:mi
            LIMIT 1
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "mt": str(meter_type), "mi": int(meter_index)},
    ).fetchone()
    if not row:
        return None
    try:
        return float(row[0])
    except Exception:
        return None


def _get_last_reading_before(conn, apartment_id: int, ym: str, meter_type: str, meter_index: int = 1) -> float | None:
    row = conn.execute(
        text(
            """
            SELECT value
            FROM meter_readings
            WHERE apartment_id=:aid
              AND ym < :ym
              AND meter_type=:mt
              AND meter_index=:mi
            ORDER BY ym DESC
            LIMIT 1
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "mt": str(meter_type), "mi": int(meter_index)},
    ).fetchone()
    if not row:
        return None
    try:
        return float(row[0])
    except Exception:
        return None


def _get_recent_training_water_values(conn, apartment_id: int, ym: str, limit: int = 36) -> list[float]:
    rows = conn.execute(
        text(
            """
            SELECT correct_value
            FROM ocr_training_samples
            WHERE apartment_id=:aid
              AND ym <= :ym
              AND meter_type IN ('cold','hot')
              AND meter_index=1
              AND correct_value IS NOT NULL
              AND correct_value > 0
            ORDER BY created_at DESC, id DESC
            LIMIT :lim
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "lim": int(max(1, limit))},
    ).fetchall()
    out: list[float] = []
    for row in rows:
        try:
            v = float(row[0])
        except Exception:
            continue
        if not math.isfinite(v) or v <= 0:
            continue
        out.append(v)
    return out


def _select_water_context_values(
    raw_values: list[float],
    *,
    max_values: int = 4,
    support_tol: float = 180.0,
    cluster_only_if_any: bool = False,
) -> list[float]:
    vals: list[float] = []
    for raw in raw_values:
        try:
            v = float(raw)
        except Exception:
            continue
        if not math.isfinite(v) or v <= 0:
            continue
        vals.append(v)
    if not vals:
        return []

    uniq: list[float] = []
    for v in vals:
        if any(abs(v - u) <= 0.01 for u in uniq):
            continue
        uniq.append(v)

    scored: list[tuple[float, int, int, float]] = []
    for v in uniq:
        support = 0
        nearest_idx = len(vals)
        for idx, x in enumerate(vals):
            if abs(x - v) <= support_tol:
                support += 1
            if nearest_idx == len(vals) and abs(x - v) <= 0.01:
                nearest_idx = idx
        score = (float(support) * 10.0) - (0.05 * float(nearest_idx))
        scored.append((score, support, nearest_idx, v))

    scored.sort(key=lambda it: (it[0], it[1], -it[2]), reverse=True)
    has_cluster = any(support >= 2 for _, support, _, _ in scored)
    out: list[float] = []
    if cluster_only_if_any and has_cluster:
        for _score, support, _idx, v in scored:
            if support < 2:
                continue
            if any(abs(v - o) <= 0.05 for o in out):
                continue
            out.append(v)
            if len(out) >= int(max_values):
                return out
        return out
    for clustered_only in ([True, False] if has_cluster else [False]):
        for _score, support, _idx, v in scored:
            if clustered_only and support < 2:
                continue
            if any(abs(v - o) <= 0.05 for o in out):
                continue
            out.append(v)
            if len(out) >= int(max_values):
                return out
    return out


def _parse_prev_values_context(ctx: str | None) -> list[float]:
    if not ctx:
        return []
    out: list[float] = []
    for part in re.split(r"[,\s;]+", str(ctx)):
        p = str(part or "").strip().replace(",", ".")
        if not p:
            continue
        try:
            v = float(p)
        except Exception:
            continue
        if (not math.isfinite(v)) or (v <= 0):
            continue
        out.append(v)
    return out


def _nearest_prev_distance(value: float | None, prev_values: list[float]) -> float:
    if value is None or not prev_values:
        return float("inf")
    try:
        v = float(value)
    except Exception:
        return float("inf")
    return min(abs(v - float(p)) for p in prev_values)


def _series_support_count(value: float, values: list[float], tol: float = 0.08) -> int:
    c = 0
    for x in values:
        try:
            if abs(float(value) - float(x)) <= tol:
                c += 1
        except Exception:
            continue
    return c


def _series_local_score(item: dict, all_items: list[dict], prev_values: list[float]) -> float:
    reading = _parse_reading_to_float(item.get("reading"))
    if reading is None:
        return -999.0
    conf = float(item.get("confidence") or 0.0)
    score = conf
    item_type = str(item.get("type") or "unknown")
    if item_type != "unknown":
        score += 0.03
    notes = str(item.get("notes") or "")
    if "water_no_ok_odometer_winner" in notes:
        score -= 0.45
    if "water_context_far_singleton" in notes:
        score -= 0.65
    if "serial_target_multi_hint_unconfirmed" in notes:
        score -= 0.40

    peers = []
    for x in all_items:
        if x is item:
            continue
        xv = _parse_reading_to_float(x.get("reading"))
        if xv is None:
            continue
        peers.append(float(xv))
    support = _series_support_count(float(reading), peers, tol=0.08)
    score += min(0.28, 0.12 * float(support))

    if prev_values:
        dist = _nearest_prev_distance(float(reading), prev_values)
        if dist > 260.0:
            score -= 0.55
        else:
            score -= min(0.24, dist / 1100.0)
    return float(score)


def _pick_best_series_local(results: list[dict], prev_values: list[float]) -> tuple[int, dict, float]:
    if not results:
        return -1, {}, -999.0
    best_idx = -1
    best_score = -1e9
    best_conf = -1e9
    for i, item in enumerate(results):
        s = _series_local_score(item, results, prev_values)
        conf = float(item.get("confidence") or 0.0)
        if (s > best_score) or (abs(s - best_score) < 1e-9 and conf > best_conf):
            best_idx = i
            best_score = s
            best_conf = conf
    if best_idx < 0:
        return 0, dict(results[0]), -999.0
    return best_idx, dict(results[best_idx]), float(best_score)


def _parse_serial_hints_context(ctx: str | None) -> list[str]:
    if not ctx:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,\s;]+", str(ctx)):
        p = str(part or "").strip()
        if not p:
            continue
        n = _normalize_serial(p)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def _serial_tail_match_len(a: str | None, b: str | None) -> int:
    sa = _normalize_serial(a)
    sb = _normalize_serial(b)
    if not sa or not sb:
        return 0
    m = min(len(sa), len(sb))
    k = 0
    while k < m and sa[-1 - k] == sb[-1 - k]:
        k += 1
    return k


def _result_serial_keys(item: dict, serial_hints: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    s = _normalize_serial(item.get("serial"))
    if s:
        seen.add(s)
        out.append(s)
    notes = str(item.get("notes") or "")
    for g in re.findall(r"\d{4,10}", notes):
        n = _normalize_serial(g)
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    if serial_hints:
        for h in serial_hints:
            tail5 = h[-5:] if len(h) >= 5 else h
            tail4 = h[-4:] if len(h) >= 4 else h
            if (tail5 and tail5 in notes) or (tail4 and tail4 in notes):
                if h not in seen:
                    seen.add(h)
                    out.append(h)
    return out


def _parse_photo_filename_dt(name: str | None) -> datetime | None:
    s = str(name or "").strip()
    if not s:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})[ _](\d{2})\.(\d{2})\.(\d{2})", s)
    if not m:
        return None
    try:
        return datetime.strptime(
            f"{m.group(1)} {m.group(2)}:{m.group(3)}:{m.group(4)}",
            "%Y-%m-%d %H:%M:%S",
        )
    except Exception:
        return None


def _series_item_needs_recovery(item: dict, prev_values: list[float]) -> bool:
    reading = _parse_reading_to_float(item.get("reading"))
    if reading is None:
        return True
    notes = str(item.get("notes") or "")
    if (
        "water_no_ok_odometer_winner" in notes
        or "water_context_far_singleton" in notes
        or "serial_target_multi_hint_unconfirmed" in notes
    ):
        return True
    serial_norm = _normalize_serial(item.get("serial"))
    if _looks_like_serial_reading(reading, serial_norm):
        return True
    if prev_values:
        dist = _nearest_prev_distance(reading, prev_values)
        if dist > 220.0:
            return True
    return False


def _recover_series_missing_with_neighbors(
    results: list[dict],
    *,
    prev_values: list[float],
    serial_hints: list[str],
) -> tuple[list[dict], list]:
    if not results:
        return results, []

    out = [dict(r or {}) for r in results]
    warnings: list = []
    numeric_vals: list[float] = []
    for r in out:
        rv = _parse_reading_to_float(r.get("reading"))
        if rv is not None:
            numeric_vals.append(float(rv))
    # Secondary fallback (without serial agreement) is safe only on tight series ranges.
    range_is_tight = bool(numeric_vals) and ((max(numeric_vals) - min(numeric_vals)) <= 2.5)

    for idx, rec in enumerate(out):
        if not _series_item_needs_recovery(rec, prev_values):
            continue
        rec_dt = _parse_photo_filename_dt(rec.get("filename"))
        target_keys = _result_serial_keys(rec, serial_hints)
        donors: list[tuple[int, int, float, float, int, int, float, dict]] = []
        for j, src in enumerate(out):
            if j == idx:
                continue
            src_reading = _parse_reading_to_float(src.get("reading"))
            if src_reading is None:
                continue
            src_dt = _parse_photo_filename_dt(src.get("filename"))
            if rec_dt and src_dt:
                if rec_dt.date() != src_dt.date():
                    continue
                dt_gap = abs((src_dt - rec_dt).total_seconds())
            elif rec_dt or src_dt:
                # Do not mix timestamped and non-timestamped items.
                continue
            else:
                dt_gap = 0.0
            src_keys = _result_serial_keys(src, serial_hints)
            tail_match = 0
            for ta in target_keys:
                for sb in src_keys:
                    tail_match = max(tail_match, _serial_tail_match_len(ta, sb))
            dist_idx = abs(j - idx)
            conf = float(src.get("confidence") or 0.0)
            ctx_dist = _nearest_prev_distance(float(src_reading), prev_values) if prev_values else 0.0
            stable = 1 if (not _series_item_needs_recovery(src, prev_values)) else 0
            donors.append((dist_idx, -tail_match, ctx_dist, -conf, -stable, j, dt_gap, src))

        if not donors:
            continue

        primary = [d for d in donors if (d[0] <= 1) and ((-d[1]) >= 4) and (d[6] <= 600.0)]
        if primary:
            primary.sort(key=lambda t: (t[0], t[6], t[2], t[3], t[4]))
            chosen = primary[0]
        else:
            adjacent = [d for d in donors if (d[0] <= 1) and (d[4] <= -1) and (d[6] <= 300.0)]
            if adjacent:
                adjacent.sort(key=lambda t: (t[0], t[6], t[2], t[3], t[1]))
                chosen = adjacent[0]
            elif (not range_is_tight) or len(numeric_vals) < 1:
                continue
            secondary = [d for d in donors if (d[0] <= 1) and (d[6] <= 300.0)]
            if not secondary:
                continue
            secondary.sort(key=lambda t: (t[0], t[6], t[2], t[3], t[4]))
            chosen = secondary[0]

        donor_idx = int(chosen[5])
        donor = chosen[7]
        donor_reading = _parse_reading_to_float(donor.get("reading"))
        if donor_reading is None:
            continue
        prev_reading = _parse_reading_to_float(rec.get("reading"))
        rec["reading"] = float(donor_reading)
        rec["type"] = donor.get("type") or rec.get("type") or "unknown"
        donor_conf = float(donor.get("confidence") or 0.0)
        rec["confidence"] = max(float(rec.get("confidence") or 0.0), min(0.72, max(0.45, donor_conf - 0.18)))
        note = str(rec.get("notes") or "").strip()
        rec["notes"] = (
            f"{note}; series_neighbor_recovered(from={donor_idx},prev={prev_reading},to={float(donor_reading):.3f})"
        ).strip("; ").strip()
        warnings.append(
            {
                "series_neighbor_recovered": {
                    "index": int(idx),
                    "from_index": int(donor_idx),
                    "from_reading": float(donor_reading),
                }
            }
        )

    return out, warnings


def _rebuild_series_best_from_payload(
    payload: dict,
    *,
    prev_values: list[float],
    serial_hints: list[str],
) -> dict | None:
    if not isinstance(payload, dict):
        return None
    raw_results = payload.get("results")
    if not isinstance(raw_results, list) or not raw_results:
        return None
    results: list[dict] = []
    for i, row in enumerate(raw_results, start=1):
        rec = dict(row) if isinstance(row, dict) else {}
        rec.setdefault("filename", f"file_{i}.jpg")
        rec.setdefault("type", "unknown")
        rec.setdefault("reading", None)
        rec.setdefault("serial", None)
        rec.setdefault("confidence", 0.0)
        rec.setdefault("notes", "")
        results.append(rec)
    results, recover_warnings = _recover_series_missing_with_neighbors(
        results,
        prev_values=prev_values,
        serial_hints=serial_hints,
    )
    best_idx, best_item, best_score = _pick_best_series_local(results, prev_values)
    return {
        "files_count": len(results),
        "best_index": best_idx,
        "best_score": best_score,
        "best": best_item,
        "results": results,
        "warnings": recover_warnings,
    }


def _choose_single_attempt_result(attempts: list[dict], prev_values: list[float]) -> tuple[dict, list]:
    if not attempts:
        return (
            {
                "type": "unknown",
                "reading": None,
                "serial": None,
                "confidence": 0.0,
                "notes": "",
            },
            [],
        )

    clusters: list[dict] = []
    for idx, item in enumerate(attempts):
        reading = _parse_reading_to_float(item.get("reading"))
        if reading is None:
            continue
        conf = float(item.get("confidence") or 0.0)
        placed = False
        for c in clusters:
            if abs(float(reading) - float(c["value"])) <= 0.08:
                c["count"] += 1
                c["conf_sum"] += conf
                c["members"].append(idx)
                c["value"] = (float(c["value"]) * (c["count"] - 1) + float(reading)) / float(c["count"])
                placed = True
                break
        if not placed:
            clusters.append(
                {
                    "value": float(reading),
                    "count": 1,
                    "conf_sum": conf,
                    "members": [idx],
                }
            )

    warnings: list = []
    if clusters:
        clusters.sort(
            key=lambda c: (
                -int(c["count"]),
                _nearest_prev_distance(float(c["value"]), prev_values),
                -(float(c["conf_sum"]) / float(c["count"])),
            )
        )
        top = clusters[0]
        if int(top["count"]) >= 2:
            cand_idxs = list(top["members"])
            best_idx = max(
                cand_idxs,
                key=lambda i: (
                    _series_local_score(attempts[i], attempts, prev_values),
                    float(attempts[i].get("confidence") or 0.0),
                ),
            )
            picked = dict(attempts[best_idx] or {})
            notes = str(picked.get("notes") or "").strip()
            picked["notes"] = (
                f"{notes}; single_vote(n={len(attempts)},k={int(top['count'])})"
            ).strip("; ").strip()
            warnings.append(
                {
                    "single_vote_selected": {
                        "attempts": len(attempts),
                        "support": int(top["count"]),
                        "reading": _parse_reading_to_float(picked.get("reading")),
                    }
                }
            )
            return picked, warnings

    best_idx, best_item, _best_score = _pick_best_series_local(attempts, prev_values)
    picked = dict(best_item or attempts[max(0, best_idx)] or {})
    if len(attempts) > 1:
        notes = str(picked.get("notes") or "").strip()
        picked["notes"] = f"{notes}; single_best_of={len(attempts)}".strip("; ").strip()
    return picked, warnings


def _call_ocr_series_via_singles(
    photos: list[tuple[bytes, str | None, str | None]],
    *,
    trace_id: str | None,
    context_prev_water: str | None,
    context_serial_hint: str | None,
) -> dict:
    prev_values = _parse_prev_values_context(context_prev_water)
    serial_hints = _parse_serial_hints_context(context_serial_hint)
    indexed_results: dict[int, dict] = {}
    warnings: list = []
    # Single-image fallback should allow slower hard frames; otherwise one timeout can nullify the whole series.
    single_timeout = min(float(OCR_SERIES_HTTP_TIMEOUT_SEC), max(130.0, float(OCR_HTTP_TIMEOUT_SEC)))
    repeat_attempts = int(OCR_SERIES_SINGLE_REPEATS)

    def _one(idx: int, blob: bytes, filename: str | None, mime_type: str | None):
        item_trace = f"{trace_id or 'ocrsf'}-sf{idx+1}"
        name = str(filename or f"file_{idx+1}.jpg")
        local_warnings: list = []
        attempts: list[dict] = []
        for att in range(max(1, repeat_attempts)):
            att_trace = f"{item_trace}-a{att+1}"
            resp, exc = _call_ocr_with_retries(
                blob,
                filename=filename,
                mime_type=mime_type,
                trace_id=att_trace,
                context_prev_water=context_prev_water,
                context_serial_hint=context_serial_hint,
                read_timeout_override_sec=single_timeout,
            )
            if resp is not None and resp.ok:
                try:
                    js = resp.json()
                except Exception:
                    js = None
                if isinstance(js, dict):
                    rec = dict(js)
                else:
                    rec = {}
                    local_warnings.append({"single_bad_json": f"{name}:a{att+1}"})
            else:
                rec = {}
                if exc is not None:
                    local_warnings.append({"single_ocr_error": f"{name}:a{att+1}: {exc}"})
                elif resp is not None:
                    local_warnings.append({"single_ocr_http": f"{name}:a{att+1}: {resp.status_code}"})
            rec.setdefault("filename", name)
            rec.setdefault("type", "unknown")
            rec.setdefault("reading", None)
            rec.setdefault("serial", None)
            rec.setdefault("confidence", 0.0)
            rec.setdefault("notes", "")
            attempts.append(rec)

            # Early stop when two recent attempts agree and result isn't marked suspicious.
            if len(attempts) >= 2:
                cur = attempts[-1]
                prev = attempts[-2]
                cur_r = _parse_reading_to_float(cur.get("reading"))
                prev_r = _parse_reading_to_float(prev.get("reading"))
                if (
                    cur_r is not None
                    and prev_r is not None
                    and abs(float(cur_r) - float(prev_r)) <= 0.08
                    and (not _series_item_needs_recovery(cur, prev_values))
                ):
                    break

        rec, vote_warnings = _choose_single_attempt_result(attempts, prev_values)
        local_warnings.extend(vote_warnings)
        return idx, rec, local_warnings

    # Reliability-first for hard photos: avoid concurrent long OCR calls causing timeouts.
    max_workers = 1
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [
            ex.submit(_one, idx, blob, filename, mime_type)
            for idx, (blob, filename, mime_type) in enumerate(photos)
        ]
        for fut in as_completed(futs):
            idx, rec, ws = fut.result()
            indexed_results[idx] = rec
            warnings.extend(ws)

    results = [indexed_results.get(i, {"filename": str(photos[i][1] or f"file_{i+1}.jpg"), "type": "unknown", "reading": None, "serial": None, "confidence": 0.0, "notes": ""}) for i in range(len(photos))]
    results, recover_warnings = _recover_series_missing_with_neighbors(
        results,
        prev_values=prev_values,
        serial_hints=serial_hints,
    )
    warnings.extend(recover_warnings)

    best_idx, best_item, best_score = _pick_best_series_local(results, prev_values)
    return {
        "trace_id": trace_id or f"ocrsf-{uuid.uuid4().hex[:12]}",
        "files_count": len(results),
        "best_index": best_idx,
        "best_score": best_score,
        "best": best_item,
        "results": results,
        "warnings": warnings,
    }


def _get_last_electric_before(conn, apartment_id: int, ym: str) -> list[float]:
    rows = conn.execute(
        text(
            """
            SELECT value
            FROM meter_readings
            WHERE apartment_id=:aid
              AND ym < :ym
              AND meter_type='electric'
              AND meter_index IN (1,2,3)
            ORDER BY ym DESC
            LIMIT 3
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchall()
    vals = []
    for r in rows:
        try:
            vals.append(float(r[0]))
        except Exception:
            continue
    return vals


def _digits_len(value: float) -> int:
    try:
        v = abs(float(value))
        s = f"{v:.3f}".split(".")[0]
        s = s.lstrip("0") or "0"
        return len(s)
    except Exception:
        return 0


def _insert_one_digit_candidates(value: float, target_digits: int) -> list[float]:
    try:
        v = float(value)
    except Exception:
        return []
    sign = -1.0 if v < 0 else 1.0
    v_abs = abs(v)
    s_int, _, s_frac = f"{v_abs:.3f}".partition(".")
    s_int = s_int.lstrip("0") or "0"
    if len(s_int) >= int(target_digits):
        return [v]
    out: list[float] = []
    for pos in range(0, len(s_int) + 1):
        for d in "0123456789":
            if pos == 0 and d == "0":
                continue
            cand_int = s_int[:pos] + d + s_int[pos:]
            if len(cand_int) != int(target_digits):
                continue
            try:
                cand = sign * float(f"{cand_int}.{s_frac}")
            except Exception:
                continue
            out.append(cand)
    # dedup keep order
    uniq: list[float] = []
    seen = set()
    for x in out:
        k = round(x, 6)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(x)
    return uniq


def _maybe_fix_missing_digit_electric(conn, apartment_id: int, ym: str, value: float) -> tuple[float, dict | None]:
    prev_vals = _get_last_electric_before(conn, int(apartment_id), str(ym))
    if not prev_vals:
        return float(value), None
    raw = float(value)
    raw_digits = _digits_len(raw)
    target_digits = max(_digits_len(v) for v in prev_vals) if prev_vals else raw_digits
    if raw_digits >= target_digits or target_digits <= 0:
        return raw, None
    cands = _insert_one_digit_candidates(raw, target_digits)
    if not cands:
        return raw, None

    def dist(x: float) -> float:
        return min(abs(float(x) - float(p)) for p in prev_vals)

    best_raw = dist(raw)
    best_c = min(cands, key=dist)
    best_c_dist = dist(best_c)

    # apply only when candidate is meaningfully closer to historical values
    if best_c_dist + 100.0 < best_raw:
        return float(best_c), {
            "reason": "auto_fix_missing_digit",
            "prev_candidates": [float(v) for v in prev_vals[:3]],
            "raw": float(raw),
            "fixed": float(best_c),
        }
    return raw, None


def _last5_serial(value: str | None) -> str:
    if not value:
        return ""
    s = _normalize_serial(value)
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) < 5:
        return digits
    return digits[-5:]


def _serial_last5_matches(ocr_last5: str, stored_last5: str) -> bool:
    """
    Fuzzy matching for serial tails:
    - exact last5
    - one-digit difference on same length
    - same last4 for degraded OCR tails
    """
    a = "".join(ch for ch in str(ocr_last5 or "") if ch.isdigit())
    b = "".join(ch for ch in str(stored_last5 or "") if ch.isdigit())
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) == len(b):
        diff = sum(1 for x, y in zip(a, b) if x != y)
        if diff <= 1:
            return True
    if len(a) >= 4 and len(b) >= 4 and a[-4:] == b[-4:]:
        return True
    return False


def _reading_digits(value: float | None) -> str:
    if value is None:
        return ""
    try:
        s = f"{abs(float(value)):.3f}"
    except Exception:
        return ""
    return "".join(ch for ch in s if ch.isdigit())


def _as_water_integer(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        return float(int(float(value)))
    except Exception:
        return None


def _looks_like_serial_reading(reading: float | None, serial_norm: str | None) -> bool:
    rd = _reading_digits(reading)
    sd = "".join(ch for ch in (serial_norm or "") if ch.isdigit())
    if not rd or not sd:
        return False
    rd_nz = rd.lstrip("0")
    sd_nz = sd.lstrip("0")
    # Typical failure: reading copied from serial tail (e.g. 27.214 from ...02714)
    if len(rd) >= 4 and (rd in sd or sd.endswith(rd)):
        return True
    if rd_nz and (rd_nz in sd_nz or sd_nz.endswith(rd_nz)):
        return True
    if len(sd) >= 5 and len(rd) >= 5 and rd.endswith(sd[-4:]):
        return True
    return False


def _black_digits_look_like_serial(black_digits: str | None, serial_norm: str | None) -> bool:
    bd = "".join(ch for ch in str(black_digits or "") if ch.isdigit())
    sd = "".join(ch for ch in str(serial_norm or "") if ch.isdigit())
    if not bd or not sd:
        return False
    bd_nz = bd.lstrip("0")
    sd_nz = sd.lstrip("0")
    if not bd_nz or not sd_nz:
        return False
    return bd_nz in sd_nz or sd_nz.endswith(bd_nz)


def _debug_candidates_from_ocr(ocr_data: dict | None) -> list[dict]:
    if not isinstance(ocr_data, dict):
        return []
    dbg = ocr_data.get("debug")
    if not isinstance(dbg, list):
        return []
    out = []
    for it in dbg:
        if not isinstance(it, dict):
            continue
        try:
            conf = float(it.get("confidence") or 0.0)
        except Exception:
            conf = 0.0
        val = _parse_reading_to_float(it.get("reading"))
        out.append(
            {
                "type": str(it.get("type") or "unknown"),
                "reading": val,
                "confidence": conf,
                "provider": str(it.get("provider") or ""),
                "variant": str(it.get("variant") or ""),
                "black_digits": "".join(ch for ch in str(it.get("black_digits") or "") if ch.isdigit()) or None,
                "red_digits": "".join(ch for ch in str(it.get("red_digits") or "") if ch.isdigit()) or None,
            }
        )
    return out


def _is_odometer_debug_candidate(c: dict) -> bool:
    provider = str(c.get("provider") or "")
    variant = str(c.get("variant") or "")
    return (
        provider.startswith("openai-odo")
        or variant.startswith("odo_")
        or variant.startswith("water_odometer_band_")
        or variant.startswith("counter_row_")
        or variant.startswith("circle_row_")
        or variant.startswith("box_window_")
    )


def _choose_water_debug_candidate_with_prev(
    candidates: list[dict],
    *,
    prev_value: float | None,
    serial_norm: str | None,
    max_delta: float | None = None,
) -> dict | None:
    def _black_with_optional_zero_insert(raw_black: str, has_prev: bool) -> list[str]:
        b = "".join(ch for ch in str(raw_black or "") if ch.isdigit())
        if not b:
            return []
        # Typical OCR miss on drum counters: one inner zero is skipped (e.g. 01003 -> 0103).
        # Only enable this when previous month exists, so we can validate by range/proximity.
        if (not has_prev) or len(b) != 4:
            return [b]
        out = [b]
        for pos in range(1, len(b) + 1):
            cand = b[:pos] + "0" + b[pos:]
            if cand not in out:
                out.append(cand)
        return out

    valid = []
    for c in candidates:
        t = str(c.get("type") or "")
        if t not in ("ХВС", "ГВС", "unknown"):
            continue
        if not _is_odometer_debug_candidate(c):
            continue
        b = "".join(ch for ch in str(c.get("black_digits") or "") if ch.isdigit()) or None
        r = "".join(ch for ch in str(c.get("red_digits") or "") if ch.isdigit()) or None
        # Берём только кандидаты со строкой барабана.
        if not b or len(b) < 4:
            continue
        for b_norm in _black_with_optional_zero_insert(b, prev_value is not None):
            if WATER_INTEGER_ONLY:
                try:
                    v = float(int(b_norm))
                except Exception:
                    continue
            else:
                try:
                    if r and len(r) >= 2:
                        v = float(f"{int(b_norm)}.{r[:3]}")
                    else:
                        # Fallback: when fraction is lost, keep integer instead of dropping candidate.
                        v = float(int(b_norm))
                except Exception:
                    continue
            # защита от "нулей" и слишком маленьких чисел из ложного OCR-окна
            if float(v) <= 0:
                continue
            if _black_digits_look_like_serial(b_norm, serial_norm):
                continue
            if _looks_like_serial_reading(float(v), serial_norm):
                continue
            c_norm = dict(c)
            c_norm["reading"] = float(v)
            c_norm["black_digits"] = b_norm
            c_norm["red_digits"] = (r[:3] if r else None)
            # Tiny penalty for synthetic 0-insert candidates to avoid overriding real exact hits.
            if b_norm != b:
                c_norm["confidence"] = max(0.0, float(c_norm.get("confidence") or 0.0) - 0.05)
                c_norm["notes"] = (
                    f"{str(c_norm.get('notes') or '').strip()}; auto_insert_zero"
                    .strip("; ")
                    .strip()
                )
            valid.append(c_norm)
    if not valid:
        return None

    if prev_value is not None:
        pv = float(prev_value)
        if max_delta is not None:
            md = float(max_delta)
            lower = pv - md
            upper = pv + md
        else:
            lower = pv * 0.6
            upper = pv + 800.0
        ranged = [c for c in valid if lower <= float(c.get("reading")) <= upper]
        # При наличии истории не возвращаем явно нереалистичные кандидаты.
        if not ranged:
            return None
        return min(
            ranged,
            key=lambda c: (
                abs(float(c.get("reading")) - pv),
                -1 if (c.get("black_digits")) else 0,
                -float(c.get("confidence") or 0.0),
            ),
        )

    return max(valid, key=lambda c: float(c.get("confidence") or 0.0))


def _find_close_water(conn, apartment_id: int, ym: str, value: float, threshold: float) -> str | None:
    rows = conn.execute(
        text(
            """
            SELECT meter_type, value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type IN ('cold','hot') AND meter_index=1
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchall()
    best = None
    for mt, v in (rows or []):
        if v is None:
            continue
        try:
            diff = abs(float(v) - float(value))
        except Exception:
            continue
        if diff <= threshold:
            if (best is None) or (diff < best[0]):
                best = (diff, str(mt))
    return best[1] if best else None


def _find_close_electric(conn, apartment_id: int, ym: str, value: float, threshold: float) -> int | None:
    rows = conn.execute(
        text(
            """
            SELECT meter_index, value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric'
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchall()
    best = None
    for mi, v in (rows or []):
        if v is None:
            continue
        try:
            diff = abs(float(v) - float(value))
        except Exception:
            continue
        if diff <= threshold:
            if (best is None) or (diff < best[0]):
                best = (diff, int(mi))
    return best[1] if best else None


def _fraction_digits(raw: str | None) -> int:
    if raw is None:
        return 0
    s = str(raw).strip().replace(" ", "")
    if not s:
        return 0
    m = re.search(r"[.,](\d+)$", s)
    return len(m.group(1)) if m else 0


def _maybe_fix_water_missing_last_decimal(
    conn,
    apartment_id: int,
    ym: str,
    meter_type: str,
    raw_reading: str | None,
    value: float,
) -> tuple[float, dict | None]:
    if meter_type not in ("cold", "hot"):
        return float(value), None
    # Typical water meter has 3 decimal digits.
    if _fraction_digits(raw_reading) != 2:
        return float(value), None

    prev_ym = _prev_ym(str(ym))
    prev_val = _get_prev_reading(conn, int(apartment_id), prev_ym, str(meter_type), 1)
    if prev_val is None:
        prev_val = _get_last_reading_before(conn, int(apartment_id), str(ym), str(meter_type), 1)
    if prev_val is None:
        return float(value), None

    base = math.floor(float(value) * 1000.0) / 1000.0
    candidates = [base + (d / 1000.0) for d in range(10)]
    # keep realistic forward progression (or equal), and close enough to expected dynamics
    viable = [c for c in candidates if (c + 1e-9) >= float(prev_val) and abs(c - float(prev_val)) <= WATER_RETAKE_THRESHOLD]
    if not viable:
        return float(value), None
    best = min(viable, key=lambda c: abs(c - float(prev_val)))
    if abs(float(best) - float(value)) < 1e-9:
        return float(value), None
    return float(best), {
        "reason": "auto_fix_water_missing_last_decimal",
        "raw": float(value),
        "fixed": float(best),
        "prev": float(prev_val),
    }


def _get_same_month_water_values(conn, apartment_id: int, ym: str) -> list[tuple[str, float]]:
    rows = conn.execute(
        text(
            """
            SELECT meter_type, value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type IN ('cold','hot') AND meter_index=1
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchall()
    out = []
    for mt, v in (rows or []):
        if v is None:
            continue
        try:
            out.append((str(mt), float(v)))
        except Exception:
            continue
    return out


def _get_same_month_electric_values(conn, apartment_id: int, ym: str) -> list[float]:
    rows = conn.execute(
        text(
            """
            SELECT value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index IN (1,2,3)
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchall()
    out = []
    for (v,) in (rows or []):
        if v is None:
            continue
        try:
            out.append(float(v))
        except Exception:
            continue
    return out


def _flag_manual_overwrite(
    conn,
    *,
    apartment_id: int,
    ym: str,
    meter_type: str,
    meter_index: int,
    prev_value: float,
    new_value: float,
    ydisk_path: str | None,
    chat_id: str,
    telegram_username: str | None,
) -> None:
    mt = str(meter_type or "unknown")
    mi = int(meter_index or 1)
    reason = {
        "reason": "ocr_overwrite_manual",
        "prev": float(prev_value),
        "curr": float(new_value),
        "ydisk_path": ydisk_path,
    }
    exists = conn.execute(
        text(
            """
            SELECT 1
            FROM meter_review_flags
            WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=:mi
              AND status='open' AND reason='ocr_overwrite_manual'
            LIMIT 1
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "mt": mt, "mi": int(mi)},
    ).fetchone()
    if not exists:
        conn.execute(
            text(
                """
                INSERT INTO meter_review_flags(
                    apartment_id, ym, meter_type, meter_index, status, reason, comment, created_at, resolved_at
                )
                VALUES(:aid, :ym, :mt, :mi, 'open', 'ocr_overwrite_manual', :comment, now(), NULL)
                """
            ),
            {
                "aid": int(apartment_id),
                "ym": str(ym),
                "mt": mt,
                "mi": int(mi),
                "comment": json.dumps(reason, ensure_ascii=False),
            },
        )

    username = (telegram_username or "").strip().lstrip("@").lower() or "Без username"
    related = json.dumps(
        {"ym": str(ym), "meter_type": mt, "meter_index": int(mi), "ydisk_path": ydisk_path},
        ensure_ascii=False,
    )
    msg = f"OCR перезаписал ручное значение ({mt}): было {prev_value}, стало {new_value}. Файл: {ydisk_path}"
    conn.execute(
        text(
            """
            INSERT INTO notifications(
                chat_id, telegram_username, apartment_id, type, message, related, status, created_at
            )
            VALUES(:chat_id, :username, :apartment_id, 'ocr_overwrite_manual', :message, CAST(:related AS JSONB), 'unread', now())
            """
        ),
        {
            "chat_id": str(chat_id),
            "username": username,
            "apartment_id": int(apartment_id),
            "message": msg,
            "related": related,
        },
    )

@router.post("/events/photo")
async def photo_event(request: Request, file: UploadFile = File(None)):
    diag = {"errors": [], "warnings": []}

    form = await request.form()
    trace_id_raw = form.get("trace_id")
    trace_id = (str(trace_id_raw).strip() if trace_id_raw is not None else "") or f"evt-{uuid.uuid4().hex[:12]}"
    diag["trace_id"] = trace_id
    chat_id = form.get("chat_id") or "unknown"
    telegram_username = form.get("telegram_username") or None
    phone = form.get("phone") or None
    t0 = time.monotonic()

    # month (ym) for this photo event. Bot may send it; otherwise default to current month.
    ym_raw = form.get("ym")
    if ym_raw is None or str(ym_raw).strip() == "":
        ym = month_now()
    else:
        ym_raw = str(ym_raw).strip()
        if not re.match(r"^\d{4}-\d{2}$", ym_raw):
            ym = month_now()
            diag["warnings"].append({"invalid_ym": ym_raw})
        else:
            ym = ym_raw

    raw_meter_index = form.get("meter_index")
    meter_index_mode = (form.get("meter_index_mode") or "").strip().lower()  # "explicit" | "" (auto)
    try:
        meter_index = int(raw_meter_index) if raw_meter_index is not None else 1
    except Exception:
        meter_index = 1
        diag["warnings"].append({"invalid_meter_index": str(raw_meter_index)})

    meter_index = max(1, min(3, meter_index))

    def _looks_like_upload(v) -> bool:
        return (
            hasattr(v, "filename")
            and hasattr(v, "content_type")
            and callable(getattr(v, "read", None))
        )

    upload_files: list[UploadFile] = []
    seen_uploads: set[int] = set()
    entries = []
    try:
        if hasattr(form, "multi_items"):
            entries = list(form.multi_items())
        else:
            for k in form.keys():
                vals = form.getlist(k) if hasattr(form, "getlist") else [form.get(k)]
                for v in vals:
                    entries.append((k, v))
    except Exception:
        entries = []
    for _k, v in entries:
        if not _looks_like_upload(v):
            continue
        vid = id(v)
        if vid in seen_uploads:
            continue
        seen_uploads.add(vid)
        upload_files.append(v)
    if isinstance(file, UploadFile):
        vid = id(file)
        if vid not in seen_uploads:
            upload_files.insert(0, file)

    if not upload_files:
        return JSONResponse(status_code=200, content={"status": "accepted", "error": "no_file", "chat_id": str(chat_id)})

    photo_payloads: list[dict] = []
    max_files = max(1, int(PHOTO_EVENT_MAX_FILES))
    for upl in upload_files[:max_files]:
        try:
            b = await upl.read()
        except Exception:
            continue
        if not b:
            continue
        photo_payloads.append(
            {
                "blob": b,
                "filename": (upl.filename or "photo.jpg"),
                "mime": (upl.content_type or "image/jpeg"),
            }
        )

    if not photo_payloads:
        return JSONResponse(status_code=200, content={"status": "accepted", "error": "no_file", "chat_id": str(chat_id)})

    selected_idx = 0
    blob = bytes(photo_payloads[selected_idx]["blob"])
    selected_filename = str(photo_payloads[selected_idx].get("filename") or "photo.jpg")
    selected_mime = str(photo_payloads[selected_idx].get("mime") or "image/jpeg")
    file_sha256 = hashlib.sha256(blob).hexdigest()
    logger.info(
        "photo_event start trace_id=%s chat_id=%s ym=%s files_count=%s selected_file=%s mime=%s size_bytes=%s",
        trace_id,
        str(chat_id),
        str(ym),
        len(photo_payloads),
        selected_filename,
        selected_mime,
        len(blob),
    )

    if db_ready():
        try:
            ensure_tables()
        except Exception as e:
            diag["errors"].append({"db_ensure_tables_error": str(e)})

    # 1) OCR
    # Resolve apartment early for OCR context hints (history-aware candidate selection).
    apartment_id = None
    if db_ready():
        try:
            apartment_id = find_apartment_by_chat(str(chat_id))
        except Exception as e:
            diag["errors"].append({"chat_binding_lookup_error": str(e)})

    context_prev_water: str | None = None
    context_serial_hint: str | None = None
    prev_vals: list[float] = []
    serial_hints: list[str] = []
    if db_ready() and apartment_id:
        try:
            prev_ym = _prev_ym(str(ym))
            use_training_cluster = False
            with engine.begin() as conn:
                apt_row = conn.execute(
                    text("SELECT cold_serial, hot_serial FROM apartments WHERE id=:aid LIMIT 1"),
                    {"aid": int(apartment_id)},
                ).mappings().first()
                if apt_row:
                    for raw_serial in (apt_row.get("cold_serial"), apt_row.get("hot_serial")):
                        s_norm = _normalize_serial(raw_serial)
                        if not s_norm:
                            continue
                        sd = "".join(ch for ch in str(s_norm) if ch.isdigit())
                        if len(sd) < 4 or sd in serial_hints:
                            continue
                        serial_hints.append(sd)
                # Prefer human-corrected history for OCR context (more reliable than stale meter_readings).
                training_vals = _get_recent_training_water_values(conn, int(apartment_id), str(ym), limit=40)
                if training_vals:
                    clustered = _select_water_context_values(
                        training_vals,
                        max_values=3,
                        support_tol=180.0,
                        cluster_only_if_any=True,
                    )
                    if clustered:
                        prev_vals.extend(clustered)
                        use_training_cluster = True
                if not use_training_cluster:
                    for mt in ("cold", "hot"):
                        pv = _get_prev_reading(conn, int(apartment_id), prev_ym, mt, 1)
                        if pv is None:
                            pv = _get_last_reading_before(conn, int(apartment_id), str(ym), mt, 1)
                        if pv is None:
                            continue
                        try:
                            prev_vals.append(float(pv))
                        except Exception:
                            continue
            if prev_vals:
                prev_vals = _select_water_context_values(
                    prev_vals,
                    max_values=3,
                    support_tol=220.0,
                    cluster_only_if_any=True,
                )
            if prev_vals:
                context_prev_water = ",".join(f"{v:.3f}" for v in prev_vals[:3])
            if serial_hints:
                context_serial_hint = ",".join(serial_hints[:3])
        except Exception as e:
            diag["warnings"].append({"ocr_context_prepare_failed": str(e)})

    ocr_data = None
    ocr_t0 = time.monotonic()
    ocr_http_ok = False
    ocr_http_status = None
    if len(photo_payloads) > 1:
        series_photos = [
            (bytes(p["blob"]), str(p.get("filename") or "photo.jpg"), str(p.get("mime") or "image/jpeg"))
            for p in photo_payloads
        ]
        ocr_resp, ocr_exc = _call_ocr_series_with_retries(
            series_photos,
            trace_id=trace_id,
            context_prev_water=context_prev_water,
            context_serial_hint=context_serial_hint,
        )
    else:
        ocr_resp, ocr_exc = _call_ocr_with_retries(
            blob,
            filename=selected_filename,
            mime_type=selected_mime,
            trace_id=trace_id,
            context_prev_water=context_prev_water,
            context_serial_hint=context_serial_hint,
        )
    diag["ocr_latency_ms"] = int((time.monotonic() - ocr_t0) * 1000)
    if ocr_resp is not None:
        ocr_http_ok = bool(ocr_resp.ok)
        ocr_http_status = int(ocr_resp.status_code)
        if ocr_resp.ok:
            ocr_json = ocr_resp.json()
            if len(photo_payloads) > 1 and isinstance(ocr_json, dict):
                hint_values = serial_hints or _parse_serial_hints_context(context_serial_hint)
                series_local = _rebuild_series_best_from_payload(
                    ocr_json,
                    prev_values=prev_vals,
                    serial_hints=hint_values,
                )
                if isinstance(series_local, dict) and isinstance(series_local.get("best"), dict):
                    best = dict(series_local.get("best") or {})
                    ocr_data = best
                    if ocr_json.get("trace_id") and (not ocr_data.get("trace_id")):
                        ocr_data["trace_id"] = ocr_json.get("trace_id")
                    try:
                        best_idx = int(series_local.get("best_index"))
                    except Exception:
                        best_idx = int(ocr_json.get("best_index") or 0)
                    if 0 <= best_idx < len(photo_payloads):
                        selected_idx = best_idx
                        blob = bytes(photo_payloads[selected_idx]["blob"])
                        selected_filename = str(photo_payloads[selected_idx].get("filename") or selected_filename)
                        selected_mime = str(photo_payloads[selected_idx].get("mime") or selected_mime)
                        file_sha256 = hashlib.sha256(blob).hexdigest()
                    diag["ocr_series"] = {
                        "files_count": int(series_local.get("files_count") or len(photo_payloads)),
                        "best_index": selected_idx,
                        "best_score": series_local.get("best_score"),
                        "mode": "service_local_rescore",
                    }
                    for w in (series_local.get("warnings") or []):
                        diag["warnings"].append(w)
                else:
                    best = ocr_json.get("best")
                    if isinstance(best, dict):
                        ocr_data = dict(best)
                        if ocr_json.get("trace_id") and (not ocr_data.get("trace_id")):
                            ocr_data["trace_id"] = ocr_json.get("trace_id")
                        try:
                            best_idx = int(ocr_json.get("best_index"))
                        except Exception:
                            best_idx = 0
                        if 0 <= best_idx < len(photo_payloads):
                            selected_idx = best_idx
                            blob = bytes(photo_payloads[selected_idx]["blob"])
                            selected_filename = str(photo_payloads[selected_idx].get("filename") or selected_filename)
                            selected_mime = str(photo_payloads[selected_idx].get("mime") or selected_mime)
                            file_sha256 = hashlib.sha256(blob).hexdigest()
                        diag["ocr_series"] = {
                            "files_count": int(ocr_json.get("files_count") or len(photo_payloads)),
                            "best_index": selected_idx,
                            "best_score": ocr_json.get("best_score"),
                        }
                    else:
                        ocr_data = None
                        diag["warnings"].append("ocr_series_bad_response")
            else:
                ocr_data = ocr_json
        else:
            diag["warnings"].append(f"ocr_http_{ocr_resp.status_code}")
    else:
        diag["warnings"].append("ocr_unavailable")
        if ocr_exc is not None:
            diag["warnings"].append({"ocr_error": str(ocr_exc)})
    # Safety fallback for multi-photo batch:
    # when /recognize-series fails or times out, run single-image OCR per file and pick best locally.
    if len(photo_payloads) > 1 and (not isinstance(ocr_data, dict)):
        try:
            series_fallback = _call_ocr_series_via_singles(
                series_photos,
                trace_id=trace_id,
                context_prev_water=context_prev_water,
                context_serial_hint=context_serial_hint,
            )
            best = series_fallback.get("best")
            if isinstance(best, dict):
                ocr_data = dict(best)
                try:
                    best_idx = int(series_fallback.get("best_index"))
                except Exception:
                    best_idx = 0
                if 0 <= best_idx < len(photo_payloads):
                    selected_idx = best_idx
                    blob = bytes(photo_payloads[selected_idx]["blob"])
                    selected_filename = str(photo_payloads[selected_idx].get("filename") or selected_filename)
                    selected_mime = str(photo_payloads[selected_idx].get("mime") or selected_mime)
                    file_sha256 = hashlib.sha256(blob).hexdigest()
                diag["ocr_series"] = {
                    "files_count": int(series_fallback.get("files_count") or len(photo_payloads)),
                    "best_index": selected_idx,
                    "best_score": series_fallback.get("best_score"),
                    "mode": "single_fallback",
                }
                ocr_http_ok = True
                ocr_http_status = 200
                diag["warnings"].append("ocr_series_single_fallback")
                for w in (series_fallback.get("warnings") or []):
                    diag["warnings"].append(w)
        except Exception as e:
            diag["warnings"].append({"ocr_series_single_fallback_error": str(e)})
    logger.info(
        "photo_event ocr_result trace_id=%s ok=%s status=%s latency_ms=%s",
        trace_id,
        ocr_http_ok,
        ocr_http_status,
        diag.get("ocr_latency_ms"),
    )

    ocr_type = None
    ocr_reading = None
    ocr_confidence = None
    ocr_serial = None
    if isinstance(ocr_data, dict):
        ocr_type = ocr_data.get("type")
        ocr_reading = ocr_data.get("reading")
        ocr_confidence = ocr_data.get("confidence")
        ocr_serial = ocr_data.get("serial")
        if ocr_data.get("trace_id"):
            diag["ocr_trace_id"] = ocr_data.get("trace_id")

    kind = _ocr_to_kind(ocr_type)
    value_float = _parse_reading_to_float(ocr_reading)
    serial_norm = _normalize_serial(ocr_serial)
    try:
        ocr_conf = float(ocr_confidence) if ocr_confidence is not None else 0.0
    except Exception:
        ocr_conf = 0.0
    is_water_unknown = str(ocr_type or "").strip().lower() == "unknown"
    debug_candidates = _debug_candidates_from_ocr(ocr_data if isinstance(ocr_data, dict) else None)
    is_water_debug = any(_is_odometer_debug_candidate(c) for c in debug_candidates)
    is_water_context = (kind in ("cold", "hot")) or (is_water_unknown and is_water_debug)

    # Guard: OCR may mistakenly read serial number as meter reading.
    # Try to recover from debug candidates first; otherwise mark as unknown reading.
    if _looks_like_serial_reading(value_float, serial_norm):
        dbg = debug_candidates
        fallback = None
        for cand in sorted(dbg, key=lambda x: float(x.get("confidence") or 0.0), reverse=True):
            c_val = cand.get("reading")
            if c_val is None:
                continue
            if _looks_like_serial_reading(c_val, serial_norm):
                continue
            fallback = cand
            break
        if fallback:
            value_float = float(fallback.get("reading"))
            kind = _ocr_to_kind(fallback.get("type"))
            if isinstance(ocr_data, dict):
                ocr_data["reading"] = float(value_float)
                ocr_data["type"] = fallback.get("type")
            diag["warnings"].append(
                {
                    "serial_as_reading_corrected": {
                        "from": ocr_reading,
                        "to": value_float,
                        "variant": fallback.get("variant"),
                        "provider": fallback.get("provider"),
                    }
                }
            )
        else:
            diag["warnings"].append({"serial_as_reading_detected": True})
            value_float = None

    if kind != "electric":
        meter_index = 1

    # For water we now store/read integer part as primary value.
    if WATER_INTEGER_ONLY and is_water_context and (value_float is not None):
        value_float = _as_water_integer(value_float)
        if isinstance(ocr_data, dict):
            ocr_data["reading"] = value_float

    # 2) resolve apartment

    if apartment_id is None and db_ready():
        try:
            apartment_id = find_apartment_by_contact(telegram_username, phone)
            if apartment_id is not None:
                bind_chat(str(chat_id), int(apartment_id))
                # Автозаполнение контактов квартиры (если пришли от пользователя)
                try:
                    if telegram_username:
                        _set_contact(int(apartment_id), "telegram", telegram_username)
                    if phone:
                        _set_contact(int(apartment_id), "phone", phone)
                except Exception as e:
                    diag["warnings"].append({"autofill_contact_error": str(e)})
        except Exception as e:
            diag["errors"].append({"apartment_match_error": str(e)})

    # Second-pass correction for water: use OCR debug candidates + previous month sanity
    if db_ready() and apartment_id and value_float is not None and is_water_context:
        try:
            dbg = debug_candidates
            prev_ym = _prev_ym(str(ym))
            with engine.begin() as conn:
                prev_cold = _get_prev_reading(conn, int(apartment_id), prev_ym, "cold", 1)
                prev_hot = _get_prev_reading(conn, int(apartment_id), prev_ym, "hot", 1)
                prev_ref = None
                if str(kind) == "hot":
                    prev_ref = float(prev_hot) if prev_hot is not None else None
                elif str(kind) == "cold":
                    prev_ref = float(prev_cold) if prev_cold is not None else None
                if prev_ref is None:
                    if prev_cold is not None and prev_hot is not None:
                        prev_ref = max(float(prev_cold), float(prev_hot))
                    elif prev_cold is not None:
                        prev_ref = float(prev_cold)
                    elif prev_hot is not None:
                        prev_ref = float(prev_hot)
            # If current value is suspiciously low, try replacing by better debug candidate
            delta_from_prev = abs(float(value_float) - float(prev_ref)) if prev_ref is not None else None
            suspicious = (
                (
                    prev_ref is not None
                    and (
                        float(value_float) + float(WATER_ANOMALY_THRESHOLD) < float(prev_ref)
                        or float(value_float) - float(WATER_ANOMALY_THRESHOLD) > float(prev_ref)
                    )
                )
                or (float(value_float) <= 0.0)
            )
            if suspicious:
                if WATER_INTEGER_ONLY:
                    # Important: in integer-only mode don't auto-replace OCR value by
                    # "closer to previous month" candidate. This was causing systematic
                    # upward drift (e.g. 1103 -> 3219) on dark photos.
                    old_v = float(value_float)
                    if _looks_like_serial_reading(old_v, serial_norm):
                        diag["warnings"].append(
                            {
                                "water_prev_sanity_saved_with_review": {
                                    "value": old_v,
                                    "prev_ref": prev_ref,
                                    "reason": "serial_like_saved_integer_only",
                                }
                            }
                        )
                    elif (prev_ref is not None) and (delta_from_prev is not None) and (
                        float(delta_from_prev) > float(WATER_ANOMALY_THRESHOLD) * 2.0
                    ):
                        diag["warnings"].append(
                            {
                                "water_prev_sanity_saved_with_review": {
                                    "value": old_v,
                                    "prev_ref": prev_ref,
                                    "reason": "severe_outlier_saved_integer_only_no_autocorrect",
                                }
                            }
                        )
                else:
                    best_c = _choose_water_debug_candidate_with_prev(
                        dbg,
                        prev_value=prev_ref,
                        serial_norm=serial_norm,
                    )
                    if best_c and best_c.get("reading") is not None:
                        old_v = float(value_float)
                        candidate_v = float(best_c.get("reading"))
                        # accept only meaningful improvement; otherwise block write
                        if abs(candidate_v - float(prev_ref)) + 120.0 < abs(old_v - float(prev_ref)):
                            value_float = candidate_v
                            kind = _ocr_to_kind(best_c.get("type")) or kind
                            if isinstance(ocr_data, dict):
                                ocr_data["reading"] = float(value_float)
                                ocr_data["type"] = best_c.get("type")
                            diag["warnings"].append(
                                {
                                    "water_prev_sanity_corrected": {
                                        "from": old_v,
                                        "to": float(value_float),
                                        "prev_ref": prev_ref,
                                        "variant": best_c.get("variant"),
                                        "provider": best_c.get("provider"),
                                    }
                                }
                            )
                        else:
                            diag["warnings"].append(
                                {
                                    "water_prev_sanity_blocked": {
                                        "value": old_v,
                                        "candidate": candidate_v,
                                        "prev_ref": prev_ref,
                                        "reason": "no_meaningful_improvement",
                                    }
                                }
                            )
                            if (prev_ref is not None) and (
                                abs(old_v - float(prev_ref)) > float(WATER_ANOMALY_THRESHOLD) * 2.0
                            ):
                                value_float = None
                                diag["warnings"].append(
                                    {
                                        "water_prev_sanity_blocked": {
                                            "value": old_v,
                                            "prev_ref": prev_ref,
                                            "reason": "blocked_severe_outlier",
                                        }
                                    }
                                )
                    elif _looks_like_serial_reading(value_float, serial_norm):
                        diag["warnings"].append(
                            {
                                "water_prev_sanity_blocked": {
                                    "value": float(value_float),
                                    "prev_ref": prev_ref,
                                    "reason": "serial_like_and_too_low",
                                }
                            }
                        )
                    elif (prev_ref is not None) and (delta_from_prev is not None) and (
                        float(delta_from_prev) > float(WATER_ANOMALY_THRESHOLD) * 2.0
                    ):
                        old_v = float(value_float)
                        value_float = None
                        diag["warnings"].append(
                            {
                                "water_prev_sanity_blocked": {
                                    "value": old_v,
                                    "prev_ref": prev_ref,
                                    "reason": "blocked_severe_outlier_no_candidate",
                                }
                            }
                        )
        except Exception as e:
            diag["warnings"].append({"water_prev_sanity_failed": str(e)})

    # 2.06) water fallback when OCR returned no numeric reading:
    # try to recover from debug candidates (including black/red digit extraction).
    if db_ready() and apartment_id and (value_float is None) and is_water_context:
        try:
            dbg = debug_candidates
            prev_ym = _prev_ym(str(ym))
            with engine.begin() as conn:
                prev_cold = _get_prev_reading(conn, int(apartment_id), prev_ym, "cold", 1)
                prev_hot = _get_prev_reading(conn, int(apartment_id), prev_ym, "hot", 1)
                prev_ref = None
                if str(kind) == "hot":
                    prev_ref = float(prev_hot) if prev_hot is not None else None
                elif str(kind) == "cold":
                    prev_ref = float(prev_cold) if prev_cold is not None else None
                if prev_ref is None:
                    if prev_cold is not None and prev_hot is not None:
                        prev_ref = max(float(prev_cold), float(prev_hot))
                    elif prev_cold is not None:
                        prev_ref = float(prev_cold)
                    elif prev_hot is not None:
                        prev_ref = float(prev_hot)
            best_c = _choose_water_debug_candidate_with_prev(
                dbg,
                prev_value=prev_ref,
                serial_norm=serial_norm,
            )
            if best_c and best_c.get("reading") is not None:
                value_float = float(best_c.get("reading"))
                kind = _ocr_to_kind(best_c.get("type")) or kind
                if isinstance(ocr_data, dict):
                    ocr_data["reading"] = float(value_float)
                    ocr_data["type"] = best_c.get("type")
                diag["warnings"].append(
                    {
                        "water_debug_recovered": {
                            "to": float(value_float),
                            "prev_ref": prev_ref,
                            "variant": best_c.get("variant"),
                            "provider": best_c.get("provider"),
                            "black_digits": best_c.get("black_digits"),
                            "red_digits": best_c.get("red_digits"),
                        }
                    }
                )
        except Exception as e:
            diag["warnings"].append({"water_debug_recover_failed": str(e)})

    # 2.05) optional heuristic fix (disabled by default): water missing last decimal digit
    if ENABLE_AGGRESSIVE_OCR_AUTOFIX and db_ready() and apartment_id and kind in ("cold", "hot") and value_float is not None:
        try:
            with engine.begin() as conn:
                fixed_value, fix_diag = _maybe_fix_water_missing_last_decimal(
                    conn,
                    int(apartment_id),
                    str(ym),
                    str(kind),
                    str(ocr_reading) if ocr_reading is not None else None,
                    float(value_float),
                )
            if fix_diag:
                value_float = float(fixed_value)
                if isinstance(ocr_data, dict):
                    ocr_data["reading"] = float(value_float)
                diag["warnings"].append({"auto_fix_water_missing_last_decimal": fix_diag})
        except Exception as e:
            diag["warnings"].append({"auto_fix_water_missing_last_decimal_failed": str(e)})

    # Re-apply integer-only normalization after all corrections.
    if WATER_INTEGER_ONLY and is_water_context and (value_float is not None):
        value_float = _as_water_integer(value_float)
        if isinstance(ocr_data, dict):
            ocr_data["reading"] = value_float

    # 2.1) optional heuristic fix (disabled by default): one missed digit in electric reading
    if ENABLE_AGGRESSIVE_OCR_AUTOFIX and db_ready() and apartment_id and kind == "electric" and value_float is not None:
        try:
            with engine.begin() as conn:
                fixed_value, fix_diag = _maybe_fix_missing_digit_electric(conn, int(apartment_id), str(ym), float(value_float))
            if fix_diag:
                value_float = float(fixed_value)
                if isinstance(ocr_data, dict):
                    ocr_data["reading"] = float(value_float)
                diag["warnings"].append({"auto_fix_missing_digit": fix_diag})
        except Exception as e:
            diag["warnings"].append({"auto_fix_missing_digit_failed": str(e)})

    # 3) upload to ydisk
    ydisk_path = None
    if ydisk_ready():
        try:
            ydisk_path = upload_to_ydisk(
                str(chat_id),
                chat_name=telegram_username or f"chat_{chat_id}",
                meter_type_label=str(ocr_type or "unknown"),
                original_filename=selected_filename,
                content=blob,
            )
        except Exception as e:
            diag["errors"].append({"ydisk_upload_error": str(e)})
    else:
        diag["warnings"].append("ydisk_not_configured")

    # 4) status/stage
    if ydisk_path and apartment_id:
        status = "assigned"
        stage = "assigned"
    elif ydisk_path:
        status = "unassigned"
        stage = "uploaded"
    else:
        status = "ydisk_error"
        stage = "received"

    # 5) insert photo_event
    photo_event_id = None
    if db_ready():
        try:
            ocr_json_str = json.dumps(ocr_data, ensure_ascii=False) if ocr_data is not None else None
            diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None

            with engine.begin() as conn:
                photo_event_id = conn.execute(
                    text("""
                        INSERT INTO photo_events
                        (
                            chat_id, telegram_username, phone, original_filename, ydisk_path,
                            status, apartment_id, ym, ocr_json,
                            meter_index,
                            stage, stage_updated_at,
                            file_sha256, ocr_type, ocr_reading,
                            meter_kind, meter_value, meter_written,
                            diag_json
                        )
                        VALUES
                        (
                            :chat_id, :username, :phone, :orig, :path,
                            :status, :apartment_id, :ym,
                            CASE WHEN :ocr_json IS NULL THEN NULL ELSE CAST(:ocr_json AS JSONB) END,
                            :meter_index,
                            :stage, now(),
                            :file_sha256, :ocr_type, :ocr_reading,
                            :meter_kind, :meter_value, false,
                            CASE WHEN :diag_json IS NULL THEN NULL ELSE CAST(:diag_json AS JSONB) END
                        )
                        RETURNING id
                    """),
                    {
                        "chat_id": str(chat_id),
                        "username": telegram_username,
                        "phone": phone,
                        "orig": selected_filename,
                        "path": ydisk_path,
                        "status": status,
                        "apartment_id": apartment_id,
                        "ym": str(ym),
                        "ocr_json": ocr_json_str,
                        "meter_index": int(meter_index),
                        "stage": stage,
                        "file_sha256": file_sha256,
                        "ocr_type": (str(ocr_type) if ocr_type is not None else None),
                        "ocr_reading": (float(value_float) if value_float is not None else None),
                        "meter_kind": (str(kind) if kind is not None else None),
                        "meter_value": (float(value_float) if value_float is not None else None),
                        "diag_json": diag_json_str,
                    },
                ).scalar_one()

        except Exception as e:
            diag["errors"].append({"db_insert_error": str(e)})

    # 6) write meter_readings + statuses
    wrote_meter = False
    # ym already defined above
    assigned_meter_index = int(meter_index)

    if db_ready() and apartment_id and (value_float is not None) and (kind or is_water_context):
        try:
            # 6.0) anomaly check vs previous month (absolute thresholds)
            anomaly = False
            anomaly_reason = None
            block_write_due_anomaly = False
            try:
                prev_ym = _prev_ym(str(ym))
                with engine.begin() as conn:
                    if kind in ("cold", "hot"):
                        prev_val = _get_prev_reading(conn, int(apartment_id), prev_ym, str(kind), 1)
                        if prev_val is None:
                            prev_val = _get_last_reading_before(conn, int(apartment_id), str(ym), str(kind), 1)
                        if (prev_val is not None) and (abs(float(value_float) - float(prev_val)) > WATER_ANOMALY_THRESHOLD):
                            anomaly = True
                            anomaly_reason = {"meter_type": str(kind), "threshold": WATER_ANOMALY_THRESHOLD, "prev": prev_val, "curr": float(value_float)}
                    elif kind == "electric":
                        rows = conn.execute(
                            text(
                                """
                                SELECT value
                                FROM meter_readings
                                WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index IN (1,2,3)
                                """
                            ),
                            {"aid": int(apartment_id), "ym": prev_ym},
                        ).fetchall()
                        prev_vals = []
                        for r in rows:
                            try:
                                prev_vals.append(float(r[0]))
                            except Exception:
                                continue
                        if not prev_vals:
                            prev_vals = _get_last_electric_before(conn, int(apartment_id), str(ym))
                        if prev_vals:
                            diffs = [abs(float(value_float) - v) for v in prev_vals]
                            min_diff = min(diffs)
                            closest_prev = prev_vals[diffs.index(min_diff)]
                            if min_diff > ELECTRIC_ANOMALY_THRESHOLD:
                                anomaly = True
                                anomaly_reason = {
                                    "meter_type": "electric",
                                    "threshold": ELECTRIC_ANOMALY_THRESHOLD,
                                    "prev": float(closest_prev),
                                    "curr": float(value_float),
                                }
            except Exception:
                anomaly = False

            # Hard guard: water reading 0 with non-zero history -> needs_review, no write.
            try:
                if kind in ("cold", "hot") and value_float is not None and float(value_float) <= 0.0:
                    prev_ym = _prev_ym(str(ym))
                    with engine.begin() as conn:
                        prev_val = _get_prev_reading(conn, int(apartment_id), prev_ym, str(kind), 1)
                        if prev_val is None:
                            prev_val = _get_last_reading_before(conn, int(apartment_id), str(ym), str(kind), 1)
                    if (prev_val is not None) and (float(prev_val) > 0.0):
                        anomaly = True
                        block_write_due_anomaly = True
                        anomaly_reason = {
                            "meter_type": str(kind),
                            "reason": "water_zero_with_history",
                            "prev": float(prev_val),
                            "curr": float(value_float),
                        }
            except Exception:
                pass

            if anomaly:
                try:
                    with engine.begin() as conn:
                        # If this is a close retake of an already stored value for the same month,
                        # don't block it as anomaly; allow overwrite to avoid loops.
                        try:
                            if (kind in ("cold", "hot")) or is_water_unknown:
                                close_mt = _find_close_water(
                                    conn,
                                    int(apartment_id),
                                    str(ym),
                                    float(value_float),
                                    WATER_RETAKE_THRESHOLD,
                                )
                                if close_mt:
                                    anomaly = False
                            elif kind == "electric":
                                close_mi = _find_close_electric(
                                    conn,
                                    int(apartment_id),
                                    str(ym),
                                    float(value_float),
                                    ELECTRIC_RETAKE_THRESHOLD,
                                )
                                if close_mi is not None:
                                    anomaly = False
                        except Exception:
                            pass

                        if anomaly:
                            diag["warnings"].append({"anomaly_jump": anomaly_reason})
                            # create review flag if missing
                            mt = str(anomaly_reason.get("meter_type") if isinstance(anomaly_reason, dict) else (kind or "unknown"))
                            if mt != "electric":
                                mi = 1
                            elif meter_index_mode == "explicit" and raw_meter_index is not None:
                                mi = int(meter_index)
                            else:
                                mi = 1
                            exists = conn.execute(
                                text(
                                    """
                                    SELECT 1
                                    FROM meter_review_flags
                                    WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=:mi
                                      AND status='open' AND reason='anomaly_jump'
                                    LIMIT 1
                                    """
                                ),
                                {"aid": int(apartment_id), "ym": str(ym), "mt": mt, "mi": int(mi)},
                            ).fetchone()
                            if not exists:
                                conn.execute(
                                    text(
                                        """
                                        INSERT INTO meter_review_flags(
                                            apartment_id, ym, meter_type, meter_index, status, reason, comment, created_at, resolved_at
                                        )
                                        VALUES(:aid, :ym, :mt, :mi, 'open', 'anomaly_jump', :comment, now(), NULL)
                                        """
                                    ),
                                    {
                                        "aid": int(apartment_id),
                                        "ym": str(ym),
                                        "mt": mt,
                                        "mi": int(mi),
                                        "comment": json.dumps(anomaly_reason, ensure_ascii=False),
                                    },
                                )
                            # create notification for admin
                            username = (telegram_username or "").strip().lstrip("@").lower() or "Без username"
                            related = json.dumps(
                                {"ym": str(ym), "meter_type": mt, "meter_index": int(mi)},
                                ensure_ascii=False,
                            )
                            msg = f"Подозрительный скачок по {('ХВС' if mt=='cold' else 'ГВС' if mt=='hot' else 'Электро')}: {anomaly_reason}"
                            conn.execute(
                                text(
                                    """
                                    INSERT INTO notifications(
                                        chat_id, telegram_username, apartment_id, type, message, related, status, created_at
                                    )
                                    VALUES(:chat_id, :username, :apartment_id, 'anomaly_jump', :message, CAST(:related AS JSONB), 'unread', now())
                                    """
                                ),
                                {
                                    "chat_id": str(chat_id),
                                    "username": username,
                                    "apartment_id": int(apartment_id),
                                    "message": msg,
                                    "related": related,
                                },
                            )
                            if photo_event_id:
                                diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
                                conn.execute(
                                    text(
                                        """
                                        UPDATE photo_events
                                        SET
                                            meter_written = false,
                                            stage = 'needs_review',
                                            stage_updated_at = now(),
                                            diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                                        WHERE id = :id
                                        """
                                    ),
                                    {"id": int(photo_event_id), "diag_json": diag_json_str},
                                )
                except Exception:
                    pass

                if block_write_due_anomaly:
                    return JSONResponse(
                        status_code=200,
                        content={
                            "status": "ok",
                            "chat_id": str(chat_id),
                            "telegram_username": telegram_username,
                            "phone": phone,
                            "photo_event_id": photo_event_id,
                            "ydisk_path": ydisk_path,
                            "apartment_id": apartment_id,
                            "event_status": status,
                            "ocr": ocr_data,
                            "meter_written": False,
                            "ocr_failed": False,
                            "diag": diag,
                            "assigned_meter_index": assigned_meter_index,
                            "ym": ym,
                            "bill": None,
                        },
                    )

                # Additional digit-length sanity check (guard against missing leading digits)
                try:
                    with engine.begin() as conn:
                        if kind in ("cold", "hot"):
                            last_val = _get_last_reading_before(conn, int(apartment_id), str(ym), str(kind), 1)
                        elif kind == "electric":
                            prev_vals = _get_last_electric_before(conn, int(apartment_id), str(ym))
                            last_val = max(prev_vals) if prev_vals else None
                        else:
                            last_val = None
                    if (last_val is not None) and (value_float is not None):
                        if _digits_len(float(last_val)) - _digits_len(float(value_float)) >= 2:
                            anomaly = True
                            anomaly_reason = {
                                "meter_type": str(kind or "unknown"),
                                "reason": "digit_length_drop",
                                "prev": float(last_val),
                                "curr": float(value_float),
                            }
                except Exception:
                    pass

                # Same-month sanity: if already have readings for this month,
                # block huge mismatch to avoid overwriting correct manual values.
                try:
                    with engine.begin() as conn:
                        if kind in ("cold", "hot"):
                            vals = _get_same_month_water_values(conn, int(apartment_id), str(ym))
                            existing = [v for mt, v in vals if mt == str(kind)]
                        elif kind == "electric":
                            existing = _get_same_month_electric_values(conn, int(apartment_id), str(ym))
                        else:
                            existing = []
                    if existing and (value_float is not None):
                        diffs = [abs(float(value_float) - v) for v in existing]
                        min_diff = min(diffs)
                        closest = existing[diffs.index(min_diff)]
                        if _digits_len(float(closest)) - _digits_len(float(value_float)) >= 2:
                            anomaly = True
                            anomaly_reason = {
                                "meter_type": str(kind or "unknown"),
                                "reason": "digit_length_drop_same_month",
                                "prev": float(closest),
                                "curr": float(value_float),
                            }
                        elif min_diff > (WATER_ANOMALY_THRESHOLD if kind in ("cold", "hot") else ELECTRIC_ANOMALY_THRESHOLD):
                            anomaly = True
                            anomaly_reason = {
                                "meter_type": str(kind or "unknown"),
                                "reason": "mismatch_same_month",
                                "prev": float(closest),
                                "curr": float(value_float),
                            }
                except Exception:
                    pass

                # even with anomaly we continue and write value to web,
                # keeping review flag/notification for admin verification
                if anomaly:
                    diag["warnings"].append({"anomaly_saved_with_review": True})

            # 6.1) write meter_readings and get assigned_meter_index
            if kind == "electric":
                # By default always auto-sort.
                # First: if value is very close to an existing one, overwrite that slot.
                close_idx = None
                prev_manual = None
                prev_manual_value = None
                with engine.begin() as conn:
                    rows = conn.execute(
                        text(
                            """
                            SELECT meter_index, value
                            FROM meter_readings
                            WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric'
                            """
                        ),
                        {"aid": int(apartment_id), "ym": str(ym)},
                    ).fetchall()
                    best = None
                    for mi, v in (rows or []):
                        if v is None:
                            continue
                        try:
                            diff = abs(float(v) - float(value_float))
                        except Exception:
                            continue
                        if diff <= ELECTRIC_RETAKE_THRESHOLD:
                            if (best is None) or (diff < best[0]):
                                best = (diff, int(mi))
                        if best:
                            close_idx = int(best[1])
                            try:
                                row = conn.execute(
                                    text(
                                        """
                                        SELECT value, source
                                        FROM meter_readings
                                        WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index=:mi
                                        LIMIT 1
                                        """
                                    ),
                                    {"aid": int(apartment_id), "ym": str(ym), "mi": int(close_idx)},
                                ).fetchone()
                                if row and str(row[1]) == "manual":
                                    prev_manual = True
                                    prev_manual_value = float(row[0])
                            except Exception:
                                pass
                            _write_electric_overwrite_then_sort(
                                conn,
                                int(apartment_id),
                                str(ym),
                                int(close_idx),
                                float(value_float),
                                source="ocr",
                            )
                            assigned_meter_index = int(close_idx)
                            diag["warnings"].append({"retake_overwrite": {"meter_type": "electric", "meter_index": int(close_idx)}})

                if close_idx is None:
                    if (meter_index_mode == "explicit") and (raw_meter_index is not None):
                        with engine.begin() as conn:
                            try:
                                row = conn.execute(
                                    text(
                                        """
                                        SELECT value, source
                                        FROM meter_readings
                                        WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index=:mi
                                        LIMIT 1
                                        """
                                    ),
                                    {"aid": int(apartment_id), "ym": str(ym), "mi": int(meter_index)},
                                ).fetchone()
                                if row and str(row[1]) == "manual":
                                    prev_manual = True
                                    prev_manual_value = float(row[0])
                            except Exception:
                                pass
                            assigned_meter_index = _write_electric_explicit(
                                conn,
                                int(apartment_id),
                                ym,
                                int(meter_index),
                                float(value_float),
                            )
                    else:
                        with engine.begin() as conn:
                            # find closest existing manual value for potential overwrite notice
                            try:
                                rows = conn.execute(
                                    text(
                                        """
                                        SELECT meter_index, value, source
                                        FROM meter_readings
                                        WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric'
                                        """
                                    ),
                                    {"aid": int(apartment_id), "ym": str(ym)},
                                ).fetchall()
                                best = None
                                for mi, v, src in (rows or []):
                                    if v is None or str(src) != "manual":
                                        continue
                                    diff = abs(float(v) - float(value_float))
                                    if (best is None) or (diff < best[0]):
                                        best = (diff, float(v), int(mi))
                                if best:
                                    prev_manual = True
                                    prev_manual_value = float(best[1])
                            except Exception:
                                pass
                        assigned_meter_index = _assign_and_write_electric_sorted(
                            int(apartment_id),
                            ym,
                            float(value_float),
                        )

            else:
                # water (cold/hot): always meter_index=1
                assigned_meter_index = 1
                water_write_blocked = False
                with engine.begin() as conn:
                    is_water = is_water_context
                    water_uncertain = False
                    water_prev_hard_block = False
                    water_prev_hard_block_reason = None
                    serial_prev_ref = None
                    serial_prev_kind = None
                    if is_water:
                        prev_map = {}
                        try:
                            rows = conn.execute(
                                text(
                                    """
                                    SELECT meter_type, value, source
                                    FROM meter_readings
                                    WHERE apartment_id=:aid AND ym=:ym AND meter_type IN ('cold','hot') AND meter_index=1
                                    """
                                ),
                                {"aid": int(apartment_id), "ym": str(ym)},
                            ).fetchall()
                            for mt, v, src in (rows or []):
                                if v is None:
                                    continue
                                prev_map[str(mt)] = (float(v), str(src))
                        except Exception:
                            prev_map = {}
                        # hard sanity guard: for water, a sharp drop vs previous month is blocked
                        # (especially important when OCR type is unknown and serial tail is misread as value)
                        try:
                            prev_values = []
                            # Use previous month baseline (not current month), otherwise a wrong write
                            # in current month poisons the sanity floor.
                            prev_ym = _prev_ym(str(ym))
                            pc = _get_prev_reading(conn, int(apartment_id), prev_ym, "cold", 1)
                            ph = _get_prev_reading(conn, int(apartment_id), prev_ym, "hot", 1)
                            if pc is not None:
                                prev_values.append(float(pc))
                            if ph is not None:
                                prev_values.append(float(ph))
                            # Fallback to current month values only if previous month is absent.
                            if not prev_values:
                                prev_values = [float(vs[0]) for vs in prev_map.values() if vs and vs[0] is not None]
                            if prev_values and value_float is not None:
                                prev_floor = min(prev_values)
                                if float(value_float) + 50.0 < float(prev_floor):
                                    water_prev_hard_block = True
                                    water_prev_hard_block_reason = {
                                        "value": float(value_float),
                                        "prev_floor": float(prev_floor),
                                        "reason": "sharp_drop_vs_prev",
                                        "ydisk_path": ydisk_path,
                                    }
                                    diag["warnings"].append(
                                        {"water_prev_hard_block": dict(water_prev_hard_block_reason)}
                                    )
                        except Exception:
                            pass
                        # --- serial-based routing: if serial matches apartment, force meter_type ---
                        force_kind = None
                        force_no_sort = False
                        try:
                            if serial_norm:
                                row = conn.execute(
                                    text(
                                        """
                                        SELECT cold_serial, hot_serial
                                        FROM apartments
                                        WHERE id=:aid
                                        """
                                    ),
                                    {"aid": int(apartment_id)},
                                ).mappings().first()
                                cold_serial = row.get("cold_serial") if row else None
                                hot_serial = row.get("hot_serial") if row else None

                                s_last5 = _last5_serial(serial_norm)
                                cold_last5 = _last5_serial(cold_serial)
                                hot_last5 = _last5_serial(hot_serial)
                                cold_match = _serial_last5_matches(s_last5, cold_last5)
                                hot_match = _serial_last5_matches(s_last5, hot_last5)

                                if cold_match and not hot_match:
                                    force_kind = "cold"
                                    force_no_sort = True
                                elif hot_match and not cold_match:
                                    force_kind = "hot"
                                    force_no_sort = True
                                elif cold_match and hot_match:
                                    # ambiguous serial tail, keep OCR flow but mark uncertainty
                                    diag["warnings"].append(
                                        {
                                            "serial_ambiguous_route": {
                                                "serial_last5": s_last5,
                                                "cold_last5": cold_last5,
                                                "hot_last5": hot_last5,
                                            }
                                        }
                                    )
                                elif s_last5 and (cold_last5 or hot_last5):
                                    # serial recognized but doesn't match stored serials -> block and notify
                                    reason = {
                                        "reason": "serial_mismatch_route",
                                        "serial_last5": s_last5,
                                        "cold_last5": cold_last5,
                                        "hot_last5": hot_last5,
                                    }
                                    diag["warnings"].append({"serial_mismatch": reason})
                                    # create review flag + notification and block writing
                                    mt = str(kind or "unknown")
                                    mi = 1
                                    exists = conn.execute(
                                        text(
                                            """
                                            SELECT 1
                                            FROM meter_review_flags
                                            WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=:mi
                                              AND status='open' AND reason='serial_mismatch'
                                            LIMIT 1
                                            """
                                        ),
                                        {"aid": int(apartment_id), "ym": str(ym), "mt": mt, "mi": int(mi)},
                                    ).fetchone()
                                    if not exists:
                                        conn.execute(
                                            text(
                                                """
                                                INSERT INTO meter_review_flags(
                                                    apartment_id, ym, meter_type, meter_index, status, reason, comment, created_at, resolved_at
                                                )
                                                VALUES(:aid, :ym, :mt, :mi, 'open', 'serial_mismatch', :comment, now(), NULL)
                                                """
                                            ),
                                            {
                                                "aid": int(apartment_id),
                                                "ym": str(ym),
                                                "mt": mt,
                                                "mi": int(mi),
                                                "comment": json.dumps(reason, ensure_ascii=False),
                                            },
                                        )
                                    username = (telegram_username or "").strip().lstrip("@").lower() or "Без username"
                                    related = json.dumps(
                                        {"ym": str(ym), "meter_type": mt, "meter_index": int(mi), "ydisk_path": ydisk_path},
                                        ensure_ascii=False,
                                    )
                                    msg = f"Несовпадение серийника ХВС/ГВС. Файл: {ydisk_path}"
                                    conn.execute(
                                        text(
                                            """
                                            INSERT INTO notifications(
                                                chat_id, telegram_username, apartment_id, type, message, related, status, created_at
                                            )
                                            VALUES(:chat_id, :username, :apartment_id, 'serial_mismatch', :message, CAST(:related AS JSONB), 'unread', now())
                                            """
                                        ),
                                        {
                                            "chat_id": str(chat_id),
                                            "username": username,
                                            "apartment_id": int(apartment_id),
                                            "message": msg,
                                            "related": related,
                                        },
                                    )
                                    if photo_event_id:
                                        diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
                                        conn.execute(
                                            text(
                                                """
                                                UPDATE photo_events
                                                SET
                                                    meter_written = false,
                                                    stage = 'needs_review',
                                                    stage_updated_at = now(),
                                                    diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                                                WHERE id = :id
                                                """
                                            ),
                                            {"id": int(photo_event_id), "diag_json": diag_json_str},
                                        )
                                    return JSONResponse(
                                        status_code=200,
                                        content={
                                            "status": "ok",
                                            "chat_id": str(chat_id),
                                            "telegram_username": telegram_username,
                                            "phone": phone,
                                            "photo_event_id": photo_event_id,
                                            "ydisk_path": ydisk_path,
                                            "apartment_id": apartment_id,
                                            "event_status": status,
                                            "ocr": ocr_data,
                                            "meter_written": False,
                                            "ocr_failed": False,
                                            "diag": diag,
                                            "assigned_meter_index": assigned_meter_index,
                                            "ym": ym,
                                            "bill": None,
                                        },
                                    )
                        except Exception:
                            force_kind = None
                            force_no_sort = False

                        # Hard serial+history sanity:
                        # if serial maps to a specific water meter, keep value near that meter's previous reading.
                        try:
                            if force_kind in ("cold", "hot") and value_float is not None:
                                serial_prev_kind = str(force_kind)
                                serial_prev_ref = _get_prev_reading(
                                    conn,
                                    int(apartment_id),
                                    _prev_ym(str(ym)),
                                    serial_prev_kind,
                                    1,
                                )
                                if serial_prev_ref is None:
                                    serial_prev_ref = _get_last_reading_before(
                                        conn,
                                        int(apartment_id),
                                        str(ym),
                                        serial_prev_kind,
                                        1,
                                    )
                                if serial_prev_ref is not None:
                                    serial_prev_ref = float(serial_prev_ref)
                                    cur_delta = abs(float(value_float) - float(serial_prev_ref))
                                    if cur_delta > float(WATER_SERIAL_HARD_DELTA):
                                        best_serial = _choose_water_debug_candidate_with_prev(
                                            debug_candidates,
                                            prev_value=float(serial_prev_ref),
                                            serial_norm=serial_norm,
                                            max_delta=float(WATER_SERIAL_HARD_DELTA),
                                        )
                                        if best_serial and best_serial.get("reading") is not None:
                                            old_v = float(value_float)
                                            value_float = float(best_serial.get("reading"))
                                            kind = _ocr_to_kind(best_serial.get("type")) or kind
                                            if isinstance(ocr_data, dict):
                                                ocr_data["reading"] = float(value_float)
                                                ocr_data["type"] = best_serial.get("type")
                                            diag["warnings"].append(
                                                {
                                                    "water_serial_prev_corrected": {
                                                        "meter_type": serial_prev_kind,
                                                        "prev_ref": float(serial_prev_ref),
                                                        "from": old_v,
                                                        "to": float(value_float),
                                                        "variant": best_serial.get("variant"),
                                                        "provider": best_serial.get("provider"),
                                                    }
                                                }
                                            )
                                        else:
                                            water_prev_hard_block = True
                                            water_prev_hard_block_reason = {
                                                "value": float(value_float),
                                                "prev_ref": float(serial_prev_ref),
                                                "meter_type": serial_prev_kind,
                                                "reason": "serial_prev_outlier",
                                                "threshold": float(WATER_SERIAL_HARD_DELTA),
                                                "ydisk_path": ydisk_path,
                                            }
                                            diag["warnings"].append(
                                                {"water_prev_hard_block": dict(water_prev_hard_block_reason)}
                                            )
                        except Exception:
                            pass

                        # если OCR не уверен в типе, сортируем как max->ХВС, min->ГВС
                        water_uncertain = is_water_unknown or (kind in ("cold", "hot") and ocr_conf < WATER_TYPE_CONF_MIN)
                        if water_uncertain:
                            diag["warnings"].append({"water_type_uncertain": {"confidence": ocr_conf, "ocr_type": ocr_type}})
                        force_sort = _has_open_water_uncertain_flag(conn, int(apartment_id), str(ym))
                        # if new value is very close to an existing water reading, overwrite that specific meter
                        rows = conn.execute(
                            text(
                                """
                                SELECT meter_type, value
                                FROM meter_readings
                                WHERE apartment_id=:aid AND ym=:ym AND meter_type IN ('cold','hot') AND meter_index=1
                                """
                            ),
                            {"aid": int(apartment_id), "ym": str(ym)},
                        ).fetchall()
                        best = None
                        for mt, v in (rows or []):
                            if v is None:
                                continue
                            try:
                                diff = abs(float(v) - float(value_float))
                            except Exception:
                                continue
                            if diff <= WATER_RETAKE_THRESHOLD:
                                if (best is None) or (diff < best[0]):
                                    best = (diff, str(mt))
                        if best:
                            best_kind = str(best[1])
                            # If OCR confidently says the other type, don't overwrite by proximity.
                            if kind in ("cold", "hot") and ocr_conf >= WATER_TYPE_CONF_MIN and best_kind != str(kind):
                                reason = {
                                    "reason": "ocr_type_conflict",
                                    "ocr_type": str(kind),
                                    "matched_type": best_kind,
                                    "value": float(value_float),
                                    "ydisk_path": ydisk_path,
                                }
                                diag["warnings"].append({"ocr_type_conflict": reason})
                                # notify admin + flag for review
                                try:
                                    exists = conn.execute(
                                        text(
                                            """
                                            SELECT 1
                                            FROM meter_review_flags
                                            WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=1
                                              AND status='open' AND reason='ocr_type_conflict'
                                            LIMIT 1
                                            """
                                        ),
                                        {"aid": int(apartment_id), "ym": str(ym), "mt": str(kind)},
                                    ).fetchone()
                                    if not exists:
                                        conn.execute(
                                            text(
                                                """
                                                INSERT INTO meter_review_flags(
                                                    apartment_id, ym, meter_type, meter_index, status, reason, comment, created_at, resolved_at
                                                )
                                                VALUES(:aid, :ym, :mt, 1, 'open', 'ocr_type_conflict', :comment, now(), NULL)
                                                """
                                            ),
                                            {
                                                "aid": int(apartment_id),
                                                "ym": str(ym),
                                                "mt": str(kind),
                                                "comment": json.dumps(reason, ensure_ascii=False),
                                            },
                                        )
                                    username = (telegram_username or "").strip().lstrip("@").lower() or "Без username"
                                    related = json.dumps(
                                        {"ym": str(ym), "meter_type": str(kind), "meter_index": 1, "ydisk_path": ydisk_path},
                                        ensure_ascii=False,
                                    )
                                    msg = f"OCR тип конфликтует со значением в месяце: {reason}. Файл: {ydisk_path}"
                                    conn.execute(
                                        text(
                                            """
                                            INSERT INTO notifications(
                                                chat_id, telegram_username, apartment_id, type, message, related, status, created_at
                                            )
                                            VALUES(:chat_id, :username, :apartment_id, 'ocr_type_conflict', :message, CAST(:related AS JSONB), 'unread', now())
                                            """
                                        ),
                                        {
                                            "chat_id": str(chat_id),
                                            "username": username,
                                            "apartment_id": int(apartment_id),
                                            "message": msg,
                                            "related": related,
                                        },
                                    )
                                except Exception:
                                    pass
                            else:
                                force_kind = best_kind
                                force_no_sort = True
                                diag["warnings"].append({"retake_overwrite": {"meter_type": str(force_kind), "meter_index": 1}})

                        # If serial matched, do not force sort even if uncertain
                        if force_kind and force_no_sort:
                            force_sort = False

                        assigned_kind = _write_water_ocr_with_uncertainty(
                            conn,
                            int(apartment_id),
                            str(ym),
                            float(value_float),
                            kind if kind in ("cold", "hot") else None,
                            float(value_float),
                            bool(water_uncertain),
                            bool(force_sort),
                            force_kind=force_kind,
                            force_no_sort=force_no_sort,
                        )
                        kind = assigned_kind

                        # On sharp drop vs previous month: keep review flag, but rollback OCR write.
                        if water_prev_hard_block:
                            mt = str(assigned_kind if assigned_kind in ("cold", "hot") else (force_kind or "cold"))
                            reason = dict(water_prev_hard_block_reason or {})
                            exists = conn.execute(
                                text(
                                    """
                                    SELECT 1
                                    FROM meter_review_flags
                                    WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=1
                                      AND status='open' AND reason='water_same_month_drop_block'
                                    LIMIT 1
                                    """
                                ),
                                {"aid": int(apartment_id), "ym": str(ym), "mt": mt},
                            ).fetchone()
                            if not exists:
                                conn.execute(
                                    text(
                                        """
                                        INSERT INTO meter_review_flags(
                                            apartment_id, ym, meter_type, meter_index, status, reason, comment, created_at, resolved_at
                                        )
                                        VALUES(:aid, :ym, :mt, 1, 'open', 'water_same_month_drop_block', :comment, now(), NULL)
                                        """
                                    ),
                                    {
                                        "aid": int(apartment_id),
                                        "ym": str(ym),
                                        "mt": mt,
                                        "comment": json.dumps(reason, ensure_ascii=False),
                                    },
                                )
                            try:
                                username = (telegram_username or "").strip().lstrip("@").lower() or "Без username"
                                related = json.dumps(
                                    {"ym": str(ym), "meter_type": mt, "meter_index": 1, "ydisk_path": ydisk_path},
                                    ensure_ascii=False,
                                )
                                msg = f"Падение показаний vs прошлый месяц: требуется проверка. Файл: {ydisk_path}"
                                conn.execute(
                                    text(
                                        """
                                        INSERT INTO notifications(
                                            chat_id, telegram_username, apartment_id, type, message, related, status, created_at
                                        )
                                        VALUES(:chat_id, :username, :apartment_id, 'water_same_month_drop_block', :message, CAST(:related AS JSONB), 'unread', now())
                                        """
                                    ),
                                    {
                                        "chat_id": str(chat_id),
                                        "username": username,
                                        "apartment_id": int(apartment_id),
                                        "message": msg,
                                        "related": related,
                                    },
                                )
                            except Exception:
                                pass

                            # Rollback current OCR write for this meter/month so bad value is not persisted.
                            try:
                                prev_entry = prev_map.get(mt)
                                if prev_entry and prev_entry[0] is not None:
                                    prev_val, prev_src = prev_entry
                                    conn.execute(
                                        text(
                                            """
                                            UPDATE meter_readings
                                            SET value=:value, source=:src
                                            WHERE apartment_id=:aid
                                              AND ym=:ym
                                              AND meter_type=:mt
                                              AND meter_index=1
                                            """
                                        ),
                                        {
                                            "aid": int(apartment_id),
                                            "ym": str(ym),
                                            "mt": mt,
                                            "value": float(prev_val),
                                            "src": str(prev_src or "manual"),
                                        },
                                    )
                                else:
                                    conn.execute(
                                        text(
                                            """
                                            DELETE FROM meter_readings
                                            WHERE apartment_id=:aid
                                              AND ym=:ym
                                              AND meter_type=:mt
                                              AND meter_index=1
                                              AND source='ocr'
                                              AND abs(value - :value) <= 0.0005
                                            """
                                        ),
                                        {
                                            "aid": int(apartment_id),
                                            "ym": str(ym),
                                            "mt": mt,
                                            "value": float(value_float),
                                        },
                                    )
                            except Exception as e:
                                diag["warnings"].append({"water_prev_hard_block_rollback_failed": str(e)})
                            water_write_blocked = True

                        try:
                            if assigned_kind in prev_map:
                                prev_val, prev_src = prev_map[assigned_kind]
                                if prev_src == "manual" and abs(float(prev_val) - float(value_float)) > 1e-6:
                                    _flag_manual_overwrite(
                                        conn,
                                        apartment_id=int(apartment_id),
                                        ym=str(ym),
                                        meter_type=str(assigned_kind),
                                        meter_index=1,
                                        prev_value=float(prev_val),
                                        new_value=float(value_float),
                                        ydisk_path=ydisk_path,
                                        chat_id=str(chat_id),
                                        telegram_username=telegram_username,
                                    )
                        except Exception:
                            pass
                    else:
                        # если OCR не распознал тип — ничего не пишем
                        raise Exception("water_type_unknown")

            # 6.2) duplicate check
            try:
                tol = 0.0005
                with engine.begin() as conn:
                    if kind in ("cold", "hot"):
                        row = conn.execute(
                            text(
                                """
                                SELECT meter_type, meter_index, value
                                FROM meter_readings
                                WHERE apartment_id=:aid
                                  AND ym=:ym
                                  AND source IN ('ocr','manual')
                                  AND meter_type IN ('cold','hot')
                                  AND abs(value - :val) <= :tol
                                  AND NOT (meter_type=:mt AND meter_index=:mi)
                                ORDER BY meter_type ASC, meter_index ASC
                                LIMIT 1
                                """
                            ),
                            {
                                "aid": int(apartment_id),
                                "ym": str(ym),
                                "val": float(value_float),
                                "tol": float(tol),
                                "mt": str(kind),
                                "mi": int(assigned_meter_index),
                            },
                        ).fetchone()
                    else:
                        row = conn.execute(
                            text(
                                """
                                SELECT meter_type, meter_index, value
                                FROM meter_readings
                                WHERE apartment_id=:aid
                                  AND ym=:ym
                                  AND source IN ('ocr','manual')
                                  AND meter_type='electric'
                                  AND abs(value - :val) <= :tol
                                  AND NOT (meter_type=:mt AND meter_index=:mi)
                                ORDER BY meter_type ASC, meter_index ASC
                                LIMIT 1
                                """
                            ),
                            {
                                "aid": int(apartment_id),
                                "ym": str(ym),
                                "val": float(value_float),
                                "tol": float(tol),
                                "mt": str(kind),
                                "mi": int(assigned_meter_index),
                            },
                        ).fetchone()

                if row:
                    existing_mt = str(row[0])
                    existing_mi = int(row[1])
                    diag["warnings"].append(
                        {
                            "possible_duplicate": {
                                "meter_type": existing_mt,
                                "meter_index": existing_mi,
                                "ym": str(ym),
                                "value": float(value_float),
                                "incoming_meter_type": str(kind),
                                "incoming_meter_index": int(assigned_meter_index),
                            }
                        }
                    )
            except Exception as e:
                diag["warnings"].append({"duplicate_check_failed": str(e)})

            # 6.3) update statuses
            try:
                _upsert_month_statuses(int(apartment_id), ym, UIStatusesPatch(meters_photo=True))
            except Exception as e:
                diag["warnings"].append({"month_status_update_failed": str(e)})

            try:
                patch = {}
                if kind == "cold":
                    patch["meters_photo_cold"] = True
                elif kind == "hot":
                    patch["meters_photo_hot"] = True
                elif kind == "electric":
                    patch["meters_photo_electric"] = True
                if patch:
                    update_apartment_statuses(int(apartment_id), patch)
            except Exception as e:
                diag["warnings"].append({"apartment_status_update_failed": str(e)})

            wrote_meter = not bool(water_write_blocked)

            # notify if OCR overwrote manual for electric
            if kind == "electric" and prev_manual and (prev_manual_value is not None):
                try:
                    with engine.begin() as conn:
                        if abs(float(prev_manual_value) - float(value_float)) > 1e-6:
                            _flag_manual_overwrite(
                                conn,
                                apartment_id=int(apartment_id),
                                ym=str(ym),
                                meter_type="electric",
                                meter_index=int(assigned_meter_index),
                                prev_value=float(prev_manual_value),
                                new_value=float(value_float),
                                ydisk_path=ydisk_path,
                                chat_id=str(chat_id),
                                telegram_username=telegram_username,
                            )
                except Exception:
                    pass

            # 6.35) auto-fill serial number (only if not manually set) + notify on mismatch
            try:
                if serial_norm and kind in ("cold", "hot"):
                    col = "cold_serial" if kind == "cold" else "hot_serial"
                    col_src = "cold_serial_source" if kind == "cold" else "hot_serial_source"
                    with engine.begin() as conn:
                        row = conn.execute(
                            text(
                                f"""
                                SELECT {col} AS serial, {col_src} AS src
                                FROM apartments
                                WHERE id=:aid
                                """
                            ),
                            {"aid": int(apartment_id)},
                        ).mappings().first()

                        existing = (row.get("serial") if row else None) or ""
                        existing_norm = _normalize_serial(existing)
                        src = (row.get("src") if row else None) or ""

                        if src == "manual" and existing_norm and (existing_norm != serial_norm):
                            # notify admin about mismatch, do not overwrite
                            username = (telegram_username or "").strip().lstrip("@").lower() or "Без username"
                            related = json.dumps(
                                {"ym": str(ym), "meter_type": str(kind), "meter_index": 1},
                                ensure_ascii=False,
                            )
                            # avoid duplicate notifications for same apartment+ym+meter_type
                            dup = conn.execute(
                                text(
                                    """
                                    SELECT 1
                                    FROM notifications
                                    WHERE apartment_id=:aid
                                      AND type='serial_mismatch'
                                      AND status='unread'
                                      AND related->>'ym' = :ym
                                      AND related->>'meter_type' = :mt
                                    LIMIT 1
                                    """
                                ),
                                {"aid": int(apartment_id), "ym": str(ym), "mt": str(kind)},
                            ).fetchone()
                            if not dup:
                                msg = (
                                    f"Несовпадение серийного номера {('ХВС' if kind=='cold' else 'ГВС')}: "
                                    f"OCR={serial_norm}, вручную={existing_norm}"
                                )
                                conn.execute(
                                    text(
                                        """
                                        INSERT INTO notifications(
                                            chat_id, telegram_username, apartment_id, type, message, related, status, created_at
                                        )
                                        VALUES(
                                            :chat_id, :username, :apartment_id, 'serial_mismatch', :message,
                                            CAST(:related AS JSONB),
                                            'unread', now()
                                        )
                                        """
                                    ),
                                    {
                                        "chat_id": str(chat_id),
                                        "username": username,
                                        "apartment_id": int(apartment_id),
                                        "message": msg,
                                        "related": related,
                                    },
                                )

                    # auto-fill only if not manually set
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                f"""
                                UPDATE apartments
                                SET {col} = CASE WHEN {col} IS NULL OR {col} = '' THEN :serial ELSE {col} END,
                                    {col_src} = CASE
                                        WHEN {col_src} = 'manual' THEN {col_src}
                                        WHEN {col} IS NULL OR {col} = '' THEN 'auto'
                                        ELSE {col_src}
                                    END
                                WHERE id = :aid
                                  AND COALESCE({col_src}, '') <> 'manual'
                                """
                            ),
                            {"aid": int(apartment_id), "serial": serial_norm},
                        )
            except Exception:
                pass

            # 6.4) update photo_events with diag_json
            if db_ready() and photo_event_id:
                try:
                    diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
                    with engine.begin() as conn:
                        conn.execute(
                            text("""
                                UPDATE photo_events
                                SET
                                    meter_written = :meter_written,
                                    meter_index = :meter_index,
                                    meter_kind = COALESCE(:meter_kind, meter_kind),
                                    meter_value = COALESCE(:meter_value, meter_value),
                                    stage = :stage,
                                    stage_updated_at = now(),
                                    diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                                WHERE id = :id
                            """),
                            {
                                "id": int(photo_event_id),
                                "meter_index": int(assigned_meter_index),
                                "meter_kind": str(kind),
                                "meter_value": float(value_float),
                                "meter_written": bool(wrote_meter),
                                "stage": "meter_written" if wrote_meter else "needs_review",
                                "diag_json": diag_json_str,
                            },
                        )
                except Exception as e:
                    diag["warnings"].append({"photo_event_post_update_failed": str(e)})

        except Exception as e:
            diag["errors"].append({"meter_write_failed": str(e)})

    # 6.5) auto-send sum
    if db_ready() and apartment_id:
        try:
            with engine.begin() as conn:
                bill = _calc_month_bill(conn, apartment_id=int(apartment_id), ym=str(ym))
                st = _get_month_bill_state(conn, int(apartment_id), str(ym))
                if (bill.get("reason") == "ok") and (bill.get("total_rub") is not None) and (not _same_total(st.get("sent_total"), bill.get("total_rub"))):
                    msg = f"Сумма оплаты по счётчикам за {ym}: {float(bill.get('total_rub')):.2f} ₽"
                    if _tg_send_message(str(chat_id), msg):
                        _set_month_bill_state(conn, int(apartment_id), str(ym), sent_at=True, sent_total=bill.get("total_rub"))
                else:
                    logger.info(
                        "tg_send skip ctx=photo_event apartment_id=%s ym=%s reason=%s total=%s sent_total=%s",
                        int(apartment_id),
                        str(ym),
                        str(bill.get("reason")),
                        bill.get("total_rub"),
                        st.get("sent_total"),
                    )
        except Exception:
            pass

    # 7) bill (for bot and web)
    bill = None
    if db_ready() and apartment_id:
        try:
            with engine.begin() as conn:
                bill = _calc_month_bill(conn, int(apartment_id), ym)
        except Exception as e:
            diag["warnings"].append({"bill_calc_failed": str(e)})

    payload = {
        "trace_id": trace_id,
        "status": "ok",
        "chat_id": str(chat_id),
        "telegram_username": telegram_username,
        "phone": phone,
        "photo_event_id": photo_event_id,
        "ydisk_path": ydisk_path,
        "apartment_id": apartment_id,
        "event_status": status,
        "ocr": ocr_data,
        "meter_written": wrote_meter,
        "ocr_failed": bool((value_float is None) or (not kind and not is_water_unknown)),
        "diag": diag,
        "assigned_meter_index": assigned_meter_index,
        "ym": ym,
        "bill": bill,
    }
    logger.info(
        "photo_event done trace_id=%s elapsed_ms=%s apartment_id=%s meter_written=%s ocr_type=%s ocr_reading=%s warnings=%s errors=%s",
        trace_id,
        int((time.monotonic() - t0) * 1000),
        apartment_id,
        bool(wrote_meter),
        (ocr_data.get("type") if isinstance(ocr_data, dict) else None),
        (ocr_data.get("reading") if isinstance(ocr_data, dict) else None),
        len(diag.get("warnings") or []),
        len(diag.get("errors") or []),
    )
    return JSONResponse(status_code=200, content=payload)
