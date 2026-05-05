import json
import re
import hashlib
import requests
import math
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

from fastapi import APIRouter, Request, UploadFile, File
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from sqlalchemy import text
from datetime import datetime

from core.config import OCR_URL, engine, logger
from core.db import db_ready, ensure_tables
from core.integrations import ydisk_ready, upload_to_ydisk, _tg_send_message
from core.billing import (
    month_now,
    is_ym,
    _calc_month_bill,
    _get_apartment_electric_expected,
    _get_month_extra_state,
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
OCR_HTTP_TIMEOUT_SEC = float(os.getenv("OCR_HTTP_TIMEOUT_SEC", "180"))
OCR_HTTP_TIMEOUT_FLOOR_SEC = float(os.getenv("OCR_HTTP_TIMEOUT_FLOOR_SEC", "130"))
OCR_HTTP_RETRIES = int(os.getenv("OCR_HTTP_RETRIES", "1"))
WATER_INTEGER_ONLY = os.getenv("WATER_INTEGER_ONLY", "0").strip().lower() in ("1", "true", "yes", "on")
OCR_SERIES_HTTP_TIMEOUT_SEC = float(os.getenv("OCR_SERIES_HTTP_TIMEOUT_SEC", "220"))
OCR_SERIES_SINGLE_REPEATS = max(1, min(5, int(os.getenv("OCR_SERIES_SINGLE_REPEATS", "3"))))
PHOTO_EVENT_MAX_FILES = int(os.getenv("PHOTO_EVENT_MAX_FILES", "6"))


def _kind_to_label(kind: str | None, meter_index: int | None = None) -> str | None:
    k = str(kind or "").strip().lower()
    if k == "cold":
        return "ХВС"
    if k == "hot":
        return "ГВС"
    if k == "electric":
        try:
            mi = int(meter_index or 1)
        except Exception:
            mi = 1
        return f"Электро T{mi}"
    return None


def _normalize_tariff_index_hint(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            idx = int(value)
        except Exception:
            return None
        return idx if idx in (1, 2, 3) else None
    text_value = str(value or "").strip()
    if not text_value:
        return None
    text_l = text_value.lower()
    for pat in (
        r"\bt\s*([123])\b",
        r"\bт\s*([123])\b",
        r"\b1[.,]8[.,]([123])\b",
        r"\btariff[_\s-]*(?:index)?\s*[:=]?\s*([123])\b",
        r"\bтариф\s*([123])\b",
    ):
        m = re.search(pat, text_l)
        if not m:
            continue
        try:
            idx = int(m.group(1))
        except Exception:
            continue
        if idx in (1, 2, 3):
            return idx
    return None


def _extract_ocr_tariff_index(ocr_data: dict | None) -> Optional[int]:
    if not isinstance(ocr_data, dict):
        return None
    for key in ("tariff_index", "meter_index", "tariff", "rate_index"):
        idx = _normalize_tariff_index_hint(ocr_data.get(key))
        if idx is not None:
            return idx
    text_parts = [
        str(ocr_data.get("notes") or ""),
        str(ocr_data.get("type") or ""),
    ]
    debug = ocr_data.get("debug")
    if isinstance(debug, list):
        for item in debug[:5]:
            if not isinstance(item, dict):
                continue
            for key in ("tariff_index", "meter_index", "tariff", "notes", "variant"):
                idx = _normalize_tariff_index_hint(item.get(key))
                if idx is not None:
                    return idx
                text_parts.append(str(item.get(key) or ""))
    return _normalize_tariff_index_hint(" ".join(text_parts))


def _extract_water_visual_type_hint(ocr_data: dict | None) -> Optional[dict[str, Any]]:
    if not isinstance(ocr_data, dict):
        return None
    raw_hint = ocr_data.get("visual_water_type_hint")
    if not isinstance(raw_hint, dict):
        wd = ocr_data.get("water_decision")
        if isinstance(wd, dict):
            raw_hint = wd.get("visual_type_hint")
            if not isinstance(raw_hint, dict):
                summary = wd.get("summary")
                if isinstance(summary, dict):
                    raw_hint = summary.get("visual_type_hint")
    if not isinstance(raw_hint, dict):
        return None
    hint_type = raw_hint.get("type")
    hint_kind = _ocr_to_kind(str(hint_type or ""))
    if hint_kind not in ("cold", "hot"):
        return None
    try:
        confidence = float(raw_hint.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    return {
        "type": str(hint_type or ""),
        "kind": hint_kind,
        "confidence": round(float(confidence), 4),
        "source": raw_hint.get("source"),
        "reason": raw_hint.get("reason"),
        "used_for_raw_kind": False,
    }


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
    context_serial_prev: str | None = None,
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
    if context_serial_prev:
        post_data["context_serial_prev"] = str(context_serial_prev)
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
    context_serial_prev: str | None = None,
):
    last_exc = None
    post_data: dict[str, str] = {}
    if trace_id:
        post_data["trace_id"] = str(trace_id)
    if context_prev_water:
        post_data["context_prev_water"] = str(context_prev_water)
    if context_serial_hint:
        post_data["context_serial_hint"] = str(context_serial_hint)
    if context_serial_prev:
        post_data["context_serial_prev"] = str(context_serial_prev)
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


def _get_stable_water_context_prev_reading(
    conn,
    apartment_id: int,
    ym: str,
    meter_type: str,
    meter_index: int = 1,
    *,
    decrease_tolerance: float = 1.0,
    lookback_limit: int = 4,
) -> tuple[float | None, dict[str, Any] | None]:
    rows = conn.execute(
        text(
            """
            SELECT id, ym, value, source, ocr_value
            FROM meter_readings
            WHERE apartment_id=:aid
              AND ym < :ym
              AND meter_type=:mt
              AND meter_index=:mi
            ORDER BY ym DESC
            LIMIT :lim
            """
        ),
        {
            "aid": int(apartment_id),
            "ym": str(ym),
            "mt": str(meter_type),
            "mi": int(meter_index),
            "lim": int(max(2, lookback_limit)),
        },
    ).mappings().all()
    parsed: list[dict[str, Any]] = []
    for row in rows:
        try:
            value = float(row.get("value"))
        except Exception:
            continue
        if not math.isfinite(value) or value <= 0:
            continue
        parsed.append(
            {
                "id": row.get("id"),
                "ym": row.get("ym"),
                "value": value,
                "source": row.get("source"),
                "ocr_value": float(row.get("ocr_value")) if row.get("ocr_value") is not None else None,
            }
        )
    if not parsed:
        return None, {"reason": "no_prior_water_readings", "meter_type": str(meter_type)}

    latest = parsed[0]
    # For OCR context, a clearly decreasing water history is worse than no history:
    # it can force the OCR scorer toward an old bad value and hide the real odometer.
    for older in parsed[1:]:
        delta = float(latest["value"]) - float(older["value"])
        if delta < -abs(float(decrease_tolerance)):
            return None, {
                "reason": "unstable_decreasing_water_history",
                "meter_type": str(meter_type),
                "meter_index": int(meter_index),
                "candidate": latest,
                "older": older,
                "delta": round(delta, 3),
            }
    return float(latest["value"]), None


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


def _get_recent_training_values_for_type(
    conn,
    apartment_id: int,
    ym: str,
    meter_type: str,
    limit: int = 16,
) -> list[float]:
    rows = conn.execute(
        text(
            """
            SELECT correct_value
            FROM ocr_training_samples
            WHERE apartment_id=:aid
              AND ym <= :ym
              AND meter_type=:mt
              AND meter_index=1
              AND correct_value IS NOT NULL
              AND correct_value > 0
            ORDER BY created_at DESC, id DESC
            LIMIT :lim
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "mt": str(meter_type), "lim": int(max(1, limit))},
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


def _single_ocr_result_is_context_sufficient(item: dict, prev_values: list[float], serial_hints: list[str]) -> bool:
    if not isinstance(item, dict):
        return False
    item_type = str(item.get("type") or "").strip().lower()
    notes = str(item.get("notes") or "")
    reading = _parse_reading_to_float(item.get("reading"))
    serial_norm = _normalize_serial(item.get("serial"))
    tail_match = 0
    if serial_norm and serial_hints:
        tail_match = max((_serial_tail_match_len(serial_norm, h) for h in serial_hints), default=0)

    if reading is None:
        if item_type and item_type != "unknown":
            return True
        return tail_match >= 4

    if _looks_like_serial_reading(reading, serial_norm):
        return False
    if (
        "water_no_ok_odometer_winner" in notes
        or "water_context_far_singleton" in notes
        or "serial_target_multi_hint_unconfirmed" in notes
    ):
        return False
    if item_type == "электро":
        return True
    if item_type and item_type != "unknown":
        return True
    if prev_values:
        return _nearest_prev_distance(reading, prev_values) <= 220.0
    return tail_match >= 4 and float(item.get("confidence") or 0.0) >= 0.86


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
    context_serial_prev: str | None = None,
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
                context_serial_prev=context_serial_prev,
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


def _resolve_kind_by_type_and_serial(
    raw_kind: str | None,
    serial_norm: str | None,
    *,
    cold_serial: str | None = None,
    hot_serial: str | None = None,
) -> dict[str, Any]:
    """
    API-side business rule:
    1) raw OCR type is useful, but not authoritative;
    2) if OCR serial matches apartment cold/hot serial, stored serial ownership wins;
    3) if serial is absent/unmatched but raw type is known, keep raw type;
    4) if neither gives a type, the caller must keep the event on review path.
    """
    rk = str(raw_kind or "").strip().lower()
    if rk not in ("cold", "hot", "electric"):
        rk = None

    s_last5 = _last5_serial(serial_norm)
    cold_last5 = _last5_serial(cold_serial)
    hot_last5 = _last5_serial(hot_serial)
    cold_match = _serial_last5_matches(s_last5, cold_last5)
    hot_match = _serial_last5_matches(s_last5, hot_last5)

    serial_force_kind: str | None = None
    serial_match = "none"
    if s_last5:
        if cold_match and not hot_match:
            serial_force_kind = "cold"
            serial_match = "cold"
        elif hot_match and not cold_match:
            serial_force_kind = "hot"
            serial_match = "hot"
        elif cold_match and hot_match:
            serial_match = "ambiguous"
        elif cold_last5 or hot_last5:
            serial_match = "mismatch"
        else:
            serial_match = "no_profile_serial"

    resolved_kind = serial_force_kind or rk
    if serial_force_kind:
        policy = "serial_authoritative"
    elif rk:
        policy = "raw_type_without_serial_match"
    else:
        policy = "review_no_type_no_serial"

    return {
        "policy": policy,
        "raw_kind": rk,
        "resolved_kind": resolved_kind,
        "serial_force_kind": serial_force_kind,
        "serial_match": serial_match,
        "serial_last5": s_last5 or None,
        "profile_serial_last5": {
            "cold": cold_last5 or None,
            "hot": hot_last5 or None,
        },
        "type_conflict": bool(serial_force_kind and rk and rk != serial_force_kind),
    }


def _should_hold_unresolved_water_for_review(
    *,
    kind: str | None,
    ocr_type: str | None,
    serial_norm: str | None,
    value_float: float | None,
    is_water_context: bool,
    serial_resolution: dict[str, Any] | None,
) -> bool:
    if value_float is None:
        return False
    if not is_water_context:
        return False
    if kind in ("cold", "hot", "electric"):
        return False
    if serial_norm:
        return False
    if isinstance(serial_resolution, dict) and serial_resolution.get("resolved_kind") in ("cold", "hot", "electric"):
        return False
    raw_type = str(ocr_type or "").strip().lower()
    return raw_type in ("", "unknown", "water", "вода")


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


_API_REVIEW_TRACE_WARNING_PHASES: dict[str, str] = {
    "water_prev_sanity_corrected": "water_prev_sanity",
    "water_prev_sanity_blocked": "water_prev_sanity",
    "water_prev_sanity_saved_with_review": "water_prev_sanity",
    "water_prev_hard_block": "water_prev_sanity",
    "water_debug_recovered": "water_debug_recovery",
    "anomaly_jump": "anomaly_gate",
    "anomaly_saved_with_review": "anomaly_gate",
    "serial_mismatch": "serial_gate",
    "water_serial_prev_corrected": "serial_history_gate",
    "water_serial_prev_saved_with_review": "serial_history_gate",
    "ocr_type_conflict": "type_resolution",
    "water_type_uncertain": "type_resolution",
    "serial_type_override": "type_resolution",
    "meter_type_unresolved": "type_resolution",
    "retake_overwrite": "write_path",
    "possible_duplicate": "write_path",
}


def _compact_water_candidate_for_diag(
    candidate: Optional[dict[str, Any]],
    *,
    include_serial_metrics: bool = False,
    include_odometer_metrics: bool = False,
) -> Optional[dict[str, Any]]:
    if not isinstance(candidate, dict):
        return None
    payload: dict[str, Any] = {
        "source": candidate.get("source"),
        "variant": candidate.get("variant"),
        "provider": candidate.get("provider"),
        "reading": candidate.get("reading"),
        "serial": candidate.get("serial"),
        "candidate_score": candidate.get("candidate_score"),
        "suspicious_flags": list(candidate.get("suspicious_flags") or [])[:5],
    }
    if include_serial_metrics:
        payload["serial_confidence"] = candidate.get("serial_confidence")
        payload["tail_match"] = candidate.get("tail_match")
    if include_odometer_metrics:
        payload["integer_digits"] = candidate.get("integer_digits")
        payload["decimal_digits"] = candidate.get("decimal_digits")
        payload["odo_confidence"] = candidate.get("odo_confidence")
        payload["geometry_confidence"] = candidate.get("geometry_confidence")
    return jsonable_encoder({k: v for k, v in payload.items() if v is not None and v != []})


def _summarize_water_decision_for_diag(water_decision: dict[str, Any]) -> dict[str, Any]:
    winner = water_decision.get("winner") if isinstance(water_decision.get("winner"), dict) else None
    serial_branch = water_decision.get("serial_branch") if isinstance(water_decision.get("serial_branch"), dict) else None
    odometer_branch = water_decision.get("odometer_branch") if isinstance(water_decision.get("odometer_branch"), dict) else None
    serial_winner = (serial_branch or {}).get("winner") if isinstance((serial_branch or {}).get("winner"), dict) else None
    odometer_winner = (odometer_branch or {}).get("winner") if isinstance((odometer_branch or {}).get("winner"), dict) else None
    ranked = [item for item in list(water_decision.get("ranked") or []) if isinstance(item, dict)]
    return jsonable_encoder(
        {
            "model": water_decision.get("model"),
            "pool_size": water_decision.get("pool_size"),
            "strict_pool_size": water_decision.get("strict_pool_size"),
            "strong_pool_size": water_decision.get("strong_pool_size"),
            "override": water_decision.get("override"),
            "context_override_applied": bool(water_decision.get("context_override_applied")),
            "serial_tail_like": bool(water_decision.get("serial_tail_like")),
            "winner": _compact_water_candidate_for_diag(winner, include_odometer_metrics=True),
            "serial_branch_winner": _compact_water_candidate_for_diag(serial_winner, include_serial_metrics=True),
            "odometer_branch_winner": _compact_water_candidate_for_diag(odometer_winner, include_odometer_metrics=True),
            "top_sources": [
                item
                for item in [
                    {
                        "source": cand.get("source"),
                        "variant": cand.get("variant"),
                        "reading": cand.get("reading"),
                        "candidate_score": cand.get("candidate_score"),
                    }
                    for cand in ranked[:3]
                ]
                if item.get("source")
            ],
        }
    )


def _compact_local_recognizer_candidate(candidate: Any) -> Optional[dict[str, Any]]:
    if not isinstance(candidate, dict):
        return None
    payload = {
        "kind": candidate.get("kind"),
        "source": candidate.get("source"),
        "reading": candidate.get("reading"),
        "serial": candidate.get("serial"),
        "digits": candidate.get("digits"),
        "integer_digits": candidate.get("integer_digits"),
        "decimal_digits": candidate.get("decimal_digits"),
        "raw_digits": candidate.get("raw_digits"),
        "zone_id": candidate.get("zone_id"),
        "zone_kind": candidate.get("zone_kind"),
        "candidate_score": candidate.get("candidate_score"),
        "serial_confidence": candidate.get("serial_confidence"),
        "serial_source": candidate.get("serial_source"),
        "geometry_confidence": candidate.get("geometry_confidence"),
        "model_version": candidate.get("model_version"),
        "digit_confidences": list(candidate.get("digit_confidences") or [])[:12],
        "suspicious_flags": list(candidate.get("suspicious_flags") or [])[:5],
    }
    return jsonable_encoder({k: v for k, v in payload.items() if v is not None and v != []})


def _summarize_local_recognizer_for_diag(local_recognizer: dict[str, Any]) -> dict[str, Any]:
    zones = [z for z in list(local_recognizer.get("zones") or []) if isinstance(z, dict)]
    water_candidates = [c for c in list(local_recognizer.get("water_candidates") or []) if isinstance(c, dict)]
    electric_candidates = [c for c in list(local_recognizer.get("electric_candidates") or []) if isinstance(c, dict)]
    serial_candidates = [c for c in list(local_recognizer.get("serial_candidates") or []) if isinstance(c, dict)]
    return jsonable_encoder(
        {
            "version": local_recognizer.get("version"),
            "status": local_recognizer.get("status"),
            "mode": local_recognizer.get("mode"),
            "tesseract_enabled": bool(local_recognizer.get("tesseract_enabled")),
            "digit_classifier_enabled": bool(local_recognizer.get("digit_classifier_enabled")),
            "digit_classifier_version": local_recognizer.get("digit_classifier_version"),
            "elapsed_ms": local_recognizer.get("elapsed_ms"),
            "winner": _compact_local_recognizer_candidate(local_recognizer.get("winner")),
            "top_water": [
                item for item in (_compact_local_recognizer_candidate(c) for c in water_candidates[:3]) if item
            ],
            "top_electric": [
                item for item in (_compact_local_recognizer_candidate(c) for c in electric_candidates[:3]) if item
            ],
            "top_serial": [
                {
                    "source": c.get("source"),
                    "serial": c.get("serial"),
                    "serial_confidence": c.get("serial_confidence"),
                    "zone_id": c.get("zone_id"),
                    "crop": c.get("crop"),
                }
                for c in serial_candidates[:3]
            ],
            "zones": [
                {
                    "id": z.get("id"),
                    "source": z.get("source"),
                    "kind_hint": z.get("kind_hint"),
                    "bbox": z.get("bbox"),
                    "digit_like_components": z.get("digit_like_components"),
                    "geometry_confidence": z.get("geometry_confidence"),
                    "red_pixel_ratio": z.get("red_pixel_ratio"),
                    "digit_classifier": z.get("digit_classifier"),
                    "tesseract_digits": ((z.get("tesseract") or {}).get("digits") if isinstance(z.get("tesseract"), dict) else None),
                }
                for z in zones[:5]
            ],
        }
    )


def _get_electric_month_snapshot(conn, apartment_id: int, ym: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT meter_index, value, source, ocr_value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index IN (1,2,3)
            ORDER BY meter_index
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchall()
    out: list[dict[str, Any]] = []
    for meter_index, value, source, ocr_value in (rows or []):
        item: dict[str, Any] = {
            "meter_index": int(meter_index),
            "source": str(source or ""),
        }
        if value is not None:
            item["value"] = float(value)
        if ocr_value is not None:
            item["ocr_value"] = float(ocr_value)
        out.append(item)
    return out


def _pick_same_file_electric_correction_candidate(
    previous_events: list[dict[str, Any]],
    rows_before: list[dict[str, Any]],
    new_value: Optional[float],
) -> Optional[dict[str, Any]]:
    if new_value is None:
        return None

    rows_by_idx: dict[int, dict[str, Any]] = {}
    for row in rows_before or []:
        try:
            idx = int(row.get("meter_index") or 0)
        except Exception:
            continue
        if idx not in (1, 2, 3):
            continue
        rows_by_idx[idx] = dict(row)

    same_value_events: list[dict[str, Any]] = []
    stale_candidates: list[dict[str, Any]] = []
    for raw in previous_events or []:
        row = dict(raw or {})
        try:
            idx = int(row.get("meter_index") or 0)
        except Exception:
            continue
        if idx not in (1, 2, 3):
            continue
        old_value = _parse_reading_to_float(row.get("meter_value"))
        if old_value is None:
            old_value = _parse_reading_to_float(row.get("ocr_reading"))
        if old_value is None:
            continue
        event_item = {
            "event_id": row.get("id"),
            "meter_index": idx,
            "value": float(old_value),
        }
        if _same_total(float(old_value), float(new_value)):
            same_value_events.append(event_item)
            continue

        db_row = rows_by_idx.get(idx)
        if not db_row:
            continue
        if str(db_row.get("source") or "").strip().lower() != "ocr":
            continue
        db_value = _parse_reading_to_float(db_row.get("value"))
        if db_value is None or not _same_total(float(db_value), float(old_value)):
            continue
        stale_candidates.append(event_item)

    if not stale_candidates:
        return None

    chosen = stale_candidates[0]
    assigned_idx = int(chosen["meter_index"])
    duplicate_indices: list[int] = []
    duplicate_event_ids: list[Any] = []
    for item in same_value_events:
        try:
            idx = int(item.get("meter_index") or 0)
        except Exception:
            continue
        if idx not in (1, 2, 3) or idx == assigned_idx:
            continue
        db_row = rows_by_idx.get(idx)
        if not db_row:
            continue
        if str(db_row.get("source") or "").strip().lower() != "ocr":
            continue
        db_value = _parse_reading_to_float(db_row.get("value"))
        if db_value is None or not _same_total(float(db_value), float(new_value)):
            continue
        if idx not in duplicate_indices:
            duplicate_indices.append(idx)
        duplicate_event_ids.append(item.get("event_id"))

    return {
        "reason": "same_file_ocr_correction",
        "stale_event_id": chosen.get("event_id"),
        "assigned_meter_index": assigned_idx,
        "previous_value": float(chosen["value"]),
        "new_value": float(new_value),
        "same_value_event_ids": [item.get("event_id") for item in same_value_events],
        "duplicate_indices": duplicate_indices,
        "duplicate_event_ids": duplicate_event_ids,
    }


def _try_apply_same_file_electric_correction(
    conn,
    *,
    apartment_id: int,
    ym: str,
    file_sha256: Optional[str],
    photo_event_id: Optional[int],
    new_value: Optional[float],
    rows_before: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not file_sha256 or new_value is None:
        return None
    previous = conn.execute(
        text(
            """
            SELECT id, meter_index, meter_value, ocr_reading, created_at
            FROM photo_events
            WHERE apartment_id=:aid
              AND ym=:ym
              AND file_sha256=:sha
              AND meter_kind='electric'
              AND meter_written=true
              AND (:peid IS NULL OR id <> :peid)
            ORDER BY created_at DESC, id DESC
            LIMIT 12
            """
        ),
        {
            "aid": int(apartment_id),
            "ym": str(ym),
            "sha": str(file_sha256),
            "peid": int(photo_event_id) if photo_event_id is not None else None,
        },
    ).mappings().all()
    correction = _pick_same_file_electric_correction_candidate(
        [dict(row) for row in (previous or [])],
        rows_before,
        float(new_value),
    )
    if not correction:
        return None

    assigned_idx = int(correction["assigned_meter_index"])
    previous_value = float(correction["previous_value"])
    result = conn.execute(
        text(
            """
            UPDATE meter_readings
            SET value=:new_value,
                source='ocr',
                ocr_value=:new_value,
                updated_at=now()
            WHERE apartment_id=:aid
              AND ym=:ym
              AND meter_type='electric'
              AND meter_index=:idx
              AND source='ocr'
              AND abs(value - :previous_value) <= 0.01
            """
        ),
        {
            "aid": int(apartment_id),
            "ym": str(ym),
            "idx": int(assigned_idx),
            "previous_value": float(previous_value),
            "new_value": float(new_value),
        },
    )
    if int(getattr(result, "rowcount", 0) or 0) <= 0:
        return None

    deleted_duplicate_indices: list[int] = []
    for idx in list(correction.get("duplicate_indices") or []):
        try:
            dup_idx = int(idx)
        except Exception:
            continue
        if dup_idx not in (1, 2, 3) or dup_idx == assigned_idx:
            continue
        dup_result = conn.execute(
            text(
                """
                DELETE FROM meter_readings
                WHERE apartment_id=:aid
                  AND ym=:ym
                  AND meter_type='electric'
                  AND meter_index=:idx
                  AND source='ocr'
                  AND abs(value - :new_value) <= 0.01
                """
            ),
            {
                "aid": int(apartment_id),
                "ym": str(ym),
                "idx": int(dup_idx),
                "new_value": float(new_value),
            },
        )
        if int(getattr(dup_result, "rowcount", 0) or 0) > 0:
            deleted_duplicate_indices.append(int(dup_idx))

    correction["deleted_duplicate_indices"] = deleted_duplicate_indices
    return jsonable_encoder(correction)


def _pick_same_file_cross_month_reuse_candidate(
    previous_events: list[dict[str, Any]],
    ym: str,
) -> Optional[dict[str, Any]]:
    target_ym = str(ym or "").strip()
    if not target_ym:
        return None
    # Same-month repeats are normal retakes/corrections. A file may have older
    # cross-month history as well, but that must not block a current-month retake.
    for raw in previous_events or []:
        row = dict(raw or {})
        if str(row.get("ym") or "").strip() == target_ym:
            return None
    for raw in previous_events or []:
        row = dict(raw or {})
        prev_ym = str(row.get("ym") or "").strip()
        if not prev_ym or prev_ym == target_ym:
            continue
        return jsonable_encoder(
            {
                "reason": "same_file_cross_month_reuse",
                "previous_event_id": row.get("id"),
                "previous_ym": prev_ym,
                "current_ym": target_ym,
                "previous_meter_kind": row.get("meter_kind"),
                "previous_meter_index": row.get("meter_index"),
                "previous_meter_value": row.get("meter_value"),
            }
        )
    return None


def _find_same_file_cross_month_reuse(
    conn,
    *,
    apartment_id: int,
    ym: str,
    file_sha256: Optional[str],
    photo_event_id: Optional[int],
) -> Optional[dict[str, Any]]:
    if not file_sha256:
        return None
    rows = conn.execute(
        text(
            """
            SELECT id, ym, meter_kind, meter_index, meter_value, created_at
            FROM photo_events
            WHERE apartment_id=:aid
              AND file_sha256=:sha
              AND (:peid IS NULL OR id <> :peid)
            ORDER BY created_at DESC, id DESC
            LIMIT 12
            """
        ),
        {
            "aid": int(apartment_id),
            "sha": str(file_sha256),
            "peid": int(photo_event_id) if photo_event_id is not None else None,
        },
    ).mappings().all()
    return _pick_same_file_cross_month_reuse_candidate([dict(row) for row in (rows or [])], str(ym))


def _set_electric_assignment_debug(
    diag: dict[str, Any],
    *,
    apartment_id: int,
    ym: str,
    expected: Optional[int],
    extra_pending: Optional[bool],
    expected_snapshot: Optional[int],
    incoming_value: Optional[float],
    mode: Optional[str],
    close_idx: Optional[int],
    tariff_index: Optional[int],
    meter_index_mode: Optional[str],
    requested_meter_index: Optional[int],
    assigned_meter_index: Optional[int],
    rows_before: list[dict[str, Any]],
    rows_after: list[dict[str, Any]],
) -> None:
    diag["electric_assignment"] = jsonable_encoder(
        {
            "apartment_id": int(apartment_id),
            "ym": str(ym),
            "expected": (int(expected) if expected is not None else None),
            "extra_pending": (bool(extra_pending) if extra_pending is not None else None),
            "expected_snapshot": (int(expected_snapshot) if expected_snapshot is not None else None),
            "incoming_value": (float(incoming_value) if incoming_value is not None else None),
            "mode": (str(mode) if mode else None),
            "close_idx": (int(close_idx) if close_idx is not None else None),
            "tariff_index": (int(tariff_index) if tariff_index is not None else None),
            "meter_index_mode": (str(meter_index_mode) if meter_index_mode is not None else None),
            "requested_meter_index": (int(requested_meter_index) if requested_meter_index is not None else None),
            "assigned_meter_index": (int(assigned_meter_index) if assigned_meter_index is not None else None),
            "rows_before": rows_before,
            "rows_after": rows_after,
        }
    )


def _resolve_electric_assigned_index(
    rows_after: list[dict[str, Any]],
    new_value: Optional[float],
    fallback_index: Optional[int],
) -> Optional[int]:
    if new_value is None or not rows_after:
        return fallback_index

    matches: list[int] = []
    for row in rows_after:
        try:
            row_value = row.get("value")
            if row_value is None:
                continue
            if _same_total(float(row_value), float(new_value)):
                matches.append(int(row.get("meter_index") or 0))
        except Exception:
            continue

    matches = [idx for idx in matches if idx in (1, 2, 3)]
    if not matches:
        return fallback_index
    if fallback_index in matches:
        return int(fallback_index)
    return int(matches[0])


def _set_api_review_trace(
    diag: dict[str, Any],
    *,
    ym: str,
    apartment_id: Optional[int],
    raw_ocr_type: Optional[str],
    raw_ocr_reading: Optional[Any],
    raw_ocr_serial: Optional[str],
    resolved_meter_kind: Optional[str],
    resolved_meter_label: Optional[str],
    assigned_meter_index: int,
    meter_written: bool,
    event_status: Optional[str],
    photo_event_id: Optional[int] = None,
    ydisk_path: Optional[str] = None,
    reason_override: Optional[str] = None,
    serial_force_kind: Optional[str] = None,
) -> None:
    entries: list[dict[str, Any]] = []

    def _append(phase: str, event: str, **details: Any) -> None:
        payload = {
            "phase": str(phase),
            "event": str(event),
            "details": jsonable_encoder(
                {
                    k: v
                    for k, v in details.items()
                    if v is not None and v != [] and v != {}
                }
            ),
        }
        entries.append(payload)

    water_decision = diag.get("ocr_water_decision") if isinstance(diag.get("ocr_water_decision"), dict) else None
    if water_decision:
        winner = water_decision.get("winner") if isinstance(water_decision.get("winner"), dict) else None
        _append(
            "ocr_winner",
            "water_candidate_selected",
            source=(winner or {}).get("source"),
            variant=(winner or {}).get("variant"),
            reading=(winner or {}).get("reading"),
            serial=(winner or {}).get("serial"),
            candidate_score=(winner or {}).get("candidate_score"),
            suspicious_flags=(winner or {}).get("suspicious_flags"),
            override=water_decision.get("override"),
            serial_tail_like=water_decision.get("serial_tail_like"),
        )

    electric_assignment = diag.get("electric_assignment") if isinstance(diag.get("electric_assignment"), dict) else None
    if electric_assignment:
        _append(
            "electric_assignment",
            "electric_assignment",
            expected=electric_assignment.get("expected"),
            extra_pending=electric_assignment.get("extra_pending"),
            expected_snapshot=electric_assignment.get("expected_snapshot"),
            incoming_value=electric_assignment.get("incoming_value"),
            mode=electric_assignment.get("mode"),
            close_idx=electric_assignment.get("close_idx"),
            tariff_index=electric_assignment.get("tariff_index"),
            meter_index_mode=electric_assignment.get("meter_index_mode"),
            requested_meter_index=electric_assignment.get("requested_meter_index"),
            assigned_meter_index=electric_assignment.get("assigned_meter_index"),
            rows_before=electric_assignment.get("rows_before"),
            rows_after=electric_assignment.get("rows_after"),
        )

    meter_resolution = diag.get("meter_resolution") if isinstance(diag.get("meter_resolution"), dict) else None
    if meter_resolution:
        _append(
            "type_resolution",
            "meter_resolution_policy",
            policy=meter_resolution.get("policy"),
            raw_kind=meter_resolution.get("raw_kind"),
            resolved_kind=meter_resolution.get("resolved_kind"),
            serial_force_kind=meter_resolution.get("serial_force_kind"),
            serial_match=meter_resolution.get("serial_match"),
            serial_last5=meter_resolution.get("serial_last5"),
            profile_serial_last5=meter_resolution.get("profile_serial_last5"),
            type_conflict=meter_resolution.get("type_conflict"),
            has_serial=meter_resolution.get("has_serial"),
        )

    _append(
        "type_resolution",
        "resolved_meter_type",
        ym=str(ym),
        apartment_id=int(apartment_id) if apartment_id is not None else None,
        raw_ocr_type=(str(raw_ocr_type) if raw_ocr_type is not None else None),
        raw_ocr_reading=raw_ocr_reading,
        raw_ocr_serial=(str(raw_ocr_serial) if raw_ocr_serial is not None else None),
        serial_force_kind=(str(serial_force_kind) if serial_force_kind else None),
        resolved_meter_kind=(str(resolved_meter_kind) if resolved_meter_kind else None),
        resolved_meter_label=(str(resolved_meter_label) if resolved_meter_label else None),
        assigned_meter_index=int(assigned_meter_index),
    )

    seen_warning_events: set[str] = set()
    for warning in list(diag.get("warnings") or []):
        if not isinstance(warning, dict):
            continue
        for key, value in warning.items():
            phase = _API_REVIEW_TRACE_WARNING_PHASES.get(str(key))
            if not phase:
                continue
            fingerprint = f"{phase}:{key}:{json.dumps(jsonable_encoder(value), ensure_ascii=False, sort_keys=True)}"
            if fingerprint in seen_warning_events:
                continue
            seen_warning_events.add(fingerprint)
            _append(phase, str(key), payload=value)

    _append(
        "final_decision",
        "meter_written" if meter_written else "needs_review",
        reason=(str(reason_override) if reason_override else None),
        meter_written=bool(meter_written),
        event_status=(str(event_status) if event_status is not None else None),
        resolved_meter_kind=(str(resolved_meter_kind) if resolved_meter_kind else None),
        resolved_meter_label=(str(resolved_meter_label) if resolved_meter_label else None),
        assigned_meter_index=int(assigned_meter_index),
        photo_event_id=int(photo_event_id) if photo_event_id is not None else None,
        ydisk_path=(str(ydisk_path) if ydisk_path else None),
    )

    diag["api_review_trace"] = entries


def _ocr_payload_quality(payload: Any) -> float:
    if not isinstance(payload, dict):
        return -1.0
    score = 0.0
    reading = _parse_reading_to_float(payload.get("reading"))
    if reading is not None:
        score += 1.0
    ocr_type = str(payload.get("type") or "").strip().lower()
    variant = str(payload.get("variant") or "").strip().lower()
    provider = str(payload.get("provider") or "").strip().lower()
    notes = str(payload.get("notes") or "").strip().lower()
    digits = "".join(ch for ch in str(payload.get("digits") or "") if ch.isdigit())
    if ocr_type == "электро":
        score += 0.25
    if digits:
        score += 0.2
    if len(digits) >= 6:
        score += 0.9
    if "electric_seed_display_fast_path" in notes:
        score += 2.5
    if _electric_has_display_support(payload):
        score += 2.4
    if any(tag in variant for tag in ("direct_bridge", "prefix_tail", "integer_bridge", "display", "cells", "template", "det_", "tess")):
        score += 1.2
    if any(tag in provider for tag in (":display", ":cells", ":integer", ":fraction", "display-hybrid")):
        score += 0.9
    if "orig_fullframe" in variant:
        score -= 1.5
    if "provider=openai:gpt-4o; variant=orig_fullframe" in notes:
        score -= 1.0
    if _is_uncorroborated_electric_fullframe(payload):
        score -= 2.0
    return score


def _electric_has_display_support(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("type") or "").strip().lower() != "электро":
        return False
    notes = str(payload.get("notes") or "").strip().lower()
    variant = str(payload.get("variant") or "").strip().lower()
    provider = str(payload.get("provider") or "").strip().lower()
    support_markers = (
        "provider=openai-electric",
        "openai-electric:",
        "electric_mercury_bridge_forced",
        "electric_seeded_display",
        "electric_seed_display_fast_path",
        "variant=electric_ed_",
        "electric_ed_",
        "display-hybrid",
        ":display",
        ":cells",
        ":integer",
        ":fraction",
        "direct_bridge",
        "prefix_tail",
        "integer_bridge",
    )
    if any(marker in notes for marker in support_markers):
        return True
    if any(marker in variant for marker in ("electric_ed_", "direct_bridge", "prefix_tail", "integer_bridge", "display", "cells", "template", "det_", "tess")):
        return True
    if any(marker in provider for marker in ("openai-electric", ":display", ":cells", ":integer", ":fraction", "display-hybrid")):
        return True
    for cand in list(payload.get("debug") or [])[:12]:
        if not isinstance(cand, dict):
            continue
        if str(cand.get("type") or payload.get("type") or "").strip().lower() not in ("электро", "electric"):
            continue
        if _parse_reading_to_float(cand.get("reading")) is None:
            continue
        c_provider = str(cand.get("provider") or "").strip().lower()
        c_variant = str(cand.get("variant") or "").strip().lower()
        if "openai-electric" in c_provider or any(marker in c_provider for marker in (":display", ":cells", ":integer", ":fraction")):
            return True
        if any(marker in c_variant for marker in ("electric_ed_", "direct_bridge", "prefix_tail", "integer_bridge", "display", "cells", "template", "det_", "tess")):
            return True
    return False


def _is_uncorroborated_electric_fullframe(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("type") or "").strip().lower() != "электро":
        return False
    if _parse_reading_to_float(payload.get("reading")) is None:
        return False
    if _electric_has_display_support(payload):
        return False
    variant = str(payload.get("variant") or "").strip().lower()
    provider = str(payload.get("provider") or "").strip().lower()
    notes = str(payload.get("notes") or "").strip().lower()
    return bool(
        "orig_fullframe" in variant
        or provider == "openai:gpt-4o"
        or "variant=orig_fullframe" in notes
        or "provider=openai:gpt-4o; variant=orig_fullframe" in notes
    )


def _needs_electric_quality_retry(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("type") or "").strip().lower() != "электро":
        return False
    variant = str(payload.get("variant") or "").strip().lower()
    provider = str(payload.get("provider") or "").strip().lower()
    notes = str(payload.get("notes") or "").strip().lower()
    digits = "".join(ch for ch in str(payload.get("digits") or "") if ch.isdigit())
    if "electric_seed_display_fast_path" in notes:
        return False
    if _electric_has_display_support(payload):
        return False
    if _is_uncorroborated_electric_fullframe(payload):
        return True
    if any(tag in variant for tag in ("direct_bridge", "prefix_tail", "integer_bridge")):
        return False
    if ("orig_fullframe" in variant) or (provider == "openai:gpt-4o") or ("variant=orig_fullframe" in notes):
        return True
    return len(digits) < 6


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
    telegram_message_id = form.get("telegram_message_id") or None
    telegram_media_group_id = form.get("telegram_media_group_id") or None
    client_file_sha256 = (str(form.get("client_file_sha256") or "").strip().lower() or None)
    client_file_size_raw = (str(form.get("client_file_size") or "").strip() or None)
    client_original_filename = (str(form.get("client_original_filename") or "").strip() or None)
    client_file_unique_id = (str(form.get("client_file_unique_id") or "").strip() or None)
    client_batch_kind = (str(form.get("client_batch_kind") or "").strip() or None)
    client_batch_item_index = (str(form.get("client_batch_item_index") or "").strip() or None)
    client_batch_total = (str(form.get("client_batch_total") or "").strip() or None)
    if telegram_message_id is not None:
        try:
            diag["telegram_message_id"] = int(str(telegram_message_id).strip())
        except Exception:
            diag["telegram_message_id"] = str(telegram_message_id)
    if telegram_media_group_id is not None and str(telegram_media_group_id).strip():
        diag["telegram_media_group_id"] = str(telegram_media_group_id).strip()
    if client_file_sha256:
        diag["client_file_sha256"] = client_file_sha256
    if client_file_size_raw:
        diag["client_file_size"] = client_file_size_raw
    if client_original_filename:
        diag["client_original_filename"] = client_original_filename
    if client_file_unique_id:
        diag["client_file_unique_id"] = client_file_unique_id
    if client_batch_kind or client_batch_item_index or client_batch_total:
        diag["client_batch"] = {
            "kind": client_batch_kind,
            "item_index": client_batch_item_index,
            "total": client_batch_total,
        }
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

    def _refresh_selected_file_diag(reason: str) -> None:
        diag["selected_file"] = {
            "index": int(selected_idx),
            "files_count": int(len(photo_payloads)),
            "filename": selected_filename,
            "mime": selected_mime,
            "size_bytes": int(len(blob)),
            "sha256": file_sha256,
            "sha16": file_sha256[:16],
            "selection_reason": str(reason),
        }
        if client_file_sha256:
            diag["selected_file"]["client_sha256"] = client_file_sha256
            diag["selected_file"]["client_sha16"] = client_file_sha256[:16]
        if client_original_filename:
            diag["selected_file"]["client_original_filename"] = client_original_filename
        if client_file_unique_id:
            diag["selected_file"]["client_file_unique_id"] = client_file_unique_id
        if client_batch_kind or client_batch_item_index or client_batch_total:
            diag["selected_file"]["client_batch"] = {
                "kind": client_batch_kind,
                "item_index": client_batch_item_index,
                "total": client_batch_total,
            }

    _refresh_selected_file_diag("initial")
    if client_file_sha256 and client_file_sha256 != file_sha256:
        diag["warnings"].append(
            {
                "client_file_sha_mismatch": {
                    "client_sha16": client_file_sha256[:16],
                    "api_sha16": file_sha256[:16],
                    "client_filename": client_original_filename,
                    "api_filename": selected_filename,
                }
            }
        )
    logger.info(
        "photo_event start trace_id=%s chat_id=%s ym=%s files_count=%s selected_file=%s mime=%s size_bytes=%s sha16=%s",
        trace_id,
        str(chat_id),
        str(ym),
        len(photo_payloads),
        selected_filename,
        selected_mime,
        len(blob),
        file_sha256[:16],
    )

    if db_ready():
        try:
            await run_in_threadpool(ensure_tables)
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
    context_serial_prev: str | None = None
    prev_vals: list[float] = []
    serial_hints: list[str] = []
    serial_prev_pairs: list[tuple[str, float]] = []
    if db_ready() and apartment_id:
        try:
            prev_ym = _prev_ym(str(ym))
            use_training_cluster = False
            with engine.begin() as conn:
                apt_row = conn.execute(
                    text("SELECT cold_serial, hot_serial FROM apartments WHERE id=:aid LIMIT 1"),
                    {"aid": int(apartment_id)},
                ).mappings().first()
                apt_serials: dict[str, str | None] = {}
                if apt_row:
                    apt_serials = {
                        "cold": apt_row.get("cold_serial"),
                        "hot": apt_row.get("hot_serial"),
                    }
                    for raw_serial in (apt_row.get("cold_serial"), apt_row.get("hot_serial")):
                        s_norm = _normalize_serial(raw_serial)
                        if not s_norm:
                            continue
                        sd = "".join(ch for ch in str(s_norm) if ch.isdigit())
                        if len(sd) < 4 or sd in serial_hints:
                            continue
                        serial_hints.append(sd)
                meter_prev_by_type: dict[str, float] = {}
                skipped_meter_prev_by_type: dict[str, dict[str, Any]] = {}
                for mt in ("cold", "hot"):
                    pv, pv_diag = _get_stable_water_context_prev_reading(
                        conn,
                        int(apartment_id),
                        str(ym),
                        mt,
                        1,
                    )
                    if pv_diag is not None:
                        skipped_meter_prev_by_type[mt] = pv_diag
                    if pv is None:
                        continue
                    try:
                        meter_prev_by_type[mt] = float(pv)
                    except Exception:
                        continue
                if skipped_meter_prev_by_type:
                    diag["warnings"].append({"water_context_history_skipped": skipped_meter_prev_by_type})
                training_prev_by_type: dict[str, float] = {}
                for mt in ("cold", "hot"):
                    typed_training = _get_recent_training_values_for_type(conn, int(apartment_id), str(ym), mt, limit=16)
                    if not typed_training:
                        continue
                    typed_cluster = _select_water_context_values(
                        typed_training,
                        max_values=1,
                        support_tol=180.0,
                        cluster_only_if_any=True,
                    )
                    if typed_cluster:
                        training_prev_by_type[mt] = float(typed_cluster[0])
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
                    prev_vals.extend(float(v) for v in meter_prev_by_type.values())
                for mt in ("cold", "hot"):
                    raw_prev_for_serial = training_prev_by_type.get(mt, meter_prev_by_type.get(mt))
                    if raw_prev_for_serial is None:
                        continue
                    pv_for_serial = float(raw_prev_for_serial)
                    s_norm = _normalize_serial(apt_serials.get(mt) if apt_row else None)
                    sd = "".join(ch for ch in str(s_norm or "") if ch.isdigit())
                    if len(sd) < 4:
                        continue
                    serial_prev_pairs.append((sd, pv_for_serial))
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
            if serial_prev_pairs:
                seen_serial_prev: set[str] = set()
                parts: list[str] = []
                for sd, pv in serial_prev_pairs:
                    if sd in seen_serial_prev:
                        continue
                    seen_serial_prev.add(sd)
                    parts.append(f"{sd}={float(pv):.3f}")
                context_serial_prev = ";".join(parts)
                diag["ocr_context_serial_prev"] = context_serial_prev
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
        ocr_resp, ocr_exc = await run_in_threadpool(
            _call_ocr_series_with_retries,
            series_photos,
            trace_id=trace_id,
            context_prev_water=context_prev_water,
            context_serial_hint=context_serial_hint,
            context_serial_prev=context_serial_prev,
        )
    else:
        ocr_resp = None
        ocr_exc = None
        fast_prev_values = _parse_prev_values_context(context_prev_water)
        fast_serial_hints = _parse_serial_hints_context(context_serial_hint)
        context_free_primary = False
        context_free_primary_sufficient = True
        if context_free_primary:
            fast_resp, fast_exc = await run_in_threadpool(
                _call_ocr_with_retries,
                blob,
                filename=selected_filename,
                mime_type=selected_mime,
                trace_id=trace_id,
                context_prev_water=None,
                context_serial_hint=None,
                read_timeout_override_sec=min(60.0, max(35.0, float(OCR_HTTP_TIMEOUT_SEC))),
            )
            if fast_resp is not None and fast_resp.ok:
                ocr_resp, ocr_exc = fast_resp, None
                try:
                    fast_json = fast_resp.json()
                except Exception:
                    fast_json = {}
                fast_payload = fast_json if isinstance(fast_json, dict) else {}
                context_free_primary_sufficient = _single_ocr_result_is_context_sufficient(
                    fast_payload,
                    fast_prev_values,
                    fast_serial_hints,
                )
                diag["warnings"].append(
                    {
                        "ocr_context_free_primary": {
                            "type": fast_payload.get("type"),
                            "reading": fast_payload.get("reading"),
                            "serial": fast_payload.get("serial"),
                            "sufficient": bool(context_free_primary_sufficient),
                        }
                    }
                )
            elif fast_resp is not None:
                ocr_resp, ocr_exc = fast_resp, None
                diag["warnings"].append(f"ocr_context_free_primary_http_{fast_resp.status_code}")
            elif fast_exc is not None:
                ocr_resp, ocr_exc = None, fast_exc
                diag["warnings"].append({"ocr_context_free_primary_error": str(fast_exc)})

        if (not context_free_primary) and ocr_resp is None and ocr_exc is None:
            ocr_resp, ocr_exc = await run_in_threadpool(
                _call_ocr_with_retries,
                blob,
                filename=selected_filename,
                mime_type=selected_mime,
                trace_id=trace_id,
                context_prev_water=context_prev_water,
                context_serial_hint=context_serial_hint,
                context_serial_prev=context_serial_prev,
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
                        _refresh_selected_file_diag("ocr_series_service_local_rescore")
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
                            _refresh_selected_file_diag("ocr_series_best_index")
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
                if context_free_primary and (not context_free_primary_sufficient):
                    raw_reading = _parse_reading_to_float((ocr_data or {}).get("reading")) if isinstance(ocr_data, dict) else None
                    if isinstance(ocr_data, dict) and raw_reading is not None:
                        diag["warnings"].append(
                            {
                                "ocr_context_free_primary_reading_rejected": {
                                    "reading": raw_reading,
                                    "type": ocr_data.get("type"),
                                    "serial": ocr_data.get("serial"),
                                }
                            }
                        )
                        ocr_data["reading"] = None
                        if str(ocr_data.get("type") or "").strip().lower() == "unknown":
                            ocr_data["confidence"] = min(float(ocr_data.get("confidence") or 0.0), 0.45)
        else:
            diag["warnings"].append(f"ocr_http_{ocr_resp.status_code}")
    else:
        diag["warnings"].append("ocr_unavailable")
        if ocr_exc is not None:
            diag["warnings"].append({"ocr_error": str(ocr_exc)})

    primary_ocr_timed_out = bool(ocr_exc is not None and "timed out" in str(ocr_exc).lower())
    context_free_fallback_used = False
    if (
        len(photo_payloads) == 1
        and (not isinstance(ocr_data, dict))
        and (context_prev_water or context_serial_hint)
        and (not context_free_primary)
        and (not primary_ocr_timed_out)
    ):
        context_free_fallback_used = True
        diag["warnings"].append("ocr_context_free_fallback")
        try:
            fallback_resp, fallback_exc = await run_in_threadpool(
                _call_ocr_with_retries,
                blob,
                filename=selected_filename,
                mime_type=selected_mime,
                trace_id=f"{trace_id}-ctxfree",
                context_prev_water=None,
                context_serial_hint=None,
                read_timeout_override_sec=min(60.0, max(35.0, float(OCR_HTTP_TIMEOUT_SEC))),
            )
            if fallback_resp is not None and fallback_resp.ok:
                fallback_json = fallback_resp.json()
                if isinstance(fallback_json, dict):
                    ocr_data = fallback_json
                    ocr_http_ok = True
                    ocr_http_status = int(fallback_resp.status_code)
                    diag["warnings"].append(
                        {
                            "ocr_context_free_fallback_used": {
                                "type": fallback_json.get("type"),
                                "reading": fallback_json.get("reading"),
                                "serial": fallback_json.get("serial"),
                            }
                        }
                    )
                else:
                    diag["warnings"].append("ocr_context_free_fallback_bad_json")
            else:
                if fallback_resp is not None:
                    diag["warnings"].append(f"ocr_context_free_fallback_http_{fallback_resp.status_code}")
                if fallback_exc is not None:
                    diag["warnings"].append({"ocr_context_free_fallback_error": str(fallback_exc)})
        except Exception as e:
            diag["warnings"].append({"ocr_context_free_fallback_exception": str(e)})
    elif (
        len(photo_payloads) == 1
        and (not isinstance(ocr_data, dict))
        and (context_prev_water or context_serial_hint)
        and (not context_free_primary)
        and primary_ocr_timed_out
    ):
        diag["warnings"].append("ocr_context_free_fallback_skipped_after_timeout")

    # Before doing a second OCR pass, try to recover a strong water reading
    # from odometer-style debug candidates already returned by OCR. This avoids
    # a regression where a good drum-window candidate (e.g. ~991.89) gets
    # overwritten by a weaker full-frame retry (e.g. ~998.88).
    if len(photo_payloads) == 1 and isinstance(ocr_data, dict):
        existing_type = str(ocr_data.get("type") or "").strip().lower()
        existing_reading = _parse_reading_to_float(ocr_data.get("reading"))
        if existing_reading is None and existing_type in ("", "unknown"):
            dbg = _debug_candidates_from_ocr(ocr_data)
            serial_norm_dbg = _normalize_serial(ocr_data.get("serial"))
            promoted = None
            promoted_prev = None
            promoted_delta = None
            if prev_vals:
                for pv in prev_vals[:3]:
                    try:
                        pvf = float(pv)
                    except Exception:
                        continue
                    cand = _choose_water_debug_candidate_with_prev(
                        dbg,
                        prev_value=pvf,
                        serial_norm=serial_norm_dbg,
                    )
                    if not cand or cand.get("reading") is None:
                        continue
                    try:
                        delta = abs(float(cand.get("reading")) - pvf)
                    except Exception:
                        continue
                    if (
                        promoted is None
                        or promoted_delta is None
                        or float(delta) < float(promoted_delta)
                        or (
                            abs(float(delta) - float(promoted_delta)) < 1e-9
                            and float(cand.get("confidence") or 0.0) > float(promoted.get("confidence") or 0.0)
                        )
                    ):
                        promoted = dict(cand)
                        promoted_prev = pvf
                        promoted_delta = float(delta)
            if promoted is None:
                odo_pool = [
                    dict(c)
                    for c in dbg
                    if _is_odometer_debug_candidate(c) and (_parse_reading_to_float(c.get("reading")) is not None)
                ]
                if odo_pool:
                    promoted = max(odo_pool, key=lambda c: float(c.get("confidence") or 0.0))
            if promoted and promoted.get("reading") is not None:
                promoted_reading = _parse_reading_to_float(promoted.get("reading"))
                promoted_dist = _nearest_prev_distance(promoted_reading, prev_vals) if prev_vals else float("inf")
                serial_norm_promoted = _normalize_serial(promoted.get("serial") or ocr_data.get("serial"))
                serial_like = _looks_like_serial_reading(promoted_reading, serial_norm_promoted)
                safe_promote = bool(
                    promoted_reading is not None
                    and (not serial_like)
                    and (
                        (prev_vals and promoted_dist <= 220.0)
                        or (not prev_vals and float(promoted.get("confidence") or 0.0) >= 0.86)
                    )
                )
                if safe_promote:
                    ocr_data["reading"] = float(promoted_reading)
                    if (not str(ocr_data.get("type") or "").strip()) or existing_type == "unknown":
                        ocr_data["type"] = promoted.get("type")
                    if (not ocr_data.get("serial")) and promoted.get("serial"):
                        ocr_data["serial"] = promoted.get("serial")
                    diag["warnings"].append(
                        {
                            "ocr_debug_promoted_before_retry": {
                                "reading": float(promoted_reading),
                                "variant": promoted.get("variant"),
                                "provider": promoted.get("provider"),
                                "prev_ref": promoted_prev,
                                "delta": promoted_delta,
                            }
                        }
                    )
                else:
                    diag["warnings"].append(
                        {
                            "ocr_debug_promotion_rejected": {
                                "reading": promoted_reading,
                                "variant": promoted.get("variant"),
                                "provider": promoted.get("provider"),
                                "context_distance": None if not math.isfinite(promoted_dist) else round(float(promoted_dist), 3),
                                "serial_like": bool(serial_like),
                            }
                        }
                    )

    # Single-photo safety retry:
    # if OCR returned unknown/empty (or transport failed), give the same image one extra chance
    # without changing the rest of the decision tree.
    single_retry_needed = False
    single_retry_skip_reason = None
    if len(photo_payloads) == 1 and (not context_free_fallback_used) and (not context_free_primary):
        if not isinstance(ocr_data, dict):
            if primary_ocr_timed_out:
                single_retry_skip_reason = "primary_timeout"
            else:
                single_retry_needed = True
        else:
            existing_type = str(ocr_data.get("type") or "").strip().lower()
            existing_notes = str(ocr_data.get("notes") or "").strip().lower()
            existing_reading = _parse_reading_to_float(ocr_data.get("reading"))
            has_water_context = bool(context_prev_water or context_serial_hint)
            waterish_empty_result = bool(
                has_water_context
                and existing_reading is None
                and (
                    existing_type in ("", "unknown", "хвс", "гвс", "cold", "hot", "water")
                    or bool(ocr_data.get("serial"))
                    or isinstance(ocr_data.get("water_decision"), dict)
                    or isinstance(diag.get("ocr_water_decision"), dict)
                )
            )
            if existing_reading is None and (
                existing_type in ("", "unknown") or "openai_empty_response" in existing_notes
            ):
                if waterish_empty_result:
                    single_retry_skip_reason = "water_empty_result"
                else:
                    single_retry_needed = True
            elif _needs_electric_quality_retry(ocr_data):
                single_retry_needed = True
    if single_retry_skip_reason:
        diag["warnings"].append({"ocr_single_second_pass_skipped": single_retry_skip_reason})
    if single_retry_needed:
        diag["warnings"].append("ocr_single_second_pass")
        try:
            retry_resp, retry_exc = await run_in_threadpool(
                _call_ocr_with_retries,
                blob,
                filename=selected_filename,
                mime_type=selected_mime,
                trace_id=f"{trace_id}-sp2",
                context_prev_water=context_prev_water,
                context_serial_hint=context_serial_hint,
                context_serial_prev=context_serial_prev,
                read_timeout_override_sec=max(float(OCR_HTTP_TIMEOUT_SEC), 110.0),
            )
            if retry_resp is not None and retry_resp.ok:
                retry_json = retry_resp.json()
                retry_type = str((retry_json or {}).get("type") or "").strip().lower()
                retry_reading = _parse_reading_to_float((retry_json or {}).get("reading"))
                current_score = _ocr_payload_quality(ocr_data)
                retry_score = _ocr_payload_quality(retry_json)
                if retry_reading is not None or retry_type not in ("", "unknown"):
                    if (not isinstance(ocr_data, dict)) or (retry_score > current_score + 0.05):
                        ocr_data = retry_json
                        diag["warnings"].append(
                            {
                                "ocr_single_second_pass_quality": {
                                    "current_score": round(float(current_score), 4),
                                    "retry_score": round(float(retry_score), 4),
                                    "chosen": "retry",
                                }
                            }
                        )
                    else:
                        diag["warnings"].append(
                            {
                                "ocr_single_second_pass_quality": {
                                    "current_score": round(float(current_score), 4),
                                    "retry_score": round(float(retry_score), 4),
                                    "chosen": "current",
                                }
                            }
                        )
                    ocr_http_ok = True
                    ocr_http_status = int(retry_resp.status_code)
                    if isinstance(ocr_data, dict) and ocr_data is retry_json:
                        diag["warnings"].append("ocr_single_second_pass_improved")
                    else:
                        diag["warnings"].append("ocr_single_second_pass_no_improve")
                else:
                    diag["warnings"].append("ocr_single_second_pass_no_improve")
            else:
                if retry_resp is not None:
                    diag["warnings"].append(f"ocr_single_second_pass_http_{retry_resp.status_code}")
                if retry_exc is not None:
                    diag["warnings"].append({"ocr_single_second_pass_error": str(retry_exc)})
        except Exception as e:
            diag["warnings"].append({"ocr_single_second_pass_exception": str(e)})

    # Electric safety retry:
    # A generic full-frame read can be confidently wrong on simple LCD photos
    # (e.g. 2834 instead of 2634). If the winner is not display-localized,
    # force a context-free OCR pass so electric display crops get a clean shot.
    if len(photo_payloads) == 1 and _is_uncorroborated_electric_fullframe(ocr_data):
        diag["warnings"].append("ocr_electric_context_free_retry")
        try:
            ef_resp, ef_exc = await run_in_threadpool(
                _call_ocr_with_retries,
                blob,
                filename=selected_filename,
                mime_type=selected_mime,
                trace_id=f"{trace_id}-electric-cf",
                context_prev_water=None,
                context_serial_hint=None,
                context_serial_prev=None,
                read_timeout_override_sec=max(float(OCR_HTTP_TIMEOUT_SEC), 130.0),
            )
            if ef_resp is not None and ef_resp.ok:
                ef_json = ef_resp.json()
                current_score = _ocr_payload_quality(ocr_data)
                retry_score = _ocr_payload_quality(ef_json)
                retry_reading = _parse_reading_to_float((ef_json or {}).get("reading")) if isinstance(ef_json, dict) else None
                if (
                    isinstance(ef_json, dict)
                    and retry_reading is not None
                    and (
                        _electric_has_display_support(ef_json)
                        or retry_score > current_score + 0.75
                    )
                ):
                    ocr_data = ef_json
                    ocr_http_ok = True
                    ocr_http_status = int(ef_resp.status_code)
                    diag["warnings"].append(
                        {
                            "ocr_electric_context_free_retry_quality": {
                                "current_score": round(float(current_score), 4),
                                "retry_score": round(float(retry_score), 4),
                                "chosen": "retry",
                                "reading": float(retry_reading),
                                "display_supported": bool(_electric_has_display_support(ef_json)),
                            }
                        }
                    )
                else:
                    diag["warnings"].append(
                        {
                            "ocr_electric_context_free_retry_quality": {
                                "current_score": round(float(current_score), 4),
                                "retry_score": round(float(retry_score), 4),
                                "chosen": "current",
                            }
                        }
                    )
            else:
                if ef_resp is not None:
                    diag["warnings"].append(f"ocr_electric_context_free_retry_http_{ef_resp.status_code}")
                if ef_exc is not None:
                    diag["warnings"].append({"ocr_electric_context_free_retry_error": str(ef_exc)})
        except Exception as e:
            diag["warnings"].append({"ocr_electric_context_free_retry_exception": str(e)})

    # Safety fallback for multi-photo batch:
    # when /recognize-series fails or times out, run single-image OCR per file and pick best locally.
    if len(photo_payloads) > 1 and (not isinstance(ocr_data, dict)):
        try:
            series_fallback = await run_in_threadpool(
                _call_ocr_series_via_singles,
                series_photos,
                trace_id=trace_id,
                context_prev_water=context_prev_water,
                context_serial_hint=context_serial_hint,
                context_serial_prev=context_serial_prev,
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
                    _refresh_selected_file_diag("ocr_series_single_fallback")
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
    ocr_tariff_index = None
    ocr_water_visual_type_hint = None
    if isinstance(ocr_data, dict):
        ocr_type = ocr_data.get("type")
        ocr_reading = ocr_data.get("reading")
        ocr_confidence = ocr_data.get("confidence")
        ocr_serial = ocr_data.get("serial")
        ocr_tariff_index = _extract_ocr_tariff_index(ocr_data)
        if ocr_tariff_index is not None:
            ocr_data["tariff_index"] = int(ocr_tariff_index)
            diag["ocr_tariff_index"] = int(ocr_tariff_index)
        if ocr_data.get("trace_id"):
            diag["ocr_trace_id"] = ocr_data.get("trace_id")
        ocr_water_visual_type_hint = _extract_water_visual_type_hint(ocr_data)
        if ocr_water_visual_type_hint is not None:
            diag["ocr_water_visual_type_hint"] = jsonable_encoder(ocr_water_visual_type_hint)
        if ocr_data.get("provider_errors"):
            try:
                diag["ocr_provider_errors"] = list(ocr_data.get("provider_errors") or [])[:8]
            except Exception:
                diag["ocr_provider_errors"] = ocr_data.get("provider_errors")
        if isinstance(ocr_data.get("local_recognizer"), dict):
            try:
                diag["ocr_local_recognizer"] = _summarize_local_recognizer_for_diag(
                    dict(ocr_data.get("local_recognizer") or {})
                )
            except Exception as e:
                diag["warnings"].append({"ocr_local_recognizer_parse_failed": str(e)})

    kind = _ocr_to_kind(ocr_type)
    if (
        kind is None
        and isinstance(ocr_water_visual_type_hint, dict)
        and ocr_water_visual_type_hint.get("kind") in ("cold", "hot")
        and float(ocr_water_visual_type_hint.get("confidence") or 0.0) >= 0.64
    ):
        kind = str(ocr_water_visual_type_hint.get("kind"))
        ocr_water_visual_type_hint["used_for_raw_kind"] = True
        diag["ocr_water_visual_type_hint"] = jsonable_encoder(ocr_water_visual_type_hint)
    value_float = _parse_reading_to_float(ocr_reading)
    serial_norm = _normalize_serial(ocr_serial)
    raw_kind_before_serial = kind
    serial_force_kind = None
    serial_resolution = _resolve_kind_by_type_and_serial(kind, serial_norm)
    if db_ready() and apartment_id:
        try:
            with engine.begin() as conn:
                apt_row = conn.execute(
                    text("SELECT cold_serial, hot_serial FROM apartments WHERE id=:aid LIMIT 1"),
                    {"aid": int(apartment_id)},
                ).mappings().first()
            if apt_row:
                serial_resolution = _resolve_kind_by_type_and_serial(
                    kind,
                    serial_norm,
                    cold_serial=apt_row.get("cold_serial"),
                    hot_serial=apt_row.get("hot_serial"),
                )
                serial_force_kind = serial_resolution.get("serial_force_kind")
        except Exception as e:
            diag["warnings"].append({"serial_route_prepare_failed": str(e)})
    else:
        serial_force_kind = serial_resolution.get("serial_force_kind")
    if serial_force_kind:
        kind = str(serial_force_kind)
    elif serial_resolution.get("resolved_kind"):
        kind = str(serial_resolution.get("resolved_kind"))
    serial_resolution["resolved_kind"] = kind if kind in ("cold", "hot", "electric") else None
    serial_resolution["raw_ocr_type"] = (str(ocr_type) if ocr_type is not None else None)
    serial_resolution["has_serial"] = bool(serial_norm)
    if isinstance(ocr_water_visual_type_hint, dict):
        serial_resolution["visual_type_hint"] = ocr_water_visual_type_hint
    diag["meter_resolution"] = jsonable_encoder(serial_resolution)
    if not serial_resolution.get("resolved_kind"):
        diag["warnings"].append(
            {
                "meter_type_unresolved": {
                    "raw_ocr_type": (str(ocr_type) if ocr_type is not None else None),
                    "has_serial": bool(serial_norm),
                    "serial_match": serial_resolution.get("serial_match"),
                    "policy": serial_resolution.get("policy"),
                }
            }
        )
    if serial_resolution.get("type_conflict"):
        diag["warnings"].append(
            {
                "serial_type_override": {
                    "raw_kind": raw_kind_before_serial,
                    "serial_kind": serial_force_kind,
                    "serial_last5": serial_resolution.get("serial_last5"),
                    "policy": serial_resolution.get("policy"),
                }
            }
        )
    try:
        ocr_conf = float(ocr_confidence) if ocr_confidence is not None else 0.0
    except Exception:
        ocr_conf = 0.0
    is_water_unknown = str(ocr_type or "").strip().lower() == "unknown"
    debug_candidates = _debug_candidates_from_ocr(ocr_data if isinstance(ocr_data, dict) else None)
    if isinstance(ocr_data, dict) and isinstance(ocr_data.get("water_decision"), dict):
        try:
            water_decision = dict(ocr_data.get("water_decision") or {})
            serial_branch = water_decision.get("serial_branch") if isinstance(water_decision.get("serial_branch"), dict) else None
            odometer_branch = water_decision.get("odometer_branch") if isinstance(water_decision.get("odometer_branch"), dict) else None
            water_summary = water_decision.get("summary") if isinstance(water_decision.get("summary"), dict) else None
            diag["ocr_water_decision"] = {
                "summary": jsonable_encoder(water_summary or _summarize_water_decision_for_diag(water_decision)),
                "model": water_decision.get("model"),
                "pool_size": water_decision.get("pool_size"),
                "strict_pool_size": water_decision.get("strict_pool_size"),
                "strong_pool_size": water_decision.get("strong_pool_size"),
                "override": water_decision.get("override"),
                "winner": water_decision.get("winner"),
                "ranked": list(water_decision.get("ranked") or [])[:5],
                "serial_branch": {
                    "winner": (serial_branch or {}).get("winner"),
                    "ranked": list((serial_branch or {}).get("ranked") or [])[:5],
                } if serial_branch else None,
                "odometer_branch": {
                    "winner": (odometer_branch or {}).get("winner"),
                    "ranked": list((odometer_branch or {}).get("ranked") or [])[:5],
                } if odometer_branch else None,
                "detection": water_decision.get("detection"),
                "context_serial_prev": water_decision.get("context_serial_prev"),
            }
        except Exception as e:
            diag["warnings"].append({"ocr_water_decision_parse_failed": str(e)})
    is_water_debug = any(_is_odometer_debug_candidate(c) for c in debug_candidates)
    ocr_notes_l = str((ocr_data or {}).get("notes") or "").strip().lower() if isinstance(ocr_data, dict) else ""
    is_water_template_hint = "water_template" in ocr_notes_l or any(
        str(c.get("provider") or "").strip().lower().startswith("det-water:template")
        for c in debug_candidates
    )
    is_water_context = (kind in ("cold", "hot")) or (
        is_water_unknown and (is_water_debug or bool(serial_norm) or is_water_template_hint)
    )
    unresolved_water_review_only = _should_hold_unresolved_water_for_review(
        kind=kind,
        ocr_type=(str(ocr_type) if ocr_type is not None else None),
        serial_norm=serial_norm,
        value_float=value_float,
        is_water_context=bool(is_water_context),
        serial_resolution=serial_resolution,
    )
    if unresolved_water_review_only:
        diag["warnings"].append(
            {
                "water_type_unresolved_review_only": {
                    "policy": "review_no_type_no_serial",
                    "raw_ocr_type": (str(ocr_type) if ocr_type is not None else None),
                    "reading": value_float,
                    "has_serial": bool(serial_norm),
                }
            }
        )

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
            fallback_kind = _ocr_to_kind(fallback.get("type"))
            kind = str(serial_force_kind) if serial_force_kind in ("cold", "hot") else fallback_kind
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
                        "resolved_kind_after_correction": kind,
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
                def _allow_severe_outlier_keep(v_now: float, v_prev) -> bool:
                    try:
                        now_v = float(v_now)
                        prev_v = float(v_prev) if v_prev is not None else None
                    except Exception:
                        return False
                    if prev_v is None:
                        return False
                    if now_v <= 0.0:
                        return False
                    # Hard cap for water counters: keep only plausible absolute range.
                    if now_v > 5000.0:
                        return False
                    # Keep only moderately severe jumps; extreme spikes remain blocked.
                    if abs(now_v - prev_v) > max(2000.0, float(WATER_ANOMALY_THRESHOLD) * 20.0):
                        return False
                    if _looks_like_serial_reading(now_v, serial_norm):
                        return False
                    return True

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
                            candidate_kind = _ocr_to_kind(best_c.get("type"))
                            kind = str(serial_force_kind) if serial_force_kind in ("cold", "hot") else (candidate_kind or kind)
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
                                if _allow_severe_outlier_keep(old_v, prev_ref):
                                    diag["warnings"].append(
                                        {
                                            "water_prev_sanity_saved_with_review": {
                                                "value": old_v,
                                                "prev_ref": prev_ref,
                                                "reason": "severe_outlier_saved_with_review",
                                            }
                                        }
                                    )
                                else:
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
                        if _allow_severe_outlier_keep(old_v, prev_ref):
                            diag["warnings"].append(
                                {
                                    "water_prev_sanity_saved_with_review": {
                                        "value": old_v,
                                        "prev_ref": prev_ref,
                                        "reason": "severe_outlier_no_candidate_saved_with_review",
                                    }
                                }
                            )
                        else:
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
                candidate_kind = _ocr_to_kind(best_c.get("type"))
                kind = str(serial_force_kind) if serial_force_kind in ("cold", "hot") else (candidate_kind or kind)
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

    unresolved_water_review_only = _should_hold_unresolved_water_for_review(
        kind=kind,
        ocr_type=(str(ocr_type) if ocr_type is not None else None),
        serial_norm=serial_norm,
        value_float=value_float,
        is_water_context=bool(is_water_context),
        serial_resolution=serial_resolution,
    )
    if unresolved_water_review_only and not any(
        isinstance(w, dict) and "water_type_unresolved_review_only" in w for w in (diag.get("warnings") or [])
    ):
        diag["warnings"].append(
            {
                "water_type_unresolved_review_only": {
                    "policy": "review_no_type_no_serial",
                    "raw_ocr_type": (str(ocr_type) if ocr_type is not None else None),
                    "reading": value_float,
                    "has_serial": bool(serial_norm),
                }
            }
        )

    electric_uncorroborated_review_only = bool(_is_uncorroborated_electric_fullframe(ocr_data))
    if electric_uncorroborated_review_only:
        diag["warnings"].append(
            {
                "electric_uncorroborated_fullframe_review_only": {
                    "policy": "review_no_display_localized_electric_candidate",
                    "reading": value_float,
                    "raw_ocr_type": (str(ocr_type) if ocr_type is not None else None),
                    "notes": (str((ocr_data or {}).get("notes") or "")[:220] if isinstance(ocr_data, dict) else None),
                }
            }
        )

    # 2.1) history-safe electric repair: if OCR likely dropped a single integer digit,
    # recover it only when one inserted-digit candidate is meaningfully closer to prior values.
    if (
        db_ready()
        and apartment_id
        and kind == "electric"
        and value_float is not None
        and (not electric_uncorroborated_review_only)
    ):
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
            upload_meter_type_label = str(ocr_type or "unknown")
            if kind in ("cold", "hot"):
                upload_meter_type_label = _kind_to_label(str(kind), 1) or upload_meter_type_label
            ydisk_path = await run_in_threadpool(
                upload_to_ydisk,
                str(chat_id),
                chat_name=telegram_username or f"chat_{chat_id}",
                meter_type_label=upload_meter_type_label,
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
    same_file_cross_month_review_only = False
    same_file_cross_month_reuse: Optional[dict[str, Any]] = None
    if db_ready() and apartment_id and photo_event_id and file_sha256:
        try:
            with engine.begin() as conn:
                same_file_cross_month_reuse = _find_same_file_cross_month_reuse(
                    conn,
                    apartment_id=int(apartment_id),
                    ym=str(ym),
                    file_sha256=file_sha256,
                    photo_event_id=int(photo_event_id),
                )
            if same_file_cross_month_reuse:
                same_file_cross_month_review_only = True
                diag["warnings"].append({"same_file_cross_month_reuse": same_file_cross_month_reuse})
        except Exception as e:
            diag["warnings"].append({"same_file_cross_month_reuse_check_failed": str(e)})

    if (
        db_ready()
        and apartment_id
        and (value_float is not None)
        and (kind or is_water_context)
        and (not unresolved_water_review_only)
        and (not electric_uncorroborated_review_only)
        and (not same_file_cross_month_review_only)
    ):
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
                    _set_api_review_trace(
                        diag,
                        ym=str(ym),
                        apartment_id=(int(apartment_id) if apartment_id is not None else None),
                        raw_ocr_type=(str(ocr_type) if ocr_type is not None else None),
                        raw_ocr_reading=ocr_reading,
                        raw_ocr_serial=(str(ocr_serial) if ocr_serial is not None else None),
                        resolved_meter_kind=(str(kind) if kind is not None else None),
                        resolved_meter_label=_kind_to_label((str(kind) if kind is not None else None), assigned_meter_index),
                        assigned_meter_index=int(assigned_meter_index),
                        meter_written=False,
                        event_status="needs_review",
                        photo_event_id=(int(photo_event_id) if photo_event_id is not None else None),
                        ydisk_path=ydisk_path,
                        reason_override="block_write_due_anomaly",
                        serial_force_kind=(str(serial_force_kind) if serial_force_kind else None),
                    )
                    if db_ready() and photo_event_id:
                        try:
                            diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
                            with engine.begin() as conn:
                                conn.execute(
                                    text(
                                        """
                                        UPDATE photo_events
                                        SET diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                                        WHERE id = :id
                                        """
                                    ),
                                    {"id": int(photo_event_id), "diag_json": diag_json_str},
                                )
                        except Exception:
                            pass
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
            water_write_blocked = False
            electric_expected = None
            electric_extra_pending = None
            electric_expected_snapshot = None
            electric_assignment_mode = None
            electric_close_idx = None
            electric_retake_warning = None
            electric_write_blocked_reason = None
            electric_assignment_done = False
            electric_rows_before: list[dict[str, Any]] = []
            electric_rows_after: list[dict[str, Any]] = []

            if kind == "electric":
                # By default always auto-sort.
                # First: if value is very close to an existing one, overwrite that slot.
                close_idx = None
                tariff_idx = int(ocr_tariff_index) if ocr_tariff_index in (1, 2, 3) else None
                prev_manual = None
                prev_manual_value = None
                with engine.begin() as conn:
                    electric_expected = _get_apartment_electric_expected(conn, int(apartment_id))
                    electric_extra_state = _get_month_extra_state(conn, int(apartment_id), str(ym))
                    electric_extra_pending = bool((electric_extra_state or {}).get("pending"))
                    snap = (electric_extra_state or {}).get("snapshot")
                    electric_expected_snapshot = int(snap) if snap is not None else None
                    electric_rows_before = _get_electric_month_snapshot(conn, int(apartment_id), str(ym))
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
                    same_file_correction = _try_apply_same_file_electric_correction(
                        conn,
                        apartment_id=int(apartment_id),
                        ym=str(ym),
                        file_sha256=file_sha256,
                        photo_event_id=(int(photo_event_id) if photo_event_id is not None else None),
                        new_value=(float(value_float) if value_float is not None else None),
                        rows_before=electric_rows_before,
                    )
                    if same_file_correction:
                        assigned_meter_index = int(same_file_correction["assigned_meter_index"])
                        electric_close_idx = int(assigned_meter_index)
                        electric_assignment_mode = "same_file_correction"
                        electric_rows_after = _get_electric_month_snapshot(conn, int(apartment_id), str(ym))
                        diag["warnings"].append({"electric_same_file_correction": same_file_correction})
                        electric_assignment_done = True
                    if (not electric_assignment_done) and tariff_idx is not None:
                        expected_int = int(electric_expected or 0)
                        if expected_int < 3 and tariff_idx > max(1, expected_int):
                            # The photo explicitly says T3 while the apartment profile expects
                            # only T1/T2. Keep the exact target index for review, but do not
                            # silently write a value that contradicts the configured contract.
                            assigned_meter_index = int(tariff_idx)
                            electric_assignment_mode = "tariff_index_exceeds_expected_review"
                            electric_rows_after = list(electric_rows_before)
                            electric_write_blocked_reason = {
                                "reason": "tariff_index_exceeds_expected",
                                "expected": int(electric_expected or 0),
                                "expected_snapshot": (
                                    int(electric_expected_snapshot) if electric_expected_snapshot is not None else None
                                ),
                                "tariff_index": int(tariff_idx),
                                "incoming_value": float(value_float),
                                "assigned_meter_index": int(assigned_meter_index),
                                "rows_before": electric_rows_before,
                                "ydisk_path": ydisk_path,
                            }
                            water_write_blocked = True
                            diag["warnings"].append(
                                {"electric_tariff_index_exceeds_expected_review": dict(electric_write_blocked_reason)}
                            )
                            electric_assignment_done = True
                        else:
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
                                    {"aid": int(apartment_id), "ym": str(ym), "mi": int(tariff_idx)},
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
                                int(tariff_idx),
                                float(value_float),
                            )
                            electric_close_idx = int(assigned_meter_index)
                            electric_assignment_mode = "tariff_index_write"
                            electric_rows_after = _get_electric_month_snapshot(conn, int(apartment_id), str(ym))
                            electric_assignment_done = True

                    best = None
                    if not electric_assignment_done:
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
                    if (not electric_assignment_done) and best:
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
                        if bool(electric_extra_pending) and int(electric_expected or 0) < 3:
                            # Do not normalize/drop the extra pending value during retake.
                            # The admin review state must keep all observed electric values.
                            conn.execute(
                                text(
                                    """
                                    INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value)
                                    VALUES(:aid,:ym,'electric',:idx,:val,'ocr',:ocr)
                                    ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET
                                        value=EXCLUDED.value,
                                        source=EXCLUDED.source,
                                        ocr_value=EXCLUDED.ocr_value,
                                        updated_at=now()
                                    """
                                ),
                                {
                                    "aid": int(apartment_id),
                                    "ym": str(ym),
                                    "idx": int(close_idx),
                                    "val": float(value_float),
                                    "ocr": float(value_float),
                                },
                            )
                            assigned_meter_index = int(close_idx)
                            electric_close_idx = int(close_idx)
                            electric_assignment_mode = "retake_overwrite_keep_extra_pending"
                            electric_retake_warning = int(close_idx)
                            electric_rows_after = _get_electric_month_snapshot(conn, int(apartment_id), str(ym))
                        else:
                            _write_electric_overwrite_then_sort(
                                conn,
                                int(apartment_id),
                                str(ym),
                                int(close_idx),
                                float(value_float),
                                source="ocr",
                            )
                            assigned_meter_index = int(close_idx)
                            electric_close_idx = int(close_idx)
                            electric_assignment_mode = "retake_overwrite"
                            electric_retake_warning = int(close_idx)
                            electric_rows_after = _get_electric_month_snapshot(conn, int(apartment_id), str(ym))

                if (not electric_assignment_done) and close_idx is None:
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
                            electric_assignment_mode = "explicit_write"
                            electric_rows_after = _get_electric_month_snapshot(conn, int(apartment_id), str(ym))
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
                        if bool(electric_extra_pending) and int(electric_expected or 0) < 3:
                            # When the month is already in "extra electric value needs admin review",
                            # never replace the extra slot with another distinct OCR value.
                            # Keep the new observation in photo_events and review trail only.
                            assigned_meter_index = max(1, min(3, int(electric_expected_snapshot or electric_expected or 2) + 1))
                            electric_assignment_mode = "extra_pending_unmapped_review"
                            electric_rows_after = list(electric_rows_before)
                            electric_write_blocked_reason = {
                                "reason": "extra_pending_unmapped",
                                "expected": int(electric_expected or 0),
                                "expected_snapshot": (
                                    int(electric_expected_snapshot) if electric_expected_snapshot is not None else None
                                ),
                                "incoming_value": float(value_float),
                                "assigned_meter_index": int(assigned_meter_index),
                                "rows_before": electric_rows_before,
                                "ydisk_path": ydisk_path,
                            }
                            water_write_blocked = True
                            diag["warnings"].append(
                                {"electric_extra_pending_unmapped_review": dict(electric_write_blocked_reason)}
                            )
                            try:
                                with engine.begin() as conn:
                                    exists = conn.execute(
                                        text(
                                            """
                                            SELECT 1
                                            FROM meter_review_flags
                                            WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric'
                                              AND meter_index=:mi AND status='open'
                                              AND reason='electric_extra_pending_unmapped'
                                            LIMIT 1
                                            """
                                        ),
                                        {
                                            "aid": int(apartment_id),
                                            "ym": str(ym),
                                            "mi": int(assigned_meter_index),
                                        },
                                    ).fetchone()
                                    if not exists:
                                        conn.execute(
                                            text(
                                                """
                                                INSERT INTO meter_review_flags(
                                                    apartment_id, ym, meter_type, meter_index, status, reason, comment, created_at, resolved_at
                                                )
                                                VALUES(:aid, :ym, 'electric', :mi, 'open', 'electric_extra_pending_unmapped', :comment, now(), NULL)
                                                """
                                            ),
                                            {
                                                "aid": int(apartment_id),
                                                "ym": str(ym),
                                                "mi": int(assigned_meter_index),
                                                "comment": json.dumps(electric_write_blocked_reason, ensure_ascii=False),
                                            },
                                        )
                                    username = (telegram_username or "").strip().lstrip("@").lower() or "Без username"
                                    related = json.dumps(
                                        {
                                            "ym": str(ym),
                                            "meter_type": "electric",
                                            "meter_index": int(assigned_meter_index),
                                            "ydisk_path": ydisk_path,
                                        },
                                        ensure_ascii=False,
                                    )
                                    msg = (
                                        "Лишнее электрическое показание требует сопоставления администратором. "
                                        f"Значение: {float(value_float)}. Файл: {ydisk_path}"
                                    )
                                    conn.execute(
                                        text(
                                            """
                                            INSERT INTO notifications(
                                                chat_id, telegram_username, apartment_id, type, message, related, status, created_at
                                            )
                                            VALUES(:chat_id, :username, :apartment_id, 'electric_extra_pending_unmapped', :message, CAST(:related AS JSONB), 'unread', now())
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
                            except Exception as e:
                                diag["warnings"].append({"electric_extra_pending_review_flag_failed": str(e)})
                        else:
                            assigned_meter_index = _assign_and_write_electric_sorted(
                                int(apartment_id),
                                ym,
                                float(value_float),
                            )
                            electric_assignment_mode = "auto_sorted_write"
                            with engine.begin() as conn:
                                electric_rows_after = _get_electric_month_snapshot(conn, int(apartment_id), str(ym))

                assigned_meter_index = _resolve_electric_assigned_index(
                    electric_rows_after,
                    (float(value_float) if value_float is not None else None),
                    assigned_meter_index,
                )
                if electric_retake_warning is not None:
                    warning_payload: dict[str, Any] = {
                        "meter_type": "electric",
                        "meter_index": int(electric_retake_warning),
                        "initial_meter_index": int(electric_retake_warning),
                    }
                    if assigned_meter_index is not None:
                        warning_payload["final_meter_index"] = int(assigned_meter_index)
                    diag["warnings"].append({"retake_overwrite": warning_payload})

                _set_electric_assignment_debug(
                    diag,
                    apartment_id=int(apartment_id),
                    ym=str(ym),
                    expected=(int(electric_expected) if electric_expected is not None else None),
                    extra_pending=electric_extra_pending,
                    expected_snapshot=electric_expected_snapshot,
                    incoming_value=(float(value_float) if value_float is not None else None),
                    mode=electric_assignment_mode,
                    close_idx=electric_close_idx,
                    tariff_index=ocr_tariff_index,
                    meter_index_mode=meter_index_mode,
                    requested_meter_index=(int(raw_meter_index) if raw_meter_index is not None else None),
                    assigned_meter_index=int(assigned_meter_index),
                    rows_before=electric_rows_before,
                    rows_after=electric_rows_after,
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
                                    _set_api_review_trace(
                                        diag,
                                        ym=str(ym),
                                        apartment_id=(int(apartment_id) if apartment_id is not None else None),
                                        raw_ocr_type=(str(ocr_type) if ocr_type is not None else None),
                                        raw_ocr_reading=ocr_reading,
                                        raw_ocr_serial=(str(ocr_serial) if ocr_serial is not None else None),
                                        resolved_meter_kind=(str(kind) if kind is not None else None),
                                        resolved_meter_label=_kind_to_label((str(kind) if kind is not None else None), assigned_meter_index),
                                        assigned_meter_index=int(assigned_meter_index),
                                        meter_written=False,
                                        event_status="needs_review",
                                        photo_event_id=(int(photo_event_id) if photo_event_id is not None else None),
                                        ydisk_path=ydisk_path,
                                        reason_override="serial_mismatch",
                                        serial_force_kind=(str(serial_force_kind) if serial_force_kind else None),
                                    )
                                    if db_ready() and photo_event_id:
                                        try:
                                            diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
                                            conn.execute(
                                                text(
                                                    """
                                                    UPDATE photo_events
                                                    SET diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                                                    WHERE id = :id
                                                    """
                                                ),
                                                {"id": int(photo_event_id), "diag_json": diag_json_str},
                                            )
                                        except Exception:
                                            pass
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

                        if force_kind in ("cold", "hot") and kind != force_kind:
                            diag["warnings"].append(
                                {
                                    "serial_type_override": {
                                        "raw_kind": kind,
                                        "serial_kind": str(force_kind),
                                        "policy": "serial_authoritative_write_path",
                                    }
                                }
                            )
                            kind = str(force_kind)

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
                                            candidate_kind = _ocr_to_kind(best_serial.get("type"))
                                            kind = str(force_kind) if force_kind in ("cold", "hot") else (candidate_kind or kind)
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
                                            keep_with_review = bool(
                                                (not _looks_like_serial_reading(value_float, serial_norm))
                                                and (float(value_float) > 0.0)
                                                and (float(value_float) <= 5000.0)
                                                and (cur_delta <= max(600.0, float(WATER_SERIAL_HARD_DELTA) * 8.0))
                                            )
                                            if keep_with_review:
                                                diag["warnings"].append(
                                                    {
                                                        "water_serial_prev_saved_with_review": {
                                                            "value": float(value_float),
                                                            "prev_ref": float(serial_prev_ref),
                                                            "meter_type": serial_prev_kind,
                                                            "delta": float(cur_delta),
                                                            "threshold": float(WATER_SERIAL_HARD_DELTA),
                                                            "reason": "serial_prev_outlier_saved_with_review",
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

    if unresolved_water_review_only and db_ready() and photo_event_id:
        try:
            _set_api_review_trace(
                diag,
                ym=str(ym),
                apartment_id=(int(apartment_id) if apartment_id is not None else None),
                raw_ocr_type=(str(ocr_type) if ocr_type is not None else None),
                raw_ocr_reading=ocr_reading,
                raw_ocr_serial=(str(ocr_serial) if ocr_serial is not None else None),
                resolved_meter_kind=None,
                resolved_meter_label=None,
                assigned_meter_index=int(assigned_meter_index),
                meter_written=False,
                event_status="needs_review",
                photo_event_id=int(photo_event_id),
                ydisk_path=ydisk_path,
                reason_override="water_type_unresolved",
                serial_force_kind=(str(serial_force_kind) if serial_force_kind else None),
            )
            diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE photo_events
                        SET
                            meter_written = false,
                            stage = 'needs_review',
                            stage_updated_at = now(),
                            meter_kind = NULL,
                            meter_value = :meter_value,
                            diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": int(photo_event_id),
                        "meter_value": (float(value_float) if value_float is not None else None),
                        "diag_json": diag_json_str,
                    },
                )
        except Exception as e:
            diag["warnings"].append({"water_type_unresolved_review_update_failed": str(e)})

    if electric_uncorroborated_review_only and db_ready() and photo_event_id:
        try:
            diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE photo_events
                        SET
                            meter_written = false,
                            stage = 'needs_review',
                            stage_updated_at = now(),
                            meter_kind = COALESCE(:meter_kind, meter_kind),
                            meter_value = :meter_value,
                            diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": int(photo_event_id),
                        "meter_kind": str(kind) if kind is not None else None,
                        "meter_value": (float(value_float) if value_float is not None else None),
                        "diag_json": diag_json_str,
                    },
                )
        except Exception as e:
            diag["warnings"].append({"electric_uncorroborated_review_update_failed": str(e)})

    if same_file_cross_month_review_only and db_ready() and photo_event_id:
        try:
            _set_api_review_trace(
                diag,
                ym=str(ym),
                apartment_id=(int(apartment_id) if apartment_id is not None else None),
                raw_ocr_type=(str(ocr_type) if ocr_type is not None else None),
                raw_ocr_reading=ocr_reading,
                raw_ocr_serial=(str(ocr_serial) if ocr_serial is not None else None),
                resolved_meter_kind=(str(kind) if kind is not None else None),
                resolved_meter_label=_kind_to_label((str(kind) if kind is not None else None), assigned_meter_index),
                assigned_meter_index=int(assigned_meter_index),
                meter_written=False,
                event_status="needs_review",
                photo_event_id=int(photo_event_id),
                ydisk_path=ydisk_path,
                reason_override="same_file_cross_month_reuse",
                serial_force_kind=(str(serial_force_kind) if serial_force_kind else None),
            )
            diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE photo_events
                        SET
                            meter_written = false,
                            stage = 'needs_review',
                            stage_updated_at = now(),
                            meter_kind = COALESCE(:meter_kind, meter_kind),
                            meter_value = :meter_value,
                            diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": int(photo_event_id),
                        "meter_kind": str(kind) if kind is not None else None,
                        "meter_value": (float(value_float) if value_float is not None else None),
                        "diag_json": diag_json_str,
                    },
                )
        except Exception as e:
            diag["warnings"].append({"same_file_cross_month_review_update_failed": str(e)})

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

    if isinstance(diag.get("ocr_water_decision"), dict):
        gate_keys = {
            "water_prev_sanity_blocked",
            "water_prev_sanity_saved_with_review",
            "water_prev_sanity_saved_with_review",
            "water_serial_prev_saved_with_review",
            "anomaly_jump",
            "anomaly_saved_with_review",
            "water_type_uncertain",
            "ocr_type_conflict",
            "serial_mismatch",
            "serial_type_override",
            "meter_type_unresolved",
        }
        gate_notes: list[dict | str] = []
        for warning in list(diag.get("warnings") or []):
            if isinstance(warning, dict):
                if any(key in warning for key in gate_keys):
                    gate_notes.append(warning)
            elif isinstance(warning, str) and ("review" in warning or "anomaly" in warning):
                gate_notes.append(warning)
        if gate_notes:
            diag["ocr_water_decision"]["review_gate"] = jsonable_encoder(gate_notes[:8])

    resolved_meter_kind = str(kind) if kind is not None else None
    resolved_meter_label = _kind_to_label(resolved_meter_kind, assigned_meter_index)
    if isinstance(ocr_data, dict):
        if resolved_meter_kind:
            ocr_data["resolved_kind"] = resolved_meter_kind
        if resolved_meter_label:
            ocr_data["resolved_type"] = resolved_meter_label
    final_review_reason = None
    if unresolved_water_review_only and not wrote_meter:
        final_review_reason = "water_type_unresolved"
    elif electric_uncorroborated_review_only and not wrote_meter:
        final_review_reason = "electric_uncorroborated_fullframe"
    elif same_file_cross_month_review_only and not wrote_meter:
        final_review_reason = "same_file_cross_month_reuse"

    _set_api_review_trace(
        diag,
        ym=str(ym),
        apartment_id=(int(apartment_id) if apartment_id is not None else None),
        raw_ocr_type=(str(ocr_type) if ocr_type is not None else None),
        raw_ocr_reading=ocr_reading,
        raw_ocr_serial=(str(ocr_serial) if ocr_serial is not None else None),
        resolved_meter_kind=resolved_meter_kind,
        resolved_meter_label=resolved_meter_label,
        assigned_meter_index=int(assigned_meter_index),
        meter_written=bool(wrote_meter),
        event_status=("meter_written" if wrote_meter else "needs_review"),
        photo_event_id=(int(photo_event_id) if photo_event_id is not None else None),
        ydisk_path=ydisk_path,
        reason_override=final_review_reason,
        serial_force_kind=(str(serial_force_kind) if serial_force_kind else None),
    )

    payload = {
        "trace_id": trace_id,
        "status": "ok",
        "chat_id": str(chat_id),
        "telegram_username": telegram_username,
        "phone": phone,
        "photo_event_id": photo_event_id,
        "ydisk_path": ydisk_path,
        "apartment_id": apartment_id,
        "event_status": (
            "needs_review"
            if ((unresolved_water_review_only or electric_uncorroborated_review_only or same_file_cross_month_review_only) and not wrote_meter)
            else status
        ),
        "ocr": ocr_data,
        "meter_kind": resolved_meter_kind,
        "meter_type_label": resolved_meter_label,
        "meter_written": wrote_meter,
        "ocr_failed": bool((value_float is None) or (not kind and not is_water_unknown)),
        "diag": diag,
        "assigned_meter_index": assigned_meter_index,
        "ym": ym,
        "bill": bill,
    }

    if db_ready() and photo_event_id:
        try:
            diag_json_str = json.dumps(diag, ensure_ascii=False) if diag is not None else None
            ocr_json_str = json.dumps(ocr_data, ensure_ascii=False) if isinstance(ocr_data, dict) else None
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE photo_events
                        SET
                            meter_kind = COALESCE(:meter_kind, meter_kind),
                            diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END,
                            ocr_json = CASE WHEN :ocr_json IS NULL THEN ocr_json ELSE CAST(:ocr_json AS JSONB) END
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": int(photo_event_id),
                        "meter_kind": resolved_meter_kind,
                        "diag_json": diag_json_str,
                        "ocr_json": ocr_json_str,
                    },
                )
        except Exception as e:
            diag["warnings"].append({"photo_event_resolved_update_failed": str(e)})

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
    return JSONResponse(status_code=200, content=jsonable_encoder(payload))
