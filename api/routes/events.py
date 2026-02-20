import json
import re
import hashlib
import requests
import math
import os
import time

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
ENABLE_AGGRESSIVE_OCR_AUTOFIX = os.getenv("ENABLE_AGGRESSIVE_OCR_AUTOFIX", "0").strip().lower() in ("1", "true", "yes", "on")
OCR_HTTP_TIMEOUT_SEC = float(os.getenv("OCR_HTTP_TIMEOUT_SEC", "75"))
OCR_HTTP_RETRIES = int(os.getenv("OCR_HTTP_RETRIES", "1"))
WATER_INTEGER_ONLY = os.getenv("WATER_INTEGER_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")


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


def _call_ocr_with_retries(blob: bytes, *, filename: str | None = None, mime_type: str | None = None):
    last_exc = None
    upload_file = _as_image_upload_tuple(blob, filename, mime_type)
    for attempt in range(max(1, OCR_HTTP_RETRIES)):
        try:
            resp = requests.post(
                OCR_URL,
                files={"file": upload_file},
                timeout=(5, OCR_HTTP_TIMEOUT_SEC),
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
) -> dict | None:
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
        if WATER_INTEGER_ONLY:
            try:
                v = float(int(b))
            except Exception:
                continue
        else:
            if not r or len(r) < 2:
                continue
            try:
                v = float(f"{int(b)}.{r[:3]}")
            except Exception:
                continue
        # защита от "нулей" и слишком маленьких чисел из ложного OCR-окна
        if float(v) <= 0:
            continue
        if _black_digits_look_like_serial(b, serial_norm):
            continue
        if _looks_like_serial_reading(float(v), serial_norm):
            continue
        c = dict(c)
        c["reading"] = float(v)
        c["black_digits"] = b
        c["red_digits"] = (r[:3] if r else None)
        valid.append(c)
    if not valid:
        return None

    if prev_value is not None:
        pv = float(prev_value)
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
    chat_id = form.get("chat_id") or "unknown"
    telegram_username = form.get("telegram_username") or None
    phone = form.get("phone") or None

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

    if file is None:
        return JSONResponse(status_code=200, content={"status": "accepted", "error": "no_file", "chat_id": str(chat_id)})

    blob = await file.read()
    file_sha256 = hashlib.sha256(blob).hexdigest()

    if db_ready():
        try:
            ensure_tables()
        except Exception as e:
            diag["errors"].append({"db_ensure_tables_error": str(e)})

    # 1) OCR
    ocr_data = None
    ocr_resp, ocr_exc = _call_ocr_with_retries(
        blob,
        filename=file.filename,
        mime_type=file.content_type,
    )
    if ocr_resp is not None:
        if ocr_resp.ok:
            ocr_data = ocr_resp.json()
        else:
            diag["warnings"].append(f"ocr_http_{ocr_resp.status_code}")
    else:
        diag["warnings"].append("ocr_unavailable")
        if ocr_exc is not None:
            diag["warnings"].append({"ocr_error": str(ocr_exc)})

    ocr_type = None
    ocr_reading = None
    ocr_confidence = None
    ocr_serial = None
    if isinstance(ocr_data, dict):
        ocr_type = ocr_data.get("type")
        ocr_reading = ocr_data.get("reading")
        ocr_confidence = ocr_data.get("confidence")
        ocr_serial = ocr_data.get("serial")

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
    apartment_id = None
    if db_ready():
        try:
            apartment_id = find_apartment_by_chat(str(chat_id))
        except Exception as e:
            diag["errors"].append({"chat_binding_lookup_error": str(e)})

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
                original_filename=file.filename,
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
                        "orig": file.filename,
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
            "meter_written": wrote_meter,
            "ocr_failed": bool((value_float is None) or (not kind and not is_water_unknown)),
            "diag": diag,
            "assigned_meter_index": assigned_meter_index,
            "ym": ym,
            "bill": bill,
        },
    )
