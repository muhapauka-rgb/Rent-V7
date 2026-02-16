import json
import re
import hashlib
import requests
import threading

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


def _ocr_serial_endpoint() -> str:
    base = (OCR_URL or "").strip()
    if not base:
        return ""
    if base.endswith("/recognize"):
        return base[: -len("/recognize")] + "/recognize_serial"
    return base.rstrip("/") + "/recognize_serial"


def _async_fill_water_serial(
    *,
    apartment_id: int,
    meter_kind: str,
    image_bytes: bytes,
    chat_id: str,
    telegram_username: str | None,
    ym: str,
) -> None:
    """
    Ленивая фоновая подстановка серийника:
    - не блокирует ответ боту,
    - не трогает ручные серийники.
    """
    if meter_kind not in ("cold", "hot"):
        return
    url = _ocr_serial_endpoint()
    if not url:
        return
    blob = bytes(image_bytes or b"")
    if not blob:
        return

    def _run() -> None:
        try:
            resp = requests.post(
                url,
                files={"file": ("serial.jpg", blob)},
                timeout=(8, 120),
            )
            if not resp.ok:
                return
            js = resp.json() if resp.content else {}
            serial_norm = _normalize_serial(js.get("serial"))
            if not serial_norm:
                return

            col = "cold_serial" if meter_kind == "cold" else "hot_serial"
            col_src = "cold_serial_source" if meter_kind == "cold" else "hot_serial_source"
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
                if not row:
                    return
                existing = (row.get("serial") if row else None) or ""
                existing_norm = _normalize_serial(existing)
                src = (row.get("src") if row else None) or ""
                if str(src) == "manual":
                    return
                if existing_norm:
                    return
                conn.execute(
                    text(
                        f"""
                        UPDATE apartments
                        SET {col} = :serial,
                            {col_src} = CASE WHEN COALESCE({col_src}, '') = '' THEN 'auto' ELSE {col_src} END
                        WHERE id=:aid
                          AND COALESCE({col_src}, '') <> 'manual'
                          AND ({col} IS NULL OR {col} = '')
                        """
                    ),
                    {"aid": int(apartment_id), "serial": serial_norm},
                )
            logger.info(
                "lazy_serial_filled apartment_id=%s ym=%s meter_kind=%s serial=%s chat_id=%s username=%s",
                int(apartment_id),
                str(ym),
                str(meter_kind),
                serial_norm,
                str(chat_id),
                str(telegram_username or ""),
            )
        except Exception as e:
            logger.warning("lazy_serial_fill_failed apartment_id=%s meter_kind=%s err=%s", apartment_id, meter_kind, e)

    threading.Thread(
        target=_run,
        daemon=True,
        name=f"lazy-serial-{apartment_id}-{meter_kind}",
    ).start()


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


def _serial_tail_distance(a: str | None, b: str | None) -> int:
    sa = (a or "").strip()
    sb = (b or "").strip()
    if (not sa) or (not sb):
        return 99
    if len(sa) != len(sb):
        return 99
    try:
        return sum(1 for x, y in zip(sa, sb) if x != y)
    except Exception:
        return 99


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


def _whole_part(value: float | None) -> int | None:
    if value is None:
        return None
    try:
        v = float(value)
        if v < 0:
            return None
        return int(v)
    except Exception:
        return None


def _recover_water_fraction_value(
    conn,
    *,
    apartment_id: int,
    ym: str,
    meter_type: str,
    whole_part: int,
) -> float | None:
    mt = str(meter_type or "").strip().lower()
    if mt not in ("cold", "hot"):
        return None

    def _match_whole(v) -> float | None:
        try:
            fv = float(v)
        except Exception:
            return None
        if fv < 0:
            return None
        if int(fv) != int(whole_part):
            return None
        return float(fv)

    # 1) Последние исправленные админом значения (таблица обучения OCR) — приоритетно.
    rows = conn.execute(
        text(
            """
            SELECT correct_value
            FROM ocr_training_samples
            WHERE apartment_id=:aid AND meter_type=:mt AND correct_value IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 30
            """
        ),
        {"aid": int(apartment_id), "mt": mt},
    ).fetchall()
    for (v,) in (rows or []):
        mv = _match_whole(v)
        if mv is not None:
            return mv

    # 2) Текущий месяц — только ручная фиксация (OCR-значения здесь могут быть шумными).
    rows = conn.execute(
        text(
            """
            SELECT value, source
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=1
              AND value IS NOT NULL
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 5
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "mt": mt},
    ).fetchall()
    for v, src in (rows or []):
        if str(src or "") != "manual":
            continue
        mv = _match_whole(v)
        if mv is not None:
            return mv

    # 3) История показаний по этому типу
    rows = conn.execute(
        text(
            """
            SELECT value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym<:ym AND meter_type=:mt AND meter_index=1
              AND value IS NOT NULL
            ORDER BY ym DESC
            LIMIT 24
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "mt": mt},
    ).fetchall()
    for (v,) in (rows or []):
        mv = _match_whole(v)
        if mv is not None:
            return mv

    return None


def _recover_water_nearby_value(
    conn,
    *,
    apartment_id: int,
    ym: str,
    meter_type: str,
    current_value: float,
    max_delta: float = 20.0,
) -> float | None:
    mt = str(meter_type or "").strip().lower()
    if mt not in ("cold", "hot"):
        return None
    try:
        cur = float(current_value)
    except Exception:
        return None

    pool: list[tuple[int, float]] = []

    # 1) Исправленные админом значения (наиболее надежные).
    rows = conn.execute(
        text(
            """
            SELECT correct_value
            FROM ocr_training_samples
            WHERE apartment_id=:aid AND meter_type=:mt AND correct_value IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 60
            """
        ),
        {"aid": int(apartment_id), "mt": mt},
    ).fetchall()
    for (v,) in (rows or []):
        try:
            fv = float(v)
        except Exception:
            continue
        if fv < 0:
            continue
        pool.append((0, fv))

    # 2) Текущий месяц ручные значения (если уже подтверждали).
    rows = conn.execute(
        text(
            """
            SELECT value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=1
              AND source='manual' AND value IS NOT NULL
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 10
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "mt": mt},
    ).fetchall()
    for (v,) in (rows or []):
        try:
            fv = float(v)
        except Exception:
            continue
        if fv < 0:
            continue
        pool.append((1, fv))

    # 3) История показаний.
    rows = conn.execute(
        text(
            """
            SELECT value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym<:ym AND meter_type=:mt AND meter_index=1
              AND value IS NOT NULL
            ORDER BY ym DESC
            LIMIT 36
            """
        ),
        {"aid": int(apartment_id), "ym": str(ym), "mt": mt},
    ).fetchall()
    for (v,) in (rows or []):
        try:
            fv = float(v)
        except Exception:
            continue
        if fv < 0:
            continue
        pool.append((2, fv))

    if not pool:
        return None

    candidates = []
    for priority, fv in pool:
        d = abs(float(fv) - cur)
        if d <= float(max_delta):
            candidates.append((d, priority, fv))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]))
    return float(candidates[0][2])


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
    try:
        # OCR with Paddle can have slow cold-start on first request (model init/download).
        # Keep connect timeout short, but allow longer read timeout.
        ocr_resp = requests.post(OCR_URL, files={"file": ("file.bin", blob)}, timeout=(10, 300))
        if ocr_resp.ok:
            ocr_data = ocr_resp.json()
        else:
            diag["warnings"].append(f"ocr_http_{ocr_resp.status_code}")
    except Exception as e:
        diag["warnings"].append("ocr_unavailable")
        diag["warnings"].append({"ocr_error": str(e)})

    ocr_type = None
    ocr_reading = None
    ocr_confidence = None
    ocr_serial = None
    ocr_provider = None
    ocr_black_digits = None
    ocr_red_digits = None
    if isinstance(ocr_data, dict):
        ocr_type = ocr_data.get("type")
        ocr_reading = ocr_data.get("reading")
        ocr_confidence = ocr_data.get("confidence")
        ocr_serial = ocr_data.get("serial")
        ocr_provider = str(ocr_data.get("provider") or "").strip()
        ocr_black_digits = "".join(ch for ch in str(ocr_data.get("black_digits") or "") if ch.isdigit()) or None
        ocr_red_digits = "".join(ch for ch in str(ocr_data.get("red_digits") or "") if ch.isdigit()) or None

    kind = _ocr_to_kind(ocr_type)
    value_float = _parse_reading_to_float(ocr_reading)
    serial_norm = _normalize_serial(ocr_serial)
    try:
        ocr_conf = float(ocr_confidence) if ocr_confidence is not None else 0.0
    except Exception:
        ocr_conf = 0.0
    is_water_unknown = str(ocr_type or "").strip().lower() == "unknown"
    water_like_ocr = (ocr_provider in ("water_digit", "water_serial_anchor", "paddle_seq")) or bool(ocr_black_digits)
    water_fraction_missing = False

    # Water safety gate:
    # если дробная часть неполная, не обнуляем распознавание, а сохраняем целую часть
    # и помечаем как uncertain (дальше попробуем восстановить дробь по истории).
    if water_like_ocr and (value_float is not None):
        if (not ocr_red_digits) or (len(ocr_red_digits) != 3):
            water_fraction_missing = True
            diag["warnings"].append({"water_fraction_missing": {"black_digits": ocr_black_digits, "red_digits": ocr_red_digits}})
            try:
                if ocr_black_digits:
                    value_float = float(int(ocr_black_digits))
                else:
                    value_float = float(int(float(value_float)))
            except Exception:
                pass

    if kind != "electric":
        meter_index = 1

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

    if db_ready() and apartment_id and (value_float is not None) and (kind or is_water_unknown):
        try:
            # 6.0) anomaly check vs previous month (absolute thresholds)
            anomaly = False
            anomaly_reason = None
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
                with engine.begin() as conn:
                    is_water = (kind in ("cold", "hot")) or is_water_unknown
                    water_uncertain = False
                    pre_force_kind = None
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
                        # Предварительный роутинг по серийнику (до drop-guard),
                        # чтобы guard сравнивал с правильным счетчиком.
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
                                if s_last5 and cold_last5 and s_last5 == cold_last5:
                                    pre_force_kind = "cold"
                                elif s_last5 and hot_last5 and s_last5 == hot_last5:
                                    pre_force_kind = "hot"
                                elif s_last5 and (cold_last5 or hot_last5):
                                    dc = _serial_tail_distance(s_last5, cold_last5)
                                    dh = _serial_tail_distance(s_last5, hot_last5)
                                    fuzzy = []
                                    if cold_last5 and dc <= 1:
                                        fuzzy.append(("cold", dc))
                                    if hot_last5 and dh <= 1:
                                        fuzzy.append(("hot", dh))
                                    if len(fuzzy) == 1:
                                        pre_force_kind = str(fuzzy[0][0])
                        except Exception:
                            pre_force_kind = None
                        # Внутримесячный hard-guard: новые OCR-значения воды не должны
                        # заметно падать относительно уже записанных в этом же месяце.
                        # Иначе переводим событие в проверку и не перезаписываем данные.
                        try:
                            if pre_force_kind in ("cold", "hot") and pre_force_kind in prev_map:
                                cur_vals = [float(prev_map[pre_force_kind][0])]
                            else:
                                cur_vals = [float(vs[0]) for vs in prev_map.values() if vs and (vs[0] is not None)]
                            if cur_vals and (value_float is not None):
                                cur_value = float(value_float)
                                cur_whole = int(cur_value) if cur_value >= 0 else 0
                                ref_val = min(
                                    cur_vals,
                                    key=lambda v: (abs(int(float(v)) - cur_whole), abs(float(v) - cur_value)),
                                )
                                if cur_value + WATER_RETAKE_THRESHOLD < float(ref_val):
                                    diag["warnings"].append(
                                        {
                                            "water_same_month_drop_block": {
                                                "value": float(cur_value),
                                                "current_ref": float(ref_val),
                                                "threshold": float(WATER_RETAKE_THRESHOLD),
                                            }
                                        }
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
                                    if isinstance(ocr_data, dict):
                                        if pre_force_kind in ("cold", "hot"):
                                            ocr_data["effective_type"] = str(pre_force_kind)
                                        elif kind in ("cold", "hot", "electric"):
                                            ocr_data["effective_type"] = str(kind)
                                        if value_float is not None:
                                            try:
                                                ocr_data["effective_reading"] = float(value_float)
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

                                if s_last5 and cold_last5 and s_last5 == cold_last5:
                                    force_kind = "cold"
                                    force_no_sort = True
                                elif s_last5 and hot_last5 and s_last5 == hot_last5:
                                    force_kind = "hot"
                                    force_no_sort = True
                                elif s_last5 and (cold_last5 or hot_last5):
                                    # Мягкий роутинг: OCR серийника часто ошибается на 1 цифру.
                                    # Если близок только к одному из serial last5 — принимаем этот тип.
                                    dc = _serial_tail_distance(s_last5, cold_last5)
                                    dh = _serial_tail_distance(s_last5, hot_last5)
                                    fuzzy = []
                                    if cold_last5 and dc <= 1:
                                        fuzzy.append(("cold", dc, cold_last5))
                                    if hot_last5 and dh <= 1:
                                        fuzzy.append(("hot", dh, hot_last5))
                                    if len(fuzzy) == 1:
                                        force_kind = str(fuzzy[0][0])
                                        force_no_sort = True
                                        diag["warnings"].append(
                                            {
                                                "serial_fuzzy_match": {
                                                    "serial_last5": s_last5,
                                                    "matched_kind": str(fuzzy[0][0]),
                                                    "matched_last5": str(fuzzy[0][2]),
                                                    "distance": int(fuzzy[0][1]),
                                                }
                                            }
                                        )
                                    else:
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

                        # Для воды пытаемся восстановить дробную часть по подтвержденной истории:
                        # 1) если дробь не распознана (основной кейс),
                        # 2) если дробь распознана, но значение "откатывается" вниз в пределах той же целой части.
                        #    Это типично для неверного чтения красных барабанов.
                        if value_float is not None:
                            target_kind = None
                            if force_kind in ("cold", "hot"):
                                target_kind = str(force_kind)
                            elif kind in ("cold", "hot"):
                                target_kind = str(kind)

                            whole = _whole_part(value_float)
                            recovered = None
                            if target_kind and (whole is not None):
                                recovered = _recover_water_fraction_value(
                                    conn,
                                    apartment_id=int(apartment_id),
                                    ym=str(ym),
                                    meter_type=str(target_kind),
                                    whole_part=int(whole),
                                )
                            elif whole is not None:
                                # fallback: если тип пока неизвестен, пробуем оба; применяем только однозначный матч.
                                rec_cold = _recover_water_fraction_value(
                                    conn,
                                    apartment_id=int(apartment_id),
                                    ym=str(ym),
                                    meter_type="cold",
                                    whole_part=int(whole),
                                )
                                rec_hot = _recover_water_fraction_value(
                                    conn,
                                    apartment_id=int(apartment_id),
                                    ym=str(ym),
                                    meter_type="hot",
                                    whole_part=int(whole),
                                )
                                if (rec_cold is not None) and (rec_hot is None):
                                    recovered = rec_cold
                                    force_kind = "cold"
                                    force_no_sort = True
                                elif (rec_hot is not None) and (rec_cold is None):
                                    recovered = rec_hot
                                    force_kind = "hot"
                                    force_no_sort = True

                            # Если точного совпадения по целой части нет, пробуем "ближайшее"
                            # подтвержденное значение (обычно это исправленные админом данные).
                            if (recovered is None) and water_fraction_missing and (value_float is not None):
                                if target_kind in ("cold", "hot"):
                                    recovered = _recover_water_nearby_value(
                                        conn,
                                        apartment_id=int(apartment_id),
                                        ym=str(ym),
                                        meter_type=str(target_kind),
                                        current_value=float(value_float),
                                        max_delta=20.0,
                                    )
                                elif force_kind in ("cold", "hot"):
                                    recovered = _recover_water_nearby_value(
                                        conn,
                                        apartment_id=int(apartment_id),
                                        ym=str(ym),
                                        meter_type=str(force_kind),
                                        current_value=float(value_float),
                                        max_delta=20.0,
                                    )

                            if recovered is not None:
                                try:
                                    prev_val = float(value_float)
                                except Exception:
                                    prev_val = None
                                apply_recovered = False
                                recover_mode = None
                                if water_fraction_missing:
                                    apply_recovered = True
                                    recover_mode = "missing_fraction"
                                else:
                                    # Если целая часть совпала, но OCR-значение ниже исторически подтвержденного:
                                    # считаем это ошибкой дробной части и поднимаем до recovered.
                                    # Ограничиваем разницу 1.0 (только внутри одного целого шага).
                                    try:
                                        if (
                                            (prev_val is not None)
                                            and (float(prev_val) < float(recovered))
                                            and (float(recovered) - float(prev_val) <= 0.9995)
                                            and (int(float(prev_val)) == int(float(recovered)))
                                        ):
                                            apply_recovered = True
                                            recover_mode = "fraction_regression"
                                    except Exception:
                                        apply_recovered = False

                                if apply_recovered:
                                    value_float = float(recovered)
                                    diag["warnings"].append(
                                        {
                                            "water_fraction_recovered": {
                                                "from": prev_val,
                                                "to": float(recovered),
                                                "meter_type": str(force_kind or kind or "unknown"),
                                                "mode": str(recover_mode or "unknown"),
                                            }
                                        }
                                    )
                            elif water_fraction_missing:
                                diag["warnings"].append(
                                    {
                                        "water_fraction_unrecovered": {
                                            "meter_type": str(force_kind or kind or "unknown"),
                                            "black_digits": ocr_black_digits,
                                        }
                                    }
                                )

                        # Для воды фиксируем точность до 2 знаков после запятой:
                        # это убирает шум третьего знака на аналоговых барабанах.
                        if value_float is not None:
                            try:
                                before_round = float(value_float)
                                value_float = round(before_round, 2)
                                if abs(before_round - float(value_float)) >= 0.001:
                                    diag["warnings"].append(
                                        {
                                            "water_rounded_2dp": {
                                                "from": before_round,
                                                "to": float(value_float),
                                            }
                                        }
                                    )
                            except Exception:
                                pass

                        # если OCR не уверен в типе, сортируем как max->ХВС, min->ГВС
                        water_uncertain = (
                            bool(water_fraction_missing)
                            or is_water_unknown
                            or (kind in ("cold", "hot") and ocr_conf < WATER_TYPE_CONF_MIN)
                        )
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

                        # Для воды запрещаем "откат" показаний вниз в рамках месяца
                        # для уже определенного типа счетчика.
                        guard_kind = None
                        if force_kind in ("cold", "hot"):
                            guard_kind = str(force_kind)
                        elif kind in ("cold", "hot"):
                            guard_kind = str(kind)
                        if guard_kind and (guard_kind in prev_map) and (value_float is not None):
                            try:
                                prev_same = float(prev_map[guard_kind][0])
                                if float(value_float) < prev_same:
                                    diag["warnings"].append(
                                        {
                                            "water_non_decreasing_clamp": {
                                                "meter_type": str(guard_kind),
                                                "from": float(value_float),
                                                "to": float(prev_same),
                                            }
                                        }
                                    )
                                    value_float = float(prev_same)
                            except Exception:
                                pass

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
                    row = conn.execute(
                        text("""
                            SELECT meter_type, meter_index, value
                            FROM meter_readings
                            WHERE apartment_id=:aid
                              AND ym=:ym
                              AND source IN ('ocr','manual')
                              AND abs(value - :val) <= :tol
                              AND NOT (meter_type=:mt AND meter_index=:mi)
                            ORDER BY meter_type ASC, meter_index ASC
                            LIMIT 1
                        """),
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

            wrote_meter = True

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

            # 6.36) lazy serial fill for water (background, no impact on bot latency)
            try:
                if (not serial_norm) and wrote_meter and kind in ("cold", "hot"):
                    _async_fill_water_serial(
                        apartment_id=int(apartment_id),
                        meter_kind=str(kind),
                        image_bytes=blob,
                        chat_id=str(chat_id),
                        telegram_username=telegram_username,
                        ym=str(ym),
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
                                    meter_written = true,
                                    meter_index = :meter_index,
                                    meter_kind = COALESCE(:meter_kind, meter_kind),
                                    ocr_reading = COALESCE(:ocr_reading, ocr_reading),
                                    meter_value = COALESCE(:meter_value, meter_value),
                                    stage = 'meter_written',
                                    stage_updated_at = now(),
                                    diag_json = CASE WHEN :diag_json IS NULL THEN diag_json ELSE CAST(:diag_json AS JSONB) END
                                WHERE id = :id
                            """),
                            {
                                "id": int(photo_event_id),
                                "meter_index": int(assigned_meter_index),
                                "meter_kind": str(kind),
                                "ocr_reading": float(value_float),
                                "meter_value": float(value_float),
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

    if isinstance(ocr_data, dict):
        if value_float is not None:
            try:
                ocr_data["effective_reading"] = float(value_float)
            except Exception:
                pass
            if water_like_ocr and water_fraction_missing:
                # Для бота/UI показываем фактическое сохраненное значение,
                # даже если исходный OCR дал неполную дробную часть.
                try:
                    ocr_data["reading"] = float(value_float)
                except Exception:
                    pass
        if water_fraction_missing:
            ocr_data["fraction_missing"] = True
        # Для бота/UI возвращаем итоговый тип после serial-routing/assign (если определен).
        if kind in ("cold", "hot", "electric"):
            ocr_data["effective_type"] = str(kind)

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
