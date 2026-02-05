import json
import re
import hashlib
import requests

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
        ocr_resp = requests.post(OCR_URL, files={"file": ("file.bin", blob)}, timeout=15)
        if ocr_resp.ok:
            ocr_data = ocr_resp.json()
        else:
            diag["warnings"].append(f"ocr_http_{ocr_resp.status_code}")
    except Exception:
        diag["warnings"].append("ocr_unavailable")

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

                if anomaly:
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

            # 6.1) write meter_readings and get assigned_meter_index
            if kind == "electric":
                # By default always auto-sort.
                # First: if value is very close to an existing one, overwrite that slot.
                close_idx = None
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
                            assigned_meter_index = _write_electric_explicit(
                                conn,
                                int(apartment_id),
                                ym,
                                int(meter_index),
                                float(value_float),
                            )
                    else:
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
                    if is_water:
                        # если OCR не уверен в типе, сортируем как max->ХВС, min->ГВС
                        water_uncertain = is_water_unknown or (kind in ("cold", "hot") and ocr_conf < WATER_TYPE_CONF_MIN)
                        if water_uncertain:
                            diag["warnings"].append({"water_type_uncertain": {"confidence": ocr_conf, "ocr_type": ocr_type}})
                        force_sort = _has_open_water_uncertain_flag(conn, int(apartment_id), str(ym))
                        # if new value is very close to an existing water reading, overwrite that specific meter
                        force_kind = None
                        force_no_sort = False
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
                            force_kind = best[1]
                            force_no_sort = True
                            diag["warnings"].append({"retake_overwrite": {"meter_type": str(force_kind), "meter_index": 1}})

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
                                    meter_written = true,
                                    meter_index = :meter_index,
                                    meter_kind = COALESCE(:meter_kind, meter_kind),
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
