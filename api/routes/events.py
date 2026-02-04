import json
import re
import hashlib
import requests

from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import JSONResponse
from sqlalchemy import text

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
from core.meters import _write_electric_explicit, _assign_and_write_electric_sorted
from core.admin_helpers import (
    find_apartment_by_chat,
    find_apartment_by_contact,
    bind_chat,
    _set_contact,
    _upsert_month_statuses,
    _ocr_to_kind,
    _parse_reading_to_float,
    update_apartment_statuses,
)
from core.schemas import UIStatusesPatch

router = APIRouter()


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
    if isinstance(ocr_data, dict):
        ocr_type = ocr_data.get("type")
        ocr_reading = ocr_data.get("reading")

    kind = _ocr_to_kind(ocr_type)
    value_float = _parse_reading_to_float(ocr_reading)

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

    if db_ready() and apartment_id and kind and (value_float is not None):
        try:
            # 6.1) write meter_readings and get assigned_meter_index
            if kind == "electric":
                # By default always auto-sort.
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
                    conn.execute(
                        text("""
                            INSERT INTO meter_readings
                                (apartment_id, ym, meter_type, meter_index, value, source, ocr_value)
                            VALUES
                                (:aid, :ym, :meter_type, 1, :value, 'ocr', :ocr_value)
                            ON CONFLICT (apartment_id, ym, meter_type, meter_index)
                            DO UPDATE SET
                                value = EXCLUDED.value,
                                source = 'ocr',
                                ocr_value = EXCLUDED.ocr_value,
                                updated_at = now()
                        """),
                        {
                            "aid": int(apartment_id),
                            "ym": ym,
                            "meter_type": str(kind),
                            "value": float(value_float),
                            "ocr_value": float(value_float),
                        },
                    )

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
                                    meter_kind = COALESCE(meter_kind, :meter_kind),
                                    meter_value = COALESCE(meter_value, :meter_value),
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
            "diag": diag,
            "assigned_meter_index": assigned_meter_index,
            "ym": ym,
            "bill": bill,
        },
    )
