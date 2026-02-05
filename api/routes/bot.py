import json
from typing import Optional
from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from core.config import engine, logger
from core.db import db_ready, ensure_tables
from core.admin_helpers import find_apartment_by_chat, find_apartment_by_contact, bind_chat, _set_contact, current_ym
from core.billing import (
    is_ym,
    month_now,
    _calc_month_bill,
    find_apartment_for_chat,
)
from core.meters import _add_meter_reading_db, _write_electric_overwrite_then_sort
from core.learning import capture_training_sample
from core.schemas import BotContactIn, BotManualReadingIn, BotDuplicateResolveIn, BotWrongReadingReportIn, BotNotificationIn

router = APIRouter()


@router.post("/bot/contact")
def bot_contact(payload: BotContactIn):
    chat_id = str(payload.chat_id or "").strip()
    telegram_username = (payload.telegram_username or "").strip()
    phone = (payload.phone or "").strip()

    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id_required")
    if not telegram_username and not phone:
        raise HTTPException(status_code=400, detail="contact_required")

    # 1) если уже привязано — просто допишем/обновим контакты
    apartment_id = find_apartment_by_chat(chat_id)

    # 2) иначе пробуем найти по контактам и привязать
    if not apartment_id:
        apartment_id = find_apartment_by_contact(telegram_username, phone)
        if apartment_id:
            bind_chat(chat_id, int(apartment_id))

    if not apartment_id:
        return {"ok": False, "apartment_id": None}

    # 3) сохраняем контакты в квартиру
    if telegram_username:
        _set_contact(int(apartment_id), "telegram", telegram_username)
    if phone:
        _set_contact(int(apartment_id), "phone", phone)

    return {"ok": True, "apartment_id": int(apartment_id)}


@router.get("/bot/chats/{chat_id}/bill")
def bot_chat_bill(chat_id: str, ym: Optional[str] = None):
    """Используется ботом для проверки “что ещё нужно” и/или выдачи суммы после всех фото."""
    chat_id = str(chat_id).strip()
    ym = (ym or "").strip() or current_ym()
    if not is_ym(ym):
        raise HTTPException(status_code=400, detail="ym must be YYYY-MM")

    with engine.begin() as conn:
        apt = find_apartment_for_chat(conn, chat_id)
        if not apt:
            return {"ok": False, "reason": "not_bound", "ym": ym}

        bill = _calc_month_bill(conn, int(apt["id"]), ym)
        return {"ok": True, "apartment_id": int(apt["id"]), "ym": ym, "bill": bill}


@router.post("/bot/manual-reading")
def bot_manual_reading(payload: BotManualReadingIn):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    chat_id = str(payload.chat_id or "").strip()
    ym = (payload.ym or "").strip()
    meter_type = str(payload.meter_type or "").strip()
    meter_index = int(payload.meter_index or 1)
    value = payload.value

    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id_required")
    if not is_ym(ym):
        raise HTTPException(status_code=400, detail="ym_invalid")
    if meter_type not in ("cold", "hot", "electric", "sewer"):
        raise HTTPException(status_code=400, detail="meter_type_invalid")
    if meter_type == "electric" and meter_index not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="meter_index_invalid")

    # find apartment
    apartment_id = find_apartment_by_chat(chat_id)
    if not apartment_id:
        return {"ok": False, "apartment_id": None}

    if meter_type == "electric":
        with engine.begin() as conn:
            _write_electric_overwrite_then_sort(conn, int(apartment_id), str(ym), int(meter_index), float(value), source="manual")
    else:
        _add_meter_reading_db(
            apartment_id=apartment_id,
            ym=ym,
            meter_type=meter_type,
            meter_index=int(meter_index),
            value=float(value),
            source="manual",
        )
    try:
        with engine.begin() as conn:
            capture_training_sample(
                conn,
                apartment_id=int(apartment_id),
                ym=str(ym),
                meter_type=str(meter_type),
                meter_index=int(meter_index),
                correct_value=float(value),
                source="bot_manual",
            )
    except Exception:
        pass

    # return updated bill
    bill = None
    try:
        with engine.begin() as conn:
            bill = _calc_month_bill(conn, int(apartment_id), ym)
    except Exception:
        bill = None

    return {"ok": True, "apartment_id": int(apartment_id), "ym": ym, "bill": bill}


@router.post("/bot/duplicate/resolve")
def bot_duplicate_resolve(payload: BotDuplicateResolveIn):
    """Resolve a possible duplicate warning coming from /events/photo."""
    if not db_ready():
        raise HTTPException(status_code=503, detail="DB not ready")
    ensure_tables()

    action = str(payload.action or "").strip().lower()
    if action not in ("ok", "repeat"):
        raise HTTPException(status_code=400, detail="Invalid action")

    peid = int(payload.photo_event_id)

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT id, chat_id, apartment_id, ym, meter_kind, meter_index
                FROM photo_events
                WHERE id=:id
                LIMIT 1
            """),
            {"id": peid},
        ).fetchone()

        if not row:
            return {"ok": False, "reason": "photo_event_not_found", "bill": None}

        apartment_id = row[2]
        ym = (row[3] or month_now())
        meter_kind = (row[4] or "")
        meter_index = int(row[5] or 1)

        if apartment_id is None:
            return {"ok": False, "reason": "photo_event_not_assigned", "bill": None}

        apartment_id = int(apartment_id)
        ym = str(ym).strip()
        meter_kind = str(meter_kind).strip()

        if not meter_kind:
            return {"ok": False, "reason": "no_meter_kind", "bill": None}

        if action == "repeat":
            # meter_readings.value NOT NULL -> NULL ставить нельзя, поэтому удаляем запись
            conn.execute(
                text("""
                    DELETE FROM meter_readings
                    WHERE apartment_id=:aid AND ym=:ym AND meter_type=:t AND meter_index=:i
                """),
                {"aid": apartment_id, "ym": ym, "t": meter_kind, "i": int(meter_index)},
            )
            conn.execute(
                text("""
                    UPDATE photo_events
                    SET status='dup_repeat', stage='dup_repeat', stage_updated_at=NOW()
                    WHERE id=:id
                """),
                {"id": peid},
            )
        else:
            conn.execute(
                text("""
                    UPDATE photo_events
                    SET status='dup_ok', stage='dup_ok', stage_updated_at=NOW()
                    WHERE id=:id
                """),
                {"id": peid},
            )

    bill = _calc_month_bill(conn, apartment_id, ym)
    return {"ok": True, "bill": bill}


@router.post("/bot/notify")
def bot_notify(payload: BotNotificationIn):
    if not db_ready():
        raise HTTPException(status_code=503, detail="DB not ready")
    ensure_tables()

    chat_id = str(payload.chat_id or "").strip()
    message = (payload.message or "").strip()
    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id_required")
    if not message:
        raise HTTPException(status_code=400, detail="message_required")

    username = (payload.telegram_username or "").strip().lstrip("@").lower()
    if not username:
        username = "Без username"

    ntype = str(payload.type or "user_message").strip() or "user_message"
    related = payload.related or None

    with engine.begin() as conn:
        apt = find_apartment_for_chat(conn, chat_id)
        apartment_id = int(apt["id"]) if apt else None
        conn.execute(
            text(
                """
                INSERT INTO notifications(
                    chat_id, telegram_username, apartment_id, type, message, related, status, created_at
                )
                VALUES(
                    :chat_id, :username, :apartment_id, :type, :message,
                    CASE WHEN :related IS NULL THEN NULL ELSE CAST(:related AS JSONB) END,
                    'unread', now()
                )
                """
            ),
            {
                "chat_id": str(chat_id),
                "username": username,
                "apartment_id": apartment_id,
                "type": ntype,
                "message": message,
                "related": (json.dumps(related, ensure_ascii=False) if related is not None else None),
            },
        )

    return {"ok": True, "apartment_id": apartment_id}


@router.post("/bot/report-wrong-reading")
def bot_report_wrong_reading(payload: BotWrongReadingReportIn):
    if not db_ready():
        raise HTTPException(status_code=503, detail="DB not ready")
    ensure_tables()

    chat_id = str(payload.chat_id or "").strip()
    ym = str(payload.ym or "").strip()
    meter_type = str(payload.meter_type or "").strip()
    meter_index = int(payload.meter_index or 1)
    comment = (payload.comment or "").strip() or None

    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id_required")
    if not is_ym(ym):
        raise HTTPException(status_code=400, detail="ym_invalid")
    if meter_type not in ("cold", "hot", "electric", "sewer"):
        raise HTTPException(status_code=400, detail="meter_type_invalid")
    if meter_type != "electric":
        meter_index = 1
    elif meter_index not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="meter_index_invalid")

    apartment_id = find_apartment_by_chat(chat_id)
    if not apartment_id:
        return {"ok": False, "reason": "not_bound"}

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO meter_review_flags(
                    apartment_id, ym, meter_type, meter_index, status, reason, comment, created_at, resolved_at
                )
                VALUES(:aid, :ym, :mt, :mi, 'open', 'user_report_wrong_ocr', :comment, now(), NULL)
                """
            ),
            {
                "aid": int(apartment_id),
                "ym": ym,
                "mt": meter_type,
                "mi": int(meter_index),
                "comment": comment,
            },
        )

    return {
        "ok": True,
        "apartment_id": int(apartment_id),
        "ym": ym,
        "meter_type": meter_type,
        "meter_index": int(meter_index),
    }


# -----------------------
# Bot callbacks: paid flags
# -----------------------

@router.post("/bot/apartments/{apartment_id}/months/{ym}/rent-paid")
def bot_mark_rent_paid(apartment_id: int, ym: str):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO apartment_month_statuses (apartment_id, ym, rent_paid, updated_at, created_at)
                VALUES (:aid, :ym, true, now(), now())
                ON CONFLICT (apartment_id, ym)
                DO UPDATE SET rent_paid=true, updated_at=now()
            """),
            {"aid": int(apartment_id), "ym": ym},
        )
        conn.execute(
            text("""
                INSERT INTO apartment_statuses (apartment_id, rent_paid, updated_at)
                VALUES (:aid, true, now())
                ON CONFLICT (apartment_id)
                DO UPDATE SET rent_paid=true, updated_at=now()
            """),
            {"aid": int(apartment_id)},
        )
    return {"ok": True}


@router.post("/bot/apartments/{apartment_id}/months/{ym}/meters-paid")
def bot_mark_meters_paid(apartment_id: int, ym: str):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO apartment_month_statuses (apartment_id, ym, meters_paid, updated_at, created_at)
                VALUES (:aid, :ym, true, now(), now())
                ON CONFLICT (apartment_id, ym)
                DO UPDATE SET meters_paid=true, updated_at=now()
            """),
            {"aid": int(apartment_id), "ym": ym},
        )
        conn.execute(
            text("""
                INSERT INTO apartment_statuses (apartment_id, meters_paid, updated_at)
                VALUES (:aid, true, now())
                ON CONFLICT (apartment_id)
                DO UPDATE SET meters_paid=true, updated_at=now()
            """),
            {"aid": int(apartment_id)},
        )
    return {"ok": True}


@router.post("/bot/apartments/{apartment_id}/months/{ym}/rent-paid/toggle")
def bot_toggle_rent_paid(apartment_id: int, ym: str):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()
    with engine.begin() as conn:
        cur = conn.execute(
            text("SELECT COALESCE(rent_paid, false) FROM apartment_month_statuses WHERE apartment_id=:aid AND ym=:ym"),
            {"aid": int(apartment_id), "ym": str(ym)},
        ).scalar()
        new_val = not bool(cur)
        conn.execute(
            text("""
                INSERT INTO apartment_month_statuses (apartment_id, ym, rent_paid, updated_at, created_at)
                VALUES (:aid, :ym, :v, now(), now())
                ON CONFLICT (apartment_id, ym)
                DO UPDATE SET rent_paid=:v, updated_at=now()
            """),
            {"aid": int(apartment_id), "ym": str(ym), "v": bool(new_val)},
        )
        conn.execute(
            text("""
                INSERT INTO apartment_statuses (apartment_id, rent_paid, updated_at)
                VALUES (:aid, :v, now())
                ON CONFLICT (apartment_id)
                DO UPDATE SET rent_paid=:v, updated_at=now()
            """),
            {"aid": int(apartment_id), "v": bool(new_val)},
        )
    return {"ok": True, "value": bool(new_val)}


@router.post("/bot/apartments/{apartment_id}/months/{ym}/meters-paid/toggle")
def bot_toggle_meters_paid(apartment_id: int, ym: str):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()
    with engine.begin() as conn:
        cur = conn.execute(
            text("SELECT COALESCE(meters_paid, false) FROM apartment_month_statuses WHERE apartment_id=:aid AND ym=:ym"),
            {"aid": int(apartment_id), "ym": str(ym)},
        ).scalar()
        new_val = not bool(cur)
        conn.execute(
            text("""
                INSERT INTO apartment_month_statuses (apartment_id, ym, meters_paid, updated_at, created_at)
                VALUES (:aid, :ym, :v, now(), now())
                ON CONFLICT (apartment_id, ym)
                DO UPDATE SET meters_paid=:v, updated_at=now()
            """),
            {"aid": int(apartment_id), "ym": str(ym), "v": bool(new_val)},
        )
        conn.execute(
            text("""
                INSERT INTO apartment_statuses (apartment_id, meters_paid, updated_at)
                VALUES (:aid, :v, now())
                ON CONFLICT (apartment_id)
                DO UPDATE SET meters_paid=:v, updated_at=now()
            """),
            {"aid": int(apartment_id), "v": bool(new_val)},
        )
    return {"ok": True, "value": bool(new_val)}
