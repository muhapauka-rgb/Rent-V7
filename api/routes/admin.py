from typing import Dict, Any
from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from core.config import engine
from core.db import db_ready, ensure_tables
from core.admin_helpers import norm_phone, bind_chat, current_ym

router = APIRouter()


# -----------------------
# Admin: apartments
# -----------------------

@router.post("/admin/apartments")
def create_apartment(title: str, address: str | None = None, note: str | None = None):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        apt_id = conn.execute(
            text("""
                INSERT INTO apartments (title, address, note)
                VALUES (:title, :address, :note)
                RETURNING id
            """),
            {"title": title, "address": address, "note": note},
        ).scalar_one()

    return {"ok": True, "apartment_id": int(apt_id)}


@router.get("/admin/apartments")
def list_apartments(limit: int = 50, offset: int = 0):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    a.id,
                    a.title,
                    a.address,
                    a.tenant_name,
                    a.note,
                    a.created_at,
                    COALESCE(c.contacts_count, 0) AS contacts_count,
                    COALESCE(b.chats_count, 0) AS active_chats_count
                FROM apartments a
                LEFT JOIN (
                    SELECT apartment_id, COUNT(*) AS contacts_count
                    FROM apartment_contacts
                    WHERE is_active=true
                    GROUP BY apartment_id
                ) c ON c.apartment_id = a.id
                LEFT JOIN (
                    SELECT apartment_id, COUNT(*) AS chats_count
                    FROM chat_bindings
                    WHERE is_active=true
                    GROUP BY apartment_id
                ) b ON b.apartment_id = a.id
                ORDER BY a.id DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset},
        ).mappings().all()

    return {"ok": True, "items": list(rows), "limit": limit, "offset": offset}


@router.get("/admin/apartments/{apartment_id}")
def get_apartment(apartment_id: int, events_limit: int = 20):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    events_limit = max(1, min(events_limit, 200))

    with engine.begin() as conn:
        apt = conn.execute(
            text("""
                SELECT id, title, address, tenant_name, note, created_at
                FROM apartments
                WHERE id=:id
            """),
            {"id": apartment_id},
        ).mappings().fetchone()

        if not apt:
            raise HTTPException(status_code=404, detail="apartment not found")

        contacts = conn.execute(
            text("""
                SELECT id, apartment_id, kind, value, is_active, created_at
                FROM apartment_contacts
                WHERE apartment_id=:id
                ORDER BY is_active DESC, id DESC
            """),
            {"id": apartment_id},
        ).mappings().all()

        chats = conn.execute(
            text("""
                SELECT chat_id, apartment_id, is_active, updated_at, created_at
                FROM chat_bindings
                WHERE apartment_id=:id
                ORDER BY is_active DESC, updated_at DESC
            """),
            {"id": apartment_id},
        ).mappings().all()

        events = conn.execute(
            text("""
                SELECT id, chat_id, telegram_username, phone, status, apartment_id, ydisk_path, created_at
                FROM photo_events
                WHERE apartment_id=:id
                ORDER BY id DESC
                LIMIT :lim
            """),
            {"id": apartment_id, "lim": events_limit},
        ).mappings().all()

    return {
        "ok": True,
        "apartment": dict(apt),
        "contacts": list(contacts),
        "chats": list(chats),
        "photo_events": list(events),
    }


# -----------------------
# Admin: contacts
# -----------------------

@router.post("/admin/apartments/{apartment_id}/contacts")
def add_contact(apartment_id: int, kind: str, value: str):
    if kind not in ("telegram", "phone"):
        raise HTTPException(status_code=400, detail="kind must be telegram or phone")
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")

    ensure_tables()
    v = value.strip()
    if kind == "telegram":
        v = v.lstrip("@").lower()
    else:
        v = norm_phone(v)

    with engine.begin() as conn:
        a = conn.execute(text("SELECT id FROM apartments WHERE id=:id"), {"id": apartment_id}).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="apartment not found")

        row = conn.execute(
            text("""
                INSERT INTO apartment_contacts (apartment_id, kind, value, is_active)
                VALUES (:apartment_id, :kind, :value, true)
                ON CONFLICT (kind, value)
                DO UPDATE SET apartment_id=EXCLUDED.apartment_id, is_active=true
                RETURNING id, apartment_id, kind, value, is_active, created_at
            """),
            {"apartment_id": apartment_id, "kind": kind, "value": v},
        ).mappings().one()

    return {"ok": True, "contact": dict(row)}


@router.post("/admin/contacts/{contact_id}/deactivate")
def deactivate_contact(contact_id: int):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                UPDATE apartment_contacts
                SET is_active=false
                WHERE id=:id
                RETURNING id, apartment_id, kind, value, is_active, created_at
            """),
            {"id": contact_id},
        ).mappings().fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="contact not found")

    return {"ok": True, "contact": dict(row)}


@router.post("/admin/contacts/{contact_id}/activate")
def activate_contact(contact_id: int):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                UPDATE apartment_contacts
                SET is_active=true
                WHERE id=:id
                RETURNING id, apartment_id, kind, value, is_active, created_at
            """),
            {"id": contact_id},
        ).mappings().fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="contact not found")

    return {"ok": True, "contact": dict(row)}


# -----------------------
# Admin: chat bindings
# -----------------------

@router.get("/admin/chats/{chat_id}")
def get_chat_binding(chat_id: str):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT chat_id, apartment_id, is_active, updated_at, created_at
                FROM chat_bindings
                WHERE chat_id=:chat_id
                LIMIT 1
            """),
            {"chat_id": str(chat_id)},
        ).mappings().fetchone()

    return {"ok": True, "item": dict(row) if row else None}


@router.post("/admin/chats/{chat_id}/bind")
def bind_chat_admin(chat_id: str, apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        a = conn.execute(text("SELECT id FROM apartments WHERE id=:id"), {"id": apartment_id}).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="apartment not found")

    bind_chat(chat_id, apartment_id)
    return {"ok": True, "chat_id": str(chat_id), "apartment_id": int(apartment_id)}


@router.post("/admin/chats/{chat_id}/unbind")
def unbind_chat_admin(chat_id: str):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        apartment_ids = [
            int(r[0])
            for r in conn.execute(
                text("SELECT apartment_id FROM chat_bindings WHERE chat_id=:chat_id AND is_active=true"),
                {"chat_id": str(chat_id)},
            ).fetchall()
        ]
        conn.execute(
            text("""
                UPDATE chat_bindings
                SET is_active=false, updated_at=now()
                WHERE chat_id=:chat_id
            """),
            {"chat_id": str(chat_id)},
        )
        for aid in apartment_ids:
            conn.execute(
                text("UPDATE apartments SET rent_monthly=0 WHERE id=:id"),
                {"id": int(aid)},
            )
            conn.execute(
                text("""
                    INSERT INTO apartment_tariffs (apartment_id, month_from, rent, updated_at)
                    VALUES (:aid, :ym, 0, now())
                    ON CONFLICT (apartment_id, month_from)
                    DO UPDATE SET rent=0, updated_at=now()
                """),
                {"aid": int(aid), "ym": current_ym()},
            )
    return {"ok": True, "chat_id": str(chat_id)}


# -----------------------
# Admin: photo-events workflow
# -----------------------

@router.get("/admin/photo-events/unassigned")
def list_unassigned(limit: int = 50, offset: int = 0):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, chat_id, telegram_username, phone, ydisk_path, status, apartment_id, ocr_json, created_at
                FROM photo_events
                WHERE status = 'unassigned' AND apartment_id IS NULL
                ORDER BY id DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset},
        ).mappings().all()

    return {"ok": True, "items": list(rows)}


@router.post("/admin/photo-events/{photo_event_id}/assign")
def assign_photo_event(photo_event_id: int, apartment_id: int, bind_chat_id: bool = True):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        a = conn.execute(text("SELECT id FROM apartments WHERE id=:id"), {"id": apartment_id}).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="apartment not found")

        ev = conn.execute(
            text("SELECT id, chat_id FROM photo_events WHERE id=:id"),
            {"id": photo_event_id},
        ).fetchone()
        if not ev:
            raise HTTPException(status_code=404, detail="photo_event not found")

        ev_chat_id = ev[1]

        conn.execute(
            text("""
                UPDATE photo_events
                SET apartment_id=:apartment_id, status='assigned', stage='assigned', stage_updated_at=now()
                WHERE id=:id
            """),
            {"apartment_id": apartment_id, "id": photo_event_id},
        )

    if bind_chat_id:
        bind_chat(str(ev_chat_id), int(apartment_id))

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT id, chat_id, telegram_username, apartment_id, status, ydisk_path, created_at
                FROM photo_events WHERE id=:id
            """),
            {"id": photo_event_id},
        ).mappings().one()

    return {"ok": True, "item": dict(row), "bind_chat_id": bind_chat_id}


@router.post("/admin/photo-events/{photo_event_id}/unassign")
def unassign_photo_event(photo_event_id: int):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        e = conn.execute(
            text("SELECT id FROM photo_events WHERE id=:id"),
            {"id": photo_event_id},
        ).fetchone()
        if not e:
            raise HTTPException(status_code=404, detail="photo_event not found")

        conn.execute(
            text("""
                UPDATE photo_events
                SET apartment_id=NULL, status='unassigned', stage='unassigned', stage_updated_at=now()
                WHERE id=:id
            """),
            {"id": photo_event_id},
        )

        row = conn.execute(
            text("""
                SELECT id, chat_id, telegram_username, apartment_id, status, ydisk_path, created_at
                FROM photo_events WHERE id=:id
            """),
            {"id": photo_event_id},
        ).mappings().one()

    return {"ok": True, "item": dict(row)}
