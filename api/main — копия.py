from fastapi import FastAPI, UploadFile, File, Request, HTTPException, Body
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Literal, Tuple
from fastapi.responses import JSONResponse
import os
import json
import requests
import hashlib
from datetime import datetime
from sqlalchemy import create_engine, text

app = FastAPI(title="Rent Backend API")

# OCR
OCR_URL = os.getenv("OCR_URL", "http://host.docker.internal:8000/recognize")

# Yandex Disk WebDAV
YANDEX_WEBDAV_BASE_URL = os.getenv("YANDEX_WEBDAV_BASE_URL", "https://webdav.yandex.ru")
YANDEX_WEBDAV_USERNAME = os.getenv("YANDEX_WEBDAV_USERNAME", "")
YANDEX_WEBDAV_PASSWORD = os.getenv("YANDEX_WEBDAV_PASSWORD", "")
YANDEX_STORAGE_ROOT = os.getenv("YANDEX_STORAGE_ROOT", "tenants")

# DB
DATABASE_URL = os.getenv("DATABASE_URL", "")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None


def db_ready() -> bool:
    return engine is not None and bool(DATABASE_URL)


def ydisk_ready() -> bool:
    return bool(YANDEX_WEBDAV_USERNAME and YANDEX_WEBDAV_PASSWORD and YANDEX_WEBDAV_BASE_URL)


def ydisk_auth():
    return (YANDEX_WEBDAV_USERNAME, YANDEX_WEBDAV_PASSWORD)


def ydisk_mkcol(path: str) -> None:
    url = f"{YANDEX_WEBDAV_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.request("MKCOL", url, auth=ydisk_auth(), timeout=30)
    # 201 created, 405 already exists
    if r.status_code not in (201, 405):
        raise RuntimeError(f"MKCOL failed {r.status_code}: {r.text}")


def ydisk_put(path: str, content: bytes) -> None:
    url = f"{YANDEX_WEBDAV_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.put(
        url,
        data=content,
        auth=ydisk_auth(),
        headers={"Content-Type": "application/octet-stream"},
        timeout=20,
    )
    if r.status_code not in (201, 204):
        raise RuntimeError(f"Upload failed {r.status_code}: {r.text}")


def _safe_part(s: str, max_len: int = 40) -> str:
    s = (s or "").strip()
    if not s:
        return "NA"
    s = "".join(ch for ch in s if ch.isalnum() or ch in ("-", "_", "."))
    if not s:
        return "NA"
    return s[:max_len]


def upload_to_ydisk(
    chat_id: str,
    chat_name: str | None,
    meter_type_label: str | None,
    original_filename: str | None,
    content: bytes,
) -> str:
    """
    Путь: tenants/<chat_id>/<YYYY-MM>/<YYYY.MM.DD-HHMMSS>__<chat>__<meter>.<ext>
    """
    now = datetime.now()
    ym = now.strftime("%Y-%m")
    ts = now.strftime("%Y.%m.%d-%H%M%S")

    ext = "bin"
    if original_filename and "." in original_filename:
        ext = original_filename.rsplit(".", 1)[-1].lower()

    chat_part = _safe_part(chat_name or "UNKNOWN_CHAT", 40)
    meter_part = _safe_part(meter_type_label or "unknown", 20)

    filename = f"{ts}__{chat_part}__{meter_part}.{ext}"

    root = YANDEX_STORAGE_ROOT.strip("/")

    ydisk_mkcol(root)
    ydisk_mkcol(f"{root}/{chat_id}")
    ydisk_mkcol(f"{root}/{chat_id}/{ym}")

    disk_path = f"{root}/{chat_id}/{ym}/{filename}"
    ydisk_put(disk_path, content)
    return disk_path


def ensure_tables() -> None:
    """Idempotent создание таблиц + мягкие миграции.
    ВАЖНО: не ломаем существующую схему — только добавляем недостающие поля/индексы.
    """
    if not db_ready():
        return

    with engine.begin() as conn:
        # --- apartments ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS apartments (
                id BIGSERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                tenant_name TEXT NULL,
                address TEXT NULL,
                note TEXT NULL,
                ls_account TEXT NULL,
                electric_expected INTEGER NOT NULL DEFAULT 3,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text("ALTER TABLE apartments ADD COLUMN IF NOT EXISTS tenant_name TEXT NULL;"))
        conn.execute(text("ALTER TABLE apartments ADD COLUMN IF NOT EXISTS address TEXT NULL;"))
        conn.execute(text("ALTER TABLE apartments ADD COLUMN IF NOT EXISTS note TEXT NULL;"))
        conn.execute(text("ALTER TABLE apartments ADD COLUMN IF NOT EXISTS ls_account TEXT NULL;"))
        conn.execute(text("ALTER TABLE apartments ADD COLUMN IF NOT EXISTS electric_expected INTEGER NOT NULL DEFAULT 3;"))
        conn.execute(text("ALTER TABLE apartments ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();"))


        # --- tariffs ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tariffs (
                month_from TEXT PRIMARY KEY,  -- YYYY-MM
                cold NUMERIC(14,3) NOT NULL,
                hot NUMERIC(14,3) NOT NULL,
                electric NUMERIC(14,3) NOT NULL,
                sewer NUMERIC(14,3) NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text("ALTER TABLE tariffs ADD COLUMN IF NOT EXISTS sewer NUMERIC(14,3) NOT NULL DEFAULT 0;"))
        conn.execute(text("ALTER TABLE tariffs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();"))
        conn.execute(text("ALTER TABLE tariffs ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();"))

        # NEW: тарифы электро T1/T2/T3 (совместимость: если NULL — используем electric)
        conn.execute(text("ALTER TABLE tariffs ADD COLUMN IF NOT EXISTS electric_t1 NUMERIC(14,3) NULL;"))
        conn.execute(text("ALTER TABLE tariffs ADD COLUMN IF NOT EXISTS electric_t2 NUMERIC(14,3) NULL;"))
        conn.execute(text("ALTER TABLE tariffs ADD COLUMN IF NOT EXISTS electric_t3 NUMERIC(14,3) NULL;"))

        # --- apartment_contacts ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS apartment_contacts (
                id BIGSERIAL PRIMARY KEY,
                apartment_id BIGINT NOT NULL REFERENCES apartments(id) ON DELETE CASCADE,
                kind TEXT NOT NULL,       -- telegram | phone
                value TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))

        # --- apartment_statuses ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS apartment_statuses (
                apartment_id BIGINT PRIMARY KEY REFERENCES apartments(id) ON DELETE CASCADE,
                rent_paid BOOLEAN NOT NULL DEFAULT FALSE,
                meters_paid BOOLEAN NOT NULL DEFAULT FALSE,
                meters_photo_cold BOOLEAN NOT NULL DEFAULT FALSE,
                meters_photo_hot BOOLEAN NOT NULL DEFAULT FALSE,
                meters_photo_electric BOOLEAN NOT NULL DEFAULT FALSE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))

        # --- apartment_month_statuses ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS apartment_month_statuses (
                apartment_id BIGINT NOT NULL REFERENCES apartments(id) ON DELETE CASCADE,
                ym TEXT NOT NULL, -- YYYY-MM
                rent_paid BOOLEAN NOT NULL DEFAULT FALSE,
                meters_photo BOOLEAN NOT NULL DEFAULT FALSE,
                meters_paid BOOLEAN NOT NULL DEFAULT FALSE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (apartment_id, ym)
            );
        """))


        # Миграции (добавление новых колонок без ломания старых БД)
        conn.execute(text("ALTER TABLE apartment_month_statuses ADD COLUMN IF NOT EXISTS electric_extra_pending BOOLEAN NOT NULL DEFAULT FALSE"))
        conn.execute(text("ALTER TABLE apartment_month_statuses ADD COLUMN IF NOT EXISTS electric_expected_snapshot INTEGER"))
        conn.execute(text("ALTER TABLE apartment_month_statuses ADD COLUMN IF NOT EXISTS electric_extra_resolved_at TIMESTAMPTZ"))


        # --- meter_readings (единая схема) ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS meter_readings (
                id BIGSERIAL PRIMARY KEY,
                apartment_id BIGINT NOT NULL REFERENCES apartments(id) ON DELETE CASCADE,
                ym TEXT NOT NULL,
                meter_type TEXT NOT NULL,
                meter_index INTEGER NOT NULL DEFAULT 1,
                value NUMERIC(12,3) NOT NULL,
                source TEXT NOT NULL DEFAULT 'ocr',
                ocr_value NUMERIC(12,3) NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (apartment_id, ym, meter_type, meter_index)
            );
        """))

        # --- chat_bindings ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS chat_bindings (
                chat_id TEXT PRIMARY KEY,
                apartment_id BIGINT NOT NULL REFERENCES apartments(id) ON DELETE CASCADE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))

        # --- photo_events ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS photo_events (
                id BIGSERIAL PRIMARY KEY,
                chat_id TEXT NOT NULL,
                telegram_username TEXT NULL,
                phone TEXT NULL,
                original_filename TEXT NULL,
                ydisk_path TEXT NULL,
                status TEXT NOT NULL DEFAULT 'unassigned',
                apartment_id BIGINT NULL,
                ocr_json JSONB NULL,

                meter_index INTEGER NOT NULL DEFAULT 1,

                stage TEXT NOT NULL DEFAULT 'received',
                stage_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                file_sha256 TEXT NULL,
                ocr_type TEXT NULL,
                ocr_reading NUMERIC(12,3) NULL,
                meter_kind TEXT NULL,
                meter_value NUMERIC(12,3) NULL,
                meter_written BOOLEAN NOT NULL DEFAULT FALSE,
                diag_json JSONB NULL,

                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))

        # Soft migrations
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS meter_index INTEGER NOT NULL DEFAULT 1;"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS stage TEXT NOT NULL DEFAULT 'received';"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS stage_updated_at TIMESTAMPTZ NOT NULL DEFAULT now();"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS file_sha256 TEXT NULL;"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS ocr_type TEXT NULL;"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS ocr_reading NUMERIC(12,3) NULL;"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS meter_kind TEXT NULL;"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS meter_value NUMERIC(12,3) NULL;"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS meter_written BOOLEAN NOT NULL DEFAULT FALSE;"))
        conn.execute(text("ALTER TABLE photo_events ADD COLUMN IF NOT EXISTS diag_json JSONB NULL;"))

        # FK безопасно
        conn.execute(text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'fk_photo_events_apartment'
            ) THEN
                ALTER TABLE photo_events
                ADD CONSTRAINT fk_photo_events_apartment
                FOREIGN KEY (apartment_id) REFERENCES apartments(id) ON DELETE SET NULL;
            END IF;
        END $$;
        """))

        # Indexes
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_apartments_ls_account ON apartments(ls_account) WHERE ls_account IS NOT NULL;"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_apartment_contacts_kind_value ON apartment_contacts(kind, value);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_apartment_contacts_apartment_id ON apartment_contacts(apartment_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_chat_bindings_apartment_id ON chat_bindings(apartment_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_chat_id ON photo_events(chat_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_created_at ON photo_events(created_at);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_status ON photo_events(status);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_apartment_id ON photo_events(apartment_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_meter_index ON photo_events(meter_index);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_stage ON photo_events(stage);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_file_sha256 ON photo_events(file_sha256);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_stage_updated_at ON photo_events(stage_updated_at);"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_apartment_month_statuses_ym ON apartment_month_statuses(ym);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_meter_readings_apartment_ym ON meter_readings(apartment_id, ym);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_meter_readings_meter_type ON meter_readings(meter_type);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_apartment_statuses_updated_at ON apartment_statuses(updated_at);"))

        # seed statuses for existing apartments
        conn.execute(text("""
            INSERT INTO apartment_statuses(apartment_id)
            SELECT id FROM apartments
            ON CONFLICT (apartment_id) DO NOTHING;
        """))


def norm_phone(p: str) -> str:
    """
    Нормализация телефона для поиска/хранения.
    Приводим к канону РФ: 11 цифр, начинается с "7".

    Принимаем варианты:
    - +7XXXXXXXXXX
    - 8XXXXXXXXXX
    - 7XXXXXXXXXX
    - XXXXXXXXXX (10 цифр без кода страны) -> добавляем "7"
    """
    digits = "".join(ch for ch in (p or "") if ch.isdigit())
    if not digits:
        return ""

    # Часто в логах/контактах могут прилетать хвосты/приставки —
    # для РФ берём последние 10 цифр как номер и добавляем "7".
    if len(digits) > 11:
        tail10 = digits[-10:]
        if len(tail10) == 10:
            digits = "7" + tail10

    if len(digits) == 10:
        digits = "7" + digits

    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]

    # Если получилось не 11 цифр — возвращаем как есть (но это будет сложнее матчить).
    return digits


def _phone_variants(phone: str) -> List[str]:
    """
    Возвращает набор вариантов телефона для поиска в БД,
    чтобы совпадать со старыми/разными форматами записи.
    """
    raw_digits = "".join(ch for ch in (phone or "") if ch.isdigit())
    variants = set()
    if raw_digits:
        variants.add(raw_digits)
        if len(raw_digits) >= 10:
            variants.add("7" + raw_digits[-10:])
            variants.add("8" + raw_digits[-10:])
    n = norm_phone(phone)
    if n:
        variants.add(n)
        if len(n) == 11 and n.startswith("7"):
            variants.add("8" + n[1:])
    # Стабильный порядок
    return [v for v in sorted(variants, key=lambda x: (len(x), x), reverse=True)]

def find_apartment_by_chat(chat_id: str) -> int | None:
    if not db_ready():
        return None
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT apartment_id
                FROM chat_bindings
                WHERE chat_id=:chat_id AND is_active=true
                LIMIT 1
            """),
            {"chat_id": str(chat_id)},
        ).fetchone()
        if row:
            return int(row[0])
    return None


def find_apartment_by_contact(telegram_username: str | None, phone: str | None) -> int | None:
    """
    Поиск квартиры по контактам (telegram username / phone).
    ВАЖНО: phone матчим по нескольким вариантам (+7/8/7/без кода),
    чтобы старые записи в базе тоже находились.
    """
    if not db_ready():
        return None

    candidates: list[tuple[str, str]] = []

    if telegram_username:
        u = telegram_username.strip().lstrip("@").lower()
        if u:
            candidates.append(("telegram", u))

    if phone:
        for ph in _phone_variants(phone):
            if ph:
                candidates.append(("phone", ph))

    if not candidates:
        return None

    with engine.begin() as conn:
        for kind, value in candidates:
            row = conn.execute(
                text("""
                    SELECT apartment_id
                    FROM apartment_contacts
                    WHERE kind=:kind AND value=:value AND is_active=true
                    LIMIT 1
                """),
                {"kind": kind, "value": value},
            ).fetchone()
            if row:
                return int(row[0])

    return None

def bind_chat(chat_id: str, apartment_id: int) -> None:
    """Upsert: chat может меняться, поэтому разрешаем перепривязку."""
    if not db_ready():
        return
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO chat_bindings (chat_id, apartment_id, is_active, updated_at)
                VALUES (:chat_id, :apartment_id, true, now())
                ON CONFLICT (chat_id)
                DO UPDATE SET apartment_id=EXCLUDED.apartment_id, is_active=true, updated_at=now()
            """),
            {"chat_id": str(chat_id), "apartment_id": int(apartment_id)},
        )


@app.on_event("startup")
def _startup():
    if not db_ready():
        return
    try:
        ensure_tables()
    except Exception as e:
        print(f"[startup] ensure_tables failed: {e}")


@app.get("/health")
def health():
    return {
        "ok": True,
        "ocr_url": OCR_URL,
        "db": "ok" if db_ready() else "disabled",
        "ydisk": "ok" if ydisk_ready() else "disabled",
    }


# -----------------------
# Admin: apartments
# -----------------------

@app.post("/admin/apartments")
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


@app.get("/admin/apartments")
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


@app.get("/admin/apartments/{apartment_id}")
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

@app.post("/admin/apartments/{apartment_id}/contacts")
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


@app.post("/admin/contacts/{contact_id}/deactivate")
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


@app.post("/admin/contacts/{contact_id}/activate")
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

@app.get("/admin/chats/{chat_id}")
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


@app.post("/admin/chats/{chat_id}/bind")
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


@app.post("/admin/chats/{chat_id}/unbind")
def unbind_chat_admin(chat_id: str):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE chat_bindings
                SET is_active=false, updated_at=now()
                WHERE chat_id=:chat_id
            """),
            {"chat_id": str(chat_id)},
        )
    return {"ok": True, "chat_id": str(chat_id)}


# -----------------------
# Admin: photo-events workflow
# -----------------------

@app.get("/admin/photo-events/unassigned")
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


@app.post("/admin/photo-events/{photo_event_id}/assign")
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


@app.post("/admin/photo-events/{photo_event_id}/unassign")
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


# -----------------------
# Admin UI (simple endpoints for the one-page dashboard)
# -----------------------

def current_ym() -> str:
    return datetime.now().strftime("%Y-%m")


class UIContacts(BaseModel):
    phone: Optional[str] = None
    telegram: Optional[str] = None


class UIStatuses(BaseModel):
    rent_paid: bool = False
    meters_photo: bool = False
    meters_paid: bool = False


class UIApartmentItem(BaseModel):
    id: int
    title: str
    address: Optional[str] = None
    tenant_name: Optional[str] = None
    note: Optional[str] = None
    ls_account: Optional[str] = None  # Лицевой счет (л/с)
    electric_expected: int = 3
    contacts: UIContacts = UIContacts()
    statuses: UIStatuses = UIStatuses()



class UIApartmentCreate(BaseModel):
    title: str = Field(..., min_length=1)
    address: Optional[str] = None
    ls_account: Optional[str] = None  # Лицевой счет (л/с)


class UIApartmentPatch(BaseModel):
    title: Optional[str] = None
    address: Optional[str] = None
    tenant_name: Optional[str] = None
    note: Optional[str] = None
    ls_account: Optional[str] = None  # Лицевой счет (л/с)
    electric_expected: Optional[int] = Field(None, ge=1, le=3)
    phone: Optional[str] = None
    telegram: Optional[str] = None



class UIStatusesPatch(BaseModel):
    rent_paid: Optional[bool] = None
    meters_photo: Optional[bool] = None
    meters_paid: Optional[bool] = None



# --- Aliases & input models (names referenced in endpoint annotations) ---
# Without these definitions, Uvicorn fails on import with NameError.
StatusPatch = UIStatusesPatch

class MeterCurrentPatch(BaseModel):
    # All fields optional: UI can patch only some cells
    cold: Optional[float] = None
    hot: Optional[float] = None
    sewer: Optional[float] = None
    electric_t1: Optional[float] = None
    electric_t2: Optional[float] = None
    electric_t3: Optional[float] = None

class TariffIn(BaseModel):
    # Accept both month_from (new) and ym_from (legacy)
    month_from: Optional[str] = None
    ym_from: Optional[str] = None

    cold: float
    hot: float
    sewer: float

    electric: Optional[float] = None
    electric_t1: Optional[float] = None
    electric_t2: Optional[float] = None
    electric_t3: Optional[float] = None

def _get_active_contact(apartment_id: int, kind: str) -> Optional[str]:
    if not db_ready():
        return None
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT value
                FROM apartment_contacts
                WHERE apartment_id=:aid AND kind=:kind AND is_active=true
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"aid": int(apartment_id), "kind": kind},
        ).fetchone()
        return row[0] if row else None


def _set_contact(apartment_id: int, kind: str, value: Optional[str]) -> None:
    if not db_ready():
        return

    v = (value or "").strip()
    if kind == "telegram":
        v = v.lstrip("@").lower().strip()
    elif kind == "phone":
        v = norm_phone(v)

    with engine.begin() as conn:
        # 1) если значение пустое — выключаем активный контакт этого типа у квартиры
        if not v:
            conn.execute(
                text("""
                    UPDATE apartment_contacts
                    SET is_active=false
                    WHERE apartment_id=:aid AND kind=:kind AND is_active=true
                """),
                {"aid": int(apartment_id), "kind": kind},
            )
            return

        # 2) если меняем значение — выключаем другие активные контакты этого типа у этой квартиры
        conn.execute(
            text("""
                UPDATE apartment_contacts
                SET is_active=false
                WHERE apartment_id=:aid AND kind=:kind AND value<>:value AND is_active=true
            """),
            {"aid": int(apartment_id), "kind": kind, "value": v},
        )

        # 3) upsert по (kind,value): если такой контакт уже есть в базе — перепривязываем к этой квартире
        conn.execute(
            text("""
                INSERT INTO apartment_contacts (apartment_id, kind, value, is_active)
                VALUES (:aid, :kind, :value, true)
                ON CONFLICT (kind, value)
                DO UPDATE SET apartment_id=EXCLUDED.apartment_id, is_active=true
            """),
            {"aid": int(apartment_id), "kind": kind, "value": v},
        )


def _get_month_statuses(apartment_id: int, ym: str) -> UIStatuses:
    if not db_ready():
        return UIStatuses()
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT rent_paid, meters_photo, meters_paid
                FROM apartment_month_statuses
                WHERE apartment_id=:aid AND ym=:ym
                LIMIT 1
            """),
            {"aid": int(apartment_id), "ym": ym},
        ).fetchone()
        if not row:
            return UIStatuses()
        return UIStatuses(rent_paid=bool(row[0]), meters_photo=bool(row[1]), meters_paid=bool(row[2]))


def _upsert_month_statuses(apartment_id: int, ym: str, patch: UIStatusesPatch) -> UIStatuses:
    if not db_ready():
        return UIStatuses()

    current = _get_month_statuses(apartment_id, ym)
    new_rent = current.rent_paid if patch.rent_paid is None else bool(patch.rent_paid)
    new_photo = current.meters_photo if patch.meters_photo is None else bool(patch.meters_photo)
    new_paid = current.meters_paid if patch.meters_paid is None else bool(patch.meters_paid)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO apartment_month_statuses (apartment_id, ym, rent_paid, meters_photo, meters_paid)
                VALUES (:aid, :ym, :rent, :photo, :paid)
                ON CONFLICT (apartment_id, ym)
                DO UPDATE SET
                  rent_paid=EXCLUDED.rent_paid,
                  meters_photo=EXCLUDED.meters_photo,
                  meters_paid=EXCLUDED.meters_paid,
                  updated_at=now()
            """),
            {"aid": int(apartment_id), "ym": ym, "rent": new_rent, "photo": new_photo, "paid": new_paid},
        )

    return UIStatuses(rent_paid=new_rent, meters_photo=new_photo, meters_paid=new_paid)


@app.get("/admin/ui/apartments")
def ui_list_apartments(ym: Optional[str] = None):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = (ym or current_ym()).strip()

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, title, address, tenant_name, note, ls_account, electric_expected
                FROM apartments
                ORDER BY id DESC
            """)
        ).fetchall()

    items: List[Dict[str, Any]] = []
    for r in rows:
        aid = int(r[0])
        phone = _get_active_contact(aid, "phone")
        telegram = _get_active_contact(aid, "telegram")
        statuses = _get_month_statuses(aid, ym_)
        items.append(
            UIApartmentItem(
                id=aid,
                title=r[1],
                address=r[2],
                tenant_name=r[3],
                note=r[4],
                ls_account=r[5],
                electric_expected=int(r[6]) if r[6] is not None else 3,
                contacts=UIContacts(phone=phone, telegram=telegram),
                statuses=statuses,
            ).model_dump()
        )

    return {"ok": True, "ym": ym_, "items": items}


@app.post("/admin/ui/apartments")
def ui_create_apartment(body: UIApartmentCreate):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    title = body.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="title_required")

    with engine.begin() as conn:
        try:
            new_id = conn.execute(
                text("""
                    INSERT INTO apartments (title, address, ls_account)
                    VALUES (:title, :address, :ls_account)
                    RETURNING id
                """),
                {"title": title, "address": (body.address or None), "ls_account": (body.ls_account or None)},
            ).scalar_one()
        except Exception as e:
            # Уникальность лицевого счёта (л/с)
            if "uq_apartments_ls_account" in str(e):
                raise HTTPException(status_code=409, detail="ls_account_taken")
            raise

    return {"ok": True, "id": int(new_id)}


@app.patch("/admin/ui/apartments/{apartment_id}")
def ui_patch_apartment(apartment_id: int, body: UIApartmentPatch):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    sets = []
    params: Dict[str, Any] = {"id": int(apartment_id)}
    if body.title is not None:
        t = body.title.strip()
        if not t:
            raise HTTPException(status_code=400, detail="title_required")
        sets.append("title=:title")
        params["title"] = t
    if body.address is not None:
        sets.append("address=:address")
        params["address"] = body.address.strip() if body.address.strip() else None
    if body.tenant_name is not None:
        sets.append("tenant_name=:tenant_name")
        params["tenant_name"] = body.tenant_name.strip() if body.tenant_name.strip() else None
    if body.note is not None:
        sets.append("note=:note")
        params["note"] = body.note.strip() if body.note.strip() else None

    if body.ls_account is not None:
        ls = body.ls_account.strip()
        params["ls_account"] = ls if ls else None
        sets.append("ls_account=:ls_account")

    if body.electric_expected is not None:
        sets.append("electric_expected=:electric_expected")
        params["electric_expected"] = int(body.electric_expected)


    if sets:
        with engine.begin() as conn:
            try:
                res = conn.execute(
                    text(f"UPDATE apartments SET {', '.join(sets)} WHERE id=:id"),
                    params,
                )
            except Exception as e:
                if "uq_apartments_ls_account" in str(e):
                    raise HTTPException(status_code=409, detail="ls_account_taken")
                raise

            if res.rowcount == 0:
                raise HTTPException(status_code=404, detail="apartment_not_found")

    if body.phone is not None:
        _set_contact(apartment_id, "phone", body.phone)
    if body.telegram is not None:
        _set_contact(apartment_id, "telegram", body.telegram)

    return {"ok": True}


@app.delete("/admin/ui/apartments/{apartment_id}")
def ui_delete_apartment(apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        row = conn.execute(text("SELECT id FROM apartments WHERE id=:id"), {"id": int(apartment_id)}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="apartment_not_found")
        conn.execute(text("DELETE FROM apartments WHERE id=:id"), {"id": int(apartment_id)})

    return {"ok": True, "deleted_id": int(apartment_id)}


@app.get("/admin/ui/apartments/{apartment_id}/card")
def ui_apartment_card(apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        apt = conn.execute(
            text("""
                SELECT id, title, address, tenant_name, note, ls_account, electric_expected, created_at
                FROM apartments
                WHERE id=:id
                LIMIT 1
            """),

            {"id": int(apartment_id)},
        ).mappings().fetchone()

        if not apt:
            raise HTTPException(status_code=404, detail="apartment_not_found")

        chats = conn.execute(
            text("""
                SELECT chat_id, is_active, updated_at, created_at
                FROM chat_bindings
                WHERE apartment_id=:id
                ORDER BY is_active DESC, updated_at DESC
            """),
            {"id": int(apartment_id)},
        ).mappings().all()

    phone = _get_active_contact(int(apartment_id), "phone")
    telegram = _get_active_contact(int(apartment_id), "telegram")

    return {
        "ok": True,
        "apartment": dict(apt),
        "contacts": {"phone": phone, "telegram": telegram},
        "chats": [dict(x) for x in chats],
    }


@app.patch("/admin/ui/apartments/{apartment_id}/statuses")
def ui_patch_statuses(apartment_id: int, body: UIStatusesPatch, ym: Optional[str] = None):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = (ym or current_ym()).strip()
    statuses = _upsert_month_statuses(apartment_id, ym_, body)
    return {"ok": True, "ym": ym_, "statuses": statuses.model_dump()}


# -----------------------
# Dashboard helpers
# -----------------------

Kind = Literal["cold", "hot", "electric"]


def _ocr_to_kind(ocr_type: str | None) -> str | None:
    if not ocr_type:
        return None

    t = str(ocr_type).strip().lower()

    if t in ("cold", "hot", "electric"):
        return t

    if "гвс" in t or "горяч" in t or "hot" in t:
        return "hot"
    if "хвс" in t or "холод" in t or "cold" in t:
        return "cold"
    if "элект" in t or "квт" in t or "kwh" in t:
        return "electric"

    return None


def _parse_reading_to_float(reading: str | None) -> float | None:
    if reading is None:
        return None
    s = str(reading).strip().replace(" ", "")
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def month_now() -> str:
    return datetime.now().strftime("%Y-%m")


def ym_prev(ym: str) -> str:
    y, m = ym.split("-")
    y = int(y)
    m = int(m)
    m -= 1
    if m == 0:
        m = 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def _get_tariff_for_month(conn, ym: str) -> Optional[Dict[str, float]]:
    row = conn.execute(
        text("""
            SELECT
                month_from,
                cold, hot, sewer,
                COALESCE(electric_t1, electric) AS electric_t1,
                COALESCE(electric_t2, electric) AS electric_t2,
                COALESCE(electric_t3, electric) AS electric_t3
            FROM tariffs
            WHERE month_from <= :ym
            ORDER BY month_from DESC
            LIMIT 1
        """),
        {"ym": ym},
    ).mappings().fetchone()
    if not row:
        return None
    return {
        "month_from": str(row["month_from"]),
        "cold": float(row["cold"]),
        "hot": float(row["hot"]),
        "sewer": float(row["sewer"]),
        "electric_t1": float(row["electric_t1"]) if row["electric_t1"] is not None else None,
        "electric_t2": float(row["electric_t2"]) if row["electric_t2"] is not None else None,
        "electric_t3": float(row["electric_t3"]) if row["electric_t3"] is not None else None,
    }


def _get_reading(conn, apartment_id: int, ym: str, meter_type: str, meter_index: int = 1) -> Optional[float]:
    row = conn.execute(
        text("""
            SELECT value
            FROM meter_readings
            WHERE apartment_id=:aid AND ym=:ym AND meter_type=:t AND meter_index=:i
            LIMIT 1
        """),
        {"aid": int(apartment_id), "ym": ym, "t": meter_type, "i": int(meter_index)},
    ).fetchone()
    if not row:
        return None
    try:
        return float(row[0])
    except Exception:
        return None



def _get_apartment_electric_expected(conn, apartment_id: int) -> int:
    row = conn.execute(
        text("SELECT COALESCE(electric_expected, 3) AS n FROM apartments WHERE id=:id"),
        {"id": apartment_id},
    ).mappings().first()
    n = int(row["n"]) if row and row["n"] is not None else 3
    if n < 1:
        n = 1
    if n > 3:
        n = 3
    return n


def _get_month_extra_state(conn, apartment_id: int, ym: str) -> Dict[str, Any]:
    row = conn.execute(
        text(
            "SELECT electric_extra_pending, electric_expected_snapshot "
            "FROM apartment_month_statuses WHERE apartment_id=:aid AND ym=:ym"
        ),
        {"aid": apartment_id, "ym": ym},
    ).mappings().first()
    if not row:
        return {"pending": False, "snapshot": None}
    return {
        "pending": bool(row.get("electric_extra_pending") or False),
        "snapshot": row.get("electric_expected_snapshot"),
    }


def _set_month_extra_state(conn, apartment_id: int, ym: str, pending: bool, snapshot: Optional[int]) -> None:
    # гарантируем строку месяца
    conn.execute(
        text(
            "INSERT INTO apartment_month_statuses(apartment_id, ym) "
            "VALUES(:aid, :ym) ON CONFLICT (apartment_id, ym) DO NOTHING"
        ),
        {"aid": apartment_id, "ym": ym},
    )
    conn.execute(
        text(
            "UPDATE apartment_month_statuses "
            "SET electric_extra_pending=:p, electric_expected_snapshot=:s, "
            "electric_extra_resolved_at = CASE WHEN :p THEN NULL ELSE NOW() END "
            "WHERE apartment_id=:aid AND ym=:ym"
        ),
        {"aid": apartment_id, "ym": ym, "p": bool(pending), "s": snapshot},
    )

def _calc_month_bill(conn, apartment_id: int, ym: str) -> Dict[str, Any]:
    """
    Возвращает:
      - is_complete_photos: есть ли все текущие показания, нужные для расчета (cold/hot + electric 1..N)
      - total_rub: сумма ₽, если можно посчитать (есть прошлый месяц + тарифы) и нет блокировок
      - missing: что ещё нужно для расчёта
      - reason: 'ok' | 'missing_photos' | 'no_prev_month' | 'pending_admin'
      - electric_expected: N (1..3)
      - extra_pending: есть ли “лишние” электрические показания, требующие решения админа
    """
    ym = (ym or "").strip()
    if not is_ym(ym):
        return {
            "is_complete_photos": False,
            "total_rub": None,
            "missing": ["invalid_ym"],
            "reason": "missing_photos",
            "electric_expected": 3,
            "extra_pending": False,
        }

    electric_expected = _get_apartment_electric_expected(conn, apartment_id)
    extra_state = _get_month_extra_state(conn, apartment_id, ym)
    extra_pending = bool(extra_state.get("pending"))

    # текущие показания (ВАЖНО: вода хранится с meter_index=1)
    cur = conn.execute(
        text(
            "SELECT meter_type, meter_index, value "
            "FROM meter_readings "
            "WHERE apartment_id=:aid AND ym=:ym AND source IN ('ocr','manual')"
        ),
        {"aid": apartment_id, "ym": ym},
    ).mappings().all()

    cur_map: Dict[str, Dict[int, Optional[float]]] = {"cold": {}, "hot": {}, "electric": {}}
    for r in cur:
        mt = r["meter_type"]
        mi = int(r["meter_index"] or 0)
        cur_map.setdefault(mt, {})[mi] = r["value"]

    missing: List[str] = []
    if cur_map.get("cold", {}).get(1) is None:
        missing.append("cold")
    if cur_map.get("hot", {}).get(1) is None:
        missing.append("hot")
    for i in range(1, electric_expected + 1):
        if cur_map.get("electric", {}).get(i) is None:
            missing.append(f"electric_{i}")

    is_complete_photos = len(missing) == 0
    if not is_complete_photos:
        return {
            "is_complete_photos": False,
            "total_rub": None,
            "missing": missing,
            "reason": "missing_photos",
            "electric_expected": electric_expected,
            "extra_pending": extra_pending,
        }

    if extra_pending:
        return {
            "is_complete_photos": True,
            "total_rub": None,
            "missing": [],
            "reason": "pending_admin",
            "electric_expected": electric_expected,
            "extra_pending": True,
        }

    prev_ym = add_months(ym, -1)
    prev = conn.execute(
        text(
            "SELECT meter_type, meter_index, value "
            "FROM meter_readings "
            "WHERE apartment_id=:aid AND ym=:ym AND source IN ('ocr','manual')"
        ),
        {"aid": apartment_id, "ym": prev_ym},
    ).mappings().all()

    prev_map: Dict[str, Dict[int, Optional[float]]] = {"cold": {}, "hot": {}, "electric": {}}
    for r in prev:
        mt = r["meter_type"]
        mi = int(r["meter_index"] or 0)
        prev_map.setdefault(mt, {})[mi] = r["value"]

    tariff = effective_tariff_for_month(conn, ym)

    dc = safe_delta(cur_map["cold"].get(1), prev_map["cold"].get(1))
    dh = safe_delta(cur_map["hot"].get(1), prev_map["hot"].get(1))

    # Водоотведение: если отдельного счётчика нет — считаем как ХВС+ГВС
    ds = safe_delta(
        cur_map.get("sewer", {}).get(1),
        prev_map.get("sewer", {}).get(1),
    )
    if ds is None:
        ds = (dc or 0) + (dh or 0)

    def elec_tariff(idx: int) -> float:
        base = float(tariff.get("electric") or 0)
        if idx == 1:
            return float(tariff.get("electric_t1") or base)
        if idx == 2:
            return float(tariff.get("electric_t2") or base)
        if idx == 3:
            return float(tariff.get("electric_t3") or base)
        return base

    re_sum = 0.0
    for idx in range(1, electric_expected + 1):
        de = safe_delta(cur_map["electric"].get(idx), prev_map["electric"].get(idx))
        if de is None:
            continue
        re_sum += de * elec_tariff(idx)

    rc = (dc or 0) * float(tariff.get("cold") or 0)
    rh = (dh or 0) * float(tariff.get("hot") or 0)
    rs = (ds or 0) * float(tariff.get("sewer") or 0)

    any_prev = (
        prev_map["cold"].get(1) is not None
        or prev_map["hot"].get(1) is not None
        or any(prev_map["electric"].get(i) is not None for i in range(1, electric_expected + 1))
    )
    if not any_prev:
        return {
            "is_complete_photos": True,
            "total_rub": None,
            "missing": [],
            "reason": "no_prev_month",
            "electric_expected": electric_expected,
            "extra_pending": False,
        }

    total = rc + rh + rs + re_sum
    return {
        "is_complete_photos": True,
        "total_rub": round(total, 2),
        "missing": [],
        "reason": "ok",
        "electric_expected": electric_expected,
        "extra_pending": False,
    }

def _write_electric_explicit(conn, apartment_id: int, ym: str, meter_index: int, new_value: float) -> int:
    """Пишем электро строго в заданный meter_index (1..3), без пересортировки по величине."""
    try:
        meter_index = int(meter_index)
    except Exception:
        return 0
    meter_index = max(1, min(3, meter_index))

    expected = _get_apartment_electric_expected(conn, apartment_id)

    conn.execute(
        text(
            "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
            "VALUES(:aid,:ym,'electric',:idx,:val,'ocr',:ocr) "
            "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
            " value=EXCLUDED.value, source='ocr', ocr_value=EXCLUDED.ocr_value, updated_at=now()"
        ),
        {"aid": int(apartment_id), "ym": ym, "idx": int(meter_index), "val": float(new_value), "ocr": float(new_value)},
    )

    # если прислали индекс “выше ожидания” (например ожидаем 1 фото, а пришел idx=2) — блокируем расчет до решения админа
    if int(meter_index) > int(expected) and int(expected) < 3:
        _set_month_extra_state(conn, int(apartment_id), str(ym), True, int(expected))

    return int(meter_index)


def _assign_and_write_electric_sorted(apartment_id: int, ym: str, new_value: float) -> int:
    """
    Совместимый вход (не меняем вызовы): возвращает индекс, в который попало новое значение.

    Новая логика:
      - учитываем apartments.electric_expected (1..3)
      - если получено больше уникальных значений, чем ожидаем, то 1 “лишнее” значение пишем
        в индекс expected+1 и помечаем месяц как electric_extra_pending=true
      - пока extra_pending=true — расчёт суммы блокируем (reason='pending_admin')
      - повторяющиеся значения (в пределах допуска) просто игнорируем, без сообщений пользователю
    """

    ym = (ym or "").strip()

    # допуск на “одинаковость” (чтобы 100 и 100.0 считались одинаковыми)
    def same(a: float, b: float) -> bool:
        try:
            return abs(float(a) - float(b)) < 1e-6
        except Exception:
            return False

    with engine.begin() as conn:
        expected = _get_apartment_electric_expected(conn, apartment_id)

        # берём все текущие электрические показания за месяц
        rows = conn.execute(
            text(
                "SELECT meter_index, value, source "
                "FROM meter_readings "
                "WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric'"
            ),
            {"aid": apartment_id, "ym": ym},
        ).mappings().all()

        # если есть manual — мы НЕ пересортировываем руками введённые значения:
        # просто пытаемся положить новое значение в первый свободный индекс (1..3),
        # учитывая expected (лишнее -> pending).
        has_manual = any((r.get("source") == "manual") for r in rows)

        existing_vals = [float(r["value"]) for r in rows if r.get("value") is not None]
        if any(same(v, new_value) for v in existing_vals):
            # дубликат — ничего не делаем
            return 0

        if has_manual:
            used = set(int(r["meter_index"]) for r in rows if r.get("meter_index") is not None)
            free = None
            for i in (1, 2, 3):
                if i not in used:
                    free = i
                    break
            if free is None:
                return 0

            # записываем и помечаем pending, если индекс "лишний"
            conn.execute(
                text(
                    "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source) "
                    "VALUES(:aid,:ym,'electric',:idx,:val,'ocr') "
                    "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET value=EXCLUDED.value, source=EXCLUDED.source"
                ),
                {"aid": apartment_id, "ym": ym, "idx": free, "val": float(new_value)},
            )

            if free > expected and expected < 3:
                _set_month_extra_state(conn, apartment_id, ym, True, expected)
            return free

        # OCR-only: собираем уникальные значения (max 3)
        uniq: List[float] = []
        for v in existing_vals + [float(new_value)]:
            if not any(same(v, u) for u in uniq):
                uniq.append(v)

        uniq = sorted(uniq)[:3]

        extra_pending = False
        extra_idx: Optional[int] = None
        extra_val: Optional[float] = None

        normal_vals = uniq
        if len(uniq) > expected and expected < 3:
            extra_pending = True
            extra_idx = expected + 1
            extra_val = uniq[expected]
            normal_vals = uniq[:expected]

        # mapping в индексы
        mapping: Dict[int, float] = {}

        if len(normal_vals) == 1:
            mapping[1] = normal_vals[0]
        elif len(normal_vals) == 2:
            # базово: min -> T1, max -> T2
            mapping[1] = normal_vals[0]
            mapping[2] = normal_vals[1]
        elif len(normal_vals) == 3:
            # по требованиям: T2 = min, T3 = max, T1 = среднее (по величине)
            mapping[2] = normal_vals[0]
            mapping[1] = normal_vals[1]
            mapping[3] = normal_vals[2]

        if extra_pending and extra_idx and extra_val is not None:
            mapping[int(extra_idx)] = float(extra_val)

        # Перезаписываем электро-строки на месяц только в диапазоне 1..3
        conn.execute(
            text(
                "DELETE FROM meter_readings "
                "WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index BETWEEN 1 AND 3"
            ),
            {"aid": apartment_id, "ym": ym},
        )
        for idx, val in mapping.items():
            conn.execute(
                text(
                    "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source) "
                    "VALUES(:aid,:ym,'electric',:idx,:val,'ocr') "
                    "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET value=EXCLUDED.value, source=EXCLUDED.source"
                ),
                {"aid": apartment_id, "ym": ym, "idx": int(idx), "val": float(val)},
            )

        # pending flag
        if extra_pending:
            _set_month_extra_state(conn, apartment_id, ym, True, expected)
        else:
            _set_month_extra_state(conn, apartment_id, ym, False, None)

        # определяем, какой индекс получил new_value
        for idx, val in mapping.items():
            if same(val, float(new_value)):
                return int(idx)

        return 0


@app.post("/events/photo")
async def photo_event(request: Request, file: UploadFile = File(None)):
    diag = {"errors": [], "warnings": []}

    form = await request.form()
    chat_id = form.get("chat_id") or "unknown"
    telegram_username = form.get("telegram_username") or None
    phone = form.get("phone") or None

    raw_meter_index = form.get("meter_index")
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
                            status, apartment_id, ocr_json,
                            meter_index,
                            stage, stage_updated_at,
                            file_sha256, ocr_type, ocr_reading,
                            meter_kind, meter_value, meter_written,
                            diag_json
                        )
                        VALUES
                        (
                            :chat_id, :username, :phone, :orig, :path,
                            :status, :apartment_id,
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
    ym = month_now()
    assigned_meter_index = int(meter_index)

    if db_ready() and apartment_id and kind and (value_float is not None):
        try:
            # 6.1) СНАЧАЛА пишем показания в meter_readings и получаем assigned_meter_index
            if kind == "electric":
                # Если клиент явно передал meter_index (бот/админ) — пишем строго в этот индекс.
                # Если meter_index не передан — авто-логика (sorted) для совместимости.
                if raw_meter_index is not None:
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
                # water (cold/hot): всегда meter_index=1
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

            # 6.2) ПОСЛЕ записи проверяем: возможно это одно и то же фото (значение совпало с другим счётчиком)
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

            # 6.3) Обновляем статусы
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
                    patch_apartment_statuses(int(apartment_id), StatusPatch(**patch))
            except Exception as e:
                diag["warnings"].append({"apartment_status_update_failed": str(e)})

            wrote_meter = True

            # 6.4) Обновляем photo_events и сохраняем актуальный diag_json (уже с possible_duplicate)
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


    # 7) bill (для бота и web)
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


# -----------------------
# Dashboard: apartments list
# -----------------------

@app.get("/bot/chats/{chat_id}/bill")
import re

_YM_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

def is_ym(value: str) -> bool:
    """
    Validate year-month string in format YYYY-MM.
    Example: 2026-01
    """
    if value is None:
        return False
    s = str(value).strip()
    return bool(_YM_RE.match(s))

def bot_chat_bill(chat_id: str, ym: Optional[str] = None):
    """Используется ботом для проверки “что ещё нужно” и/или выдачи суммы после всех фото.
    Не привязано к UI, работает без фронтенда.
    """
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



# -----------------------
# Bot: manual entry + duplicate resolve
# -----------------------

class ManualReadingIn(BaseModel):
    chat_id: str
    ym: str
    meter_type: Literal["cold", "hot", "electric"]
    meter_index: int = 1
    value: float

@app.post("/bot/manual-reading")
def bot_manual_reading(payload: ManualReadingIn):
    """Ручной ввод показаний из Telegram.
    Пишем source='manual'. Возвращаем обновлённый bill.
    """
    if not db_ready():
        raise HTTPException(status_code=503, detail="db disabled")

    chat_id = str(payload.chat_id).strip()
    ym = str(payload.ym).strip() or current_ym()
    if not is_ym(ym):
        raise HTTPException(status_code=400, detail="ym must be YYYY-MM")

    meter_type = str(payload.meter_type).strip()
    meter_index = int(payload.meter_index or 1)
    value = float(payload.value)

    if value <= 0:
        raise HTTPException(status_code=400, detail="value must be > 0")

    if meter_type in ("cold", "hot"):
        meter_index = 1
    elif meter_type == "electric":
        meter_index = max(1, min(3, meter_index))
    else:
        raise HTTPException(status_code=400, detail="meter_type invalid")

    with engine.begin() as conn:
        apt = find_apartment_for_chat(conn, chat_id)
        if not apt:
            return {"ok": False, "reason": "not_bound", "ym": ym}

        aid = int(apt["id"])

        conn.execute(
            text("""
                INSERT INTO meter_readings
                    (apartment_id, ym, meter_type, meter_index, value, source, ocr_value)
                VALUES
                    (:aid, :ym, :mt, :mi, :val, 'manual', NULL)
                ON CONFLICT (apartment_id, ym, meter_type, meter_index)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    source = 'manual',
                    ocr_value = NULL,
                    updated_at = now()
            """),
            {"aid": aid, "ym": ym, "mt": meter_type, "mi": meter_index, "val": value},
        )

        bill = _calc_month_bill(conn, aid, ym)

    return {"ok": True, "apartment_id": aid, "ym": ym, "bill": bill}


class DuplicateResolveIn(BaseModel):
    photo_event_id: int
    action: Literal["ok", "repeat"]

@app.post("/bot/duplicate/resolve")
def bot_duplicate_resolve(payload: DuplicateResolveIn):
    """Решение по возможному дублю: ok = оставить, repeat = откатить запись и попросить другое фото."""
    if not db_ready():
        raise HTTPException(status_code=503, detail="db disabled")

    peid = int(payload.photo_event_id)
    action = str(payload.action)

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT id, apartment_id, ym, meter_kind, meter_index, meter_value
                FROM photo_events
                WHERE id=:id
            """),
            {"id": peid},
        ).mappings().first()

        if not row:
            raise HTTPException(status_code=404, detail="photo_event not found")

        aid = row.get("apartment_id")
        ym = row.get("ym") or month_now()
        mt = row.get("meter_kind")
        mi = int(row.get("meter_index") or 1)
        mv = row.get("meter_value")

        if not aid or not mt or mv is None:
            # Нечего откатывать — просто помечаем решение
            conn.execute(
                text("""
                    UPDATE photo_events
                    SET stage=:stage, stage_updated_at=now()
                    WHERE id=:id
                """),
                {"id": peid, "stage": "duplicate_ok" if action == "ok" else "duplicate_repeat"},
            )
            bill = _calc_month_bill(conn, int(aid), str(ym)) if aid else None
            return {"ok": True, "bill": bill}

        if action == "repeat":
            # Откатить запись показаний, сделанную этим событием (самый безопасный вариант: удалить текущую запись)
            conn.execute(
                text("""
                    DELETE FROM meter_readings
                    WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=:mi
                """),
                {"aid": int(aid), "ym": str(ym), "mt": str(mt), "mi": int(mi)},
            )
            conn.execute(
                text("""
                    UPDATE photo_events
                    SET meter_written=false, stage='duplicate_repeat', stage_updated_at=now()
                    WHERE id=:id
                """),
                {"id": peid},
            )
        else:
            conn.execute(
                text("""
                    UPDATE photo_events
                    SET stage='duplicate_ok', stage_updated_at=now()
                    WHERE id=:id
                """),
                {"id": peid},
            )

        bill = _calc_month_bill(conn, int(aid), str(ym))

    return {"ok": True, "bill": bill}





@app.get("/dashboard/apartments")
def dashboard_apartments():
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT
              a.id, a.title, a.address, a.note,
              COALESCE(s.rent_paid, false) as rent_paid,
              COALESCE(s.meters_paid, false) as meters_paid,
              COALESCE(s.meters_photo_cold, false) as meters_photo_cold,
              COALESCE(s.meters_photo_hot, false) as meters_photo_hot,
              COALESCE(s.meters_photo_electric, false) as meters_photo_electric,
              (SELECT max(created_at) FROM photo_events pe WHERE pe.apartment_id = a.id) as last_event_at
            FROM apartments a
            LEFT JOIN apartment_statuses s ON s.apartment_id = a.id
            ORDER BY a.id ASC;
        """)).mappings().all()

    items = []
    for r in rows:
        items.append({
            "id": int(r["id"]),
            "title": r["title"],
            "address": r["address"],
            "note": r["note"],
            "statuses": {
                "rent_paid": bool(r["rent_paid"]),
                "meters_paid": bool(r["meters_paid"]),
                "meters_photo_cold": bool(r["meters_photo_cold"]),
                "meters_photo_hot": bool(r["meters_photo_hot"]),
                "meters_photo_electric": bool(r["meters_photo_electric"]),
            },
            "last_event_at": (r["last_event_at"].isoformat() if r["last_event_at"] else None),
        })

    return {"ok": True, "items": items}


# -----------------------
# Dashboard: meters table (+ ₽)
# -----------------------

@app.get("/dashboard/apartments/{apartment_id}/meters")
def dashboard_apartment_meters(apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    mr.ym,
                    mr.meter_type,
                    mr.meter_index,
                    mr.value,
                    t.cold AS tariff_cold,
                    t.hot AS tariff_hot,
                    COALESCE(t.electric_t1, t.electric) AS tariff_electric_t1,
                    COALESCE(t.electric_t2, t.electric) AS tariff_electric_t2,
                    COALESCE(t.electric_t3, t.electric) AS tariff_electric_t3,
                    t.sewer AS tariff_sewer
                FROM meter_readings mr
                LEFT JOIN tariffs t
                    ON t.month_from = (
                        SELECT MAX(month_from)
                        FROM tariffs
                        WHERE month_from <= mr.ym
                    )
                WHERE mr.apartment_id = :aid
                  AND (
                        (mr.meter_type IN ('cold','hot') AND mr.meter_index = 1)
                     OR (mr.meter_type = 'electric' AND mr.meter_index IN (1,2,3))
                  )
                ORDER BY mr.ym, mr.meter_type, mr.meter_index
            """),
            {"aid": apartment_id},
        ).mappings().all()

    by_month: Dict[str, Any] = {}

    def _mk_kind(tariff_val):
        return {"current": None, "previous": None, "delta": None, "tariff": tariff_val, "rub": None}

    for r in rows:
        ym = r["ym"]
        if ym not in by_month:
            by_month[ym] = {
                "month": ym,
                "kinds": {
                    "cold": _mk_kind(r["tariff_cold"]),
                    "hot": _mk_kind(r["tariff_hot"]),
                    "electric": {
                        "title": "Электро",
                        "t1": _mk_kind(r["tariff_electric_t1"]),
                        "t2": _mk_kind(r["tariff_electric_t2"]),
                        "t3": _mk_kind(r["tariff_electric_t3"]),
                    },
                },
                "sewer": {"delta": None, "tariff": r["tariff_sewer"], "rub": None},
                "total_rub": None,  # NEW: сумма по месяцу
            }

        mt = r["meter_type"]
        mi = int(r["meter_index"] or 1)
        val = float(r["value"]) if r["value"] is not None else None

        if mt == "cold":
            by_month[ym]["kinds"]["cold"]["current"] = val
        elif mt == "hot":
            by_month[ym]["kinds"]["hot"]["current"] = val
        elif mt == "electric":
            if mi == 1:
                by_month[ym]["kinds"]["electric"]["t1"]["current"] = val
            elif mi == 2:
                by_month[ym]["kinds"]["electric"]["t2"]["current"] = val
            elif mi == 3:
                by_month[ym]["kinds"]["electric"]["t3"]["current"] = val

    months = sorted(by_month.keys())

    prev_cold = None
    prev_hot = None
    prev_e1 = None
    prev_e2 = None
    prev_e3 = None

    for ym in months:
        entry = by_month[ym]

        # cold
        cur = entry["kinds"]["cold"]["current"]
        entry["kinds"]["cold"]["previous"] = prev_cold
        if cur is not None and prev_cold is not None:
            d = cur - prev_cold
            entry["kinds"]["cold"]["delta"] = d
            t = entry["kinds"]["cold"]["tariff"]
            if t is not None:
                entry["kinds"]["cold"]["rub"] = float(d) * float(t)
        prev_cold = cur

        # hot
        cur = entry["kinds"]["hot"]["current"]
        entry["kinds"]["hot"]["previous"] = prev_hot
        if cur is not None and prev_hot is not None:
            d = cur - prev_hot
            entry["kinds"]["hot"]["delta"] = d
            t = entry["kinds"]["hot"]["tariff"]
            if t is not None:
                entry["kinds"]["hot"]["rub"] = float(d) * float(t)
        prev_hot = cur

        # electric t1
        cur = entry["kinds"]["electric"]["t1"]["current"]
        entry["kinds"]["electric"]["t1"]["previous"] = prev_e1
        if cur is not None and prev_e1 is not None:
            d = cur - prev_e1
            entry["kinds"]["electric"]["t1"]["delta"] = d
            t = entry["kinds"]["electric"]["t1"]["tariff"]
            if t is not None:
                entry["kinds"]["electric"]["t1"]["rub"] = float(d) * float(t)
        prev_e1 = cur

        # electric t2
        cur = entry["kinds"]["electric"]["t2"]["current"]
        entry["kinds"]["electric"]["t2"]["previous"] = prev_e2
        if cur is not None and prev_e2 is not None:
            d = cur - prev_e2
            entry["kinds"]["electric"]["t2"]["delta"] = d
            t = entry["kinds"]["electric"]["t2"]["tariff"]
            if t is not None:
                entry["kinds"]["electric"]["t2"]["rub"] = float(d) * float(t)
        prev_e2 = cur

        # electric t3
        cur = entry["kinds"]["electric"]["t3"]["current"]
        entry["kinds"]["electric"]["t3"]["previous"] = prev_e3
        if cur is not None and prev_e3 is not None:
            d = cur - prev_e3
            entry["kinds"]["electric"]["t3"]["delta"] = d
            t = entry["kinds"]["electric"]["t3"]["tariff"]
            if t is not None:
                entry["kinds"]["electric"]["t3"]["rub"] = float(d) * float(t)
        prev_e3 = cur

        # sewer
        cold_delta = entry["kinds"]["cold"]["delta"]
        hot_delta = entry["kinds"]["hot"]["delta"]
        if cold_delta is not None and hot_delta is not None:
            sewer_delta = cold_delta + hot_delta
            entry["sewer"]["delta"] = sewer_delta
            if entry["sewer"]["tariff"] is not None:
                entry["sewer"]["rub"] = float(sewer_delta) * float(entry["sewer"]["tariff"])

        # NEW: total_rub (сумма по месяцу)
        rubs = [
            entry["kinds"]["cold"]["rub"],
            entry["kinds"]["hot"]["rub"],
            entry["kinds"]["electric"]["t1"]["rub"],
            entry["kinds"]["electric"]["t2"]["rub"],
            entry["kinds"]["electric"]["t3"]["rub"],
            entry["sewer"]["rub"],
        ]
        if all(x is not None for x in rubs):
            entry["total_rub"] = float(sum(float(x) for x in rubs if x is not None))
        else:
            entry["total_rub"] = None

    return {"apartment_id": apartment_id, "months": list(by_month.values())}


# -----------------------
# Dashboard: edit CURRENT month only
# -----------------------

@app.patch("/dashboard/apartments/{apartment_id}/meters/current")
def patch_current_month_readings(apartment_id: int, payload: MeterCurrentPatch):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    m = month_now()
    data = payload.model_dump(exclude_unset=True) if hasattr(payload, "model_dump") else payload.dict(exclude_unset=True)

    # Map UI keys to (meter_type, meter_index) in DB
    mapping = {
        "cold": ("cold", 1),
        "hot": ("hot", 1),
        "sewer": ("sewer", 1),
        "electric_t1": ("electric", 1),
        "electric_t2": ("electric", 2),
        "electric_t3": ("electric", 3),
    }

    updates = {k: v for k, v in data.items() if v is not None and k in mapping}
    if not updates:
        return {"ok": True, "message": "no changes"}

    with engine.begin() as conn:
        a = conn.execute(text("SELECT id FROM apartments WHERE id=:id"), {"id": apartment_id}).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="apartment not found")

        for key, val in updates.items():
            meter_type, meter_index = mapping[key]
            conn.execute(
                text(
                    """
                    INSERT INTO meter_readings(
                        apartment_id, ym, meter_type, meter_index, value, source, ocr_value
                    )
                    VALUES (
                        :aid, :ym, :meter_type, :meter_index, :val, 'manual', NULL
                    )
                    ON CONFLICT (apartment_id, ym, meter_type, meter_index)
                    DO UPDATE SET
                        value = EXCLUDED.value,
                        source = 'manual',
                        updated_at = now()
                    """
                ),
                {
                    "aid": int(apartment_id),
                    "ym": m,
                    "meter_type": meter_type,
                    "meter_index": int(meter_index),
                    "val": float(val),
                },
            )

    return {"ok": True, "apartment_id": apartment_id, "month": m, "updated": list(updates.keys())}

@app.patch("/dashboard/apartments/{apartment_id}/statuses")
def patch_apartment_statuses(apartment_id: int, payload: StatusPatch):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not data:
        return {"ok": True, "message": "no changes"}

    allowed = {"rent_paid", "meters_paid", "meters_photo_cold", "meters_photo_hot", "meters_photo_electric"}
    for k in data.keys():
        if k not in allowed:
            raise HTTPException(status_code=400, detail=f"invalid field: {k}")

    set_clause = ", ".join([f"{k} = :{k}" for k in data.keys()]) + ", updated_at = now()"
    params = {"aid": apartment_id, **data}

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO apartment_statuses(apartment_id)
            VALUES (:aid)
            ON CONFLICT (apartment_id) DO NOTHING
        """), {"aid": apartment_id})

        conn.execute(text(f"""
            UPDATE apartment_statuses
            SET {set_clause}
            WHERE apartment_id = :aid
        """), params)

    return {"ok": True, "apartment_id": apartment_id, "updated": list(data.keys())}


# -----------------------
# Bot callbacks: paid flags
# -----------------------

@app.post("/bot/apartments/{apartment_id}/months/{ym}/rent-paid")
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


@app.post("/bot/apartments/{apartment_id}/months/{ym}/meters-paid")
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


# -----------------------
# Tariffs: get + upsert
# -----------------------

@app.get("/tariffs")
def get_tariffs():
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT
                month_from,
                cold,
                hot,
                electric,
                COALESCE(electric_t1, electric) AS electric_t1,
                COALESCE(electric_t2, electric) AS electric_t2,
                COALESCE(electric_t3, electric) AS electric_t3,
                sewer,
                created_at
            FROM tariffs
            ORDER BY month_from ASC
        """)).mappings().all()

    return {
        "ok": True,
        "items": [{
            "ym_from": r["month_from"],
            "month_from": r["month_from"],
            "cold": float(r["cold"]),
            "hot": float(r["hot"]),
            "electric": float(r["electric"]),
            "electric_t1": float(r["electric_t1"]),
            "electric_t2": float(r["electric_t2"]),
            "electric_t3": float(r["electric_t3"]),
            "sewer": float(r["sewer"]),
            "created_at": (r["created_at"].isoformat() if r["created_at"] else None),
        } for r in rows]
    }


@app.post("/tariffs")
def upsert_tariff(payload: TariffIn):
    # Accept both month_from and ym_from
    ym_from = (payload.month_from or payload.ym_from or "").strip()
    if not ym_from:
        raise HTTPException(status_code=400, detail="month_from is required")

    # В таблице tariffs.electric NOT NULL, значит базовый тариф должен быть всегда
    if payload.electric is None and payload.electric_t1 is None:
        raise HTTPException(status_code=400, detail="electric or electric_t1 is required")

    electric_base = payload.electric if payload.electric is not None else payload.electric_t1

    # tier-тарифы: если передали — пишем их, иначе будут NULL и в расчетах возьмется base
    e1 = payload.electric_t1 if payload.electric_t1 is not None else electric_base
    e2 = payload.electric_t2
    e3 = payload.electric_t3

    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO tariffs(month_from, cold, hot, electric, electric_t1, electric_t2, electric_t3, sewer, updated_at)
                VALUES(:month_from, :cold, :hot, :electric, :e1, :e2, :e3, :sewer, now())
                ON CONFLICT(month_from) DO UPDATE SET
                  cold=EXCLUDED.cold,
                  hot=EXCLUDED.hot,
                  electric=EXCLUDED.electric,
                  electric_t1=EXCLUDED.electric_t1,
                  electric_t2=EXCLUDED.electric_t2,
                  electric_t3=EXCLUDED.electric_t3,
                  sewer=EXCLUDED.sewer,
                  updated_at=now()
                """
            ),
            {
                "month_from": ym_from,
                "cold": float(payload.cold),
                "hot": float(payload.hot),
                "electric": float(electric_base),
                "e1": float(e1) if e1 is not None else None,
                "e2": float(e2) if e2 is not None else None,
                "e3": float(e3) if e3 is not None else None,
                "sewer": float(payload.sewer),
            },
        )
    return {"ok": True}


@app.get("/admin/ui/apartments/{apartment_id}/history")
def ui_apartment_history(apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="DB not ready")
    ensure_tables()

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT ym, meter_type AS kind, meter_index, value
                FROM meter_readings
                WHERE apartment_id = :apartment_id
                  AND (
                        (meter_type IN ('cold','hot') AND meter_index = 1)
                     OR (meter_type = 'electric' AND meter_index IN (1,2,3))
                  )
                ORDER BY ym ASC, kind ASC, meter_index ASC
            """),
            {"apartment_id": apartment_id},
        ).fetchall()

    by_month: Dict[str, Dict[str, float]] = {}
    for ym, kind, meter_index, value in rows:
        by_month.setdefault(ym, {})
        k = str(kind)
        idx = int(meter_index)
        v = float(value)
        if k == "electric":
            by_month[ym][f"electric_{idx}"] = v
        else:
            by_month[ym][k] = v

    def prev_month(ym: str) -> str:
        y, m = ym.split("-")
        y = int(y)
        m = int(m) - 1
        if m == 0:
            m = 12
            y -= 1
        return f"{y:04d}-{m:02d}"

    history: List[Dict[str, Any]] = []

    for ym in sorted(by_month.keys()):
        cur = by_month.get(ym, {})
        pm = prev_month(ym)
        prev = by_month.get(pm, {})

        cold_cur = cur.get("cold")
        hot_cur = cur.get("hot")

        e1_cur = cur.get("electric_1")
        e2_cur = cur.get("electric_2")
        e3_cur = cur.get("electric_3")

        cold_prev = prev.get("cold")
        hot_prev = prev.get("hot")

        e1_prev = prev.get("electric_1")
        e2_prev = prev.get("electric_2")
        e3_prev = prev.get("electric_3")

        cold_delta = (cold_cur - cold_prev) if (cold_cur is not None and cold_prev is not None) else None
        hot_delta = (hot_cur - hot_prev) if (hot_cur is not None and hot_prev is not None) else None

        e1_delta = (e1_cur - e1_prev) if (e1_cur is not None and e1_prev is not None) else None
        e2_delta = (e2_cur - e2_prev) if (e2_cur is not None and e2_prev is not None) else None
        e3_delta = (e3_cur - e3_prev) if (e3_cur is not None and e3_prev is not None) else None

        sewer_delta = None
        if cold_delta is not None and hot_delta is not None:
            sewer_delta = cold_delta + hot_delta

        history.append({
            "month": ym,
            "meters": {
                "cold": {"title": "ХВС", "current": cold_cur, "previous": cold_prev, "delta": cold_delta},
                "hot": {"title": "ГВС", "current": hot_cur, "previous": hot_prev, "delta": hot_delta},
                "electric": {
                    "title": "Электро",
                    "t1": {"title": "T1", "current": e1_cur, "previous": e1_prev, "delta": e1_delta},
                    "t2": {"title": "T2", "current": e2_cur, "previous": e2_prev, "delta": e2_delta},
                    "t3": {"title": "T3", "current": e3_cur, "previous": e3_prev, "delta": e3_delta},
                },
                "sewer": {"title": "Водоотведение", "current": None, "previous": None, "delta": sewer_delta},
            }
        })

    return {"apartment_id": apartment_id, "history": history}


@app.post("/admin/ui/apartments/{apartment_id}/months/{ym}/electric-extra/accept")
def admin_accept_electric_extra(apartment_id: int, ym: str):
    """Админ подтверждает, что “лишний” столбец электро нужно принять: увеличиваем electric_expected на +1 (макс 3) и снимаем блокировку."""
    ym = (ym or "").strip()
    if not is_ym(ym):
        raise HTTPException(status_code=400, detail="ym must be YYYY-MM")

    with engine.begin() as conn:
        state = _get_month_extra_state(conn, apartment_id, ym)
        if not state.get("pending"):
            return {"ok": True, "changed": False, "reason": "no_pending"}

        snapshot = state.get("snapshot")
        if snapshot is None:
            snapshot = _get_apartment_electric_expected(conn, apartment_id)

        new_expected = min(3, int(snapshot) + 1)

        conn.execute(
            text("UPDATE apartments SET electric_expected=:n WHERE id=:id"),
            {"id": apartment_id, "n": new_expected},
        )
        _set_month_extra_state(conn, apartment_id, ym, False, None)

    return {"ok": True, "changed": True, "electric_expected": new_expected}


@app.post("/admin/ui/apartments/{apartment_id}/months/{ym}/electric-extra/reject")
def admin_reject_electric_extra(apartment_id: int, ym: str):
    """Админ отклоняет “лишний” столбец электро: удаляем записи idx > snapshot_expected и снимаем блокировку."""
    ym = (ym or "").strip()
    if not is_ym(ym):
        raise HTTPException(status_code=400, detail="ym must be YYYY-MM")

    with engine.begin() as conn:
        state = _get_month_extra_state(conn, apartment_id, ym)
        snapshot = state.get("snapshot")
        if snapshot is None:
            snapshot = _get_apartment_electric_expected(conn, apartment_id)

        snapshot = int(snapshot)
        if snapshot < 1:
            snapshot = 1
        if snapshot > 3:
            snapshot = 3

        conn.execute(
            text(
                "DELETE FROM meter_readings "
                "WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index > :snap AND meter_index BETWEEN 1 AND 3"
            ),
            {"aid": apartment_id, "ym": ym, "snap": snapshot},
        )
        _set_month_extra_state(conn, apartment_id, ym, False, None)

    return {"ok": True, "electric_expected_snapshot": snapshot}


@app.post("/admin/ui/apartments/{apartment_id}/meters")
def add_meter_reading(apartment_id: int, payload: dict = Body(...)):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    ym = payload.get("month")
    meter_type = payload.get("kind")
    value = payload.get("value")

    meter_index = payload.get("meter_index", 1)
    try:
        meter_index = int(meter_index)
    except Exception:
        meter_index = 1

    meter_index = max(1, min(3, meter_index))

    if meter_type in {"cold", "hot"}:
        meter_index = 1

    if not ym or not meter_type or value is None:
        raise HTTPException(status_code=400, detail="month, kind and value are required")

    if meter_type not in {"cold", "hot", "electric"}:
        raise HTTPException(status_code=400, detail="kind must be one of: cold, hot, electric")

    try:
        value = float(value)
    except Exception:
        raise HTTPException(status_code=400, detail="value must be a number")

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO meter_readings(
                    apartment_id,
                    ym,
                    meter_type,
                    meter_index,
                    value,
                    source
                )
                VALUES (
                    :aid,
                    :ym,
                    :meter_type,
                    :meter_index,
                    :value,
                    'manual'
                )
                ON CONFLICT (apartment_id, ym, meter_type, meter_index)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    source = 'manual',
                    updated_at = now()
            """),
            {
                "aid": int(apartment_id),
                "ym": ym,
                "meter_type": meter_type,
                "meter_index": int(meter_index),
                "value": value,
            },
        )

    return {
        "status": "ok",
        "apartment_id": apartment_id,
        "month": ym,
        "kind": meter_type,
        "meter_index": int(meter_index),
        "value": value,
    }