import threading
from sqlalchemy import text

from core.config import DATABASE_URL, engine, logger

# --- schema init guard (prevents deadlocks on concurrent requests) ---
_SCHEMA_INIT_LOCK = threading.Lock()
_SCHEMA_INIT_DONE = False
_SCHEMA_ADVISORY_LOCK_KEY = 987654321  # any stable 64-bit int


def db_ready() -> bool:
    return engine is not None and bool(DATABASE_URL)


def ensure_tables() -> None:
    """Create/migrate DB schema once per process.

    IMPORTANT: this function can be called from many endpoints. We guard it to avoid
    concurrent DDL (ALTER TABLE / CREATE INDEX) which can deadlock in Postgres.
    """
    if not db_ready():
        return
    global _SCHEMA_INIT_DONE
    if _SCHEMA_INIT_DONE:
        return
    with _SCHEMA_INIT_LOCK:
        if _SCHEMA_INIT_DONE:
            return
        # DDL can deadlock if multiple requests hit it concurrently. Retry a few times.
        for attempt in range(1, 6):
            try:
                with engine.begin() as conn:
                    # Keep waits bounded (Postgres only).
                    try:
                        conn.execute(text("SET LOCAL lock_timeout = '3s'"))
                        conn.execute(text("SET LOCAL statement_timeout = '30s'"))
                    except Exception:
                        pass

                    # Cross-process lock (Postgres) to prevent concurrent schema changes
                    # from multiple workers/containers.
                    _use_unlock = False
                    try:
                        conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _SCHEMA_ADVISORY_LOCK_KEY})
                        _use_unlock = True
                    except Exception:
                        _use_unlock = False

                    # ---- original schema DDL (kept as-is) ----
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
                            sewer NUMERIC(14,3) NOT NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        );
                    """))
                    conn.execute(text("ALTER TABLE tariffs ADD COLUMN IF NOT EXISTS sewer NUMERIC(14,3) NULL;"))
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
                    conn.execute(text("ALTER TABLE apartment_month_statuses ADD COLUMN IF NOT EXISTS bill_pending JSONB NULL"))
                    conn.execute(text("ALTER TABLE apartment_month_statuses ADD COLUMN IF NOT EXISTS bill_last_json JSONB NULL"))
                    conn.execute(text("ALTER TABLE apartment_month_statuses ADD COLUMN IF NOT EXISTS bill_approved_at TIMESTAMPTZ NULL"))
                    conn.execute(text("ALTER TABLE apartment_month_statuses ADD COLUMN IF NOT EXISTS bill_sent_at TIMESTAMPTZ NULL"))
                    conn.execute(text("ALTER TABLE apartment_month_statuses ADD COLUMN IF NOT EXISTS bill_sent_total NUMERIC(14,2) NULL"))

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
                            ym TEXT NULL,
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
                            diag_json JSONB NULL
                        );
                    """))

                    # --- meter_review_flags (bot/user reports wrong reading) ---
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS meter_review_flags (
                            id BIGSERIAL PRIMARY KEY,
                            apartment_id BIGINT NOT NULL REFERENCES apartments(id) ON DELETE CASCADE,
                            ym TEXT NOT NULL,
                            meter_type TEXT NOT NULL,
                            meter_index INTEGER NOT NULL DEFAULT 1,
                            status TEXT NOT NULL DEFAULT 'open', -- open | resolved
                            reason TEXT NOT NULL DEFAULT 'user_report_wrong_ocr',
                            comment TEXT NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            resolved_at TIMESTAMPTZ NULL,
                            resolved_by TEXT NULL
                        );
                    """))
                    try:
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_meter_review_flags_apartment_ym ON meter_review_flags(apartment_id, ym)"))
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_meter_review_flags_status ON meter_review_flags(status)"))
                    except Exception:
                        pass

                    # --- notifications (web bell) ---
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS notifications (
                            id BIGSERIAL PRIMARY KEY,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            read_at TIMESTAMPTZ NULL,
                            status TEXT NOT NULL DEFAULT 'unread', -- unread | read
                            chat_id TEXT NULL,
                            telegram_username TEXT NULL,
                            apartment_id BIGINT NULL REFERENCES apartments(id) ON DELETE SET NULL,
                            type TEXT NOT NULL DEFAULT 'user_message',
                            message TEXT NOT NULL,
                            related JSONB NULL
                        );
                    """))
                    try:
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status)"))
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_notifications_apartment_id ON notifications(apartment_id)"))
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications(created_at DESC)"))
                    except Exception:
                        pass

                    # Postgres-only indexes (no-op on other DBs).
                    try:
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_status ON photo_events(status)"))
                        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_photo_events_apartment_id ON photo_events(apartment_id)"))
                    except Exception:
                        pass

                    if _use_unlock:
                        try:
                            conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _SCHEMA_ADVISORY_LOCK_KEY})
                        except Exception:
                            pass

                _SCHEMA_INIT_DONE = True
                break
            except Exception as e:
                if attempt >= 5:
                    logger.exception("ensure_tables failed after retries")
                else:
                    try:
                        logger.warning("ensure_tables retry %s after error: %s", attempt, str(e))
                    except Exception:
                        pass
                # small backoff
                import time as _t
                _t.sleep(0.2 * attempt)
