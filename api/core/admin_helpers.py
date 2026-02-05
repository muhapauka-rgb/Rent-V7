from datetime import datetime
from typing import Optional, List

from sqlalchemy import text

from core.config import engine
from core.db import db_ready
from core.schemas import UIStatuses, UIStatusesPatch


def norm_phone(p: str) -> str:
    """
    Нормализация телефона для поиска/хранения.
    Приводим к канону РФ: 11 цифр, начинается с "7".
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
    """Генерация вариантов телефона для поиска."""
    p = norm_phone(phone)
    if not p:
        return []
    out = {p}
    # возможные варианты без + и с 8
    if p.startswith("7") and len(p) == 11:
        out.add("8" + p[1:])
        out.add(p[1:])
    return list(out)


def find_apartment_by_chat(chat_id: str) -> int | None:
    if not db_ready():
        return None
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT apartment_id
                FROM chat_bindings
                WHERE chat_id=:cid AND is_active=true
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
            """),
            {"cid": str(chat_id)},
        ).fetchone()
        return int(row[0]) if row else None


def find_apartment_by_contact(telegram_username: str | None, phone: str | None) -> int | None:
    """
    Поиск квартиры по контактам (telegram username / phone).
    """
    if not db_ready():
        return None
    candidates = []
    if telegram_username:
        u = telegram_username.strip().lstrip("@").lower()
        if u:
            candidates.append(("telegram", u))
    if phone:
        for p in _phone_variants(phone):
            candidates.append(("phone", p))

    if not candidates:
        return None

    with engine.begin() as conn:
        for kind, value in candidates:
            row = conn.execute(
                text("""
                    SELECT apartment_id
                    FROM apartment_contacts
                    WHERE kind=:kind AND value=:val AND is_active=true
                    ORDER BY created_at DESC
                    LIMIT 1
                """),
                {"kind": kind, "val": value},
            ).fetchone()
            if row:
                return int(row[0])
    return None


def bind_chat(chat_id: str, apartment_id: int) -> None:
    if not db_ready():
        return
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO chat_bindings(chat_id, apartment_id, is_active, updated_at, created_at)
                VALUES (:cid, :aid, true, now(), now())
                ON CONFLICT (chat_id) DO UPDATE
                SET apartment_id = EXCLUDED.apartment_id,
                    is_active = true,
                    updated_at = now()
            """),
            {"cid": str(chat_id), "aid": int(apartment_id)},
        )


def current_ym() -> str:
    return datetime.now().strftime("%Y-%m")


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

        # 2) если значение есть — выключаем остальные активные контакты этого типа
        # у квартиры, но не трогаем текущее значение (если оно уже есть)
        conn.execute(
            text("""
                UPDATE apartment_contacts
                SET is_active=false
                WHERE apartment_id=:aid AND kind=:kind AND is_active=true
                  AND value <> :value
            """),
            {"aid": int(apartment_id), "kind": kind, "value": v},
        )

        # 3) пытаемся переиспользовать существующую запись (в БД может быть UNIQUE(kind, value))
        existing = conn.execute(
            text("""
                SELECT id, apartment_id
                FROM apartment_contacts
                WHERE kind=:kind AND value=:value
                LIMIT 1
            """),
            {"kind": kind, "value": v},
        ).fetchone()

        if existing:
            conn.execute(
                text("""
                    UPDATE apartment_contacts
                    SET apartment_id=:aid, is_active=true
                    WHERE id=:id
                """),
                {"aid": int(apartment_id), "id": int(existing[0])},
            )
            return

        # 4) если записи нет — добавляем новую активную
        conn.execute(
            text("""
                INSERT INTO apartment_contacts(apartment_id, kind, value, is_active, created_at)
                VALUES (:aid, :kind, :value, true, now())
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
                VALUES (:aid, :ym, :rent_paid, :meters_photo, :meters_paid)
                ON CONFLICT (apartment_id, ym) DO UPDATE SET
                  rent_paid=EXCLUDED.rent_paid,
                  meters_photo=EXCLUDED.meters_photo,
                  meters_paid=EXCLUDED.meters_paid,
                  updated_at=now()
            """),
            {
                "aid": int(apartment_id),
                "ym": str(ym),
                "rent_paid": bool(new_rent),
                "meters_photo": bool(new_photo),
                "meters_paid": bool(new_paid),
            },
        )

    return UIStatuses(rent_paid=new_rent, meters_photo=new_photo, meters_paid=new_paid)


def update_apartment_statuses(apartment_id: int, data: dict) -> List[str]:
    """Low-level update for apartment_statuses table."""
    if not db_ready():
        return []
    if not data:
        return []

    allowed = {"rent_paid", "meters_paid", "meters_photo_cold", "meters_photo_hot", "meters_photo_electric"}
    for k in data.keys():
        if k not in allowed:
            raise ValueError(f"invalid field: {k}")

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

    return list(data.keys())


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


def _normalize_serial(value: str | None) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    out = []
    for ch in s:
        if ch.isdigit() or ch == "-":
            out.append(ch)
    return "".join(out)
