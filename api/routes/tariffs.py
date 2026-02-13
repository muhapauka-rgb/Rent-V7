from fastapi import APIRouter, HTTPException
from sqlalchemy import text
import re

from core.config import engine
from core.db import db_ready, ensure_tables
from core.schemas import TariffIn

router = APIRouter()


def _normalize_ym_any(v: str) -> str | None:
    s = str(v or "").strip()
    if not s:
        return None
    s = s.replace("/", "-").replace(".", "-").replace("_", "-")

    m = re.fullmatch(r"(\d{4})-(\d{1,2})", s)
    if m:
        y = int(m.group(1))
        mm = int(m.group(2))
        if 1900 <= y <= 2100 and 1 <= mm <= 12:
            return f"{y:04d}-{mm:02d}"

    m = re.fullmatch(r"(\d{1,2})-(\d{4})", s)
    if m:
        mm = int(m.group(1))
        y = int(m.group(2))
        if 1900 <= y <= 2100 and 1 <= mm <= 12:
            return f"{y:04d}-{mm:02d}"

    m = re.fullmatch(r"(\d{4})(\d{2})", s)
    if m:
        y = int(m.group(1))
        mm = int(m.group(2))
        if 1900 <= y <= 2100 and 1 <= mm <= 12:
            return f"{y:04d}-{mm:02d}"

    nums = re.findall(r"\d+", s)
    if len(nums) >= 2:
        y = None
        mm = None
        for n in nums:
            if len(n) == 4:
                y_ = int(n)
                if 1900 <= y_ <= 2100:
                    y = y_
                    break
        if y is None and len(nums[-1]) == 2:
            y = 2000 + int(nums[-1])
        for n in nums:
            m_ = int(n)
            if 1 <= m_ <= 12:
                mm = m_
                break
        if y is not None and mm is not None:
            return f"{y:04d}-{mm:02d}"
    return None


@router.get("/tariffs")
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
                created_at,
                updated_at
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
            "updated_at": (r["updated_at"].isoformat() if r["updated_at"] else None),
        } for r in rows]
    }


@router.post("/tariffs")
def upsert_tariff(payload: TariffIn):
    # Accept both month_from and ym_from
    ym_from = _normalize_ym_any((getattr(payload, "month_from", None) or payload.ym_from or "").strip())
    if not ym_from:
        raise HTTPException(status_code=400, detail="month_from is required (any common month format is allowed)")

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
