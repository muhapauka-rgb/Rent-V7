from typing import Dict, Any
from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from core.config import engine, logger
from core.db import db_ready, ensure_tables
from core.billing import (
    month_now,
    _calc_month_bill,
    _get_month_bill_state,
    _set_month_bill_state,
    _get_active_chat_id,
    _same_total,
    _get_apartment_electric_expected,
)
from core.meters import _auto_fill_t3_from_t1_t2_if_needed
from core.admin_helpers import update_apartment_statuses
from core.integrations import _tg_send_message
from core.schemas import MeterCurrentPatch, UIStatusesPatch

router = APIRouter()

StatusPatch = UIStatusesPatch


@router.get("/dashboard/apartments")
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

@router.get("/dashboard/apartments/{apartment_id}/meters")
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
                    COALESCE(t.cold, 0) AS cold_tariff,
                    COALESCE(t.hot, 0) AS hot_tariff,
                    COALESCE(t.sewer, 0) AS sewer_tariff,
                    COALESCE(t.electric_t1, t.electric, 0) AS e1_tariff,
                    COALESCE(t.electric_t2, t.electric, 0) AS e2_tariff,
                    COALESCE(t.electric_t3, t.electric, 0) AS e3_tariff
                FROM meter_readings mr
                LEFT JOIN (
                    SELECT DISTINCT ON (month_from)
                        month_from, cold, hot, sewer, electric, electric_t1, electric_t2, electric_t3
                    FROM tariffs
                    ORDER BY month_from DESC
                ) t ON t.month_from <= mr.ym
                WHERE mr.apartment_id = :aid
                  AND mr.meter_type IN ('cold','hot','electric','sewer')
                ORDER BY mr.ym ASC, mr.meter_type ASC, mr.meter_index ASC
            """),
            {"aid": int(apartment_id)},
        ).mappings().all()

    # group by month and type
    by_month: Dict[str, Any] = {}
    for r in rows:
        ym = r["ym"]
        mt = r["meter_type"]
        mi = int(r["meter_index"] or 1)
        val = float(r["value"]) if r["value"] is not None else None

        if ym not in by_month:
            by_month[ym] = {
                "month": ym,
                "kinds": {
                    "cold": {"title": "ХВС", "current": None, "previous": None, "delta": None, "tariff": float(r["cold_tariff"] or 0), "rub": None},
                    "hot": {"title": "ГВС", "current": None, "previous": None, "delta": None, "tariff": float(r["hot_tariff"] or 0), "rub": None},
                    "electric": {
                        "title": "Электро",
                        "t1": {"title": "T1", "current": None, "previous": None, "delta": None, "tariff": float(r["e1_tariff"] or 0), "rub": None},
                        "t2": {"title": "T2", "current": None, "previous": None, "delta": None, "tariff": float(r["e2_tariff"] or 0), "rub": None},
                        "t3": {"title": "T3", "current": None, "previous": None, "delta": None, "tariff": float(r["e3_tariff"] or 0), "rub": None, "derived": False},
                    },
                    "sewer": {"title": "Водоотведение", "current": None, "previous": None, "delta": None, "tariff": float(r["sewer_tariff"] or 0), "rub": None},
                },
            }

        entry = by_month[ym]
        if mt == "cold":
            entry["kinds"]["cold"]["current"] = val
        elif mt == "hot":
            entry["kinds"]["hot"]["current"] = val
        elif mt == "sewer":
            entry["kinds"]["sewer"]["current"] = val
        elif mt == "electric":
            if mi == 1:
                entry["kinds"]["electric"]["t1"]["current"] = val
            elif mi == 2:
                entry["kinds"]["electric"]["t2"]["current"] = val
            elif mi == 3:
                entry["kinds"]["electric"]["t3"]["current"] = val

    # compute deltas & rubles
    prev_cold = prev_hot = prev_e1 = prev_e2 = prev_e3 = None
    history = []
    for ym in sorted(by_month.keys()):
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
            entry["kinds"]["sewer"]["delta"] = sewer_delta
            if entry["kinds"]["sewer"]["tariff"] is not None:
                entry["kinds"]["sewer"]["rub"] = float(sewer_delta) * float(entry["kinds"]["sewer"]["tariff"])

        # total_rub (month sum)
        rubs = [
            entry["kinds"]["cold"]["rub"],
            entry["kinds"]["hot"]["rub"],
            entry["kinds"]["electric"]["t1"]["rub"],
            entry["kinds"]["electric"]["t2"]["rub"],
            entry["kinds"]["electric"]["t3"]["rub"],
            entry["kinds"]["sewer"]["rub"],
        ]
        if all(x is not None for x in rubs):
            entry["total_rub"] = float(sum(float(x) for x in rubs if x is not None))
        else:
            entry["total_rub"] = None

        history.append(entry)

    return {"apartment_id": apartment_id, "months": list(by_month.values())}


# -----------------------
# Dashboard: edit CURRENT month only
# -----------------------

@router.patch("/dashboard/apartments/{apartment_id}/meters/current")
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

        # For expected=3: after manual T1/T2 edit set T3=T1+T2 if T3 is not OCR
        try:
            if any(k in ("electric_t1", "electric_t2") for k in updates.keys()):
                expected = _get_apartment_electric_expected(conn, int(apartment_id))
                if int(expected) == 3:
                    _auto_fill_t3_from_t1_t2_if_needed(conn, int(apartment_id), str(m))
        except Exception:
            pass

    # --- after web edit: recompute bill and auto-send if allowed ---
    try:
        with engine.begin() as conn:
            bill = _calc_month_bill(conn, apartment_id=int(apartment_id), ym=str(m))
            if (bill.get("reason") == "ok") and (bill.get("total_rub") is not None):
                st = _get_month_bill_state(conn, int(apartment_id), str(m))
                if _same_total(st.get("sent_total"), bill.get("total_rub")):
                    logger.info(
                        "tg_send skip ctx=dashboard_current_same_total apartment_id=%s ym=%s total=%s",
                        int(apartment_id),
                        str(m),
                        bill.get("total_rub"),
                    )
                else:
                    chat_id = _get_active_chat_id(conn, int(apartment_id))
                    if chat_id:
                        msg = f"Сумма оплаты по счётчикам за {m}: {float(bill.get('total_rub')):.2f} ₽"
                        if _tg_send_message(str(chat_id), msg):
                            _set_month_bill_state(conn, int(apartment_id), str(m), sent_at=True, sent_total=bill.get("total_rub"))
                    else:
                        logger.info(
                            "tg_send skip ctx=dashboard_current_no_chat apartment_id=%s ym=%s",
                            int(apartment_id),
                            str(m),
                        )
            else:
                logger.info(
                    "tg_send skip ctx=dashboard_current apartment_id=%s ym=%s reason=%s total=%s",
                    int(apartment_id),
                    str(m),
                    str(bill.get("reason")),
                    bill.get("total_rub"),
                )
    except Exception:
        pass

    return {"ok": True, "apartment_id": apartment_id, "month": m, "updated": list(updates.keys())}


@router.patch("/dashboard/apartments/{apartment_id}/statuses")
def patch_apartment_statuses(apartment_id: int, payload: StatusPatch):
    if not db_ready():
        raise HTTPException(status_code=500, detail="DB is not configured")
    ensure_tables()

    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not data:
        return {"ok": True, "message": "no changes"}

    try:
        updated = update_apartment_statuses(apartment_id, data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"ok": True, "apartment_id": apartment_id, "updated": updated}
