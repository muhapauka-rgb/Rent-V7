from typing import Optional, Dict, Any, List
from datetime import date, timedelta
import calendar
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy import text
import threading
import subprocess
import mimetypes
import re

from core.config import engine
from core.db import db_ready, ensure_tables
from core.admin_helpers import (
    current_ym,
    _get_active_contact,
    _set_contact,
    _get_month_statuses,
    _upsert_month_statuses,
    _normalize_serial,
)
from core.billing import (
    _calc_month_bill,
    _get_month_bill_state,
    _set_month_bill_state,
    _get_active_chat_id,
    _get_apartment_electric_expected,
    _get_month_extra_state,
    _set_month_extra_state,
    is_ym,
)
from core.meters import _add_meter_reading_db, _write_electric_overwrite_then_sort, _auto_fill_t3_from_t1_t2_if_needed, _normalize_water_after_manual
from core.integrations import _tg_send_message, ydisk_get
from core.schemas import (
    UIContacts,
    UIStatuses,
    UIApartmentItem,
    UIApartmentCreate,
    UIApartmentPatch,
    UIStatusesPatch,
    BillApproveIn,
)
from core.learning import capture_training_sample

router = APIRouter()

StatusPatch = UIStatusesPatch


def _normalize_ym_any(v: Any) -> Optional[str]:
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
            if len(n) == 4 and y is None:
                y = int(n)
                break
        if y is None and len(nums[-1]) == 2:
            y2 = int(nums[-1])
            y = 2000 + y2
        for n in nums:
            iv = int(n)
            if 1 <= iv <= 12:
                mm = iv
                break
        if y is not None and mm is not None and 1900 <= y <= 2100:
            return f"{y:04d}-{mm:02d}"

    return None


def _normalize_date_any(v: Any) -> Optional[str]:
    s = str(v or "").strip()
    if not s:
        return None
    s = s.replace("/", "-").replace(".", "-").replace("_", "-")
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mm, dd).isoformat()
        except Exception:
            return None
    m = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", s)
    if m:
        dd, mm, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mm, dd).isoformat()
        except Exception:
            return None
    return None


def _due_day_from_tenant_since(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, date):
        return int(v.day)
    s = _normalize_date_any(v)
    if not s:
        return None
    try:
        return int(s.split("-")[2])
    except Exception:
        return None


def _is_rent_overdue_for_month(ym: str, due_day: int, today: date) -> bool:
    if not is_ym(ym):
        return False
    y = int(ym[:4])
    m = int(ym[5:7])
    last = calendar.monthrange(y, m)[1]
    day = max(1, min(int(due_day), int(last)))
    due_date = date(y, m, day)
    # one-day grace after due date
    return today > (due_date + timedelta(days=1))


def _to_nullable_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, str):
        t = v.strip().replace(",", ".")
        if t == "":
            return None
        v = t
    try:
        return float(v)
    except Exception:
        raise HTTPException(status_code=400, detail=f"invalid numeric value: {v!r}")


def _ym_to_index(ym: str) -> Optional[int]:
    if not is_ym(ym):
        return None
    return int(ym[:4]) * 12 + int(ym[5:7]) - 1


def _is_cycle_start_month(ym: str, anchor_ym: Optional[str], cycle_months: int) -> bool:
    yi = _ym_to_index(ym)
    ai = _ym_to_index(anchor_ym or "")
    if yi is None:
        return False
    if ai is None:
        return True
    c = max(2, int(cycle_months or 3))
    return (yi - ai) % c == 0


def _start_ocr_dataset_job(force: bool = True) -> None:
    cmd = [
        "python",
        "/app/scripts/build_ocr_dataset.py",
        "--limit",
        "2000",
        "--rate",
        "0.5",
        "--ydisk-root",
        "ocr-datasets",
        "--keep-months",
        "3",
    ]
    if force:
        cmd.append("--force")

    def _run():
        try:
            subprocess.run(cmd, check=False)
        except Exception:
            pass

    threading.Thread(target=_run, daemon=True).start()


@router.get("/admin/ui/apartments")
def ui_list_apartments(ym: Optional[str] = None):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = (ym or current_ym()).strip()

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, title, address, tenant_name, note, ls_account, electric_expected, cold_serial, hot_serial, tenant_since, rent_monthly,
                       utilities_mode, utilities_fixed_monthly, utilities_advance_amount, utilities_advance_cycle_months,
                       utilities_advance_anchor_ym, utilities_show_actual_to_tenant
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
        has_active_chat = False
        try:
            with engine.begin() as conn:
                has_active_chat = bool(_get_active_chat_id(conn, aid))
        except Exception:
            has_active_chat = False
        all_photos_received = False
        try:
            with engine.begin() as conn:
                bill = _calc_month_bill(conn, apartment_id=int(aid), ym=str(ym_))
                all_photos_received = bool(bill.get("is_complete_photos"))
        except Exception:
            all_photos_received = False
        items.append(
            UIApartmentItem(
                id=aid,
                title=r[1],
                address=r[2],
                tenant_name=r[3],
                note=r[4],
                ls_account=r[5],
                electric_expected=int(r[6]) if r[6] is not None else 3,
                cold_serial=r[7],
                hot_serial=r[8],
                tenant_since=str(r[9]) if len(r) > 9 and r[9] is not None else None,
                rent_monthly=float(r[10]) if len(r) > 10 and r[10] is not None else 0.0,
                utilities_mode=str(r[11] or "by_actual_monthly"),
                utilities_fixed_monthly=float(r[12]) if len(r) > 12 and r[12] is not None else None,
                utilities_advance_amount=float(r[13]) if len(r) > 13 and r[13] is not None else None,
                utilities_advance_cycle_months=int(r[14]) if len(r) > 14 and r[14] is not None else 3,
                utilities_advance_anchor_ym=str(r[15]) if len(r) > 15 and r[15] is not None else None,
                utilities_show_actual_to_tenant=bool(r[16]) if len(r) > 16 and r[16] is not None else False,
                has_active_chat=has_active_chat,
                contacts=UIContacts(phone=phone, telegram=telegram),
                statuses=UIStatuses(
                    rent_paid=bool(getattr(statuses, "rent_paid", False)),
                    meters_photo=bool(getattr(statuses, "meters_photo", False)),
                    meters_paid=bool(getattr(statuses, "meters_paid", False)),
                    all_photos_received=bool(all_photos_received),
                ),
            ).model_dump()
        )

    return {"ok": True, "ym": ym_, "items": items}


@router.post("/admin/ui/rent-reminders/check")
def ui_check_rent_reminders():
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = current_ym().strip()
    today = date.today()
    sent: List[Dict[str, Any]] = []

    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT id, title, tenant_since, rent_monthly FROM apartments ORDER BY id DESC")
        ).mappings().all()

        for r in rows:
            aid = int(r["id"])
            rent = float(r["rent_monthly"] or 0)
            if rent <= 0:
                continue

            due_day = _due_day_from_tenant_since(r["tenant_since"])
            if due_day is None:
                continue
            if not _is_rent_overdue_for_month(ym_, int(due_day), today):
                continue

            status_row = conn.execute(
                text(
                    """
                    SELECT COALESCE(rent_paid, false) AS rent_paid, rent_reminder_sent_at
                    FROM apartment_month_statuses
                    WHERE apartment_id=:aid AND ym=:ym
                    """
                ),
                {"aid": aid, "ym": ym_},
            ).mappings().first()
            if status_row and bool(status_row["rent_paid"]):
                continue
            if status_row and status_row["rent_reminder_sent_at"] is not None:
                continue

            chat_id = _get_active_chat_id(conn, aid)
            if not chat_id:
                continue

            msg = f"Добрый день, необходимо провести оплату арендной платы в размере {rent:.2f} руб"
            ok = _tg_send_message(str(chat_id), msg)
            if not ok:
                continue

            conn.execute(
                text(
                    """
                    INSERT INTO apartment_month_statuses (apartment_id, ym, rent_paid, meters_photo, meters_paid, rent_reminder_sent_at, updated_at, created_at)
                    VALUES (:aid, :ym, false, false, false, now(), now(), now())
                    ON CONFLICT (apartment_id, ym)
                    DO UPDATE SET rent_reminder_sent_at=now(), updated_at=now()
                    """
                ),
                {"aid": aid, "ym": ym_},
            )
            sent.append({"apartment_id": aid, "title": r["title"], "ym": ym_})

    return {"ok": True, "ym": ym_, "sent_count": len(sent), "sent": sent}


@router.post("/admin/ui/apartments")
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


@router.patch("/admin/ui/apartments/{apartment_id}")
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
        params["address"] = body.address.strip() or None
    if body.tenant_name is not None:
        sets.append("tenant_name=:tenant_name")
        params["tenant_name"] = body.tenant_name.strip() or None
    if body.note is not None:
        sets.append("note=:note")
        params["note"] = body.note.strip() or None
    if body.ls_account is not None:
        sets.append("ls_account=:ls_account")
        params["ls_account"] = body.ls_account.strip() or None
    if body.electric_expected is not None:
        sets.append("electric_expected=:electric_expected")
        params["electric_expected"] = int(body.electric_expected)
    if body.rent_monthly is not None:
        sets.append("rent_monthly=:rent_monthly")
        params["rent_monthly"] = float(body.rent_monthly)
    if body.tenant_since is not None:
        norm_date = _normalize_date_any(body.tenant_since)
        if body.tenant_since and not norm_date:
            raise HTTPException(status_code=400, detail="invalid_tenant_since")
        sets.append("tenant_since=:tenant_since")
        params["tenant_since"] = norm_date
    if body.utilities_mode is not None:
        mode = str(body.utilities_mode).strip()
        if mode not in {"by_actual_monthly", "fixed_monthly", "quarterly_advance"}:
            raise HTTPException(status_code=400, detail="invalid_utilities_mode")
        sets.append("utilities_mode=:utilities_mode")
        params["utilities_mode"] = mode
    if body.utilities_fixed_monthly is not None:
        fixed = float(body.utilities_fixed_monthly)
        if fixed < 0:
            raise HTTPException(status_code=400, detail="invalid_utilities_fixed_monthly")
        sets.append("utilities_fixed_monthly=:utilities_fixed_monthly")
        params["utilities_fixed_monthly"] = fixed
    if body.utilities_advance_amount is not None:
        adv = float(body.utilities_advance_amount)
        if adv < 0:
            raise HTTPException(status_code=400, detail="invalid_utilities_advance_amount")
        sets.append("utilities_advance_amount=:utilities_advance_amount")
        params["utilities_advance_amount"] = adv
    if body.utilities_advance_cycle_months is not None:
        cycle = int(body.utilities_advance_cycle_months)
        if cycle < 2 or cycle > 24:
            raise HTTPException(status_code=400, detail="invalid_utilities_advance_cycle_months")
        sets.append("utilities_advance_cycle_months=:utilities_advance_cycle_months")
        params["utilities_advance_cycle_months"] = cycle
    if body.utilities_advance_anchor_ym is not None:
        norm_ym = _normalize_ym_any(body.utilities_advance_anchor_ym)
        if body.utilities_advance_anchor_ym and not norm_ym:
            raise HTTPException(status_code=400, detail="invalid_utilities_advance_anchor_ym")
        sets.append("utilities_advance_anchor_ym=:utilities_advance_anchor_ym")
        params["utilities_advance_anchor_ym"] = norm_ym
    if body.utilities_show_actual_to_tenant is not None:
        sets.append("utilities_show_actual_to_tenant=:utilities_show_actual_to_tenant")
        params["utilities_show_actual_to_tenant"] = bool(body.utilities_show_actual_to_tenant)

    body_data = body.model_dump(exclude_unset=True)
    has_phone = "phone" in body_data
    has_telegram = "telegram" in body_data
    has_cold_serial = "cold_serial" in body_data
    has_hot_serial = "hot_serial" in body_data

    if not sets and (not has_phone and not has_telegram) and (not has_cold_serial and not has_hot_serial):
        return {"ok": True, "updated": []}

    with engine.begin() as conn:
        a = conn.execute(text("SELECT id FROM apartments WHERE id=:id"), {"id": apartment_id}).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="apartment_not_found")

        if sets:
            conn.execute(
                text(f"""
                    UPDATE apartments
                    SET {', '.join(sets)}
                    WHERE id=:id
                """),
                params,
            )

    if has_phone:
        _set_contact(apartment_id, "phone", body.phone)
    if has_telegram:
        _set_contact(apartment_id, "telegram", body.telegram)
    if has_cold_serial:
        raw = body.cold_serial
        norm = _normalize_serial(raw) or None
        if raw is not None and str(raw).strip() != "" and not norm:
            raise HTTPException(status_code=400, detail="invalid_cold_serial")
        params["cold_serial"] = norm
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE apartments
                    SET cold_serial=:cold_serial, cold_serial_source='manual'
                    WHERE id=:id
                """),
                {"id": int(apartment_id), "cold_serial": params["cold_serial"]},
            )
    if has_hot_serial:
        raw = body.hot_serial
        norm = _normalize_serial(raw) or None
        if raw is not None and str(raw).strip() != "" and not norm:
            raise HTTPException(status_code=400, detail="invalid_hot_serial")
        params["hot_serial"] = norm
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE apartments
                    SET hot_serial=:hot_serial, hot_serial_source='manual'
                    WHERE id=:id
                """),
                {"id": int(apartment_id), "hot_serial": params["hot_serial"]},
            )

    return {"ok": True, "updated": list(params.keys())}


@router.delete("/admin/ui/apartments/{apartment_id}")
def ui_delete_apartment(apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        row = conn.execute(
            text("DELETE FROM apartments WHERE id=:id RETURNING id"),
            {"id": int(apartment_id)},
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="apartment_not_found")

    return {"ok": True, "id": int(apartment_id)}


@router.get("/admin/ui/apartments/{apartment_id}/card")
def ui_apartment_card(apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        a = conn.execute(
            text("""
                SELECT id, title, address, tenant_name, note, electric_expected, cold_serial, hot_serial, tenant_since, rent_monthly,
                       utilities_mode, utilities_fixed_monthly, utilities_advance_amount, utilities_advance_cycle_months,
                       utilities_advance_anchor_ym, utilities_show_actual_to_tenant
                FROM apartments WHERE id=:id
            """),
            {"id": int(apartment_id)},
        ).mappings().first()
        if not a:
            raise HTTPException(status_code=404, detail="apartment_not_found")

        chats = conn.execute(
            text("""
                SELECT chat_id, is_active, updated_at, created_at
                FROM chat_bindings
                WHERE apartment_id=:id
                  AND is_active=true
                ORDER BY is_active DESC, updated_at DESC
            """),
            {"id": int(apartment_id)},
        ).mappings().all()

    phone = _get_active_contact(int(apartment_id), "phone")
    telegram = _get_active_contact(int(apartment_id), "telegram")

    return {
        "ok": True,
        "apartment": dict(a),
        "contacts": {"phone": phone, "telegram": telegram},
        "chats": list(chats),
    }


@router.get("/admin/ui/apartments/{apartment_id}/photo")
def ui_get_meter_photo(apartment_id: int, ym: str, meter_type: str, meter_index: int = 1):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    mt = str(meter_type or "").strip().lower()
    if mt not in {"cold", "hot", "electric", "sewer"}:
        raise HTTPException(status_code=400, detail="invalid_meter_type")
    if not is_ym(ym):
        raise HTTPException(status_code=400, detail="invalid_ym")
    try:
        mi = int(meter_index)
    except Exception:
        mi = 1
    mi = max(1, min(3, mi))

    with engine.begin() as conn:
        val_row = conn.execute(
            text(
                "SELECT value FROM meter_readings "
                "WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=:mi "
                "LIMIT 1"
            ),
            {"aid": int(apartment_id), "ym": str(ym), "mt": str(mt), "mi": int(mi)},
        ).fetchone()
        cur_val = float(val_row[0]) if val_row and val_row[0] is not None else None

        ydisk_path = None
        if cur_val is not None:
            row = conn.execute(
                text(
                    "SELECT ydisk_path FROM photo_events "
                    "WHERE apartment_id=:aid AND ym=:ym AND meter_kind=:mk AND meter_index=:mi "
                    "AND meter_written=true AND meter_value=:val "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"aid": int(apartment_id), "ym": str(ym), "mk": str(mt), "mi": int(mi), "val": float(cur_val)},
            ).fetchone()
            if row and row[0]:
                ydisk_path = row[0]

        if not ydisk_path:
            row = conn.execute(
                text(
                    "SELECT ydisk_path FROM photo_events "
                    "WHERE apartment_id=:aid AND ym=:ym AND meter_kind=:mk AND meter_index=:mi "
                    "AND meter_written=true "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"aid": int(apartment_id), "ym": str(ym), "mk": str(mt), "mi": int(mi)},
            ).fetchone()
            if row and row[0]:
                ydisk_path = row[0]

    if not ydisk_path:
        raise HTTPException(status_code=404, detail="photo_not_found")

    try:
        content = ydisk_get(str(ydisk_path))
    except Exception:
        raise HTTPException(status_code=404, detail="ydisk_get_failed")

    content_type, _ = mimetypes.guess_type(str(ydisk_path))
    if not content_type:
        content_type = "image/jpeg"
    return Response(content=content, media_type=content_type)


@router.patch("/admin/ui/apartments/{apartment_id}/statuses")
def ui_patch_statuses(apartment_id: int, body: UIStatusesPatch, ym: Optional[str] = None):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = (ym or current_ym()).strip()
    statuses = _upsert_month_statuses(apartment_id, ym_, body)
    return {"ok": True, "ym": ym_, "statuses": statuses.model_dump()}


@router.get("/admin/ui/apartments/{apartment_id}/history")
def ui_apartment_history(apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        ap = conn.execute(
            text(
                """
                SELECT
                    id,
                    electric_expected,
                    tenant_since,
                    utilities_mode,
                    utilities_fixed_monthly,
                    utilities_advance_amount,
                    utilities_advance_cycle_months,
                    utilities_advance_anchor_ym
                FROM apartments
                WHERE id=:aid
                """
            ),
            {"aid": int(apartment_id)},
        ).mappings().first()
        if not ap:
            raise HTTPException(status_code=404, detail="apartment_not_found")

        rows = conn.execute(
            text(
                """
                SELECT
                    ym,
                    meter_type,
                    meter_index,
                    value,
                    source
                FROM meter_readings
                WHERE apartment_id=:aid AND meter_type IN ('cold','hot','electric','sewer')
                ORDER BY ym ASC, meter_type ASC, meter_index ASC
                """
            ),
            {"aid": int(apartment_id)},
        ).mappings().all()

        global_tariffs = conn.execute(
            text(
                """
                SELECT month_from, cold, hot, sewer,
                       COALESCE(electric_t1, electric) AS electric_t1,
                       COALESCE(electric_t2, electric) AS electric_t2
                FROM tariffs
                ORDER BY month_from ASC
                """
            )
        ).mappings().all()

        apartment_overrides = conn.execute(
            text(
                """
                SELECT month_from, cold, hot, sewer, electric_t1, electric_t2
                FROM apartment_tariffs
                WHERE apartment_id=:aid
                ORDER BY month_from ASC
                """
            ),
            {"aid": int(apartment_id)},
        ).mappings().all()

    # группировка по месяцу
    by_month: Dict[str, Any] = {}
    for r in rows:
        ym = r["ym"]
        mt = r["meter_type"]
        mi = int(r["meter_index"] or 1)
        val = float(r["value"]) if r["value"] is not None else None

        if ym not in by_month:
            by_month[ym] = {
                "month": ym,
                "meters": {
                    "cold": {"title": "ХВС", "current": None, "previous": None, "delta": None, "source": None},
                    "hot": {"title": "ГВС", "current": None, "previous": None, "delta": None, "source": None},
                    "electric": {
                        "title": "Электро",
                        "t1": {"title": "T1", "current": None, "previous": None, "delta": None, "source": None, "derived": False},
                        "t2": {"title": "T2", "current": None, "previous": None, "delta": None, "source": None},
                        "t3": {"title": "T3", "current": None, "previous": None, "delta": None, "source": None, "derived": False},
                    },
                    "sewer": {"title": "Водоотведение", "current": None, "previous": None, "delta": None, "source": None},
                },
            }

        if mt == "cold":
            by_month[ym]["meters"]["cold"]["current"] = val
            by_month[ym]["meters"]["cold"]["source"] = r.get("source")
        elif mt == "hot":
            by_month[ym]["meters"]["hot"]["current"] = val
            by_month[ym]["meters"]["hot"]["source"] = r.get("source")
        elif mt == "sewer":
            by_month[ym]["meters"]["sewer"]["current"] = val
            by_month[ym]["meters"]["sewer"]["source"] = r.get("source")
        elif mt == "electric":
            if mi == 1:
                by_month[ym]["meters"]["electric"]["t1"]["current"] = val
                by_month[ym]["meters"]["electric"]["t1"]["source"] = r.get("source")
            elif mi == 2:
                by_month[ym]["meters"]["electric"]["t2"]["current"] = val
                by_month[ym]["meters"]["electric"]["t2"]["source"] = r.get("source")
            elif mi == 3:
                by_month[ym]["meters"]["electric"]["t3"]["current"] = val
                by_month[ym]["meters"]["electric"]["t3"]["source"] = r.get("source")

    # вычисляем дельты по истории
    history = []
    prev_cold = prev_hot = prev_e1 = prev_e2 = prev_e3 = None
    for m in sorted(by_month.keys()):
        entry = by_month[m]
        cur_cold = entry["meters"]["cold"]["current"]
        cur_hot = entry["meters"]["hot"]["current"]
        cur_e1 = entry["meters"]["electric"]["t1"]["current"]
        cur_e2 = entry["meters"]["electric"]["t2"]["current"]
        cur_e3 = entry["meters"]["electric"]["t3"]["current"]

        # cold
        entry["meters"]["cold"]["previous"] = prev_cold
        if cur_cold is not None and prev_cold is not None:
            entry["meters"]["cold"]["delta"] = cur_cold - prev_cold
        prev_cold = cur_cold

        # hot
        entry["meters"]["hot"]["previous"] = prev_hot
        if cur_hot is not None and prev_hot is not None:
            entry["meters"]["hot"]["delta"] = cur_hot - prev_hot
        prev_hot = cur_hot

        # electric t1
        entry["meters"]["electric"]["t1"]["previous"] = prev_e1
        if cur_e1 is not None and prev_e1 is not None:
            entry["meters"]["electric"]["t1"]["delta"] = cur_e1 - prev_e1
        prev_e1 = cur_e1

        # electric t2
        entry["meters"]["electric"]["t2"]["previous"] = prev_e2
        if cur_e2 is not None and prev_e2 is not None:
            entry["meters"]["electric"]["t2"]["delta"] = cur_e2 - prev_e2
        prev_e2 = cur_e2

        # electric t3
        entry["meters"]["electric"]["t3"]["previous"] = prev_e3
        if cur_e3 is not None and prev_e3 is not None:
            entry["meters"]["electric"]["t3"]["delta"] = cur_e3 - prev_e3
        prev_e3 = cur_e3

        history.append(entry)

    e_expected = max(1, min(3, int(ap.get("electric_expected") or 3)))
    mode = str(ap.get("utilities_mode") or "by_actual_monthly")
    fixed_monthly = float(ap.get("utilities_fixed_monthly") or 0)
    advance_amount = float(ap.get("utilities_advance_amount") or 0)
    cycle_months = int(ap.get("utilities_advance_cycle_months") or 3)
    anchor_ym = str(ap.get("utilities_advance_anchor_ym") or "").strip() or None
    tenant_since = ap.get("tenant_since")
    tenant_since_ym = None
    if tenant_since is not None:
        try:
            tenant_since_ym = f"{tenant_since.year:04d}-{tenant_since.month:02d}"
        except Exception:
            tenant_since_ym = None

    def _tariff_for_month(ym: str) -> Dict[str, float]:
        base = None
        for t in global_tariffs:
            tf_ym = str(t.get("month_from") or "")
            if tf_ym <= ym:
                base = t
            else:
                break
        if base is None:
            base = {}
        ov = None
        for t in apartment_overrides:
            tf_ym = str(t.get("month_from") or "")
            if tf_ym <= ym:
                ov = t
            else:
                break
        return {
            "cold": float((ov.get("cold") if ov and ov.get("cold") is not None else base.get("cold") if base else 0) or 0),
            "hot": float((ov.get("hot") if ov and ov.get("hot") is not None else base.get("hot") if base else 0) or 0),
            "sewer": float((ov.get("sewer") if ov and ov.get("sewer") is not None else base.get("sewer") if base else 0) or 0),
            "e1": float((ov.get("electric_t1") if ov and ov.get("electric_t1") is not None else base.get("electric_t1") if base else 0) or 0),
            "e2": float((ov.get("electric_t2") if ov and ov.get("electric_t2") is not None else base.get("electric_t2") if base else 0) or 0),
        }

    carry = 0.0
    for entry in history:
        ym = str(entry.get("month") or "")
        meters = entry.get("meters") or {}
        cold = (meters.get("cold") or {}).get("current")
        hot = (meters.get("hot") or {}).get("current")
        t1 = ((meters.get("electric") or {}).get("t1") or {}).get("current")
        t2 = ((meters.get("electric") or {}).get("t2") or {}).get("current")
        t3 = ((meters.get("electric") or {}).get("t3") or {}).get("current")

        dc = (meters.get("cold") or {}).get("delta")
        dh = (meters.get("hot") or {}).get("delta")
        de1 = ((meters.get("electric") or {}).get("t1") or {}).get("delta")
        de2 = ((meters.get("electric") or {}).get("t2") or {}).get("delta")
        ds = (meters.get("sewer") or {}).get("delta")
        if ds is None:
            ds = (dc or 0) + (dh or 0)

        is_complete = (
            cold is not None
            and hot is not None
            and t1 is not None
            and (e_expected < 2 or t2 is not None)
            and (e_expected < 3 or t3 is not None)
        )

        tf = _tariff_for_month(ym)
        rc = (float(dc) * tf["cold"]) if dc is not None else None
        rh = (float(dh) * tf["hot"]) if dh is not None else None
        re1 = (float(de1) * tf["e1"]) if de1 is not None else None
        re2 = (float(de2) * tf["e2"]) if de2 is not None else None
        rs = (float(ds) * tf["sewer"]) if ds is not None else None

        actual = None
        if is_complete:
            parts = [x for x in [rc, rh, re1, re2, rs] if x is not None]
            actual = float(sum(parts)) if parts else None

        active_for_month = True
        if tenant_since_ym and is_ym(ym):
            active_for_month = ym >= tenant_since_ym

        planned = None
        if active_for_month:
            if mode == "fixed_monthly":
                planned = float(fixed_monthly or 0)
            elif mode == "quarterly_advance":
                planned = float(advance_amount or 0) if _is_cycle_start_month(ym, anchor_ym or tenant_since_ym, cycle_months) else 0.0
            else:
                planned = actual
        else:
            planned = 0.0

        carry = carry + float(planned or 0) - float(actual or 0)
        entry["utilities"] = {
            "mode": mode,
            "actual_accrual": (round(float(actual), 2) if actual is not None else None),
            "planned_due": (round(float(planned), 2) if planned is not None else None),
            "carry_balance": round(float(carry), 2),
        }

    return {"apartment_id": apartment_id, "history": history}


@router.get("/admin/ui/apartments/{apartment_id}/tariffs")
def ui_get_apartment_tariffs(apartment_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM apartments WHERE id=:id LIMIT 1"),
            {"id": int(apartment_id)},
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="apartment_not_found")

        rows = conn.execute(
            text(
                """
                SELECT
                    month_from,
                    cold,
                    hot,
                    sewer,
                    electric_t1,
                    electric_t2,
                    electric_t3,
                    rent,
                    created_at,
                    updated_at
                FROM apartment_tariffs
                WHERE apartment_id=:aid
                ORDER BY month_from ASC
                """
            ),
            {"aid": int(apartment_id)},
        ).mappings().all()

    items = []
    for r in rows:
        items.append(
            {
                "ym_from": r["month_from"],
                "month_from": r["month_from"],
                "cold": (float(r["cold"]) if r["cold"] is not None else None),
                "hot": (float(r["hot"]) if r["hot"] is not None else None),
                "sewer": (float(r["sewer"]) if r["sewer"] is not None else None),
                "electric_t1": (float(r["electric_t1"]) if r["electric_t1"] is not None else None),
                "electric_t2": (float(r["electric_t2"]) if r["electric_t2"] is not None else None),
                "electric_t3": (float(r["electric_t3"]) if r["electric_t3"] is not None else None),
                "rent": (float(r["rent"]) if r["rent"] is not None else None),
                "created_at": (r["created_at"].isoformat() if r["created_at"] else None),
                "updated_at": (r["updated_at"].isoformat() if r["updated_at"] else None),
            }
        )

    return {"ok": True, "apartment_id": int(apartment_id), "items": items}


@router.post("/admin/ui/apartments/{apartment_id}/tariffs")
async def ui_upsert_apartment_tariff(apartment_id: int, request: Request):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    payload = await request.json()
    ym = _normalize_ym_any(payload.get("month_from") or payload.get("ym_from"))
    if not ym:
        raise HTTPException(status_code=400, detail="month_from is required (any month format is allowed)")

    cold = _to_nullable_float(payload.get("cold"))
    hot = _to_nullable_float(payload.get("hot"))
    sewer = _to_nullable_float(payload.get("sewer"))
    e1 = _to_nullable_float(payload.get("electric_t1"))
    e2 = _to_nullable_float(payload.get("electric_t2"))
    e3 = _to_nullable_float(payload.get("electric_t3"))
    rent = _to_nullable_float(payload.get("rent"))

    # back-compat: if old "electric" was sent, use it as fallback for T1/T2
    e_legacy = _to_nullable_float(payload.get("electric"))
    if e_legacy is not None:
        if e1 is None:
            e1 = e_legacy
        if e2 is None:
            e2 = e_legacy

    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM apartments WHERE id=:id LIMIT 1"),
            {"id": int(apartment_id)},
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="apartment_not_found")

        conn.execute(
            text(
                """
                INSERT INTO apartment_tariffs
                    (apartment_id, month_from, cold, hot, sewer, electric_t1, electric_t2, electric_t3, rent, updated_at)
                VALUES
                    (:aid, :month_from, :cold, :hot, :sewer, :e1, :e2, :e3, :rent, now())
                ON CONFLICT (apartment_id, month_from)
                DO UPDATE SET
                    cold=EXCLUDED.cold,
                    hot=EXCLUDED.hot,
                    sewer=EXCLUDED.sewer,
                    electric_t1=EXCLUDED.electric_t1,
                    electric_t2=EXCLUDED.electric_t2,
                    electric_t3=EXCLUDED.electric_t3,
                    rent=EXCLUDED.rent,
                    updated_at=now()
                """
            ),
            {
                "aid": int(apartment_id),
                "month_from": ym,
                "cold": cold,
                "hot": hot,
                "sewer": sewer,
                "e1": e1,
                "e2": e2,
                "e3": e3,
                "rent": rent,
            },
        )

    return {"ok": True, "apartment_id": int(apartment_id), "month_from": ym}


@router.get("/admin/ui/apartments/{apartment_id}/review-flags")
def ui_list_review_flags(apartment_id: int, ym: Optional[str] = None, status: str = "open"):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = (ym or "").strip() or None
    status_ = (status or "open").strip().lower()

    allowed_status = {"open", "resolved", "all"}
    if status_ not in allowed_status:
        raise HTTPException(status_code=400, detail="status must be open|resolved|all")

    where = ["apartment_id=:aid"]
    params: Dict[str, Any] = {"aid": int(apartment_id)}
    if ym_:
        where.append("ym=:ym")
        params["ym"] = str(ym_)
    if status_ != "all":
        where.append("status=:st")
        params["st"] = status_

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT id, apartment_id, ym, meter_type, meter_index, status, reason, comment, created_at, resolved_at, resolved_by
                FROM meter_review_flags
                WHERE {' AND '.join(where)}
                ORDER BY created_at DESC
                """
            ),
            params,
        ).mappings().all()

    items = []
    for r in rows:
        items.append(
            {
                "id": int(r["id"]),
                "apartment_id": int(r["apartment_id"]),
                "ym": r["ym"],
                "meter_type": r["meter_type"],
                "meter_index": int(r["meter_index"] or 1),
                "status": r["status"],
                "reason": r["reason"],
                "comment": r["comment"],
                "created_at": (r["created_at"].isoformat() if r["created_at"] else None),
                "resolved_at": (r["resolved_at"].isoformat() if r["resolved_at"] else None),
                "resolved_by": r["resolved_by"],
            }
        )

    return {"ok": True, "apartment_id": int(apartment_id), "items": items}


@router.post("/admin/ui/review-flags/{flag_id}/resolve")
def ui_resolve_review_flag(flag_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id, status FROM meter_review_flags WHERE id=:id"),
            {"id": int(flag_id)},
        ).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="flag_not_found")

        if str(row["status"] or "") != "resolved":
            conn.execute(
                text(
                    """
                    UPDATE meter_review_flags
                    SET status='resolved', resolved_at=now(), resolved_by='admin_ui'
                    WHERE id=:id
                    """
                ),
                {"id": int(flag_id)},
            )

    return {"ok": True, "id": int(flag_id), "status": "resolved"}


@router.get("/admin/ui/apartments/{apartment_id}/bill")
def ui_get_bill(apartment_id: int, ym: Optional[str] = None):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = (ym or current_ym()).strip()
    with engine.begin() as conn:
        bill = _calc_month_bill(conn, apartment_id=int(apartment_id), ym=str(ym_))
        state = _get_month_bill_state(conn, int(apartment_id), str(ym_))
        return {"ok": True, "apartment_id": int(apartment_id), "ym": str(ym_), "bill": bill, "state": state}


@router.post("/admin/ui/apartments/{apartment_id}/bill/approve")
def ui_approve_bill(apartment_id: int, payload: BillApproveIn):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = (payload.ym or "").strip() or current_ym()
    with engine.begin() as conn:
        _set_month_bill_state(conn, int(apartment_id), str(ym_), pending={}, approved_at=True)
        bill = _calc_month_bill(conn, apartment_id=int(apartment_id), ym=str(ym_))

        sent = False
        if payload.send and (bill.get("reason") == "ok") and bill.get("total_rub") is not None:
            st = _get_month_bill_state(conn, int(apartment_id), str(ym_))
            from core.billing import _same_total  # local import to avoid cycles
            if not _same_total(st.get("sent_total"), bill.get("total_rub")):
                chat_id = _get_active_chat_id(conn, int(apartment_id))
                if chat_id:
                    msg = f"Сумма оплаты по счётчикам за {ym_}: {float(bill.get('total_rub')):.2f} ₽"
                    sent = _tg_send_message(chat_id, msg)
                    if sent:
                        _set_month_bill_state(conn, int(apartment_id), str(ym_), sent_at=True, sent_total=bill.get("total_rub"))
        bill = _calc_month_bill(conn, apartment_id, ym_)
        return {"ok": True, "apartment_id": int(apartment_id), "ym": str(ym_), "sent": bool(sent), "bill": bill}


@router.post("/admin/ui/apartments/{apartment_id}/bill/send-without-t3-photo")
def ui_send_bill_without_t3_photo(apartment_id: int, payload: BillApproveIn):
    """
    Manual override: allow sending sum when only missing photo is electric_3.
    Keeps all other billing rules unchanged.
    """
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    ym_ = (payload.ym or "").strip() or current_ym()

    with engine.begin() as conn:
        strict_bill = _calc_month_bill(conn, apartment_id=int(apartment_id), ym=str(ym_))
        missing = strict_bill.get("missing") or []
        if (strict_bill.get("reason") != "missing_photos") or (set(missing) != {"electric_3"}):
            raise HTTPException(status_code=400, detail="override_allowed_only_for_missing_electric_3")

        bill = _calc_month_bill(conn, apartment_id=int(apartment_id), ym=str(ym_), allow_missing_t3_photo=True)
        if (bill.get("reason") != "ok") or (bill.get("total_rub") is None):
            raise HTTPException(status_code=400, detail=f"cannot_send_reason_{bill.get('reason')}")

        chat_id = _get_active_chat_id(conn, int(apartment_id))
        if not chat_id:
            raise HTTPException(status_code=400, detail="no_active_chat")

        st = _get_month_bill_state(conn, int(apartment_id), str(ym_))
        from core.billing import _same_total  # local import to avoid cycles
        if _same_total(st.get("sent_total"), bill.get("total_rub")):
            return {"ok": True, "apartment_id": int(apartment_id), "ym": str(ym_), "sent": False, "reason": "same_total_already_sent"}

        msg = f"Сумма оплаты по счётчикам за {ym_}: {float(bill.get('total_rub')):.2f} ₽"
        sent = _tg_send_message(str(chat_id), msg)
        if sent:
            _set_month_bill_state(conn, int(apartment_id), str(ym_), sent_at=True, sent_total=bill.get("total_rub"))

        return {"ok": True, "apartment_id": int(apartment_id), "ym": str(ym_), "sent": bool(sent), "bill": bill}


@router.post("/admin/ui/apartments/{apartment_id}/months/{ym}/electric-extra/accept")
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


@router.post("/admin/ui/apartments/{apartment_id}/months/{ym}/electric-extra/reject")
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


@router.post("/admin/ui/apartments/{apartment_id}/meters")
async def admin_add_meter_reading(apartment_id: int, request: Request):
    """Add/update meter reading.

    Supports 2 payload formats:
    1) Single value (used by UI now):
       {"month":"YYYY-MM","kind":"cold|hot|sewer|electric","meter_index":1,"value":123}
    2) Bulk values (handy for scripts/curl):
       {"ym":"YYYY-MM","cold":1,"hot":2,"sewer":3,"electric_t1":10,"electric_t2":20,"electric_t3":30}
    """
    ensure_tables()
    data = await request.json()

    # ---- Bulk format ----
    if ("ym" in data) and ("month" not in data) and ("kind" not in data):
        ym = data.get("ym")
        if not ym or not isinstance(ym, str):
            raise HTTPException(status_code=400, detail="ym is required")

        def _norm_val(v):
            if v is None:
                return None
            if isinstance(v, str) and v.strip() == "":
                return None
            try:
                return float(v)
            except Exception:
                raise HTTPException(status_code=400, detail=f"invalid value: {v!r}")

        updates = []  # (kind, meter_index, value)
        for key, kind, mi in (
            ("cold", "cold", 1),
            ("hot", "hot", 1),
            ("sewer", "sewer", 1),
            ("electric_t1", "electric", 1),
            ("electric_t2", "electric", 2),
            ("electric_t3", "electric", 3),
            ("electric_1", "electric", 1),
            ("electric_2", "electric", 2),
            ("electric_3", "electric", 3),
        ):
            if key in data:
                v = _norm_val(data.get(key))
                if v is not None:
                    updates.append((kind, mi, v))

        if not updates:
            raise HTTPException(status_code=400, detail="no values to update")

        try:
            with engine.begin() as conn:
                for kind, mi, v in updates:
                    if kind == "electric":
                        _write_electric_overwrite_then_sort(conn, int(apartment_id), str(ym), int(mi), float(v), source="manual")
                    else:
                        _add_meter_reading_db(
                            apartment_id=apartment_id,
                            ym=ym,
                            meter_type=kind,
                            meter_index=int(mi),
                            value=v,
                            source="manual",
                        )
                        if kind in ("cold", "hot"):
                            _normalize_water_after_manual(conn, int(apartment_id), str(ym))

                    try:
                        capture_training_sample(
                            conn,
                            apartment_id=int(apartment_id),
                            ym=str(ym),
                            meter_type=str(kind),
                            meter_index=int(mi),
                            correct_value=float(v),
                            source="admin_ui_bulk",
                        )
                    except Exception:
                        pass
        except Exception:
            pass

        # --- after manual edit: recompute bill and auto-send if allowed ---
        try:
            with engine.begin() as conn:
                bill = _calc_month_bill(conn, apartment_id=int(apartment_id), ym=str(ym))
                if (bill.get("reason") == "ok") and (bill.get("total_rub") is not None):
                    _set_month_bill_state(conn, int(apartment_id), str(ym), pending={}, approved_at=True)
                    st = _get_month_bill_state(conn, int(apartment_id), str(ym))
                    from core.billing import _same_total  # local import to avoid cycles
                    if not _same_total(st.get("sent_total"), bill.get("total_rub")):
                        chat_id = _get_active_chat_id(conn, int(apartment_id))
                        if chat_id:
                            msg = f"Сумма оплаты по счётчикам за {ym}: {float(bill.get('total_rub')):.2f} ₽"
                            if _tg_send_message(str(chat_id), msg):
                                _set_month_bill_state(conn, int(apartment_id), str(ym), sent_at=True, sent_total=bill.get("total_rub"))
        except Exception:
            pass

        return {"status": "ok", "apartment_id": apartment_id, "month": ym, "updated": len(updates)}

    # ---- Single-value format (UI) ----
    month = data.get("month")
    kind = data.get("kind")
    value = data.get("value")
    meter_index = data.get("meter_index", 1)

    if month is None or kind is None or value is None:
        raise HTTPException(status_code=400, detail="month, kind and value are required")

    if kind not in {"cold", "hot", "electric", "sewer"}:
        raise HTTPException(status_code=400, detail="unknown kind")

    try:
        meter_index = int(meter_index) if meter_index is not None else 1
    except Exception:
        raise HTTPException(status_code=400, detail="meter_index must be int")

    if kind != "electric":
        meter_index = 1
    else:
        if meter_index not in (1, 2, 3):
            raise HTTPException(status_code=400, detail="electric meter_index must be 1/2/3")

    try:
        value_f = float(value)
    except Exception:
        raise HTTPException(status_code=400, detail="value must be number")

    if kind == "electric":
        with engine.begin() as conn:
            _write_electric_overwrite_then_sort(conn, int(apartment_id), str(month), int(meter_index), float(value_f), source="manual")
    else:
        with engine.begin() as conn:
            _add_meter_reading_db(
                apartment_id=apartment_id,
                ym=month,
                meter_type=kind,
                meter_index=int(meter_index),
                value=value_f,
                source="manual",
            )
            if kind in ("cold", "hot"):
                _normalize_water_after_manual(conn, int(apartment_id), str(month))

    try:
        with engine.begin() as conn:
            capture_training_sample(
                conn,
                apartment_id=int(apartment_id),
                ym=str(month),
                meter_type=str(kind),
                meter_index=int(meter_index),
                correct_value=float(value_f),
                source="admin_ui_single",
            )
    except Exception:
        pass

    # For expected=3: after manual T1/T2 edit set T3=T1+T2
    try:
        with engine.begin() as conn:
            expected = _get_apartment_electric_expected(conn, int(apartment_id))
            if int(expected) == 3 and int(meter_index) in (1, 2):
                _auto_fill_t3_from_t1_t2_if_needed(conn, int(apartment_id), str(month))
    except Exception:
        pass

    # --- after manual edit: recompute bill and auto-send if allowed ---
    try:
        with engine.begin() as conn:
            bill = _calc_month_bill(conn, apartment_id=int(apartment_id), ym=str(month))
            if (bill.get("reason") == "ok") and (bill.get("total_rub") is not None):
                st = _get_month_bill_state(conn, int(apartment_id), str(month))
                from core.billing import _same_total  # local import to avoid cycles
                if not _same_total(st.get("sent_total"), bill.get("total_rub")):
                    chat_id = _get_active_chat_id(conn, int(apartment_id))
                    if chat_id:
                        msg = f"Сумма оплаты по счётчикам за {month}: {float(bill.get('total_rub')):.2f} ₽"
                        if _tg_send_message(str(chat_id), msg):
                            _set_month_bill_state(conn, int(apartment_id), str(month), sent_at=True, sent_total=bill.get("total_rub"))
    except Exception:
        pass

    return {
        "status": "ok",
        "apartment_id": apartment_id,
        "month": month,
        "kind": kind,
        "meter_index": int(meter_index),
        "value": value_f,
    }


# -----------------------
# Admin: notifications (bell)
# -----------------------

@router.post("/admin/ocr-dataset/run")
def ui_run_ocr_dataset():
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    _start_ocr_dataset_job(force=True)
    return {"ok": True, "message": "Запуск сборки OCR-датасета поставлен в очередь."}


@router.get("/admin/ocr-dataset/last")
def ui_last_ocr_dataset():
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT message, created_at
                FROM notifications
                WHERE type='ocr_training'
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
        ).mappings().first()
    if not row:
        return {"ok": True, "message": None, "created_at": None}
    return {"ok": True, "message": row.get("message"), "created_at": str(row.get("created_at"))}


@router.get("/admin/notifications")
def ui_list_notifications(status: str = "unread", limit: int = 50, offset: int = 0):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    status_ = (status or "unread").strip().lower()
    if status_ not in {"unread", "all"}:
        raise HTTPException(status_code=400, detail="status must be unread|all")

    limit = max(1, min(int(limit or 50), 200))
    offset = max(0, int(offset or 0))

    where = []
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if status_ == "unread":
        where.append("n.status='unread'")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with engine.begin() as conn:
        unread_count = conn.execute(
            text("SELECT COUNT(*) FROM notifications WHERE status='unread'")
        ).scalar() or 0

        rows = conn.execute(
            text(
                f"""
                SELECT
                    n.id, n.created_at, n.read_at, n.status,
                    n.chat_id, n.telegram_username, n.apartment_id,
                    n.type, n.message, n.related,
                    a.title AS apartment_title
                FROM notifications n
                LEFT JOIN apartments a ON a.id = n.apartment_id
                {where_sql}
                ORDER BY n.created_at DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            params,
        ).mappings().all()

    items = []
    for r in rows:
        items.append(
            {
                "id": int(r["id"]),
                "created_at": (r["created_at"].isoformat() if r["created_at"] else None),
                "read_at": (r["read_at"].isoformat() if r["read_at"] else None),
                "status": r["status"],
                "chat_id": r["chat_id"],
                "telegram_username": r["telegram_username"] or "Без username",
                "apartment_id": r["apartment_id"],
                "apartment_title": r["apartment_title"],
                "type": r["type"],
                "message": r["message"],
                "related": r["related"],
            }
        )

    return {"ok": True, "items": items, "unread_count": int(unread_count)}


@router.post("/admin/notifications/{notification_id}/read")
def ui_mark_notification_read(notification_id: int):
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE notifications
                SET status='read', read_at=now()
                WHERE id=:id
                """
            ),
            {"id": int(notification_id)},
        )
    return {"ok": True, "id": int(notification_id)}


@router.post("/admin/notifications/clear-read")
def ui_clear_read_notifications():
    if not db_ready():
        raise HTTPException(status_code=503, detail="db_disabled")
    ensure_tables()

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM notifications WHERE status='read'"))
    return {"ok": True}
