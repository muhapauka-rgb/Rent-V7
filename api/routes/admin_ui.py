from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import text

from core.config import engine
from core.db import db_ready, ensure_tables
from core.admin_helpers import (
    current_ym,
    _get_active_contact,
    _set_contact,
    _get_month_statuses,
    _upsert_month_statuses,
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
from core.meters import _add_meter_reading_db, _write_electric_overwrite_then_sort, _auto_fill_t3_from_t1_t2_if_needed
from core.integrations import _tg_send_message
from core.schemas import (
    UIContacts,
    UIStatuses,
    UIApartmentItem,
    UIApartmentCreate,
    UIApartmentPatch,
    UIStatusesPatch,
    BillApproveIn,
)

router = APIRouter()

StatusPatch = UIStatusesPatch


@router.get("/admin/ui/apartments")
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

    if not sets and (body.phone is None and body.telegram is None):
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

    if body.phone is not None:
        _set_contact(apartment_id, "phone", body.phone)
    if body.telegram is not None:
        _set_contact(apartment_id, "telegram", body.telegram)

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
                SELECT id, title, address, tenant_name, note, electric_expected
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
        rows = conn.execute(
            text("""
                SELECT
                    ym,
                    meter_type,
                    meter_index,
                    value,
                    source
                FROM meter_readings
                WHERE apartment_id=:aid AND meter_type IN ('cold','hot','electric','sewer')
                ORDER BY ym ASC, meter_type ASC, meter_index ASC
            """),
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

    return {"apartment_id": apartment_id, "history": history}


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

        for kind, mi, v in updates:
            _add_meter_reading_db(
                apartment_id=apartment_id,
                ym=ym,
                meter_type=kind,
                meter_index=int(mi),
                value=v,
                source="manual",
            )

        # For expected=3: after manual T1/T2 edits set T3=T1+T2
        try:
            if any(k == "electric" for (k, _, _) in updates):
                with engine.begin() as conn:
                    expected = _get_apartment_electric_expected(conn, int(apartment_id))
                    if int(expected) == 3:
                        _auto_fill_t3_from_t1_t2_if_needed(conn, int(apartment_id), str(ym))
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
        _add_meter_reading_db(
            apartment_id=apartment_id,
            ym=month,
            meter_type=kind,
            meter_index=int(meter_index),
            value=value_f,
            source="manual",
        )

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
