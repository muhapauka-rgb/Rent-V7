import json
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Dict, Any, List

from sqlalchemy import text

from core.config import BILL_DIFF_THRESHOLD_RUB

# -------------------------
# YM helpers (YYYY-MM)
# -------------------------

_YM_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


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


def is_ym(ym: str) -> bool:
    """Validate year-month string in format YYYY-MM."""
    if ym is None:
        return False
    ym = str(ym).strip()
    return bool(_YM_RE.match(ym))


def add_months(ym: str, delta_months: int) -> str:
    """Add delta months to YYYY-MM and return YYYY-MM."""
    if not is_ym(ym):
        return month_now()
    y, m = map(int, ym.split("-"))
    m0 = (y * 12 + (m - 1)) + int(delta_months)
    y2 = m0 // 12
    m2 = (m0 % 12) + 1
    return f"{y2:04d}-{m2:02d}"


def safe_delta(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous is None:
        return None
    try:
        return float(current) - float(previous)
    except Exception:
        return None


def _get_tariff_for_month(conn, ym: str) -> Optional[Dict[str, float]]:
    row = conn.execute(
        text("""
            SELECT
                month_from,
                cold, hot, sewer,
                electric,
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
        "electric": float(row["electric"]),
        "electric_t1": float(row["electric_t1"]) if row["electric_t1"] is not None else None,
        "electric_t2": float(row["electric_t2"]) if row["electric_t2"] is not None else None,
        "electric_t3": float(row["electric_t3"]) if row["electric_t3"] is not None else None,
    }


def effective_tariff_for_month(conn, ym: str) -> dict:
    """Return tariff dict for month. Falls back to the most recent earlier tariff, or defaults."""
    t = _get_tariff_for_month(conn, ym)
    if not isinstance(t, dict):
        t = {}

    # ВАЖНО: ключи должны совпадать с тем, как ниже используются тарифы в _calc_month_bill()
    return {
        "cold": float(t.get("cold", 0) or 0),
        "hot": float(t.get("hot", 0) or 0),
        "sewer": float(t.get("sewer", 0) or 0),

        # базовый тариф электро (на случай, если tier-тарифы отсутствуют)
        "electric": float(t.get("electric", 0) or 0),

        "electric_t1": float(t.get("electric_t1", 0) or 0),
        "electric_t2": float(t.get("electric_t2", 0) or 0),
        "electric_t3": float(t.get("electric_t3", 0) or 0),
    }


def find_apartment_for_chat(conn, chat_id: str) -> Optional[dict]:
    """Return apartment row (dict) bound to chat_id, or None."""
    try:
        row = conn.execute(
            text("""
                SELECT a.*
                FROM chat_bindings b
                JOIN apartments a ON a.id = b.apartment_id
                WHERE b.chat_id = :chat_id AND b.is_active = true
                LIMIT 1
            """),
            {"chat_id": str(chat_id).strip()},
        ).mappings().fetchone()
        return dict(row) if row else None
    except Exception:
        return None


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


def _get_active_chat_id(conn, apartment_id: int) -> Optional[str]:
    row = conn.execute(
        text(
            "SELECT chat_id FROM chat_bindings "
            "WHERE apartment_id=:aid AND is_active=true "
            "ORDER BY updated_at DESC, created_at DESC "
            "LIMIT 1"
        ),
        {"aid": int(apartment_id)},
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _ensure_month_row(conn, apartment_id: int, ym: str) -> None:
    conn.execute(
        text(
            "INSERT INTO apartment_month_statuses(apartment_id, ym) "
            "VALUES (:aid, :ym) "
            "ON CONFLICT (apartment_id, ym) DO NOTHING"
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    )


def _get_month_bill_state(conn, apartment_id: int, ym: str) -> Dict[str, Any]:
    row = conn.execute(
        text(
            "SELECT bill_pending, bill_last_json, bill_approved_at, bill_sent_at, bill_sent_total "
            "FROM apartment_month_statuses "
            "WHERE apartment_id=:aid AND ym=:ym"
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchone()
    if not row:
        return {"pending": None, "last": None, "approved_at": None, "sent_at": None, "sent_total": None}
    return {
        "pending": row[0],
        "last": row[1],
        "approved_at": (row[2].isoformat() if row[2] is not None else None),
        "sent_at": (row[3].isoformat() if row[3] is not None else None),
        "sent_total": (float(row[4]) if row[4] is not None else None),
    }


def _json_sanitize(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _json_sanitize(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_sanitize(x) for x in v]
    return v


def _same_total(a: Optional[float], b: Optional[float]) -> bool:
    try:
        if a is None or b is None:
            return False
        return round(float(a), 2) == round(float(b), 2)
    except Exception:
        return False


def _set_month_bill_state(
    conn,
    apartment_id: int,
    ym: str,
    *,
    pending: Optional[dict] = None,
    last_json: Optional[dict] = None,
    approved_at: Optional[bool] = None,
    sent_at: Optional[bool] = None,
    sent_total: Optional[float] = None,
    reset_approval: bool = False,
) -> None:
    """Update bill state columns. approved_at/sent_at: True->now(), False->NULL, None->keep."""
    _ensure_month_row(conn, apartment_id, ym)

    sets = []
    params: Dict[str, Any] = {"aid": int(apartment_id), "ym": str(ym)}

    if pending is not None:
        sets.append("bill_pending = CASE WHEN :bill_pending IS NULL THEN NULL ELSE CAST(:bill_pending AS JSONB) END")
        params["bill_pending"] = json.dumps(_json_sanitize(pending), ensure_ascii=False) if pending else None

    if last_json is not None:
        sets.append("bill_last_json = CASE WHEN :bill_last_json IS NULL THEN NULL ELSE CAST(:bill_last_json AS JSONB) END")
        params["bill_last_json"] = json.dumps(_json_sanitize(last_json), ensure_ascii=False) if last_json else None

    if reset_approval:
        sets.append("bill_approved_at = NULL")

    if approved_at is True:
        sets.append("bill_approved_at = now()")
    elif approved_at is False:
        sets.append("bill_approved_at = NULL")

    if sent_at is True:
        sets.append("bill_sent_at = now()")
    elif sent_at is False:
        sets.append("bill_sent_at = NULL")

    if sent_total is not None:
        sets.append("bill_sent_total = :sent_total")
        params["sent_total"] = float(sent_total)

    if not sets:
        return

    sql = "UPDATE apartment_month_statuses SET " + ", ".join(sets) + ", updated_at=now() WHERE apartment_id=:aid AND ym=:ym"
    conn.execute(text(sql), params)


def _calc_month_bill(conn, apartment_id: int, ym: str, *, allow_missing_t3_photo: bool = False) -> Dict[str, Any]:
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
            "SELECT meter_type, meter_index, value, source "
            "FROM meter_readings "
            "WHERE apartment_id=:aid AND ym=:ym AND source IN ('ocr','manual')"
        ),
        {"aid": apartment_id, "ym": ym},
    ).mappings().all()

    cur_map: Dict[str, Dict[int, Optional[float]]] = {"cold": {}, "hot": {}, "electric": {}}
    cur_src: Dict[str, Dict[int, Optional[str]]] = {"cold": {}, "hot": {}, "electric": {}}
    for r in cur:
        mt = r["meter_type"]
        mi = int(r["meter_index"] or 0)
        cur_map.setdefault(mt, {})[mi] = r["value"]
        cur_src.setdefault(mt, {})[mi] = (r.get("source") or None)

    # --- completeness rules ---
    # For electricity:
    #   - if electric_expected == 1 -> require only T1
    #   - if electric_expected == 2 -> require T1 and T2
    #   - if electric_expected >= 3 -> require T1/T2/T3
    #   - for required T3 we only accept OCR/photo value
    #   - exception: allow_missing_t3_photo=True (manual admin override path)
    missing: List[str] = []
    if cur_map.get("cold", {}).get(1) is None:
        missing.append("cold")
    if cur_map.get("hot", {}).get(1) is None:
        missing.append("hot")

    req_electric = [1] if int(electric_expected) <= 1 else ([1, 2] if int(electric_expected) == 2 else [1, 2, 3])
    for i in req_electric:
        val = cur_map.get("electric", {}).get(i)
        if val is None:
            missing.append(f"electric_{i}")
            continue
        if int(i) == 3 and int(electric_expected) >= 3:
            src = (cur_src.get("electric", {}).get(i) or "").lower()
            if (src != "ocr") and (not allow_missing_t3_photo):
                # T3 is present, but not from photo yet.
                missing.append("electric_3")

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

    # --- T3 derive + mismatch check (does not affect rub) ---
    e1_cur_for_t3 = cur_map.get("electric", {}).get(1)
    e2_cur_for_t3 = cur_map.get("electric", {}).get(2)
    e3_raw_for_t3 = cur_map.get("electric", {}).get(3)
    t3_expected = None  # T1+T2
    t3_mismatch = False
    if e1_cur_for_t3 is not None and e2_cur_for_t3 is not None:
        t3_expected = float(e1_cur_for_t3) + float(e2_cur_for_t3)
        if e3_raw_for_t3 is not None:
            try:
                t3_raw = float(e3_raw_for_t3)
                if abs(t3_raw - t3_expected) > 0.01:
                    t3_mismatch = True
            except Exception:
                # If cannot parse, treat as mismatch to force admin review.
                t3_mismatch = True

    if extra_pending:
        return {
            "is_complete_photos": True,
            "total_rub": None,
            "missing": [],
            "reason": "pending_admin",
            "electric_expected": electric_expected,
            "extra_pending": True,
            "threshold_rub": BILL_DIFF_THRESHOLD_RUB,
            "t3": {"expected": t3_expected, "raw": e3_raw_for_t3, "mismatch": bool(t3_mismatch)},
            "pending_flags": [
                {
                    "code": "duplicate_photos",
                    "message": "Обнаружены одинаковые показания (возможно отправили одно и то же фото). Нужна проверка.",
                }
            ],
        }

    prev_ym = add_months(ym, -1)
    prev = conn.execute(
        text(
            "SELECT meter_type, meter_index, value "
            "FROM meter_readings "
            "WHERE apartment_id=:aid AND ym=:pym AND source IN ('ocr','manual')"
        ),
        {"aid": apartment_id, "pym": prev_ym},
    ).mappings().all()

    prev_map: Dict[str, Dict[int, Optional[float]]] = {"cold": {}, "hot": {}, "electric": {}}
    for r in prev:
        mt = r["meter_type"]
        mi = int(r["meter_index"] or 0)
        prev_map.setdefault(mt, {})[mi] = r["value"]
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

    # Электро тарифицируем только T1 и T2.
    # T3 (итого) — информационное поле, его НЕ включаем в оплату.
    tariffed_electric = electric_expected
    if electric_expected >= 3:
        tariffed_electric = 2

    re_sum = 0.0
    for idx in range(1, tariffed_electric + 1):
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

    # --- per-article diff check vs previous month bill (рубли) ---
    pending_items: Dict[str, Any] = {}
    pending_flags: List[Dict[str, Any]] = []
    if bool(t3_mismatch):
        pending_flags.append(
            {
                "code": "t3_mismatch",
                "message": "Т3 не совпадает с суммой Т1+Т2. Т3 не участвует в расчёте, но нужна проверка.",
                "expected": t3_expected,
                "raw": e3_raw_for_t3,
            }
        )

    prev_components: Optional[Dict[str, Any]] = None

    try:
        prev_ym_bill = add_months(ym, -1)
        prevprev_ym_bill = add_months(ym, -2)

        def _v(ym_: str, mt: str, idx: int = 1) -> Optional[float]:
            r = conn.execute(
                text(
                    "SELECT value FROM meter_readings "
                    "WHERE apartment_id=:aid AND ym=:ym AND meter_type=:mt AND meter_index=:mi "
                    "LIMIT 1"
                ),
                {"aid": apartment_id, "ym": str(ym_), "mt": str(mt), "mi": int(idx)},
            ).fetchone()
            return float(r[0]) if r and r[0] is not None else None

        # prev and prevprev month readings (для расчёта предыдущей суммы)
        pc, ph = _v(prev_ym_bill, "cold", 1), _v(prev_ym_bill, "hot", 1)
        ppc, pph = _v(prevprev_ym_bill, "cold", 1), _v(prevprev_ym_bill, "hot", 1)

        pe1, pe2 = _v(prev_ym_bill, "electric", 1), _v(prev_ym_bill, "electric", 2)
        ppe1, ppe2 = _v(prevprev_ym_bill, "electric", 1), _v(prevprev_ym_bill, "electric", 2)

        ps, pps = _v(prev_ym_bill, "sewer", 1), _v(prevprev_ym_bill, "sewer", 1)

        prev_dc = safe_delta(pc, ppc)
        prev_dh = safe_delta(ph, pph)

        prev_ds = safe_delta(ps, pps)
        if prev_ds is None:
            if prev_dc is not None and prev_dh is not None:
                prev_ds = (prev_dc or 0) + (prev_dh or 0)

        prev_de1 = safe_delta(pe1, ppe1)
        prev_de2 = safe_delta(pe2, ppe2)

        rc_prev = None if prev_dc is None else float(prev_dc * float(tariff.get("cold") or 0))
        rh_prev = None if prev_dh is None else float(prev_dh * float(tariff.get("hot") or 0))
        rs_prev = None if prev_ds is None else float(prev_ds * float(tariff.get("sewer") or 0))

        re_prev = None
        if prev_de1 is not None or prev_de2 is not None:
            re_prev = 0.0
            if prev_de1 is not None:
                re_prev += float(prev_de1 * float(tariff.get("electric_t1") or tariff.get("electric") or 0))
            if prev_de2 is not None:
                re_prev += float(prev_de2 * float(tariff.get("electric_t2") or tariff.get("electric") or 0))
            re_prev = float(re_prev)

        total_prev = None
        if rc_prev is not None and rh_prev is not None and rs_prev is not None and re_prev is not None:
            total_prev = float(rc_prev + rh_prev + rs_prev + re_prev)

        prev_components = {
            "prev_ym": str(prev_ym_bill),
            "cold_rub": rc_prev,
            "hot_rub": rh_prev,
            "sewer_rub": rs_prev,
            "electric_rub": re_prev,
            "total_rub": total_prev,
        }

        def _flag(name: str, cur_val: float, prev_val: Optional[float]):
            if prev_val is None:
                return
            diff = float(cur_val) - float(prev_val)
            if abs(diff) > float(BILL_DIFF_THRESHOLD_RUB):
                pending_items[name] = {"cur_rub": float(cur_val), "prev_rub": float(prev_val), "diff_rub": float(diff)}

        _flag("cold", float(rc), rc_prev)
        _flag("hot", float(rh), rh_prev)
        _flag("sewer", float(rs), rs_prev)
        _flag("electric", float(re_sum), re_prev)
        _flag("total", float(total), total_prev)
    except Exception:
        pending_items = {}
        prev_components = None

    # --- admin approval gate (per-article diffs) ---
    bill_state = _get_month_bill_state(conn, int(apartment_id), str(ym))
    last = bill_state.get("last") if isinstance(bill_state, dict) else None
    approved_at = bill_state.get("approved_at") if isinstance(bill_state, dict) else None
    sent_at = bill_state.get("sent_at") if isinstance(bill_state, dict) else None

    reset_approval = False
    if pending_items and approved_at:
        try:
            last_components = (last or {}).get("components") if isinstance(last, dict) else None
            cur_components = {
                "cold_rub": float(rc),
                "hot_rub": float(rh),
                "sewer_rub": float(rs),
                "electric_rub": float(re_sum),
                "total_rub": float(total),
            }
            if last_components != cur_components:
                reset_approval = True
                approved_at = None
        except Exception:
            reset_approval = True
            approved_at = None

    reason_override = None
    if (pending_items or pending_flags) and not approved_at:
        reason_override = "pending_admin"

    snap = {
        "ym": str(ym),
        "components": {
            "cold_rub": float(rc),
            "hot_rub": float(rh),
            "sewer_rub": float(rs),
            "electric_rub": float(re_sum),
            "total_rub": float(total),
        },
        "prev_components": prev_components,
        "pending_items": pending_items,
            "pending_flags": pending_flags,
            "t3": {"expected": t3_expected, "raw": e3_raw_for_t3, "mismatch": bool(t3_mismatch)},
        "threshold_rub": float(BILL_DIFF_THRESHOLD_RUB),
    }
    _set_month_bill_state(
        conn,
        int(apartment_id),
        str(ym),
        pending=(pending_items if reason_override == "pending_admin" else {}),
        last_json=snap,
        reset_approval=bool(reset_approval),
    )

    return {
        "is_complete_photos": True,
        "total_rub": round(total, 2),
        "missing": [],
        "reason": (reason_override or "ok"),
        "electric_expected": electric_expected,
        "extra_pending": False,
        "threshold_rub": float(BILL_DIFF_THRESHOLD_RUB),
        "pending_items": pending_items,
        "prev_components": prev_components,
        "approved_at": approved_at,
        "sent_at": sent_at,
    }
