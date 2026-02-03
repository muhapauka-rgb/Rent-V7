from typing import Optional, Dict, Any, List
from sqlalchemy import text

from core.config import engine
from core.billing import (
    month_now,
    _get_apartment_electric_expected,
    _set_month_extra_state,
)


# -----------------------
# INTERNAL DB helper (manual/ocr write into meter_readings)
# -----------------------
def _add_meter_reading_db_impl(
    conn,
    apartment_id: int,
    ym: str,
    meter_type: str,
    meter_index: int,
    value: float,
    source: str = "manual",
):
    """
    Низкоуровневая запись одного показания в БД (внутри уже открытого conn/transaction).
    """
    conn.execute(
        text(
            """
            INSERT INTO meter_readings(
                apartment_id, ym, meter_type, meter_index, value, source, ocr_value
            )
            VALUES(
                :aid, :ym, :mt, :mi, :val, :src, NULL
            )
            ON CONFLICT (apartment_id, ym, meter_type, meter_index)
            DO UPDATE SET
                value = EXCLUDED.value,
                source = EXCLUDED.source,
                updated_at = now()
            """
        ),
        {
            "aid": int(apartment_id),
            "ym": str(ym),
            "mt": str(meter_type),
            "mi": int(meter_index),
            "val": float(value),
            "src": str(source),
        },
    )


def _add_meter_reading_db(*args, **kwargs):
    """
    Совместимость вызовов по всему проекту.

    Поддерживаем форматы:
      1) _add_meter_reading_db(conn, apartment_id, ym, meter_type, meter_index, value, source="manual")
      2) _add_meter_reading_db(apartment_id, ym, meter_type, meter_index, value, source="manual")  # conn отсутствует
      3) _add_meter_reading_db(apartment_id=..., ym=..., meter_type=..., meter_index=..., value=..., source="manual", conn=conn)
    """
    conn = kwargs.pop("conn", None)

    # Вариант (1): первый аргумент похож на SQLAlchemy connection (у него есть .execute)
    if args and hasattr(args[0], "execute") and not isinstance(args[0], (int, float, str, bool, dict, list, tuple)):
        conn = args[0]
        args = args[1:]

    # Разбор параметров
    if len(args) >= 5:
        apartment_id = args[0]
        ym = args[1]
        meter_type = args[2]
        meter_index = args[3]
        value = args[4]
        source = args[5] if len(args) >= 6 else kwargs.pop("source", "manual")
    else:
        apartment_id = kwargs.pop("apartment_id")
        ym = kwargs.pop("ym")
        meter_type = kwargs.pop("meter_type")
        meter_index = kwargs.pop("meter_index")
        value = kwargs.pop("value")
        source = kwargs.pop("source", "manual")

    if conn is None:
        # Если conn не передали — открываем транзакцию сами.
        with engine.begin() as _conn:
            return _add_meter_reading_db_impl(
                _conn,
                apartment_id=int(apartment_id),
                ym=str(ym),
                meter_type=str(meter_type),
                meter_index=int(meter_index),
                value=float(value),
                source=str(source),
            )

    return _add_meter_reading_db_impl(
        conn,
        apartment_id=int(apartment_id),
        ym=str(ym),
        meter_type=str(meter_type),
        meter_index=int(meter_index),
        value=float(value),
        source=str(source),
    )


def _write_electric_explicit(conn, apartment_id: int, ym: str, meter_index: int, new_value: float) -> int:
    """
    expected=3:
      T2(idx=2)=MIN, T1(idx=1)=MID, T3(idx=3)=MAX
      Новое значение кладём в свободный слот (если есть), чтобы НЕ терять старые.
      Потом пересортируем все 1..3.
    иначе: пишем строго в meter_index как раньше.
    """
    try:
        meter_index = int(meter_index)
    except Exception:
        return 0
    meter_index = max(1, min(3, meter_index))

    ym = (str(ym).strip() or month_now())

    expected = _get_apartment_electric_expected(conn, apartment_id)

    # ---- expected=3: add-without-losing ----
    if int(expected) == 3:
        rows_before = conn.execute(
            text(
                "SELECT meter_index, value "
                "FROM meter_readings "
                "WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index IN (1,2,3)"
            ),
            {"aid": int(apartment_id), "ym": str(ym)},
        ).fetchall()

        filled = set()
        for mi, v in (rows_before or []):
            if v is not None:
                filled.add(int(mi))

        free = [i for i in (1, 2, 3) if i not in filled]

        # если выбранный индекс занят и есть свободный — пишем в свободный
        target_idx = int(meter_index)
        if (target_idx in filled) and free:
            target_idx = int(free[0])

        # upsert new value to target slot
        conn.execute(
            text(
                "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
                "VALUES(:aid,:ym,'electric',:idx,:val,'ocr',:ocr) "
                "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
                " value=EXCLUDED.value, source='ocr', ocr_value=EXCLUDED.ocr_value, updated_at=now()"
            ),
            {"aid": int(apartment_id), "ym": str(ym), "idx": int(target_idx), "val": float(new_value), "ocr": float(new_value)},
        )

        _normalize_electric_expected3(conn, int(apartment_id), str(ym))
        return int(target_idx)

    # ---- expected!=3: old behavior ----
    conn.execute(
        text(
            "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
            "VALUES(:aid,:ym,'electric',:idx,:val,'ocr',:ocr) "
            "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
            " value=EXCLUDED.value, source='ocr', ocr_value=EXCLUDED.ocr_value, updated_at=now()"
        ),
        {"aid": int(apartment_id), "ym": str(ym), "idx": int(meter_index), "val": float(new_value), "ocr": float(new_value)},
    )

    if int(meter_index) > int(expected) and int(expected) < 3:
        _set_month_extra_state(conn, int(apartment_id), str(ym), True, int(expected))

    return int(meter_index)


def _normalize_electric_expected3(conn, apartment_id: int, ym: str) -> None:
    """Normalize electric readings for expected=3:
    - if 3 values exist: idx2=min, idx1=mid, idx3=idx1+idx2 (derived)
    - if 2 values: idx1=min, idx2=max, idx3=idx1+idx2 (derived)
    - if 1 value: idx1=value, idx2/idx3 removed
    This is used after ADMIN/BOT corrections where we WANT to overwrite a slot, then re-sort.
    """
    ym = (str(ym).strip() or month_now())
    rows = conn.execute(
        text(
            "SELECT meter_index, value, source, ocr_value "
            "FROM meter_readings "
            "WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index IN (1,2,3)"
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchall()

    items = []
    for mi, v, src, ocrv in (rows or []):
        if v is None:
            continue
        items.append({"value": float(v), "source": (src or "manual"), "ocr_value": ocrv})

    if not items:
        return

    if len(items) == 1:
        it = items[0]
        conn.execute(
            text(
                "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
                "VALUES(:aid,:ym,'electric',1,:val,:src,:ocr) "
                "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
                " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
            ),
            {"aid": int(apartment_id), "ym": str(ym), "val": float(it["value"]), "src": str(it["source"]), "ocr": it["ocr_value"]},
        )
        conn.execute(
            text("DELETE FROM meter_readings WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index IN (2,3)"),
            {"aid": int(apartment_id), "ym": str(ym)},
        )
        return

    if len(items) == 2:
        items_sorted = sorted(items, key=lambda x: x["value"])
        it_min = items_sorted[0]
        it_max = items_sorted[1]

        # при 2 значениях держим их в idx=1 и idx=2
        conn.execute(
            text(
                "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
                "VALUES(:aid,:ym,'electric',1,:val,:src,:ocr) "
                "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
                " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
            ),
            {"aid": int(apartment_id), "ym": str(ym), "val": float(it_min["value"]), "src": str(it_min["source"]), "ocr": it_min["ocr_value"]},
        )
        conn.execute(
            text(
                "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
                "VALUES(:aid,:ym,'electric',2,:val,:src,:ocr) "
                "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
                " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
            ),
            {"aid": int(apartment_id), "ym": str(ym), "val": float(it_max["value"]), "src": str(it_max["source"]), "ocr": it_max["ocr_value"]},
        )

        # idx=3 = T1+T2 (derived)
        t3_val = float(it_min["value"]) + float(it_max["value"])
        conn.execute(
            text(
                "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
                "VALUES(:aid,:ym,'electric',3,:val,:src,:ocr) "
                "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
                " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
            ),
            {"aid": int(apartment_id), "ym": str(ym), "val": t3_val, "src": "manual", "ocr": None},
        )
        return

    items_sorted = sorted(items, key=lambda x: x["value"])
    it_min, it_mid, it_max = items_sorted[0], items_sorted[1], items_sorted[2]

    conn.execute(
        text(
            "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
            "VALUES(:aid,:ym,'electric',2,:val,:src,:ocr) "
            "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
            " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
        ),
        {"aid": int(apartment_id), "ym": str(ym), "val": float(it_min["value"]), "src": str(it_min["source"]), "ocr": it_min["ocr_value"]},
    )
    conn.execute(
        text(
            "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
            "VALUES(:aid,:ym,'electric',1,:val,:src,:ocr) "
            "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
            " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
        ),
        {"aid": int(apartment_id), "ym": str(ym), "val": float(it_mid["value"]), "src": str(it_mid["source"]), "ocr": it_mid["ocr_value"]},
    )
    conn.execute(
        text(
            "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
            "VALUES(:aid,:ym,'electric',3,:val,:src,:ocr) "
            "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
            " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
        ),
        {"aid": int(apartment_id), "ym": str(ym), "val": float(it_mid["value"]) + float(it_min["value"]), "src": "manual", "ocr": None},
    )


def _write_electric_overwrite_then_sort(conn, apartment_id: int, ym: str, meter_index: int, new_value: float, *, source: str = "manual") -> int:
    """Overwrite the specified slot, then normalize (used by admin/bot corrections)."""
    try:
        meter_index = int(meter_index)
    except Exception:
        meter_index = 1
    meter_index = max(1, min(3, meter_index))
    ym = (str(ym).strip() or month_now())

    expected = _get_apartment_electric_expected(conn, int(apartment_id))
    if int(expected) != 3:
        conn.execute(
            text(
                "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
                "VALUES(:aid,:ym,'electric',:idx,:val,:src,:ocr) "
                "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
                " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
            ),
            {"aid": int(apartment_id), "ym": str(ym), "idx": int(meter_index), "val": float(new_value), "src": str(source), "ocr": float(new_value)},
        )
        return int(meter_index)

    # overwrite selected slot
    conn.execute(
        text(
            "INSERT INTO meter_readings(apartment_id, ym, meter_type, meter_index, value, source, ocr_value) "
            "VALUES(:aid,:ym,'electric',:idx,:val,:src,:ocr) "
            "ON CONFLICT (apartment_id, ym, meter_type, meter_index) DO UPDATE SET "
            " value=EXCLUDED.value, source=EXCLUDED.source, ocr_value=EXCLUDED.ocr_value, updated_at=now()"
        ),
        {"aid": int(apartment_id), "ym": str(ym), "idx": int(meter_index), "val": float(new_value), "src": str(source), "ocr": float(new_value)},
    )

    _normalize_electric_expected3(conn, int(apartment_id), str(ym))

    # best-effort: where new_value ended up
    rows2 = conn.execute(
        text(
            "SELECT meter_index, value "
            "FROM meter_readings "
            "WHERE apartment_id=:aid AND ym=:ym AND meter_type='electric' AND meter_index IN (1,2,3)"
        ),
        {"aid": int(apartment_id), "ym": str(ym)},
    ).fetchall()

    candidates = []
    for mi, v in (rows2 or []):
        try:
            if abs(float(v) - float(new_value)) < 1e-9:
                candidates.append(int(mi))
        except Exception:
            continue

    if candidates:
        return int(candidates[0])
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
