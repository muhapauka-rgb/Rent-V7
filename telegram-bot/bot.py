import os
import asyncio
import re
import requests
import io
import uuid
import aiohttp
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from contextvars import ContextVar

import logging
logging.basicConfig(level=logging.INFO)

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.utils import exceptions as tg_exceptions
from aiogram.types import (
    ContentType,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from aiogram.dispatcher.middlewares import BaseMiddleware


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_BASE = os.getenv("API_BASE", "http://api:8000").strip()

# ---- Timeouts (seconds)
# IMPORTANT:
# - bot must not block event-loop; all HTTP is done in threads
# - API can be slow because of WebDAV upload; allow longer read timeout
HTTP_CONNECT_TIMEOUT = 10
HTTP_READ_TIMEOUT_PHOTO = 180
HTTP_READ_TIMEOUT_FAST = 25

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
TG_SEND_RETRIES = int(os.getenv("TG_SEND_RETRIES", "3"))


async def _retry_tg_send(coro_factory):
    last_exc = None
    for attempt in range(max(1, TG_SEND_RETRIES)):
        try:
            return await coro_factory()
        except (tg_exceptions.NetworkError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_exc = e
            if attempt < max(1, TG_SEND_RETRIES) - 1:
                await asyncio.sleep(0.35 * (attempt + 1))
            else:
                raise
    if last_exc:
        raise last_exc


_orig_send_message = bot.send_message
_orig_send_photo = bot.send_photo


async def _send_message_with_retry(*args, **kwargs):
    return await _retry_tg_send(lambda: _orig_send_message(*args, **kwargs))


async def _send_photo_with_retry(*args, **kwargs):
    return await _retry_tg_send(lambda: _orig_send_photo(*args, **kwargs))


bot.send_message = _send_message_with_retry
bot.send_photo = _send_photo_with_retry


# -------------------------
# DEBUG middleware: prints every incoming update (message/callback)
# (IMPORTANT: only one middleware, no duplicates)
# -------------------------
class DebugUpdatesMiddleware(BaseMiddleware):
    async def on_pre_process_update(self, update: types.Update, data: Dict[str, Any]):
        chat_id = None
        try:
            if update.callback_query:
                if update.callback_query.message:
                    chat_id = int(update.callback_query.message.chat.id)
                logging.info(f"DEBUG_UPDATE callback_query: data={update.callback_query.data!r}")
            elif update.message:
                chat_id = int(update.message.chat.id)
                logging.info(
                    f"DEBUG_UPDATE message: content_type={update.message.content_type} text={update.message.text!r}"
                )
            else:
                logging.info("DEBUG_UPDATE other type")
        except Exception:
            logging.exception("DEBUG_UPDATE failed")
        finally:
            if chat_id is not None:
                CURRENT_CHAT_ID.set(chat_id)


dp.middleware.setup(DebugUpdatesMiddleware())


# chat_id -> phone
CHAT_PHONES: Dict[int, str] = {}
CONTACT_CONFIRMED: set[int] = set()
# chat_id -> 1..3 (electric index expected for next file)
CHAT_METER_INDEX: Dict[int, int] = {}
CURRENT_CHAT_ID: ContextVar[Optional[int]] = ContextVar("current_chat_id", default=None)

# Avoid repeated month total spam
SENT_BILL: set[Tuple[int, str]] = set()          # (chat_id, ym)
PENDING_NOTICE: set[Tuple[int, str]] = set()     # (chat_id, ym)
REMIND_TASKS: Dict[Tuple[int, str], asyncio.Task] = {}
MEDIA_GROUP_BUFFER: Dict[Tuple[int, str], List[Tuple[bytes, str, str]]] = {}
MEDIA_GROUP_ANCHOR: Dict[Tuple[int, str], types.Message] = {}
MEDIA_GROUP_TASKS: Dict[Tuple[int, str], asyncio.Task] = {}
MEDIA_GROUP_COLLECT_SEC = float(os.getenv("MEDIA_GROUP_COLLECT_SEC", "1.4"))
SEQUENTIAL_PHOTO_BUFFER: Dict[int, List[Tuple[bytes, str, str]]] = {}
SEQUENTIAL_PHOTO_ANCHOR: Dict[int, types.Message] = {}
SEQUENTIAL_PHOTO_TASKS: Dict[int, asyncio.Task] = {}
SEQUENTIAL_PHOTO_COLLECT_SEC = float(os.getenv("SEQUENTIAL_PHOTO_COLLECT_SEC", "2.2"))
SEQUENTIAL_PHOTO_MAX_BATCH = max(1, int(os.getenv("SEQUENTIAL_PHOTO_MAX_BATCH", "4")))

# Manual entry flow
MANUAL_CTX: Dict[int, Dict[str, Any]] = {}       # chat_id -> {ym, missing, step, meter_type, meter_index}


# -------------------------
# Keyboards
# -------------------------

def _kb_main(chat_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    # Главная клавиатура: контакт + старт месяца + отметки оплат
    if chat_id is None:
        chat_id = CURRENT_CHAT_ID.get()
    show_contact = not (chat_id is not None and int(chat_id) in CONTACT_CONFIRMED)

    rows = []
    if show_contact:
        rows.append([KeyboardButton("Передать контакт", request_contact=True)])
    rows.extend(
        [
            [KeyboardButton("Старт месяца")],
            [KeyboardButton("Аренда оплачена"), KeyboardButton("Счётчики оплачены")],
            [KeyboardButton("Сообщить об ошибке распознавания")],
        ]
    )

    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        row_width=2,
        keyboard=rows,
    )



def _kb_manual_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="manual_start")],
            [InlineKeyboardButton(text="📸 Пришлю новое фото", callback_data="manual_photo")],
        ]
    )


def _kb_report_wrong_pick() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="ХВС", callback_data="report_pick|cold|1")],
            [InlineKeyboardButton(text="ГВС", callback_data="report_pick|hot|1")],
            [InlineKeyboardButton(text="Электро T1", callback_data="report_pick|electric|1")],
            [InlineKeyboardButton(text="Электро T2", callback_data="report_pick|electric|2")],
            [InlineKeyboardButton(text="Электро T3", callback_data="report_pick|electric|3")],
            [InlineKeyboardButton(text="Отмена", callback_data="report_cancel")],
        ]
    )
def _kb_fix_fields() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="ХВС", callback_data="fix_pick|cold|1")],
            [InlineKeyboardButton(text="ГВС", callback_data="fix_pick|hot|1")],
            [InlineKeyboardButton(text="Электро T1 (среднее)", callback_data="fix_pick|electric|1")],
            [InlineKeyboardButton(text="Электро T2 (минимум)", callback_data="fix_pick|electric|2")],
            [InlineKeyboardButton(text="Электро T3 (максимум)", callback_data="fix_pick|electric|3")],
            [InlineKeyboardButton(text="Отмена", callback_data="fix_cancel")],
        ]
    )


def _kb_manual_missing(missing: List[str]) -> InlineKeyboardMarkup:
    mapping = {
        "cold": ("ХВС", "manual_pick|cold|1"),
        "hot": ("ГВС", "manual_pick|hot|1"),
        "electric_1": ("Электро T1", "manual_pick|electric|1"),
        "electric_2": ("Электро T2", "manual_pick|electric|2"),
        "electric_3": ("Электро T3", "manual_pick|electric|3"),
        "electric_t1": ("Электро T1", "manual_pick|electric|1"),
        "electric_t2": ("Электро T2", "manual_pick|electric|2"),
        "electric_t3": ("Электро T3", "manual_pick|electric|3"),
        "sewer": ("Водоотведение", "manual_pick|sewer|1"),
    }

    buttons = []
    seen = set()
    for m in (missing or []):
        if m in seen:
            continue
        seen.add(m)
        title, cb = mapping.get(m, (m, f"manual_pick|{m}|1"))
        buttons.append([InlineKeyboardButton(text=title, callback_data=cb)])

    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="manual_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# -------------------------
# Helpers
# -------------------------

def _get_meter_index(chat_id: int) -> int:
    try:
        v = int(CHAT_METER_INDEX.get(chat_id, 1))
    except Exception:
        v = 1
    return max(1, min(3, v))


def _set_meter_index(chat_id: int, idx: int) -> None:
    try:
        idx = int(idx)
    except Exception:
        idx = 1
    CHAT_METER_INDEX[chat_id] = max(1, min(3, idx))


def _missing_to_text(missing: List[str]) -> str:
    mapping = {
        "cold": "ХВС",
        "hot": "ГВС",
        "electric_1": "Электро T1",
        "electric_2": "Электро T2",
        "electric_3": "Электро T3",
        "electric_t1": "Электро T1",
        "electric_t2": "Электро T2",
        "electric_t3": "Электро T3",
        "sewer": "Водоотведение",
    }
    nice = []
    for m in (missing or []):
        nice.append(mapping.get(m, m))
    out = []
    for x in nice:
        if x not in out:
            out.append(x)
    return ", ".join(out)

def _expected_missing_from_bill(bill: dict) -> List[str]:
    # For expected >=2 we require T1+T2; T3 is derived and not required
    missing = ["cold", "hot"]
    try:
        expected = int(bill.get("electric_expected") or 1)
    except Exception:
        expected = 1
    if expected <= 1:
        missing.append("electric_1")
    elif expected == 2:
        missing.extend(["electric_1", "electric_2"])
    else:
        missing.extend(["electric_1", "electric_2", "electric_3"])
    return missing


def _extract_duplicate_info(js: dict) -> Optional[dict]:
    diag = js.get("diag") or {}
    warnings = diag.get("warnings") or []
    for w in warnings:
        if isinstance(w, dict) and "possible_duplicate" in w:
            return w.get("possible_duplicate")
    return None


def _has_anomaly_warning(js: dict) -> bool:
    diag = js.get("diag") or {}
    warnings = diag.get("warnings") or []
    for w in warnings:
        if isinstance(w, dict) and "anomaly_jump" in w:
            return True
    return False


def _extract_anomaly_warning(js: dict) -> Optional[dict]:
    diag = js.get("diag") or {}
    warnings = diag.get("warnings") or []
    for w in warnings:
        if isinstance(w, dict) and "anomaly_jump" in w and isinstance(w.get("anomaly_jump"), dict):
            return w.get("anomaly_jump")
    return None


def _extract_review_reason(js: dict) -> Optional[str]:
    diag = js.get("diag") or {}
    warnings = diag.get("warnings") or []
    reasons: list[str] = []
    for w in warnings:
        if isinstance(w, str):
            if w.startswith("ocr_http_"):
                reasons.append("ошибка OCR сервиса")
            continue
        if not isinstance(w, dict):
            continue
        if "anomaly_jump" in w:
            reasons.append("аномалия относительно прошлого месяца")
        if "water_type_uncertain" in w:
            reasons.append("неуверенный тип водосчётчика")
        if "serial_mismatch" in w:
            reasons.append("серийный номер не совпал")
        if "serial_as_reading_detected" in w:
            reasons.append("распознано как серийный номер, а не показание")
    uniq: list[str] = []
    for r in reasons:
        if r not in uniq:
            uniq.append(r)
    return ", ".join(uniq) if uniq else None


def _parse_float(text: str) -> Optional[float]:
    if text is None:
        return None
    t = str(text).strip()
    if not t:
        return None
    t = t.replace(",", ".")
    t = re.sub(r"\s+", "", t)
    try:
        return float(t)
    except Exception:
        return None


async def _http_post(url: str, *, data=None, json_body=None, files=None, read_timeout=HTTP_READ_TIMEOUT_FAST) -> requests.Response:
    def _do():
        return requests.post(
            url,
            data=data,
            json=json_body,
            files=files,
            timeout=(HTTP_CONNECT_TIMEOUT, read_timeout),
        )
    return await asyncio.to_thread(_do)


async def _http_get(url: str, *, params=None, read_timeout=HTTP_READ_TIMEOUT_FAST) -> requests.Response:
    def _do():
        return requests.get(
            url,
            params=params,
            timeout=(HTTP_CONNECT_TIMEOUT, read_timeout),
        )
    return await asyncio.to_thread(_do)


async def _post_photo_event(
    *,
    chat_id: int,
    telegram_username: Optional[str],
    phone: Optional[str],
    ym: str,
    meter_index: int,
    file_bytes: Optional[bytes] = None,
    filename: Optional[str] = None,
    mime_type: Optional[str] = None,
    file_payloads: Optional[List[Tuple[bytes, str, str]]] = None,
) -> dict:
    url = f"{API_BASE}/events/photo"
    trace_id = f"tg-{uuid.uuid4().hex[:12]}"
    files = None
    if file_payloads:
        if len(file_payloads) > 1:
            files = [
                (
                    "files",
                    (
                        (fn or "file.bin"),
                        fb,
                        (mt or "application/octet-stream"),
                    ),
                )
                for fb, fn, mt in file_payloads
            ]
        else:
            fb, fn, mt = file_payloads[0]
            files = {"file": ((fn or "file.bin"), fb, (mt or "application/octet-stream"))}
    elif file_bytes is not None:
        files = {"file": ((filename or "file.bin"), file_bytes, (mime_type or "application/octet-stream"))}
    else:
        files = {}
    data = {
        "trace_id": trace_id,
        "chat_id": str(chat_id),
        "telegram_username": telegram_username or "",
        "phone": phone or "",
        "ym": ym,
        "meter_index": str(meter_index),
        "meter_index_mode": "explicit",
    }
    resp = await _http_post(url, data=data, files=files, read_timeout=HTTP_READ_TIMEOUT_PHOTO)
    payload = resp.json() if resp.ok else None
    return {
        "status_code": resp.status_code,
        "ok": resp.ok,
        "text": resp.text,
        "json": payload,
        "trace_id": trace_id,
        "server_trace_id": (payload.get("trace_id") if isinstance(payload, dict) else None),
    }


async def _fetch_bill(chat_id: int, ym: str) -> Optional[dict]:
    url = f"{API_BASE}/bot/chats/{chat_id}/bill"
    try:
        resp = await _http_get(url, params={"ym": ym}, read_timeout=HTTP_READ_TIMEOUT_FAST)
        if resp.status_code != 200:
            logging.warning(f"_fetch_bill: non-200 status={resp.status_code} text={resp.text[:300]!r}")
            return None
        data = resp.json()
        return data.get("bill")
    except Exception:
        logging.exception("_fetch_bill failed")
        return None


def _current_ym() -> str:
    return datetime.now().strftime("%Y-%m")


async def _fetch_bill_wrap(chat_id: int, ym: str) -> Optional[dict]:
    """Return full JSON: {ok, apartment_id, bill, ...}"""
    url = f"{API_BASE}/bot/chats/{chat_id}/bill"
    try:
        resp = await _http_get(url, params={"ym": ym}, read_timeout=HTTP_READ_TIMEOUT_FAST)
        if resp.status_code != 200:
            logging.warning(f"_fetch_bill_wrap: non-200 status={resp.status_code} text={resp.text[:300]!r}")
            return None
        return resp.json()
    except Exception:
        logging.exception("_fetch_bill_wrap failed")
        return None


def _choose_meter_index_from_missing(missing: List[str]) -> int:
    # Приоритет: электро T1/T2/T3 (первый недостающий), иначе 1
    if not missing:
        return 1
    candidates = []
    for m in missing:
        mm = str(m).lower()
        if "electric" in mm:
            # electric_2 / electric_t2 / electric2
            if ("_1" in mm) or ("t1" in mm) or mm.endswith("1"):
                candidates.append(1)
            if ("_2" in mm) or ("t2" in mm) or mm.endswith("2"):
                candidates.append(2)
            if ("_3" in mm) or ("t3" in mm) or mm.endswith("3"):
                candidates.append(3)
    if candidates:
        return max(1, min(3, min(candidates)))
    return 1


async def _mark_paid_by_chat(chat_id: int, ym: str, which: str) -> Optional[bool]:
    wrap = await _fetch_bill_wrap(chat_id, ym)
    if not wrap or not wrap.get("ok"):
        return None
    apartment_id = wrap.get("apartment_id")
    if not apartment_id:
        return None

    if which == "rent":
        url = f"{API_BASE}/bot/apartments/{int(apartment_id)}/months/{ym}/rent-paid/toggle"
    else:
        url = f"{API_BASE}/bot/apartments/{int(apartment_id)}/months/{ym}/meters-paid/toggle"

    try:
        resp = await _http_post(url, json_body={}, read_timeout=HTTP_READ_TIMEOUT_FAST)
        if resp.status_code != 200:
            return None
        js = resp.json()
        if not isinstance(js, dict) or not bool(js.get("ok")):
            return None
        return bool(js.get("value"))
    except Exception:
        logging.exception("_mark_paid_by_chat failed")
        return None


async def _start_month(chat_id: int, ym: str) -> Optional[dict]:
    # Сброс одноразовых уведомлений на новый месяц
    key = (chat_id, ym)
    SENT_BILL.discard(key)
    PENDING_NOTICE.discard(key)
    return await _fetch_bill_wrap(chat_id, ym)



async def _manual_write(chat_id: int, ym: str, meter_type: str, meter_index: int, value: float) -> Optional[dict]:
    url = f"{API_BASE}/bot/manual-reading"
    payload = {
        "chat_id": str(chat_id),
        "ym": str(ym),
        "meter_type": str(meter_type),
        "meter_index": int(meter_index),
        "value": float(value),
    }
    try:
        resp = await _http_post(url, json_body=payload, read_timeout=HTTP_READ_TIMEOUT_FAST)
        if resp.status_code != 200:
            logging.warning(f"_manual_write: non-200 status={resp.status_code} text={resp.text[:300]!r}")
            return None
        return resp.json()
    except Exception:
        logging.exception("_manual_write failed")
        return None


async def _report_wrong_reading(chat_id: int, ym: str, meter_type: str, meter_index: int, comment: Optional[str] = None) -> Optional[dict]:
    url = f"{API_BASE}/bot/report-wrong-reading"
    payload = {
        "chat_id": str(chat_id),
        "ym": str(ym),
        "meter_type": str(meter_type),
        "meter_index": int(meter_index),
        "comment": (comment or "").strip() or None,
    }
    try:
        resp = await _http_post(url, json_body=payload, read_timeout=HTTP_READ_TIMEOUT_FAST)
        if resp.status_code != 200:
            logging.warning(f"_report_wrong_reading: non-200 status={resp.status_code} text={resp.text[:300]!r}")
            return None
        return resp.json()
    except Exception:
        logging.exception("_report_wrong_reading failed")
        return None


async def _post_contact_now(chat_id: int, telegram_username: Optional[str], phone: Optional[str]) -> Optional[dict]:
    url = f"{API_BASE}/bot/contact"
    payload = {
        "chat_id": str(chat_id),
        "telegram_username": telegram_username or "",
        "phone": phone or "",
    }
    try:
        resp = await _http_post(url, json_body=payload, read_timeout=HTTP_READ_TIMEOUT_FAST)
        if resp.status_code != 200:
            logging.warning(f"_post_contact_now: non-200 status={resp.status_code} text={resp.text[:300]!r}")
            return None
        return resp.json()
    except Exception:
        logging.exception("_post_contact_now failed")
        return None


async def _post_notification(
    chat_id: int,
    telegram_username: Optional[str],
    message: str,
    ntype: str = "user_message",
    related: Optional[dict] = None,
) -> Optional[dict]:
    url = f"{API_BASE}/bot/notify"
    payload = {
        "chat_id": str(chat_id),
        "telegram_username": telegram_username or "",
        "message": message,
        "type": ntype,
        "related": related or None,
    }
    try:
        resp = await _http_post(url, json_body=payload, read_timeout=HTTP_READ_TIMEOUT_FAST)
        if resp.status_code != 200:
            logging.warning(f"_post_notification: non-200 status={resp.status_code} text={resp.text[:300]!r}")
            return None
        return resp.json()
    except Exception:
        logging.exception("_post_notification failed")
        return None



def _try_send_bill_if_ready(chat_id: int, ym: str, bill: dict):
    if not bill:
        return None

    ctx = MANUAL_CTX.get(chat_id)
    if ctx and ctx.get("ym") == ym and ctx.get("step") in ("idle", "pick", "await_value"):
        return None

    reason = bill.get("reason")
    is_complete = bool(bill.get("is_complete_photos"))
    total_rub = bill.get("total_rub")

    if reason == "pending_admin":
        key = (chat_id, ym)
        if key not in PENDING_NOTICE:
            PENDING_NOTICE.add(key)
            return ("Фото получены. Данные требуют проверки администратором. Итоговую сумму пришлю после подтверждения.", None)
        return None

    if is_complete and total_rub is not None:
        key = (chat_id, ym)
        if key in SENT_BILL:
            return None
        SENT_BILL.add(key)
        PENDING_NOTICE.discard(key)
        return (f"Спасибо за фото, все данные учтены.\nСумма оплаты по счётчикам за {ym}: {float(total_rub):.2f} ₽", None)

    return None


def _schedule_missing_reminder(chat_id: int, ym: str):
    key = (chat_id, ym)

    t = REMIND_TASKS.get(key)
    if t and not t.done():
        t.cancel()

    async def _job():
        try:
            await asyncio.sleep(40)

            if key in SENT_BILL:
                return
            if chat_id in MANUAL_CTX and MANUAL_CTX[chat_id].get("ym") == ym:
                return

            bill = await _fetch_bill(chat_id, ym)
            if not bill:
                return

            if bill.get("reason") == "pending_admin":
                return
            if bool(bill.get("is_complete_photos")):
                return

            missing = bill.get("missing") or []
            if not missing:
                return

            await bot.send_message(chat_id, f"Не хватает фото/показаний: {_missing_to_text(missing)}. Пришлите, пожалуйста, недостающие фото.")
        except asyncio.CancelledError:
            return
        except Exception:
            return

    REMIND_TASKS[key] = asyncio.create_task(_job())


# -------------------------
# Handlers
# -------------------------

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("fix_pick|"))
async def on_fix_pick(call: types.CallbackQuery):
    await call.answer("Ок", show_alert=False)

    chat_id = call.message.chat.id
    parts = (call.data or "").split("|")
    if len(parts) < 3:
        await bot.send_message(chat_id, "Ошибка выбора поля.", reply_markup=_kb_main())
        return

    meter_type = parts[1]
    try:
        meter_index = int(parts[2])
    except Exception:
        meter_index = 1

    ym = _current_ym()

    MANUAL_CTX[chat_id] = {
        "ym": ym,
        "step": "await_value",
        "meter_type": meter_type,
        "meter_index": meter_index,
    }

    title = meter_type
    if meter_type == "cold":
        title = "ХВС"
    elif meter_type == "hot":
        title = "ГВС"
    elif meter_type == "electric":
        if meter_index == 1:
            title = "Электро T1 (среднее)"
        elif meter_index == 2:
            title = "Электро T2 (минимум)"
        else:
            title = "Электро T3 (максимум)"

    await bot.send_message(
        chat_id,
        f"Введите корректное показание для {title} (число). Пример: 123.45",
        reply_markup=_kb_main(),
    )


@dp.callback_query_handler(lambda c: c.data == "fix_cancel")
async def on_fix_cancel(call: types.CallbackQuery):
    await call.answer("Ок", show_alert=False)
    await bot.send_message(call.message.chat.id, "Ок. Исправление отменено.", reply_markup=_kb_main())


@dp.callback_query_handler(lambda c: c.data == "report_cancel")
async def on_report_cancel(call: types.CallbackQuery):
    await call.answer("Ок", show_alert=False)
    await bot.send_message(call.message.chat.id, "Ок, отменил сообщение об ошибке.", reply_markup=_kb_main())


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("report_pick|"))
async def on_report_pick(call: types.CallbackQuery):
    await call.answer("Отправляю", show_alert=False)
    parts = (call.data or "").split("|")
    if len(parts) < 3:
        await bot.send_message(call.message.chat.id, "Ошибка выбора счётчика.", reply_markup=_kb_main())
        return

    meter_type = parts[1]
    try:
        meter_index = int(parts[2])
    except Exception:
        meter_index = 1

    ym = _current_ym()
    res = await _report_wrong_reading(call.message.chat.id, ym, meter_type, meter_index)
    if not res or not res.get("ok"):
        await bot.send_message(
            call.message.chat.id,
            "Не получилось отправить отметку администратору. Попробуйте ещё раз.",
            reply_markup=_kb_main(),
        )
        return

    await bot.send_message(
        call.message.chat.id,
        "Спасибо, отправил администратору пометку: \"Проверить значение\".",
        reply_markup=_kb_main(),
    )


@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    MANUAL_CTX.pop(message.chat.id, None)
    try:
        wrap = await _fetch_bill_wrap(message.chat.id, _current_ym())
        if wrap and wrap.get("ok"):
            CONTACT_CONFIRMED.add(int(message.chat.id))
        else:
            CONTACT_CONFIRMED.discard(int(message.chat.id))
    except Exception:
        CONTACT_CONFIRMED.discard(int(message.chat.id))
        pass
    await message.reply(
        "Привет!\n"
        "1) Нажми «Старт месяца» в начале месяца.\n"
        "2) Пришли фото счётчиков (ХВС/ГВС/Электро).\n"
        "3) Когда оплатишь — нажми «Аренда оплачена» / «Счётчики оплачены».\n"
        "4) Здесь вы можете отправлять любые сообщения для администратора.",
        reply_markup=_kb_main(message.chat.id),
    )

@dp.message_handler(content_types=ContentType.CONTACT)
async def on_contact(message: types.Message):
    c = message.contact
    if not c or not c.phone_number:
        await message.reply("Контакт пустой. Нажмите «Передать контакт» ещё раз.", reply_markup=_kb_main())
        return

    # защита: контакт должен быть от самого пользователя
    if message.from_user and c.user_id and int(c.user_id) != int(message.from_user.id):
        await message.reply("Пожалуйста, отправьте СВОЙ контакт кнопкой «Передать контакт».", reply_markup=_kb_main())
        return

    CHAT_PHONES[message.chat.id] = c.phone_number
    CONTACT_CONFIRMED.add(int(message.chat.id))
    username = message.from_user.username if message.from_user else None
    res = await _post_contact_now(message.chat.id, username, c.phone_number)

    if not res or not res.get("ok"):
        await message.reply(
            "✅ Контакт получен.\n"
            "Но квартиру по номеру пока не нашёл.\n"
            "Попросите администратора добавить ваш номер в карточку квартиры.",
            reply_markup=_kb_main(message.chat.id),
        )
        return

    await message.reply(
        "✅ Контакт получен.\n"
        "Теперь пришлите фото счётчика.\n"
        "Если ваш номер уже внесён администратором в квартиру — привязка произойдёт автоматически.",
        reply_markup=_kb_main(message.chat.id),
    )

@dp.message_handler(content_types=ContentType.TEXT)
async def on_text(message: types.Message):
    ctx = MANUAL_CTX.get(message.chat.id)
    if ctx and ctx.get("step") == "await_value":
        v = _parse_float(message.text)
        if v is None:
            await message.reply("Не понял число. Пример: 123.45", reply_markup=_kb_main())
            return

        ym = ctx.get("ym")
        mt = ctx.get("meter_type")
        mi = int(ctx.get("meter_index") or 1)

        await message.reply("Принято. Сохраняю…", reply_markup=_kb_main())

        res = await _manual_write(message.chat.id, ym, mt, mi, v)
        if not res or not res.get("ok"):
            await message.reply("Не удалось сохранить вручную (backend не ответил). Попробуйте ещё раз.", reply_markup=_kb_main())
            return

        bill = (res.get("bill") or None)
        MANUAL_CTX.pop(message.chat.id, None)

        await message.reply("Готово. Значение записано.", reply_markup=_kb_main())

        if ym and isinstance(bill, dict):
            out = _try_send_bill_if_ready(message.chat.id, ym, bill)
            if out:
                text, kb = out
                await message.reply(text, reply_markup=kb)
            else:
                if bill.get("reason") == "missing_photos":
                    _schedule_missing_reminder(message.chat.id, ym)
        return
    text_in = (message.text or "").strip()

    # Любое пользовательское сообщение (кроме служебных кнопок/команд) шлём в уведомления
    sys_texts = {"Старт месяца", "Аренда оплачена", "Счётчики оплачены", "Сообщить об ошибке распознавания"}
    if text_in and not text_in.startswith("/") and text_in not in sys_texts:
        username = message.from_user.username if message.from_user else None
        await _post_notification(message.chat.id, username, text_in, "user_message")
        return

    # Главные кнопки
    ym = _current_ym()

    if text_in == "Старт месяца":
        wrap = await _start_month(message.chat.id, ym)
        if not wrap or not wrap.get("ok"):
            await message.reply("Я не нашёл вашу квартиру в базе. Напишите администратору.", reply_markup=_kb_main())
            return

        bill = wrap.get("bill") or {}
        if bill.get("reason") == "missing_photos":
            missing = bill.get("missing") or []
            # If this apartment expects 3 electric photos, explicitly mention T3 as well
            try:
                expected = int(bill.get("electric_expected") or 1)
            except Exception:
                expected = 1
            if expected >= 3 and "electric_3" not in missing:
                missing = list(missing) + ["electric_3"]
            tail = (("\nНе хватает: " + _missing_to_text(missing)) if missing else "")
            await message.reply("Месяц начат. Пришлите фото счётчиков." + tail, reply_markup=_kb_main())
            return

        expected = _expected_missing_from_bill(bill)
        tail = (("\nЖду: " + _missing_to_text(expected)) if expected else "")
        await message.reply("Месяц начат. Пришлите фото счётчиков." + tail, reply_markup=_kb_main())
        return

    if text_in == "Аренда оплачена":
        v = await _mark_paid_by_chat(message.chat.id, ym, "rent")
        if v is None:
            await message.reply("Не получилось изменить отметку аренды. Проверьте привязку квартиры.", reply_markup=_kb_main())
        elif v:
            await message.reply("✅ Отметил аренду как оплаченную за " + ym, reply_markup=_kb_main())
        else:
            await message.reply("↩️ Снял отметку оплаты аренды за " + ym, reply_markup=_kb_main())
        return

    if text_in == "Счётчики оплачены":
        v = await _mark_paid_by_chat(message.chat.id, ym, "meters")
        if v is None:
            await message.reply("Не получилось изменить отметку счётчиков. Проверьте привязку квартиры.", reply_markup=_kb_main())
        elif v:
            await message.reply("✅ Отметил счётчики как оплаченные за " + ym, reply_markup=_kb_main())
        else:
            await message.reply("↩️ Снял отметку оплаты счётчиков за " + ym, reply_markup=_kb_main())
        return

    if text_in == "Сообщить об ошибке распознавания":
        username = message.from_user.username if message.from_user else None
        await _post_notification(message.chat.id, username, "Нажал: Сообщить об ошибке распознавания", "bot_warning")
        await message.reply(
            "Выберите счётчик, где значение распознано неверно:",
            reply_markup=_kb_report_wrong_pick(),
        )
        return

    # Не отвечаем на прочий текст — он уже отправлен администратору


async def _handle_file_message(
    message: types.Message,
    *,
    file_bytes: Optional[bytes] = None,
    filename: Optional[str] = None,
    mime_type: Optional[str] = None,
    file_payloads: Optional[List[Tuple[bytes, str, str]]] = None,
):
    username = message.from_user.username if message.from_user else None
    phone = CHAT_PHONES.get(message.chat.id)  # берём телефон, который пользователь отправил кнопкой

    payloads: List[Tuple[bytes, str, str]] = []
    if file_payloads:
        payloads = [(bytes(b), str(fn or "file.bin"), str(mt or "application/octet-stream")) for b, fn, mt in file_payloads if b]
    elif file_bytes is not None:
        payloads = [(bytes(file_bytes), str(filename or "file.bin"), str(mime_type or "application/octet-stream"))]
    if not payloads:
        await message.reply("Не удалось прочитать файл(ы). Пришлите фото ещё раз.", reply_markup=_kb_main())
        return

    preview_bytes, preview_name, _preview_mime = payloads[0]

    ym = _current_ym()

    # Пытаемся выбрать, какой индекс (особенно для электро T1/T2/T3) сейчас не заполнен
    meter_index = 1
    try:
        bill = await _fetch_bill(message.chat.id, ym)
        missing = (bill.get("missing") or []) if isinstance(bill, dict) else []
        meter_index = _choose_meter_index_from_missing(missing)
    except Exception:
        meter_index = 1

    try:
        r = await _post_photo_event(
            chat_id=message.chat.id,
            telegram_username=username,
            phone=phone,
            ym=ym,
            meter_index=meter_index,
            file_payloads=payloads,
        )
    except requests.exceptions.ReadTimeout:
        await message.reply(
            "Фото получено, но backend долго обрабатывает запрос (возможно загрузка на диск).\n"
            "Попробуйте отправить ещё раз через минуту.",
            reply_markup=_kb_main(),
        )
        return
    except Exception:
        await message.reply(
            "Фото получено, но backend сейчас недоступен. Попробуйте ещё раз позже.",
            reply_markup=_kb_main(),
        )
        return

    if not r.get("ok"):
        await message.reply(f"Ошибка отправки в backend: HTTP {r.get('status_code')}", reply_markup=_kb_main())
        return

    js = r.get("json") or {}
    ym = js.get("ym") or ""
    assigned = js.get("assigned_meter_index", meter_index)
    trace_id = js.get("trace_id") or r.get("server_trace_id") or r.get("trace_id")

    ocr = js.get("ocr") or {}
    ocr_type = ocr.get("type")
    ocr_reading = ocr.get("reading")
    ocr_conf = ocr.get("confidence")

    meter_written = js.get("meter_written")
    ocr_failed = bool(js.get("ocr_failed"))
    review_reason = _extract_review_reason(js)
    conf_txt = None
    if isinstance(ocr_conf, (int, float)):
        conf_txt = f"{float(ocr_conf):.2f}"

    anomaly_info = _extract_anomaly_warning(js)
    logging.info(
        "PHOTO_EVENT trace_id=%s meter_written=%s ocr_failed=%s ocr_type=%s ocr_reading=%s ocr_conf=%s review_reason=%s",
        trace_id,
        meter_written,
        ocr_failed,
        ocr_type,
        ocr_reading,
        conf_txt,
        review_reason,
    )

    if ocr_failed or ((meter_written is False) and (ocr_reading is None)):
        await message.reply(
            "Фото получено, но не удалось распознать показания (нечётко/блики/обрезано).\n"
            "Пожалуйста, пришлите фото лучшего качества.\n\n"
            "Если удобнее — можно ввести вручную (только для незаполненных полей).",
            reply_markup=_kb_manual_start(),
        )
        MANUAL_CTX[message.chat.id] = {"ym": ym, "step": "idle"}
        logging.info(f"MANUAL_CTX set for chat_id={message.chat.id} ym={ym!r} step='idle'")
        return

    if (meter_written is False) and (ocr_reading is not None):
        shown_reading = ocr_reading
        if isinstance(shown_reading, (int, float)):
            try:
                shown_reading = f"{float(shown_reading):.2f}"
            except Exception:
                pass
        conf_line = f"\nУверенность OCR: {conf_txt}" if conf_txt is not None else ""
        reason_line = f"\nПричина проверки: {review_reason}" if review_reason else ""
        msg = (
            "Фото получено.\n"
            f"Распознано: {ocr_type or '—'} / {shown_reading}\n"
            f"Значение выглядит спорным: мы отметили «Проверить» для администратора."
            f"{conf_line}{reason_line}"
        )
        await message.reply(msg, reply_markup=_kb_main())
        return

    shown_reading = ocr_reading
    if shown_reading is None and isinstance(anomaly_info, dict):
        shown_reading = anomaly_info.get("curr")

    msg = f"Принято. (meter_index={assigned})"
    if ocr_type or shown_reading is not None:
        msg += f"\nРаспознано: {ocr_type or '—'} / {shown_reading if shown_reading is not None else '—'}"
    if conf_txt is not None:
        msg += f"\nУверенность OCR: {conf_txt}"
    if review_reason:
        msg += f"\nПричина проверки: {review_reason}"
    if anomaly_info:
        msg += "\nЗначение выглядит подозрительным, но мы сохранили его и отметили «Проверить значение» для администратора."
    await message.reply(msg, reply_markup=_kb_main())


    dup = _extract_duplicate_info(js)
    if dup and ym:
        mt = dup.get("meter_type")
        mi = dup.get("meter_index")
        val = dup.get("value")
        caption = (
            "Похоже, это дубликат уже присланного значения.\n"
            f"Совпало с: {mt} #{mi}, значение {val}."
        )
        try:
            await bot.send_photo(
                message.chat.id,
                photo=types.InputFile(io.BytesIO(preview_bytes), filename=preview_name or "duplicate.jpg"),
                caption=caption,
                reply_markup=_kb_main(),
            )
        except Exception:
            await message.reply(caption, reply_markup=_kb_main())

        bill = js.get("bill")
        if isinstance(bill, dict) and bill.get("reason") == "missing_photos":
            missing = bill.get("missing") or []
            if missing:
                await message.reply("Сейчас не хватает: " + _missing_to_text(missing), reply_markup=_kb_main())
                _schedule_missing_reminder(message.chat.id, ym)
        return

    bill = js.get("bill")
    if ym and isinstance(bill, dict):
        res = _try_send_bill_if_ready(message.chat.id, ym, bill)
        if res:
            text, kb = res
            await message.reply(text, reply_markup=kb)
        else:
            if bill.get("reason") == "missing_photos":
                _schedule_missing_reminder(message.chat.id, ym)


async def _flush_media_group(key: Tuple[int, str]) -> None:
    try:
        await asyncio.sleep(max(0.4, float(MEDIA_GROUP_COLLECT_SEC)))
        items = MEDIA_GROUP_BUFFER.pop(key, [])
        anchor = MEDIA_GROUP_ANCHOR.pop(key, None)
        if not items or anchor is None:
            return
        logging.info(
            "TG media_group flush: chat_id=%s media_group_id=%s items=%s",
            key[0],
            key[1],
            len(items),
        )
        await _handle_file_message(anchor, file_payloads=items)
    except Exception:
        logging.exception("media_group_flush failed")
    finally:
        MEDIA_GROUP_TASKS.pop(key, None)


async def _flush_sequential_photos(chat_id: int) -> None:
    try:
        await asyncio.sleep(max(0.5, float(SEQUENTIAL_PHOTO_COLLECT_SEC)))
        items = SEQUENTIAL_PHOTO_BUFFER.pop(chat_id, [])
        anchor = SEQUENTIAL_PHOTO_ANCHOR.pop(chat_id, None)
        if not items or anchor is None:
            return
        batch = items[:SEQUENTIAL_PHOTO_MAX_BATCH]
        logging.info(
            "TG sequential flush: chat_id=%s items=%s",
            chat_id,
            len(batch),
        )
        await _handle_file_message(anchor, file_payloads=batch)
    except Exception:
        logging.exception("sequential_photo_flush failed")
    finally:
        SEQUENTIAL_PHOTO_TASKS.pop(chat_id, None)


def _queue_sequential_photo(message: types.Message, payload: bytes, filename: str, mime: str) -> None:
    chat_id = int(message.chat.id)
    SEQUENTIAL_PHOTO_BUFFER.setdefault(chat_id, []).append((payload, filename, mime))
    if chat_id not in SEQUENTIAL_PHOTO_ANCHOR:
        SEQUENTIAL_PHOTO_ANCHOR[chat_id] = message
    task = SEQUENTIAL_PHOTO_TASKS.get(chat_id)
    if task is None or task.done():
        SEQUENTIAL_PHOTO_TASKS[chat_id] = asyncio.create_task(_flush_sequential_photos(chat_id))


@dp.message_handler(content_types=ContentType.PHOTO)
async def on_photo(message: types.Message):
    logging.info(
        "TG photo received: chat_id=%s message_id=%s photos=%s",
        message.chat.id,
        message.message_id,
        len(message.photo or []),
    )
    photo = message.photo[-1]
    f = await bot.get_file(photo.file_id)
    stream = await bot.download_file(f.file_path)
    payload = stream.read()
    logging.info(
        "TG photo downloaded: chat_id=%s message_id=%s bytes=%s file_id=%s",
        message.chat.id,
        message.message_id,
        len(payload),
        photo.file_id,
    )
    mgid = str(message.media_group_id or "").strip()
    if mgid:
        key = (int(message.chat.id), mgid)
        MEDIA_GROUP_BUFFER.setdefault(key, []).append(
            (
                payload,
                f"photo_{photo.file_unique_id}.jpg",
                "image/jpeg",
            )
        )
        if key not in MEDIA_GROUP_ANCHOR:
            MEDIA_GROUP_ANCHOR[key] = message
        task = MEDIA_GROUP_TASKS.get(key)
        if task is None or task.done():
            MEDIA_GROUP_TASKS[key] = asyncio.create_task(_flush_media_group(key))
        return
    _queue_sequential_photo(
        message,
        payload,
        f"photo_{photo.file_unique_id}.jpg",
        "image/jpeg",
    )


@dp.message_handler(content_types=ContentType.DOCUMENT)
async def on_document(message: types.Message):
    logging.info(
        "TG document received: chat_id=%s message_id=%s file_name=%s mime=%s",
        message.chat.id,
        message.message_id,
        (message.document.file_name if message.document else None),
        (message.document.mime_type if message.document else None),
    )
    doc = message.document
    f = await bot.get_file(doc.file_id)
    stream = await bot.download_file(f.file_path)
    payload = stream.read()
    logging.info(
        "TG document downloaded: chat_id=%s message_id=%s bytes=%s file_id=%s",
        message.chat.id,
        message.message_id,
        len(payload),
        doc.file_id,
    )
    await _handle_file_message(
        message,
        file_bytes=payload,
        filename=doc.file_name or "file.bin",
        mime_type=doc.mime_type or "application/octet-stream",
    )


# -------------------------
# Callback: manual entry
# -------------------------

@dp.callback_query_handler(lambda c: c.data == "manual_photo")
async def on_manual_photo(call: types.CallbackQuery):
    await call.answer("Ок", show_alert=False)
    MANUAL_CTX.pop(call.message.chat.id, None)
    await bot.send_message(
        call.message.chat.id,
        "Ок. Пришлите, пожалуйста, новое фото лучшего качества.",
        reply_markup=_kb_main(),
    )


@dp.callback_query_handler(lambda c: c.data == "manual_start")
async def on_manual_start(call: types.CallbackQuery):
    # IMPORTANT: always send a visible message, even if backend/ym fails
    await call.answer("Ок", show_alert=False)

    chat_id = call.message.chat.id
    ctx = MANUAL_CTX.get(chat_id) or {}
    ym = (ctx.get("ym") or "").strip()

    logging.info(f"MANUAL_START pressed: chat_id={chat_id} ctx={ctx} resolved_ym={ym!r}")

    # If ym is missing - tell user clearly (so it is never "silent")
    if not ym:
        await bot.send_message(
            chat_id,
            "Не получилось начать ручной ввод: не определён месяц (ym).\n"
            "Пришлите, пожалуйста, фото ещё раз (или нажмите /start и повторите).",
            reply_markup=_kb_main(),
        )
        return

    # show progress message so user sees something immediately
    await bot.send_message(chat_id, "Открываю ручной ввод…", reply_markup=_kb_main())

    bill = await _fetch_bill(chat_id, ym)
    if not bill:
        await bot.send_message(
            chat_id,
            "Не удалось получить список незаполненных полей от сервера.\n"
            "Попробуйте ещё раз через 10–20 секунд или пришлите новое фото.",
            reply_markup=_kb_main(),
        )
        return

    missing = bill.get("missing") or []
    if not missing:
        out = _try_send_bill_if_ready(chat_id, ym, bill)
        if out:
            text, kb = out
            await bot.send_message(chat_id, text, reply_markup=kb)
        else:
            await bot.send_message(chat_id, "Сейчас нет незаполненных полей.", reply_markup=_kb_main())
        MANUAL_CTX.pop(chat_id, None)
        return

    MANUAL_CTX[chat_id] = {"ym": ym, "missing": missing, "step": "pick"}
    await bot.send_message(
        chat_id,
        "Выберите, какое поле заполнить вручную:",
        reply_markup=_kb_manual_missing(missing),
    )


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("manual_pick|"))
async def on_manual_pick(call: types.CallbackQuery):
    await call.answer("Ок", show_alert=False)

    chat_id = call.message.chat.id

    parts = (call.data or "").split("|")
    if len(parts) < 3:
        await bot.send_message(chat_id, "Ошибка выбора поля.", reply_markup=_kb_main())
        return

    meter_type = parts[1]
    try:
        meter_index = int(parts[2])
    except Exception:
        meter_index = 1

    ctx = MANUAL_CTX.get(chat_id) or {}
    ym = (ctx.get("ym") or "").strip()
    if not ym:
        await bot.send_message(chat_id, "Не удалось определить месяц. Пришлите фото ещё раз.", reply_markup=_kb_main())
        return

    MANUAL_CTX[chat_id] = {
        "ym": ym,
        "step": "await_value",
        "meter_type": meter_type,
        "meter_index": meter_index,
    }

    title = meter_type
    if meter_type == "cold":
        title = "ХВС"
    elif meter_type == "hot":
        title = "ГВС"
    elif meter_type == "electric":
        title = f"Электро T{meter_index}"

    await bot.send_message(
        chat_id,
        f"Введите показание для {title} (число). Пример: 123.45",
        reply_markup=_kb_main(),
    )


@dp.callback_query_handler(lambda c: c.data == "manual_cancel")
async def on_manual_cancel(call: types.CallbackQuery):
    await call.answer("Ок", show_alert=False)
    MANUAL_CTX.pop(call.message.chat.id, None)
    await bot.send_message(call.message.chat.id, "Ок. Отменил ручной ввод.", reply_markup=_kb_main())


if __name__ == "__main__":
    executor.start_polling(
        dp,
        skip_updates=False,
        allowed_updates=["message", "callback_query"]
    )
