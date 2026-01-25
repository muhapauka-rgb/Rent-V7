import os
import asyncio
import re
import requests
from typing import Optional, Dict, Any, List, Tuple

import logging
logging.basicConfig(level=logging.INFO)

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
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


# -------------------------
# DEBUG middleware: prints every incoming update (message/callback)
# (IMPORTANT: only one middleware, no duplicates)
# -------------------------
class DebugUpdatesMiddleware(BaseMiddleware):
    async def on_pre_process_update(self, update: types.Update, data: Dict[str, Any]):
        try:
            if update.callback_query:
                logging.info(f"DEBUG_UPDATE callback_query: data={update.callback_query.data!r}")
            elif update.message:
                logging.info(
                    f"DEBUG_UPDATE message: content_type={update.message.content_type} text={update.message.text!r}"
                )
            else:
                logging.info("DEBUG_UPDATE other type")
        except Exception:
            logging.exception("DEBUG_UPDATE failed")


dp.middleware.setup(DebugUpdatesMiddleware())


# chat_id -> phone
CHAT_PHONES: Dict[int, str] = {}
# chat_id -> 1..3 (electric index expected for next file)
CHAT_METER_INDEX: Dict[int, int] = {}

# Avoid repeated month total spam
SENT_BILL: set[Tuple[int, str]] = set()          # (chat_id, ym)
PENDING_NOTICE: set[Tuple[int, str]] = set()     # (chat_id, ym)
REMIND_TASKS: Dict[Tuple[int, str], asyncio.Task] = {}

# Duplicate confirm flow
DUP_PENDING: Dict[int, Dict[str, Any]] = {}      # photo_event_id -> {ym, dup}

# Manual entry flow
MANUAL_CTX: Dict[int, Dict[str, Any]] = {}       # chat_id -> {ym, missing, step, meter_type, meter_index}


# -------------------------
# Keyboards
# -------------------------

def _kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        row_width=2,
        keyboard=[
            [KeyboardButton("Поделиться контактом", request_contact=True)],
            [KeyboardButton("Электро T1"), KeyboardButton("Электро T2")],
            [KeyboardButton("Электро T3"), KeyboardButton("Вода (ХВС/ГВС)")],
        ],
    )


def _kb_duplicate(photo_event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Это разные счётчики (оставить)",
                    callback_data=f"dup_ok|{photo_event_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="Это повтор (пришлю другое фото)",
                    callback_data=f"dup_repeat|{photo_event_id}",
                ),
            ],
        ]
    )


def _kb_manual_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="manual_start")],
            [InlineKeyboardButton(text="📸 Пришлю новое фото", callback_data="manual_photo")],
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


def _extract_duplicate_info(js: dict) -> Optional[dict]:
    diag = js.get("diag") or {}
    warnings = diag.get("warnings") or []
    for w in warnings:
        if isinstance(w, dict) and "possible_duplicate" in w:
            return w.get("possible_duplicate")
    return None


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
    meter_index: int,
    file_bytes: bytes,
    filename: str,
    mime_type: str,
) -> dict:
    url = f"{API_BASE}/events/photo"
    files = {"file": (filename or "file.bin", file_bytes, mime_type or "application/octet-stream")}
    data = {
        "chat_id": str(chat_id),
        "telegram_username": telegram_username or "",
        "phone": phone or "",
        "meter_index": str(meter_index),
    }
    resp = await _http_post(url, data=data, files=files, read_timeout=HTTP_READ_TIMEOUT_PHOTO)
    return {"status_code": resp.status_code, "ok": resp.ok, "text": resp.text, "json": (resp.json() if resp.ok else None)}


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


async def _resolve_duplicate(photo_event_id: int, action: str) -> Optional[dict]:
    url = f"{API_BASE}/bot/duplicate/resolve"
    try:
        resp = await _http_post(
            url,
            json_body={"photo_event_id": int(photo_event_id), "action": str(action)},
            read_timeout=HTTP_READ_TIMEOUT_FAST
        )
        if resp.status_code != 200:
            logging.warning(f"_resolve_duplicate: non-200 status={resp.status_code} text={resp.text[:300]!r}")
            return None
        return resp.json()
    except Exception:
        logging.exception("_resolve_duplicate failed")
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

@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    _set_meter_index(message.chat.id, 1)
    MANUAL_CTX.pop(message.chat.id, None)
    await message.reply(
        "Привет.\n"
        "1) Чтобы привязать квартиру — нажми «Поделиться контактом».\n"
        "2) Дальше присылай фото счётчиков.\n"
        "Для электро выбери T1/T2/T3 перед отправкой.",
        reply_markup=_kb_main(),
    )


@dp.message_handler(commands=["t1", "t2", "t3", "water"])
async def cmd_set_meter(message: types.Message):
    cmd = (message.text or "").strip().lower()
    if cmd.endswith("t1"):
        _set_meter_index(message.chat.id, 1)
        await message.reply("Ок: Электро T1.", reply_markup=_kb_main())
    elif cmd.endswith("t2"):
        _set_meter_index(message.chat.id, 2)
        await message.reply("Ок: Электро T2.", reply_markup=_kb_main())
    elif cmd.endswith("t3"):
        _set_meter_index(message.chat.id, 3)
        await message.reply("Ок: Электро T3.", reply_markup=_kb_main())
    elif cmd.endswith("water"):
        _set_meter_index(message.chat.id, 1)
        await message.reply("Ок: Вода (ХВС/ГВС).", reply_markup=_kb_main())


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

    txt = (message.text or "").strip().lower()

    if txt == "электро t1":
        _set_meter_index(message.chat.id, 1)
        await message.reply("Ок: Электро T1.", reply_markup=_kb_main())
        return
    if txt == "электро t2":
        _set_meter_index(message.chat.id, 2)
        await message.reply("Ок: Электро T2.", reply_markup=_kb_main())
        return
    if txt == "электро t3":
        _set_meter_index(message.chat.id, 3)
        await message.reply("Ок: Электро T3.", reply_markup=_kb_main())
        return
    if txt == "вода (хвс/гвс)":
        _set_meter_index(message.chat.id, 1)
        await message.reply("Ок: Вода (ХВС/ГВС).", reply_markup=_kb_main())
        return

    await message.reply(
        "Пришли фото/файл счётчика.\n"
        "Если это электричество — выбери Электро T1/T2/T3 перед отправкой.\n"
        "Команды: /t1 /t2 /t3 /water",
        reply_markup=_kb_main(),
    )


@dp.message_handler(content_types=ContentType.CONTACT)
async def on_contact(message: types.Message):
    if message.contact and message.contact.phone_number:
        CHAT_PHONES[message.chat.id] = message.contact.phone_number
        await message.reply("Контакт сохранён. Теперь присылай фото счётчиков.", reply_markup=_kb_main())
    else:
        await message.reply("Не вижу номера в контакте. Попробуй ещё раз.", reply_markup=_kb_main())


async def _handle_file_message(message: types.Message, *, file_bytes: bytes, filename: str, mime_type: str):
    username = message.from_user.username if message.from_user else None
    phone = CHAT_PHONES.get(message.chat.id)
    meter_index = _get_meter_index(message.chat.id)

    try:
        r = await _post_photo_event(
            chat_id=message.chat.id,
            telegram_username=username,
            phone=phone,
            meter_index=meter_index,
            file_bytes=file_bytes,
            filename=filename,
            mime_type=mime_type,
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

    ocr = js.get("ocr") or {}
    ocr_type = ocr.get("type")
    ocr_reading = ocr.get("reading")

    meter_written = js.get("meter_written")
    ocr_failed = bool(js.get("ocr_failed"))

    if (meter_written is False) or ocr_failed:
        await message.reply(
            "Фото получено, но не удалось распознать показания (нечётко/блики/обрезано).\n"
            "Пожалуйста, пришлите фото лучшего качества.\n\n"
            "Если удобнее — можно ввести вручную (только для незаполненных полей).",
            reply_markup=_kb_manual_start(),
        )
        MANUAL_CTX[message.chat.id] = {"ym": ym, "step": "idle"}
        logging.info(f"MANUAL_CTX set for chat_id={message.chat.id} ym={ym!r} step='idle'")
        return

    msg = f"Принято. (meter_index={assigned})"
    if ocr_type or ocr_reading:
        msg += f"\nРаспознано: {ocr_type or '—'} / {ocr_reading or '—'}"
    await message.reply(msg, reply_markup=_kb_main())

    dup = _extract_duplicate_info(js)
    photo_event_id = js.get("photo_event_id")
    if dup and ym and photo_event_id:
        DUP_PENDING[int(photo_event_id)] = {"ym": ym, "dup": dup}
        await message.reply(
            "Похоже, вы прислали одно и то же фото/значение для разных счётчиков.\n\n"
            "Выберите, что делать дальше:",
            reply_markup=_kb_duplicate(int(photo_event_id)),
        )
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


@dp.message_handler(content_types=ContentType.PHOTO)
async def on_photo(message: types.Message):
    photo = message.photo[-1]
    f = await bot.get_file(photo.file_id)
    stream = await bot.download_file(f.file_path)
    await _handle_file_message(
        message,
        file_bytes=stream.read(),
        filename=f"photo_{photo.file_unique_id}.jpg",
        mime_type="image/jpeg",
    )


@dp.message_handler(content_types=ContentType.DOCUMENT)
async def on_document(message: types.Message):
    doc = message.document
    f = await bot.get_file(doc.file_id)
    stream = await bot.download_file(f.file_path)
    await _handle_file_message(
        message,
        file_bytes=stream.read(),
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


# -------------------------
# Callback: duplicates
# -------------------------

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("dup_ok|"))
async def on_dup_ok(call: types.CallbackQuery):
    try:
        _, peid_raw = call.data.split("|", 1)
        photo_event_id = int(peid_raw)
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    payload = DUP_PENDING.pop(photo_event_id, None)
    ym = (payload or {}).get("ym") if isinstance(payload, dict) else None

    await call.answer("Ок", show_alert=False)

    res = await _resolve_duplicate(photo_event_id, "ok")
    bill = None
    if isinstance(res, dict):
        bill = (res.get("bill") or None)

    if ym and not bill:
        bill = await _fetch_bill(call.message.chat.id, ym)

    if ym and bill:
        out = _try_send_bill_if_ready(call.message.chat.id, ym, bill)
        if out:
            text, kb = out
            await bot.send_message(call.message.chat.id, text, reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("dup_repeat|"))
async def on_dup_repeat(call: types.CallbackQuery):
    try:
        _, peid_raw = call.data.split("|", 1)
        photo_event_id = int(peid_raw)
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    payload = DUP_PENDING.pop(photo_event_id, None)
    ym = (payload or {}).get("ym") if isinstance(payload, dict) else None
    dup = (payload or {}).get("dup") if isinstance(payload, dict) else None

    await call.answer("Ок", show_alert=False)

    await _resolve_duplicate(photo_event_id, "repeat")

    extra = ""
    if isinstance(dup, dict):
        mt = dup.get("meter_type")
        mi = dup.get("meter_index")
        val = dup.get("value")
        extra = f"\n(Повтор: {mt} idx={mi}, значение={val})"

    await bot.send_message(
        call.message.chat.id,
        "Понял. Тогда пришлите, пожалуйста, другое фото нужного счётчика." + extra,
        reply_markup=_kb_main(),
    )

    if ym:
        bill = await _fetch_bill(call.message.chat.id, ym)
        if bill and bill.get("reason") == "missing_photos":
            missing = bill.get("missing") or []
            await bot.send_message(
                call.message.chat.id,
                "Сейчас не хватает: " + _missing_to_text(missing),
                reply_markup=_kb_main(),
            )


if __name__ == "__main__":
    executor.start_polling(
        dp,
        skip_updates=True,
        allowed_updates=["message", "callback_query"]
    )
