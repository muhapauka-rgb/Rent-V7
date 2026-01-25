import os
import asyncio
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import (
    ContentType,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_BASE = os.getenv("API_BASE", "http://api:8000").strip()

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# chat_id -> phone
CHAT_PHONES: dict[int, str] = {}
# chat_id -> 1..3 (какой индекс электро ожидаем на следующий файл)
CHAT_METER_INDEX: dict[int, int] = {}

# чтобы не слать сумму повторно десять раз за один месяц
SENT_BILL: set[tuple[int, str]] = set()          # (chat_id, ym)
PENDING_NOTICE: set[tuple[int, str]] = set()     # (chat_id, ym) — чтобы не спамить "ждём админа"
REMIND_TASKS: dict[tuple[int, str], asyncio.Task] = {}

# подтверждение “похоже, прислали одно и то же”
DUP_PENDING: dict[tuple[int, str], dict] = {}    # (chat_id, ym) -> info


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


def _kb_duplicate(ym: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Это разные счётчики (оставить)", callback_data=f"dup_ok|{ym}"),
            ],
            [
                InlineKeyboardButton(text="Это повтор (пришлю другое фото)", callback_data=f"dup_repeat|{ym}"),
            ],
        ]
    )


def _get_meter_index(chat_id: int) -> int:
    v = CHAT_METER_INDEX.get(chat_id, 1)
    try:
        v = int(v)
    except Exception:
        v = 1
    return max(1, min(3, v))


def _set_meter_index(chat_id: int, idx: int) -> None:
    try:
        idx = int(idx)
    except Exception:
        idx = 1
    CHAT_METER_INDEX[chat_id] = max(1, min(3, idx))


def _post_photo_event(
    *,
    chat_id: int,
    telegram_username: str | None,
    phone: str | None,
    meter_index: int,
    file_bytes: bytes,
    filename: str,
    mime_type: str,
):
    url = f"{API_BASE}/events/photo"
    files = {"file": (filename or "file.bin", file_bytes, mime_type or "application/octet-stream")}
    data = {
        "chat_id": str(chat_id),
        "telegram_username": telegram_username or "",
        "phone": phone or "",
        "meter_index": str(meter_index),
    }
    return requests.post(url, data=data, files=files, timeout=60)


def _extract_duplicate_info(js: dict) -> dict | None:
    diag = js.get("diag") or {}
    warnings = diag.get("warnings") or []
    for w in warnings:
        if isinstance(w, dict) and "possible_duplicate" in w:
            return w.get("possible_duplicate")
    return None


def _missing_to_text(missing: list[str]) -> str:
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


async def _fetch_bill(chat_id: int, ym: str) -> dict | None:
    url = f"{API_BASE}/bot/chats/{chat_id}/bill"
    try:
        resp = requests.get(url, params={"ym": ym}, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data.get("bill")
    except Exception:
        return None


def _try_send_bill_if_ready(chat_id: int, ym: str, bill: dict):
    if not bill:
        return None

    if (chat_id, ym) in DUP_PENDING:
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
        return (f"Готово. Сумма за {ym}: {float(total_rub):.2f} ₽", None)

    return None


def _schedule_missing_reminder(chat_id: int, ym: str):
    key = (chat_id, ym)

    t = REMIND_TASKS.get(key)
    if t and not t.done():
        t.cancel()

    async def _job():
        try:
            # ВАЖНО: таймер напоминания = 40 секунд (по твоему пункту 3)
            await asyncio.sleep(40)

            if key in SENT_BILL:
                return
            if key in DUP_PENDING:
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


@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    _set_meter_index(message.chat.id, 1)
    await message.reply(
        "Привет. Чтобы привязать квартиру, нажми «Поделиться контактом».\n"
        "Дальше присылай фото счётчиков.\n"
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

    r = _post_photo_event(
        chat_id=message.chat.id,
        telegram_username=username,
        phone=phone,
        meter_index=meter_index,
        file_bytes=file_bytes,
        filename=filename,
        mime_type=mime_type,
    )

    if not r.ok:
        await message.reply(f"Ошибка отправки в backend: HTTP {r.status_code}", reply_markup=_kb_main())
        return

    try:
        js = r.json()
    except Exception:
        await message.reply("Принято, но ответ backend не JSON.", reply_markup=_kb_main())
        return

    ym = js.get("ym") or ""
    assigned = js.get("assigned_meter_index", meter_index)
    ocr = js.get("ocr") or {}
    ocr_type = ocr.get("type")
    ocr_reading = ocr.get("reading")

    msg = f"Принято. (meter_index={assigned})"
    if ocr_type or ocr_reading:
        msg += f"\nРаспознано: {ocr_type or '—'} / {ocr_reading or '—'}"
    await message.reply(msg, reply_markup=_kb_main())

    dup = _extract_duplicate_info(js)
    if dup and ym:
        DUP_PENDING[(message.chat.id, ym)] = dup
        await message.reply(
            "Похоже, вы прислали одно и то же фото/значение для разных счётчиков.\n"
            "Уточните, пожалуйста:",
            reply_markup=_kb_duplicate(ym),
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


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("dup_ok|"))
async def on_dup_ok(call: types.CallbackQuery):
    try:
        _, ym = call.data.split("|", 1)
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    key = (call.message.chat.id, ym)
    DUP_PENDING.pop(key, None)
    await call.answer("Ок", show_alert=False)

    bill = await _fetch_bill(call.message.chat.id, ym)
    if bill:
        res = _try_send_bill_if_ready(call.message.chat.id, ym, bill)
        if res:
            text, kb = res
            await bot.send_message(call.message.chat.id, text, reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("dup_repeat|"))
async def on_dup_repeat(call: types.CallbackQuery):
    try:
        _, ym = call.data.split("|", 1)
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    key = (call.message.chat.id, ym)
    info = DUP_PENDING.pop(key, None)
    await call.answer("Ок", show_alert=False)

    extra = ""
    if isinstance(info, dict):
        mt = info.get("meter_type")
        mi = info.get("meter_index")
        val = info.get("value")
        extra = f"\n(Повтор: {mt} idx={mi}, значение={val})"

    await bot.send_message(
        call.message.chat.id,
        "Понял. Тогда пришлите, пожалуйста, другое фото нужного счётчика." + extra,
        reply_markup=_kb_main(),
    )


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
