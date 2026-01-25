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

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN env var for telegram-bot service.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# chat_id -> phone (в памяти; после рестарта бота забудется)
CHAT_PHONES: dict[int, str] = {}
# chat_id -> 1..3 (какой индекс электро ожидаем на следующий файл)
CHAT_METER_INDEX: dict[int, int] = {}

# чтобы не слать сумму повторно десять раз за один месяц
SENT_BILL: set[tuple[int, str]] = set()          # (chat_id, ym)
PENDING_NOTICE: set[tuple[int, str]] = set()     # (chat_id, ym) — чтобы не спамить "ждём админа"
REMIND_TASKS: dict[tuple[int, str], asyncio.Task] = {}

# подтверждение “похоже, прислали одно и то же”
# photo_event_id -> {"chat_id": int, "ym": str, "dup": dict}
DUP_PENDING: dict[int, dict] = {}
# чтобы не отправлять сумму, пока человек не нажал кнопку (chat_id, ym)
DUP_PENDING_MONTH: set[tuple[int, str]] = set()

# ручной ввод:
# chat_id -> {"ym": str, "code": str}  (ждём число)
MANUAL_AWAIT_VALUE: dict[int, dict] = {}
# chat_id -> {"ym": str}  (ждём выбор типа из missing)
MANUAL_AWAIT_PICK: dict[int, dict] = {}


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
            [InlineKeyboardButton(text="Это разные счётчики (оставить)", callback_data=f"dup_ok|{photo_event_id}")],
            [InlineKeyboardButton(text="Это повтор (пришлю другое фото)", callback_data=f"dup_repeat|{photo_event_id}")],
        ]
    )


def _kb_manual_start(ym: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Ввести вручную", callback_data=f"mstart|{ym}")],
        ]
    )


def _kb_manual_pick(ym: str, missing: list[str]) -> InlineKeyboardMarkup:
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
    rows = []
    used = set()
    for code in (missing or []):
        code = str(code)
        if code in used:
            continue
        used.add(code)
        title = mapping.get(code, code)
        rows.append([InlineKeyboardButton(text=title, callback_data=f"mpick|{ym}|{code}")])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data="mcancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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


def _parse_number(text: str) -> float | None:
    if text is None:
        return None
    s = str(text).strip().replace(",", ".")
    s = s.replace(" ", "")
    try:
        v = float(s)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _code_to_manual_payload(code: str) -> tuple[str, int] | None:
    code = str(code).strip().lower()
    if code == "cold":
        return ("cold", 1)
    if code == "hot":
        return ("hot", 1)
    if code == "sewer":
        return ("sewer", 1)
    if code in ("electric_1", "electric_t1"):
        return ("electric", 1)
    if code in ("electric_2", "electric_t2"):
        return ("electric", 2)
    if code in ("electric_3", "electric_t3"):
        return ("electric", 3)
    return None


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


async def _resolve_duplicate(photo_event_id: int, action: str) -> dict | None:
    url = f"{API_BASE}/bot/duplicate/resolve"
    try:
        resp = requests.post(url, json={"photo_event_id": int(photo_event_id), "action": str(action)}, timeout=20)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


async def _post_manual_reading(chat_id: int, ym: str, code: str, value: float) -> dict | None:
    mapped = _code_to_manual_payload(code)
    if not mapped:
        return {"ok": False, "reason": "bad_code"}
    meter_type, meter_index = mapped
    url = f"{API_BASE}/bot/manual-reading"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id": str(chat_id),
                "ym": str(ym),
                "meter_type": str(meter_type),
                "meter_index": int(meter_index),
                "value": float(value),
            },
            timeout=20,
        )
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def _final_bill_text(ym: str, total_rub: float) -> str:
    return f"Спасибо за фото, все данные учтены. Сумма оплаты по счётчикам за {ym}: {float(total_rub):.2f} ₽"


def _try_send_bill_if_ready(chat_id: int, ym: str, bill: dict):
    if not bill:
        return None

    if (chat_id, ym) in DUP_PENDING_MONTH:
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
        return (_final_bill_text(ym, float(total_rub)), None)

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
            if key in DUP_PENDING_MONTH:
                return
            if chat_id in MANUAL_AWAIT_VALUE or chat_id in MANUAL_AWAIT_PICK:
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
    chat_id = message.chat.id
    txt_raw = message.text or ""
    txt = txt_raw.strip().lower()

    # 0) ждём ручной ввод числа
    if chat_id in MANUAL_AWAIT_VALUE:
        ctx = MANUAL_AWAIT_VALUE.get(chat_id) or {}
        ym = ctx.get("ym")
        code = ctx.get("code")
        val = _parse_number(txt_raw)
        if val is None:
            await message.reply("Введите, пожалуйста, число (пример: 123.45).")
            return

        resp = await _post_manual_reading(chat_id, str(ym), str(code), float(val))
        MANUAL_AWAIT_VALUE.pop(chat_id, None)

        if not resp or not isinstance(resp, dict):
            await message.reply("Ошибка: не удалось сохранить значение. Попробуйте ещё раз или пришлите фото.")
            return

        if not resp.get("ok"):
            reason = resp.get("reason") or "error"
            if reason == "month_closed":
                await message.reply("Этот месяц уже закрыт. Обратитесь к администратору.", reply_markup=_kb_main())
                return
            if reason == "not_bound":
                await message.reply("Квартира не привязана. Нажмите «Поделиться контактом».", reply_markup=_kb_main())
                return
            await message.reply("Не удалось сохранить значение. Пришлите фото или попробуйте ещё раз.", reply_markup=_kb_main())
            return

        bill = resp.get("bill")
        if not bill:
            bill = await _fetch_bill(chat_id, str(ym))

        if bill:
            out = _try_send_bill_if_ready(chat_id, str(ym), bill)
            if out:
                text, kb = out
                await bot.send_message(chat_id, text, reply_markup=kb)
            else:
                if bill.get("reason") == "missing_photos":
                    missing = bill.get("missing") or []
                    if missing:
                        await bot.send_message(chat_id, "Сейчас не хватает: " + _missing_to_text(missing), reply_markup=_kb_main())
        return

    # 1) переключатели
    if txt == "электро t1":
        _set_meter_index(chat_id, 1)
        await message.reply("Ок: Электро T1.", reply_markup=_kb_main())
        return
    if txt == "электро t2":
        _set_meter_index(chat_id, 2)
        await message.reply("Ок: Электро T2.", reply_markup=_kb_main())
        return
    if txt == "электро t3":
        _set_meter_index(chat_id, 3)
        await message.reply("Ок: Электро T3.", reply_markup=_kb_main())
        return
    if txt == "вода (хвс/гвс)":
        _set_meter_index(chat_id, 1)
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
    bill = js.get("bill") if isinstance(js.get("bill"), dict) else None

    # месяц закрыт
    if str(js.get("reason") or "") == "month_closed":
        await message.reply("Этот месяц уже закрыт. Если нужно внести изменения — обратитесь к администратору.", reply_markup=_kb_main())
        return

    # OCR не распознал
    if bool(js.get("ocr_failed")) or str(js.get("reason") or "") == "ocr_unreadable":
        await message.reply(
            "Фото получено, но не удалось распознать показания (нечётко/блики/обрезано).\n"
            "Пожалуйста, пришлите фото лучшего качества.\n\n"
            "Если удобнее — можно ввести вручную (только для незаполненных полей).",
            reply_markup=_kb_main(),
        )
        if ym:
            MANUAL_AWAIT_PICK[message.chat.id] = {"ym": str(ym)}
            await message.reply("Нажмите, чтобы ввести вручную:", reply_markup=_kb_manual_start(str(ym)))
        return

    # обычный ответ
    assigned = js.get("assigned_meter_index", meter_index)
    ocr = js.get("ocr") or {}
    ocr_type = ocr.get("type")
    ocr_reading = ocr.get("reading")

    msg = f"Принято. (meter_index={assigned})"
    if ocr_type or ocr_reading:
        msg += f"\nРаспознано: {ocr_type or '—'} / {ocr_reading or '—'}"
    await message.reply(msg, reply_markup=_kb_main())

    # дубль
    dup = _extract_duplicate_info(js)
    photo_event_id = js.get("photo_event_id")
    if dup and ym and photo_event_id:
        peid = int(photo_event_id)
        DUP_PENDING[peid] = {"chat_id": message.chat.id, "ym": str(ym), "dup": dup}
        DUP_PENDING_MONTH.add((message.chat.id, str(ym)))
        await message.reply(
            "Похоже, вы прислали одно и то же фото/значение для разных счётчиков.\n\n"
            "Выберите, что делать дальше:",
            reply_markup=_kb_duplicate(peid),
        )
        return

    # bill
    if ym and bill:
        res = _try_send_bill_if_ready(message.chat.id, str(ym), bill)
        if res:
            text, kb = res
            await message.reply(text, reply_markup=kb)
        else:
            if bill.get("reason") == "missing_photos":
                _schedule_missing_reminder(message.chat.id, str(ym))


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


@dp.callback_query_handler(lambda c: c.data == "mcancel")
async def on_manual_cancel(call: types.CallbackQuery):
    chat_id = call.message.chat.id if call.message else None
    if chat_id is not None:
        MANUAL_AWAIT_PICK.pop(int(chat_id), None)
        MANUAL_AWAIT_VALUE.pop(int(chat_id), None)
    await call.answer("Отменено", show_alert=False)
    if call.message:
        await bot.send_message(call.message.chat.id, "Ок. Пришлите фото счётчика.", reply_markup=_kb_main())


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("mstart|"))
async def on_manual_start(call: types.CallbackQuery):
    try:
        _, ym = call.data.split("|", 1)
        ym = str(ym)
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    chat_id = call.message.chat.id if call.message else None
    if chat_id is None:
        await call.answer("Ошибка", show_alert=True)
        return

    await call.answer("Ок", show_alert=False)

    bill = await _fetch_bill(int(chat_id), ym)
    if not bill:
        await bot.send_message(int(chat_id), "Не удалось получить список незаполненных полей. Пришлите фото.", reply_markup=_kb_main())
        return

    missing = bill.get("missing") or []
    if not missing:
        await bot.send_message(int(chat_id), "Сейчас нет незаполненных полей. Если нужно — пришлите фото.", reply_markup=_kb_main())
        return

    MANUAL_AWAIT_PICK[int(chat_id)] = {"ym": ym}
    await bot.send_message(
        int(chat_id),
        "Выберите, что именно не заполнено (введите только это поле):",
        reply_markup=_kb_manual_pick(ym, list(missing)),
    )


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("mpick|"))
async def on_manual_pick(call: types.CallbackQuery):
    try:
        _, ym, code = call.data.split("|", 2)
        ym = str(ym)
        code = str(code)
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    chat_id = call.message.chat.id if call.message else None
    if chat_id is None:
        await call.answer("Ошибка", show_alert=True)
        return

    await call.answer("Ок", show_alert=False)
    MANUAL_AWAIT_PICK.pop(int(chat_id), None)
    MANUAL_AWAIT_VALUE[int(chat_id)] = {"ym": ym, "code": code}

    await bot.send_message(int(chat_id), "Введите показание числом (пример: 123.45).", reply_markup=_kb_main())


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("dup_ok|"))
async def on_dup_ok(call: types.CallbackQuery):
    try:
        _, peid_raw = call.data.split("|", 1)
        photo_event_id = int(peid_raw)
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    payload = DUP_PENDING.pop(photo_event_id, None) or {}
    ym = payload.get("ym")
    chat_id = payload.get("chat_id") or (call.message.chat.id if call.message else None)

    if chat_id and ym:
        DUP_PENDING_MONTH.discard((int(chat_id), str(ym)))

    await call.answer("Ок", show_alert=False)

    res = await _resolve_duplicate(photo_event_id, "ok")
    bill = None
    if isinstance(res, dict):
        bill = res.get("bill") or None
        ym = res.get("ym") or ym

    if ym and not bill and chat_id:
        bill = await _fetch_bill(int(chat_id), str(ym))

    if ym and bill and chat_id:
        out = _try_send_bill_if_ready(int(chat_id), str(ym), bill)
        if out:
            text, kb = out
            await bot.send_message(int(chat_id), text, reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("dup_repeat|"))
async def on_dup_repeat(call: types.CallbackQuery):
    try:
        _, peid_raw = call.data.split("|", 1)
        photo_event_id = int(peid_raw)
    except Exception:
        await call.answer("Ошибка", show_alert=True)
        return

    payload = DUP_PENDING.pop(photo_event_id, None) or {}
    ym = payload.get("ym")
    chat_id = payload.get("chat_id") or (call.message.chat.id if call.message else None)
    dup = payload.get("dup") if isinstance(payload, dict) else None

    if chat_id and ym:
        DUP_PENDING_MONTH.discard((int(chat_id), str(ym)))

    await call.answer("Ок", show_alert=False)

    await _resolve_duplicate(photo_event_id, "repeat")

    extra = ""
    if isinstance(dup, dict):
        mt = dup.get("meter_type")
        mi = dup.get("meter_index")
        val = dup.get("value")
        extra = f"\n(Повтор: {mt} idx={mi}, значение={val})"

    if chat_id:
        await bot.send_message(
            int(chat_id),
            "Понял. Тогда пришлите, пожалуйста, другое фото нужного счётчика." + extra,
            reply_markup=_kb_main(),
        )

    if ym and chat_id:
        bill = await _fetch_bill(int(chat_id), str(ym))
        if bill and bill.get("reason") == "missing_photos":
            missing = bill.get("missing") or []
            await bot.send_message(int(chat_id), "Сейчас не хватает: " + _missing_to_text(missing), reply_markup=_kb_main())


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
