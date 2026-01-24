import os
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

CHAT_PHONES = {}        # chat_id -> phone
CHAT_METER_INDEX = {}   # chat_id -> 1..3 (по умолчанию 1)

# чтобы не слать сумму повторно десять раз за один месяц
SENT_BILL = set()       # (chat_id, apartment_id, ym)


def _kb_main():
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        row_width=2,
        keyboard=[
            [KeyboardButton("Поделиться контактом", request_contact=True)],
            [KeyboardButton("Электро T1"), KeyboardButton("Электро T2")],
            [KeyboardButton("Электро T3"), KeyboardButton("Вода (ХВС/ГВС)")],
        ],
    )


def _kb_paid(apartment_id: int, ym: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Оплатил аренду",
                    callback_data=f"rent_paid|{apartment_id}|{ym}",
                ),
                InlineKeyboardButton(
                    text="Оплатил счётчики",
                    callback_data=f"meters_paid|{apartment_id}|{ym}",
                ),
            ]
        ]
    )


def _get_meter_index(chat_id: int) -> int:
    v = CHAT_METER_INDEX.get(chat_id, 1)
    try:
        v = int(v)
    except Exception:
        v = 1
    if v < 1:
        v = 1
    if v > 3:
        v = 3
    return v


def _set_meter_index(chat_id: int, idx: int) -> None:
    try:
        idx = int(idx)
    except Exception:
        idx = 1
    if idx < 1:
        idx = 1
    if idx > 3:
        idx = 3
    CHAT_METER_INDEX[chat_id] = idx


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
    files = {
        "file": (filename or "file.bin", file_bytes, mime_type or "application/octet-stream"),
    }
    data = {
        "chat_id": str(chat_id),
        "telegram_username": telegram_username or "",
        "phone": phone or "",
        "meter_index": str(meter_index),
    }
    return requests.post(url, data=data, files=files, timeout=60)


def _try_send_bill_if_ready(chat_id: int, ym: str, bill: dict):
    """
    Возвращает (text, reply_markup) если надо что-то отправить пользователю, иначе None.
    """
    if not bill:
        return None

    reason = bill.get("reason")  # например: "pending_admin"
    is_complete = bool(bill.get("is_complete_photos"))
    total_rub = bill.get("total_rub")

    # Блокируем выдачу суммы, если требуется решение администратора
    if reason == "pending_admin":
        key = (chat_id, ym)
        if key not in PENDING_NOTICE:
            PENDING_NOTICE.add(key)
            return ("Фото получены. Данные требуют проверки администратором. Итоговую сумму пришлю после подтверждения.", None)
        return None

    # Если всё собрано — отправляем сумму один раз
    if is_complete and total_rub is not None:
        key = (chat_id, ym)
        if key in SENT_BILL:
            return None
        SENT_BILL.add(key)
        # если раньше было уведомление “ждём админа” — снимаем (на всякий случай)
        PENDING_NOTICE.discard(key)

        text = f"Готово. Сумма за {ym}: {total_rub:.2f} ₽"
        return (text, None)

    return None

def _missing_to_text(missing: list[str]) -> str:
    # Приводим к коротким русским названиям
    mapping = {
        "cold": "ХВС",
        "hot": "ГВС",
        "electric_t1": "Электро T1",
        "electric_t2": "Электро T2",
        "electric_t3": "Электро T3",
        "sewer": "Водоотведение",
    }
    nice = []
    for m in missing or []:
        nice.append(mapping.get(m, m))
    # уникализируем, сохраняя порядок
    out = []
    for x in nice:
        if x not in out:
            out.append(x)
    return ", ".join(out)

async def _fetch_bill(chat_id: int, ym: str) -> dict | None:
    # backend: /bot/chats/{chat_id}/bill?ym=YYYY-MM
    url = f"{API_BASE}/bot/chats/{chat_id}/bill"
    try:
        resp = requests.get(url, params={"ym": ym}, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data.get("bill")
    except Exception:
        return None

def _schedule_missing_reminder(chat_id: int, ym: str):
    key = (chat_id, ym)

    # отменяем предыдущий таймер
    t = REMIND_TASKS.get(key)
    if t and not t.done():
        t.cancel()

    async def _job():
        try:
            await asyncio.sleep(30)

            # если уже отправили сумму — не пишем
            if key in SENT_BILL:
                return

            bill = await _fetch_bill(chat_id, ym)
            if not bill:
                return

            if bill.get("reason") == "pending_admin":
                # в этом случае мы НЕ просим ещё фото
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
        "Ок. Пришли фото счётчика.\n\n"
        "1) Для авто-поиска квартиры отправь контакт.\n"
        "2) Для электричества выбери режим: Электро T1/T2/T3.\n"
        "3) Для воды нажми «Вода (ХВС/ГВС)».\n",
        reply_markup=_kb_main(),
    )


@dp.message_handler(commands=["t1", "t2", "t3", "water"])
async def cmd_set_mode(message: types.Message):
    cmd = (message.get_command() or "").lower()
    if cmd == "/t1":
        _set_meter_index(message.chat.id, 1)
        await message.reply("Ок: Электро T1 (meter_index=1).", reply_markup=_kb_main())
    elif cmd == "/t2":
        _set_meter_index(message.chat.id, 2)
        await message.reply("Ок: Электро T2 (meter_index=2).", reply_markup=_kb_main())
    elif cmd == "/t3":
        _set_meter_index(message.chat.id, 3)
        await message.reply("Ок: Электро T3 (meter_index=3).", reply_markup=_kb_main())
    elif cmd == "/water":
        _set_meter_index(message.chat.id, 1)
        await message.reply("Ок: Вода (meter_index=1).", reply_markup=_kb_main())


@dp.message_handler(content_types=ContentType.TEXT)
async def on_text(message: types.Message):
    txt = (message.text or "").strip().lower()

    if txt == "электро t1":
        _set_meter_index(message.chat.id, 1)
        await message.reply("Ок: Электро T1 (meter_index=1).", reply_markup=_kb_main())
        return
    if txt == "электро t2":
        _set_meter_index(message.chat.id, 2)
        await message.reply("Ок: Электро T2 (meter_index=2).", reply_markup=_kb_main())
        return
    if txt == "электро t3":
        _set_meter_index(message.chat.id, 3)
        await message.reply("Ок: Электро T3 (meter_index=3).", reply_markup=_kb_main())
        return
    if txt == "вода (хвс/гвс)":
        _set_meter_index(message.chat.id, 1)
        await message.reply("Ок: Вода (meter_index=1).", reply_markup=_kb_main())
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


@dp.message_handler(content_types=ContentType.PHOTO)
async def on_photo(message: types.Message):
    photo = message.photo[-1]
    f = await bot.get_file(photo.file_id)
    stream = await bot.download_file(f.file_path)

    username = message.from_user.username if message.from_user else None
    phone = CHAT_PHONES.get(message.chat.id)
    meter_index = _get_meter_index(message.chat.id)

    r = _post_photo_event(
        chat_id=message.chat.id,
        telegram_username=username,
        phone=phone,
        meter_index=meter_index,
        file_bytes=stream.read(),
        filename=f"photo_{photo.file_unique_id}.jpg",
        mime_type="image/jpeg",
    )

    if not r.ok:
        await message.reply(f"Ошибка отправки в backend: HTTP {r.status_code}", reply_markup=_kb_main())
        return

    try:
        js = r.json()
    except Exception:
        await message.reply("Фото принято, но ответ backend не JSON.", reply_markup=_kb_main())
        return

    await message.reply(f"Фото принято. (meter_index={js.get('assigned_meter_index', meter_index)})", reply_markup=_kb_main())
    await _maybe_send_duplicate_prompt(message, js)

    await _maybe_send_duplicate_prompt(message, js)


    # если готовы все показания и есть сумма — отправим отдельным сообщением + кнопки
    res = _try_send_bill_if_ready(message.chat.id, js)
    if res:
        text, kb = res
        await message.reply(text, reply_markup=kb)


@dp.message_handler(content_types=ContentType.DOCUMENT)
async def on_document(message: types.Message):
    doc = message.document
    f = await bot.get_file(doc.file_id)
    stream = await bot.download_file(f.file_path)

    username = message.from_user.username if message.from_user else None
    phone = CHAT_PHONES.get(message.chat.id)
    meter_index = _get_meter_index(message.chat.id)

    r = _post_photo_event(
        chat_id=message.chat.id,
        telegram_username=username,
        phone=phone,
        meter_index=meter_index,
        file_bytes=stream.read(),
        filename=doc.file_name or "file.bin",
        mime_type=doc.mime_type or "application/octet-stream",
    )

    if not r.ok:
        await message.reply(f"Ошибка отправки в backend: HTTP {r.status_code}", reply_markup=_kb_main())
        return

    try:
        js = r.json()
    except Exception:
        await message.reply("Файл принят, но ответ backend не JSON.", reply_markup=_kb_main())
        return

    await message.reply(f"Файл принят. (meter_index={js.get('assigned_meter_index', meter_index)})", reply_markup=_kb_main())

    res = _try_send_bill_if_ready(message.chat.id, js)
    if res:
        text, kb = res
        await message.reply(text, reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data and (c.data.startswith("dup_ok|") or c.data.startswith("dup_new|")))
async def on_dup_callback(call: types.CallbackQuery):
    try:
        parts = (call.data or "").split("|")
        action = parts[0] if len(parts) > 0 else ""
        # parts[1]=meter_type, parts[2]=meter_index, parts[3]=ym (нам не обязательно)
        if action == "dup_ok":
            await call.answer("Принято")
            try:
                await call.message.edit_text("Ок, считаем что все верно.")
            except Exception:
                pass
        else:
            await call.answer("Ок")
            try:
                await call.message.edit_text("Ок, пришли другое фото (можно просто сразу отправить новое).")
            except Exception:
                pass
    except Exception:
        await call.answer("Ошибка", show_alert=True)


@dp.callback_query_handler(lambda c: c.data and (c.data.startswith("rent_paid|") or c.data.startswith("meters_paid|")))

async def on_paid_callback(call: types.CallbackQuery):
    try:
        parts = (call.data or "").split("|")
        action = parts[0]
        apartment_id = int(parts[1])
        ym = parts[2]

        if action == "rent_paid":
            url = f"{API_BASE}/bot/apartments/{apartment_id}/months/{ym}/rent-paid"
        else:
            url = f"{API_BASE}/bot/apartments/{apartment_id}/months/{ym}/meters-paid"

        rr = requests.post(url, timeout=20)
        if rr.ok:
            await call.answer("Готово")
            # обновим текст сообщения (коротко)
            try:
                txt = call.message.text or ""
                if action == "rent_paid":
                    txt2 = txt + "\n\nОтмечено: аренда оплачена."
                else:
                    txt2 = txt + "\n\nОтмечено: счётчики оплачены."
                await call.message.edit_text(txt2)
            except Exception:
                pass
        else:
            await call.answer("Ошибка backend", show_alert=True)
    except Exception:
        await call.answer("Ошибка", show_alert=True)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
