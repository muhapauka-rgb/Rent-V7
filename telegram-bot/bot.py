import os
import asyncio
import re
import requests
import io
import uuid
import aiohttp
import time
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
TG_FETCH_RETRIES = max(1, int(os.getenv("TG_FETCH_RETRIES", "10")))
TG_FETCH_RETRY_BASE_SEC = float(os.getenv("TG_FETCH_RETRY_BASE_SEC", "0.8"))
TG_FETCH_RETRY_MAX_SEC = float(os.getenv("TG_FETCH_RETRY_MAX_SEC", "15"))
MEDIA_GROUP_MAX_WAIT_SEC = float(os.getenv("MEDIA_GROUP_MAX_WAIT_SEC", "90"))
TG_FETCH_RETRY_EXC = (
    tg_exceptions.NetworkError,
    aiohttp.ClientError,
    asyncio.TimeoutError,
    OSError,
    ConnectionError,
)


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


def _tg_fetch_retry_delay(attempt: int) -> float:
    return min(float(TG_FETCH_RETRY_MAX_SEC), float(TG_FETCH_RETRY_BASE_SEC) * (1.7 ** max(0, attempt)))


async def _retry_tg_fetch(coro_factory, *, op: str, file_id: str):
    last_exc = None
    for attempt in range(max(1, TG_FETCH_RETRIES)):
        try:
            return await coro_factory()
        except TG_FETCH_RETRY_EXC as e:
            last_exc = e
            if attempt < max(1, TG_FETCH_RETRIES) - 1:
                delay = _tg_fetch_retry_delay(attempt)
                logging.warning(
                    "TG %s retry scheduled: file_id=%s attempt=%s/%s delay=%.2fs error=%s",
                    op,
                    file_id,
                    attempt + 1,
                    TG_FETCH_RETRIES,
                    delay,
                    e,
                )
                await asyncio.sleep(delay)
            else:
                raise
    if last_exc:
        raise last_exc


async def _download_tg_file(file_id: str) -> tuple[bytes, str]:
    f = await _retry_tg_fetch(lambda: bot.get_file(file_id), op="get_file", file_id=file_id)
    stream = await _retry_tg_fetch(lambda: bot.download_file(f.file_path), op="download_file", file_id=file_id)
    payload = stream.read()
    return payload, str(f.file_path or "")


def _track_background_task(task: asyncio.Task) -> None:
    BACKGROUND_FILE_TASKS.add(task)
    task.add_done_callback(lambda t: BACKGROUND_FILE_TASKS.discard(t))


async def _safe_progress_message(chat_id: int, text: str) -> None:
    try:
        await asyncio.wait_for(
            bot.send_message(chat_id, text, reply_markup=_kb_main(chat_id)),
            timeout=8.0,
        )
    except Exception as e:
        logging.warning("progress_message skipped: chat_id=%s error=%s", chat_id, e)


def _queue_progress_message(chat_id: int, text: str) -> None:
    _track_background_task(asyncio.create_task(_safe_progress_message(chat_id, text)))


def _start_delayed_progress_message(chat_id: int, text: str, *, delay_sec: float) -> Optional[asyncio.Task]:
    if delay_sec <= 0:
        return None

    async def _runner() -> None:
        try:
            await asyncio.sleep(delay_sec)
            await _safe_progress_message(chat_id, text)
        except asyncio.CancelledError:
            return
        except Exception:
            logging.exception("delayed_progress_message failed: chat_id=%s", chat_id)

    task = asyncio.create_task(_runner())
    _track_background_task(task)
    return task


def _cancel_background_task(task: Optional[asyncio.Task]) -> None:
    if task is not None and not task.done():
        task.cancel()


def _build_batch_result(kind: str, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"kind": kind}
    out.update(extra)
    return out


def _build_batch_summary(results: List[Dict[str, Any]]) -> Optional[str]:
    if len(results) <= 1:
        return None

    counts: Dict[str, int] = {}
    for item in results:
        kind = str(item.get("kind") or "unknown")
        counts[kind] = counts.get(kind, 0) + 1

    lines = ["Пачка обработана. Ответ по каждому фото уже отправил выше."]
    if counts.get("accepted"):
        lines.append(f"Записано: {counts['accepted']}")
    if counts.get("review"):
        lines.append(f"На проверке: {counts['review']}")
    if counts.get("manual"):
        lines.append(f"Нужно новое фото или ручной ввод: {counts['manual']}")
    if counts.get("duplicate"):
        lines.append(f"Дубликаты: {counts['duplicate']}")

    error_count = sum(counts.get(k, 0) for k in ("error", "backend_timeout", "backend_unavailable", "backend_http"))
    if error_count:
        lines.append(f"Технические ошибки: {error_count}")

    if len(lines) == 1:
        return None
    return "\n".join(lines)


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
MEDIA_GROUP_BUFFER: Dict[Tuple[int, str], List[Tuple[int, types.Message, bytes, str, str]]] = {}
MEDIA_GROUP_ANCHOR: Dict[Tuple[int, str], types.Message] = {}
MEDIA_GROUP_TASKS: Dict[Tuple[int, str], asyncio.Task] = {}
MEDIA_GROUP_PENDING: Dict[Tuple[int, str], int] = {}
MEDIA_GROUP_LAST_ACTIVITY: Dict[Tuple[int, str], float] = {}
MEDIA_GROUP_ACKED: set[Tuple[int, str]] = set()
MEDIA_GROUP_COLLECT_SEC = float(os.getenv("MEDIA_GROUP_COLLECT_SEC", "1.4"))
SEQUENTIAL_PHOTO_BUFFER: Dict[int, List[Tuple[bytes, str, str]]] = {}
SEQUENTIAL_PHOTO_ANCHOR: Dict[int, types.Message] = {}
SEQUENTIAL_PHOTO_TASKS: Dict[int, asyncio.Task] = {}
SEQUENTIAL_PHOTO_ACKED: set[int] = set()
SEQUENTIAL_PHOTO_COLLECT_SEC = float(os.getenv("SEQUENTIAL_PHOTO_COLLECT_SEC", "0.6"))
SEQUENTIAL_PHOTO_MAX_BATCH = max(1, int(os.getenv("SEQUENTIAL_PHOTO_MAX_BATCH", "4")))
BACKGROUND_FILE_TASKS: set[asyncio.Task] = set()
LONG_PROCESS_NOTICE_SEC = float(os.getenv("LONG_PROCESS_NOTICE_SEC", "18"))

# Manual entry flow
MANUAL_CTX: Dict[int, Dict[str, Any]] = {}       # chat_id -> {ym, missing, step, meter_type, meter_index}


# -------------------------
# Keyboards
# -------------------------

def _kb_main(chat_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    # Главная клавиатура: контакт + отметки оплат
    if chat_id is None:
        chat_id = CURRENT_CHAT_ID.get()
    show_contact = not (chat_id is not None and int(chat_id) in CONTACT_CONFIRMED)

    rows = []
    if show_contact:
        rows.append([KeyboardButton("Передать контакт", request_contact=True)])
    rows.extend(
        [
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
    meter_index_mode: str = "auto",
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
        "meter_index_mode": str(meter_index_mode or "auto"),
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
        "1) Пришли фото счётчиков (ХВС/ГВС/Электро).\n"
        "2) Когда оплатишь — нажми «Аренда оплачена» / «Счётчики оплачены».\n"
        "3) Здесь вы можете отправлять любые сообщения для администратора.",
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
    sys_texts = {"Аренда оплачена", "Счётчики оплачены", "Сообщить об ошибке распознавания"}
    if text_in and not text_in.startswith("/") and text_in not in sys_texts:
        username = message.from_user.username if message.from_user else None
        await _post_notification(message.chat.id, username, text_in, "user_message")
        return

    # Главные кнопки
    ym = _current_ym()

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
    response_prefix: str = "",
    allow_long_progress: bool = True,
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
        return _build_batch_result("error", reason="empty_payload")

    payload_summary = [
        {
            "filename": str(fn or "file.bin"),
            "bytes": len(b or b""),
            "mime": str(mt or "application/octet-stream"),
        }
        for b, fn, mt in payloads
    ]
    logging.info(
        "HANDLE_FILE start: chat_id=%s message_id=%s prefix=%r payloads=%s",
        message.chat.id,
        message.message_id,
        response_prefix,
        payload_summary,
    )

    async def _reply_here(label: str, text: str, **kwargs):
        logging.info(
            "HANDLE_FILE reply_start: chat_id=%s message_id=%s label=%s prefix=%r",
            message.chat.id,
            message.message_id,
            label,
            response_prefix,
        )
        try:
            resp = await message.reply(text, **kwargs)
            logging.info(
                "HANDLE_FILE reply_done: chat_id=%s message_id=%s label=%s reply_message_id=%s",
                message.chat.id,
                message.message_id,
                label,
                getattr(resp, "message_id", None),
            )
            return resp
        except Exception:
            logging.exception(
                "HANDLE_FILE reply_failed: chat_id=%s message_id=%s label=%s",
                message.chat.id,
                message.message_id,
                label,
            )
            raise

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

    delayed_notice_task: Optional[asyncio.Task] = None
    if allow_long_progress:
        delayed_notice_task = _start_delayed_progress_message(
            message.chat.id,
            f"{response_prefix}Распознавание еще идет. Как только закончим, сразу пришлю результат.",
            delay_sec=float(LONG_PROCESS_NOTICE_SEC),
        )

    try:
        try:
            logging.info(
                "HANDLE_FILE backend_start: chat_id=%s message_id=%s prefix=%r meter_index=%s payload_count=%s",
                message.chat.id,
                message.message_id,
                response_prefix,
                meter_index,
                len(payloads),
            )
            r = await _post_photo_event(
                chat_id=message.chat.id,
                telegram_username=username,
                phone=phone,
                ym=ym,
                meter_index=meter_index,
                meter_index_mode="auto",
                file_payloads=payloads,
            )
            logging.info(
                "HANDLE_FILE backend_done: chat_id=%s message_id=%s prefix=%r http=%s trace_id=%s server_trace_id=%s",
                message.chat.id,
                message.message_id,
                response_prefix,
                r.get("status_code"),
                r.get("trace_id"),
                r.get("server_trace_id"),
            )
        except requests.exceptions.ReadTimeout:
            await _reply_here(
                "backend_timeout",
                f"{response_prefix}Фото получено, но backend долго обрабатывает запрос (возможно загрузка на диск).\n"
                "Если итоговый ответ не придёт в течение пары минут, отправьте фото ещё раз.",
                reply_markup=_kb_main(),
            )
            logging.info(
                "HANDLE_FILE done: chat_id=%s message_id=%s result=%s",
                message.chat.id,
                message.message_id,
                "backend_timeout",
            )
            return _build_batch_result("backend_timeout")
        except Exception:
            logging.exception(
                "HANDLE_FILE backend_failed: chat_id=%s message_id=%s prefix=%r",
                message.chat.id,
                message.message_id,
                response_prefix,
            )
            await _reply_here(
                "backend_unavailable",
                f"{response_prefix}Фото получено, но backend сейчас недоступен. Попробуйте ещё раз позже.",
                reply_markup=_kb_main(),
            )
            logging.info(
                "HANDLE_FILE done: chat_id=%s message_id=%s result=%s",
                message.chat.id,
                message.message_id,
                "backend_unavailable",
            )
            return _build_batch_result("backend_unavailable")

        if not r.get("ok"):
            await _reply_here(
                "backend_http",
                f"{response_prefix}Ошибка отправки в backend: HTTP {r.get('status_code')}",
                reply_markup=_kb_main(),
            )
            logging.info(
                "HANDLE_FILE done: chat_id=%s message_id=%s result=%s status_code=%s",
                message.chat.id,
                message.message_id,
                "backend_http",
                r.get("status_code"),
            )
            return _build_batch_result("backend_http", status_code=r.get("status_code"))

        js = r.get("json") or {}
        ym = js.get("ym") or ""
        assigned = js.get("assigned_meter_index", meter_index)
        trace_id = js.get("trace_id") or r.get("server_trace_id") or r.get("trace_id")

        ocr = js.get("ocr") or {}
        ocr_type = ocr.get("type")
        ocr_reading = ocr.get("reading")
        ocr_conf = ocr.get("confidence")
        resolved_type = js.get("meter_type_label") or ocr.get("resolved_type") or ocr_type
        ocr_serial = ocr.get("serial")

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
            resolved_line = ""
            if resolved_type or ocr_serial:
                resolved_line = (
                    f"\n\nОпределили прибор: {resolved_type or '—'}"
                    + (f"\nСерийный номер: {ocr_serial}" if ocr_serial else "")
                )
            await _reply_here(
                "ocr_failed_manual",
                f"{response_prefix}Фото получено, но не удалось распознать показания (нечётко/блики/обрезано).\n"
                "Пожалуйста, пришлите фото лучшего качества.\n\n"
                "Если удобнее — можно ввести вручную (только для незаполненных полей)."
                f"{resolved_line}",
                reply_markup=_kb_manual_start(),
            )
            MANUAL_CTX[message.chat.id] = {"ym": ym, "step": "idle"}
            logging.info(f"MANUAL_CTX set for chat_id={message.chat.id} ym={ym!r} step='idle'")
            logging.info(
                "HANDLE_FILE done: chat_id=%s message_id=%s result=%s trace_id=%s",
                message.chat.id,
                message.message_id,
                "manual",
                trace_id,
            )
            return _build_batch_result("manual", resolved_type=resolved_type, serial=ocr_serial)

        if (meter_written is False) and (ocr_reading is not None):
            shown_reading = ocr_reading
            if isinstance(shown_reading, (int, float)):
                try:
                    shown_reading = f"{float(shown_reading):.2f}"
                except Exception:
                    pass
            conf_line = f"\nУверенность OCR: {conf_txt}" if conf_txt is not None else ""
            reason_line = f"\nПричина проверки: {review_reason}" if review_reason else ""
            serial_line = f"\nСерийный номер: {ocr_serial}" if ocr_serial else ""
            msg = (
                f"{response_prefix}Фото получено.\n"
                f"Распознано: {resolved_type or '—'} / {shown_reading}\n"
                f"Значение выглядит спорным: мы отметили «Проверить» для администратора."
                f"{serial_line}{conf_line}{reason_line}"
            )
            await _reply_here("review", msg, reply_markup=_kb_main())
            logging.info(
                "HANDLE_FILE done: chat_id=%s message_id=%s result=%s trace_id=%s",
                message.chat.id,
                message.message_id,
                "review",
                trace_id,
            )
            return _build_batch_result("review", resolved_type=resolved_type, reading=shown_reading, serial=ocr_serial)

        shown_reading = ocr_reading
        if shown_reading is None and isinstance(anomaly_info, dict):
            shown_reading = anomaly_info.get("curr")

        msg = f"{response_prefix}Принято. (meter_index={assigned})"
        if resolved_type or shown_reading is not None:
            msg += f"\nРаспознано: {resolved_type or '—'} / {shown_reading if shown_reading is not None else '—'}"
        if ocr_serial:
            msg += f"\nСерийный номер: {ocr_serial}"
        if conf_txt is not None:
            msg += f"\nУверенность OCR: {conf_txt}"
        if review_reason:
            msg += f"\nПричина проверки: {review_reason}"
        if anomaly_info:
            msg += "\nЗначение выглядит подозрительным, но мы сохранили его и отметили «Проверить значение» для администратора."
        await _reply_here("accepted", msg, reply_markup=_kb_main())


        dup = _extract_duplicate_info(js)
        if dup and ym:
            mt = dup.get("meter_type")
            mi = dup.get("meter_index")
            val = dup.get("value")
            caption = (
                f"{response_prefix}Похоже, это дубликат уже присланного значения.\n"
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
                await _reply_here("duplicate_fallback", caption, reply_markup=_kb_main())

            bill = js.get("bill")
            if isinstance(bill, dict) and bill.get("reason") == "missing_photos":
                missing = bill.get("missing") or []
                if missing:
                    await message.reply(f"{response_prefix}Сейчас не хватает: " + _missing_to_text(missing), reply_markup=_kb_main())
                    _schedule_missing_reminder(message.chat.id, ym)
            return _build_batch_result("duplicate", meter_type=mt, meter_index=mi, value=val)

        bill = js.get("bill")
        if ym and isinstance(bill, dict):
            res = _try_send_bill_if_ready(message.chat.id, ym, bill)
            if res:
                text, kb = res
                await message.reply(text, reply_markup=kb)
            else:
                if bill.get("reason") == "missing_photos":
                    _schedule_missing_reminder(message.chat.id, ym)

        return _build_batch_result(
            "accepted",
            meter_type=resolved_type,
            reading=shown_reading,
            serial=ocr_serial,
            meter_index=assigned,
        )
    finally:
        logging.info(
            "HANDLE_FILE finally: chat_id=%s message_id=%s prefix=%r",
            message.chat.id,
            message.message_id,
            response_prefix,
        )
        _cancel_background_task(delayed_notice_task)


async def _flush_media_group(key: Tuple[int, str]) -> None:
    batch_notice_task: Optional[asyncio.Task] = None
    batch_results: List[Dict[str, Any]] = []
    try:
        started_at = time.monotonic()
        while True:
            await asyncio.sleep(max(0.35, float(MEDIA_GROUP_COLLECT_SEC)))
            pending = int(MEDIA_GROUP_PENDING.get(key, 0) or 0)
            items = MEDIA_GROUP_BUFFER.get(key, [])
            anchor = MEDIA_GROUP_ANCHOR.get(key)
            last_activity = float(MEDIA_GROUP_LAST_ACTIVITY.get(key, started_at) or started_at)
            quiet_enough = (time.monotonic() - last_activity) >= max(0.35, float(MEDIA_GROUP_COLLECT_SEC))
            if pending <= 0 and anchor is not None and items and quiet_enough:
                break
            if pending <= 0 and not items:
                return
            if (time.monotonic() - started_at) >= float(MEDIA_GROUP_MAX_WAIT_SEC):
                logging.warning(
                    "TG media_group flush timeout: chat_id=%s media_group_id=%s pending=%s items=%s",
                    key[0],
                    key[1],
                    pending,
                    len(items),
                )
                break

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
        items = sorted(items, key=lambda x: x[0])
        if len(items) > 1:
            batch_notice_task = _start_delayed_progress_message(
                key[0],
                "Распознавание еще идет. Пришлю результат по каждому фото отдельно.",
                delay_sec=float(LONG_PROCESS_NOTICE_SEC),
            )
            logging.info(
                "TG media_group process individually: chat_id=%s media_group_id=%s items=%s",
                key[0],
                key[1],
                len(items),
            )
            total_items = len(items)
            for idx, (_mid, item_message, payload, filename, mime) in enumerate(items, start=1):
                logging.info(
                    "TG media_group item_start: chat_id=%s media_group_id=%s item=%s/%s message_id=%s filename=%s bytes=%s",
                    key[0],
                    key[1],
                    idx,
                    total_items,
                    item_message.message_id,
                    filename,
                    len(payload),
                )
                try:
                    result = await _handle_file_message(
                        item_message,
                        file_payloads=[(payload, filename, mime)],
                        response_prefix=f"Фото {idx}/{total_items}.\n",
                        allow_long_progress=False,
                    )
                    if result:
                        batch_results.append(result)
                    logging.info(
                        "TG media_group item_done: chat_id=%s media_group_id=%s item=%s/%s message_id=%s result=%s",
                        key[0],
                        key[1],
                        idx,
                        total_items,
                        item_message.message_id,
                        (result or {}).get("status"),
                    )
                except Exception:
                    logging.exception(
                        "media_group item failed: chat_id=%s media_group_id=%s item=%s/%s",
                        key[0],
                        key[1],
                        idx,
                        total_items,
                    )
                    batch_results.append(_build_batch_result("error", reason="handler_exception"))
        else:
            _mid, item_message, payload, filename, mime = items[0]
            await _handle_file_message(item_message, file_payloads=[(payload, filename, mime)])
        summary_text = _build_batch_summary(batch_results)
        if summary_text:
            await _safe_progress_message(key[0], summary_text)
    except Exception:
        logging.exception("media_group_flush failed")
    finally:
        _cancel_background_task(batch_notice_task)
        MEDIA_GROUP_TASKS.pop(key, None)
        MEDIA_GROUP_PENDING.pop(key, None)
        MEDIA_GROUP_LAST_ACTIVITY.pop(key, None)
        MEDIA_GROUP_ACKED.discard(key)


async def _flush_sequential_photos(chat_id: int) -> None:
    batch_notice_task: Optional[asyncio.Task] = None
    batch_results: List[Dict[str, Any]] = []
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
        if len(batch) > 1:
            batch_notice_task = _start_delayed_progress_message(
                chat_id,
                "Распознавание еще идет. Пришлю результат по каждому фото отдельно.",
                delay_sec=float(LONG_PROCESS_NOTICE_SEC),
            )
            for idx, item in enumerate(batch, start=1):
                try:
                    result = await _handle_file_message(
                        anchor,
                        file_payloads=[item],
                        response_prefix=f"Фото {idx}/{len(batch)}.\n",
                        allow_long_progress=False,
                    )
                    if result:
                        batch_results.append(result)
                except Exception:
                    logging.exception(
                        "sequential item failed: chat_id=%s item=%s/%s",
                        chat_id,
                        idx,
                        len(batch),
                    )
                    batch_results.append(_build_batch_result("error", reason="handler_exception"))
        else:
            await _handle_file_message(anchor, file_payloads=batch)
        summary_text = _build_batch_summary(batch_results)
        if summary_text:
            await _safe_progress_message(chat_id, summary_text)
    except Exception:
        logging.exception("sequential_photo_flush failed")
    finally:
        _cancel_background_task(batch_notice_task)
        SEQUENTIAL_PHOTO_TASKS.pop(chat_id, None)
        SEQUENTIAL_PHOTO_ACKED.discard(chat_id)


def _queue_sequential_photo(message: types.Message, payload: bytes, filename: str, mime: str) -> None:
    chat_id = int(message.chat.id)
    SEQUENTIAL_PHOTO_BUFFER.setdefault(chat_id, []).append((payload, filename, mime))
    if chat_id not in SEQUENTIAL_PHOTO_ANCHOR:
        SEQUENTIAL_PHOTO_ANCHOR[chat_id] = message
    task = SEQUENTIAL_PHOTO_TASKS.get(chat_id)
    if task is None or task.done():
        SEQUENTIAL_PHOTO_TASKS[chat_id] = asyncio.create_task(_flush_sequential_photos(chat_id))


async def _report_download_failure(message: types.Message, *, file_id: str, kind: str, extra: Optional[dict] = None) -> None:
    username = message.from_user.username if message.from_user else None
    payload = {
        "message_id": int(message.message_id),
        "file_id": str(file_id),
        "kind": str(kind),
    }
    if extra:
        payload.update(extra)
    try:
        await _post_notification(
            message.chat.id,
            username,
            f"Не удалось скачать файл из Telegram после повторных попыток: {kind}",
            "bot_warning",
            payload,
        )
    except Exception:
        logging.exception("report_download_failure failed")


async def _download_and_queue_media_group_photo(
    key: Tuple[int, str],
    message: types.Message,
    *,
    file_id: str,
    file_unique_id: str,
) -> None:
    try:
        payload, _file_path = await _download_tg_file(file_id)
        logging.info(
            "TG photo downloaded: chat_id=%s message_id=%s bytes=%s file_id=%s",
            message.chat.id,
            message.message_id,
            len(payload),
            file_id,
        )
        MEDIA_GROUP_BUFFER.setdefault(key, []).append(
            (
                int(message.message_id),
                message,
                payload,
                f"photo_{file_unique_id}.jpg",
                "image/jpeg",
            )
        )
    except Exception:
        logging.exception(
            "TG media_group photo download failed: chat_id=%s message_id=%s file_id=%s media_group_id=%s",
            message.chat.id,
            message.message_id,
            file_id,
            key[1],
        )
        await _report_download_failure(
            message,
            file_id=file_id,
            kind="photo_media_group_download",
            extra={"media_group_id": key[1]},
        )
    finally:
        MEDIA_GROUP_PENDING[key] = max(0, int(MEDIA_GROUP_PENDING.get(key, 0) or 0) - 1)
        MEDIA_GROUP_LAST_ACTIVITY[key] = time.monotonic()
        task = MEDIA_GROUP_TASKS.get(key)
        if task is None or task.done():
            MEDIA_GROUP_TASKS[key] = asyncio.create_task(_flush_media_group(key))


async def _download_and_queue_single_photo(message: types.Message, *, file_id: str, file_unique_id: str) -> None:
    try:
        payload, _file_path = await _download_tg_file(file_id)
        logging.info(
            "TG photo downloaded: chat_id=%s message_id=%s bytes=%s file_id=%s",
            message.chat.id,
            message.message_id,
            len(payload),
            file_id,
        )
        _queue_sequential_photo(
            message,
            payload,
            f"photo_{file_unique_id}.jpg",
            "image/jpeg",
        )
    except Exception:
        logging.exception(
            "TG single photo download failed: chat_id=%s message_id=%s file_id=%s",
            message.chat.id,
            message.message_id,
            file_id,
        )
        await _report_download_failure(message, file_id=file_id, kind="photo_download")


async def _download_and_handle_document(message: types.Message) -> None:
    doc = message.document
    if not doc:
        return
    try:
        payload, _file_path = await _download_tg_file(doc.file_id)
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
    except Exception:
        logging.exception(
            "TG document download failed: chat_id=%s message_id=%s file_id=%s",
            message.chat.id,
            message.message_id,
            doc.file_id,
        )
        await _report_download_failure(message, file_id=doc.file_id, kind="document_download")


@dp.message_handler(content_types=ContentType.PHOTO)
async def on_photo(message: types.Message):
    logging.info(
        "TG photo received: chat_id=%s message_id=%s photos=%s",
        message.chat.id,
        message.message_id,
        len(message.photo or []),
    )
    photo = message.photo[-1]
    mgid = str(message.media_group_id or "").strip()
    if mgid:
        key = (int(message.chat.id), mgid)
        if key not in MEDIA_GROUP_ANCHOR:
            MEDIA_GROUP_ANCHOR[key] = message
        if key not in MEDIA_GROUP_ACKED:
            MEDIA_GROUP_ACKED.add(key)
            _queue_progress_message(
                message.chat.id,
                "Фото получены. Обрабатываем, это может занять до нескольких минут. Результат пришлю по каждому фото отдельно.",
            )
        MEDIA_GROUP_PENDING[key] = int(MEDIA_GROUP_PENDING.get(key, 0) or 0) + 1
        MEDIA_GROUP_LAST_ACTIVITY[key] = time.monotonic()
        task = MEDIA_GROUP_TASKS.get(key)
        if task is None or task.done():
            MEDIA_GROUP_TASKS[key] = asyncio.create_task(_flush_media_group(key))
        _track_background_task(
            asyncio.create_task(
                _download_and_queue_media_group_photo(
                    key,
                    message,
                    file_id=photo.file_id,
                    file_unique_id=photo.file_unique_id,
                )
            )
        )
        return
    if int(message.chat.id) not in SEQUENTIAL_PHOTO_ACKED:
        SEQUENTIAL_PHOTO_ACKED.add(int(message.chat.id))
        _queue_progress_message(
            message.chat.id,
            "Фото получены. Обрабатываем, это может занять до нескольких минут.",
        )
    _track_background_task(
        asyncio.create_task(
            _download_and_queue_single_photo(
                message,
                file_id=photo.file_id,
                file_unique_id=photo.file_unique_id,
            )
        )
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
    _queue_progress_message(
        message.chat.id,
        "Файл получен. Обрабатываем, это может занять до нескольких минут.",
    )
    _track_background_task(asyncio.create_task(_download_and_handle_document(message)))


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
