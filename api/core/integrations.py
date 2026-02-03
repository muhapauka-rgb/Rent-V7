import requests
from datetime import datetime

from core.config import (
    TG_BOT_TOKEN,
    YANDEX_WEBDAV_BASE_URL,
    YANDEX_WEBDAV_PASSWORD,
    YANDEX_WEBDAV_USERNAME,
    YANDEX_STORAGE_ROOT,
    logger,
)


def _tg_send_message(chat_id: str, text_msg: str) -> bool:
    """Return True if message was accepted by Telegram API."""
    if not TG_BOT_TOKEN:
        logger.warning("tg_send skipped: missing token")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": str(chat_id), "text": str(text_msg)},
            timeout=10,
        )
        if r.ok:
            logger.info("tg_send ok chat_id=%s", str(chat_id))
        else:
            logger.warning("tg_send failed chat_id=%s status=%s text=%s", str(chat_id), r.status_code, (r.text or "")[:200])
        return bool(r.ok)
    except Exception:
        logger.exception("tg_send exception chat_id=%s", str(chat_id))
        return False


def ydisk_ready() -> bool:
    return bool(YANDEX_WEBDAV_USERNAME and YANDEX_WEBDAV_PASSWORD and YANDEX_WEBDAV_BASE_URL)


def ydisk_auth():
    return (YANDEX_WEBDAV_USERNAME, YANDEX_WEBDAV_PASSWORD)


def ydisk_mkcol(path: str) -> None:
    url = f"{YANDEX_WEBDAV_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.request("MKCOL", url, auth=ydisk_auth(), timeout=30)
    # 201 created, 405 already exists
    if r.status_code not in (201, 405):
        raise RuntimeError(f"MKCOL failed {r.status_code}: {r.text}")


def ydisk_put(path: str, content: bytes) -> None:
    url = f"{YANDEX_WEBDAV_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    r = requests.put(
        url,
        data=content,
        auth=ydisk_auth(),
        headers={"Content-Type": "application/octet-stream"},
        timeout=60,
    )
    if r.status_code not in (201, 204):
        raise RuntimeError(f"Upload failed {r.status_code}: {r.text}")


def _safe_part(s: str, max_len: int = 40) -> str:
    s = (s or "").strip()
    if not s:
        return "NA"
    s = "".join(ch for ch in s if ch.isalnum() or ch in ("-", "_", "."))
    if not s:
        return "NA"
    return s[:max_len]


def upload_to_ydisk(
    chat_id: str,
    chat_name: str | None,
    meter_type_label: str | None,
    original_filename: str | None,
    content: bytes,
) -> str:
    """
    Путь: tenants/<chat_id>/<YYYY-MM>/<YYYY.MM.DD-HHMMSS>__<chat>__<meter>.<ext>
    """
    now = datetime.now()
    ym = now.strftime("%Y-%m")
    ts = now.strftime("%Y.%m.%d-%H%M%S")

    ext = "bin"
    if original_filename and "." in original_filename:
        ext = original_filename.rsplit(".", 1)[-1].lower()

    chat_part = _safe_part(chat_name or "UNKNOWN_CHAT", 40)
    meter_part = _safe_part(meter_type_label or "unknown", 20)

    filename = f"{ts}__{chat_part}__{meter_part}.{ext}"

    root = YANDEX_STORAGE_ROOT.strip("/")

    ydisk_mkcol(root)
    ydisk_mkcol(f"{root}/{chat_id}")
    ydisk_mkcol(f"{root}/{chat_id}/{ym}")

    disk_path = f"{root}/{chat_id}/{ym}/{filename}"
    ydisk_put(disk_path, content)
    return disk_path
