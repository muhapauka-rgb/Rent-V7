import logging
import os
from sqlalchemy import create_engine

logger = logging.getLogger("rent_api")

# OCR
OCR_URL = os.getenv("OCR_URL", "http://host.docker.internal:8000/recognize")

# --- Billing / approvals ---
BILL_DIFF_THRESHOLD_RUB = float((os.getenv("BILL_DIFF_THRESHOLD_RUB") or "500").strip() or 500)

# --- Telegram push (optional): API can send messages directly to tenant ---
TG_BOT_TOKEN = (os.getenv("TG_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()

# Yandex Disk WebDAV
YANDEX_WEBDAV_BASE_URL = os.getenv("YANDEX_WEBDAV_BASE_URL", "https://webdav.yandex.ru")
YANDEX_WEBDAV_USERNAME = os.getenv("YANDEX_WEBDAV_USERNAME", "")
YANDEX_WEBDAV_PASSWORD = os.getenv("YANDEX_WEBDAV_PASSWORD", "")
YANDEX_STORAGE_ROOT = os.getenv("YANDEX_STORAGE_ROOT", "tenants")

# DB
DATABASE_URL = os.getenv("DATABASE_URL", "")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None
