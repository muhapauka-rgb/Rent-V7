import os
import time
from pathlib import Path

from fastapi import FastAPI, Request

from core.config import OCR_URL
from core.db import db_ready, ensure_tables
from core.integrations import ydisk_ready
from routes.admin_ui import router as admin_ui_router
from routes.admin import router as admin_router
from routes.events import router as events_router
from routes.bot import router as bot_router
from routes.dashboard import router as dashboard_router
from routes.tariffs import router as tariffs_router


app = FastAPI(title="Rent Backend API")
app.include_router(admin_ui_router)
app.include_router(admin_router)
app.include_router(events_router)
app.include_router(bot_router)
app.include_router(dashboard_router)
app.include_router(tariffs_router)


ACTIVITY_FILE = os.getenv("RENT_ACTIVITY_FILE", "/runtime/last_activity").strip()
ACTIVITY_TOUCH_ENABLED = os.getenv("RENT_ACTIVITY_TOUCH", "1").strip().lower() in ("1", "true", "yes", "on")


def _touch_activity() -> None:
    if not ACTIVITY_TOUCH_ENABLED or not ACTIVITY_FILE:
        return
    try:
        path = Path(ACTIVITY_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(int(time.time())), encoding="ascii")
    except Exception:
        # Activity tracking must never break API requests.
        pass


@app.middleware("http")
async def _activity_middleware(request: Request, call_next):
    if request.url.path not in {"/health", "/docs", "/openapi.json"}:
        _touch_activity()
    return await call_next(request)


@app.on_event("startup")
def _startup():
    if not db_ready():
        return
    try:
        ensure_tables()
    except Exception as e:
        print(f"[startup] ensure_tables failed: {e}")


@app.get("/health")
def health():
    return {
        "ok": True,
        "ocr_url": OCR_URL,
        "db": "ok" if db_ready() else "disabled",
        "ydisk": "ok" if ydisk_ready() else "disabled",
    }
