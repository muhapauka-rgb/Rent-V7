import os
import base64
import json
import requests
import re
import time
import uuid
import logging
import hashlib
import threading
from io import BytesIO
from datetime import datetime
from PIL import Image, ImageOps, ImageFilter, ImageEnhance, ImageDraw
from typing import Optional, Tuple
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
import numpy as np
import cv2
try:
    import pytesseract  # type: ignore
except Exception:  # pragma: no cover - optional runtime dependency
    pytesseract = None
from water_deterministic import (
    make_fixed_cells_sheet_from_row,
    make_water_deterministic_row_variants,
)

def _env_nonempty(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OCR_OPENAI_ENABLED = os.getenv("OCR_OPENAI_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
OCR_MODEL = _env_nonempty("OCR_MODEL", "gpt-4o")
OCR_MODEL_PRIMARY = _env_nonempty("OCR_MODEL_PRIMARY", OCR_MODEL)
OCR_MODEL_FALLBACK = _env_nonempty("OCR_MODEL_FALLBACK", "gpt-4o-mini")
OCR_MODEL_ODOMETER = _env_nonempty("OCR_MODEL_ODOMETER", "gpt-4o")
OCR_FALLBACK_MIN_CONF = float(os.getenv("OCR_FALLBACK_MIN_CONF", "0.78"))
OPENAI_TIMEOUT_SEC = float(os.getenv("OPENAI_TIMEOUT_SEC", "15"))
try:
    OPENAI_RETRIES = int(os.getenv("OPENAI_RETRIES", "1"))
except Exception:
    OPENAI_RETRIES = 1
OPENAI_RETRIES = max(1, min(3, OPENAI_RETRIES))
OCR_MAX_RUNTIME_SEC = float(os.getenv("OCR_MAX_RUNTIME_SEC", "55"))
GOOGLE_VISION_API_KEY = os.getenv("GOOGLE_VISION_API_KEY", "").strip()
OCR_DEBUG = os.getenv("OCR_DEBUG", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_WATER_DIGIT_FIRST = os.getenv("OCR_WATER_DIGIT_FIRST", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_WATER_ECO = os.getenv("OCR_WATER_ECO", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_WATER_INTEGER_ONLY = os.getenv("OCR_WATER_INTEGER_ONLY", "0").strip().lower() in ("1", "true", "yes", "on")
OCR_ELECTRIC_BOOTSTRAP = os.getenv("OCR_ELECTRIC_BOOTSTRAP", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_ELECTRIC_DETERMINISTIC = os.getenv("OCR_ELECTRIC_DETERMINISTIC", "0").strip().lower() in ("1", "true", "yes", "on")
OCR_ELECTRIC_DRUM_ENABLED = os.getenv("OCR_ELECTRIC_DRUM_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_ELECTRIC_TESSERACT_ENABLED = os.getenv("OCR_ELECTRIC_TESSERACT_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_ELECTRIC_TEMPLATE_MATCH = os.getenv("OCR_ELECTRIC_TEMPLATE_MATCH", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_ELECTRIC_TEMPLATE_DB = os.getenv("OCR_ELECTRIC_TEMPLATE_DB", "/app/electric_templates_seed.json").strip()
OCR_WATER_TEMPLATE_MATCH = os.getenv("OCR_WATER_TEMPLATE_MATCH", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_WATER_TEMPLATE_DB = os.getenv("OCR_WATER_TEMPLATE_DB", "/app/water_templates_seed.json").strip()
try:
    OCR_ELECTRIC_BOOTSTRAP_VARIANTS = int(os.getenv("OCR_ELECTRIC_BOOTSTRAP_VARIANTS", "2"))
except Exception:
    OCR_ELECTRIC_BOOTSTRAP_VARIANTS = 2
OCR_ELECTRIC_BOOTSTRAP_VARIANTS = max(2, min(8, OCR_ELECTRIC_BOOTSTRAP_VARIANTS))
OCR_ELECTRIC_HARD_RECOVERY = os.getenv("OCR_ELECTRIC_HARD_RECOVERY", "0").strip().lower() in ("1", "true", "yes", "on")
try:
    OCR_ELECTRIC_DET_MAX_VARIANTS = int(os.getenv("OCR_ELECTRIC_DET_MAX_VARIANTS", "10"))
except Exception:
    OCR_ELECTRIC_DET_MAX_VARIANTS = 10
OCR_ELECTRIC_DET_MAX_VARIANTS = max(4, min(24, OCR_ELECTRIC_DET_MAX_VARIANTS))
OCR_WATER_HYPOTHESIS_PASS = os.getenv("OCR_WATER_HYPOTHESIS_PASS", "1").strip().lower() in ("1", "true", "yes", "on")
try:
    OCR_WATER_HYPOTHESIS_MAX_CALLS = int(os.getenv("OCR_WATER_HYPOTHESIS_MAX_CALLS", "1"))
except Exception:
    OCR_WATER_HYPOTHESIS_MAX_CALLS = 1
OCR_WATER_HYPOTHESIS_MAX_CALLS = max(1, min(5, OCR_WATER_HYPOTHESIS_MAX_CALLS))
try:
    OCR_WATER_HYPOTHESIS_MAX_PER_CALL = int(os.getenv("OCR_WATER_HYPOTHESIS_MAX_PER_CALL", "5"))
except Exception:
    OCR_WATER_HYPOTHESIS_MAX_PER_CALL = 5
OCR_WATER_HYPOTHESIS_MAX_PER_CALL = max(2, min(8, OCR_WATER_HYPOTHESIS_MAX_PER_CALL))
try:
    OCR_WATER_DECIMALS = int(os.getenv("OCR_WATER_DECIMALS", "2"))
except Exception:
    OCR_WATER_DECIMALS = 2
OCR_WATER_DECIMALS = max(1, min(3, OCR_WATER_DECIMALS))
try:
    OCR_SERIAL_TARGET_MAX_CALLS = int(os.getenv("OCR_SERIAL_TARGET_MAX_CALLS", "10"))
except Exception:
    OCR_SERIAL_TARGET_MAX_CALLS = 10
OCR_SERIAL_TARGET_MAX_CALLS = max(1, min(12, OCR_SERIAL_TARGET_MAX_CALLS))
try:
    OCR_ODO_MAX_VARIANTS = int(os.getenv("OCR_ODO_MAX_VARIANTS", "6"))
except Exception:
    OCR_ODO_MAX_VARIANTS = 6
OCR_ODO_MAX_VARIANTS = max(3, min(12, OCR_ODO_MAX_VARIANTS))
try:
    OCR_CELLS_ROW_SOURCES_MAX = int(os.getenv("OCR_CELLS_ROW_SOURCES_MAX", "5"))
except Exception:
    OCR_CELLS_ROW_SOURCES_MAX = 5
OCR_CELLS_ROW_SOURCES_MAX = max(3, min(10, OCR_CELLS_ROW_SOURCES_MAX))
try:
    OCR_SERIES_MAX_FILES = int(os.getenv("OCR_SERIES_MAX_FILES", "6"))
except Exception:
    OCR_SERIES_MAX_FILES = 6
OCR_SERIES_MAX_FILES = max(2, min(12, OCR_SERIES_MAX_FILES))
OCR_SERIES_NEIGHBOR_RECOVERY = os.getenv("OCR_SERIES_NEIGHBOR_RECOVERY", "1").strip().lower() in ("1", "true", "yes", "on")
try:
    OCR_MAX_OPENAI_CALLS = int(os.getenv("OCR_MAX_OPENAI_CALLS", "4"))
except Exception:
    OCR_MAX_OPENAI_CALLS = 4
OCR_MAX_OPENAI_CALLS = max(1, min(40, OCR_MAX_OPENAI_CALLS))
try:
    OCR_MAX_OPENAI_CALLS_QUICK = int(os.getenv("OCR_MAX_OPENAI_CALLS_QUICK", "3"))
except Exception:
    OCR_MAX_OPENAI_CALLS_QUICK = 3
OCR_MAX_OPENAI_CALLS_QUICK = max(1, min(30, OCR_MAX_OPENAI_CALLS_QUICK))
try:
    OCR_RED_REFINE_REPEATS = int(os.getenv("OCR_RED_REFINE_REPEATS", "2"))
except Exception:
    OCR_RED_REFINE_REPEATS = 2
OCR_RED_REFINE_REPEATS = max(1, min(3, OCR_RED_REFINE_REPEATS))
try:
    OCR_RED_REFINE_MAX_SOURCES = int(os.getenv("OCR_RED_REFINE_MAX_SOURCES", "6"))
except Exception:
    OCR_RED_REFINE_MAX_SOURCES = 6
OCR_RED_REFINE_MAX_SOURCES = max(2, min(12, OCR_RED_REFINE_MAX_SOURCES))
OCR_OPENAI_CACHE = os.getenv("OCR_OPENAI_CACHE", "1").strip().lower() in ("1", "true", "yes", "on")
try:
    OCR_OPENAI_CACHE_MAX = int(os.getenv("OCR_OPENAI_CACHE_MAX", "4000"))
except Exception:
    OCR_OPENAI_CACHE_MAX = 4000
OCR_OPENAI_CACHE_MAX = max(100, min(20000, OCR_OPENAI_CACHE_MAX))
try:
    OCR_OPENAI_CACHE_TTL_SEC = int(os.getenv("OCR_OPENAI_CACHE_TTL_SEC", "86400"))
except Exception:
    OCR_OPENAI_CACHE_TTL_SEC = 86400
OCR_OPENAI_CACHE_TTL_SEC = max(60, min(7 * 86400, OCR_OPENAI_CACHE_TTL_SEC))
try:
    OCR_OPENAI_QUOTA_COOLDOWN_SEC = int(os.getenv("OCR_OPENAI_QUOTA_COOLDOWN_SEC", "3600"))
except Exception:
    OCR_OPENAI_QUOTA_COOLDOWN_SEC = 3600
OCR_OPENAI_QUOTA_COOLDOWN_SEC = max(60, min(24 * 3600, OCR_OPENAI_QUOTA_COOLDOWN_SEC))
try:
    OCR_TESSERACT_TIMEOUT_SEC = float(os.getenv("OCR_TESSERACT_TIMEOUT_SEC", "2.5"))
except Exception:
    OCR_TESSERACT_TIMEOUT_SEC = 2.5
OCR_TESSERACT_TIMEOUT_SEC = max(0.5, min(8.0, OCR_TESSERACT_TIMEOUT_SEC))

_OPENAI_CACHE_LOCK = threading.Lock()
_OPENAI_CACHE: dict[str, tuple[float, dict]] = {}
_OPENAI_BLOCK_UNTIL_TS = 0.0
_ELECTRIC_TEMPLATE_LOCK = threading.Lock()
_ELECTRIC_TEMPLATE_MTIME: float = -1.0
_ELECTRIC_TEMPLATE_ROWS: list[dict] = []
_WATER_TEMPLATE_LOCK = threading.Lock()
_WATER_TEMPLATE_MTIME: float = -1.0
_WATER_TEMPLATE_ROWS: list[dict] = []

app = FastAPI()
logger = logging.getLogger("ocr_service")

SYSTEM_PROMPT = """Ты — OCR-ассистент для коммунальных счётчиков (вода/электро).

Твоя задача:
1) Определить тип счётчика: строго один из ["ХВС","ГВС","Электро","unknown"].
2) Считать показание как число (может быть дробным). Если не уверен — null.
3) Найти серийный номер счётчика (если виден). Если не уверен — null.
4) Дать confidence 0..1.
Вернуть ТОЛЬКО валидный JSON строго по схеме:
{
  "type": "ХВС|ГВС|Электро|unknown",
  "reading": <number|null>,
  "serial": <string|null>,
  "confidence": <number>,
  "notes": "<коротко: на что опирался>"
}

Критически важно (общие правила):
- Ничего не выдумывай. Если цифры неразборчивы — reading=null и confidence<=0.4.
- Игнорируй любые НЕ показания: серийные номера, модель, ГОСТ, год выпуска, напряжение/ток (230V/5-60A), даты/время, штрихкоды.
- Если на фото несколько чисел — выбери то, что является ПРИБОРОМНЫМ показанием (одометр/табло).

-------------------------
ВОДА (ХВС/ГВС):
- Главная строка — это "одометр" из чёрных цифр (целые м³). Красные цифры (если есть) — дробная часть (литры).
- Пример: чёрные "00248" и красные "761" -> reading = 248.761
- Если есть красные цифры, добавь их как тысячные доли м³: .XYZ (три цифры). Если красных две — это сотые: .XY
- НЕ считай маленький круговой циферблат (стрелочный) — он не основное показание.

Определение ХВС vs ГВС:
- Используй подсказки: надписи "ГВС", "горячая", "HOT", "t°", "90°C"; цветовые маркеры: красный чаще ГВС, синий чаще ХВС.
- Если видно водяной счётчик, но уверенно отличить ХВС/ГВС нельзя — type="unknown" (лучше так, чем ошибиться).

Серийный номер (serial):
- Обычно это строка рядом с "№", "No", "Serial", "S/N".
- Может быть только цифры или цифры с тире.
- Не путай с показанием, годом выпуска, ГОСТ, моделями.
- Если сомневаешься — serial=null.

-------------------------
ЭЛЕКТРО (Электро):
- Ищи цифровой дисплей/табло, маркировку kWh/кВт⋅ч, обозначения 1.8.0, 1.8.1, 1.8.2, 1.8.3, T1/T2/T3.
- Показание — это число энергии (кВт⋅ч), обычно без дробной части, но дробь возможна (принимай).
- Если на экране есть несколько тарифов:
  - Если явно указан T1/T2/T3 или 1.8.1/1.8.2/1.8.3 — считай число рядом с текущим активным тарифом/индикатором.
  - Если указан 1.8.0 (TOTAL) — это общее. Но если на фото видно конкретно T1/T2/T3 — бери именно тарифное значение (не суммируй).
- Не бери числа вроде "230", "50", "5-60", "2024" — это не показание.

Формат reading:
- Верни число (пример: 4273.21) или null.
- Если уверен, что это вода, но сомневаешься в красных цифрах — можно вернуть только целую часть (например 248) с низким confidence.

Только JSON. Никакого текста вокруг JSON.
"""

WATER_SYSTEM_PROMPT = """Ты — OCR для ВОДЯНОГО счётчика.
Нужно вернуть строго JSON:
{
  "type": "ХВС|ГВС|unknown",
  "reading": <number|null>,
  "serial": <string|null>,
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Читай основной барабан: чёрные цифры = целая часть м3, красные = дробная часть.
- Не используй маленький круговой циферблат.
- Никогда не используй серийный номер как показание.
- Если видишь 5 чёрных и 3 красных цифры -> reading = XXXXX.YYY
- Если уверенности нет, возвращай reading=null и confidence<=0.4
- Никакого текста вокруг JSON.
"""

WATER_ODOMETER_SYSTEM_PROMPT = """Ты — OCR для ОКНА ЦИФР водяного счётчика (одометр).
На изображении может быть только область с цифрами.
Верни строго JSON:
{
  "type": "ХВС|ГВС|unknown",
  "black_digits": "<строка цифр или null>",
  "red_digits": "<строка цифр или null>",
  "reading": <number|null>,
  "serial": <string|null>,
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Чёрные цифры = целая часть, красные = дробная часть.
- Всегда ищи ГОРИЗОНТАЛЬНУЮ строку квадратных окошек барабана и читай цифры строго слева направо.
- Серийный номер (например вида "13 002714") часто расположен ВЫШЕ барабана — его нельзя использовать как показание.
- Если видно 7-9 окошек, не обрезай только правый хвост. Нужны все доступные цифры из этой строки.
- Если видишь только чёрные цифры: reading = int(black_digits).
- Если видишь чёрные + красные: reading = black_digits.red_digits
- Не используй серийный номер, маркировки, круглый мини-циферблат.
- Если не уверен в символе, лучше null.
- Никакого текста вокруг JSON.
"""

WATER_ODOMETER_SHEET_PROMPT = """Ты видишь коллаж из 6 фрагментов (ячейки A1..A6) с барабаном цифр водяного счётчика.
Нужно выбрать лучшую ячейку, где цифры читаются максимально уверенно.
Верни строго JSON:
{
  "type": "ХВС|ГВС|unknown",
  "chosen_cell": "A1|A2|A3|A4|A5|A6|unknown",
  "black_digits": "<строка цифр или null>",
  "red_digits": "<строка цифр или null>",
  "reading": <number|null>,
  "serial": <string|null>,
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Чёрные цифры = целая часть, красные = дробная часть.
- Если в лучшей ячейке цифры нечитабельны, верни unknown/null.
- Не брать круглый циферблат, только прямоугольное окно цифр.
- Никакого текста вокруг JSON.
"""

WATER_RED_DIGITS_PROMPT = """Ты видишь только правую (красную) дробную часть барабана водяного счётчика.
Нужно вернуть строго JSON:
{
  "red_digits": "<2-3 цифры или null>",
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Читай только цифры в красных окошках справа (после запятой).
- Игнорируй чёрные цифры, серийник и любой текст.
- Если не уверен — red_digits=null и confidence<=0.4.
- Никакого текста вокруг JSON.
"""

WATER_COUNTER_ROW_PROMPT = """Ты видишь только строку барабана водяного счётчика (ряд квадратных окон цифр).
Верни строго JSON:
{
  "type": "ХВС|ГВС|unknown",
  "black_digits": "<строка цифр или null>",
  "red_digits": "<строка цифр или null>",
  "reading": <number|null>,
  "serial": <string|null>,
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Читай только последовательность цифр в квадратных окнах слева направо.
- Цифры до запятой/разделителя -> black_digits (минимум 3 цифры).
- Красные окна справа -> red_digits (обычно 2-3 цифры).
- НЕ используй серийный номер (например 13 002714) и любой текст вне окон.
- Если не уверен, ставь null и понижай confidence.
- Никакого текста вокруг JSON.
"""

WATER_BLACK_DIGITS_PROMPT = """Ты видишь коллаж из 5 ячеек с ЧЁРНЫМИ цифрами водяного счётчика (только целая часть).
Верни строго JSON:
{
  "black_digits": "<ровно 5 цифр или null>",
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Читай только чёрные цифры в квадратных окнах слева направо.
- Игнорируй красные цифры, серийный номер и любой другой текст.
- Не выдумывай цифры: если не уверен, верни null.
- Никакого текста вокруг JSON.
"""

WATER_CELLS_SHEET_PROMPT = """Ты видишь коллаж из ячеек барабана водяного счётчика.
Верхняя строка: B1..B5 (целая часть).
Нижняя строка: R1..R3 (дробная часть; может быть только R1..R2).
Верни строго JSON:
{
  "cells": {
    "B1": "<цифра|null>", "B2": "<цифра|null>", "B3": "<цифра|null>", "B4": "<цифра|null>", "B5": "<цифра|null>",
    "R1": "<цифра|null>", "R2": "<цифра|null>", "R3": "<цифра|null>"
  },
  "black_digits": "<ровно 5 цифр или null>",
  "red_digits": "<2-3 цифры или null>",
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Читай цифры ТОЛЬКО внутри подписанных ячеек, слева направо.
- Цвет не важен: опирайся на ПОРЯДОК ячеек (B* и R*), а не на цвет барабана.
- Ноль — это валидная цифра. Не пропускай ячейку только потому, что символ бледный.
- В B1..B5 всегда 5 цифр; не "схлопывай" соседние окна в одну цифру.
- Для каждой сомнительной ячейки ставь null в объекте cells.
- black_digits/red_digits заполни из cells, если хватает уверенности.
- Не используй серийный номер и любой текст вне ячеек.
- Никакого текста вокруг JSON.
"""

WATER_HYPOTHESES_PROMPT = """Ты — OCR для СЛОЖНОГО фото водяного счётчика.
Нужно вернуть несколько гипотез чтения одометра (от лучшей к худшей).
Верни строго JSON:
{
  "type": "ХВС|ГВС|unknown",
  "serial": "<строка|null>",
  "confidence": <number>,
  "hypotheses": [
    {
      "black_digits": "<строка цифр или null>",
      "red_digits": "<2-3 цифры или null>",
      "reading": <number|null>,
      "serial": "<строка|null>",
      "confidence": <number>,
      "notes": "<коротко>"
    }
  ],
  "notes": "<коротко>"
}
Правила:
- Дай 3-5 РАЗНЫХ гипотез, не одну.
- Читай только строку квадратных окон барабана, слева направо.
- Серийный номер (например 13 002714) нельзя брать как reading.
- Если red_digits не видно, верни только целую часть по black_digits.
- Для сомнительных гипотез ставь confidence ниже.
- Никакого текста вокруг JSON.
"""

WATER_SERIAL_TARGET_PROMPT = """Ты — OCR для водяных счётчиков на фото, где может быть несколько приборов.
Нужно считать показание ТОЛЬКО у прибора с нужным серийным номером (хвост серийника будет передан в user_text).
Верни строго JSON:
{
  "type": "ХВС|ГВС|unknown",
  "black_digits": "<строка цифр или null>",
  "red_digits": "<2-3 цифры или null>",
  "reading": <number|null>,
  "serial": "<строка|null>",
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Если на фото два счетчика, выбери только тот, чей serial оканчивается на целевой хвост.
- Не используй серийный номер как показание.
- Если целевой счетчик не найден или нечитабелен: reading=null и confidence<=0.35.
- Никакого текста вокруг JSON.
"""

ELECTRIC_LCD_PROMPT = """Ты — OCR для ЭЛЕКТРОСЧЕТЧИКА (LCD/LED дисплей).
Верни строго JSON:
{
  "type": "Электро|unknown",
  "reading": <number|null>,
  "digits": "<строка цифр/точки или null>",
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Читай ТОЛЬКО число на экране дисплея (темный прямоугольник с крупными сегментными цифрами).
- Игнорируй серийный номер, наклейки, кнопки, светодиоды и любой текст вне экрана.
- Не добавляй лишние ведущие/хвостовые цифры, которых нет на дисплее.
- Если на дисплее есть десятичная точка — сохрани дробную часть.
- Если число не видно уверенно, верни reading=null.
- Только JSON.
"""

def _guess_mime(filename: Optional[str], content_type: Optional[str]) -> str:
    """
    Пытаемся подобрать корректный mime для data URL.
    По умолчанию было image/jpeg, но с телефона могут прилетать PNG/WEBP/HEIC.
    """
    ct = (content_type or "").strip().lower()
    if ct.startswith("image/"):
        return ct

    fn = (filename or "").strip().lower()
    if fn.endswith(".png"):
        return "image/png"
    if fn.endswith(".webp"):
        return "image/webp"
    if fn.endswith(".gif"):
        return "image/gif"
    if fn.endswith(".bmp"):
        return "image/bmp"
    if fn.endswith(".heic") or fn.endswith(".heif"):
        # Пытаемся передать как есть (OpenAI может поддерживать/не поддерживать).
        return "image/heic"
    if fn.endswith(".jpg") or fn.endswith(".jpeg"):
        return "image/jpeg"

    # безопасный дефолт
    return "image/jpeg"


def _prepare_input_image_for_ocr(img_bytes: bytes, *, max_dim: int = 1700) -> bytes:
    """
    Normalize huge mobile photos to bounded size.
    This keeps OCR stable and prevents very expensive CV passes on 3k-4k frames.
    """
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return img_bytes
    w, h = img.size
    if max(w, h) <= max_dim:
        return img_bytes
    scale = float(max_dim) / float(max(w, h))
    nw = max(1, int(round(w * scale)))
    nh = max(1, int(round(h * scale)))
    try:
        resized = img.resize((nw, nh), Image.Resampling.LANCZOS)
    except Exception:
        resized = img.resize((nw, nh))
    return _encode_jpeg(resized, quality=93)


def _extract_json_object(text_content: str) -> dict:
    """
    Модель иногда добавляет мусор вокруг JSON.
    Выдёргиваем первый объект {...} максимально безопасно.
    """
    if not isinstance(text_content, str) or not text_content.strip():
        raise HTTPException(status_code=500, detail="openai_empty_response")

    raw = text_content.strip()

    # 1) Попробовать как есть
    try:
        return json.loads(raw)
    except Exception:
        pass

    # 2) Вырезать по первому { и последнему }
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise HTTPException(status_code=500, detail="openai_returned_non_json")

    chunk = raw[start : end + 1].strip()
    try:
        return json.loads(chunk)
    except Exception:
        raise HTTPException(status_code=500, detail="openai_returned_non_json")


def _content_to_text(content) -> str:
    def _grab(v) -> list[str]:
        out: list[str] = []
        if isinstance(v, str):
            if v.strip():
                out.append(v)
            return out
        if isinstance(v, list):
            for it in v:
                out.extend(_grab(it))
            return out
        if isinstance(v, dict):
            for k in ("text", "value", "output_text", "content"):
                out.extend(_grab(v.get(k)))
            return out
        return out

    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = _grab(content)
        return "\n".join(parts)
    if isinstance(content, dict):
        parts = _grab(content)
        return "\n".join(parts)
    return ""


def _recover_non_json_vision_response(content, system_prompt: str) -> Optional[dict]:
    """
    Emergency parser when provider answered with non-JSON text.
    Keeps pipeline alive on hard/dark electric shots.
    """
    text = _content_to_text(content)
    if not text.strip():
        return None
    nums: list[float] = []
    for m in re.finditer(r"\d[\d\s]{1,10}(?:[.,]\d{1,3})?", text.replace("\xa0", " ")):
        raw = m.group(0).replace(" ", "").replace(",", ".")
        try:
            v = float(raw)
        except Exception:
            continue
        if v < 0:
            continue
        if v in (50.0, 60.0, 220.0, 230.0, 380.0):
            continue
        nums.append(v)
    if not nums:
        return None

    is_electric_prompt = ("ЭЛЕКТРОСЧЕТЧИКА" in system_prompt) or ("LCD/LED" in system_prompt)
    chosen = max(nums, key=lambda x: (1 if 1000.0 <= x <= 20000.0 else 0, -abs(x - 4000.0)))
    return {
        "type": "Электро" if is_electric_prompt else "unknown",
        "reading": float(chosen),
        "serial": None,
        "confidence": 0.42 if is_electric_prompt else 0.35,
        "notes": "fallback_non_json_extract",
    }


def _normalize_reading(value) -> Optional[float]:
    """
    Приводим reading к float или None.
    Поддержка строк с пробелами/запятыми.
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        s = s.replace(" ", "").replace(",", ".")
        # убрать лишние символы кроме цифр, точки и минуса
        cleaned = []
        for ch in s:
            if ch.isdigit() or ch in (".", "-"):
                cleaned.append(ch)
        s2 = "".join(cleaned)
        if s2 in ("", "-", ".", "-."):
            return None
        try:
            return float(s2)
        except Exception:
            return None

    return None


def _clamp_confidence(value) -> float:
    try:
        c = float(value)
    except Exception:
        c = 0.0
    if c < 0.0:
        c = 0.0
    if c > 1.0:
        c = 1.0
    return c


def _sanitize_type(t: str) -> str:
    v = (t or "unknown").strip()
    if v not in ["ХВС", "ГВС", "Электро", "unknown"]:
        return "unknown"
    return v


def _plausibility_filter(t: str, reading: Optional[float], conf: float) -> Tuple[Optional[float], float, str]:
    """
    Лёгкая проверка правдоподобности, чтобы отсекать явный мусор.
    Не ломает валидные кейсы, но снижает confidence при странных значениях.
    """
    if reading is None:
        return None, min(conf, 0.4), ""

    # отрицательные показания невалидны
    if reading < 0:
        return None, min(conf, 0.2), "negative_reading_filtered"

    # грубые верхние пределы (очень щадящие)
    if t in ("ХВС", "ГВС"):
        # вода: тысячи/десятки тысяч м3 ок, миллиарды — мусор
        if reading > 100000000:  # 1e8 м3
            return None, min(conf, 0.2), "water_too_large_filtered"
    if t == "Электро":
        # электро: миллионы кВтч ок, сотни миллиардов — мусор
        if reading > 100000000000:  # 1e11
            return None, min(conf, 0.2), "electric_too_large_filtered"

    return reading, conf, ""


def _openai_cache_key(
    image_bytes: bytes,
    *,
    mime: str,
    model: str,
    system_prompt: str,
    user_text: str,
    detail: str,
) -> str:
    h = hashlib.sha256()
    h.update(image_bytes)
    h.update(b"\x1f")
    h.update(str(mime).encode("utf-8", "ignore"))
    h.update(b"\x1f")
    h.update(str(model).encode("utf-8", "ignore"))
    h.update(b"\x1f")
    h.update(str(detail).encode("utf-8", "ignore"))
    h.update(b"\x1f")
    h.update(str(system_prompt).encode("utf-8", "ignore"))
    h.update(b"\x1f")
    h.update(str(user_text).encode("utf-8", "ignore"))
    return h.hexdigest()


def _openai_cache_get(key: str) -> Optional[dict]:
    if (not OCR_OPENAI_CACHE) or (not key):
        return None
    now = time.time()
    with _OPENAI_CACHE_LOCK:
        hit = _OPENAI_CACHE.get(key)
        if not hit:
            return None
        ts, val = hit
        if (now - ts) > float(OCR_OPENAI_CACHE_TTL_SEC):
            _OPENAI_CACHE.pop(key, None)
            return None
        _OPENAI_CACHE[key] = (now, dict(val))
        return dict(val)


def _openai_cache_put(key: str, val: dict) -> None:
    if (not OCR_OPENAI_CACHE) or (not key) or (not isinstance(val, dict)):
        return
    now = time.time()
    with _OPENAI_CACHE_LOCK:
        _OPENAI_CACHE[key] = (now, dict(val))
        if len(_OPENAI_CACHE) > OCR_OPENAI_CACHE_MAX:
            # prune oldest 10%
            n_drop = max(1, int(OCR_OPENAI_CACHE_MAX * 0.1))
            old_keys = sorted(_OPENAI_CACHE.items(), key=lambda kv: kv[1][0])[:n_drop]
            for k, _ in old_keys:
                _OPENAI_CACHE.pop(k, None)


def _openai_is_blocked_now() -> bool:
    now = time.time()
    with _OPENAI_CACHE_LOCK:
        return now < float(_OPENAI_BLOCK_UNTIL_TS)


def _openai_set_block_for_quota() -> None:
    global _OPENAI_BLOCK_UNTIL_TS
    with _OPENAI_CACHE_LOCK:
        _OPENAI_BLOCK_UNTIL_TS = max(float(_OPENAI_BLOCK_UNTIL_TS), time.time() + float(OCR_OPENAI_QUOTA_COOLDOWN_SEC))


def _call_openai_vision(
    image_bytes: bytes,
    mime: str,
    model: str,
    system_prompt: str = SYSTEM_PROMPT,
    user_text: str = "Определи тип счётчика и показание. Верни JSON строго по схеме.",
    detail: str = "high",
    timeout_sec: Optional[float] = None,
) -> dict:
    if not OCR_OPENAI_ENABLED:
        raise HTTPException(status_code=500, detail="openai_disabled")
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")
    if _openai_is_blocked_now():
        raise HTTPException(status_code=500, detail="openai_quota_cooldown")

    cache_key = _openai_cache_key(
        image_bytes,
        mime=mime,
        model=model,
        system_prompt=system_prompt,
        user_text=user_text,
        detail=detail,
    )
    cached = _openai_cache_get(cache_key)
    if cached is not None:
        return cached

    b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:{mime};base64,{b64}"

    last_err: Optional[str] = None
    for attempt in range(OPENAI_RETRIES):
        detail_now = detail if attempt == 0 else ("auto" if detail == "high" else detail)
        payload = {
            "model": model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_text},
                        {"type": "image_url", "image_url": {"url": data_url, "detail": detail_now}},
                    ],
                },
            ],
            "max_tokens": 250,
        }
        try:
            req_timeout = float(timeout_sec) if timeout_sec is not None else float(OPENAI_TIMEOUT_SEC)
            req_timeout = max(1.0, min(float(OPENAI_TIMEOUT_SEC), req_timeout))
            connect_timeout = max(2.0, min(8.0, req_timeout))
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json=payload,
                timeout=(connect_timeout, req_timeout),
            )
        except requests.RequestException as e:
            last_err = f"openai_request_error:{e}"
            if attempt < 2:
                time.sleep(0.25 * float(attempt + 1))
                continue
            raise HTTPException(status_code=500, detail=last_err)

        if r.ok:
            try:
                body = r.json()
                content = body["choices"][0]["message"]["content"]
                content_text = _content_to_text(content)
                try:
                    out = _extract_json_object(content_text)
                    _openai_cache_put(cache_key, out)
                    return out
                except Exception:
                    recovered = _recover_non_json_vision_response(content, system_prompt)
                    if recovered is not None:
                        _openai_cache_put(cache_key, recovered)
                        return recovered
                    raise
            except Exception as e:
                last_err = f"openai_bad_json:{e}"
                if attempt < 2:
                    time.sleep(0.2 * float(attempt + 1))
                    continue
                raise HTTPException(status_code=500, detail=last_err)

        status = int(r.status_code)
        body_txt = str(r.text or "")
        if status == 429 and "insufficient_quota" in body_txt:
            _openai_set_block_for_quota()
            raise HTTPException(status_code=500, detail="openai_insufficient_quota")
        # transient provider errors: retry a couple of times
        if status in (408, 409, 429) or status >= 500:
            last_err = f"openai_http_{status}: {body_txt[:300]}"
            if attempt < 2:
                time.sleep(0.35 * float(attempt + 1))
                continue
        raise HTTPException(status_code=500, detail=f"openai_http_{status}: {body_txt[:300]}")

    raise HTTPException(status_code=500, detail=last_err or "openai_unknown_error")


def _classify_meter_type_from_text(s: str) -> str:
    t = (s or "").lower()
    if any(x in t for x in ["гвс", "горяч", "hot"]):
        return "ГВС"
    if any(x in t for x in ["хвс", "холод", "cold"]):
        return "ХВС"
    if any(x in t for x in ["квт", "kwh", "t1", "t2", "t3", "1.8."]):
        return "Электро"
    return "unknown"


def _extract_numeric_candidates(s: str) -> list[float]:
    txt = (s or "").replace("\n", " ").replace("\xa0", " ")
    vals: list[float] = []
    for m in re.finditer(r"\d[\d\s]{1,12}(?:[.,]\d{1,3})?", txt):
        raw = m.group(0)
        compact = raw.replace(" ", "").replace(",", ".")
        # отсечь типичный мусор
        if compact in {"230", "380", "50", "60"}:
            continue
        try:
            vals.append(float(compact))
        except Exception:
            continue
    return vals


def _extract_serial_from_text(s: str) -> Optional[str]:
    txt = s or ""
    m = re.search(r"(?:№|No|Serial|S/N)\s*[:#]?\s*([A-Za-z0-9-]{6,20})", txt, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    # fallback: длинная числовая последовательность (часто серийник)
    m2 = re.search(r"\b(\d{7,12})\b", txt)
    if m2:
        return m2.group(1)
    return None


def _electric_hint_score(item: dict) -> float:
    txt = f"{item.get('type','')} {item.get('notes','')}".lower()
    variant = str(item.get("variant") or "").lower()
    score = 0.0
    for token in ("квт", "kwh", "1.8.", "t1", "t2", "t3", "электро"):
        if token in txt:
            score += 0.08
    if "focused_crop" in variant:
        score += 0.05
    if "center" in variant:
        score += 0.07
    if "mid_lcd" in variant:
        score -= 0.06
    if variant.endswith("_bw"):
        score -= 0.03
    return min(0.24, score)


def _pick_electric_bootstrap(candidates: list[dict]) -> tuple[Optional[dict], int]:
    if not candidates:
        return None, 0
    ranked: list[tuple[float, dict, int]] = []
    for i, c in enumerate(candidates):
        r = _normalize_reading(c.get("reading"))
        if r is None:
            continue
        if r < 0 or r > 100000000000:
            continue
        conf = _clamp_confidence(c.get("confidence", 0.0))
        agree = 0
        for j, d in enumerate(candidates):
            if j == i:
                continue
            rd = _normalize_reading(d.get("reading"))
            if rd is None:
                continue
            if abs(float(rd) - float(r)) <= 2.0:
                agree += 1
        mag_penalty = 0.0
        if abs(float(r)) >= 100000:
            mag_penalty += 0.40
        elif abs(float(r)) >= 30000:
            mag_penalty += 0.70
        elif abs(float(r)) >= 10000:
            mag_penalty += 0.42
        elif abs(float(r)) < 100:
            mag_penalty += 0.06
        score = conf + _electric_hint_score(c) + min(0.20, 0.06 * float(agree)) - mag_penalty
        ranked.append((score, c, agree))
    if not ranked:
        return None, 0
    ranked.sort(key=lambda x: x[0], reverse=True)
    top_score, top, top_agree = ranked[0]
    top_conf = _clamp_confidence(top.get("confidence", 0.0))
    if top_conf >= 0.90:
        return top, top_agree
    if top_conf >= 0.72 and top_agree >= 1:
        return top, top_agree
    if top_score >= 0.98:
        return top, top_agree
    return None, top_agree


def _pick_electric_bootstrap_relaxed(candidates: list[dict]) -> tuple[Optional[dict], int]:
    """
    Relaxed selector for hard photos:
    allow uncertain type if several variants converge to close numeric readings.
    """
    numeric: list[dict] = []
    for c in candidates:
        prov = str(c.get("provider") or "")
        if prov.endswith(":scale10") or prov.endswith(":scale100"):
            continue
        r = _normalize_reading(c.get("reading"))
        if r is None:
            continue
        if r < 0 or r > 100000000000:
            continue
        cc = dict(c)
        cc["reading"] = float(r)
        numeric.append(cc)
    if not numeric:
        return None, 0
    best = None
    best_support = -1
    best_score = -1e9
    for c in numeric:
        r = float(c.get("reading"))
        conf = _clamp_confidence(c.get("confidence", 0.0))
        support = 0
        for d in numeric:
            rd = _normalize_reading(d.get("reading"))
            if rd is None:
                continue
            if abs(float(rd) - r) <= 2.0:
                support += 1
        mag_penalty = 0.0
        if abs(float(r)) >= 100000:
            mag_penalty += 0.40
        elif abs(float(r)) >= 10000:
            mag_penalty += 0.16
        elif abs(float(r)) < 100:
            mag_penalty += 0.06
        score = support * 1.0 + conf * 0.7 + _electric_hint_score(c) - mag_penalty
        if (support > best_support) or (support == best_support and score > best_score):
            best_support = support
            best_score = score
            best = c
    if best is None:
        return None, 0
    best_conf = _clamp_confidence(best.get("confidence", 0.0))
    if best_support >= 2:
        return best, best_support - 1
    if best_conf >= 0.90:
        return best, 0
    return None, best_support - 1


def _electric_needs_hard_recovery(best: Optional[dict], agree: int) -> bool:
    if best is None:
        return True
    r = _normalize_reading(best.get("reading"))
    if r is None:
        return True
    if agree <= 1:
        return True
    if r < 500.0 or r > 20000.0:
        return True
    return False


def _pick_electric_hard_consensus(candidates: list[dict]) -> tuple[Optional[dict], int]:
    rows: list[dict] = []
    for c in candidates:
        r = _normalize_reading(c.get("reading"))
        if r is None:
            continue
        if r < 0 or r > 100000000:
            continue
        cc = dict(c)
        cc["reading"] = float(r)
        rows.append(cc)
    if not rows:
        return None, 0

    supports: dict[float, float] = {}
    counts: dict[float, int] = {}
    all_readings = [float(c.get("reading")) for c in rows]
    row_pairs = [
        (float(c.get("reading")), _clamp_confidence(c.get("confidence", 0.0)))
        for c in rows
    ]
    has_kilo_candidate = any(900.0 <= float(c.get("reading")) <= 20000.0 for c in rows)
    for c in rows:
        r = float(c.get("reading"))
        conf = _clamp_confidence(c.get("confidence", 0.0))
        key = round(r, 1)
        bonus = 0.0
        if 1800.0 <= r <= 9000.0:
            bonus += 0.15
        elif 900.0 <= r <= 20000.0:
            bonus += 0.06
        elif r < 500.0:
            bonus -= 0.25
        elif r > 50000.0:
            bonus -= 1.05
        elif r > 30000.0:
            bonus -= 0.70
        elif r > 12000.0:
            bonus -= 0.42
        if has_kilo_candidate:
            if 1000.0 <= r <= 20000.0:
                bonus += 0.12
            elif r < 100.0:
                bonus -= 0.80
            elif r < 500.0:
                bonus -= 0.35
        div10_peers = [d for d in all_readings if d <= 1200.0 and abs((r / 10.0) - d) <= 1.5]
        has_mul10_peer = any(abs((r * 10.0) - d) <= 15.0 for d in all_readings if d >= 1200.0)
        if r >= 1200.0:
            if len(div10_peers) >= 2:
                bonus -= 0.55
            elif len(div10_peers) == 1:
                bonus -= 0.28
        if r <= 1200.0 and has_mul10_peer:
            bonus += 0.20
        has_decimal_peer = any(
            (abs(d * 10.0 - r) <= 2.0) and (abs(d - round(d)) > 1e-6) and (dc >= 0.82)
            for d, dc in row_pairs
        )
        has_int_peer = any((abs(d - (r * 10.0)) <= 20.0) and (dc >= 0.82) for d, dc in row_pairs)
        if r >= 1200.0 and has_decimal_peer:
            bonus -= 0.40
        if r <= 1200.0 and has_int_peer and (abs(r - round(r)) > 1e-6):
            bonus += 0.18
        ri = int(round(r))
        if 1000 <= ri <= 9999:
            same_suffix = [
                int(round(v))
                for v in all_readings
                if 1000.0 <= v <= 9999.0 and (int(round(v)) % 1000) == (ri % 1000)
            ]
            uniq_suffix = sorted(set(same_suffix))
            if len(uniq_suffix) >= 2:
                if ri == uniq_suffix[-1]:
                    bonus += 0.22
                elif ri == uniq_suffix[0]:
                    bonus -= 0.18
        w = conf + _electric_hint_score(c) + bonus
        supports[key] = supports.get(key, 0.0) + w
        counts[key] = counts.get(key, 0) + 1

    ranked = sorted(supports.items(), key=lambda kv: (kv[1], counts.get(kv[0], 0)), reverse=True)
    if not ranked:
        return None, 0
    best_key, _ = ranked[0]
    if has_kilo_candidate and float(best_key) < 900.0:
        for k, _ in ranked:
            if float(k) >= 900.0:
                best_key = k
                break
    cluster = [c for c in rows if abs(float(c.get("reading")) - float(best_key)) <= 1.2]
    if not cluster:
        return None, 0
    pick = max(cluster, key=lambda c: (_clamp_confidence(c.get("confidence", 0.0)), _electric_hint_score(c)))
    support = max(0, len(cluster) - 1)
    if support >= 1 or _clamp_confidence(pick.get("confidence", 0.0)) >= 0.90:
        return pick, support
    return None, support


def _expand_electric_scaled_candidates(candidates: list[dict]) -> list[dict]:
    """
    Build scaled alternatives for common LCD OCR drift:
    extra trailing digit(s) in integer reads (e.g. 55362 -> 5536.2 / 553.62).
    """
    out: list[dict] = list(candidates)
    for c in candidates:
        r = _normalize_reading(c.get("reading"))
        if r is None:
            continue
        conf = _clamp_confidence(c.get("confidence", 0.0))
        if conf < 0.70:
            continue
        if abs(float(r) - round(float(r))) > 1e-6:
            continue
        v = int(round(float(r)))
        if abs(v) < 10000:
            continue
        for div, tag, penalty in ((10.0, "scale10", 0.08), (100.0, "scale100", 0.12)):
            rr = float(v) / div
            cc = dict(c)
            cc["reading"] = rr
            cc["confidence"] = _clamp_confidence(conf - penalty)
            cc["variant"] = f"{str(c.get('variant') or 'electric')}_{tag}"
            base_notes = str(c.get("notes") or "").strip()
            cc["notes"] = f"{base_notes}; electric_{tag}".strip("; ").strip()
            cc["provider"] = str(c.get("provider") or "openai-electric") + f":{tag}"
            out.append(cc)
    return out


def _electric_variant_rank(label: str) -> int:
    s = str(label or "").lower()
    if "center_clahe" in s:
        return 0
    if "lcd_tight_clahe" in s:
        return 1
    if "lcd_wide_clahe" in s:
        return 2
    if "lcd_digits_clahe" in s:
        return 3
    if "center_lcd" in s:
        return 4
    if "lcd_tight_lcd" in s:
        return 5
    if "mid_clahe" in s:
        return 6
    if "center_contrast" in s:
        return 7
    if "mid_contrast" in s:
        return 8
    if s.endswith("_bw"):
        return 10
    return 20


def _looks_like_serial_candidate(reading: Optional[float], serial: Optional[str]) -> bool:
    if reading is None or not serial:
        return False
    try:
        rd = "".join(ch for ch in f"{abs(float(reading)):.3f}" if ch.isdigit())
    except Exception:
        return False
    sd = "".join(ch for ch in str(serial) if ch.isdigit())
    if not rd or not sd:
        return False
    rd_nz = rd.lstrip("0")
    sd_nz = sd.lstrip("0")
    if rd and (rd in sd or sd.endswith(rd)):
        return True
    if rd_nz and (rd_nz in sd_nz or sd_nz.endswith(rd_nz)):
        return True
    return False


def _pick_best_serial(candidates: list[dict]) -> Optional[str]:
    best = None
    best_score = -1
    for c in candidates:
        s = c.get("serial")
        if not isinstance(s, str):
            continue
        digits = "".join(ch for ch in s if ch.isdigit())
        if not digits:
            continue
        # prefer longer digit-only serials
        sc = len(digits)
        if sc > best_score:
            best_score = sc
            best = s
    return best


def _call_google_vision_candidate(image_bytes: bytes) -> Optional[dict]:
    if not GOOGLE_VISION_API_KEY:
        return None
    b64 = base64.b64encode(image_bytes).decode("ascii")
    url = f"https://vision.googleapis.com/v1/images:annotate?key={GOOGLE_VISION_API_KEY}"
    payload = {
        "requests": [
            {
                "image": {"content": b64},
                "features": [{"type": "TEXT_DETECTION"}],
            }
        ]
    }
    r = requests.post(url, json=payload, timeout=30)
    if not r.ok:
        return None
    data = r.json()
    text_blob = ""
    try:
        text_blob = str(
            data["responses"][0]
            .get("fullTextAnnotation", {})
            .get("text", "")
        )
    except Exception:
        text_blob = ""
    if not text_blob.strip():
        return None

    meter_type = _classify_meter_type_from_text(text_blob)
    nums = _extract_numeric_candidates(text_blob)
    reading = max(nums) if nums else None
    serial = _extract_serial_from_text(text_blob)

    # conservative confidence: grows if type+reading found
    conf = 0.35
    if meter_type != "unknown":
        conf += 0.18
    if reading is not None:
        conf += 0.22
    if serial:
        conf += 0.05
    conf = _clamp_confidence(conf)
    return {
        "type": meter_type,
        "reading": reading,
        "serial": serial,
        "confidence": conf,
        "notes": "google_vision_text_detection",
    }


def _encode_jpeg(img: Image.Image, quality: int = 90) -> bytes:
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()


def _make_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    variants: list[tuple[str, bytes]] = []
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return [("orig", img_bytes)]

    variants.append(("orig", _encode_jpeg(img, quality=90)))

    # Helper: focused crop around dark/text-like region
    focused = None
    try:
        gray = ImageOps.autocontrast(img.convert("L"))
        gray = gray.filter(ImageFilter.MedianFilter(3))
        bw = gray.point(lambda p: 0 if p < 200 else 255, "L")
        inv = ImageOps.invert(bw)
        bbox = inv.getbbox()
        if bbox:
            w, h = img.size
            bw_box, bh_box = bbox[2] - bbox[0], bbox[3] - bbox[1]
            if (bw_box * bh_box) >= (w * h * 0.05):
                pad = int(min(w, h) * 0.03)
                left = max(0, bbox[0] - pad)
                upper = max(0, bbox[1] - pad)
                right = min(w, bbox[2] + pad)
                lower = min(h, bbox[3] + pad)
                v = img.crop((left, upper, right, lower))
                v = ImageEnhance.Contrast(v).enhance(1.5)
                v = v.filter(ImageFilter.UnsharpMask(radius=1, percent=160, threshold=3))
                focused = _encode_jpeg(v, quality=92)
    except Exception:
        focused = None

    # Helper: full-frame contrast boost (fallback if focused not available)
    contrast = None
    try:
        v2 = ImageEnhance.Contrast(img).enhance(1.6)
        v2 = v2.filter(ImageFilter.UnsharpMask(radius=1, percent=150, threshold=3))
        contrast = _encode_jpeg(v2, quality=92)
    except Exception:
        contrast = None

    # Orientation variant: rotate portrait or center-crop landscape
    orient = None
    orient_label = None
    try:
        if img.height > img.width:
            v3 = img.rotate(90, expand=True)
            orient = _encode_jpeg(v3, quality=90)
            orient_label = "rotate90"
        else:
            w, h = img.size
            cx, cy = w // 2, h // 2
            cw, ch = int(w * 0.8), int(h * 0.8)
            left = max(0, cx - cw // 2)
            upper = max(0, cy - ch // 2)
            right = min(w, left + cw)
            lower = min(h, upper + ch)
            v3 = img.crop((left, upper, right, lower))
            v3 = ImageEnhance.Contrast(v3).enhance(1.3)
            orient = _encode_jpeg(v3, quality=92)
            orient_label = "center_crop"
    except Exception:
        orient = None
        orient_label = None

    # Helper: center crop (useful when image contains too much background)
    center = None
    try:
        w, h = img.size
        cw, ch = int(w * 0.78), int(h * 0.78)
        left = max(0, (w - cw) // 2)
        upper = max(0, (h - ch) // 2)
        right = min(w, left + cw)
        lower = min(h, upper + ch)
        v4 = img.crop((left, upper, right, lower))
        v4 = ImageEnhance.Contrast(v4).enhance(1.35)
        v4 = v4.filter(ImageFilter.UnsharpMask(radius=1, percent=180, threshold=2))
        center = _encode_jpeg(v4, quality=92)
    except Exception:
        center = None

    # Helper: middle band (often contains digital display on electric meters)
    mid_band = None
    try:
        w, h = img.size
        top = int(h * 0.22)
        bottom = int(h * 0.68)
        if bottom > top:
            v5 = img.crop((0, top, w, bottom))
            v5 = ImageEnhance.Contrast(v5).enhance(1.5)
            v5 = v5.filter(ImageFilter.UnsharpMask(radius=1, percent=190, threshold=2))
            mid_band = _encode_jpeg(v5, quality=92)
    except Exception:
        mid_band = None

    # Helper: low-light enhancement for dark utility closet photos.
    lowlight = None
    try:
        g = ImageOps.autocontrast(img.convert("L"), cutoff=1)
        g_np = np.array(g)
        clahe = cv2.createCLAHE(clipLimit=2.4, tileGridSize=(8, 8))
        g2 = clahe.apply(g_np)
        rgb = Image.fromarray(cv2.cvtColor(g2, cv2.COLOR_GRAY2RGB))
        rgb = ImageEnhance.Brightness(rgb).enhance(1.12)
        rgb = ImageEnhance.Contrast(rgb).enhance(1.55)
        rgb = rgb.filter(ImageFilter.UnsharpMask(radius=1, percent=180, threshold=2))
        lowlight = _encode_jpeg(rgb, quality=92)
    except Exception:
        lowlight = None

    # Choose up to 6 variants (quality > speed; still bounded)
    if img.height > img.width:
        if orient and len(variants) < 6:
            variants.append((orient_label or "rotate90", orient))
        if focused and len(variants) < 6:
            variants.append(("focused_crop", focused))
        if center and len(variants) < 6:
            variants.append(("center_crop_strong", center))
        if mid_band and len(variants) < 6:
            variants.append(("middle_band", mid_band))
        if lowlight and len(variants) < 6:
            variants.append(("lowlight_enhanced", lowlight))
        if contrast and len(variants) < 6:
            variants.append(("contrast", contrast))
    else:
        if focused and len(variants) < 6:
            variants.append(("focused_crop", focused))
        if mid_band and len(variants) < 6:
            variants.append(("middle_band", mid_band))
        if center and len(variants) < 6:
            variants.append(("center_crop_strong", center))
        if lowlight and len(variants) < 6:
            variants.append(("lowlight_enhanced", lowlight))
        if orient and len(variants) < 6:
            variants.append((orient_label or "center_crop", orient))
        if contrast and len(variants) < 6:
            variants.append(("contrast", contrast))

    return variants[:6]


def _looks_like_water_meter_face(img_bytes: bytes) -> bool:
    """
    Cheap geometry heuristic to avoid running heavy electric bootstrap on water photos.
    Water meters usually have a dominant circular dial in the frame.
    """
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception:
        return False
    if im is None:
        return False
    h, w = im.shape[:2]
    if h < 120 or w < 120:
        return False
    max_dim = max(h, w)
    if max_dim > 960:
        scale = 960.0 / float(max_dim)
        im = cv2.resize(im, (int(round(w * scale)), int(round(h * scale))), interpolation=cv2.INTER_AREA)
        h, w = im.shape[:2]
    gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (7, 7), 0)
    min_side = max(1, min(h, w))
    min_r = max(24, int(min_side * 0.12))
    max_r = max(min_r + 2, int(min_side * 0.48))
    try:
        circles = cv2.HoughCircles(
            gray,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=float(min_side * 0.35),
            param1=110,
            param2=24,
            minRadius=min_r,
            maxRadius=max_r,
        )
    except Exception:
        return False
    return circles is not None and len(circles[0]) > 0


def _make_electric_display_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return out
    try:
        w, h = img.size
        bands = [
            ("ed_mid", (0, int(h * 0.26), w, int(h * 0.72))),
            ("ed_center", (int(w * 0.08), int(h * 0.22), int(w * 0.92), int(h * 0.78))),
            ("ed_lower_mid", (0, int(h * 0.34), w, int(h * 0.82))),
            # Tight LCD-first windows for Mercury-like meters:
            # display is often on the left block, with strong glare/noise around.
            ("ed_lcd_tight", (int(w * 0.10), int(h * 0.33), int(w * 0.78), int(h * 0.66))),
            ("ed_lcd_wide", (int(w * 0.05), int(h * 0.30), int(w * 0.84), int(h * 0.70))),
            ("ed_lcd_digits", (int(w * 0.14), int(h * 0.38), int(w * 0.70), int(h * 0.62))),
        ]
        for base_label, (x1, y1, x2, y2) in bands:
            x1 = max(0, min(w - 1, x1))
            y1 = max(0, min(h - 1, y1))
            x2 = max(x1 + 1, min(w, x2))
            y2 = max(y1 + 1, min(h, y2))
            crop = img.crop((x1, y1, x2, y2))

            c1 = ImageEnhance.Contrast(crop).enhance(1.75)
            c1 = ImageEnhance.Sharpness(c1).enhance(1.45)
            c1 = c1.filter(ImageFilter.UnsharpMask(radius=1, percent=220, threshold=2))
            out.append((f"{base_label}_contrast", _encode_jpeg(c1, quality=94)))

            g = cv2.cvtColor(np.array(crop), cv2.COLOR_RGB2GRAY)
            g = cv2.GaussianBlur(g, (3, 3), 0)
            clahe = cv2.createCLAHE(clipLimit=2.8, tileGridSize=(8, 8))
            g2 = clahe.apply(g)
            rgb2 = Image.fromarray(cv2.cvtColor(g2, cv2.COLOR_GRAY2RGB))
            rgb2 = ImageEnhance.Contrast(rgb2).enhance(1.38)
            out.append((f"{base_label}_clahe", _encode_jpeg(rgb2, quality=94)))

            bw = cv2.adaptiveThreshold(
                g2,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                31,
                6,
            )
            rgb3 = Image.fromarray(cv2.cvtColor(bw, cv2.COLOR_GRAY2RGB))
            out.append((f"{base_label}_bw", _encode_jpeg(rgb3, quality=94)))

            # LCD-dark enhancement: brighten dark screen while suppressing dust texture.
            g3 = cv2.GaussianBlur(g2, (0, 0), 1.0)
            g3 = cv2.addWeighted(g2, 1.45, g3, -0.45, 0)
            g3 = cv2.normalize(g3, None, 0, 255, cv2.NORM_MINMAX)
            rgb4 = Image.fromarray(cv2.cvtColor(g3, cv2.COLOR_GRAY2RGB))
            rgb4 = ImageEnhance.Contrast(rgb4).enhance(1.55)
            out.append((f"{base_label}_lcd", _encode_jpeg(rgb4, quality=94)))

    except Exception:
        return out[:14]
    return out[:14]


_SEGMENT_DIGIT_BY_MASK: dict[int, int] = {
    0b1110111: 0,
    0b0010010: 1,
    0b1011101: 2,
    0b1011011: 3,
    0b0111010: 4,
    0b1101011: 5,
    0b1101111: 6,
    0b1010010: 7,
    0b1111111: 8,
    0b1111011: 9,
}
def _hamming_distance_bits(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


def _decode_7seg_digit_from_bin(bin_digit: np.ndarray) -> tuple[Optional[int], float]:
    h, w = bin_digit.shape[:2]
    if h < 12 or w < 6:
        return None, 0.0
    # Normalized 7-seg probing windows:
    #  000
    # 1   2
    #  333
    # 4   5
    #  666
    windows = [
        (0.22, 0.05, 0.78, 0.18),  # top
        (0.08, 0.18, 0.28, 0.47),  # upper-left
        (0.72, 0.18, 0.92, 0.47),  # upper-right
        (0.22, 0.43, 0.78, 0.58),  # middle
        (0.08, 0.54, 0.28, 0.84),  # lower-left
        (0.72, 0.54, 0.92, 0.84),  # lower-right
        (0.22, 0.80, 0.78, 0.95),  # bottom
    ]
    vals: list[float] = []
    bits = 0
    for idx, (x1f, y1f, x2f, y2f) in enumerate(windows):
        x1 = max(0, min(w - 1, int(round(w * x1f))))
        y1 = max(0, min(h - 1, int(round(h * y1f))))
        x2 = max(x1 + 1, min(w, int(round(w * x2f))))
        y2 = max(y1 + 1, min(h, int(round(h * y2f))))
        cell = bin_digit[y1:y2, x1:x2]
        if cell.size == 0:
            vals.append(0.0)
            continue
        mean_on = float(np.mean(cell > 0))
        vals.append(mean_on)
        if mean_on >= 0.42:
            bits |= (1 << idx)
    if bits in _SEGMENT_DIGIT_BY_MASK:
        conf = min(1.0, 0.72 + 0.28 * float(sum(vals) / 7.0))
        return _SEGMENT_DIGIT_BY_MASK[bits], conf
    # tolerate a single missed/extra segment
    best_digit = None
    best_dist = 99
    for mask, digit in _SEGMENT_DIGIT_BY_MASK.items():
        d = _hamming_distance_bits(bits, mask)
        if d < best_dist:
            best_dist = d
            best_digit = digit
    if best_digit is not None and best_dist <= 1:
        conf = 0.58 - 0.12 * float(best_dist)
        return int(best_digit), max(0.35, conf)
    return None, 0.0


def _extract_digit_boxes_from_bin(bin_img: np.ndarray) -> list[tuple[int, int]]:
    h, w = bin_img.shape[:2]
    if h < 16 or w < 16:
        return []
    bw = (bin_img > 0).astype(np.uint8)
    # ignore left/right frame strips
    margin = max(2, int(w * 0.03))
    inner = bw[:, margin : max(margin + 1, w - margin)]
    if inner.size == 0:
        return []
    proj = np.sum(inner, axis=0).astype(np.float32)
    mx = float(np.max(proj))
    if mx <= 0.0:
        return []
    thr = max(1.0, mx * 0.08)
    runs: list[tuple[int, int]] = []
    s = -1
    for i, v in enumerate(proj):
        if v >= thr and s < 0:
            s = i
        elif v < thr and s >= 0:
            if (i - s) >= max(4, int(w * 0.012)):
                runs.append((s + margin, i + margin))
            s = -1
    if s >= 0 and (len(proj) - s) >= max(4, int(w * 0.012)):
        runs.append((s + margin, len(proj) + margin))
    if not runs:
        return []
    inner_w = max(1, (w - 2 * margin))
    if len(runs) == 1:
        a0, b0 = runs[0]
        if (b0 - a0) >= int(inner_w * 0.55):
            parts = 5 if inner_w >= 240 else (4 if inner_w >= 170 else 3)
            sw = (b0 - a0) / float(parts)
            forced: list[tuple[int, int]] = []
            for p in range(parts):
                x1 = int(round(a0 + p * sw))
                x2 = int(round(a0 + (p + 1) * sw))
                if (x2 - x1) >= max(4, int(w * 0.010)):
                    forced.append((x1, x2))
            if len(forced) >= 3:
                runs = forced
    # split very wide runs (multiple digits glued together)
    split_runs: list[tuple[int, int]] = []
    medw = float(np.median([b - a for a, b in runs])) if runs else 0.0
    if medw <= 0:
        medw = max(8.0, float(w) * 0.08)
    for a, b in runs:
        rw = b - a
        if rw > medw * 1.95:
            parts = int(round(rw / medw))
            parts = max(2, min(4, parts))
            sw = rw / float(parts)
            for p in range(parts):
                x1 = int(round(a + p * sw))
                x2 = int(round(a + (p + 1) * sw))
                if (x2 - x1) >= max(4, int(w * 0.010)):
                    split_runs.append((x1, x2))
        else:
            split_runs.append((a, b))
    # keep plausible widths
    min_w = max(4, int(w * 0.010))
    max_w = max(12, int(w * 0.28))
    out = [(a, b) for a, b in split_runs if min_w <= (b - a) <= max_w]
    return out[:8]


def _decode_7seg_reading(gray: np.ndarray) -> tuple[Optional[float], float]:
    if gray.size == 0:
        return None, 0.0
    # deterministic enhancement
    g = cv2.GaussianBlur(gray, (3, 3), 0)
    g = cv2.createCLAHE(clipLimit=2.8, tileGridSize=(8, 8)).apply(g)
    b1 = cv2.adaptiveThreshold(g, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 6)
    b2 = cv2.adaptiveThreshold(g, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 25, 4)
    candidates = [b1, cv2.bitwise_not(b1), b2, cv2.bitwise_not(b2)]

    best_read: Optional[float] = None
    best_score = -1.0
    for b in candidates:
        fill = float(np.mean(b > 0))
        if fill < 0.02 or fill > 0.62:
            continue
        boxes = _extract_digit_boxes_from_bin(b)
        if len(boxes) < 3 or len(boxes) > 6:
            continue
        digits: list[str] = []
        confs: list[float] = []
        widths: list[int] = []
        for x1, x2 in boxes:
            roi = b[:, x1:x2]
            d, dc = _decode_7seg_digit_from_bin(roi)
            if d is None:
                continue
            digits.append(str(d))
            confs.append(dc)
            widths.append(max(1, x2 - x1))
        if len(digits) < 3:
            continue
        s = "".join(digits)
        if len(s) < 3 or len(s) > 6:
            continue
        # Reject degenerate runs (typical false positive on dusty LCD background).
        if len(s) >= 3 and len(set(s)) <= 1:
            continue
        # Reject unlikely width distortion.
        ww = float(np.mean(widths)) if widths else 0.0
        if ww <= 0.0:
            continue
        if max(widths) > ww * 2.8:
            continue
        # integer-first policy for electric: decimals often unstable on these LCD shots
        try:
            reading = float(int(s))
        except Exception:
            continue
        if reading < 0 or reading > 30000:
            continue
        mean_conf = float(sum(confs) / max(1, len(confs)))
        uniq_bonus = 0.04 * min(4, len(set(s)))
        len_bonus = 0.03 * min(5, len(s))
        score = mean_conf + uniq_bonus + len_bonus
        if len(s) >= 4 and reading >= 10000:
            score -= 0.32
        if reading >= 20000:
            score -= 0.30
        if score > best_score:
            best_score = score
            best_read = reading
    if best_read is None:
        return None, 0.0
    return best_read, max(0.35, min(0.72, best_score))


def _tighten_lcd_row(gray: np.ndarray) -> np.ndarray:
    h, w = gray.shape[:2]
    if h < 24 or w < 24:
        return gray
    g = cv2.GaussianBlur(gray, (3, 3), 0)
    gx = cv2.Sobel(g, cv2.CV_32F, 1, 0, ksize=3)
    gy = cv2.Sobel(g, cv2.CV_32F, 0, 1, ksize=3)
    mag = cv2.convertScaleAbs(cv2.addWeighted(cv2.absdiff(gx, 0), 0.7, cv2.absdiff(gy, 0), 0.3, 0))
    row_score = np.sum(mag, axis=1).astype(np.float32)
    if float(np.max(row_score)) <= 0.0:
        return gray
    k = max(5, (h // 20) * 2 + 1)
    row_score = cv2.GaussianBlur(row_score.reshape(-1, 1), (1, k), 0).reshape(-1)
    cy = int(np.argmax(row_score))
    band = max(int(h * 0.28), 18)
    y1 = max(0, cy - band // 2)
    y2 = min(h, y1 + band)
    if (y2 - y1) < 12:
        return gray
    return gray[y1:y2, :]


def _dhash_from_gray(gray: np.ndarray, size: int = 8) -> int:
    if gray.size == 0:
        return 0
    small = cv2.resize(gray, (size + 1, size), interpolation=cv2.INTER_AREA)
    bits = 0
    bit_idx = 0
    for y in range(size):
        for x in range(size):
            if int(small[y, x]) > int(small[y, x + 1]):
                bits |= (1 << bit_idx)
            bit_idx += 1
    return bits


def _hamming64(a: int, b: int) -> int:
    return int((int(a) ^ int(b)).bit_count())


def _electric_template_hashes(img_bytes: bytes) -> dict[str, int]:
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return {}
    arr = np.array(img)
    h, w = arr.shape[:2]
    if h < 10 or w < 10:
        return {}

    def _crop_hash(box: tuple[float, float, float, float]) -> int:
        x1 = max(0, min(w - 1, int(round(w * box[0]))))
        y1 = max(0, min(h - 1, int(round(h * box[1]))))
        x2 = max(x1 + 1, min(w, int(round(w * box[2]))))
        y2 = max(y1 + 1, min(h, int(round(h * box[3]))))
        g = cv2.cvtColor(arr[y1:y2, x1:x2], cv2.COLOR_RGB2GRAY)
        return _dhash_from_gray(g, size=8)

    return {
        "full": _crop_hash((0.00, 0.00, 1.00, 1.00)),
        "lcd_wide": _crop_hash((0.08, 0.30, 0.84, 0.70)),
        "lcd_mid": _crop_hash((0.14, 0.36, 0.72, 0.60)),
        "lcd_tight": _crop_hash((0.18, 0.40, 0.70, 0.55)),
    }


def _load_electric_template_rows() -> list[dict]:
    global _ELECTRIC_TEMPLATE_MTIME, _ELECTRIC_TEMPLATE_ROWS
    path = OCR_ELECTRIC_TEMPLATE_DB
    if not OCR_ELECTRIC_TEMPLATE_MATCH or not path:
        return []
    try:
        st = os.stat(path)
    except Exception:
        with _ELECTRIC_TEMPLATE_LOCK:
            _ELECTRIC_TEMPLATE_MTIME = -1.0
            _ELECTRIC_TEMPLATE_ROWS = []
        return []
    with _ELECTRIC_TEMPLATE_LOCK:
        if st.st_mtime == _ELECTRIC_TEMPLATE_MTIME:
            return list(_ELECTRIC_TEMPLATE_ROWS)
        try:
            raw = json.loads(open(path, "r", encoding="utf-8").read())
        except Exception:
            _ELECTRIC_TEMPLATE_MTIME = st.st_mtime
            _ELECTRIC_TEMPLATE_ROWS = []
            return []
        raw_rows = raw.get("rows") if isinstance(raw, dict) else raw
        rows: list[dict] = []
        if isinstance(raw_rows, list):
            for r in raw_rows:
                if not isinstance(r, dict):
                    continue
                reading = _normalize_reading(r.get("reading"))
                hashes = r.get("hashes")
                if reading is None or not isinstance(hashes, dict):
                    continue
                row_hashes: dict[str, int] = {}
                for key in ("full", "lcd_wide", "lcd_mid", "lcd_tight"):
                    try:
                        row_hashes[key] = int(str(hashes.get(key, "0")), 16)
                    except Exception:
                        row_hashes[key] = 0
                rows.append(
                    {
                        "reading": reading,
                        "type": _sanitize_type(r.get("type", "Электро")),
                        "serial": r.get("serial"),
                        "hashes": row_hashes,
                    }
                )
        _ELECTRIC_TEMPLATE_MTIME = st.st_mtime
        _ELECTRIC_TEMPLATE_ROWS = rows
        return list(rows)


def _electric_template_candidates(img_bytes: bytes) -> list[dict]:
    rows = _load_electric_template_rows()
    if not rows:
        return []
    qh = _electric_template_hashes(img_bytes)
    if not qh:
        return []
    scored: list[tuple[float, dict]] = []
    for row in rows:
        rh = row.get("hashes") or {}
        d = (
            _hamming64(qh.get("full", 0), rh.get("full", 0))
            + _hamming64(qh.get("lcd_wide", 0), rh.get("lcd_wide", 0))
            + _hamming64(qh.get("lcd_mid", 0), rh.get("lcd_mid", 0))
            + _hamming64(qh.get("lcd_tight", 0), rh.get("lcd_tight", 0))
        )
        scored.append((float(d), row))
    if not scored:
        return []
    scored.sort(key=lambda x: x[0])
    best_d, best_row = scored[0]
    second_d = scored[1][0] if len(scored) > 1 else 1e9
    # Acceptance tuned for recompressed JPEG paths (docker cp / messaging apps).
    if best_d > 40.0:
        return []
    if (second_d - best_d) < 6.0:
        return []
    conf = 0.80 + max(0.0, (40.0 - best_d) * 0.004)
    return [
        {
            "type": "Электро",
            "reading": _normalize_reading(best_row.get("reading")),
            "serial": best_row.get("serial"),
            "confidence": _clamp_confidence(conf),
            "notes": "template_hash_match",
            "note2": "",
            "variant": "electric_template",
            "provider": "det-electric:template",
        }
    ]


def _water_template_hashes(img_bytes: bytes) -> dict[str, int]:
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return {}
    arr = np.array(img)
    h, w = arr.shape[:2]
    if h < 10 or w < 10:
        return {}

    def _crop_hash(box: tuple[float, float, float, float]) -> int:
        x1 = max(0, min(w - 1, int(round(w * box[0]))))
        y1 = max(0, min(h - 1, int(round(h * box[1]))))
        x2 = max(x1 + 1, min(w, int(round(w * box[2]))))
        y2 = max(y1 + 1, min(h, int(round(h * box[3]))))
        g = cv2.cvtColor(arr[y1:y2, x1:x2], cv2.COLOR_RGB2GRAY)
        return _dhash_from_gray(g, size=8)

    return {
        "full": _crop_hash((0.00, 0.00, 1.00, 1.00)),
        "mid": _crop_hash((0.10, 0.22, 0.90, 0.88)),
        "center": _crop_hash((0.18, 0.30, 0.82, 0.78)),
    }


def _load_water_template_rows() -> list[dict]:
    global _WATER_TEMPLATE_MTIME, _WATER_TEMPLATE_ROWS
    path = OCR_WATER_TEMPLATE_DB
    if not OCR_WATER_TEMPLATE_MATCH or not path:
        return []
    try:
        st = os.stat(path)
    except Exception:
        with _WATER_TEMPLATE_LOCK:
            _WATER_TEMPLATE_MTIME = -1.0
            _WATER_TEMPLATE_ROWS = []
        return []
    with _WATER_TEMPLATE_LOCK:
        if st.st_mtime == _WATER_TEMPLATE_MTIME:
            return list(_WATER_TEMPLATE_ROWS)
        try:
            raw = json.loads(open(path, "r", encoding="utf-8").read())
        except Exception:
            _WATER_TEMPLATE_MTIME = st.st_mtime
            _WATER_TEMPLATE_ROWS = []
            return []
        raw_rows = raw.get("rows") if isinstance(raw, dict) else raw
        rows: list[dict] = []
        if isinstance(raw_rows, list):
            for r in raw_rows:
                if not isinstance(r, dict):
                    continue
                reading = _normalize_reading(r.get("reading"))
                hashes = r.get("hashes")
                if reading is None or not isinstance(hashes, dict):
                    continue
                row_hashes: dict[str, int] = {}
                for key in ("full", "mid", "center"):
                    try:
                        row_hashes[key] = int(str(hashes.get(key, "0")), 16)
                    except Exception:
                        row_hashes[key] = 0
                rows.append(
                    {
                        "reading": reading,
                        "type": _sanitize_type(r.get("type", "unknown")),
                        "serial": r.get("serial"),
                        "hashes": row_hashes,
                    }
                )
        _WATER_TEMPLATE_MTIME = st.st_mtime
        _WATER_TEMPLATE_ROWS = rows
        return list(rows)


def _water_template_candidates(img_bytes: bytes) -> list[dict]:
    rows = _load_water_template_rows()
    if not rows:
        return []
    qh = _water_template_hashes(img_bytes)
    if not qh:
        return []
    scored: list[tuple[float, dict]] = []
    for row in rows:
        rh = row.get("hashes") or {}
        d = (
            _hamming64(qh.get("full", 0), rh.get("full", 0))
            + _hamming64(qh.get("mid", 0), rh.get("mid", 0))
            + _hamming64(qh.get("center", 0), rh.get("center", 0))
        )
        scored.append((float(d), row))
    if not scored:
        return []
    scored.sort(key=lambda x: x[0])
    best_d, best_row = scored[0]
    second_d = scored[1][0] if len(scored) > 1 else 1e9
    if best_d > 36.0:
        return []
    if (second_d - best_d) < 6.0:
        return []
    conf = _clamp_confidence(0.78 + max(0.0, (46.0 - best_d) * 0.004))
    return [
        {
            "type": best_row.get("type") or "unknown",
            "reading": _normalize_reading(best_row.get("reading")),
            "serial": best_row.get("serial"),
            "confidence": conf,
            "notes": "template_hash_match_water",
            "note2": "",
            "variant": "water_template",
            "provider": "det-water:template",
            "black_digits": None,
            "red_digits": None,
        }
    ]


_DRUM_TEMPLATE_CACHE: Optional[dict[int, list[np.ndarray]]] = None


def _build_drum_digit_templates() -> dict[int, list[np.ndarray]]:
    global _DRUM_TEMPLATE_CACHE
    if _DRUM_TEMPLATE_CACHE is not None:
        return _DRUM_TEMPLATE_CACHE
    templates: dict[int, list[np.ndarray]] = {d: [] for d in range(10)}
    fonts = (
        cv2.FONT_HERSHEY_SIMPLEX,
        cv2.FONT_HERSHEY_DUPLEX,
        cv2.FONT_HERSHEY_TRIPLEX,
    )
    w, h = 44, 72
    for d in range(10):
        txt = str(d)
        for font in fonts:
            for scale in (1.4, 1.6, 1.8, 2.0):
                for thick in (2, 3, 4):
                    img = np.zeros((h, w), dtype=np.uint8)
                    (tw, th), _ = cv2.getTextSize(txt, font, scale, thick)
                    x = max(0, (w - tw) // 2)
                    y = max(th + 1, (h + th) // 2)
                    cv2.putText(img, txt, (x, y), font, scale, 255, thick, cv2.LINE_AA)
                    b = cv2.threshold(img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
                    templates[d].append(b)
                    templates[d].append(255 - b)
    _DRUM_TEMPLATE_CACHE = templates
    return templates


def _drum_recognize_slot(slot_bgr: np.ndarray, templates: dict[int, list[np.ndarray]]) -> tuple[Optional[int], float, float]:
    if slot_bgr.size == 0:
        return None, 0.0, 0.0
    g = cv2.cvtColor(slot_bgr, cv2.COLOR_BGR2GRAY)
    g = cv2.GaussianBlur(g, (3, 3), 0)
    g = cv2.createCLAHE(clipLimit=2.8, tileGridSize=(4, 4)).apply(g)
    b = cv2.threshold(g, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    probes = (
        cv2.resize(b, (44, 72), interpolation=cv2.INTER_CUBIC),
        cv2.resize(255 - b, (44, 72), interpolation=cv2.INTER_CUBIC),
    )
    best = (-2.0, None)
    second = (-2.0, None)
    for p in probes:
        for digit, arr in templates.items():
            for t in arr:
                score = float(cv2.matchTemplate(p, t, cv2.TM_CCOEFF_NORMED)[0, 0])
                if score > best[0]:
                    second = best
                    best = (score, digit)
                elif score > second[0]:
                    second = (score, digit)
    if best[1] is None:
        return None, 0.0, 0.0
    # Map matcher score to practical confidence range.
    conf = max(0.0, min(1.0, (best[0] - 0.12) / 0.55))
    margin = max(0.0, best[0] - max(-1.0, second[0]))
    return int(best[1]), float(conf), float(margin)


def _electric_drum_candidates(img_bytes: bytes) -> list[dict]:
    if not OCR_ELECTRIC_DRUM_ENABLED:
        return []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception:
        return []
    if img is None:
        return []
    h, w = img.shape[:2]
    if h < 120 or w < 120:
        return []

    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    red_mask = cv2.inRange(hsv, (0, 70, 55), (15, 255, 255)) | cv2.inRange(hsv, (160, 70, 55), (179, 255, 255))
    red_mask = cv2.morphologyEx(red_mask, cv2.MORPH_OPEN, np.ones((3, 3), np.uint8), iterations=1)
    red_mask = cv2.medianBlur(red_mask, 5)
    cnts, _ = cv2.findContours(red_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not cnts:
        return []

    templates = _build_drum_digit_templates()
    out: list[dict] = []
    for c in cnts:
        x, y, bw, bh = cv2.boundingRect(c)
        area = float(bw * bh)
        if area < (w * h * 0.0018) or area > (w * h * 0.08):
            continue
        cx = (x + bw * 0.5) / float(w)
        cy = (y + bh * 0.5) / float(h)
        ar = bw / float(max(1, bh))
        # Red decimal wheel on drum meters is usually near right side of counter window.
        if not (0.48 <= cx <= 0.82 and 0.20 <= cy <= 0.58 and 0.35 <= ar <= 1.45):
            continue

        slot_w = max(10, int(round(bw * 0.95)))
        right = x + bw
        left = max(0, right - slot_w * 6)
        y1 = max(0, y - int(round(bh * 0.45)))
        y2 = min(h, y + int(round(bh * 1.20)))
        band = img[y1:y2, left:right]
        if band.size == 0:
            continue
        band = cv2.resize(band, (slot_w * 6 * 3, max(20, (y2 - y1) * 3)), interpolation=cv2.INTER_CUBIC)
        sw = max(8, band.shape[1] // 6)

        digits: list[str] = []
        confs: list[float] = []
        margins: list[float] = []
        for i in range(6):
            slot = band[:, i * sw : (i + 1) * sw]
            d, dc, dm = _drum_recognize_slot(slot, templates)
            if d is None:
                digits = []
                break
            digits.append(str(d))
            confs.append(dc)
            margins.append(dm)
        if len(digits) != 6:
            continue

        int_part = "".join(digits[:5]).lstrip("0") or "0"
        frac_digit = digits[5]
        try:
            reading = float(f"{int(int_part)}.{int(frac_digit)}")
        except Exception:
            continue
        if reading < 0 or reading > 30000:
            continue

        mean_conf = float(sum(confs) / len(confs))
        mean_margin = float(sum(margins) / len(margins))
        # Guard against false positives on LCD shots with random red LEDs.
        if mean_conf < 0.30 or mean_margin < 0.008:
            continue
        total_conf = _clamp_confidence(0.55 + 0.35 * mean_conf + 0.10 * min(1.0, mean_margin / 0.05))
        out.append(
            {
                "type": "Электро",
                "reading": reading,
                "serial": None,
                "confidence": float(total_conf),
                "notes": "deterministic_drum",
                "note2": "",
                "variant": "electric_drum_red_anchor",
                "provider": "det-electric:drum",
            }
        )
    return out


def _electric_mask_sticker(img_bgr: np.ndarray) -> np.ndarray:
    out = img_bgr.copy()
    h, w = out.shape[:2]
    hsv = cv2.cvtColor(out, cv2.COLOR_BGR2HSV)
    # White sticker masks often occlude LCD digits on Mercury photos.
    m = cv2.inRange(hsv, (0, 0, 150), (180, 65, 255))
    m = cv2.morphologyEx(m, cv2.MORPH_OPEN, np.ones((5, 5), np.uint8), iterations=1)
    cnts, _ = cv2.findContours(m, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for c in cnts:
        x, y, bw, bh = cv2.boundingRect(c)
        area = bw * bh
        if area < int(w * h * 0.02) or area > int(w * h * 0.45):
            continue
        ar = bw / float(max(1, bh))
        if ar < 1.5 or ar > 8.0:
            continue
        cx = (x + bw * 0.5) / float(max(1, w))
        cy = (y + bh * 0.5) / float(max(1, h))
        if not (0.18 <= cx <= 0.82 and 0.30 <= cy <= 0.82):
            continue
        mm = np.zeros((h, w), dtype=np.uint8)
        cv2.rectangle(mm, (x, y), (x + bw, y + bh), 255, -1)
        out = cv2.inpaint(out, mm, 5, cv2.INPAINT_TELEA)
    return out


def _electric_parse_numeric_text(text: str) -> list[float]:
    if not text:
        return []
    s = text.replace(",", ".")
    chunks = re.findall(r"\d{3,6}(?:\.\d{1,2})?", s)
    out: list[float] = []
    for c in chunks:
        try:
            v = float(c)
        except Exception:
            continue
        if 0.0 <= v <= 30000.0:
            out.append(v)
    # Also keep integer-only chunks when decimal dot was dropped by OCR.
    for c in re.findall(r"\d{3,6}", s):
        try:
            v = float(int(c))
        except Exception:
            continue
        if 0.0 <= v <= 30000.0:
            out.append(v)
    uniq: list[float] = []
    seen: set[int] = set()
    for v in out:
        key = int(round(v * 100))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(v)
    return uniq


def _electric_tesseract_candidates(img_bytes: bytes) -> list[dict]:
    if not OCR_ELECTRIC_TESSERACT_ENABLED or pytesseract is None:
        return []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception:
        return []
    if img is None:
        return []
    h, w = img.shape[:2]
    if h < 100 or w < 100:
        return []

    # Wide coverage windows for LCD and drum counters.
    rois = [
        ("lcd_main", (0.10, 0.18, 0.88, 0.52)),
        ("lcd_tight", (0.16, 0.22, 0.80, 0.46)),
        ("drum_main", (0.32, 0.18, 0.72, 0.45)),
    ]
    out: list[dict] = []
    for tag, (x1f, y1f, x2f, y2f) in rois:
        x1 = max(0, min(w - 1, int(round(w * x1f))))
        y1 = max(0, min(h - 1, int(round(h * y1f))))
        x2 = max(x1 + 1, min(w, int(round(w * x2f))))
        y2 = max(y1 + 1, min(h, int(round(h * y2f))))
        crop = img[y1:y2, x1:x2]
        if crop.size == 0:
            continue
        crop = _electric_mask_sticker(crop)
        crop = cv2.resize(crop, (crop.shape[1] * 3, crop.shape[0] * 3), interpolation=cv2.INTER_CUBIC)
        g = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
        cl = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8)).apply(g)
        th = cv2.adaptiveThreshold(cl, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 5)
        inv = cv2.bitwise_not(th)
        variants = [
            ("cl", cl),
            ("th", th),
            ("inv", inv),
        ]
        for vname, arr2d in variants:
            try:
                data = pytesseract.image_to_data(
                    arr2d,
                    output_type=pytesseract.Output.DICT,
                    config="--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789.,",
                    timeout=OCR_TESSERACT_TIMEOUT_SEC,
                )
            except Exception:
                continue
            txt = " ".join([str(t or "") for t in data.get("text", [])]).strip()
            vals = _electric_parse_numeric_text(txt)
            if not vals:
                continue
            confs: list[float] = []
            for c in data.get("conf", []) or []:
                try:
                    cf = float(c)
                except Exception:
                    continue
                if cf >= 0:
                    confs.append(cf)
            mean_conf = float(sum(confs) / max(1, len(confs))) if confs else 0.0
            # Very conservative: accept only reasonably clean OCR words.
            if mean_conf < 45.0:
                continue
            base_conf = _clamp_confidence(min(0.88, 0.42 + (mean_conf / 100.0) * 0.52))
            for v in vals[:3]:
                out.append(
                    {
                        "type": "Электро",
                        "reading": float(v),
                        "serial": None,
                        "confidence": float(base_conf),
                        "notes": "deterministic_tesseract",
                        "note2": "",
                        "variant": f"electric_tess_{tag}_{vname}",
                        "provider": "det-electric:tesseract",
                    }
                )
    return out


def _electric_deterministic_candidates(img_bytes: bytes) -> list[dict]:
    if not OCR_ELECTRIC_DETERMINISTIC and not OCR_ELECTRIC_TEMPLATE_MATCH:
        return []
    out: list[dict] = []
    try:
        out.extend(_electric_template_candidates(img_bytes))
    except Exception:
        pass
    try:
        out.extend(_electric_drum_candidates(img_bytes))
    except Exception:
        pass
    try:
        out.extend(_electric_tesseract_candidates(img_bytes))
    except Exception:
        pass
    if not OCR_ELECTRIC_DETERMINISTIC:
        return out
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return out
    w, h = img.size
    roi_specs = [
        ("det_lcd_digits", (0.14, 0.38, 0.70, 0.62)),
        ("det_lcd_tight", (0.10, 0.33, 0.78, 0.66)),
        ("det_lcd_wide", (0.05, 0.30, 0.84, 0.70)),
        ("det_center", (0.08, 0.22, 0.92, 0.78)),
        ("det_mid", (0.00, 0.26, 1.00, 0.72)),
    ]
    seg_rows: list[dict] = []
    for label, (x1f, y1f, x2f, y2f) in roi_specs[:OCR_ELECTRIC_DET_MAX_VARIANTS]:
        x1 = max(0, min(w - 1, int(round(w * x1f))))
        y1 = max(0, min(h - 1, int(round(h * y1f))))
        x2 = max(x1 + 1, min(w, int(round(w * x2f))))
        y2 = max(y1 + 1, min(h, int(round(h * y2f))))
        crop = img.crop((x1, y1, x2, y2))
        arr = np.array(crop)
        gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
        for suffix, gsrc in (("full", gray), ("row", _tighten_lcd_row(gray))):
            reading, conf = _decode_7seg_reading(gsrc)
            if reading is None:
                continue
            seg_rows.append(
                {
                    "type": "Электро",
                    "reading": float(reading),
                    "serial": None,
                    "confidence": float(conf),
                    "notes": "deterministic_7seg",
                    "note2": "",
                    "variant": f"electric_{label}_{suffix}",
                    "provider": "det-electric:7seg",
                }
            )
    # Guard against random false-positive reads on noisy LCD photos:
    # require repeated agreement across ROIs; otherwise confidence is downgraded.
    if seg_rows:
        freq: dict[int, int] = {}
        for r in seg_rows:
            rr = _normalize_reading(r.get("reading"))
            if rr is None:
                continue
            key = int(round(float(rr)))
            freq[key] = freq.get(key, 0) + 1
        for r in seg_rows:
            rr = _normalize_reading(r.get("reading"))
            if rr is None:
                continue
            key = int(round(float(rr)))
            agree = int(freq.get(key, 0))
            c = float(r.get("confidence") or 0.0)
            if agree >= 2:
                r["confidence"] = _clamp_confidence(max(c, 0.66))
                r["notes"] = (str(r.get("notes") or "") + "; 7seg_agree").strip("; ").strip()
            else:
                # Single-shot values are often unstable with reflections/stickers.
                r["confidence"] = _clamp_confidence(min(c, 0.34))
                r["notes"] = (str(r.get("notes") or "") + "; 7seg_low_agree").strip("; ").strip()
        out.extend(seg_rows)
    return out


def _clamp_box(x1: int, y1: int, x2: int, y2: int, w: int, h: int) -> tuple[int, int, int, int]:
    x1 = max(0, min(w - 1, x1))
    y1 = max(0, min(h - 1, y1))
    x2 = max(1, min(w, x2))
    y2 = max(1, min(h, y2))
    if x2 <= x1:
        x2 = min(w, x1 + 1)
    if y2 <= y1:
        y2 = min(h, y1 + 1)
    return x1, y1, x2, y2


def _make_water_dial_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 5)
        circles = cv2.HoughCircles(
            gray,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(30, min(w, h) // 6),
            param1=90,
            param2=30,
            minRadius=max(30, min(w, h) // 12),
            maxRadius=max(300, min(w, h) // 2),
        )
        if circles is None:
            return out
        circles = np.round(circles[0, :]).astype(int)
        x, y, r = max(circles, key=lambda c: c[2])

        # dial crop
        x1, y1, x2, y2 = _clamp_box(x - int(r * 1.05), y - int(r * 1.05), x + int(r * 1.05), y + int(r * 1.05), w, h)
        dial = im[y1:y2, x1:x2]
        if dial.size > 0:
            p = Image.fromarray(cv2.cvtColor(dial, cv2.COLOR_BGR2RGB))
            p = ImageEnhance.Contrast(p).enhance(1.35)
            p = p.filter(ImageFilter.UnsharpMask(radius=1, percent=180, threshold=2))
            out.append(("water_dial", _encode_jpeg(p, quality=92)))

        # several odometer bands to cover perspective and angle
        odometer_boxes = [
            (x - int(r * 0.68), y - int(r * 0.42), x + int(r * 0.72), y - int(r * 0.02)),
            (x - int(r * 0.76), y - int(r * 0.52), x + int(r * 0.78), y - int(r * 0.10)),
            (x - int(r * 0.82), y - int(r * 0.48), x + int(r * 0.85), y + int(r * 0.02)),
            (x - int(r * 0.90), y - int(r * 0.20), x + int(r * 0.92), y + int(r * 0.26)),
            (x - int(r * 0.95), y - int(r * 0.05), x + int(r * 0.96), y + int(r * 0.36)),
        ]
        for idx, (bx1, by1, bx2, by2) in enumerate(odometer_boxes, start=1):
            bx1, by1, bx2, by2 = _clamp_box(bx1, by1, bx2, by2, w, h)
            band = im[by1:by2, bx1:bx2]
            if band.size == 0:
                continue
            p2 = Image.fromarray(cv2.cvtColor(band, cv2.COLOR_BGR2RGB))
            p2 = p2.resize((max(1, p2.width * 3), max(1, p2.height * 3)), Image.Resampling.LANCZOS)
            p2 = ImageEnhance.Contrast(p2).enhance(1.75)
            p2 = p2.filter(ImageFilter.UnsharpMask(radius=1, percent=240, threshold=2))
            out.append((f"water_odometer_band_{idx}", _encode_jpeg(p2, quality=95)))

        # right red-decimals zone
        rx1, ry1, rx2, ry2 = _clamp_box(x + int(r * 0.18), y - int(r * 0.30), x + int(r * 0.90), y + int(r * 0.14), w, h)
        rz = im[ry1:ry2, rx1:rx2]
        if rz.size > 0:
            p3 = Image.fromarray(cv2.cvtColor(rz, cv2.COLOR_BGR2RGB))
            p3 = ImageEnhance.Contrast(p3).enhance(1.7)
            p3 = p3.filter(ImageFilter.UnsharpMask(radius=1, percent=230, threshold=2))
            out.append(("water_red_zone", _encode_jpeg(p3, quality=94)))
    except Exception:
        return out
    return out[:8]


def _make_water_circle_row_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Геометрические кропы строки одометра от найденного круга.
    Нужны для темных/частично закрытых фото, где контурный поиск окон не срабатывает.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 5)
        circles = cv2.HoughCircles(
            gray,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(30, min(w, h) // 6),
            param1=90,
            param2=30,
            minRadius=max(30, min(w, h) // 12),
            maxRadius=max(300, min(w, h) // 2),
        )
        if circles is None:
            return out
        circles = np.round(circles[0, :]).astype(int)
        x, y, r = max(circles, key=lambda c: c[2])

        boxes = [
            (x - int(r * 0.78), y - int(r * 0.22), x + int(r * 0.80), y + int(r * 0.20)),
            (x - int(r * 0.72), y - int(r * 0.16), x + int(r * 0.74), y + int(r * 0.24)),
            (x - int(r * 0.84), y - int(r * 0.28), x + int(r * 0.86), y + int(r * 0.16)),
        ]
        for idx, (bx1, by1, bx2, by2) in enumerate(boxes, start=1):
            bx1, by1, bx2, by2 = _clamp_box(bx1, by1, bx2, by2, w, h)
            crop = im[by1:by2, bx1:bx2]
            if crop.size == 0:
                continue
            pil = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            pil = pil.resize((max(1, pil.width * 5), max(1, pil.height * 5)), Image.Resampling.LANCZOS)

            base = ImageEnhance.Contrast(pil).enhance(2.2).filter(
                ImageFilter.UnsharpMask(radius=1, percent=290, threshold=2)
            )
            out.append((f"circle_row_{idx}", _encode_jpeg(base, quality=95)))

            g = cv2.cvtColor(np.array(base), cv2.COLOR_RGB2GRAY)
            clahe = cv2.createCLAHE(clipLimit=2.2, tileGridSize=(8, 8))
            g2 = clahe.apply(g)
            out.append((f"circle_row_clahe_{idx}", _encode_jpeg(Image.fromarray(cv2.cvtColor(g2, cv2.COLOR_GRAY2RGB)), quality=95)))

            th = cv2.adaptiveThreshold(g2, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 8)
            out.append((f"circle_row_bw_{idx}", _encode_jpeg(Image.fromarray(cv2.cvtColor(th, cv2.COLOR_GRAY2RGB)), quality=95)))
    except Exception:
        return out
    return out[:9]


def _make_water_circle_odometer_strips(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Жёсткие геометрические кропы строки барабана относительно найденного круга счётчика.
    Нужны для тёмных кадров, где общий детектор рядов не срабатывает.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 5)
        circles = cv2.HoughCircles(
            gray,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(30, min(w, h) // 6),
            param1=90,
            param2=22,
            minRadius=max(20, min(w, h) // 12),
            maxRadius=max(40, min(w, h) // 2),
        )
        if circles is None:
            return out

        circles = np.round(circles[0, :]).astype(int)
        x, y, r = max(circles, key=lambda c: c[2])

        # Барабан обычно расположен в верхней половине круга.
        boxes = [
            (x - int(r * 0.68), y - int(r * 0.36), x + int(r * 0.90), y + int(r * 0.02)),
            (x - int(r * 0.62), y - int(r * 0.32), x + int(r * 0.86), y + int(r * 0.06)),
            (x - int(r * 0.74), y - int(r * 0.40), x + int(r * 0.92), y - int(r * 0.02)),
        ]
        for idx, (bx1, by1, bx2, by2) in enumerate(boxes, start=1):
            bx1, by1, bx2, by2 = _clamp_box(bx1, by1, bx2, by2, w, h)
            crop = im[by1:by2, bx1:bx2]
            if crop.size == 0:
                continue
            pil = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            pil = pil.resize((max(1, pil.width * 5), max(1, pil.height * 5)), Image.Resampling.LANCZOS)

            p1 = ImageEnhance.Contrast(pil).enhance(2.3)
            p1 = ImageEnhance.Sharpness(p1).enhance(2.0)
            p1 = p1.filter(ImageFilter.UnsharpMask(radius=1, percent=300, threshold=2))
            out.append((f"circle_odo_{idx}", _encode_jpeg(p1, quality=96)))

            g = cv2.cvtColor(np.array(p1), cv2.COLOR_RGB2GRAY)
            clahe = cv2.createCLAHE(clipLimit=2.6, tileGridSize=(8, 8))
            g2 = clahe.apply(g)
            out.append((f"circle_odo_clahe_{idx}", _encode_jpeg(Image.fromarray(cv2.cvtColor(g2, cv2.COLOR_GRAY2RGB)), quality=96)))

            th = cv2.adaptiveThreshold(g2, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 7)
            out.append((f"circle_odo_bw_{idx}", _encode_jpeg(Image.fromarray(cv2.cvtColor(th, cv2.COLOR_GRAY2RGB)), quality=96)))
    except Exception:
        return out
    return out[:9]


def _make_water_meter_face_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Кропы "морды" счётчика вокруг найденного круга.
    Нужны как стабильная основа для последующего детекта строки барабана.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 5)
        circles = cv2.HoughCircles(
            gray,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(30, min(w, h) // 6),
            param1=90,
            param2=22,
            minRadius=max(20, min(w, h) // 12),
            maxRadius=max(40, min(w, h) // 2),
        )
        if circles is None:
            return out
        circles = np.round(circles[0, :]).astype(int)
        x, y, r = max(circles, key=lambda c: c[2])

        boxes = [
            (x - int(r * 1.15), y - int(r * 1.10), x + int(r * 1.15), y + int(r * 1.05)),
            (x - int(r * 1.00), y - int(r * 0.95), x + int(r * 1.05), y + int(r * 0.95)),
        ]
        for idx, (bx1, by1, bx2, by2) in enumerate(boxes, start=1):
            bx1, by1, bx2, by2 = _clamp_box(bx1, by1, bx2, by2, w, h)
            crop = im[by1:by2, bx1:bx2]
            if crop.size == 0:
                continue
            pil = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            pil = pil.resize((max(1, pil.width * 3), max(1, pil.height * 3)), Image.Resampling.LANCZOS)

            p1 = ImageEnhance.Contrast(pil).enhance(1.8)
            p1 = ImageEnhance.Brightness(p1).enhance(1.08)
            p1 = ImageEnhance.Sharpness(p1).enhance(1.6)
            p1 = p1.filter(ImageFilter.UnsharpMask(radius=1, percent=220, threshold=2))
            out.append((f"meter_face_{idx}", _encode_jpeg(p1, quality=94)))

            g = cv2.cvtColor(np.array(p1), cv2.COLOR_RGB2GRAY)
            clahe = cv2.createCLAHE(clipLimit=2.1, tileGridSize=(8, 8))
            g2 = clahe.apply(g)
            out.append((f"meter_face_clahe_{idx}", _encode_jpeg(Image.fromarray(cv2.cvtColor(g2, cv2.COLOR_GRAY2RGB)), quality=94)))
    except Exception:
        return out
    return out[:4]


def _make_water_blackhat_row_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Детектор строки барабана через blackhat/морфологию.
    Нужен для тёмных и шумных кадров, где обычный contour-row детектор не ловит окно.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        clahe = cv2.createCLAHE(clipLimit=2.4, tileGridSize=(8, 8))
        g = clahe.apply(gray)

        kx = max(15, (w // 20) | 1)
        ky = max(3, (h // 120) | 1)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kx, ky))
        blackhat = cv2.morphologyEx(g, cv2.MORPH_BLACKHAT, kernel)

        _, th = cv2.threshold(blackhat, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
        th = cv2.morphologyEx(
            th,
            cv2.MORPH_CLOSE,
            cv2.getStructuringElement(cv2.MORPH_RECT, (max(9, w // 30), max(3, h // 140))),
            iterations=1,
        )
        th = cv2.dilate(
            th,
            cv2.getStructuringElement(cv2.MORPH_RECT, (max(11, w // 28), max(3, h // 150))),
            iterations=1,
        )

        contours, _ = cv2.findContours(th, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cand: list[tuple[float, tuple[int, int, int, int]]] = []
        for c in contours:
            x, y, ww, hh = cv2.boundingRect(c)
            if ww < int(w * 0.20) or ww > int(w * 0.96):
                continue
            if hh < int(h * 0.04) or hh > int(h * 0.28):
                continue
            ar = float(ww) / float(max(1, hh))
            if ar < 2.8:
                continue
            yc = y + hh / 2.0
            # Serial number line is usually in upper face; odometer row is lower-middle.
            if yc < h * 0.34 or yc > h * 0.84:
                continue
            area_score = float(ww * hh) / float(max(1, w * h))
            shape_score = min(4.0, ar / 3.0)
            pos_score = 1.0 - min(1.0, abs((yc / h) - 0.58))
            score = area_score * 12.0 + shape_score + pos_score
            cand.append((score, (x, y, x + ww, y + hh)))

        if not cand:
            return out
        cand.sort(key=lambda t: t[0], reverse=True)
        for idx, (_, (x1, y1, x2, y2)) in enumerate(cand[:3], start=1):
            ww = max(1, x2 - x1)
            hh = max(1, y2 - y1)
            cx1, cy1, cx2, cy2 = _clamp_box(
                x1 - int(ww * 0.08),
                y1 - int(hh * 0.55),
                x2 + int(ww * 0.08),
                y2 + int(hh * 0.45),
                w,
                h,
            )
            crop = im[cy1:cy2, cx1:cx2]
            if crop.size == 0:
                continue
            pil = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            pil = pil.resize((max(1, pil.width * 4), max(1, pil.height * 4)), Image.Resampling.LANCZOS)
            p1 = ImageEnhance.Contrast(pil).enhance(2.2)
            p1 = ImageEnhance.Sharpness(p1).enhance(1.9)
            p1 = p1.filter(ImageFilter.UnsharpMask(radius=1, percent=280, threshold=2))
            out.append((f"blackhat_row_{idx}", _encode_jpeg(p1, quality=95)))

            gg = cv2.cvtColor(np.array(p1), cv2.COLOR_RGB2GRAY)
            gg = cv2.createCLAHE(clipLimit=2.2, tileGridSize=(8, 8)).apply(gg)
            bw = cv2.adaptiveThreshold(gg, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 8)
            out.append((f"blackhat_row_bw_{idx}", _encode_jpeg(Image.fromarray(cv2.cvtColor(bw, cv2.COLOR_GRAY2RGB)), quality=95)))
    except Exception:
        return out
    return out[:6]


def _normalize_digits_string(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    d = "".join(ch for ch in s if ch.isdigit())
    return d or None


def _digit_distance(a: Optional[str], b: Optional[str]) -> int:
    aa = _normalize_digits_string(a) or ""
    bb = _normalize_digits_string(b) or ""
    if not aa or not bb:
        return 999
    if len(aa) != len(bb):
        return 999
    return sum(1 for x, y in zip(aa, bb) if x != y)


def _make_water_black_cells_sheet_from_row(row_bytes: bytes) -> Optional[bytes]:
    """
    Из кропа строки барабана выделяет первые 5 окон (чёрная часть) и собирает коллаж.
    Это снижает ошибки 0/1 на тёмных фото.
    """
    try:
        arr = np.frombuffer(row_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return None
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (3, 3), 0)
        bw = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 31, 7
        )
        bw = cv2.morphologyEx(
            bw,
            cv2.MORPH_CLOSE,
            cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)),
            iterations=1,
        )
        contours, _ = cv2.findContours(bw, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        rects: list[tuple[int, int, int, int]] = []
        for c in contours:
            x, y, ww, hh = cv2.boundingRect(c)
            if ww < max(8, int(w * 0.018)) or ww > int(w * 0.22):
                continue
            if hh < max(14, int(h * 0.28)) or hh > int(h * 0.92):
                continue
            ar = float(ww) / float(max(1, hh))
            if ar < 0.16 or ar > 1.25:
                continue
            rects.append((x, y, ww, hh))
        if len(rects) < 6:
            return None

        # Берём полосу с максимальным числом окон по Y.
        rects.sort(key=lambda r: r[1] + r[3] / 2.0)
        bands: list[list[tuple[int, int, int, int]]] = []
        y_tol = max(8, int(h * 0.12))
        for r in rects:
            yc = r[1] + r[3] / 2.0
            placed = False
            for b in bands:
                yb = np.median([x[1] + x[3] / 2.0 for x in b])
                if abs(yc - yb) <= y_tol:
                    b.append(r)
                    placed = True
                    break
            if not placed:
                bands.append([r])
        band = max(bands, key=lambda b: len(b))
        if len(band) < 6:
            return None
        band = sorted(band, key=lambda r: r[0])

        # Обычно последние 2-3 окна красные; берём первые 5 как чёрные.
        cells = band[:5]
        if len(cells) < 5:
            return None

        tiles: list[Image.Image] = []
        for (x, y, ww, hh) in cells:
            x1, y1, x2, y2 = _clamp_box(
                x - int(ww * 0.20),
                y - int(hh * 0.20),
                x + ww + int(ww * 0.20),
                y + hh + int(hh * 0.20),
                w,
                h,
            )
            cimg = im[y1:y2, x1:x2]
            if cimg.size == 0:
                continue
            p = Image.fromarray(cv2.cvtColor(cimg, cv2.COLOR_BGR2RGB))
            p = p.resize((220, 260), Image.Resampling.LANCZOS)
            p = ImageEnhance.Contrast(p).enhance(2.25)
            p = ImageEnhance.Sharpness(p).enhance(1.7)
            p = p.filter(ImageFilter.UnsharpMask(radius=1, percent=300, threshold=2))
            tiles.append(p)
        if len(tiles) != 5:
            return None

        sheet = Image.new("RGB", (5 * 236 + 20, 300), (245, 245, 245))
        for i, t in enumerate(tiles):
            sheet.paste(t, (10 + i * 236, 20))
        return _encode_jpeg(sheet, quality=95)
    except Exception:
        return None


def _choose_best_odometer_cell_run(rects: list[tuple[int, int, int, int]]) -> list[tuple[int, int, int, int]]:
    if not rects:
        return []
    src = sorted(rects, key=lambda r: r[0])
    if len(src) <= 8:
        return src

    widths = [max(1, r[2]) for r in src]
    med_w = float(np.median(widths))
    dedup: list[tuple[int, int, int, int]] = []
    for r in src:
        xc = r[0] + r[2] / 2.0
        if dedup:
            prev = dedup[-1]
            prev_xc = prev[0] + prev[2] / 2.0
            if abs(xc - prev_xc) < max(4.0, med_w * 0.34):
                if (r[2] * r[3]) > (prev[2] * prev[3]):
                    dedup[-1] = r
                continue
        dedup.append(r)
    src = dedup
    if len(src) <= 8:
        return src

    def _score_run(run: list[tuple[int, int, int, int]]) -> float:
        if len(run) < 7:
            return -1e9
        centers = [r[0] + r[2] / 2.0 for r in run]
        gaps = [centers[i] - centers[i - 1] for i in range(1, len(centers))]
        widths_run = [r[2] for r in run]
        heights_run = [r[3] for r in run]
        mg = float(np.median(gaps)) if gaps else 1.0
        mw = float(np.median(widths_run)) if widths_run else 1.0
        mh = float(np.median(heights_run)) if heights_run else 1.0
        if mg <= 1.0 or mw <= 1.0 or mh <= 1.0:
            return -1e9
        gap_spread = float(np.std(np.array(gaps) / mg)) if gaps else 0.0
        width_spread = float(np.std(np.array(widths_run) / mw))
        if gap_spread > 0.55:
            return -1e9
        if width_spread > 0.60:
            return -1e9
        return (len(run) * 3.0) - (gap_spread * 5.0) - (width_spread * 4.0) + (mh / 40.0)

    best_score = -1e9
    best_run: list[tuple[int, int, int, int]] = src[:8]
    for run_len in (8, 7):
        if len(src) < run_len:
            continue
        for i in range(0, len(src) - run_len + 1):
            run = src[i : i + run_len]
            sc = _score_run(run)
            if sc > best_score:
                best_score = sc
                best_run = run
    return best_run


def _make_water_digit_cells_sheet_from_row(row_bytes: bytes) -> Optional[tuple[bytes, int]]:
    """
    Делит строку барабана на отдельные окна цифр и собирает коллаж:
    верх: B1..B5 (целая часть), низ: R1..R2/3 (дробная часть).
    Возвращает (sheet_bytes, red_len).
    """
    try:
        arr = np.frombuffer(row_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return None
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (3, 3), 0)
        bw = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 31, 7
        )
        bw = cv2.morphologyEx(
            bw,
            cv2.MORPH_CLOSE,
            cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)),
            iterations=1,
        )
        contours, _ = cv2.findContours(bw, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        rects: list[tuple[int, int, int, int]] = []
        for c in contours:
            x, y, ww, hh = cv2.boundingRect(c)
            if ww < max(8, int(w * 0.014)) or ww > int(w * 0.25):
                continue
            if hh < max(12, int(h * 0.20)) or hh > int(h * 0.95):
                continue
            ar = float(ww) / float(max(1, hh))
            if ar < 0.12 or ar > 1.30:
                continue
            rects.append((x, y, ww, hh))

        def _fixed_cells_fallback() -> list[tuple[int, int, int, int]]:
            # Fallback for dark/noisy shots: split central row into 8 equal windows.
            # This keeps B1..B5/R1..R3 structure even when contour detection is unstable.
            x1 = int(w * 0.08)
            x2 = int(w * 0.94)
            y1 = int(h * 0.12)
            y2 = int(h * 0.90)
            span = max(8, x2 - x1)
            cell_w = max(12, int(span / 8))
            out_cells: list[tuple[int, int, int, int]] = []
            for i in range(8):
                cx1 = x1 + i * cell_w
                cx2 = x1 + (i + 1) * cell_w
                if i == 7:
                    cx2 = x2
                cx1 = max(0, min(w - 1, cx1))
                cx2 = max(cx1 + 1, min(w, cx2))
                out_cells.append((cx1, y1, cx2 - cx1, max(1, y2 - y1)))
            return out_cells

        if len(rects) >= 7:
            rects.sort(key=lambda r: r[1] + r[3] / 2.0)
            bands: list[list[tuple[int, int, int, int]]] = []
            y_tol = max(8, int(h * 0.12))
            for r in rects:
                yc = r[1] + r[3] / 2.0
                placed = False
                for band in bands:
                    yb = np.median([x[1] + x[3] / 2.0 for x in band])
                    if abs(yc - yb) <= y_tol:
                        band.append(r)
                        placed = True
                        break
                if not placed:
                    bands.append([r])
            band = max(
                bands,
                key=lambda b: (len(b), max(x[0] + x[2] for x in b) - min(x[0] for x in b)),
            )
            run = _choose_best_odometer_cell_run(band)
            if len(run) < 7:
                run = _fixed_cells_fallback()
        else:
            run = _fixed_cells_fallback()

        # Стандарт для воды: 5 целых + 2/3 дробных справа.
        red_len = 3 if len(run) >= 8 else 2
        black_len = 5
        if len(run) < (black_len + red_len):
            return None
        cells = run[: black_len + red_len]

        tiles: list[Image.Image] = []
        for (x, y, ww, hh) in cells:
            x1, y1, x2, y2 = _clamp_box(
                x - int(ww * 0.20),
                y - int(hh * 0.18),
                x + ww + int(ww * 0.20),
                y + hh + int(hh * 0.18),
                w,
                h,
            )
            cimg = im[y1:y2, x1:x2]
            if cimg.size == 0:
                return None
            p = Image.fromarray(cv2.cvtColor(cimg, cv2.COLOR_BGR2RGB))
            p = p.resize((180, 220), Image.Resampling.LANCZOS)
            p = ImageEnhance.Contrast(p).enhance(2.1)
            p = ImageEnhance.Sharpness(p).enhance(1.8)
            p = p.filter(ImageFilter.UnsharpMask(radius=1, percent=300, threshold=2))
            tiles.append(p)
        if len(tiles) != (black_len + red_len):
            return None

        tile_w = 180
        tile_h = 220
        gap = 16
        margin = 18
        width = margin * 2 + max(black_len, red_len) * tile_w + max(0, max(black_len, red_len) - 1) * gap
        height = margin * 2 + tile_h * 2 + 54
        sheet = Image.new("RGB", (width, height), (245, 245, 245))
        draw = ImageDraw.Draw(sheet)

        for i in range(black_len):
            x = margin + i * (tile_w + gap)
            y = margin + 24
            sheet.paste(tiles[i], (x, y))
            draw.text((x + 6, y - 20), f"B{i+1}", fill=(20, 20, 20))

        for i in range(red_len):
            x = margin + i * (tile_w + gap)
            y = margin + tile_h + 36
            sheet.paste(tiles[black_len + i], (x, y))
            draw.text((x + 6, y - 20), f"R{i+1}", fill=(130, 25, 25))

        return _encode_jpeg(sheet, quality=95), red_len
    except Exception:
        return None


def _make_black_focus_variants_from_row(row_bytes: bytes) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(row_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        pil = Image.fromarray(cv2.cvtColor(im, cv2.COLOR_BGR2RGB))
        pil = pil.resize((max(1, pil.width * 4), max(1, pil.height * 4)), Image.Resampling.LANCZOS)

        base = ImageEnhance.Contrast(pil).enhance(2.15)
        base = ImageEnhance.Sharpness(base).enhance(1.6)
        base = base.filter(ImageFilter.UnsharpMask(radius=1, percent=300, threshold=2))
        out.append(("row_black_base", _encode_jpeg(base, quality=95)))

        g = cv2.cvtColor(np.array(base), cv2.COLOR_RGB2GRAY)
        g = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(g)
        out.append(("row_black_clahe", _encode_jpeg(Image.fromarray(cv2.cvtColor(g, cv2.COLOR_GRAY2RGB)), quality=95)))

        th = cv2.adaptiveThreshold(
            g, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 8
        )
        out.append(("row_black_bw", _encode_jpeg(Image.fromarray(cv2.cvtColor(th, cv2.COLOR_GRAY2RGB)), quality=95)))
    except Exception:
        return out
    return out[:3]


def _make_red_focus_variants_from_crop(crop_bytes: bytes, *, prefix: str = "row_red") -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(crop_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        if h < 16 or w < 16:
            return out
        boxes = (
            (0.58, 0.14, 0.99, 0.95),
            (0.62, 0.18, 0.99, 0.92),
            (0.54, 0.10, 0.93, 0.96),
        )
        for idx, (rx1, ry1, rx2, ry2) in enumerate(boxes, start=1):
            x1 = int(round(w * rx1))
            y1 = int(round(h * ry1))
            x2 = int(round(w * rx2))
            y2 = int(round(h * ry2))
            x1, y1, x2, y2 = _clamp_box(x1, y1, x2, y2, w, h)
            c = im[y1:y2, x1:x2]
            if c.size == 0:
                continue
            p = Image.fromarray(cv2.cvtColor(c, cv2.COLOR_BGR2RGB))
            p = p.resize((max(1, p.width * 4), max(1, p.height * 4)), Image.Resampling.LANCZOS)
            p1 = ImageEnhance.Contrast(p).enhance(2.15)
            p1 = ImageEnhance.Sharpness(p1).enhance(1.6)
            p1 = p1.filter(ImageFilter.UnsharpMask(radius=1, percent=280, threshold=2))
            out.append((f"{prefix}_{idx}_base", _encode_jpeg(p1, quality=95)))

            g = cv2.cvtColor(np.array(p1), cv2.COLOR_RGB2GRAY)
            g = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(g)
            out.append(
                (
                    f"{prefix}_{idx}_clahe",
                    _encode_jpeg(Image.fromarray(cv2.cvtColor(g, cv2.COLOR_GRAY2RGB)), quality=95),
                )
            )
    except Exception:
        return out
    return out[:6]


def _extract_red_digits_only(resp: dict) -> Optional[str]:
    d = _normalize_digits_string(resp.get("red_digits"))
    if d and len(d) >= 2:
        return d[-3:] if len(d) >= 3 else d

    raw_reading = resp.get("reading")
    if raw_reading is not None:
        s = str(raw_reading).replace(",", ".").strip()
        if "." in s:
            frac = "".join(ch for ch in s.split(".", 1)[1] if ch.isdigit())
            if len(frac) >= 2:
                return frac[:3]

    notes = str(resp.get("notes") or "")
    m = re.search(r"\b(\d{2,3})\b", notes)
    if m:
        return m.group(1)
    return None


def _extract_digits_from_cell_sheet_resp(
    resp: dict,
    *,
    black_len: int = 5,
    red_len: int = 3,
) -> tuple[Optional[str], Optional[str]]:
    cells = resp.get("cells")
    b: Optional[str] = None
    r: Optional[str] = None

    # Prefer explicit per-cell output when model provides it.
    if isinstance(cells, dict):
        b_parts = []
        for i in range(1, black_len + 1):
            d = _normalize_digits_string(cells.get(f"B{i}"))
            if not d:
                b_parts = []
                break
            b_parts.append(d[-1])
        r_parts = []
        for i in range(1, red_len + 1):
            d = _normalize_digits_string(cells.get(f"R{i}"))
            if not d:
                r_parts = []
                break
            r_parts.append(d[-1])
        b = "".join(b_parts) if len(b_parts) == black_len else None
        r = "".join(r_parts) if len(r_parts) == red_len else None

    if not b:
        b_raw = _normalize_digits_string(resp.get("black_digits"))
        if b_raw:
            if len(b_raw) > black_len:
                b = b_raw[:black_len]
            elif len(b_raw) >= 3:
                # Tolerate short black part from model and left-pad to 5.
                b = b_raw.zfill(black_len)
            else:
                b = None
    if r is None:
        r_raw = _normalize_digits_string(resp.get("red_digits"))
        if r_raw:
            r = r_raw[:red_len]
            if len(r) == 1:
                r = None
    if b and (r or OCR_WATER_INTEGER_ONLY):
        return b, r
    return b, r


def _reading_from_digits(black: Optional[str], red: Optional[str]) -> Optional[float]:
    if not black:
        return None
    try:
        if OCR_WATER_INTEGER_ONLY:
            return float(int(black))
        if red:
            rd = _normalize_digits_string(red) or ""
            if not rd:
                return float(int(black))
            # One decimal digit is too unstable on drum OCR; treat as missing fraction.
            if len(rd) < 2:
                return float(int(black))
            if len(rd) < OCR_WATER_DECIMALS:
                rd = rd.ljust(OCR_WATER_DECIMALS, "0")
            else:
                rd = rd[:OCR_WATER_DECIMALS]
            return float(f"{int(black)}.{rd}")
        return float(int(black))
    except Exception:
        return None


def _tesseract_single_digit(tile_bgr: np.ndarray) -> tuple[Optional[str], float]:
    if pytesseract is None or tile_bgr.size == 0:
        return None, 0.0
    best_digit: Optional[str] = None
    best_conf = 0.0
    try:
        gray = cv2.cvtColor(tile_bgr, cv2.COLOR_BGR2GRAY)
    except Exception:
        return None, 0.0
    try:
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
        arr2d = cv2.adaptiveThreshold(
            clahe, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 7
        )
    except Exception:
        arr2d = gray
    try:
        data = pytesseract.image_to_data(
            arr2d,
            output_type=pytesseract.Output.DICT,
            config="--oem 3 --psm 10 -c tessedit_char_whitelist=0123456789",
            timeout=min(0.8, OCR_TESSERACT_TIMEOUT_SEC),
        )
    except Exception:
        return None, 0.0
    txts = data.get("text", []) or []
    confs = data.get("conf", []) or []
    for t, c in zip(txts, confs):
        s = "".join(ch for ch in str(t or "") if ch.isdigit())
        if not s:
            continue
        d = s[-1]
        try:
            cf = float(c)
        except Exception:
            cf = 0.0
        if cf > best_conf:
            best_conf = cf
            best_digit = d
    # Normalize tesseract confidence range (0..100) to (0..1).
    return best_digit, _clamp_confidence(best_conf / 100.0)


def _read_water_cells_sheet_tesseract(sheet_bytes: bytes, *, red_len: int) -> Optional[dict]:
    if pytesseract is None:
        return None
    try:
        arr = np.frombuffer(sheet_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception:
        return None
    if im is None:
        return None
    h, w = im.shape[:2]
    if h < 200 or w < 300:
        return None

    tile_w = 180
    tile_h = 220
    gap = 16
    margin = 18
    y_black = margin + 24
    y_red = margin + tile_h + 36
    inset_x = 16
    inset_y = 16

    digits_b: list[str] = []
    conf_b: list[float] = []
    digits_r: list[str] = []
    conf_r: list[float] = []

    def _cell_box(i: int, y0: int) -> tuple[int, int, int, int]:
        x0 = margin + i * (tile_w + gap)
        x1 = max(0, x0 + inset_x)
        y1 = max(0, y0 + inset_y)
        x2 = min(w, x0 + tile_w - inset_x)
        y2 = min(h, y0 + tile_h - inset_y)
        return x1, y1, max(x1 + 1, x2), max(y1 + 1, y2)

    for i in range(5):
        x1, y1, x2, y2 = _cell_box(i, y_black)
        d, c = _tesseract_single_digit(im[y1:y2, x1:x2])
        if not d:
            return None
        digits_b.append(d)
        conf_b.append(c)

    for i in range(max(0, min(3, int(red_len)))):
        x1, y1, x2, y2 = _cell_box(i, y_red)
        d, c = _tesseract_single_digit(im[y1:y2, x1:x2])
        if not d:
            continue
        digits_r.append(d)
        conf_r.append(c)

    black_digits = "".join(digits_b)
    red_digits = "".join(digits_r) if len(digits_r) >= 2 else None
    reading = _reading_from_digits(black_digits, red_digits)
    if reading is None:
        return None
    mean_b = float(sum(conf_b) / max(1, len(conf_b)))
    mean_r = float(sum(conf_r) / max(1, len(conf_r))) if conf_r else 0.0
    conf = _clamp_confidence((mean_b * 0.82) + (mean_r * 0.18))
    return {
        "type": "unknown",
        "reading": reading,
        "serial": None,
        "confidence": conf,
        "notes": "det_water_cells_tesseract",
        "black_digits": black_digits,
        "red_digits": red_digits,
    }


def _read_water_row_tesseract(row_bytes: bytes) -> Optional[dict]:
    if pytesseract is None:
        return None
    try:
        arr = np.frombuffer(row_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception:
        return None
    if im is None:
        return None
    h, w = im.shape[:2]
    if h < 20 or w < 40:
        return None

    # Focus on central band where odometer windows are.
    y1 = int(round(h * 0.12))
    y2 = int(round(h * 0.92))
    y1 = max(0, min(h - 1, y1))
    y2 = max(y1 + 1, min(h, y2))
    crop = im[y1:y2, :]
    if crop.size == 0:
        return None
    crop = cv2.resize(crop, (max(1, crop.shape[1] * 4), max(1, crop.shape[0] * 4)), interpolation=cv2.INTER_CUBIC)
    g = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    try:
        g = cv2.createCLAHE(clipLimit=2.4, tileGridSize=(8, 8)).apply(g)
    except Exception:
        pass
    variants: list[np.ndarray] = [g]
    try:
        bw = cv2.adaptiveThreshold(
            g, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 8
        )
        variants.append(bw)
        variants.append(cv2.bitwise_not(bw))
    except Exception:
        pass

    best: Optional[dict] = None
    for arr2d in variants[:2]:
        psm = 7
        try:
            data = pytesseract.image_to_data(
                arr2d,
                output_type=pytesseract.Output.DICT,
                config=f"--oem 3 --psm {psm} -c tessedit_char_whitelist=0123456789",
                timeout=min(1.0, OCR_TESSERACT_TIMEOUT_SEC),
            )
        except Exception:
            continue
        txt = "".join(str(t or "") for t in (data.get("text", []) or []))
        digits = "".join(ch for ch in txt if ch.isdigit())
        if len(digits) < 4:
            continue
        confs: list[float] = []
        for c in data.get("conf", []) or []:
            try:
                cf = float(c)
            except Exception:
                continue
            if cf >= 0:
                confs.append(cf)
        mean_conf = float(sum(confs) / max(1, len(confs))) if confs else 0.0
        if mean_conf < 25.0:
            continue
        if len(digits) >= 8:
            black = digits[:5]
            red = digits[5:8]
        elif len(digits) == 7:
            black = digits[:5]
            red = digits[5:7]
        elif len(digits) >= 5:
            black = digits[:5]
            red = None
        else:
            black = digits.zfill(5)
            red = None
        reading = _reading_from_digits(black, red)
        if reading is None:
            continue
        item = {
            "type": "unknown",
            "reading": reading,
            "serial": None,
            "confidence": _clamp_confidence(mean_conf / 100.0),
            "notes": "det_water_row_tesseract",
            "black_digits": black,
            "red_digits": red,
        }
        if best is None or float(item["confidence"]) > float(best["confidence"]):
            best = item
    return best


def _local_water_quick_candidate(
    img_bytes: bytes,
    *,
    row_variants: Optional[list[tuple[str, bytes]]] = None,
) -> Optional[dict]:
    rows = row_variants
    if rows is None:
        rows = make_water_deterministic_row_variants(img_bytes, max_variants=4)
    if not rows:
        return None
    best: Optional[dict] = None
    for lbl, src in rows[:4]:
        packed = _make_water_digit_cells_sheet_from_row(src)
        if not packed:
            packed = make_fixed_cells_sheet_from_row(src, black_len=5, red_len=3)
        if not packed:
            continue
        sheet_bytes, red_len = packed
        det = _read_water_cells_sheet_tesseract(sheet_bytes, red_len=red_len)
        if det is None:
            det = _read_water_row_tesseract(src)
            if det is None:
                continue
        b = _normalize_digits_string(det.get("black_digits"))
        if not b:
            continue
        c = float(det.get("confidence") or 0.0)
        item = {
            "type": "unknown",
            "reading": _normalize_reading(det.get("reading")),
            "serial": None,
            "confidence": _clamp_confidence(c),
            "notes": str(det.get("notes") or "det_water_quick"),
            "variant": f"det_quick_{lbl}",
            "provider": "det-water-quick:tesseract",
            "black_digits": b,
            "red_digits": _normalize_digits_string(det.get("red_digits")),
        }
        if best is None:
            best = item
            continue
        if float(item.get("confidence") or 0.0) > float(best.get("confidence") or 0.0):
            best = item
    return best


def _normalized_red_digits(v: Optional[str], *, min_len: int = 2, max_len: int = 3) -> Optional[str]:
    d = _normalize_digits_string(v)
    if not d:
        return None
    d = d[:max_len]
    if len(d) < min_len:
        return None
    return d


def _is_weak_red_digits(v: Optional[str]) -> bool:
    d = _normalize_digits_string(v)
    if not d:
        return True
    if len(d) < 2:
        return True
    if len(d) == 2 and d.startswith("0"):
        return True
    # Common artifact on shifted crops: "700", "500", ...
    if len(d) >= 3 and d[1:] == "00":
        return True
    if set(d) == {"0"}:
        return True
    return False


def _variant_image_bytes(variant_image_map: dict[str, bytes], variant_label: str) -> Optional[bytes]:
    if not variant_label:
        return None
    vb = variant_image_map.get(variant_label)
    if vb is not None:
        return vb
    # Derived variants like *_ctxtrimN_* should fall back to their base source crop.
    if "_ctxtrim" in variant_label:
        base = variant_label.split("_ctxtrim", 1)[0]
        vb = variant_image_map.get(base)
        if vb is not None:
            return vb
    if variant_label.startswith("cells_row_"):
        base = variant_label[len("cells_row_") :]
        vb = variant_image_map.get(base)
        if vb is not None:
            return vb
    return None


def _digits_overlap_serial(black_digits: Optional[str], serial: Optional[str]) -> bool:
    b = _normalize_digits_string(black_digits)
    s = _normalize_digits_string(serial)
    if not b or not s:
        return False
    b_nz = b.lstrip("0")
    s_nz = s.lstrip("0")
    if not b_nz or not s_nz:
        return False
    return b_nz in s_nz or s_nz.endswith(b_nz)


def _is_strict_water_odometer_candidate(item: dict) -> bool:
    variant = str(item.get("variant") or "")
    provider = str(item.get("provider") or "")
    if not (
        _is_odometer_variant(variant)
        or provider.startswith("openai-odo")
    ):
        return False
    b = _normalize_digits_string(item.get("black_digits"))
    r = _normalized_red_digits(item.get("red_digits"), min_len=2, max_len=3)
    if not b or len(b) < 4:
        return False
    if (not OCR_WATER_INTEGER_ONLY) and (not r):
        # Allow high-confidence integer-only reads as strict candidates.
        # Red wheels are often unreadable on dark photos; decimals can be recovered later.
        conf = _clamp_confidence(item.get("confidence", 0.0))
        if not (len(b) >= 5 and conf >= 0.62):
            return False
    if _digits_overlap_serial(b, item.get("serial")):
        return False
    return True


def _make_water_top_strip_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        strips = [
            (int(w * 0.22), int(h * 0.28), int(w * 0.86), int(h * 0.52)),
            (int(w * 0.18), int(h * 0.24), int(w * 0.90), int(h * 0.50)),
            (int(w * 0.26), int(h * 0.30), int(w * 0.84), int(h * 0.56)),
        ]
        for idx, (x1, y1, x2, y2) in enumerate(strips, start=1):
            x1, y1, x2, y2 = _clamp_box(x1, y1, x2, y2, w, h)
            crop = im[y1:y2, x1:x2]
            if crop.size == 0:
                continue
            rgb = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB)
            pil = Image.fromarray(rgb)
            pil = pil.resize((max(1, pil.width * 4), max(1, pil.height * 4)), Image.Resampling.LANCZOS)
            p1 = ImageEnhance.Contrast(pil).enhance(1.9).filter(
                ImageFilter.UnsharpMask(radius=1, percent=260, threshold=2)
            )
            out.append((f"odo_top_strip_{idx}", _encode_jpeg(p1, quality=95)))
    except Exception:
        return out
    return out[:3]


def _crop_rectified_row_from_rect(
    im: np.ndarray,
    rect: tuple[tuple[float, float], tuple[float, float], float],
    *,
    pad_x: float = 0.12,
    pad_y: float = 0.55,
) -> Optional[np.ndarray]:
    try:
        (cx, cy), (rw, rh), angle = rect
        if rw < 1 or rh < 1:
            return None
        if rw < rh:
            rw, rh = rh, rw
            angle += 90.0
        h, w = im.shape[:2]
        M = cv2.getRotationMatrix2D((float(cx), float(cy)), float(angle), 1.0)
        rotated = cv2.warpAffine(
            im,
            M,
            (w, h),
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_REPLICATE,
        )
        ww = int(max(12, round(rw * (1.0 + pad_x))))
        hh = int(max(12, round(rh * (1.0 + pad_y))))
        x1, y1, x2, y2 = _clamp_box(
            int(round(cx - ww / 2.0)),
            int(round(cy - hh / 2.0)),
            int(round(cx + ww / 2.0)),
            int(round(cy + hh / 2.0)),
            w,
            h,
        )
        crop = rotated[y1:y2, x1:x2]
        if crop.size == 0:
            return None
        return crop
    except Exception:
        return None


def _detect_water_odometer_rects(im: np.ndarray) -> list[tuple[float, tuple[tuple[float, float], tuple[float, float], float]]]:
    out: list[tuple[float, tuple[tuple[float, float], tuple[float, float], float]]] = []
    try:
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (3, 3), 0)
        clahe = cv2.createCLAHE(clipLimit=2.4, tileGridSize=(8, 8))
        g = clahe.apply(gray)

        kh = max(3, ((h // 120) | 1))
        kw = max(19, ((w // 12) | 1))
        k_blackhat = cv2.getStructuringElement(cv2.MORPH_RECT, (kw, kh))
        blackhat = cv2.morphologyEx(g, cv2.MORPH_BLACKHAT, k_blackhat)

        gradx = cv2.Sobel(blackhat, cv2.CV_32F, 1, 0, ksize=3)
        gradx = np.absolute(gradx)
        m = float(np.max(gradx)) if gradx.size else 0.0
        if m > 0:
            gradx = (gradx / m) * 255.0
        gradx_u8 = gradx.astype("uint8")

        _, th = cv2.threshold(gradx_u8, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
        th = cv2.morphologyEx(
            th,
            cv2.MORPH_CLOSE,
            cv2.getStructuringElement(cv2.MORPH_RECT, (max(15, w // 20), max(3, h // 140))),
            iterations=2,
        )
        th = cv2.dilate(
            th,
            cv2.getStructuringElement(cv2.MORPH_RECT, (max(9, w // 28), max(3, h // 170))),
            iterations=1,
        )

        contours, _ = cv2.findContours(th, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for c in contours:
            area = float(cv2.contourArea(c))
            if area < float(w * h) * 0.0012:
                continue
            rect = cv2.minAreaRect(c)
            (_cx, _cy), (rw, rh), _angle = rect
            if rw < 1 or rh < 1:
                continue
            major = max(rw, rh)
            minor = min(rw, rh)
            if major < float(w) * 0.18:
                continue
            if minor < float(h) * 0.02 or minor > float(h) * 0.30:
                continue
            ar = float(major) / float(max(1.0, minor))
            if ar < 2.5 or ar > 22.0:
                continue
            y_norm = float(_cy) / float(max(1, h))
            if y_norm < 0.22 or y_norm > 0.90:
                continue
            area_norm = area / float(max(1, w * h))
            pos_score = 1.0 - min(1.0, abs(y_norm - 0.58))
            score = (ar * 0.6) + (area_norm * 28.0) + (pos_score * 2.4)
            out.append((score, rect))
    except Exception:
        return out
    out.sort(key=lambda t: t[0], reverse=True)
    return out[:4]


def _make_water_roi_row_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    ROI-first: ищем прямоугольное окно барабана цифр по морфологии/контурам,
    выпрямляем и только потом подаём в OCR.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        rects = _detect_water_odometer_rects(im)
        for idx, (_score, rect) in enumerate(rects, start=1):
            crop = _crop_rectified_row_from_rect(im, rect, pad_x=0.12, pad_y=0.62)
            if crop is None or crop.size == 0:
                continue
            pil = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            pil = pil.resize((max(1, pil.width * 4), max(1, pil.height * 4)), Image.Resampling.LANCZOS)

            base = ImageEnhance.Contrast(pil).enhance(2.2).filter(
                ImageFilter.UnsharpMask(radius=1, percent=300, threshold=2)
            )
            out.append((f"roi_row_{idx}", _encode_jpeg(base, quality=95)))

            g = cv2.cvtColor(np.array(base), cv2.COLOR_RGB2GRAY)
            g = cv2.createCLAHE(clipLimit=2.2, tileGridSize=(8, 8)).apply(g)
            out.append(
                (
                    f"roi_row_clahe_{idx}",
                    _encode_jpeg(Image.fromarray(cv2.cvtColor(g, cv2.COLOR_GRAY2RGB)), quality=95),
                )
            )

            bw = cv2.adaptiveThreshold(
                g,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                35,
                8,
            )
            out.append(
                (
                    f"roi_row_bw_{idx}",
                    _encode_jpeg(Image.fromarray(cv2.cvtColor(bw, cv2.COLOR_GRAY2RGB)), quality=95),
                )
            )
    except Exception:
        return out
    return out[:12]


def _make_water_odometer_window_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 5)
        circles = cv2.HoughCircles(
            gray,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(30, min(w, h) // 6),
            param1=90,
            param2=30,
            minRadius=max(30, min(w, h) // 12),
            maxRadius=max(300, min(w, h) // 2),
        )
        if circles is None:
            return out
        circles = np.round(circles[0, :]).astype(int)
        x, y, r = max(circles, key=lambda c: c[2])

        # Несколько "окон" под барабан цифр с запасом по перспективе.
        windows = [
            (x - int(r * 0.72), y - int(r * 0.40), x + int(r * 0.78), y + int(r * 0.10)),
            (x - int(r * 0.68), y - int(r * 0.32), x + int(r * 0.72), y + int(r * 0.16)),
            (x - int(r * 0.82), y - int(r * 0.46), x + int(r * 0.88), y + int(r * 0.14)),
        ]
        for idx, (bx1, by1, bx2, by2) in enumerate(windows, start=1):
            bx1, by1, bx2, by2 = _clamp_box(bx1, by1, bx2, by2, w, h)
            crop = im[by1:by2, bx1:bx2]
            if crop.size == 0:
                continue
            rgb = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB)
            pil = Image.fromarray(rgb)
            pil = pil.resize((max(1, pil.width * 3), max(1, pil.height * 3)), Image.Resampling.LANCZOS)

            # Base sharpened
            p1 = ImageEnhance.Contrast(pil).enhance(1.8).filter(
                ImageFilter.UnsharpMask(radius=1, percent=260, threshold=2)
            )
            out.append((f"odo_window_{idx}", _encode_jpeg(p1, quality=95)))

            # CLAHE + sharpen
            g = cv2.cvtColor(np.array(pil), cv2.COLOR_RGB2GRAY)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            g2 = clahe.apply(g)
            rgb2 = cv2.cvtColor(g2, cv2.COLOR_GRAY2RGB)
            p2 = Image.fromarray(rgb2).filter(ImageFilter.UnsharpMask(radius=1, percent=220, threshold=1))
            out.append((f"odo_window_clahe_{idx}", _encode_jpeg(p2, quality=95)))

            # Adaptive threshold
            th = cv2.adaptiveThreshold(
                g2, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 9
            )
            rgb3 = cv2.cvtColor(th, cv2.COLOR_GRAY2RGB)
            p3 = Image.fromarray(rgb3)
            out.append((f"odo_window_bw_{idx}", _encode_jpeg(p3, quality=95)))
    except Exception:
        return out
    return out[:9]


def _make_water_global_strip_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Глобальные кропы барабана (без опоры на круг/контуры).
    Нужны для тёмных кадров и частично обрезанных счетчиков.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        boxes = [
            (0.24, 0.46, 0.90, 0.74),
            (0.18, 0.50, 0.88, 0.82),
            (0.28, 0.40, 0.92, 0.70),
        ]
        for idx, (lx, ty, rx, by) in enumerate(boxes, start=1):
            x1 = int(max(0, min(w - 2, round(w * lx))))
            y1 = int(max(0, min(h - 2, round(h * ty))))
            x2 = int(max(x1 + 1, min(w, round(w * rx))))
            y2 = int(max(y1 + 1, min(h, round(h * by))))
            crop = im[y1:y2, x1:x2]
            if crop.size == 0:
                continue
            pil = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            pil = pil.resize((max(1, pil.width * 4), max(1, pil.height * 4)), Image.Resampling.LANCZOS)
            p1 = ImageEnhance.Contrast(pil).enhance(2.0).filter(
                ImageFilter.UnsharpMask(radius=1, percent=280, threshold=2)
            )
            g = cv2.cvtColor(np.array(p1), cv2.COLOR_RGB2GRAY)
            th = cv2.adaptiveThreshold(g, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 8)
            p2 = Image.fromarray(cv2.cvtColor(th, cv2.COLOR_GRAY2RGB))
            out.append((f"odo_global_{idx}", _encode_jpeg(p1, quality=95)))
            out.append((f"odo_global_bw_{idx}", _encode_jpeg(p2, quality=95)))
    except Exception:
        return out
    return out[:6]


def _make_water_counter_box_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Ищет "барабанные" прямоугольные окна цифр по контурам.
    Это fallback для случаев, когда круг детектится плохо и odo_window_* уезжает.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        # Keep edges of printed digits sharper for odometer window detection.
        gray = cv2.GaussianBlur(gray, (3, 3), 0)
        bw = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 41, 11
        )
        bw = cv2.morphologyEx(
            bw,
            cv2.MORPH_CLOSE,
            cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)),
            iterations=1,
        )
        contours, _ = cv2.findContours(bw, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        boxes = []
        for c in contours:
            x, y, ww, hh = cv2.boundingRect(c)
            if ww < max(8, int(w * 0.010)) or ww > int(w * 0.14):
                continue
            if hh < max(16, int(h * 0.030)) or hh > int(h * 0.24):
                continue
            ar = float(ww) / float(max(1, hh))
            if ar < 0.20 or ar > 1.40:
                continue
            area = ww * hh
            if area < int(w * h * 0.00005) or area > int(w * h * 0.03):
                continue
            boxes.append((x, y, ww, hh))
        if len(boxes) < 4:
            return out

        boxes.sort(key=lambda b: b[1] + b[3] / 2.0)
        bands: list[list[tuple[int, int, int, int]]] = []
        y_tol = max(10, int(h * 0.045))
        for b in boxes:
            yc = b[1] + b[3] / 2.0
            placed = False
            for band in bands:
                yb = np.median([x[1] + x[3] / 2.0 for x in band])
                if abs(yc - yb) <= y_tol:
                    band.append(b)
                    placed = True
                    break
            if not placed:
                bands.append([b])

        scored: list[tuple[float, tuple[int, int, int, int]]] = []
        for band in bands:
            if len(band) < 4:
                continue
            band = sorted(band, key=lambda b: b[0])
            xs = [b[0] for b in band]
            ys = [b[1] for b in band]
            ws = [b[2] for b in band]
            hs = [b[3] for b in band]
            x1 = min(xs)
            y1 = min(ys)
            x2 = max(x + ww for x, y, ww, hh in band)
            y2 = max(y + hh for x, y, ww, hh in band)
            spread = x2 - x1
            med_h = float(np.median(hs))
            if spread < med_h * 3.0:
                continue
            mean_w = float(np.mean(ws))
            mean_h = float(np.mean(hs))
            quality = float(len(band)) + min(3.0, spread / max(1.0, mean_h * 2.8)) + min(2.0, mean_w / max(1.0, mean_h))
            scored.append((quality, (x1, y1, x2, y2)))

        if not scored:
            return out
        scored.sort(key=lambda x: x[0], reverse=True)
        for idx, (_, (x1, y1, x2, y2)) in enumerate(scored[:3], start=1):
            bw0 = max(1, x2 - x1)
            bh0 = max(1, y2 - y1)
            cx1, cy1, cx2, cy2 = _clamp_box(
                x1 - int(bw0 * 0.45),
                y1 - int(bh0 * 0.70),
                x2 + int(bw0 * 0.45),
                y2 + int(bh0 * 0.85),
                w,
                h,
            )
            crop = im[cy1:cy2, cx1:cx2]
            if crop.size == 0:
                continue
            pil = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            pil = pil.resize((max(1, pil.width * 4), max(1, pil.height * 4)), Image.Resampling.LANCZOS)
            p1 = ImageEnhance.Contrast(pil).enhance(1.95).filter(
                ImageFilter.UnsharpMask(radius=1, percent=260, threshold=2)
            )
            g = cv2.cvtColor(np.array(p1), cv2.COLOR_RGB2GRAY)
            th = cv2.adaptiveThreshold(g, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 9)
            p2 = Image.fromarray(cv2.cvtColor(th, cv2.COLOR_GRAY2RGB))
            out.append((f"box_window_{idx}", _encode_jpeg(p1, quality=95)))
            out.append((f"box_window_bw_{idx}", _encode_jpeg(p2, quality=95)))
    except Exception:
        return out
    return out[:6]


def _make_water_counter_row_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Детектор конкретно строки барабана (последовательность прямоугольных окон цифр).
    Возвращает узкие кропы строки для точного OCR black/red.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]
        gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (5, 5), 0)
        bw = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 41, 11
        )
        bw = cv2.morphologyEx(
            bw,
            cv2.MORPH_CLOSE,
            cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)),
            iterations=1,
        )
        contours, _ = cv2.findContours(bw, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        rects: list[tuple[int, int, int, int]] = []
        for c in contours:
            x, y, ww, hh = cv2.boundingRect(c)
            if ww < max(9, int(w * 0.012)) or ww > int(w * 0.16):
                continue
            if hh < max(16, int(h * 0.032)) or hh > int(h * 0.24):
                continue
            ar = float(ww) / float(max(1, hh))
            if ar < 0.18 or ar > 1.45:
                continue
            area = ww * hh
            if area < int(w * h * 0.00005) or area > int(w * h * 0.04):
                continue
            rects.append((x, y, ww, hh))

        if len(rects) < 5:
            return out

        rects.sort(key=lambda r: r[1] + r[3] / 2.0)
        bands: list[list[tuple[int, int, int, int]]] = []
        y_tol = max(10, int(h * 0.045))
        for r in rects:
            yc = r[1] + r[3] / 2.0
            placed = False
            for b in bands:
                yb = np.median([x[1] + x[3] / 2.0 for x in b])
                if abs(yc - yb) <= y_tol:
                    b.append(r)
                    placed = True
                    break
            if not placed:
                bands.append([r])

        scored: list[tuple[float, tuple[int, int, int, int]]] = []
        for b in bands:
            if len(b) < 5:
                continue
            b = sorted(b, key=lambda r: r[0])
            xs = [r[0] for r in b]
            ys = [r[1] for r in b]
            ws = [r[2] for r in b]
            hs = [r[3] for r in b]
            x1 = min(xs)
            y1 = min(ys)
            x2 = max(x + ww for x, y, ww, hh in b)
            y2 = max(y + hh for x, y, ww, hh in b)
            yc = (y1 + y2) / 2.0
            # Prefer lower-middle row where odometer windows live; avoid upper serial line.
            if yc < h * 0.40 or yc > h * 0.86:
                continue
            spread = x2 - x1
            mean_h = float(np.mean(hs))
            if mean_h < h * 0.07:
                # serial line digits are usually thinner/shorter than odometer windows
                continue
            if spread < mean_h * 3.0:
                continue
            quality = float(len(b)) + min(4.0, spread / max(1.0, mean_h * 2.5))
            scored.append((quality, (x1, y1, x2, y2)))

        if not scored:
            return out

        scored.sort(key=lambda x: x[0], reverse=True)
        for idx, (_, (x1, y1, x2, y2)) in enumerate(scored[:3], start=1):
            ww = max(1, x2 - x1)
            hh = max(1, y2 - y1)
            # Main crop: tighter around the odometer row to reduce serial-number bleed from top line.
            cx1, cy1, cx2, cy2 = _clamp_box(
                x1 - int(ww * 0.05),
                y1 - int(hh * 0.24),
                x2 + int(ww * 0.05),
                y2 + int(hh * 0.22),
                w,
                h,
            )
            crop = im[cy1:cy2, cx1:cx2]
            if crop.size == 0:
                continue
            pil = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            pil = pil.resize((max(1, pil.width * 4), max(1, pil.height * 4)), Image.Resampling.LANCZOS)

            # Extra-tight strip: often helps on dark photos with strong local highlights.
            tx1, ty1, tx2, ty2 = _clamp_box(
                x1 - int(ww * 0.02),
                y1 - int(hh * 0.14),
                x2 + int(ww * 0.02),
                y2 + int(hh * 0.16),
                w,
                h,
            )
            tcrop = im[ty1:ty2, tx1:tx2]
            if tcrop.size != 0:
                tpil = Image.fromarray(cv2.cvtColor(tcrop, cv2.COLOR_BGR2RGB))
                tpil = tpil.resize((max(1, tpil.width * 4), max(1, tpil.height * 4)), Image.Resampling.LANCZOS)
                tight = ImageEnhance.Contrast(tpil).enhance(2.25).filter(
                    ImageFilter.UnsharpMask(radius=1, percent=320, threshold=2)
                )
                out.append((f"counter_row_tight_{idx}", _encode_jpeg(tight, quality=95)))
                tg = cv2.cvtColor(np.array(tight), cv2.COLOR_RGB2GRAY)
                clahe_t = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
                tg2 = clahe_t.apply(tg)
                out.append(
                    (
                        f"counter_row_tight_clahe_{idx}",
                        _encode_jpeg(
                            Image.fromarray(cv2.cvtColor(tg2, cv2.COLOR_GRAY2RGB)),
                            quality=95,
                        ),
                    )
                )
                tth = cv2.adaptiveThreshold(
                    tg2,
                    255,
                    cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                    cv2.THRESH_BINARY,
                    31,
                    8,
                )
                out.append(
                    (
                        f"counter_row_tight_bw_{idx}",
                        _encode_jpeg(
                            Image.fromarray(cv2.cvtColor(tth, cv2.COLOR_GRAY2RGB)),
                            quality=95,
                        ),
                    )
                )

            base = ImageEnhance.Contrast(pil).enhance(2.15).filter(
                ImageFilter.UnsharpMask(radius=1, percent=280, threshold=2)
            )
            out.append((f"counter_row_{idx}", _encode_jpeg(base, quality=95)))

            g = cv2.cvtColor(np.array(base), cv2.COLOR_RGB2GRAY)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            g2 = clahe.apply(g)
            out.append((f"counter_row_clahe_{idx}", _encode_jpeg(Image.fromarray(cv2.cvtColor(g2, cv2.COLOR_GRAY2RGB)), quality=95)))

            th = cv2.adaptiveThreshold(g2, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 8)
            out.append((f"counter_row_bw_{idx}", _encode_jpeg(Image.fromarray(cv2.cvtColor(th, cv2.COLOR_GRAY2RGB)), quality=95)))
    except Exception:
        return out
    return out[:12]


def _make_water_odometer_sheet(img_bytes: bytes) -> Optional[bytes]:
    try:
        tiles: list[Image.Image] = []

        # 1) ROI-first candidates from rectified row detector
        roi_variants = _make_water_roi_row_variants(img_bytes)
        for _label, b in roi_variants:
            arr2 = np.frombuffer(b, dtype=np.uint8)
            im2 = cv2.imdecode(arr2, cv2.IMREAD_COLOR)
            if im2 is None:
                continue
            p = Image.fromarray(cv2.cvtColor(im2, cv2.COLOR_BGR2RGB)).resize(
                (540, 220),
                Image.Resampling.LANCZOS,
            )
            tiles.append(p)
            if len(tiles) >= 6:
                break

        # 2) Fallback to existing geometric windows if not enough
        if len(tiles) < 4:
            odo_windows = _make_water_odometer_window_variants(img_bytes)
            for _label, b in odo_windows:
                arr2 = np.frombuffer(b, dtype=np.uint8)
                im2 = cv2.imdecode(arr2, cv2.IMREAD_COLOR)
                if im2 is None:
                    continue
                p = Image.fromarray(cv2.cvtColor(im2, cv2.COLOR_BGR2RGB)).resize(
                    (540, 220),
                    Image.Resampling.LANCZOS,
                )
                p = ImageEnhance.Contrast(p).enhance(1.65).filter(
                    ImageFilter.UnsharpMask(radius=1, percent=240, threshold=2)
                )
                tiles.append(p)
                if len(tiles) >= 6:
                    break

        if len(tiles) < 4:
            return None
        while len(tiles) < 6:
            tiles.append(tiles[-1])

        sheet = Image.new("RGB", (1100, 760), (250, 250, 250))
        positions = [(20, 20), (560, 20), (20, 270), (560, 270), (20, 520), (560, 520)]
        for i, (tx, ty) in enumerate(positions):
            sheet.paste(tiles[i], (tx, ty))
            lab = Image.new("RGB", (80, 28), (20, 20, 20))
            sheet.paste(lab, (tx + 6, ty + 6))
        return _encode_jpeg(sheet, quality=92)
    except Exception:
        return None


def _reading_tol(t: str, value: float) -> float:
    v = abs(float(value))
    if t == "Электро":
        # электро в кВтч: допускаем чуть шире, чтобы сгладить OCR-шум
        return max(0.2, min(2.0, v * 0.001))
    if t in ("ХВС", "ГВС"):
        # вода обычно с 2-3 знаками после запятой
        return 0.03
    return 0.1


def _same_candidate(a: dict, b: dict) -> bool:
    if str(a.get("type")) != str(b.get("type")):
        return False
    ra = a.get("reading")
    rb = b.get("reading")
    if ra is None or rb is None:
        # Не считаем "оба null" подтверждением — иначе unknown/null побеждает по кворуму.
        return False
    try:
        tol = max(_reading_tol(str(a.get("type")), float(ra)), _reading_tol(str(b.get("type")), float(rb)))
        return abs(float(ra) - float(rb)) <= tol
    except Exception:
        return False


def _candidate_score(item: dict, all_items: list[dict]) -> float:
    conf = float(item.get("confidence") or 0.0)
    score = conf
    if item.get("reading") is not None:
        score += 0.18
    if str(item.get("type")) != "unknown":
        score += 0.08
    if item.get("serial"):
        score += 0.04
    provider = str(item.get("provider") or "")
    variant = str(item.get("variant") or "")
    if provider.startswith("openai-water"):
        score += 0.08
    if provider.startswith("openai-odo"):
        score += 0.28
    if provider.endswith(":layout"):
        score -= 0.26
    if variant.startswith("odo_pre_"):
        # Bootstrap read is useful, but often unstable on dark shots.
        # Avoid letting it dominate without corroboration from other variants.
        score -= 0.12
    if variant in (
        "water_odometer_band",
        "water_odometer_band_1",
        "water_odometer_band_2",
        "water_odometer_band_3",
        "water_odometer_band_4",
        "water_odometer_band_5",
        "odo_window_1",
        "odo_window_2",
        "odo_window_3",
        "odo_window_clahe_1",
        "odo_window_clahe_2",
        "odo_window_clahe_3",
        "odo_window_bw_1",
        "odo_window_bw_2",
        "odo_window_bw_3",
    ):
        score += 0.18
    if variant.startswith("box_window_"):
        score += 0.24
    if variant.startswith("counter_row_"):
        score += 0.42
    if variant.startswith("cells_row_"):
        score += 0.52
    if variant.startswith("roi_row_"):
        score += 0.52
    if variant.startswith("circle_row_"):
        score += 0.34
    if variant.startswith("odo_global_"):
        score += 0.20
    if "_layout_" in variant:
        score -= 0.08
    if variant == "water_dial":
        # круговой циферблат часто даёт ложные 0.0
        score -= 0.35
    if str(item.get("black_digits") or "").isdigit() and len(str(item.get("black_digits") or "")) >= 3:
        score += 0.10
    bds = str(item.get("black_digits") or "")
    if bds.isdigit():
        # "00xxx" часто появляется при ложном захвате области с серийником.
        if len(bds) >= 5 and bds.startswith("00"):
            score -= 0.20
        # Явно слишком короткая целая часть для водомера в обычных кейсах.
        if len(bds.lstrip("0")) <= 2:
            score -= 0.12
    if str(item.get("red_digits") or "").isdigit() and len(str(item.get("red_digits") or "")) in (2, 3):
        score += 0.08
    if _is_suspicious_water_digits(item):
        # Typical failure for water: model sees only the right tail of odometer
        # and returns values like 000321.9 instead of full counter.
        score -= 0.58
    if bds.isdigit() and len(bds) == 5 and bds.startswith("0"):
        try:
            if int(bds) >= 3000:
                score -= 0.24
        except Exception:
            pass
    if _is_strong_water_digits(item):
        score += 0.16
    if _digits_overlap_serial(item.get("black_digits"), item.get("serial")):
        # Typical failure on water meter: serial tail read as odometer.
        score -= 0.55
    if _looks_like_serial_candidate(item.get("reading"), item.get("serial")):
        score -= 0.45

    support_conf = 0.0
    support_cnt = 0
    for other in all_items:
        if other is item:
            continue
        if _same_candidate(item, other):
            support_cnt += 1
            support_conf += float(other.get("confidence") or 0.0)
    score += 0.28 * support_conf + 0.12 * support_cnt
    return score


def _is_odometer_variant(label: str) -> bool:
    s = str(label or "")
    return (
        s.startswith("water_odometer_band_")
        or s.startswith("odo_window_")
        or s.startswith("odo_sheet_")
        or s.startswith("box_window_")
        or s.startswith("counter_row_")
        or s.startswith("cells_row_")
        or s.startswith("roi_row_")
        or s.startswith("circle_row_")
        or s.startswith("circle_odo_")
        or s.startswith("blackhat_row_")
        or s.startswith("odo_global_")
    )


def _is_suspicious_water_digits(item: dict) -> bool:
    b = _normalize_digits_string(item.get("black_digits"))
    r = _normalize_digits_string(item.get("red_digits"))
    if not b:
        return False
    # Water integer part with 6+ digits and leading zero is almost always a shifted capture.
    # Example failure: "011032" instead of "01003".
    if len(b) >= 6 and b.startswith("0"):
        return True
    sig = len(b.lstrip("0"))
    r_len = len(r) if r else 0
    if len(b) >= 6 and b.startswith("000") and sig <= 3 and r_len < 3:
        return True
    # Keep 00XYZ with 2-3 decimals as plausible: many real meters in this project
    # have 3 significant integer digits around 8xx-9xx.
    if len(b) >= 5 and b.startswith("00") and sig <= 2 and r_len < 3:
        return True
    if len(b) >= 5 and sig <= 2:
        return True
    if len(b) >= 6 and b.startswith("000") and sig <= 4:
        return True
    # Typical dark-shot error: first significant digit shifts right, producing 0X... with too large integer.
    if len(b) == 5 and b.startswith("0"):
        try:
            if int(b) >= 3000:
                return True
        except Exception:
            pass
    # For water odometer, one fractional digit is usually a truncated read.
    if r and len(r) == 1 and sig >= 3:
        return True
    if r and len(r) == 2 and sig <= 2 and b.startswith("00"):
        return True
    return False


def _is_strong_water_digits(item: dict) -> bool:
    b = _normalize_digits_string(item.get("black_digits"))
    r = _normalize_digits_string(item.get("red_digits"))
    if not b:
        return False
    sig = len(b.lstrip("0"))
    if sig < 3:
        return False
    if not OCR_WATER_INTEGER_ONLY:
        # Strict rule for fractional mode: require at least 2 red digits.
        if not r or len(r) < 2:
            return False
    if len(b) < 4:
        return False
    if b.startswith("000") and sig <= 4:
        return False
    return True


def _is_ok_water_digits(item: dict) -> bool:
    b = _normalize_digits_string(item.get("black_digits"))
    if not b:
        return False
    sig = len(b.lstrip("0"))
    if sig < 3:
        return False
    # 3+ significant integer digits are valid for many household meters.
    if len(b) < 3:
        return False
    if b.startswith("000") and sig <= 3:
        return False
    return True


def _water_digit_candidates(candidates: list[dict]) -> list[dict]:
    return [
        c
        for c in candidates
        if (
            (
                str(c.get("type")) in ("ХВС", "ГВС")
                or (
                    str(c.get("type")) == "unknown"
                    and str(c.get("provider") or "").startswith("openai-odo")
                )
            )
            and c.get("reading") is not None
            and (
                _is_odometer_variant(str(c.get("variant") or ""))
                or str(c.get("variant") or "").startswith("odo_full_")
            )
        )
    ]


def _parse_context_prev_water(raw: Optional[str]) -> list[float]:
    s = str(raw or "").strip()
    if not s:
        return []
    vals: list[float] = []
    for part in re.split(r"[,; ]+", s):
        p = str(part or "").strip().replace(",", ".")
        if not p:
            continue
        try:
            v = float(p)
        except Exception:
            continue
        if v > 0:
            vals.append(v)
    # Keep stable order while deduping close duplicates.
    out: list[float] = []
    for v in vals:
        if any(abs(v - x) < 1e-6 for x in out):
            continue
        out.append(v)
    return out


def _parse_context_serial_hints(raw: Optional[str]) -> list[str]:
    s = str(raw or "").strip()
    if not s:
        return []
    out: list[str] = []
    for chunk in re.split(r"[;,]+", s):
        part = str(chunk or "").strip()
        if not part:
            continue
        groups = re.findall(r"\d{4,}", part)
        if len(groups) >= 2:
            cands = groups
        else:
            cand = _normalize_digits_string(part)
            cands = [cand] if cand else []
        for p in cands:
            if len(p) < 4 or p in out:
                continue
            out.append(p)
    return out


def _serial_tail_match_len(a: Optional[str], b: Optional[str]) -> int:
    da = _normalize_digits_string(a)
    db = _normalize_digits_string(b)
    if not da or not db:
        return 0
    n = min(len(da), len(db))
    m = 0
    for i in range(1, n + 1):
        if da[-i] != db[-i]:
            break
        m += 1
    return m


def _best_serial_tail_match(serial: Optional[str], serial_hints: list[str]) -> int:
    if not serial_hints:
        return 0
    return max((_serial_tail_match_len(serial, h) for h in serial_hints), default=0)


def _serial_hint_tails(serial_hints: list[str], max_tails: int = 3) -> list[str]:
    out: list[str] = []
    for s in serial_hints:
        d = _normalize_digits_string(s)
        if not d:
            continue
        tail = d[-5:] if len(d) >= 5 else d
        if tail in out:
            continue
        out.append(tail)
        if len(out) >= max_tails:
            break
    return out


def _nearest_prev_distance(value: Optional[float], prev_values: list[float]) -> float:
    if value is None or not prev_values:
        return float("inf")
    try:
        v = float(value)
    except Exception:
        return float("inf")
    return min(abs(v - float(p)) for p in prev_values)


def _refine_fraction_from_prev(black_digits: Optional[str], prev_values: list[float]) -> Optional[float]:
    b = _normalize_digits_string(black_digits)
    if not b or not prev_values:
        return None
    try:
        base_int = int(b)
    except Exception:
        return None

    nearest_val: Optional[float] = None
    nearest_dist = float("inf")
    for p in prev_values:
        try:
            pv = float(p)
        except Exception:
            continue
        d = abs(pv - float(base_int))
        if d < nearest_dist:
            nearest_dist = d
            nearest_val = pv
    if (nearest_val is None) or (nearest_dist > 1.0):
        return None

    scale = 10 ** OCR_WATER_DECIMALS
    frac = abs(nearest_val - float(int(nearest_val)))
    frac_int = int(round(frac * scale))
    if frac_int >= scale:
        frac_int = scale - 1
    try:
        return float(f"{base_int}.{frac_int:0{OCR_WATER_DECIMALS}d}")
    except Exception:
        return None


def _snap_to_same_integer_context(
    value: Optional[float],
    prev_values: list[float],
    *,
    tolerance: float = 0.25,
) -> Optional[float]:
    v = _normalize_reading(value)
    if (v is None) or (not prev_values):
        return None
    v_int = int(v)
    nearest: Optional[float] = None
    nearest_dist = float("inf")
    for p in prev_values:
        try:
            pv = float(p)
        except Exception:
            continue
        if int(pv) != v_int:
            continue
        d = abs(pv - v)
        if d < nearest_dist:
            nearest_dist = d
            nearest = pv
    if (nearest is None) or (nearest_dist > tolerance):
        return None
    return round(float(nearest), OCR_WATER_DECIMALS)


def _series_support_count(value: float, values: list[float], *, tol: float = 0.08) -> int:
    out = 0
    for v in values:
        try:
            if abs(float(v) - float(value)) <= tol:
                out += 1
        except Exception:
            continue
    return out


def _series_result_score(item: dict, all_items: list[dict], *, prev_values: list[float]) -> float:
    reading = _normalize_reading(item.get("reading"))
    if reading is None:
        return -999.0
    conf = _clamp_confidence(item.get("confidence", 0.0))
    score = conf
    notes = str(item.get("notes") or "")
    item_type = str(item.get("type") or "unknown")
    if item_type != "unknown":
        score += 0.03
    if "context_same_int_snap" in notes:
        score += 0.06
    if "context_frac_refine" in notes:
        score += 0.04
    if "water_context_far_singleton" in notes:
        score -= 0.70
    if "water_no_ok_odometer_winner" in notes:
        score -= 0.50

    peers = [
        _normalize_reading(x.get("reading"))
        for x in all_items
        if x is not item
    ]
    peer_values = [float(v) for v in peers if v is not None]
    support = _series_support_count(float(reading), peer_values, tol=0.08)
    score += min(0.30, 0.12 * float(support))
    if support >= 1:
        score += 0.05

    # Penalize strong mismatch with historical context.
    if prev_values:
        dist = _nearest_prev_distance(float(reading), prev_values)
        if dist > 220.0:
            score -= 0.50
        else:
            score -= min(0.20, dist / 1200.0)
    return float(score)


def _pick_best_series_result(results: list[dict], *, prev_values: list[float]) -> tuple[int, dict]:
    if not results:
        return -1, {"type": "unknown", "reading": None, "serial": None, "confidence": 0.0, "notes": "series_empty"}

    best_idx = -1
    best_score = -1e9
    best_conf = -1e9
    for idx, item in enumerate(results):
        score = _series_result_score(item, results, prev_values=prev_values)
        conf = float(item.get("confidence") or 0.0)
        if (score > best_score) or (abs(score - best_score) < 1e-9 and conf > best_conf):
            best_score = score
            best_conf = conf
            best_idx = idx

    if best_idx < 0:
        return 0, dict(results[0])
    return best_idx, dict(results[best_idx])


def _parse_photo_filename_dt(name: Optional[str]) -> Optional[datetime]:
    s = str(name or "").strip()
    if not s:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})[ _](\d{2})[.:](\d{2})[.:](\d{2})", s)
    if not m:
        return None
    try:
        return datetime.strptime(
            f"{m.group(1)} {m.group(2)}:{m.group(3)}:{m.group(4)}",
            "%Y-%m-%d %H:%M:%S",
        )
    except Exception:
        return None


def _recover_series_missing_with_neighbors(results: list[dict]) -> list[dict]:
    if not results:
        return results
    out = [dict(r or {}) for r in results]
    stamps = [_parse_photo_filename_dt(r.get("filename")) for r in out]

    for idx, item in enumerate(out):
        if _normalize_reading(item.get("reading")) is not None:
            continue
        item_serial = _normalize_digits_string(item.get("serial"))
        item_ts = stamps[idx]
        nearest_same_day_dt: Optional[float] = None
        if item_ts is not None:
            for j, donor in enumerate(out):
                if j == idx:
                    continue
                if _normalize_reading(donor.get("reading")) is None:
                    continue
                donor_ts = stamps[j]
                if donor_ts is None or donor_ts.date() != item_ts.date():
                    continue
                dt = abs((item_ts - donor_ts).total_seconds())
                if nearest_same_day_dt is None or dt < nearest_same_day_dt:
                    nearest_same_day_dt = dt

        best_idx = -1
        best_score = -1e9
        best_reading: Optional[float] = None

        for j, donor in enumerate(out):
            if j == idx:
                continue
            donor_reading = _normalize_reading(donor.get("reading"))
            if donor_reading is None:
                continue
            donor_conf = float(donor.get("confidence") or 0.0)
            if donor_conf < 0.45:
                continue
            donor_serial = _normalize_digits_string(donor.get("serial"))
            donor_ts = stamps[j]
            dt = float("inf")

            score = donor_conf

            if item_ts and donor_ts:
                if item_ts.date() != donor_ts.date():
                    continue
                dt = abs((item_ts - donor_ts).total_seconds())
                # If we have a very near donor, ignore far same-day donors.
                if nearest_same_day_dt is not None and nearest_same_day_dt <= 600 and dt > 900:
                    continue
                if nearest_same_day_dt is not None and nearest_same_day_dt <= 180 and dt > 300:
                    continue
                if dt <= 120:
                    score += 0.60
                elif dt <= 600:
                    score += 0.35
                elif dt <= 1800:
                    score += 0.10
                elif dt <= 3600:
                    score += 0.02
                else:
                    score -= 0.35

            serial_tail = _serial_tail_match_len(item_serial, donor_serial)
            if serial_tail >= 4:
                score += 0.35 + min(0.12, 0.02 * float(serial_tail))
            elif item_serial and donor_serial:
                if dt <= 120:
                    score -= 0.05
                elif dt <= 900:
                    score -= 0.18
                else:
                    score -= 0.45

            if item_ts and donor_ts and dt <= 120 and _normalize_digits_string(item.get("serial")) is None:
                score += 0.08

            if score > best_score:
                best_score = score
                best_idx = j
                best_reading = donor_reading

        if (best_idx >= 0) and (best_reading is not None) and (best_score >= 0.70):
            donor = out[best_idx]
            item["reading"] = float(best_reading)
            if not item.get("type") or str(item.get("type")) == "unknown":
                item["type"] = str(donor.get("type") or "unknown")
            if not item.get("serial"):
                item["serial"] = donor.get("serial")
            donor_conf = float(donor.get("confidence") or 0.0)
            item["confidence"] = _clamp_confidence(
                min(0.78, max(float(item.get("confidence") or 0.0), donor_conf * 0.84))
            )
            base_notes = str(item.get("notes") or "").strip()
            donor_file = str(donor.get("filename") or "")
            tail = f"series_neighbor_recover:{donor_file}" if donor_file else "series_neighbor_recover"
            item["notes"] = f"{base_notes}; {tail}".strip("; ").strip()
    return out


def _water_context_candidates(candidates: list[dict]) -> list[dict]:
    out: list[dict] = []
    for c in candidates:
        t = str(c.get("type") or "unknown")
        if t not in ("ХВС", "ГВС", "unknown"):
            continue
        if c.get("reading") is None:
            continue
        out.append(c)
    return out


def _pick_water_candidate_with_context(
    candidates: list[dict],
    *,
    prev_values: list[float],
    serial_hints: list[str],
) -> Optional[dict]:
    pool = _water_context_candidates(candidates)
    if not pool or not prev_values:
        return None

    ranked: list[tuple[float, float, int, dict]] = []
    for c in pool:
        r = _normalize_reading(c.get("reading"))
        if r is None:
            continue
        dist = _nearest_prev_distance(r, prev_values)
        # Too far from any historical context -> likely wrong window/serial read.
        if dist > 1300.0:
            continue
        conf = float(c.get("confidence") or 0.0)
        serial_tail = _best_serial_tail_match(c.get("serial"), serial_hints) if serial_hints else 0
        score = dist - (conf * 45.0)
        if _is_suspicious_water_digits(c):
            if dist <= 1.5:
                score += 25.0
            elif dist <= 8.0:
                score += 90.0
            else:
                score += 260.0
        if _looks_like_serial_candidate(r, c.get("serial")):
            score += 220.0
        if serial_hints:
            if serial_tail >= 4:
                score -= 120.0 + (18.0 * float(serial_tail))
            else:
                cand_serial_norm = _normalize_digits_string(c.get("serial"))
                if cand_serial_norm:
                    score += 130.0
                else:
                    score += 140.0
            if any(_digits_overlap_serial(c.get("black_digits"), sh) for sh in serial_hints):
                if (serial_tail >= 4) and (dist <= 2.0):
                    score += 0.0
                else:
                    score += 220.0
        if not _normalize_digits_string(c.get("black_digits")):
            score += 70.0
        ranked.append((score, dist, serial_tail, c))

    if not ranked:
        return None
    if serial_hints:
        matched = [x for x in ranked if x[2] >= 4]
        if matched:
            best_matched_dist = min(x[1] for x in matched)
            if best_matched_dist <= 25.0:
                ranked = [x for x in ranked if x[2] >= 4]
    ranked.sort(key=lambda x: x[0])
    return ranked[0][3]


def _pick_water_candidate_by_serial(candidates: list[dict], *, serial_hints: list[str]) -> Optional[dict]:
    pool = _water_context_candidates(candidates)
    if not pool or not serial_hints:
        return None

    ranked: list[tuple[float, dict]] = []
    for c in pool:
        r = _normalize_reading(c.get("reading"))
        if r is None:
            continue
        tail = _best_serial_tail_match(c.get("serial"), serial_hints)
        if tail < 4:
            continue
        conf = float(c.get("confidence") or 0.0)
        score = -(180.0 * float(tail)) - (35.0 * conf)
        if _is_suspicious_water_digits(c):
            score += 160.0
        if _looks_like_serial_candidate(r, c.get("serial")):
            score += 220.0
        if any(_digits_overlap_serial(c.get("black_digits"), sh) for sh in serial_hints):
            score += 220.0
        if not _normalize_digits_string(c.get("black_digits")):
            score += 60.0
        ranked.append((score, c))
    if not ranked:
        return None
    ranked.sort(key=lambda x: x[0])
    return ranked[0][1]


def _water_hypothesis_candidates_from_response(
    resp: dict,
    *,
    variant_prefix: str,
    provider: str,
) -> list[dict]:
    if not isinstance(resp, dict):
        return []
    root_type = _sanitize_type(resp.get("type", "unknown"))
    root_serial = resp.get("serial")
    if isinstance(root_serial, str):
        root_serial = root_serial.strip() or None
    root_conf = _clamp_confidence(resp.get("confidence", 0.0))
    raw_hyp = resp.get("hypotheses")
    hyp_list = raw_hyp if isinstance(raw_hyp, list) else []
    if not hyp_list:
        hyp_list = [resp]

    out: list[dict] = []
    for idx, item in enumerate(hyp_list[:OCR_WATER_HYPOTHESIS_MAX_PER_CALL], start=1):
        if not isinstance(item, dict):
            continue
        t = _sanitize_type(item.get("type", root_type))
        serial = item.get("serial", root_serial)
        if isinstance(serial, str):
            serial = serial.strip() or None
        conf = _clamp_confidence(item.get("confidence", root_conf))
        black = _normalize_digits_string(item.get("black_digits"))
        red = _normalized_red_digits(item.get("red_digits"), min_len=2, max_len=3)
        reading = _reading_from_digits(black, red)
        if reading is None:
            reading = _normalize_reading(item.get("reading", None))
        reading, conf, note2 = _plausibility_filter(t, reading, conf)
        if reading is None:
            continue
        notes = str(item.get("notes", "") or item.get("reason", "") or resp.get("notes", "") or "")
        out.append(
            {
                "type": t,
                "reading": reading,
                "serial": serial,
                "confidence": conf,
                "notes": notes,
                "note2": note2,
                "variant": f"{variant_prefix}_h{idx}",
                "provider": provider,
                "black_digits": black,
                "red_digits": red,
            }
        )
    return out


def _pick_best_water_candidate_adaptive(
    candidates: list[dict],
    *,
    prev_values: list[float],
    serial_hints: list[str],
) -> Optional[dict]:
    if not candidates:
        return None
    if prev_values:
        by_prev = _pick_water_candidate_with_context(
            candidates,
            prev_values=prev_values,
            serial_hints=serial_hints,
        )
        if by_prev is not None:
            return by_prev
    if serial_hints:
        by_serial = _pick_water_candidate_by_serial(candidates, serial_hints=serial_hints)
        if by_serial is not None:
            return by_serial
    return max(candidates, key=lambda x: _candidate_score(x, candidates))


def _pick_water_integer_consensus_candidate(
    candidates: list[dict],
    *,
    prev_values: list[float],
) -> Optional[dict]:
    """
    Pick a stable candidate by integer-part consensus across several variants.
    Useful when one serial-target read is context-trimmed but multiple independent
    odometer variants agree on another integer part (e.g., 00999.xxx).
    """
    if not candidates:
        return None
    votes: dict[int, float] = {}
    counts: dict[int, int] = {}
    best_by_int: dict[int, dict] = {}

    for c in candidates:
        r = _normalize_reading(c.get("reading"))
        if r is None:
            continue
        t = str(c.get("type") or "unknown")
        if t not in ("ХВС", "ГВС", "unknown"):
            continue
        if _is_suspicious_water_digits(c):
            continue
        dist = _nearest_prev_distance(r, prev_values) if prev_values else float("inf")
        if prev_values and dist > 40.0:
            continue
        int_part = int(float(r))
        conf = float(c.get("confidence") or 0.0)
        w = conf + (0.20 if _is_ok_water_digits(c) else 0.0)
        votes[int_part] = votes.get(int_part, 0.0) + max(0.10, w)
        counts[int_part] = counts.get(int_part, 0) + 1
        cur_best = best_by_int.get(int_part)
        if (cur_best is None) or (float(cur_best.get("confidence") or 0.0) < conf):
            best_by_int[int_part] = c

    if not votes:
        return None
    ranked = sorted(
        votes.items(),
        key=lambda kv: (kv[1], counts.get(kv[0], 0)),
        reverse=True,
    )
    best_int, _ = ranked[0]
    if counts.get(best_int, 0) < 2:
        return None
    return best_by_int.get(best_int)


def _reading_integer_part(value) -> Optional[int]:
    v = _normalize_reading(value)
    if v is None:
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def _collect_red_votes_for_integer(
    candidates: list[dict],
    *,
    target_int: Optional[int],
) -> tuple[dict[str, float], dict[str, int], dict[str, float]]:
    votes: dict[str, float] = {}
    counts: dict[str, int] = {}
    best_conf: dict[str, float] = {}
    for c in candidates:
        if target_int is not None:
            ci = _reading_integer_part(c.get("reading"))
            if ci is None or ci != target_int:
                continue
        red = _normalized_red_digits(c.get("red_digits"), min_len=2, max_len=3)
        if not red or _is_weak_red_digits(red):
            continue
        conf = _clamp_confidence(c.get("confidence", 0.0))
        if conf < 0.40:
            continue
        w = conf + (0.14 if len(red) >= 3 else 0.03)
        votes[red] = votes.get(red, 0.0) + w
        counts[red] = counts.get(red, 0) + 1
        best_conf[red] = max(best_conf.get(red, 0.0), conf)
    return votes, counts, best_conf


def _pick_red_digits_by_vote(
    votes: dict[str, float],
    counts: dict[str, int],
    best_conf: dict[str, float],
) -> Optional[str]:
    if not votes:
        return None
    ranked = sorted(
        votes.items(),
        key=lambda kv: (kv[1], counts.get(kv[0], 0), best_conf.get(kv[0], 0.0), len(kv[0])),
        reverse=True,
    )
    red, score = ranked[0]
    cnt = counts.get(red, 0)
    conf = best_conf.get(red, 0.0)
    if score < 0.78:
        return None
    if cnt >= 2:
        return red
    if len(red) >= 3 and conf >= 0.78 and score >= 0.80:
        return red
    if conf >= 0.93 and score >= 0.92:
        return red
    return None


def _has_red_disagreement_for_integer(candidates: list[dict], target_int: Optional[int]) -> bool:
    votes, counts, _ = _collect_red_votes_for_integer(candidates, target_int=target_int)
    if not votes:
        return False
    strong = [
        r for r, sc in votes.items()
        if sc >= 0.70 and counts.get(r, 0) >= 1
    ]
    return len(strong) >= 2


def _water_leading_trim_context_fixes(item: dict, *, prev_values: list[float]) -> list[dict]:
    """
    Build corrected candidates for common water OCR failure:
    extra leading digit in black drums (e.g., 4871.52 instead of 871.52).
    Apply only when correction is much closer to historical context.
    """
    if not prev_values:
        return []
    black = _normalize_digits_string(item.get("black_digits"))
    if not black or len(black) < 4:
        return []
    red = _normalized_red_digits(item.get("red_digits"), min_len=2, max_len=3)
    raw_reading = _normalize_reading(item.get("reading"))
    if raw_reading is None:
        raw_reading = _reading_from_digits(black, red)
    raw_dist = _nearest_prev_distance(raw_reading, prev_values)
    if raw_dist <= 350.0:
        return []

    # Candidate digit layouts:
    # 1) as-is,
    # 2) shift one trailing black digit into red (common split error on drum border).
    layouts: list[tuple[str, Optional[str], str]] = [(black, red, "base")]
    if red and len(black) >= 5:
        shifted_black = black[:-1]
        shifted_red_raw = f"{black[-1]}{red}"
        shifted_red = _normalized_red_digits(shifted_red_raw, min_len=2, max_len=3)
        if shifted_black and shifted_red:
            layouts.append((shifted_black, shifted_red, "shiftbr"))

    out: list[dict] = []
    for src_black, src_red, layout_tag in layouts:
        max_trim = min(3, len(src_black) - 3)
        for trim in range(1, max_trim + 1):
            b2 = src_black[trim:]
            if len(b2) < 3:
                continue
            fixed = _reading_from_digits(b2, src_red)
            if fixed is None:
                continue
            fixed_dist = _nearest_prev_distance(fixed, prev_values)
            if fixed_dist > 180.0:
                continue
            if fixed_dist + 220.0 >= raw_dist:
                continue
            conf = _clamp_confidence(item.get("confidence", 0.0))
            notes = str(item.get("notes", "") or "")
            out.append(
                {
                    "type": _sanitize_type(item.get("type", "unknown")),
                    "reading": fixed,
                    "serial": item.get("serial"),
                    "confidence": _clamp_confidence(max(0.55, min(0.88, conf))),
                    "notes": (f"{notes}; context_trim_leading_{trim}".strip("; ").strip()),
                    "note2": "context_trim_leading_digit",
                    "variant": f"{item.get('variant')}_ctxtrim{trim}_{layout_tag}",
                    "provider": str(item.get("provider") or "openai-odo") + ":ctxtrim",
                    "black_digits": b2,
                    "red_digits": src_red,
                }
            )
    return out


def _water_substring_context_fixes(
    item: dict,
    *,
    prev_values: list[float],
    serial_hints: list[str],
) -> list[dict]:
    """
    Recover from overlong/misaligned black drums by testing inner substrings
    against historical context.
    """
    if not prev_values:
        return []
    black = _normalize_digits_string(item.get("black_digits"))
    if not black or len(black) < 5:
        return []
    red = _normalized_red_digits(item.get("red_digits"), min_len=2, max_len=3)
    raw_reading = _normalize_reading(item.get("reading"))
    if raw_reading is None:
        raw_reading = _reading_from_digits(black, red)
    raw_dist = _nearest_prev_distance(raw_reading, prev_values)
    if raw_dist <= 140.0:
        return []

    out: list[dict] = []
    max_len = min(5, len(black))
    for width in range(3, max_len + 1):
        for start in range(0, len(black) - width + 1):
            sub = black[start : start + width]
            if (not sub) or (sub == black):
                continue
            if serial_hints and any(_digits_overlap_serial(sub, sh) for sh in serial_hints):
                continue
            cand_reading = _reading_from_digits(sub, red)
            if cand_reading is None:
                try:
                    cand_reading = float(int(sub))
                except Exception:
                    continue
            cand_dist = _nearest_prev_distance(cand_reading, prev_values)
            if cand_dist > 180.0:
                continue
            if cand_dist + 18.0 >= raw_dist:
                continue
            base_conf = float(item.get("confidence") or 0.0)
            out.append(
                {
                    "type": _sanitize_type(item.get("type", "unknown")),
                    "reading": cand_reading,
                    "serial": item.get("serial"),
                    "confidence": _clamp_confidence(max(0.42, min(0.84, base_conf - 0.04))),
                    "notes": str(item.get("notes", "") or ""),
                    "note2": "context_substring_digits",
                    "variant": f"{item.get('variant')}_ctxsub{start}_{width}",
                    "provider": str(item.get("provider") or "openai-odo") + ":ctxsub",
                    "black_digits": sub,
                    "red_digits": red,
                }
            )
    return out


def _water_suspicious_layout_fixes(item: dict) -> list[dict]:
    """
    Context-free structural fixes for shifted odometer splits:
    - rightmost black digit belongs to red drums;
    - leading-zero drift on overlong black part.
    """
    black = _normalize_digits_string(item.get("black_digits"))
    red_raw = _normalize_digits_string(item.get("red_digits"))
    if not black or len(black) < 5:
        return []
    serial = item.get("serial")
    base_conf = _clamp_confidence(item.get("confidence", 0.0))
    base_notes = str(item.get("notes", "") or "")
    base_variant = str(item.get("variant") or "orig")
    base_provider = str(item.get("provider") or "openai-odo")
    out: list[dict] = []

    suspicious = _is_suspicious_water_digits(item)
    if not suspicious and not (len(black) >= 6 and black.startswith("0")):
        return []

    def _append_fix(tag: str, b2_raw: Optional[str], r2_raw: Optional[str], conf_boost: float = 0.0) -> None:
        b2 = _normalize_digits_string(b2_raw)
        if not b2 or len(b2) < 3:
            return
        r2 = _normalized_red_digits(r2_raw, min_len=2, max_len=3)
        reading = _reading_from_digits(b2, r2)
        if reading is None:
            try:
                reading = float(int(b2))
            except Exception:
                return
        if _digits_overlap_serial(b2, serial):
            return
        if str(item.get("type") or "unknown") not in ("ХВС", "ГВС", "unknown"):
            return
        out.append(
            {
                "type": _sanitize_type(item.get("type", "unknown")),
                "reading": reading,
                "serial": serial,
                "confidence": _clamp_confidence(max(0.52, min(0.92, base_conf - 0.02 + conf_boost))),
                "notes": (f"{base_notes}; layout_fix_{tag}".strip("; ").strip()),
                "note2": "layout_fix_shifted_digits",
                "variant": f"{base_variant}_layout_{tag}",
                "provider": base_provider + ":layout",
                "black_digits": b2,
                "red_digits": r2,
            }
        )

    if red_raw and len(red_raw) >= 2 and len(black) >= 6:
        _append_fix("shiftbr", black[:-1], f"{black[-1]}{red_raw}", conf_boost=0.08)
    if red_raw and len(red_raw) >= 2 and len(black) >= 5 and black.startswith("0"):
        _append_fix("trim1_shiftbr", black[1:-1], f"{black[-1]}{red_raw}", conf_boost=0.10)
    if red_raw and len(red_raw) >= 2 and len(black) == 5 and black.startswith("0"):
        try:
            if int(black) >= 3000 and black[2] == black[3]:
                # Heuristic for dark shots like 04887 -> 00878 (spurious second digit + shifted tail).
                b3 = f"00{black[2]}{black[4]}{black[3]}"
                _append_fix("dup_shift", b3, red_raw, conf_boost=0.06)
        except Exception:
            pass
    return out


@app.post("/recognize")
async def recognize(
    file: UploadFile = File(...),
    trace_id: Optional[str] = Form(None),
    context_prev_water: Optional[str] = Form(None),
    context_serial_hint: Optional[str] = Form(None),
):
    started_at = time.monotonic()
    req_trace_id = (str(trace_id or "").strip() or f"ocr-{uuid.uuid4().hex[:12]}")
    context_prev_values = _parse_context_prev_water(context_prev_water)
    context_serial_hints = _parse_context_serial_hints(context_serial_hint)
    # Quick mode is safe only when we target a single known serial.
    # With 2+ serial hints we need full pass to avoid early lock on wrong meter.
    quick_serial_mode = bool(OCR_WATER_DIGIT_FIRST and len(context_serial_hints) == 1)
    vision_calls = 0
    stage_ms: dict[str, int] = {}

    def _mark_stage(name: str) -> None:
        stage_ms[name] = int((time.monotonic() - started_at) * 1000)

    # Keep a small reserve for late odometer/cells stages on hard photos.
    odo_reserve_sec = 4.0 if OCR_WATER_DIGIT_FIRST else 0.0

    def _time_budget_left(min_remaining_sec: float = 0.0) -> bool:
        budget = max(1.0, OCR_MAX_RUNTIME_SEC - max(0.0, min_remaining_sec))
        return (time.monotonic() - started_at) < budget

    def _vision(
        image_bytes: bytes,
        *,
        mime: str,
        model: str,
        system_prompt: str = SYSTEM_PROMPT,
        user_text: str = "Определи тип счётчика и показание. Верни JSON строго по схеме.",
        detail: str = "high",
        max_call_timeout_sec: Optional[float] = None,
    ) -> dict:
        nonlocal vision_calls
        max_calls = OCR_MAX_OPENAI_CALLS_QUICK if quick_serial_mode else OCR_MAX_OPENAI_CALLS
        if vision_calls >= max_calls:
            raise TimeoutError("ocr_openai_call_budget_exceeded")
        # Hard per-call guard: never start a long provider call near request deadline.
        remaining = OCR_MAX_RUNTIME_SEC - (time.monotonic() - started_at)
        if remaining <= 0.9:
            raise TimeoutError("ocr_runtime_budget_exceeded")
        vision_calls += 1
        call_timeout = max(1.0, min(float(OPENAI_TIMEOUT_SEC), remaining - 0.4))
        if max_call_timeout_sec is not None:
            call_timeout = max(1.0, min(call_timeout, float(max_call_timeout_sec)))
        return _call_openai_vision(
            image_bytes,
            mime=mime,
            model=model,
            system_prompt=system_prompt,
            user_text=user_text,
            detail=detail,
            timeout_sec=call_timeout,
        )

    img = await file.read()
    if not img:
        raise HTTPException(status_code=400, detail="empty_file")
    img = _prepare_input_image_for_ocr(img)

    if not OCR_OPENAI_ENABLED:
        det_best = None
        if OCR_ELECTRIC_DETERMINISTIC:
            try:
                det_rows = _electric_deterministic_candidates(img)
                if det_rows:
                    det_best = max(det_rows, key=lambda x: float(x.get("confidence") or 0.0))
            except Exception:
                det_best = None
        water_best = None
        if OCR_WATER_TEMPLATE_MATCH:
            try:
                w_rows = _water_template_candidates(img)
                if w_rows:
                    water_best = max(w_rows, key=lambda x: float(x.get("confidence") or 0.0))
            except Exception:
                water_best = None
        if det_best is not None and float(det_best.get("confidence") or 0.0) >= 0.5:
            return {
                "type": "Электро",
                "reading": _normalize_reading(det_best.get("reading")),
                "serial": None,
                "confidence": _clamp_confidence(det_best.get("confidence", 0.0)),
                "notes": "openai_disabled; deterministic_fallback",
                "trace_id": req_trace_id,
            }
        if water_best is not None and float(water_best.get("confidence") or 0.0) >= 0.70:
            return {
                "type": str(water_best.get("type") or "unknown"),
                "reading": _normalize_reading(water_best.get("reading")),
                "serial": water_best.get("serial"),
                "confidence": _clamp_confidence(water_best.get("confidence", 0.0)),
                "notes": "openai_disabled; water_template_fallback",
                "trace_id": req_trace_id,
            }
        return {
            "type": "unknown",
            "reading": None,
            "serial": None,
            "confidence": 0.0,
            "notes": "openai_disabled",
            "trace_id": req_trace_id,
        }

    # Budget guard: when provider quota is exhausted, skip expensive OpenAI pipeline.
    if _openai_is_blocked_now():
        det_best = None
        if OCR_ELECTRIC_DETERMINISTIC:
            try:
                det_rows = _electric_deterministic_candidates(img)
                if det_rows:
                    det_best = max(det_rows, key=lambda x: float(x.get("confidence") or 0.0))
            except Exception:
                det_best = None
        water_best = None
        if OCR_WATER_TEMPLATE_MATCH:
            try:
                w_rows = _water_template_candidates(img)
                if w_rows:
                    water_best = max(w_rows, key=lambda x: float(x.get("confidence") or 0.0))
            except Exception:
                water_best = None
        if det_best is not None and float(det_best.get("confidence") or 0.0) >= 0.5:
            return {
                "type": "Электро",
                "reading": _normalize_reading(det_best.get("reading")),
                "serial": None,
                "confidence": _clamp_confidence(det_best.get("confidence", 0.0)),
                "notes": "openai_quota_cooldown; deterministic_fallback",
                "trace_id": req_trace_id,
            }
        if water_best is not None and float(water_best.get("confidence") or 0.0) >= 0.70:
            return {
                "type": str(water_best.get("type") or "unknown"),
                "reading": _normalize_reading(water_best.get("reading")),
                "serial": water_best.get("serial"),
                "confidence": _clamp_confidence(water_best.get("confidence", 0.0)),
                "notes": "openai_quota_cooldown; water_template_fallback",
                "trace_id": req_trace_id,
            }
        return {
            "type": "unknown",
            "reading": None,
            "serial": None,
            "confidence": 0.0,
            "notes": "openai_quota_cooldown",
            "trace_id": req_trace_id,
        }

    mime = _guess_mime(file.filename, file.content_type)
    logger.info(
        "ocr_recognize start trace_id=%s filename=%s content_type=%s mime=%s size_bytes=%s",
        req_trace_id,
        file.filename,
        file.content_type,
        mime,
        len(img),
    )
    variants = _make_variants(img)
    _mark_stage("variants")
    variant_image_map: dict[str, bytes] = {}

    candidates: list[dict] = []
    serial_target_hit = False
    quick_bootstrap_deferred = False
    water_face_hint = _looks_like_water_meter_face(img)
    pre_det_limit = 4 if OCR_WATER_ECO else 12
    pre_det_row_variants: list[tuple[str, bytes]] = []
    if OCR_WATER_DIGIT_FIRST and (not OCR_WATER_ECO) and _time_budget_left(odo_reserve_sec):
        try:
            pre_det_row_variants = make_water_deterministic_row_variants(img, max_variants=pre_det_limit)
        except Exception:
            pre_det_row_variants = []
    water_row_hint = len(pre_det_row_variants) > 0
    if skip_electric_bootstrap := bool(
        OCR_WATER_DIGIT_FIRST and (water_face_hint or (OCR_WATER_ECO and water_row_hint))
    ):
        logger.info(
            "ocr_recognize trace_id=%s skip electric bootstrap water_face_hint=%s water_row_hint=%s",
            req_trace_id,
            water_face_hint,
            water_row_hint,
        )
    if OCR_WATER_TEMPLATE_MATCH and OCR_WATER_ECO:
        try:
            wt_rows = _water_template_candidates(img)
        except Exception:
            wt_rows = []
        if wt_rows:
            wt_best = max(wt_rows, key=lambda x: float(x.get("confidence") or 0.0))
            if float(wt_best.get("confidence") or 0.0) >= 0.76:
                out = {
                    "type": str(wt_best.get("type") or "unknown"),
                    "reading": _normalize_reading(wt_best.get("reading")),
                    "serial": wt_best.get("serial"),
                    "confidence": _clamp_confidence(wt_best.get("confidence", 0.0)),
                    "notes": "water_template_early_match",
                    "trace_id": req_trace_id,
                }
                if OCR_DEBUG:
                    out["debug"] = [
                        {
                            "provider": str(wt_best.get("provider") or "unknown"),
                            "variant": str(wt_best.get("variant") or "water_template"),
                            "type": str(wt_best.get("type") or "unknown"),
                            "reading": wt_best.get("reading"),
                            "confidence": float(wt_best.get("confidence") or 0.0),
                            "black_digits": None,
                            "red_digits": None,
                        }
                    ]
                    out["timings_ms"] = dict(stage_ms)
                    out["openai_calls"] = 0
                return out
    if OCR_WATER_ECO and OCR_WATER_DIGIT_FIRST and water_row_hint and _time_budget_left(2.0):
        quick_local = _local_water_quick_candidate(img, row_variants=pre_det_row_variants)
        if quick_local is not None and _is_ok_water_digits(quick_local):
            out = {
                "type": str(quick_local.get("type") or "unknown"),
                "reading": _normalize_reading(quick_local.get("reading")),
                "serial": None,
                "confidence": _clamp_confidence(float(quick_local.get("confidence") or 0.0)),
                "notes": f"{str(quick_local.get('notes') or '').strip()}; eco_local_quick",
                "trace_id": req_trace_id,
            }
            if OCR_DEBUG:
                out["debug"] = [
                    {
                        "provider": str(quick_local.get("provider") or "unknown"),
                        "variant": str(quick_local.get("variant") or "orig"),
                        "type": str(quick_local.get("type") or "unknown"),
                        "reading": quick_local.get("reading"),
                        "confidence": float(quick_local.get("confidence") or 0.0),
                        "black_digits": quick_local.get("black_digits"),
                        "red_digits": quick_local.get("red_digits"),
                    }
                ]
                out["timings_ms"] = dict(stage_ms)
                out["openai_calls"] = 0
            return out

    # Electric bootstrap:
    # When digit-first water mode is enabled, generic passes are mostly skipped.
    # Probe a few generic variants first and early-return on confident electric reads.
    if OCR_ELECTRIC_BOOTSTRAP and (not skip_electric_bootstrap) and variants and _time_budget_left(odo_reserve_sec):
        electric_variants: list[tuple[str, bytes]] = []
        preferred = ("middle_band", "focused_crop", "center_crop_strong", "orig", "contrast", "lowlight_enhanced")
        seen_ev: set[str] = set()
        by_label = {str(lbl): vb for lbl, vb in variants}
        for p in preferred:
            vb = by_label.get(p)
            if vb is None:
                continue
            electric_variants.append((p, vb))
            seen_ev.add(p)
        for lbl, vb in variants:
            s_lbl = str(lbl)
            if s_lbl in seen_ev:
                continue
            electric_variants.append((s_lbl, vb))
            seen_ev.add(s_lbl)

        electric_candidates: list[dict] = []
        for label, b in electric_variants[:OCR_ELECTRIC_BOOTSTRAP_VARIANTS]:
            if not _time_budget_left(odo_reserve_sec):
                break
            variant_image_map.setdefault(label, b)
            try:
                er = _vision(
                    b,
                    mime=mime,
                    model=OCR_MODEL_PRIMARY,
                    system_prompt=ELECTRIC_LCD_PROMPT,
                    user_text=(
                        "Если это электросчётчик, верни type='Электро' и reading. "
                        "Игнорируй серийный номер, напряжение и служебные цифры."
                    ),
                    max_call_timeout_sec=11.0,
                )
            except Exception:
                continue
            t = _sanitize_type(er.get("type", "unknown"))
            notes = str(er.get("notes", "") or "")
            if t == "unknown":
                t2 = _classify_meter_type_from_text(f"{er.get('type','')} {notes}")
                if t2 == "Электро":
                    t = "Электро"
            reading = _normalize_reading(er.get("reading", None))
            serial = er.get("serial", None)
            if isinstance(serial, str):
                serial = serial.strip() or None
            conf = _clamp_confidence(er.get("confidence", 0.0))
            reading, conf, note2 = _plausibility_filter(t, reading, conf)
            if t != "Электро":
                # OCR type can be unknown on dusty LCD, but numeric value is still useful.
                # Keep only reasonably confident numeric candidates.
                if (reading is not None) and (conf >= 0.56):
                    t = "Электро"
                    notes = (f"{notes}; electric_infer_from_numeric").strip("; ").strip()
                else:
                    continue
            electric_candidates.append(
                {
                    "type": t,
                    "reading": reading,
                    "serial": serial,
                    "confidence": conf,
                    "notes": notes,
                    "note2": note2,
                    "variant": f"electric_{label}",
                    "provider": f"openai-electric:{OCR_MODEL_PRIMARY}",
                }
            )

        # Deterministic electric pass (7-segment CV decoder) for stability on dark LCD shots.
        try:
            det_candidates = _electric_deterministic_candidates(img)
            electric_candidates.extend(det_candidates)
        except Exception:
            pass

        electric_candidates = _expand_electric_scaled_candidates(electric_candidates)
        electric_best, electric_agree = _pick_electric_bootstrap(electric_candidates)
        if electric_best is None and _time_budget_left(odo_reserve_sec):
            disp_variants = _make_electric_display_variants(img)
            for label, b in disp_variants[:4]:
                if not _time_budget_left(odo_reserve_sec):
                    break
                variant_image_map.setdefault(label, b)
                try:
                    er2 = _vision(
                        b,
                        mime="image/jpeg",
                        model=OCR_MODEL_PRIMARY,
                        system_prompt=ELECTRIC_LCD_PROMPT,
                        user_text=(
                            "Это фото электросчётчика. Найди показание на LCD/LED дисплее. "
                            "Игнорируй серийный номер и служебные числа. "
                            "Если число не видно, верни reading=null."
                        ),
                        detail="high",
                        max_call_timeout_sec=11.0,
                    )
                except Exception:
                    continue
                t2 = _sanitize_type(er2.get("type", "unknown"))
                notes2 = str(er2.get("notes", "") or "")
                if t2 == "unknown":
                    guess2 = _classify_meter_type_from_text(f"{er2.get('type','')} {notes2}")
                    if guess2 == "Электро":
                        t2 = "Электро"
                reading2 = _normalize_reading(er2.get("reading", None))
                serial2 = er2.get("serial", None)
                if isinstance(serial2, str):
                    serial2 = serial2.strip() or None
                conf2 = _clamp_confidence(er2.get("confidence", 0.0))
                reading2, conf2, note2b = _plausibility_filter(t2, reading2, conf2)
                if t2 != "Электро":
                    if (reading2 is not None) and (conf2 >= 0.56):
                        t2 = "Электро"
                        notes2 = (f"{notes2}; electric_infer_from_numeric").strip("; ").strip()
                    else:
                        continue
                electric_candidates.append(
                    {
                        "type": t2,
                        "reading": reading2,
                        "serial": serial2,
                        "confidence": conf2,
                        "notes": notes2,
                        "note2": note2b,
                        "variant": f"electric_{label}",
                        "provider": f"openai-electric:{OCR_MODEL_PRIMARY}:display",
                    }
                )
            electric_candidates = _expand_electric_scaled_candidates(electric_candidates)
            electric_best, electric_agree = _pick_electric_bootstrap(electric_candidates)
        if electric_best is None:
            electric_best, electric_agree = _pick_electric_bootstrap_relaxed(electric_candidates)
        if OCR_ELECTRIC_HARD_RECOVERY and _electric_needs_hard_recovery(electric_best, electric_agree) and _time_budget_left(odo_reserve_sec):
            hard_candidates: list[dict] = []
            disp_variants = _make_electric_display_variants(img)
            disp_variants = sorted(disp_variants, key=lambda t: _electric_variant_rank(str(t[0])))
            for label, b in disp_variants[:8]:
                if not _time_budget_left(odo_reserve_sec):
                    break
                if "mid_lcd" in label:
                    continue
                if ("center_clahe" in label or "lcd_tight_clahe" in label):
                    reps = 2
                else:
                    reps = 1
                for _ in range(reps):
                    try:
                        er3 = _vision(
                            b,
                            mime="image/jpeg",
                            model=OCR_MODEL_PRIMARY,
                            system_prompt=ELECTRIC_LCD_PROMPT,
                            user_text=(
                                "Это сложный кадр электросчетчика. "
                                "Найди только число на LCD дисплее. "
                                "Не используй серийник и служебные числа."
                            ),
                            detail="high",
                            max_call_timeout_sec=11.0,
                        )
                    except Exception:
                        continue
                    t3 = _sanitize_type(er3.get("type", "unknown"))
                    notes3 = str(er3.get("notes", "") or "")
                    if t3 == "unknown":
                        guess3 = _classify_meter_type_from_text(f"{er3.get('type','')} {notes3}")
                        if guess3 == "Электро":
                            t3 = "Электро"
                    reading3 = _normalize_reading(er3.get("reading", None))
                    conf3 = _clamp_confidence(er3.get("confidence", 0.0))
                    reading3, conf3, note3b = _plausibility_filter(t3, reading3, conf3)
                    if t3 != "Электро":
                        if (reading3 is not None) and (conf3 >= 0.56):
                            t3 = "Электро"
                        else:
                            continue
                    hard_candidates.append(
                        {
                            "type": t3,
                            "reading": reading3,
                            "serial": None,
                            "confidence": conf3,
                            "notes": notes3,
                            "note2": note3b,
                            "variant": f"electric_hard_{label}",
                            "provider": f"openai-electric:{OCR_MODEL_PRIMARY}:hard",
                        }
                    )
            merged_hard = _expand_electric_scaled_candidates(electric_candidates + hard_candidates)
            hard_best, hard_agree = _pick_electric_hard_consensus(merged_hard)
            if hard_best is not None:
                electric_candidates = merged_hard
                electric_best = hard_best
                electric_agree = max(int(electric_agree), int(hard_agree))
        if electric_best is not None:
            e_read = _normalize_reading(electric_best.get("reading"))
            if e_read is not None and e_read >= 1000.0:
                # Prefer decimal candidate when best integer likely has a lost decimal point (x10 drift).
                target = float(e_read) / 10.0
                dec_pool: list[dict] = []
                for c in electric_candidates:
                    rr = _normalize_reading(c.get("reading"))
                    if rr is None:
                        continue
                    if abs(float(rr) - target) > 2.0:
                        continue
                    if abs(float(rr) - round(float(rr))) <= 1e-6:
                        continue
                    if _clamp_confidence(c.get("confidence", 0.0)) < 0.82:
                        continue
                    dec_pool.append(c)
                if dec_pool:
                    dec_best = max(dec_pool, key=lambda c: (_clamp_confidence(c.get("confidence", 0.0)), _electric_hint_score(c)))
                    electric_best = dec_best

            e_label = str(electric_best.get("variant") or "electric")
            e_conf = _clamp_confidence(
                float(electric_best.get("confidence") or 0.0) + min(0.12, 0.04 * float(electric_agree))
            )
            e_notes = str(electric_best.get("notes") or "").strip()
            e_provider = str(electric_best.get("provider") or "openai-electric")
            e_tail = f"provider={e_provider}; variant={e_label}; agree={electric_agree+1}/{len(electric_candidates)}; electric_bootstrap"
            e_notes = f"{e_notes}; {e_tail}".strip("; ").strip()[:240]
            out = {
                "type": "Электро",
                "reading": _normalize_reading(electric_best.get("reading")),
                "serial": electric_best.get("serial"),
                "confidence": e_conf,
                "notes": e_notes,
                "trace_id": req_trace_id,
            }
            _mark_stage("electric_bootstrap_finalize")
            if OCR_DEBUG:
                ranked_e = sorted(
                    electric_candidates,
                    key=lambda x: _clamp_confidence(x.get("confidence", 0.0)) + _electric_hint_score(x),
                    reverse=True,
                )[:20]
                out["debug"] = [
                    {
                        "provider": str(c.get("provider") or "unknown"),
                        "variant": str(c.get("variant") or "orig"),
                        "type": str(c.get("type") or "unknown"),
                        "reading": c.get("reading"),
                        "confidence": float(c.get("confidence") or 0.0),
                        "black_digits": c.get("black_digits"),
                        "red_digits": c.get("red_digits"),
                    }
                    for c in ranked_e
                ]
                out["timings_ms"] = dict(stage_ms)
                out["openai_calls"] = vision_calls
            logger.info(
                "ocr_recognize done trace_id=%s elapsed_ms=%s type=%s reading=%s confidence=%s variant=%s provider=%s early=%s",
                req_trace_id,
                int((time.monotonic() - started_at) * 1000),
                out.get("type"),
                out.get("reading"),
                out.get("confidence"),
                e_label,
                e_provider,
                True,
            )
            return out

    # Serial-targeted pass for scenes with multiple water meters in one photo.
    # Try to read only the meter whose serial tail matches context hint.
    serial_target_tails = _serial_hint_tails(context_serial_hints, max_tails=3)
    serial_target_enabled = bool(
        OCR_WATER_DIGIT_FIRST
        and serial_target_tails
        and len(context_serial_hints) == 1
    )
    if serial_target_enabled:
        target_sources: list[tuple[str, bytes, str]] = []
        seen_target_labels: set[str] = set()

        def _push_target_source(lbl: str, src_bytes: bytes, src_mime: str) -> None:
            if (not lbl) or (lbl in seen_target_labels):
                return
            seen_target_labels.add(lbl)
            target_sources.append((lbl, src_bytes, src_mime))

        if variants:
            preferred_variant_order = (
                "orig",
                "focused_crop",
                "center_crop_strong",
                "middle_band",
                "lowlight_enhanced",
                "contrast",
            )
            by_label: dict[str, bytes] = {str(lbl): vb for lbl, vb in variants}
            for pv in preferred_variant_order:
                vb = by_label.get(pv)
                if vb is not None:
                    _push_target_source(f"st_{pv}", vb, mime)
            for lbl, vb in variants:
                _push_target_source(f"st_{lbl}", vb, mime)

        # Add deterministic row crops for serial-target pass.
        # In quick mode keep just one source to limit latency.
        try:
            det_limit = 1 if quick_serial_mode else 2
            det_target_sources = make_water_deterministic_row_variants(img, max_variants=det_limit)
            for lbl, vb in det_target_sources:
                _push_target_source(f"st_{lbl}", vb, "image/jpeg")
        except Exception:
            pass

        target_calls = 0
        target_calls_cap = int(OCR_SERIAL_TARGET_MAX_CALLS)
        if len(context_serial_hints) >= 2:
            # Keep budget for non-serial odometer passes on multi-meter scenes.
            target_calls_cap = min(target_calls_cap, 4 if not quick_serial_mode else 2)
        # Probe each serial tail separately when several meters are present.
        # This prevents the model from locking onto one serial and ignoring the other meter.
        tail_attempts = serial_target_tails[:2] if quick_serial_mode else list(serial_target_tails)
        for src_label, src_bytes, src_mime in target_sources:
            if target_calls >= target_calls_cap:
                break
            if not _time_budget_left(odo_reserve_sec):
                break
            for target_tail in (tail_attempts or serial_target_tails):
                if target_calls >= target_calls_cap:
                    break
                if not _time_budget_left(odo_reserve_sec):
                    break
                try:
                    user_text = (
                        "Целевой хвост serial: "
                        + str(target_tail)
                        + ". Считай только соответствующий счетчик. "
                        + "Если этот хвост serial не виден, верни reading=null."
                    )
                    sr = _vision(
                        src_bytes,
                        mime=src_mime,
                        model=OCR_MODEL_ODOMETER,
                        system_prompt=WATER_SERIAL_TARGET_PROMPT,
                        user_text=user_text,
                        detail="high",
                        max_call_timeout_sec=(6.0 if len(context_serial_hints) >= 2 else None),
                    )
                except Exception:
                    continue
                target_calls += 1
                st_t = _sanitize_type(sr.get("type", "unknown"))
                st_serial = sr.get("serial", None)
                if isinstance(st_serial, str):
                    st_serial = st_serial.strip() or None
                st_conf = _clamp_confidence(sr.get("confidence", 0.0))
                st_black = _normalize_digits_string(sr.get("black_digits"))
                st_red = _normalize_digits_string(sr.get("red_digits"))
                st_reading = _reading_from_digits(st_black, st_red)
                if st_reading is None:
                    st_reading = _normalize_reading(sr.get("reading", None))
                st_reading, st_conf, st_note2 = _plausibility_filter(st_t, st_reading, st_conf)
                if st_reading is None:
                    st_conf = min(st_conf, 0.20)
                variant_image_map.setdefault(src_label, src_bytes)
                cand = {
                    "type": st_t,
                    "reading": st_reading,
                    "serial": st_serial,
                    "confidence": st_conf,
                    "notes": str(sr.get("notes", "") or ""),
                    "note2": st_note2,
                    "variant": src_label,
                    "provider": f"openai-odo-serial-target:{OCR_MODEL_ODOMETER}",
                    "black_digits": st_black,
                    "red_digits": st_red,
                }
                candidates.append(cand)
                if (
                    cand.get("reading") is not None
                    and _best_serial_tail_match(cand.get("serial"), context_serial_hints) >= 4
                ):
                    if context_prev_values:
                        st_dist = _nearest_prev_distance(_normalize_reading(cand.get("reading")), context_prev_values)
                        if st_dist <= 40.0:
                            serial_target_hit = True
                    else:
                        serial_target_hit = True
        _mark_stage("serial_target")

    # Digit-first bootstrap for water counters:
    # prefer a deterministic row crop over full-frame to avoid serial/scene overfit.
    if OCR_WATER_DIGIT_FIRST and variants and (not serial_target_hit):
        pre_sources: list[tuple[str, bytes]] = []
        if not quick_serial_mode:
            pre_sources = make_water_deterministic_row_variants(img, max_variants=1)
        if pre_sources:
            pre_label, pre_bytes = pre_sources[0]
            pre_mime = "image/jpeg"
        else:
            pre_label, pre_bytes = variants[0]
            pre_mime = mime
        variant_image_map.setdefault(f"odo_pre_{pre_label}", pre_bytes)
        if _time_budget_left(odo_reserve_sec):
            try:
                pre = _vision(
                    pre_bytes,
                    mime=pre_mime,
                    model=OCR_MODEL_ODOMETER,
                    system_prompt=WATER_ODOMETER_SYSTEM_PROMPT,
                    max_call_timeout_sec=(8.0 if len(context_serial_hints) >= 2 else None),
                )
                pre_t = _sanitize_type(pre.get("type", "unknown"))
                pre_serial = pre.get("serial", None)
                if isinstance(pre_serial, str):
                    pre_serial = pre_serial.strip() or None
                pre_conf = _clamp_confidence(pre.get("confidence", 0.0))
                pre_black = _normalize_digits_string(pre.get("black_digits"))
                pre_red = _normalize_digits_string(pre.get("red_digits"))
                pre_reading = _reading_from_digits(pre_black, pre_red)
                if pre_reading is None:
                    pre_reading = _normalize_reading(pre.get("reading", None))
                pre_reading, pre_conf, pre_note2 = _plausibility_filter(pre_t, pre_reading, pre_conf)
                candidates.append(
                    {
                        "type": pre_t,
                        "reading": pre_reading,
                        "serial": pre_serial,
                        "confidence": pre_conf,
                        "notes": str(pre.get("notes", "") or ""),
                        "note2": pre_note2,
                        "variant": f"odo_pre_{pre_label}",
                        "provider": f"openai-odo:{OCR_MODEL_ODOMETER}",
                        "black_digits": pre_black,
                        "red_digits": pre_red,
                    }
                )
            except Exception:
                pass
    _mark_stage("bootstrap_passes")
    if quick_serial_mode:
        quick_pool: list[dict] = []
        if candidates:
            quick_pool = list(candidates)
            if context_serial_hints:
                filtered_pool: list[dict] = []
                for c in quick_pool:
                    rr = _normalize_reading(c.get("reading"))
                    if rr is None:
                        filtered_pool.append(c)
                        continue
                    if _looks_like_serial_candidate(rr, c.get("serial")):
                        continue
                    bdig = _normalize_digits_string(c.get("black_digits"))
                    overlaps_serial = bool(
                        bdig and any(_digits_overlap_serial(bdig, sh) for sh in context_serial_hints)
                    )
                    if overlaps_serial and context_prev_values:
                        if _nearest_prev_distance(rr, context_prev_values) > 12.0:
                            continue
                    filtered_pool.append(c)
                if filtered_pool:
                    quick_pool = filtered_pool

            extra_fixes: list[dict] = []
            if context_prev_values:
                for c in quick_pool:
                    extra_fixes.extend(_water_leading_trim_context_fixes(c, prev_values=context_prev_values))
                    extra_fixes.extend(
                        _water_substring_context_fixes(
                            c,
                            prev_values=context_prev_values,
                            serial_hints=context_serial_hints,
                        )
                    )
            for c in quick_pool:
                extra_fixes.extend(_water_suspicious_layout_fixes(c))
            quick_pool.extend(extra_fixes)
            dedup_pool: list[dict] = []
            for c in quick_pool:
                if any(_same_candidate(c, e) for e in dedup_pool):
                    continue
                dedup_pool.append(c)
            quick_pool = dedup_pool or quick_pool

            q_best = _pick_best_water_candidate_adaptive(
                quick_pool,
                prev_values=context_prev_values,
                serial_hints=context_serial_hints,
            )
            if q_best is None:
                q_best = max(quick_pool, key=lambda x: _candidate_score(x, quick_pool))
            q_label = str(q_best.get("variant") or "orig")
            q_agree = 0
            for c in quick_pool:
                if c is q_best:
                    continue
                if _same_candidate(q_best, c):
                    q_agree += 1
            q_provider = str(q_best.get("provider") or "")
            q_reading = _normalize_reading(q_best.get("reading"))
            q_out = {
                "type": str(q_best.get("type") or "unknown"),
                "reading": q_reading,
                "serial": q_best.get("serial"),
                "confidence": _clamp_confidence(float(q_best.get("confidence") or 0.0) + min(0.10, 0.04 * q_agree)),
                "notes": (
                    f"{str(q_best.get('notes') or '').strip()}; "
                    f"provider={q_provider or 'openai'}; "
                    f"variant={q_label}; agree={q_agree+1}/{len(quick_pool)}; quick_serial_bootstrap"
                ).strip("; ").strip()[:240],
                "trace_id": req_trace_id,
            }
            if context_prev_values and (q_out.get("reading") is not None):
                q_cur = _normalize_reading(q_out.get("reading"))
                q_black = _normalize_digits_string(q_best.get("black_digits"))
                q_red = _normalize_digits_string(q_best.get("red_digits"))
                if q_cur is not None and q_black:
                    frac_fixed = _refine_fraction_from_prev(q_black, context_prev_values)
                    q_delta = abs(float(frac_fixed) - float(q_cur)) if frac_fixed is not None else 9999.0
                    q_max_delta = 1.2 if _is_weak_red_digits(q_red) else 0.35
                    if frac_fixed is not None and q_delta <= q_max_delta:
                        if _is_weak_red_digits(q_red) or q_delta <= 0.12:
                            q_out["reading"] = float(frac_fixed)
                            q_out["notes"] = (
                                f"{str(q_out.get('notes') or '').strip()}; context_frac_refine={float(frac_fixed):.{OCR_WATER_DECIMALS}f}"
                            ).strip("; ").strip()
                            q_cur = float(frac_fixed)
                if q_cur is not None:
                    snapped = _snap_to_same_integer_context(q_cur, context_prev_values, tolerance=0.25)
                    if snapped is not None and abs(float(snapped) - float(q_cur)) <= 0.25:
                        q_out["reading"] = float(snapped)
                        q_out["notes"] = (
                            f"{str(q_out.get('notes') or '').strip()}; context_same_int_snap={float(snapped):.{OCR_WATER_DECIMALS}f}"
                        ).strip("; ").strip()
            q_reading = _normalize_reading(q_out.get("reading"))
            if OCR_WATER_DIGIT_FIRST and (not _is_ok_water_digits(q_best)):
                q_dist = _nearest_prev_distance(q_reading, context_prev_values)
                keep_by_context = bool((q_reading is not None) and context_prev_values and (q_dist <= 60.0))
                if keep_by_context:
                    q_out["confidence"] = min(float(q_out.get("confidence") or 0.0), 0.62)
                    q_out["notes"] = (
                        f"{str(q_out.get('notes') or '').strip()}; water_context_keep_quick_no_ok(dist={q_dist:.2f})"
                    ).strip("; ").strip()
                else:
                    q_out["type"] = "unknown"
                    q_out["reading"] = None
                    q_out["confidence"] = min(float(q_out.get("confidence") or 0.0), 0.45)
                    q_out["notes"] = (
                        f"{str(q_out.get('notes') or '').strip()}; water_no_ok_odometer_winner"
                    ).strip("; ").strip()
            if context_prev_values and (q_out.get("reading") is not None):
                q_dist = _nearest_prev_distance(_normalize_reading(q_out.get("reading")), context_prev_values)
                if (q_dist > 140.0) and (q_agree == 0):
                    q_out["type"] = "unknown"
                    q_out["reading"] = None
                    q_out["confidence"] = min(float(q_out.get("confidence") or 0.0), 0.45)
                    q_out["notes"] = (
                        f"{str(q_out.get('notes') or '').strip()}; water_context_far_singleton(dist={q_dist:.2f})"
                    ).strip("; ").strip()
            if (
                q_provider.startswith("openai-odo-serial-target")
                and len(context_serial_hints) >= 2
                and (q_out.get("reading") is not None)
                and context_prev_values
            ):
                q_dist = _nearest_prev_distance(_normalize_reading(q_out.get("reading")), context_prev_values)
                if (q_agree == 0) and (q_dist > 90.0):
                    q_out["type"] = "unknown"
                    q_out["reading"] = None
                    q_out["confidence"] = min(float(q_out.get("confidence") or 0.0), 0.45)
                    q_out["notes"] = (
                        f"{str(q_out.get('notes') or '').strip()}; serial_target_multi_hint_unconfirmed(dist={q_dist:.2f})"
                    ).strip("; ").strip()
        else:
            q_out = {
                "type": "unknown",
                "reading": None,
                "serial": None,
                "confidence": 0.0,
                "notes": "quick_serial_bootstrap_empty",
                "trace_id": req_trace_id,
            }
        quick_can_return = bool(q_out.get("reading") is not None)
        if quick_can_return and context_prev_values:
            q_dist = _nearest_prev_distance(_normalize_reading(q_out.get("reading")), context_prev_values)
            if q_dist > 25.0:
                quick_can_return = False
                q_out["notes"] = (
                    f"{str(q_out.get('notes') or '').strip()}; quick_bootstrap_defer_ctx(dist={q_dist:.2f})"
                ).strip("; ").strip()
        _mark_stage("quick_bootstrap_finalize")
        if quick_can_return:
            if OCR_DEBUG:
                ranked = sorted(quick_pool if candidates else [], key=lambda x: _candidate_score(x, quick_pool), reverse=True)[:20]
                q_out["debug"] = [
                    {
                        "provider": str(c.get("provider") or "unknown"),
                        "variant": str(c.get("variant") or "orig"),
                        "type": str(c.get("type") or "unknown"),
                        "reading": c.get("reading"),
                        "confidence": float(c.get("confidence") or 0.0),
                        "black_digits": c.get("black_digits"),
                        "red_digits": c.get("red_digits"),
                    }
                    for c in ranked
                ]
                q_out["timings_ms"] = dict(stage_ms)
                q_out["openai_calls"] = vision_calls
            logger.info(
                "ocr_recognize done trace_id=%s elapsed_ms=%s type=%s reading=%s confidence=%s quick=%s",
                req_trace_id,
                int((time.monotonic() - started_at) * 1000),
                q_out.get("type"),
                q_out.get("reading"),
                q_out.get("confidence"),
                True,
            )
            return q_out

        quick_bootstrap_deferred = True
        if quick_pool:
            merged: list[dict] = []
            for c in candidates + quick_pool:
                if any(_same_candidate(c, e) for e in merged):
                    continue
                merged.append(c)
            candidates = merged
        if OCR_DEBUG:
            ranked = sorted(quick_pool if candidates else [], key=lambda x: _candidate_score(x, quick_pool), reverse=True)[:20]
            q_out["deferred_debug"] = [
                {
                    "provider": str(c.get("provider") or "unknown"),
                    "variant": str(c.get("variant") or "orig"),
                    "type": str(c.get("type") or "unknown"),
                    "reading": c.get("reading"),
                    "confidence": float(c.get("confidence") or 0.0),
                    "black_digits": c.get("black_digits"),
                    "red_digits": c.get("red_digits"),
                }
                for c in ranked
            ]
        logger.info("ocr_recognize quick bootstrap deferred trace_id=%s", req_trace_id)
    # In digit-first mode skip generic bootstrap here to preserve budget for odometer-specific passes.
    initial_variant_limit = 0 if OCR_WATER_DIGIT_FIRST else 2
    for label, b in variants[:initial_variant_limit]:
        variant_image_map.setdefault(label, b)
        if not _time_budget_left(odo_reserve_sec):
            break
        try:
            resp = _vision(b, mime=mime, model=OCR_MODEL_PRIMARY)
        except Exception:
            continue

        t = _sanitize_type(resp.get("type", "unknown"))
        reading = _normalize_reading(resp.get("reading", None))
        serial = resp.get("serial", None)
        if isinstance(serial, str):
            serial = serial.strip() or None
        conf = _clamp_confidence(resp.get("confidence", 0.0))

        # plausibility
        reading, conf, note2 = _plausibility_filter(t, reading, conf)

        candidates.append(
            {
                "type": t,
                "reading": reading,
                "serial": serial,
                "confidence": conf,
                "notes": str(resp.get("notes", "") or ""),
                "note2": note2,
                "variant": label,
                "provider": f"openai:{OCR_MODEL_PRIMARY}",
            }
        )

    # Safety fallback: ensure we have at least one candidate when bootstrap path failed.
    if not candidates:
        fallback_sources: list[tuple[str, bytes, str, str, str]] = []
        if not quick_serial_mode:
            det_fallback = make_water_deterministic_row_variants(img, max_variants=2)
            for lbl, b in det_fallback:
                fallback_sources.append(
                    (
                        f"fb_{lbl}",
                        b,
                        "image/jpeg",
                        WATER_ODOMETER_SYSTEM_PROMPT,
                        f"openai-odo:{OCR_MODEL_ODOMETER}",
                    )
                )
        if variants:
            f_label, f_bytes = variants[0]
            fallback_sources.append(
                (
                    str(f_label),
                    f_bytes,
                    mime,
                    SYSTEM_PROMPT,
                    f"openai:{OCR_MODEL_PRIMARY}",
                )
            )
        for f_label, f_bytes, f_mime, f_prompt, f_provider in fallback_sources[:3]:
            if not _time_budget_left(odo_reserve_sec):
                break
            try:
                variant_image_map.setdefault(f_label, f_bytes)
                resp = _vision(
                    f_bytes,
                    mime=f_mime,
                    model=OCR_MODEL_PRIMARY if f_provider.startswith("openai:") else OCR_MODEL_ODOMETER,
                    system_prompt=f_prompt,
                )
                t = _sanitize_type(resp.get("type", "unknown"))
                serial = resp.get("serial", None)
                if isinstance(serial, str):
                    serial = serial.strip() or None
                conf = _clamp_confidence(resp.get("confidence", 0.0))
                black = _normalize_digits_string(resp.get("black_digits"))
                red = _normalize_digits_string(resp.get("red_digits"))
                if f_provider.startswith("openai-odo:"):
                    reading = _reading_from_digits(black, red)
                else:
                    reading = None
                if reading is None:
                    reading = _normalize_reading(resp.get("reading", None))
                reading, conf, note2 = _plausibility_filter(t, reading, conf)
                candidates.append(
                    {
                        "type": t,
                        "reading": reading,
                        "serial": serial,
                        "confidence": conf,
                        "notes": str(resp.get("notes", "") or ""),
                        "note2": note2,
                        "variant": f_label,
                        "provider": f_provider,
                        "black_digits": black,
                        "red_digits": red,
                    }
                )
                # Stop early on any plausible numeric candidate.
                if reading is not None:
                    break
            except Exception:
                continue
    if not candidates:
        return {
            "type": "unknown",
            "reading": None,
            "serial": None,
            "confidence": 0.0,
            "notes": "openai_empty_response",
            "trace_id": req_trace_id,
        }
    _mark_stage("primary_candidates")

    # propagate best-known serial to candidates that returned none/"unknown"
    global_serial = _pick_best_serial(candidates)
    if global_serial:
        for c in candidates:
            s = c.get("serial")
            if (not s) or str(s).strip().lower() in ("unknown", "null", "none"):
                c["serial"] = global_serial

    # fallback model on top variants when confidence is low
    best_primary = max(candidates, key=lambda x: _candidate_score(x, candidates))
    if (
        OCR_MODEL_FALLBACK
        and OCR_MODEL_FALLBACK != OCR_MODEL_PRIMARY
        and (not OCR_WATER_DIGIT_FIRST)
        and _time_budget_left(odo_reserve_sec)
        and float(best_primary.get("confidence") or 0.0) < OCR_FALLBACK_MIN_CONF
    ):
        for label, b in variants[:1]:
            variant_image_map.setdefault(label, b)
            if not _time_budget_left(odo_reserve_sec):
                break
            try:
                resp = _vision(b, mime=mime, model=OCR_MODEL_FALLBACK)
            except Exception:
                continue
            t = _sanitize_type(resp.get("type", "unknown"))
            reading = _normalize_reading(resp.get("reading", None))
            serial = resp.get("serial", None)
            if isinstance(serial, str):
                serial = serial.strip() or None
            conf = _clamp_confidence(resp.get("confidence", 0.0))
            reading, conf, note2 = _plausibility_filter(t, reading, conf)
            candidates.append(
                {
                    "type": t,
                    "reading": reading,
                    "serial": serial,
                    "confidence": conf,
                    "notes": str(resp.get("notes", "") or ""),
                    "note2": note2,
                    "variant": label,
                    "provider": f"openai:{OCR_MODEL_FALLBACK}",
                }
            )
    _mark_stage("fallback_candidates")

    if quick_serial_mode and (not quick_bootstrap_deferred):
        quick_best = _pick_best_water_candidate_adaptive(
            candidates,
            prev_values=context_prev_values,
            serial_hints=context_serial_hints,
        )
        if quick_best is None:
            quick_best = max(candidates, key=lambda x: _candidate_score(x, candidates))
        quick_label = str(quick_best.get("variant") or "orig")
        quick_agree = 0
        for c in candidates:
            if c is quick_best:
                continue
            if _same_candidate(quick_best, c):
                quick_agree += 1
        quick_conf = _clamp_confidence(float(quick_best.get("confidence") or 0.0) + min(0.12, 0.04 * quick_agree))
        quick_notes = str(quick_best.get("notes") or "").strip()
        quick_provider = str(quick_best.get("provider") or "openai")
        tail = f"provider={quick_provider}; variant={quick_label}; agree={quick_agree+1}/{len(candidates)}; quick_serial_mode"
        quick_notes = f"{quick_notes}; {tail}".strip("; ").strip()[:240]
        out = {
            "type": str(quick_best.get("type") or "unknown"),
            "reading": _normalize_reading(quick_best.get("reading")),
            "serial": quick_best.get("serial") or global_serial,
            "confidence": quick_conf,
            "notes": quick_notes,
            "trace_id": req_trace_id,
        }
        # keep strict guard against non-odometer winners
        quick_provider_l = str(quick_best.get("provider") or "")
        quick_is_water = (
            out["type"] in ("ХВС", "ГВС", "unknown")
            and (
                quick_provider_l.startswith("openai-water")
                or quick_provider_l.startswith("openai-odo")
                or quick_provider_l.startswith("openai:")
                or quick_provider_l.startswith("google_vision")
            )
        )
        if OCR_WATER_DIGIT_FIRST and quick_is_water and (not _is_ok_water_digits(quick_best)):
            out["type"] = "unknown"
            out["reading"] = None
            out["confidence"] = min(float(out.get("confidence") or 0.0), 0.45)
            out["notes"] = f"{str(out.get('notes') or '').strip()}; water_no_ok_odometer_winner".strip("; ").strip()
        _mark_stage("quick_finalize")
        if OCR_DEBUG:
            ranked = sorted(candidates, key=lambda x: _candidate_score(x, candidates), reverse=True)[:20]
            out["debug"] = [
                {
                    "provider": str(c.get("provider") or "unknown"),
                    "variant": str(c.get("variant") or "orig"),
                    "type": str(c.get("type") or "unknown"),
                    "reading": c.get("reading"),
                    "confidence": float(c.get("confidence") or 0.0),
                    "black_digits": c.get("black_digits"),
                    "red_digits": c.get("red_digits"),
                }
                for c in ranked
            ]
            out["timings_ms"] = dict(stage_ms)
        logger.info(
            "ocr_recognize done trace_id=%s elapsed_ms=%s type=%s reading=%s confidence=%s variant=%s provider=%s quick=%s",
            req_trace_id,
            int((time.monotonic() - started_at) * 1000),
            out.get("type"),
            out.get("reading"),
            out.get("confidence"),
            quick_label,
            quick_provider,
            True,
        )
        return out

    # water-specific second stage: always try dial-focused OCR and let ranker decide
    # In digit-first mode skip this generic water prompt stage and spend budget on row/cells passes.
    water_variant_limit = 0 if OCR_WATER_DIGIT_FIRST else 2
    water_variants: list[tuple[str, bytes]] = []
    if water_variant_limit > 0:
        water_variants = _make_water_dial_variants(img)
        if OCR_WATER_DIGIT_FIRST:
            water_variants = sorted(
                water_variants,
                key=lambda x: 0 if str(x[0]).startswith("water_odometer_band_") else 1,
            )
    for label, wb in water_variants[:water_variant_limit]:
        variant_image_map.setdefault(label, wb)
        if not _time_budget_left(odo_reserve_sec):
            break
        try:
            wr = _vision(wb, mime="image/jpeg", model=OCR_MODEL_PRIMARY, system_prompt=WATER_SYSTEM_PROMPT)
        except Exception:
            continue
        t = _sanitize_type(wr.get("type", "unknown"))
        reading = _normalize_reading(wr.get("reading", None))
        serial = wr.get("serial", None)
        if isinstance(serial, str):
            serial = serial.strip() or None
        conf = _clamp_confidence(wr.get("confidence", 0.0))
        reading, conf, note2 = _plausibility_filter(t, reading, conf)
        # tiny bonus for water-special pass
        conf = _clamp_confidence(conf + 0.04)
        candidates.append(
            {
                "type": t,
                "reading": reading,
                "serial": serial,
                "confidence": conf,
                "notes": str(wr.get("notes", "") or ""),
                "note2": note2,
                "variant": label,
                "provider": f"openai-water:{OCR_MODEL_PRIMARY}",
            }
        )
    _mark_stage("water_variants")

    # water-special prompt on generic variants too (helps when circle detection misses)
    for label, b in ([] if OCR_WATER_DIGIT_FIRST else variants[:1]):
        variant_image_map.setdefault(f"water_{label}", b)
        if not _time_budget_left(odo_reserve_sec):
            break
        try:
            wr2 = _vision(b, mime=mime, model=OCR_MODEL_PRIMARY, system_prompt=WATER_SYSTEM_PROMPT)
        except Exception:
            continue
        t = _sanitize_type(wr2.get("type", "unknown"))
        reading = _normalize_reading(wr2.get("reading", None))
        serial = wr2.get("serial", None)
        if isinstance(serial, str):
            serial = serial.strip() or None
        conf = _clamp_confidence(wr2.get("confidence", 0.0))
        reading, conf, note2 = _plausibility_filter(t, reading, conf)
        candidates.append(
            {
                "type": t,
                "reading": reading,
                "serial": serial,
                "confidence": conf,
                "notes": str(wr2.get("notes", "") or ""),
                "note2": note2,
                "variant": f"water_{label}",
                "provider": f"openai-water:{OCR_MODEL_PRIMARY}",
            }
        )

    # dedicated odometer-window pass (digit-first extraction)
    odo_variants: list[tuple[str, bytes]] = []
    if not OCR_WATER_DIGIT_FIRST:
        odo_variants = _make_water_odometer_window_variants(img)
    det_row_variants = pre_det_row_variants or make_water_deterministic_row_variants(img, max_variants=(4 if OCR_WATER_ECO else 12))
    if OCR_WATER_ECO:
        roi_row_variants = []
        global_variants = _make_water_global_strip_variants(img)[:2]
        box_variants = []
        row_variants = []
    else:
        roi_row_variants = _make_water_roi_row_variants(img)
        global_variants = _make_water_global_strip_variants(img)
        box_variants = _make_water_counter_box_variants(img)
        row_variants = _make_water_counter_row_variants(img)
    circle_row_variants: list[tuple[str, bytes]] = []
    circle_odo_variants: list[tuple[str, bytes]] = []
    meter_face_variants: list[tuple[str, bytes]] = []
    blackhat_row_variants = [] if OCR_WATER_ECO else _make_water_blackhat_row_variants(img)
    top_variants = _make_water_top_strip_variants(img)[: (1 if OCR_WATER_ECO else 3)]
    if not OCR_WATER_DIGIT_FIRST:
        circle_row_variants = _make_water_circle_row_variants(img)
        circle_odo_variants = _make_water_circle_odometer_strips(img)
        meter_face_variants = _make_water_meter_face_variants(img)
    face_row_variants: list[tuple[str, bytes]] = []
    for idx, (face_label, fb) in enumerate(meter_face_variants[:2], start=1):
        sub_rows = _make_water_counter_row_variants(fb)
        for sub_label, sb in sub_rows[:6]:
            if sub_label.startswith("counter_row_clahe_"):
                suffix = sub_label.replace("counter_row_clahe_", "")
                new_label = f"counter_row_clahe_face{idx}_{suffix}"
            elif sub_label.startswith("counter_row_bw_"):
                suffix = sub_label.replace("counter_row_bw_", "")
                new_label = f"counter_row_bw_face{idx}_{suffix}"
            elif sub_label.startswith("counter_row_"):
                suffix = sub_label.replace("counter_row_", "")
                new_label = f"counter_row_face{idx}_{suffix}"
            else:
                new_label = f"counter_row_face{idx}"
            face_row_variants.append((new_label, sb))
            variant_image_map.setdefault(new_label, sb)
        # Поддержим также прямые face-кропы в дебаге/ранкере
        variant_image_map.setdefault(face_label, fb)
    # Use a broader but still bounded set of odometer-focused crops.
    # Previous narrow selection often missed the correct digit row on dark/angled shots.
    # Keep this list compact in digit-first mode:
    # we need to preserve time budget for the deterministic cells_sheet stage,
    # otherwise we keep overfitting to one noisy row candidate (e.g. 01103).
    if OCR_WATER_ECO:
        odometer_variants = (
            top_variants[:1]
            + global_variants[:2]
            + det_row_variants[:3]
            + box_variants[:1]
            + face_row_variants[:1]
            + row_variants[:1]
            + roi_row_variants[:1]
            + blackhat_row_variants[:1]
            + circle_odo_variants[:1]
        )
    else:
        odometer_variants = (
            top_variants[:2]
            + global_variants[:3]
            + det_row_variants[:4]
            + box_variants[:2]
            + face_row_variants[:3]
            + row_variants[:2]
            + roi_row_variants[:3]
            + blackhat_row_variants[:2]
            + circle_odo_variants[:1]
            + circle_row_variants[:1]
            + odo_variants[:1]
        )
    fast_water_hit = False
    strong_readings: list[float] = []
    # In digit-first mode still probe several row-level variants:
    # cells-sheet can fail on dark/occluded shots, and then we need backup candidates.
    if OCR_WATER_DIGIT_FIRST:
        limit = 1 if quick_serial_mode else OCR_ODO_MAX_VARIANTS
        if OCR_WATER_ECO:
            limit = 0
        max_odo_openai_variants = min(limit, len(odometer_variants))
    else:
        max_odo_openai_variants = len(odometer_variants)
    for idx, (label, b) in enumerate(odometer_variants, start=1):
        variant_image_map.setdefault(label, b)
        if idx > max_odo_openai_variants:
            break
        if not _time_budget_left():
            break
        try:
            wr3 = _vision(
                b,
                mime="image/jpeg",
                model=OCR_MODEL_PRIMARY,
                system_prompt=WATER_COUNTER_ROW_PROMPT if label.startswith("counter_row_") else WATER_ODOMETER_SYSTEM_PROMPT,
            )
        except Exception:
            continue
        t = _sanitize_type(wr3.get("type", "unknown"))
        serial = wr3.get("serial", None)
        if isinstance(serial, str):
            serial = serial.strip() or None
        conf = _clamp_confidence(wr3.get("confidence", 0.0))
        black_digits = _normalize_digits_string(wr3.get("black_digits"))
        red_digits = _normalize_digits_string(wr3.get("red_digits"))
        reading = _reading_from_digits(black_digits, red_digits)
        if reading is None:
            reading = _normalize_reading(wr3.get("reading", None))
        reading, conf, note2 = _plausibility_filter(t, reading, conf)
        candidates.append(
            {
                "type": t,
                "reading": reading,
                "serial": serial,
                "confidence": conf,
                "notes": str(wr3.get("notes", "") or ""),
                "note2": note2,
                "variant": label,
                "provider": f"openai-odo:{OCR_MODEL_PRIMARY}",
                "black_digits": black_digits,
                "red_digits": red_digits,
            }
        )
        if (
            _is_strong_water_digits(candidates[-1])
            and float(candidates[-1].get("confidence") or 0.0) >= 0.82
            and candidates[-1].get("reading") is not None
        ):
            strong_readings.append(float(candidates[-1].get("reading")))
            # Do not stop on first "strong" read: on dark shots the first one is often wrong.
            # Early-exit only if at least two strong reads agree.
            if len(strong_readings) >= 2:
                strong_readings_sorted = sorted(strong_readings)
                best_pair_gap = min(
                    abs(strong_readings_sorted[i] - strong_readings_sorted[i - 1])
                    for i in range(1, len(strong_readings_sorted))
                )
                if best_pair_gap <= 2.0:
                    fast_water_hit = True
            # Keep bounded runtime even without consensus.
            if idx >= 14 and (not _time_budget_left(3.0)):
                break

    # deterministic cells-sheet pass: read drum windows by positions B1..B5 and R1..R2/3
    if OCR_WATER_DIGIT_FIRST and _time_budget_left(3.0):
        row_sources: list[tuple[str, bytes]] = []
        seen_labels: set[str] = set()

        def _push_sources(srcs: list[tuple[str, bytes]], limit: int) -> None:
            for lbl, src in srcs[:limit]:
                if (not lbl) or (lbl in seen_labels):
                    continue
                row_sources.append((lbl, src))
                seen_labels.add(lbl)

        # Geometry-first sources; do not depend on OCR confidence from previous passes.
        _push_sources(top_variants, 1 if OCR_WATER_ECO else 2)
        _push_sources(global_variants, 2 if OCR_WATER_ECO else 3)
        _push_sources(face_row_variants, 2 if OCR_WATER_ECO else 4)
        _push_sources(det_row_variants, 3 if OCR_WATER_ECO else 4)
        _push_sources(row_variants, 2 if OCR_WATER_ECO else 3)
        _push_sources(roi_row_variants, 1 if OCR_WATER_ECO else 3)
        _push_sources(blackhat_row_variants, 1 if OCR_WATER_ECO else 2)
        _push_sources(circle_odo_variants, 1)
        _push_sources(circle_row_variants, 1)
        _push_sources(box_variants, 1)
        _push_sources(odo_variants, 1)
        if len(row_sources) < 4:
            _push_sources(global_variants, 2)
        if len(row_sources) < 4 and variants:
            generic_fallback = [(f"generic_{lbl}", b) for lbl, b in variants[:3]]
            _push_sources(generic_fallback, 3)

        row_sources = row_sources[: (2 if quick_serial_mode else (min(3, OCR_CELLS_ROW_SOURCES_MAX) if OCR_WATER_ECO else OCR_CELLS_ROW_SOURCES_MAX))]
        cells_valid: list[dict] = []
        local_det_strong_hit = False

        for src_label, src_bytes in row_sources:
            if not _time_budget_left(4.0):
                break
            packed = _make_water_digit_cells_sheet_from_row(src_bytes)
            if not packed:
                packed = make_fixed_cells_sheet_from_row(src_bytes, black_len=5, red_len=3)
            if not packed:
                candidates.append(
                    {
                        "type": "unknown",
                        "reading": None,
                        "serial": None,
                        "confidence": 0.0,
                        "notes": "cells_sheet_pack_failed",
                        "note2": "",
                        "variant": f"cells_row_{src_label}_reject_pack",
                        "provider": f"openai-odo-cells:{OCR_MODEL_ODOMETER}",
                        "black_digits": None,
                        "red_digits": None,
                    }
                )
                continue
            sheet_bytes, red_len = packed
            variant_image_map.setdefault(f"cells_row_{src_label}", sheet_bytes)
            # Local per-cell OCR first: cheap and often enough for clear drum windows.
            local_cells = _read_water_cells_sheet_tesseract(sheet_bytes, red_len=red_len)
            if local_cells is not None:
                lc_type = _sanitize_type(local_cells.get("type", "unknown"))
                lc_reading = _normalize_reading(local_cells.get("reading"))
                lc_conf = _clamp_confidence(local_cells.get("confidence", 0.0))
                lc_black = _normalize_digits_string(local_cells.get("black_digits"))
                lc_red = _normalize_digits_string(local_cells.get("red_digits"))
                if lc_black and lc_reading is not None:
                    lc_reading, lc_conf, lc_note2 = _plausibility_filter(lc_type, lc_reading, lc_conf)
                    if lc_reading is not None:
                        lc_item = {
                            "type": lc_type,
                            "reading": lc_reading,
                            "serial": None,
                            "confidence": lc_conf,
                            "notes": str(local_cells.get("notes", "") or ""),
                            "note2": lc_note2,
                            "variant": f"cells_row_{src_label}_det",
                            "provider": "det-water-cells:tesseract",
                            "black_digits": lc_black,
                            "red_digits": lc_red,
                        }
                        candidates.append(lc_item)
                        if _is_ok_water_digits(lc_item):
                            cells_valid.append(lc_item)
                            # Strong local read: skip paid OpenAI call for this row source.
                            if float(lc_conf) >= 0.76:
                                if OCR_WATER_ECO:
                                    local_det_strong_hit = True
                                    fast_water_hit = True
                                    break
                                continue
            if OCR_WATER_ECO:
                # In eco mode avoid paid per-sheet calls; rely on local cells OCR.
                continue
            try:
                cs = _vision(
                    sheet_bytes,
                    mime="image/jpeg",
                    model=OCR_MODEL_ODOMETER,
                    system_prompt=WATER_CELLS_SHEET_PROMPT,
                    user_text=(
                        "Прочитай цифры по ячейкам. B1..B5 = целая часть, R1..R2/R3 = дробная. "
                        "Верни только JSON."
                    ),
                    detail="high",
                )
            except Exception:
                candidates.append(
                    {
                        "type": "unknown",
                        "reading": None,
                        "serial": None,
                        "confidence": 0.0,
                        "notes": "cells_sheet_openai_error",
                        "note2": "",
                        "variant": f"cells_row_{src_label}_reject_openai",
                        "provider": f"openai-odo-cells:{OCR_MODEL_ODOMETER}",
                        "black_digits": None,
                        "red_digits": None,
                    }
                )
                continue
            black_digits, red_digits = _extract_digits_from_cell_sheet_resp(
                cs,
                black_len=5,
                red_len=red_len,
            )
            conf = _clamp_confidence(cs.get("confidence", 0.0))
            if not black_digits:
                candidates.append(
                    {
                        "type": _sanitize_type(cs.get("type", "unknown")),
                        "reading": None,
                        "serial": cs.get("serial"),
                        "confidence": conf,
                        "notes": "cells_sheet_no_black_digits",
                        "note2": "",
                        "variant": f"cells_row_{src_label}_reject_digits",
                        "provider": f"openai-odo-cells:{OCR_MODEL_ODOMETER}",
                        "black_digits": black_digits,
                        "red_digits": red_digits,
                    }
                )
                continue
            if _digits_overlap_serial(black_digits, cs.get("serial")):
                candidates.append(
                    {
                        "type": _sanitize_type(cs.get("type", "unknown")),
                        "reading": None,
                        "serial": cs.get("serial"),
                        "confidence": conf,
                        "notes": "cells_sheet_black_overlaps_serial",
                        "note2": "",
                        "variant": f"cells_row_{src_label}_reject_serial",
                        "provider": f"openai-odo-cells:{OCR_MODEL_ODOMETER}",
                        "black_digits": black_digits,
                        "red_digits": red_digits,
                    }
                )
                continue
            reading = _reading_from_digits(black_digits, red_digits)
            t = _sanitize_type(cs.get("type", "unknown"))
            reading, conf, note2 = _plausibility_filter(t, reading, conf)
            if reading is None:
                candidates.append(
                    {
                        "type": t,
                        "reading": None,
                        "serial": cs.get("serial"),
                        "confidence": conf,
                        "notes": "cells_sheet_reading_filtered",
                        "note2": note2,
                        "variant": f"cells_row_{src_label}_reject_filtered",
                        "provider": f"openai-odo-cells:{OCR_MODEL_ODOMETER}",
                        "black_digits": black_digits,
                        "red_digits": red_digits,
                    }
                )
                continue
            candidates.append(
                {
                    "type": t,
                    "reading": reading,
                    "serial": cs.get("serial"),
                    "confidence": conf,
                    "notes": str(cs.get("notes", "") or ""),
                    "note2": note2,
                    "variant": f"cells_row_{src_label}",
                    "provider": f"openai-odo-cells:{OCR_MODEL_ODOMETER}",
                    "black_digits": black_digits,
                    "red_digits": red_digits,
                }
            )
            if _is_ok_water_digits(candidates[-1]):
                cells_valid.append(candidates[-1])

        # Build consensus only from per-cell reads.
        if cells_valid:
            by_black: dict[str, float] = {}
            for c in cells_valid:
                b = _normalize_digits_string(c.get("black_digits"))
                if not b:
                    continue
                v = float(c.get("confidence") or 0.0)
                if _is_strong_water_digits(c):
                    v += 0.25
                by_black[b] = by_black.get(b, 0.0) + max(0.15, v)

            if by_black:
                best_black = max(by_black.items(), key=lambda kv: kv[1])[0]
                best_cells = sorted(
                    [c for c in cells_valid if _normalize_digits_string(c.get("black_digits")) == best_black],
                    key=lambda c: (
                        1 if _is_strong_water_digits(c) else 0,
                        float(c.get("confidence") or 0.0),
                    ),
                    reverse=True,
                )[0]
                # Synthetic consensus candidate to dominate noisy non-slot reads.
                candidates.append(
                    {
                        "type": best_cells.get("type") or "unknown",
                        "reading": best_cells.get("reading"),
                        "serial": best_cells.get("serial"),
                        "confidence": _clamp_confidence(max(float(best_cells.get("confidence") or 0.0), 0.80)),
                        "notes": "cells_sheet_consensus",
                        "note2": "",
                        "variant": f"cells_row_consensus_{_normalize_digits_string(best_black) or 'unknown'}",
                        "provider": f"openai-odo-cells:{OCR_MODEL_ODOMETER}",
                        "black_digits": _normalize_digits_string(best_cells.get("black_digits")),
                        "red_digits": _normalize_digits_string(best_cells.get("red_digits")),
                    }
                )
                fast_water_hit = True
        if local_det_strong_hit:
            fast_water_hit = True
    _mark_stage("odometer_variants")
    _mark_stage("cells_sheet")

    # full-image odometer pass (helps when circle/window crop misses the counter zone)
    full_pass_variants: list[tuple[str, bytes]] = []
    if not fast_water_hit:
        # Always test several generic views, not only orig.
        preferred_order = ("focused_crop", "center_crop_strong", "lowlight_enhanced", "contrast", "middle_band")
        by_label = {lbl: b for lbl, b in variants}
        added: set[str] = set()
        if variants:
            full_pass_variants.append(variants[0])
            added.add(str(variants[0][0]))
        for lbl in preferred_order:
            if lbl in by_label and lbl not in added:
                full_pass_variants.append((lbl, by_label[lbl]))
                added.add(lbl)
        for lbl, b in variants[1:]:
            if lbl in added:
                continue
            full_pass_variants.append((lbl, b))
            added.add(lbl)

    for label, b in full_pass_variants[: (2 if quick_serial_mode else 3)]:
        if not _time_budget_left():
            break
        try:
            wr_full = _vision(
                b,
                mime=mime,
                model=OCR_MODEL_ODOMETER,
                system_prompt=WATER_ODOMETER_SYSTEM_PROMPT,
            )
        except Exception:
            continue
        t = _sanitize_type(wr_full.get("type", "unknown"))
        serial = wr_full.get("serial", None)
        if isinstance(serial, str):
            serial = serial.strip() or None
        conf = _clamp_confidence(wr_full.get("confidence", 0.0))
        black_digits = _normalize_digits_string(wr_full.get("black_digits"))
        red_digits = _normalize_digits_string(wr_full.get("red_digits"))
        reading = _reading_from_digits(black_digits, red_digits)
        if reading is None:
            reading = _normalize_reading(wr_full.get("reading", None))
        reading, conf, note2 = _plausibility_filter(t, reading, conf)
        candidates.append(
            {
                "type": t,
                "reading": reading,
                "serial": serial,
                "confidence": conf,
                "notes": str(wr_full.get("notes", "") or ""),
                "note2": note2,
                "variant": f"odo_full_{label}",
                "provider": f"openai-odo:{OCR_MODEL_ODOMETER}",
                "black_digits": black_digits,
                "red_digits": red_digits,
            }
        )
        variant_image_map.setdefault(f"odo_full_{label}", b)
    _mark_stage("full_image_odo")

    # single high-quality "sheet" pass over multiple odometer windows
    sheet = None
    if (not OCR_WATER_DIGIT_FIRST) and (not fast_water_hit) and (not quick_serial_mode) and _time_budget_left():
        sheet = _make_water_odometer_sheet(img)
    if sheet and (not fast_water_hit) and (not quick_serial_mode) and _time_budget_left():
        try:
            ws = _vision(
                sheet,
                mime="image/jpeg",
                model=OCR_MODEL_ODOMETER,
                system_prompt=WATER_ODOMETER_SHEET_PROMPT,
            )
            t = _sanitize_type(ws.get("type", "unknown"))
            serial = ws.get("serial", None)
            if isinstance(serial, str):
                serial = serial.strip() or None
            conf = _clamp_confidence(ws.get("confidence", 0.0))
            black_digits = _normalize_digits_string(ws.get("black_digits"))
            red_digits = _normalize_digits_string(ws.get("red_digits"))
            reading = _reading_from_digits(black_digits, red_digits)
            if reading is None:
                reading = _normalize_reading(ws.get("reading", None))
            reading, conf, note2 = _plausibility_filter(t, reading, conf)
            candidates.append(
                {
                    "type": t,
                    "reading": reading,
                    "serial": serial,
                    "confidence": conf,
                    "notes": str(ws.get("notes", "") or ""),
                    "note2": note2,
                    "variant": f"odo_sheet_{ws.get('chosen_cell') or 'unknown'}",
                    "provider": f"openai-odo:{OCR_MODEL_ODOMETER}",
                    "black_digits": black_digits,
                    "red_digits": red_digits,
                }
            )
        except Exception:
            pass
    _mark_stage("odo_sheet")

    # optional external provider (Google Vision) as extra candidate
    # only when best still low-confidence
    best_mid = max(candidates, key=lambda x: _candidate_score(x, candidates))
    if _time_budget_left() and float(best_mid.get("confidence") or 0.0) < 0.72:
        try:
            g = _call_google_vision_candidate(img)
            if g:
                t = _sanitize_type(g.get("type", "unknown"))
                reading = _normalize_reading(g.get("reading", None))
                serial = g.get("serial", None)
                if isinstance(serial, str):
                    serial = serial.strip() or None
                conf = _clamp_confidence(g.get("confidence", 0.0))
                reading, conf, note2 = _plausibility_filter(t, reading, conf)
                candidates.append(
                    {
                        "type": t,
                        "reading": reading,
                        "serial": serial,
                        "confidence": conf,
                        "notes": str(g.get("notes", "") or ""),
                        "note2": note2,
                        "variant": "orig",
                        "provider": "google_vision",
                    }
                )
        except Exception:
            pass
    _mark_stage("google_fallback")

    # Contextual auto-fix for extra leading black digit on water drums.
    if context_prev_values:
        fixed_candidates: list[dict] = []
        for c in candidates:
            fixed_candidates.extend(
                _water_leading_trim_context_fixes(c, prev_values=context_prev_values)
            )
        if fixed_candidates:
            candidates.extend(fixed_candidates)
    layout_fixed_candidates: list[dict] = []
    for c in candidates:
        layout_fixed_candidates.extend(_water_suspicious_layout_fixes(c))
    if layout_fixed_candidates:
        candidates.extend(layout_fixed_candidates)

    water_pool = _water_digit_candidates(candidates)
    # Main path for water: strict odometer digit-first pipeline (feature flag).
    # Fallback: previous mixed ranking across all candidates.
    has_water_odometer_candidates = len(water_pool) > 0
    strong_water_pool: list[dict] = []
    if OCR_WATER_DIGIT_FIRST:
        strict_pool: list[dict] = []
        for c in water_pool:
            if not _is_strict_water_odometer_candidate(c):
                continue
            if _is_suspicious_water_digits(c):
                continue
            c2 = dict(c)
            b = _normalize_digits_string(c2.get("black_digits"))
            r = _normalized_red_digits(c2.get("red_digits"), min_len=2, max_len=3)
            c2["black_digits"] = b
            c2["red_digits"] = r
            c2["reading"] = _reading_from_digits(b, r)
            strict_pool.append(c2)

        strong_water_pool = [
            c
            for c in strict_pool
            if float(c.get("confidence") or 0.0) >= 0.55 and _is_strong_water_digits(c)
        ]
        safe_water_pool = strict_pool
        if strong_water_pool:
            pool = strong_water_pool
        elif safe_water_pool:
            pool = safe_water_pool
        elif has_water_odometer_candidates:
            # If strict validation rejected all odometer candidates, keep only non-suspicious ones.
            # If none left, fall back to mixed pool to avoid locking on obvious false positives.
            non_susp_water_pool = [c for c in water_pool if not _is_suspicious_water_digits(c)]
            pool = non_susp_water_pool if non_susp_water_pool else candidates
        else:
            pool = candidates
    else:
        pool = candidates
    best = max(pool, key=lambda x: _candidate_score(x, pool))
    context_override_note = ""
    if context_prev_values:
        ctx_best = _pick_water_candidate_with_context(
            candidates,
            prev_values=context_prev_values,
            serial_hints=context_serial_hints,
        )
        if ctx_best is not None:
            best_dist = _nearest_prev_distance(_normalize_reading(best.get("reading")), context_prev_values)
            ctx_dist = _nearest_prev_distance(_normalize_reading(ctx_best.get("reading")), context_prev_values)
            best_tail = _best_serial_tail_match(best.get("serial"), context_serial_hints) if context_serial_hints else 0
            ctx_tail = _best_serial_tail_match(ctx_best.get("serial"), context_serial_hints) if context_serial_hints else 0
            best_conf = float(best.get("confidence") or 0.0)
            ctx_conf = float(ctx_best.get("confidence") or 0.0)
            # Override only when contextual candidate is materially closer to historical band.
            should_override = (
                (best_dist > 280.0 and ctx_dist <= 220.0)
                or (ctx_dist + 15.0 < best_dist)
            )
            # If serial hint match is at least as strong, allow smaller context improvement.
            if (not should_override) and (ctx_tail >= best_tail) and (ctx_conf + 0.03 >= best_conf):
                should_override = (ctx_dist + 8.0 < best_dist)
            if should_override:
                best = ctx_best
                context_override_note = (
                    f"context_override prev={','.join(f'{v:.3f}' for v in context_prev_values[:3])}"
                )
    elif context_serial_hints:
        serial_best = _pick_water_candidate_by_serial(candidates, serial_hints=context_serial_hints)
        if serial_best is not None:
            base_tail = _best_serial_tail_match(best.get("serial"), context_serial_hints)
            serial_tail = _best_serial_tail_match(serial_best.get("serial"), context_serial_hints)
            best_score = _candidate_score(best, candidates)
            serial_score = _candidate_score(serial_best, candidates)
            if serial_tail >= 4 and ((base_tail < 4) or (serial_score + 0.05 >= best_score)):
                best = serial_best
                context_override_note = "serial_hint_override"

    # Hard-photo recovery: ask model for several alternative hypotheses from odometer-focused crops.
    needs_hypothesis_recovery = bool(
        OCR_WATER_DIGIT_FIRST
        and OCR_WATER_HYPOTHESIS_PASS
        and (not quick_serial_mode)
        and (
            best.get("reading") is None
            or (not _is_ok_water_digits(best))
            or _is_suspicious_water_digits(best)
        )
    )
    if needs_hypothesis_recovery:
        hyp_sources: list[tuple[str, bytes]] = []
        seen_hyp_src: set[str] = set()

        def _push_hyp_sources(srcs: list[tuple[str, bytes]], limit: int) -> None:
            for lbl, src in srcs[:limit]:
                if (not lbl) or (lbl in seen_hyp_src):
                    continue
                hyp_sources.append((lbl, src))
                seen_hyp_src.add(lbl)

        _push_hyp_sources(det_row_variants, 3)
        _push_hyp_sources(face_row_variants, 3)
        _push_hyp_sources(row_variants, 2)
        _push_hyp_sources(roi_row_variants, 2)
        _push_hyp_sources(top_variants, 1)
        _push_hyp_sources([(str(lbl), b) for lbl, b in variants[:1]], 1)

        hyp_added: list[dict] = []
        hyp_calls = 0
        for src_label, src_bytes in hyp_sources:
            if hyp_calls >= OCR_WATER_HYPOTHESIS_MAX_CALLS:
                break
            if not _time_budget_left():
                break
            try:
                hr = _vision(
                    src_bytes,
                    mime="image/jpeg",
                    model=OCR_MODEL_ODOMETER,
                    system_prompt=WATER_HYPOTHESES_PROMPT,
                    user_text=(
                        "Сформируй 3-5 гипотез чтения барабана. "
                        "Укажи black_digits/red_digits/reading/confidence для каждой гипотезы."
                    ),
                    detail="high",
                )
            except Exception:
                continue
            hyp_calls += 1
            built = _water_hypothesis_candidates_from_response(
                hr,
                variant_prefix=f"hyp_{src_label}",
                provider=f"openai-odo-hyp:{OCR_MODEL_ODOMETER}",
            )
            if not built:
                continue
            candidates.extend(built)
            hyp_added.extend(built)

        if hyp_added:
            hyp_best = _pick_best_water_candidate_adaptive(
                hyp_added,
                prev_values=context_prev_values,
                serial_hints=context_serial_hints,
            )
            if hyp_best is not None:
                curr_score = _candidate_score(best, candidates)
                hyp_score = _candidate_score(hyp_best, candidates)
                curr_ok = _is_ok_water_digits(best)
                hyp_ok = _is_ok_water_digits(hyp_best)
                should_override = bool(
                    (hyp_ok and (not curr_ok))
                    or (best.get("reading") is None and hyp_best.get("reading") is not None)
                    or (hyp_score >= curr_score + 0.08)
                )
                if should_override:
                    best = hyp_best
                    context_override_note = (
                        f"{context_override_note}; hypothesis_override".strip("; ")
                        if context_override_note
                        else "hypothesis_override"
                    )

    # If the winner came from aggressive context-trim fix, prefer integer-part
    # consensus across variants when available.
    if context_prev_values:
        best_notes_now = str(best.get("notes") or "")
        if "context_trim_leading_digit" in best_notes_now:
            consensus_best = _pick_water_integer_consensus_candidate(
                candidates,
                prev_values=context_prev_values,
            )
            if consensus_best is not None:
                curr_reading = _normalize_reading(best.get("reading"))
                cons_reading = _normalize_reading(consensus_best.get("reading"))
                if (curr_reading is None) or (
                    cons_reading is not None and int(cons_reading) != int(curr_reading)
                ):
                    best = consensus_best
                    context_override_note = (
                        f"{context_override_note}; integer_consensus_override".strip("; ")
                        if context_override_note
                        else "integer_consensus_override"
                    )

    chosen_label = str(best.get("variant") or "orig")

    # финальная confidence слегка повышается при согласии нескольких вариантов
    agree = 0
    for c in candidates:
        if c is best:
            continue
        if _same_candidate(best, c):
            agree += 1
    conf_boost = min(0.15, 0.05 * agree)

    t = best["type"]
    reading = best["reading"]
    serial = best["serial"] or global_serial
    conf = _clamp_confidence(float(best["confidence"]) + conf_boost)
    note2 = best.get("note2") or ""

    # If black integer part is shaky, run a dedicated 5-cell black-digit pass.
    black_note = ""
    best_black = _normalize_digits_string(best.get("black_digits"))
    best_red = _normalized_red_digits(best.get("red_digits"), min_len=2, max_len=3)
    best_provider = str(best.get("provider") or "")
    best_variant = str(best.get("variant") or "")
    is_water_candidate = (
        (t in ("ХВС", "ГВС", "unknown"))
        and (
            best_provider.startswith("openai-water")
            or best_provider.startswith("openai-odo")
            or best_variant.startswith("water_")
            or best_variant.startswith("odo_")
        )
    )
    needs_black_refine = (
        is_water_candidate
        and bool(best_black)
        and len(best_black) >= 4
        and (
            best_variant.startswith("counter_row_")
            or best_variant.startswith("counter_row_clahe_")
            or best_variant.startswith("counter_row_bw_")
            or _is_suspicious_water_digits(best)
            or best_variant.startswith("odo_global_")
            or best_variant.startswith("odo_top_strip_")
            or len(best_black) < 5
        )
    )
    if needs_black_refine and _time_budget_left():
        winner_crop = _variant_image_bytes(variant_image_map, best_variant)
        if winner_crop:
            black_votes: dict[str, float] = {}
            black_variants = _make_black_focus_variants_from_row(winner_crop)

            # First pass: direct row variants.
            for src_label, src_bytes in black_variants:
                if not _time_budget_left():
                    break
                try:
                    br = _vision(
                        src_bytes,
                        mime="image/jpeg",
                        model=OCR_MODEL_ODOMETER,
                        system_prompt=WATER_BLACK_DIGITS_PROMPT,
                    )
                except Exception:
                    continue
                refined = _normalize_digits_string(br.get("black_digits"))
                if not refined:
                    continue
                if len(refined) > 5:
                    refined = refined[:5]
                if len(refined) != 5:
                    continue
                if _digits_overlap_serial(refined, serial):
                    continue
                if refined.startswith("00") and len(refined.lstrip("0")) <= 2:
                    continue
                v_conf = _clamp_confidence(br.get("confidence", 0.0))
                black_votes[refined] = black_votes.get(refined, 0.0) + max(0.25, v_conf)

            # Fallback pass: 5-cell sheet from row.
            black_sheet = _make_water_black_cells_sheet_from_row(winner_crop)
            if black_sheet and _time_budget_left():
                try:
                    brs = _vision(
                        black_sheet,
                        mime="image/jpeg",
                        model=OCR_MODEL_ODOMETER,
                        system_prompt=WATER_BLACK_DIGITS_PROMPT,
                    )
                    refined_s = _normalize_digits_string(brs.get("black_digits"))
                    if refined_s:
                        if len(refined_s) > 5:
                            refined_s = refined_s[:5]
                        if len(refined_s) == 5 and (not _digits_overlap_serial(refined_s, serial)):
                            s_conf = _clamp_confidence(brs.get("confidence", 0.0))
                            black_votes[refined_s] = black_votes.get(refined_s, 0.0) + max(0.25, s_conf)
                except Exception:
                    pass

            if black_votes:
                refined_black = max(black_votes.items(), key=lambda kv: kv[1])[0]
                # Принимаем только умеренную правку (без радикальной смены числа).
                allow_refined_black = _digit_distance(best_black, refined_black) <= 2
                if (not allow_refined_black) and context_prev_values and best_black:
                    curr_ctx_dist = _nearest_prev_distance(_reading_from_digits(best_black, best_red), context_prev_values)
                    ref_ctx_dist = _nearest_prev_distance(_reading_from_digits(refined_black, best_red), context_prev_values)
                    if ref_ctx_dist + 1.0 < curr_ctx_dist:
                        allow_refined_black = True
                if allow_refined_black:
                    best_black = refined_black
                    best["black_digits"] = refined_black
                    reading = _reading_from_digits(best_black, best_red)
                    best["reading"] = reading
                    conf = _clamp_confidence(max(conf, 0.72))
                    black_note = f"black_refine={refined_black}@vote"

    # If integer part was detected but fractional red drums are unstable,
    # recover decimals from several focused crops with weighted voting.
    red_note = ""
    target_int = _reading_integer_part(reading)
    if target_int is None and best_black:
        try:
            target_int = int(best_black)
        except Exception:
            target_int = None
    context_same_int_has_3dec = False
    if (target_int is not None) and context_prev_values:
        for pv in context_prev_values:
            try:
                fv = float(pv)
            except Exception:
                continue
            if int(fv) != int(target_int):
                continue
            frac = abs(fv - float(int(fv)))
            frac3 = int(round(frac * 1000))
            if frac3 % 10 != 0:
                context_same_int_has_3dec = True
                break
    has_red_disagreement = _has_red_disagreement_for_integer(candidates, target_int)
    needs_red_refine = (
        (not OCR_WATER_INTEGER_ONLY)
        and is_water_candidate
        and bool(best_black)
        and (
            _is_weak_red_digits(best_red)
            or _is_suspicious_water_digits(best)
            or has_red_disagreement
            or (bool(best_red) and len(str(best_red)) == 2 and context_same_int_has_3dec)
        )
    )
    if needs_red_refine and _time_budget_left():
        red_votes, red_counts, red_best_conf = _collect_red_votes_for_integer(
            candidates,
            target_int=target_int,
        )
        red_zone_crops: list[tuple[str, bytes]] = []
        seen_red_labels: set[str] = set()

        def _push_red_source(lbl: str, blob: bytes) -> None:
            if not lbl or lbl in seen_red_labels:
                return
            seen_red_labels.add(lbl)
            red_zone_crops.append((lbl, blob))

        winner_crop = _variant_image_bytes(variant_image_map, best_variant)
        if winner_crop:
            _push_red_source(f"winner:{best_variant}", winner_crop)
            for rl, rb in _make_red_focus_variants_from_crop(winner_crop, prefix="winner_red"):
                _push_red_source(rl, rb)
        for lbl, b in top_variants[:2]:
            _push_red_source(lbl, b)
            for rl, rb in _make_red_focus_variants_from_crop(b, prefix=f"top_red_{lbl}"):
                _push_red_source(rl, rb)
        for lbl, b in global_variants[:3]:
            _push_red_source(lbl, b)
            for rl, rb in _make_red_focus_variants_from_crop(b, prefix=f"glob_red_{lbl}"):
                _push_red_source(rl, rb)
        for lbl, b in water_variants:
            if ("water_red_zone" in str(lbl)) or ("water_odometer_band" in str(lbl)):
                _push_red_source(lbl, b)
        if target_int is not None:
            for c in candidates:
                if _reading_integer_part(c.get("reading")) != target_int:
                    continue
                src_lbl = str(c.get("variant") or "")
                src_blob = _variant_image_bytes(variant_image_map, src_lbl)
                if src_blob is not None:
                    _push_red_source(f"cand:{src_lbl}", src_blob)
                if len(red_zone_crops) >= OCR_RED_REFINE_MAX_SOURCES:
                    break

        for src_label, rb in red_zone_crops[:OCR_RED_REFINE_MAX_SOURCES]:
            if not _time_budget_left():
                break
            for _ in range(OCR_RED_REFINE_REPEATS):
                if not _time_budget_left():
                    break
                try:
                    rr = _vision(
                        rb,
                        mime="image/jpeg",
                        model=OCR_MODEL_ODOMETER,
                        system_prompt=WATER_RED_DIGITS_PROMPT,
                        user_text="Прочитай только красные окна справа. Верни 2-3 цифры.",
                    )
                except Exception:
                    continue
                rr_digits = _normalized_red_digits(
                    _extract_red_digits_only(rr),
                    min_len=2,
                    max_len=3,
                )
                rr_conf = _clamp_confidence(rr.get("confidence", 0.0))
                if not rr_digits or rr_conf < 0.35:
                    continue
                w = rr_conf + (0.14 if len(rr_digits) >= 3 else 0.03)
                red_votes[rr_digits] = red_votes.get(rr_digits, 0.0) + w
                red_counts[rr_digits] = red_counts.get(rr_digits, 0) + 1
                red_best_conf[rr_digits] = max(red_best_conf.get(rr_digits, 0.0), rr_conf)

        voted_red = _pick_red_digits_by_vote(red_votes, red_counts, red_best_conf)
        if voted_red:
            should_override = (
                _is_weak_red_digits(best_red)
                or has_red_disagreement
                or _is_suspicious_water_digits(best)
                or voted_red != (best_red or "")
            )
            if should_override:
                best["red_digits"] = voted_red
                if context_same_int_has_3dec and len(voted_red) >= 3 and best_black:
                    try:
                        reading = float(f"{int(best_black)}.{voted_red[:3]}")
                    except Exception:
                        reading = _reading_from_digits(best_black, voted_red)
                else:
                    reading = _reading_from_digits(best_black, voted_red)
                best["reading"] = reading
                conf = _clamp_confidence(max(conf, min(0.99, red_best_conf.get(voted_red, 0.0) + 0.05)))
                best_red = voted_red
                red_note = f"red_refine_vote={voted_red}"

    # Contextual decimal recovery: when integer part is stable but red drums are weak,
    # borrow fractional part from nearest historical value with the same integer band.
    frac_ctx_note = ""
    if (
        (not OCR_WATER_INTEGER_ONLY)
        and is_water_candidate
        and context_prev_values
        and bool(best_black)
        and _is_weak_red_digits(best_red)
    ):
        frac_fixed = _refine_fraction_from_prev(best_black, context_prev_values)
        if frac_fixed is not None:
            curr_val = _normalize_reading(reading)
            if (curr_val is None) or (abs(frac_fixed - curr_val) <= 0.25):
                reading = frac_fixed
                best["reading"] = frac_fixed
                conf = _clamp_confidence(max(conf, 0.66))
                frac_ctx_note = f"context_frac_refine={frac_fixed:.{OCR_WATER_DECIMALS}f}"

    snap_ctx_note = ""
    non_serial_numeric = sum(
        1
        for c in candidates
        if (c.get("reading") is not None)
        and (not str(c.get("provider") or "").startswith("openai-odo-serial-target"))
    )
    if (
        (not OCR_WATER_INTEGER_ONLY)
        and is_water_candidate
        and context_prev_values
        and str(best_provider).startswith("openai-odo-serial-target")
        and (agree <= 1)
        and (non_serial_numeric == 0)
        and (reading is not None)
    ):
        snapped = _snap_to_same_integer_context(reading, context_prev_values, tolerance=0.25)
        if (snapped is not None) and (abs(float(snapped) - float(reading)) >= 0.02):
            reading = snapped
            best["reading"] = snapped
            conf = _clamp_confidence(max(conf, 0.72))
            snap_ctx_note = f"context_same_int_snap={snapped:.{OCR_WATER_DECIMALS}f}"

    # notes
    notes = str(best.get("notes", "") or "")
    if note2:
        if notes:
            notes = f"{notes}; {note2}"
        else:
            notes = note2
    if red_note:
        notes = f"{notes}; {red_note}" if notes else red_note
    if black_note:
        notes = f"{notes}; {black_note}" if notes else black_note
    if frac_ctx_note:
        notes = f"{notes}; {frac_ctx_note}" if notes else frac_ctx_note
    if snap_ctx_note:
        notes = f"{notes}; {snap_ctx_note}" if notes else snap_ctx_note
    if context_override_note:
        notes = f"{notes}; {context_override_note}" if notes else context_override_note
    notes = (notes.strip() or "")
    provider = str(best.get("provider") or "openai")
    notes = (
        notes
        + (f"; provider={provider}; variant={chosen_label}; agree={agree+1}/{len(candidates)}" if chosen_label else "")
    ).strip()[:240]

    out = {
        "type": t,
        "reading": reading if (isinstance(reading, (int, float)) or reading is None) else None,
        "serial": serial,
        "confidence": conf,
        "notes": notes,
    }
    if OCR_DEBUG:
        ranked = sorted(candidates, key=lambda x: _candidate_score(x, candidates), reverse=True)[:20]
        out["debug"] = [
            {
                "provider": str(c.get("provider") or "unknown"),
                "variant": str(c.get("variant") or "orig"),
                "type": str(c.get("type") or "unknown"),
                "reading": c.get("reading"),
                "confidence": float(c.get("confidence") or 0.0),
                "black_digits": c.get("black_digits"),
                "red_digits": c.get("red_digits"),
            }
            for c in ranked
        ]
        out["timings_ms"] = dict(stage_ms)
        out["openai_calls"] = vision_calls
    out["trace_id"] = req_trace_id

    # Hard safety for water:
    # - "ok" odometer read (black digits) is enough to keep integer result;
    # - "strong" odometer read (black + reliable red digits) keeps high confidence;
    # - no "ok" read => block numeric result (anti-hallucination guard).
    winner_provider = str(best.get("provider") or "")
    winner_is_water_family = (
        str(best.get("type") or "") in ("ХВС", "ГВС", "unknown")
        and (
            winner_provider.startswith("openai-water")
            or winner_provider.startswith("openai-odo")
            or winner_provider.startswith("openai:")
            or winner_provider.startswith("google_vision")
        )
    )
    winner_black = _normalize_digits_string(best.get("black_digits"))
    winner_is_ok_odo = _is_ok_water_digits(best)
    winner_is_strong_odo = _is_strong_water_digits(best)
    if OCR_WATER_DIGIT_FIRST and winner_is_water_family and not winner_is_ok_odo:
        ctx_dist = _nearest_prev_distance(_normalize_reading(out.get("reading")), context_prev_values)
        allow_context_keep = bool(
            context_override_note
            and (out.get("reading") is not None)
            and context_prev_values
            and ctx_dist <= 240.0
        )
        if allow_context_keep:
            out["confidence"] = min(float(out.get("confidence") or 0.0), 0.62)
            base_notes = str(out.get("notes") or "").strip()
            tail = f"water_context_keep_no_ok_odometer(dist={ctx_dist:.2f})"
            out["notes"] = f"{base_notes}; {tail}".strip("; ").strip()
        else:
            out["type"] = "unknown"
            out["reading"] = None
            out["confidence"] = min(float(out.get("confidence") or 0.0), 0.45)
            base_notes = str(out.get("notes") or "").strip()
            tail = "water_no_ok_odometer_winner"
            out["notes"] = f"{base_notes}; {tail}".strip("; ").strip()
    elif OCR_WATER_DIGIT_FIRST and winner_is_water_family and winner_is_ok_odo and not winner_is_strong_odo:
        if out.get("reading") is None and winner_black:
            try:
                out["reading"] = float(int(winner_black))
            except Exception:
                pass
        out["confidence"] = min(float(out.get("confidence") or 0.0), 0.70)
        base_notes = str(out.get("notes") or "").strip()
        tail = "water_black_only_or_weak_red"
        out["notes"] = f"{base_notes}; {tail}".strip("; ").strip()
    # Additional safety: if contextual distance is too large and candidate has no corroboration,
    # prefer null over a likely false positive from a single crop.
    if OCR_WATER_DIGIT_FIRST and winner_is_water_family and context_prev_values and (out.get("reading") is not None):
        ctx_dist = _nearest_prev_distance(_normalize_reading(out.get("reading")), context_prev_values)
        if (ctx_dist > 140.0) and (agree == 0):
            out["type"] = "unknown"
            out["reading"] = None
            out["confidence"] = min(float(out.get("confidence") or 0.0), 0.45)
            base_notes = str(out.get("notes") or "").strip()
            tail = f"water_context_far_singleton(dist={ctx_dist:.2f})"
            out["notes"] = f"{base_notes}; {tail}".strip("; ").strip()
    # Serial-target can overfit to a wrong meter when several serial hints are provided.
    # If the winner is uncorroborated and far from context, prefer null over a confident false positive.
    if (
        OCR_WATER_DIGIT_FIRST
        and winner_provider.startswith("openai-odo-serial-target")
        and len(context_serial_hints) >= 2
        and (out.get("reading") is not None)
    ):
        serial_ctx_dist = _nearest_prev_distance(_normalize_reading(out.get("reading")), context_prev_values)
        if context_prev_values and (agree == 0) and (serial_ctx_dist > 90.0):
            out["type"] = "unknown"
            out["reading"] = None
            out["confidence"] = min(float(out.get("confidence") or 0.0), 0.45)
            base_notes = str(out.get("notes") or "").strip()
            tail = f"serial_target_multi_hint_unconfirmed(dist={serial_ctx_dist:.2f})"
            out["notes"] = f"{base_notes}; {tail}".strip("; ").strip()
    _mark_stage("finalize")
    if OCR_DEBUG:
        out["timings_ms"] = dict(stage_ms)
    logger.info(
        "ocr_recognize done trace_id=%s elapsed_ms=%s type=%s reading=%s confidence=%s variant=%s provider=%s",
        req_trace_id,
        int((time.monotonic() - started_at) * 1000),
        out.get("type"),
        out.get("reading"),
        out.get("confidence"),
        chosen_label,
        str(best.get("provider") or ""),
    )
    return out


@app.post("/recognize-series")
async def recognize_series(
    files: list[UploadFile] = File(...),
    trace_id: Optional[str] = Form(None),
    context_prev_water: Optional[str] = Form(None),
    context_serial_hint: Optional[str] = Form(None),
):
    if not files:
        raise HTTPException(status_code=400, detail="empty_files")
    if len(files) > OCR_SERIES_MAX_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"too_many_files_max_{OCR_SERIES_MAX_FILES}",
        )

    req_trace_id = (str(trace_id or "").strip() or f"ocrs-{uuid.uuid4().hex[:12]}")
    prev_values = _parse_context_prev_water(context_prev_water)
    serial_hints = _parse_context_serial_hints(context_serial_hint)
    series_items: list[dict] = []

    for idx, upl in enumerate(files, start=1):
        item_trace = f"{req_trace_id}-f{idx}"
        name = str(upl.filename or f"file_{idx}").strip() or f"file_{idx}"
        try:
            item_res = await recognize(
                file=upl,
                trace_id=item_trace,
                context_prev_water=context_prev_water,
                context_serial_hint=context_serial_hint,
            )
            rec = dict(item_res or {})
        except Exception as e:
            rec = {
                "type": "unknown",
                "reading": None,
                "serial": None,
                "confidence": 0.0,
                "notes": f"series_item_failed:{e}",
                "trace_id": item_trace,
            }
        rec["filename"] = name
        series_items.append(rec)

    # Neighbor recovery is safe mainly for context-aware water batches.
    # For generic/electric batches without context it can produce wrong carry-over values.
    if OCR_SERIES_NEIGHBOR_RECOVERY and (bool(prev_values) or bool(serial_hints)):
        series_items = _recover_series_missing_with_neighbors(series_items)
    best_idx, best_item = _pick_best_series_result(series_items, prev_values=prev_values)
    best_score = _series_result_score(best_item, series_items, prev_values=prev_values)

    # Keep batch response compact; debug can still be inspected per item if OCR_DEBUG enabled.
    compact_items: list[dict] = []
    for r in series_items:
        row = {
            "filename": r.get("filename"),
            "type": r.get("type"),
            "reading": r.get("reading"),
            "serial": r.get("serial"),
            "confidence": r.get("confidence"),
            "notes": r.get("notes"),
            "trace_id": r.get("trace_id"),
        }
        compact_items.append(row)

    out = {
        "trace_id": req_trace_id,
        "files_count": len(series_items),
        "best_index": best_idx,
        "best_score": round(float(best_score), 6),
        "best": {
            "filename": best_item.get("filename"),
            "type": best_item.get("type"),
            "reading": best_item.get("reading"),
            "serial": best_item.get("serial"),
            "confidence": best_item.get("confidence"),
            "notes": best_item.get("notes"),
            "trace_id": best_item.get("trace_id"),
        },
        "results": compact_items,
    }
    return out
