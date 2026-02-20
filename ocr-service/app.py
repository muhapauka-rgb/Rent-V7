import os
import base64
import json
import requests
import re
import time
from io import BytesIO
from PIL import Image, ImageOps, ImageFilter, ImageEnhance
from typing import Optional, Tuple
from fastapi import FastAPI, UploadFile, File, HTTPException
import numpy as np
import cv2

def _env_nonempty(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OCR_MODEL = _env_nonempty("OCR_MODEL", "gpt-4o")
OCR_MODEL_PRIMARY = _env_nonempty("OCR_MODEL_PRIMARY", OCR_MODEL)
OCR_MODEL_FALLBACK = _env_nonempty("OCR_MODEL_FALLBACK", "gpt-4o-mini")
OCR_MODEL_ODOMETER = _env_nonempty("OCR_MODEL_ODOMETER", "gpt-4o")
OCR_FALLBACK_MIN_CONF = float(os.getenv("OCR_FALLBACK_MIN_CONF", "0.78"))
OPENAI_TIMEOUT_SEC = float(os.getenv("OPENAI_TIMEOUT_SEC", "15"))
OCR_MAX_RUNTIME_SEC = float(os.getenv("OCR_MAX_RUNTIME_SEC", "35"))
GOOGLE_VISION_API_KEY = os.getenv("GOOGLE_VISION_API_KEY", "").strip()
OCR_DEBUG = os.getenv("OCR_DEBUG", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_WATER_DIGIT_FIRST = os.getenv("OCR_WATER_DIGIT_FIRST", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_WATER_INTEGER_ONLY = os.getenv("OCR_WATER_INTEGER_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")

app = FastAPI()

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


def _call_openai_vision(image_bytes: bytes, mime: str, model: str, system_prompt: str = SYSTEM_PROMPT) -> dict:
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

    b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:{mime};base64,{b64}"

    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Определи тип счётчика и показание. Верни JSON строго по схеме."},
                    {"type": "image_url", "image_url": {"url": data_url, "detail": "high"}},
                ],
            },
        ],
        "max_tokens": 250,
    }

    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        json=payload,
        timeout=(5, OPENAI_TIMEOUT_SEC),
    )
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"openai_http_{r.status_code}: {r.text[:300]}")

    content = r.json()["choices"][0]["message"]["content"]
    return _extract_json_object(content)


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

    # Choose up to 5 variants (quality > speed; still bounded)
    if img.height > img.width:
        if orient and len(variants) < 5:
            variants.append((orient_label or "rotate90", orient))
        if focused and len(variants) < 5:
            variants.append(("focused_crop", focused))
        if center and len(variants) < 5:
            variants.append(("center_crop_strong", center))
        if mid_band and len(variants) < 5:
            variants.append(("middle_band", mid_band))
        if contrast and len(variants) < 5:
            variants.append(("contrast", contrast))
    else:
        if focused and len(variants) < 5:
            variants.append(("focused_crop", focused))
        if mid_band and len(variants) < 5:
            variants.append(("middle_band", mid_band))
        if center and len(variants) < 5:
            variants.append(("center_crop_strong", center))
        if orient and len(variants) < 5:
            variants.append((orient_label or "center_crop", orient))
        if contrast and len(variants) < 5:
            variants.append(("contrast", contrast))

    return variants[:5]


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


def _reading_from_digits(black: Optional[str], red: Optional[str]) -> Optional[float]:
    if not black:
        return None
    try:
        # For water now we prioritize stable integer part.
        # Decimal (red drums) can be noisy on dark/angled photos.
        if OCR_WATER_INTEGER_ONLY:
            return float(int(black))
        if red:
            return float(f"{int(black)}.{red}")
        return float(int(black))
    except Exception:
        return None


def _normalized_red_digits(v: Optional[str], *, min_len: int = 2, max_len: int = 3) -> Optional[str]:
    d = _normalize_digits_string(v)
    if not d:
        return None
    d = d[:max_len]
    if len(d) < min_len:
        return None
    return d


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
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return None
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
            return None
        circles = np.round(circles[0, :]).astype(int)
        x, y, r = max(circles, key=lambda c: c[2])

        boxes = [
            (x - int(r * 0.72), y - int(r * 0.40), x + int(r * 0.78), y + int(r * 0.10)),
            (x - int(r * 0.68), y - int(r * 0.32), x + int(r * 0.72), y + int(r * 0.16)),
            (x - int(r * 0.82), y - int(r * 0.46), x + int(r * 0.88), y + int(r * 0.14)),
            (x - int(r * 0.76), y - int(r * 0.52), x + int(r * 0.78), y - int(r * 0.10)),
            (x - int(r * 0.90), y - int(r * 0.20), x + int(r * 0.92), y + int(r * 0.26)),
            (x - int(r * 0.95), y - int(r * 0.05), x + int(r * 0.96), y + int(r * 0.36)),
        ]

        tiles = []
        for (bx1, by1, bx2, by2) in boxes:
            bx1, by1, bx2, by2 = _clamp_box(bx1, by1, bx2, by2, w, h)
            crop = im[by1:by2, bx1:bx2]
            if crop.size == 0:
                continue
            rgb = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB)
            p = Image.fromarray(rgb).resize((540, 220), Image.Resampling.LANCZOS)
            p = ImageEnhance.Contrast(p).enhance(1.7).filter(
                ImageFilter.UnsharpMask(radius=1, percent=240, threshold=2)
            )
            tiles.append(p)
        if len(tiles) < 4:
            return None
        while len(tiles) < 6:
            tiles.append(tiles[-1])

        sheet = Image.new("RGB", (1100, 760), (250, 250, 250))
        positions = [(20, 20), (560, 20), (20, 270), (560, 270), (20, 520), (560, 520)]
        labels = ["A1", "A2", "A3", "A4", "A5", "A6"]
        for i, (tx, ty) in enumerate(positions):
            sheet.paste(tiles[i], (tx, ty))
            # simple label strip
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
    if variant.startswith("circle_row_"):
        score += 0.34
    if variant.startswith("odo_global_"):
        score += 0.20
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
        score -= 0.40
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
    if len(b) >= 5 and b.startswith("00") and sig <= 3 and r_len < 3:
        return True
    if len(b) >= 5 and sig <= 2:
        return True
    if len(b) >= 6 and b.startswith("000") and sig <= 4:
        return True
    # For water odometer, one fractional digit is usually a truncated read.
    if r and len(r) == 1 and sig >= 3:
        return True
    if r and len(r) == 2 and sig <= 3 and b.startswith("00"):
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


@app.post("/recognize")
async def recognize(file: UploadFile = File(...)):
    started_at = time.monotonic()

    # Keep part of time budget for dedicated odometer passes on water meters.
    odo_reserve_sec = 16.0 if OCR_WATER_DIGIT_FIRST else 0.0

    def _time_budget_left(min_remaining_sec: float = 0.0) -> bool:
        budget = max(1.0, OCR_MAX_RUNTIME_SEC - max(0.0, min_remaining_sec))
        return (time.monotonic() - started_at) < budget

    img = await file.read()
    if not img:
        raise HTTPException(status_code=400, detail="empty_file")

    mime = _guess_mime(file.filename, file.content_type)
    variants = _make_variants(img)
    variant_image_map: dict[str, bytes] = {}

    candidates: list[dict] = []

    # Digit-first bootstrap for water counters:
    # run one strict odometer pass on original frame before generic pipeline.
    if OCR_WATER_DIGIT_FIRST and variants:
        pre_label, pre_bytes = variants[0]
        variant_image_map.setdefault(f"odo_pre_{pre_label}", pre_bytes)
        if _time_budget_left(odo_reserve_sec):
            try:
                pre = _call_openai_vision(
                    pre_bytes,
                    mime=mime,
                    model=OCR_MODEL_ODOMETER,
                    system_prompt=WATER_ODOMETER_SYSTEM_PROMPT,
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
    initial_variant_limit = 1 if OCR_WATER_DIGIT_FIRST else 2
    for label, b in variants[:initial_variant_limit]:
        variant_image_map.setdefault(label, b)
        if not _time_budget_left(odo_reserve_sec):
            break
        try:
            resp = _call_openai_vision(b, mime=mime, model=OCR_MODEL_PRIMARY)
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

    if not candidates:
        raise HTTPException(status_code=500, detail="openai_empty_response")

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
                resp = _call_openai_vision(b, mime=mime, model=OCR_MODEL_FALLBACK)
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

    # water-specific second stage: always try dial-focused OCR and let ranker decide
    water_variants = _make_water_dial_variants(img)
    if OCR_WATER_DIGIT_FIRST:
        water_variants = sorted(
            water_variants,
            key=lambda x: 0 if str(x[0]).startswith("water_odometer_band_") else 1,
        )
    water_variant_limit = 0 if OCR_WATER_DIGIT_FIRST else 2
    for label, wb in water_variants[:water_variant_limit]:
        variant_image_map.setdefault(label, wb)
        if not _time_budget_left(odo_reserve_sec):
            break
        try:
            wr = _call_openai_vision(wb, mime="image/jpeg", model=OCR_MODEL_PRIMARY, system_prompt=WATER_SYSTEM_PROMPT)
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

    # water-special prompt on generic variants too (helps when circle detection misses)
    for label, b in ([] if OCR_WATER_DIGIT_FIRST else variants[:1]):
        variant_image_map.setdefault(f"water_{label}", b)
        if not _time_budget_left(odo_reserve_sec):
            break
        try:
            wr2 = _call_openai_vision(b, mime=mime, model=OCR_MODEL_PRIMARY, system_prompt=WATER_SYSTEM_PROMPT)
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
    odo_variants = _make_water_odometer_window_variants(img)
    global_variants = _make_water_global_strip_variants(img)
    box_variants = _make_water_counter_box_variants(img)
    row_variants = _make_water_counter_row_variants(img)
    circle_row_variants = _make_water_circle_row_variants(img)
    circle_odo_variants = _make_water_circle_odometer_strips(img)
    meter_face_variants = _make_water_meter_face_variants(img)
    blackhat_row_variants = _make_water_blackhat_row_variants(img)
    top_variants = _make_water_top_strip_variants(img)
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
    odometer_variants = (
        face_row_variants[:8]
        + blackhat_row_variants[:4]
        + circle_odo_variants[:4]
        +
        row_variants[:4]
        + circle_row_variants[:2]
        + box_variants[:1]
        + odo_variants[:1]
        + top_variants[:1]
    )
    fast_water_hit = False
    strong_readings: list[float] = []
    for idx, (label, b) in enumerate(odometer_variants, start=1):
        variant_image_map.setdefault(label, b)
        if not _time_budget_left():
            break
        try:
            wr3 = _call_openai_vision(
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

    # full-image odometer pass (helps when circle/window crop misses the counter zone)
    for label, b in ([] if fast_water_hit else variants[:1]):
        if not _time_budget_left():
            break
        try:
            wr_full = _call_openai_vision(
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

    # single high-quality "sheet" pass over multiple odometer windows
    sheet = _make_water_odometer_sheet(img)
    if sheet and (not fast_water_hit) and _time_budget_left():
        try:
            ws = _call_openai_vision(
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
            # Не смешиваем с full-frame кандидатами, если одометр-кандидаты есть,
            # но пока не прошли строгую валидацию.
            pool = water_pool
        else:
            pool = candidates
    else:
        pool = candidates
    best = max(pool, key=lambda x: _candidate_score(x, pool))
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
        )
    )
    if needs_black_refine and _time_budget_left():
        winner_crop = variant_image_map.get(best_variant)
        if winner_crop:
            black_votes: dict[str, float] = {}
            black_variants = _make_black_focus_variants_from_row(winner_crop)

            # First pass: direct row variants.
            for src_label, src_bytes in black_variants:
                if not _time_budget_left():
                    break
                try:
                    br = _call_openai_vision(
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
                    brs = _call_openai_vision(
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
                if _digit_distance(best_black, refined_black) <= 2:
                    best_black = refined_black
                    best["black_digits"] = refined_black
                    reading = _reading_from_digits(best_black, best_red)
                    best["reading"] = reading
                    conf = _clamp_confidence(max(conf, 0.72))
                    black_note = f"black_refine={refined_black}@vote"

    # If integer part was detected but fractional red drums were missed,
    # run one focused red-zone pass to recover decimals.
    red_note = ""
    needs_red_refine = (
        (not OCR_WATER_INTEGER_ONLY)
        and
        is_water_candidate
        and bool(best_black)
        and (
            not best_red
            or len(best_red) < 2
            or _is_suspicious_water_digits(best)
        )
    )
    if needs_red_refine and _time_budget_left():
        red_zone_crops: list[tuple[str, bytes]] = []
        winner_crop = variant_image_map.get(best_variant)
        if winner_crop:
            red_zone_crops.append((f"winner:{best_variant}", winner_crop))
        red_zone_crops.extend(
            [(lbl, b) for lbl, b in water_variants if lbl == "water_red_zone"]
        )
        for src_label, rb in red_zone_crops[:2]:
            if not _time_budget_left():
                break
            try:
                rr = _call_openai_vision(
                    rb,
                    mime="image/jpeg",
                    model=OCR_MODEL_ODOMETER,
                    system_prompt=WATER_RED_DIGITS_PROMPT,
                )
            except Exception:
                continue
            rr_digits = _normalized_red_digits(
                _extract_red_digits_only(rr),
                min_len=2,
                max_len=3,
            )
            rr_conf = _clamp_confidence(rr.get("confidence", 0.0))
            if rr_digits and rr_conf >= 0.35:
                should_override = (
                    (not best_red)
                    or (len(best_red) < 2)
                    or (rr_conf >= max(0.6, float(conf) - 0.15))
                    or _is_suspicious_water_digits(best)
                )
                if not should_override:
                    continue
                best["red_digits"] = rr_digits
                reading = _reading_from_digits(best_black, rr_digits)
                best["reading"] = reading
                conf = _clamp_confidence(max(conf, min(0.99, rr_conf + 0.05)))
                best_red = rr_digits
                red_note = f"red_refine={rr_digits}@{src_label}"
                break

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

    # Hard safety for water:
    # if final winner is not a strong odometer-digit read, do not return numeric value.
    # This blocks hallucinated large readings from full-frame prompts.
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
    winner_is_strong_odo = _is_strong_water_digits(best)
    if OCR_WATER_DIGIT_FIRST and winner_is_water_family and not winner_is_strong_odo:
        out["type"] = "unknown"
        out["reading"] = None
        out["confidence"] = min(float(out.get("confidence") or 0.0), 0.45)
        base_notes = str(out.get("notes") or "").strip()
        tail = "water_no_strong_odometer_winner"
        out["notes"] = f"{base_notes}; {tail}".strip("; ").strip()
    return out
