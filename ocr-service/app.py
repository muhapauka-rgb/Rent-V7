import os
import base64
import json
import requests
import re
import logging
import threading
from io import BytesIO
from PIL import Image, ImageOps, ImageFilter, ImageEnhance
from typing import Optional, Tuple
from fastapi import FastAPI, UploadFile, File, HTTPException
import numpy as np
import cv2

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OCR_MODEL = os.getenv("OCR_MODEL", "gpt-4o-mini").strip()
OCR_ENABLE_PADDLE = os.getenv("OCR_ENABLE_PADDLE", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_FAST_MODE = os.getenv("OCR_FAST_MODE", "1").strip().lower() in ("1", "true", "yes", "on")
OCR_WATER_SIMPLE_MODE = os.getenv("OCR_WATER_SIMPLE_MODE", "1").strip().lower() in ("1", "true", "yes", "on")

_PADDLE_OCR = None
_PADDLE_INIT_FAILED = False
_PADDLE_PREWARM_STARTED = False

app = FastAPI()
logger = logging.getLogger("ocr-service")

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

WATER_ODOMETER_PROMPT = """Ты — OCR только для окна цифр водяного счётчика.
Верни строго JSON:
{
  "type": "ХВС|ГВС|unknown",
  "black_digits": "<только цифры или null>",
  "red_digits": "<только цифры или null>",
  "reading": <number|null>,
  "serial": <string|null>,
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Чёрные цифры = целая часть.
- Красные цифры = дробная часть (обычно 2-3 знака).
- Если есть black_digits и red_digits, reading = black_digits.red_digits.
- Не используй серийный номер как показание.
- Если не уверен, лучше null.
- Только JSON.
"""

WATER_RED_DIGITS_PROMPT = """Ты видишь ТОЛЬКО красную дробную часть (правые цифры) водяного счётчика.
Верни строго JSON:
{
  "red_digits": "<ровно 3 цифры или null>",
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Возвращай только красные цифры справа.
- Если уверен только в 2 цифрах, всё равно попробуй определить 3-ю; если не получается — null.
- Никаких букв, только цифры.
- Только JSON.
"""

DIGIT_CELL_PROMPT = """Ты видишь одну ячейку с одной цифрой счетчика.
Верни строго JSON:
{
  "digit": "<одна цифра 0-9 или null>",
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Только одна цифра.
- Если не уверен, верни null.
- Только JSON.
"""

SERIAL_ONLY_PROMPT = """Ты OCR-ассистент. Нужно извлечь только серийный номер счётчика.
Верни строго JSON:
{
  "serial": "<строка только цифры или цифры+тире, или null>",
  "confidence": <number>,
  "notes": "<коротко>"
}
Правила:
- Ищи номер рядом с "№", "No", "S/N", "Serial".
- Не путай с показаниями, датами, моделью, ГОСТ.
- Если не уверен — serial=null.
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


def _call_openai_vision(image_bytes: bytes, mime: str, *, system_prompt: Optional[str] = None, user_text: Optional[str] = None) -> dict:
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

    b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:{mime};base64,{b64}"

    payload = {
        "model": OCR_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_prompt or SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": user_text or "Определи тип счётчика и показание. Верни JSON строго по схеме.",
                    },
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ],
    }

    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"openai_http_{r.status_code}: {r.text[:300]}")

    content = r.json()["choices"][0]["message"]["content"]
    return _extract_json_object(content)


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

    # Choose up to 3 variants (speed)
    if img.height > img.width:
        if orient and len(variants) < 3:
            variants.append((orient_label or "rotate90", orient))
        if focused and len(variants) < 3:
            variants.append(("focused_crop", focused))
        if contrast and len(variants) < 3:
            variants.append(("contrast", contrast))
    else:
        if focused and len(variants) < 3:
            variants.append(("focused_crop", focused))
        if orient and len(variants) < 3:
            variants.append((orient_label or "center_crop", orient))
        if contrast and len(variants) < 3:
            variants.append(("contrast", contrast))

    return variants[:3]


def _normalize_digits(value) -> Optional[str]:
    if value is None:
        return None
    d = "".join(ch for ch in str(value) if ch.isdigit())
    return d or None


def _reading_from_digits(black_digits: Optional[str], red_digits: Optional[str]) -> Optional[float]:
    if not black_digits:
        return None
    try:
        if red_digits:
            return float(f"{int(black_digits)}.{red_digits}")
        return float(int(black_digits))
    except Exception:
        return None


def _digits_overlap_serial(black_digits: Optional[str], serial: Optional[str]) -> bool:
    b = _normalize_digits(black_digits)
    s = _normalize_digits(serial)
    if not b or not s:
        return False
    b_nz = b.lstrip("0")
    s_nz = s.lstrip("0")
    if not b_nz or not s_nz:
        return False
    return b_nz in s_nz or s_nz.endswith(b_nz)


def _make_water_digit_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return out

    w, h = img.size
    # 1) Сначала пробуем "умно" найти окно барабана по красной зоне.
    # Это снижает риск чтения серийника вместо показаний.
    smart_variants: list[tuple[str, bytes]] = _make_water_smart_window_variants(img_bytes)
    for sv in smart_variants:
        out.append(sv)

    # Кропы верхней средней части, где чаще всего окно с цифрами у воды.
    boxes = [
        (int(w * 0.22), int(h * 0.28), int(w * 0.86), int(h * 0.52)),
        (int(w * 0.18), int(h * 0.24), int(w * 0.90), int(h * 0.50)),
        (int(w * 0.26), int(h * 0.30), int(w * 0.84), int(h * 0.56)),
    ]
    for idx, (x1, y1, x2, y2) in enumerate(boxes, start=1):
        x1 = max(0, min(x1, w - 1))
        y1 = max(0, min(y1, h - 1))
        x2 = max(x1 + 1, min(x2, w))
        y2 = max(y1 + 1, min(y2, h))
        crop = img.crop((x1, y1, x2, y2))
        crop = crop.resize((max(1, crop.width * 4), max(1, crop.height * 4)))
        sharp = ImageEnhance.Contrast(crop).enhance(1.8).filter(
            ImageFilter.UnsharpMask(radius=1, percent=240, threshold=2)
        )
        bw = ImageOps.autocontrast(sharp.convert("L")).point(lambda p: 255 if p > 150 else 0, "L").convert("RGB")
        out.append((f"odo_strip_{idx}", _encode_jpeg(sharp, quality=95)))
        out.append((f"odo_strip_bw_{idx}", _encode_jpeg(bw, quality=95)))
    # Удаляем дубликаты по байтам и ограничиваем количество вариантов.
    seen = set()
    deduped: list[tuple[str, bytes]] = []
    for label, data in out:
        key = hash(data)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((label, data))
    return deduped[:8]


def _score_odometer_roi(gray_roi: np.ndarray) -> float:
    """
    Скоринг кандидата окна барабана:
    - достаточное число вертикальных "цифровых" компонент
    - наличие красной зоны справа (проверяется выше), здесь только форма/структура.
    """
    try:
        if gray_roi is None or gray_roi.size == 0:
            return -1.0
        h, w = gray_roi.shape[:2]
        if h < 18 or w < 50:
            return -1.0
        blur = cv2.GaussianBlur(gray_roi, (3, 3), 0)
        _, bw = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        n, _, stats, _ = cv2.connectedComponentsWithStats(bw, connectivity=8)
        digit_like = 0
        for i in range(1, n):
            x, y, cw, ch, area = stats[i]
            if area < 14:
                continue
            if ch < int(0.28 * h) or ch > int(0.98 * h):
                continue
            ratio = cw / float(max(1, ch))
            if 0.15 <= ratio <= 1.15:
                digit_like += 1
        edge = cv2.Canny(blur, 70, 170)
        edge_density = float(np.count_nonzero(edge)) / float(max(1, h * w))
        score = float(digit_like) + min(1.0, edge_density * 8.0)
        return score
    except Exception:
        return -1.0


def _make_water_smart_window_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Пытается локализовать окно показаний воды по красным цифрам справа
    и формирует 1-2 качественных кропа только окна барабана.
    """
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        bgr0 = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if bgr0 is None:
            return []
    except Exception:
        return []

    h0, w0 = bgr0.shape[:2]
    scale = 1.0
    max_side = max(w0, h0)
    if max_side > 1500:
        scale = 1500.0 / float(max_side)
        bgr = cv2.resize(bgr0, (int(w0 * scale), int(h0 * scale)), interpolation=cv2.INTER_AREA)
    else:
        bgr = bgr0

    hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
    m1 = cv2.inRange(hsv, (0, 30, 25), (14, 255, 255))
    m2 = cv2.inRange(hsv, (158, 30, 25), (179, 255, 255))
    red_mask = cv2.bitwise_or(m1, m2)
    kernel = np.ones((3, 3), np.uint8)
    red_mask = cv2.morphologyEx(red_mask, cv2.MORPH_OPEN, kernel, iterations=1)
    red_mask = cv2.morphologyEx(red_mask, cv2.MORPH_CLOSE, kernel, iterations=2)

    contours, _ = cv2.findContours(red_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return []

    h, w = bgr.shape[:2]
    candidates: list[tuple[float, tuple[int, int, int, int]]] = []
    for c in contours:
        x, y, cw, ch = cv2.boundingRect(c)
        if cw < 6 or ch < 6:
            continue
        area = cw * ch
        if area < 50 or area > int(0.08 * w * h):
            continue
        ar = cw / float(max(1, ch))
        if ar < 0.30 or ar > 5.0:
            continue

        # Красная область обычно справа в окне барабана.
        for lf in (1.6, 2.0, 2.4):
            wx1 = max(0, x - int(cw * lf))
            wx2 = min(w - 1, x + int(cw * 1.35))
            wy1 = max(0, y - int(ch * 1.0))
            wy2 = min(h - 1, y + int(ch * 1.20))
            if wx2 - wx1 < 55 or wy2 - wy1 < 18:
                continue
            roi_gray = cv2.cvtColor(bgr[wy1 : wy2 + 1, wx1 : wx2 + 1], cv2.COLOR_BGR2GRAY)
            sc = _score_odometer_roi(roi_gray)
            if sc >= 3.2:
                candidates.append((sc, (wx1, wy1, wx2, wy2)))

    if not candidates:
        return []

    # Лучшие 2 разных кандидата.
    candidates.sort(key=lambda x: x[0], reverse=True)
    picked: list[tuple[float, tuple[int, int, int, int]]] = []
    for sc, box in candidates:
        if len(picked) >= 2:
            break
        bx1, by1, bx2, by2 = box
        too_close = False
        for _, pb in picked:
            px1, py1, px2, py2 = pb
            ix1, iy1 = max(bx1, px1), max(by1, py1)
            ix2, iy2 = min(bx2, px2), min(by2, py2)
            iw, ih = max(0, ix2 - ix1 + 1), max(0, iy2 - iy1 + 1)
            inter = iw * ih
            a1 = max(1, (bx2 - bx1 + 1) * (by2 - by1 + 1))
            a2 = max(1, (px2 - px1 + 1) * (py2 - py1 + 1))
            iou = inter / float(a1 + a2 - inter)
            if iou > 0.65:
                too_close = True
                break
        if not too_close:
            picked.append((sc, box))

    if not picked:
        return []

    try:
        pil = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return []
    pw, ph = pil.size
    inv = 1.0 / scale

    out: list[tuple[str, bytes]] = []
    for idx, (_, (x1, y1, x2, y2)) in enumerate(picked, start=1):
        ox1 = max(0, min(pw - 1, int(x1 * inv)))
        oy1 = max(0, min(ph - 1, int(y1 * inv)))
        ox2 = max(ox1 + 1, min(pw, int((x2 + 1) * inv)))
        oy2 = max(oy1 + 1, min(ph, int((y2 + 1) * inv)))
        crop = pil.crop((ox1, oy1, ox2, oy2))
        resample = getattr(Image, "Resampling", Image).LANCZOS
        crop = crop.resize((max(1, crop.width * 4), max(1, crop.height * 4)), resample)
        sharp = ImageEnhance.Contrast(crop).enhance(1.95).filter(
            ImageFilter.UnsharpMask(radius=1, percent=280, threshold=2)
        )
        bw = ImageOps.autocontrast(sharp.convert("L")).point(lambda p: 255 if p > 148 else 0, "L").convert("RGB")
        out.append((f"smart_odo_{idx}", _encode_jpeg(sharp, quality=95)))
        out.append((f"smart_odo_bw_{idx}", _encode_jpeg(bw, quality=95)))
    return out


def _detect_serial_bbox_paddle(img_bytes: bytes) -> Optional[tuple[int, int, int, int, str, float]]:
    """
    Возвращает bbox серийника (в координатах исходного изображения) и его digits.
    Используется как якорь для построения кропа окна барабана.
    """
    ocr = _get_paddle_ocr()
    if ocr is None:
        return None
    try:
        arr = np.array(Image.open(BytesIO(img_bytes)).convert("RGB"))
        res = ocr.ocr(arr, cls=False)
    except Exception:
        return None

    best = None
    best_score = -1e9
    lines = res[0] if isinstance(res, list) and res else []
    for line in (lines or []):
        try:
            box = line[0]
            txt = str(line[1][0] or "")
            conf = float(line[1][1] or 0.0)
        except Exception:
            continue
        digits = _normalize_digits(txt)
        if not digits:
            continue
        # Серийники воды обычно 7..12 цифр.
        if len(digits) < 7 or len(digits) > 12:
            continue
        try:
            pts = np.array(box, dtype=float)
            x1 = int(max(0.0, float(pts[:, 0].min())))
            y1 = int(max(0.0, float(pts[:, 1].min())))
            x2 = int(max(0.0, float(pts[:, 0].max())))
            y2 = int(max(0.0, float(pts[:, 1].max())))
        except Exception:
            continue
        bw = max(1, x2 - x1)
        bh = max(1, y2 - y1)
        if bw < 24 or bh < 10:
            continue
        horiz_bonus = 0.06 if bw >= int(2.2 * bh) else 0.0
        score = float(conf) + horiz_bonus + min(0.08, 0.02 * max(0, len(digits) - 7))
        if score > best_score:
            best_score = score
            best = (x1, y1, x2, y2, digits, float(conf))
    return best


def _make_water_serial_anchor_variants(img_bytes: bytes) -> list[tuple[str, bytes]]:
    """
    Строит кропы окна барабана относительно bbox серийника.
    Это уменьшает вероятность чтения серийника как показания.
    """
    anchor = _detect_serial_bbox_paddle(img_bytes)
    if not anchor:
        return []
    x1, y1, x2, y2, _, _ = anchor
    try:
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
    except Exception:
        return []
    w, h = img.size
    sw = max(1, x2 - x1)
    sh = max(1, y2 - y1)

    # Геометрия подобрана для типовых водосчетчиков:
    # окно барабана расположено выше серийника и немного левее.
    cfgs = [
        (-1.25, +0.55, -0.20, +2.45),
        (-1.05, +0.50, -0.16, +2.30),
        (-1.45, +0.72, -0.28, +2.75),
    ]

    out: list[tuple[str, bytes]] = []
    resample = getattr(Image, "Resampling", Image).LANCZOS
    for idx, (lx, rx, ty, by) in enumerate(cfgs, start=1):
        cx1 = max(0, min(w - 2, int(x1 + lx * sw)))
        cx2 = max(cx1 + 1, min(w, int(x2 + rx * sw)))
        cy1 = max(0, min(h - 2, int(y1 + ty * sh)))
        cy2 = max(cy1 + 1, min(h, int(y2 + by * sh)))
        if (cx2 - cx1) < 60 or (cy2 - cy1) < 24:
            continue
        crop = img.crop((cx1, cy1, cx2, cy2))
        crop = crop.resize((max(1, crop.width * 4), max(1, crop.height * 4)), resample)
        sharp = ImageEnhance.Contrast(crop).enhance(1.9).filter(
            ImageFilter.UnsharpMask(radius=1, percent=260, threshold=2)
        )
        bw = ImageOps.autocontrast(sharp.convert("L")).point(lambda p: 255 if p > 150 else 0, "L").convert("RGB")
        out.append((f"serial_odo_{idx}", _encode_jpeg(sharp, quality=95)))
        out.append((f"serial_odo_bw_{idx}", _encode_jpeg(bw, quality=95)))

    # dedupe by content hash
    seen = set()
    deduped: list[tuple[str, bytes]] = []
    for label, data in out:
        key = hash(data)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((label, data))
    return deduped[:4]


def _get_paddle_ocr():
    global _PADDLE_OCR, _PADDLE_INIT_FAILED
    if _PADDLE_OCR is not None or _PADDLE_INIT_FAILED:
        return _PADDLE_OCR
    try:
        from paddleocr import PaddleOCR
        _PADDLE_OCR = PaddleOCR(use_angle_cls=False, lang="en", show_log=False)
    except Exception:
        _PADDLE_INIT_FAILED = True
        _PADDLE_OCR = None
    return _PADDLE_OCR


def _prewarm_paddle_if_enabled() -> None:
    global _PADDLE_PREWARM_STARTED
    if _PADDLE_PREWARM_STARTED:
        return
    _PADDLE_PREWARM_STARTED = True
    if not OCR_ENABLE_PADDLE:
        return

    def _run() -> None:
        try:
            logger.info("Paddle prewarm: start")
            _get_paddle_ocr()
            logger.info("Paddle prewarm: done")
        except Exception as e:
            logger.warning("Paddle prewarm failed: %s", e)

    threading.Thread(target=_run, daemon=True).start()


@app.on_event("startup")
async def _startup_prewarm() -> None:
    _prewarm_paddle_if_enabled()


def _paddle_water_candidates(image_bytes: bytes) -> list[dict]:
    ocr = _get_paddle_ocr()
    if ocr is None:
        return []
    try:
        img = Image.open(BytesIO(image_bytes)).convert("RGB")
        arr = np.array(img)
        result = ocr.ocr(arr, cls=False)
    except Exception:
        return []

    out: list[dict] = []
    lines = result[0] if isinstance(result, list) and result else []
    for item in lines or []:
        try:
            text = str(item[1][0] or "")
            conf = float(item[1][1] or 0.0)
        except Exception:
            continue
        digits = re.sub(r"[^0-9]", "", text)
        if len(digits) < 4:
            continue
        # Water main mode: last 3 digits are fractional (liters)
        b3 = digits[:-3]
        r3 = digits[-3:]
        if b3:
            try:
                v3 = float(f"{int(b3)}.{r3}")
            except Exception:
                v3 = None
            if v3 is not None:
                out.append(
                    {
                        "black_digits": b3,
                        "red_digits": r3,
                        "reading": v3,
                        "confidence": max(0.0, min(1.0, conf)),
                        "raw_text": text,
                        "raw_digits": digits,
                    }
                )
    return out


def _make_red_zone_variants(strip_jpeg: bytes) -> list[bytes]:
    out: list[bytes] = []
    try:
        img = Image.open(BytesIO(strip_jpeg)).convert("RGB")
    except Exception:
        return out
    w, h = img.size
    # Важно: исключаем верх (серийник) и низ (циферблат), берем узкую полосу барабана.
    boxes = [
        (int(w * 0.66), int(h * 0.32), int(w * 0.99), int(h * 0.66)),
        (int(w * 0.62), int(h * 0.28), int(w * 0.98), int(h * 0.68)),
        (int(w * 0.70), int(h * 0.30), int(w * 0.995), int(h * 0.64)),
        (int(w * 0.58), int(h * 0.30), int(w * 0.995), int(h * 0.70)),
    ]
    for (x1, y1, x2, y2) in boxes:
        crop = img.crop((max(0, x1), max(0, y1), min(w, x2), min(h, y2)))
        crop = crop.resize((max(1, crop.width * 5), max(1, crop.height * 5)))
        c = ImageEnhance.Contrast(crop).enhance(2.2).filter(
            ImageFilter.UnsharpMask(radius=1, percent=260, threshold=2)
        )
        out.append(_encode_jpeg(c, quality=95))
        hi = ImageEnhance.Sharpness(c).enhance(1.8)
        out.append(_encode_jpeg(hi, quality=95))
        # Вариант с бинаризацией часто лучше для LLM по барабанам.
        bw = ImageOps.autocontrast(c.convert("L")).point(lambda p: 255 if p > 150 else 0, "L").convert("RGB")
        out.append(_encode_jpeg(bw, quality=95))
    return out[:6]


def _segment_red_cells_from_strip(strip_jpeg: bytes) -> list[list[bytes]]:
    """
    Возвращает несколько вариантов сегментации 3 красных ячеек.
    Это важно для случаев, когда первый красный разряд частично "съеден" кропом.
    """
    out: list[list[bytes]] = []
    try:
        img = Image.open(BytesIO(strip_jpeg)).convert("RGB")
    except Exception:
        return out
    w, h = img.size
    band = img.crop((int(w * 0.03), int(h * 0.14), int(w * 0.98), int(h * 0.90)))
    bw, bh = band.size
    # Набор стартов/ширин для красной зоны: берем немного левее, чем раньше.
    configs = [
        (0.56, 0.42),
        (0.60, 0.40),
        (0.62, 0.38),
        (0.64, 0.36),
        (0.66, 0.34),
    ]
    for start_ratio, width_ratio in configs:
        x1 = int(bw * start_ratio)
        x2 = int(bw * min(0.995, start_ratio + width_ratio))
        if x2 - x1 < 12:
            continue
        red_zone = band.crop((x1, 0, x2, bh))
        rw, rh = red_zone.size
        cell_w = max(1, rw // 3)
        variant_cells: list[bytes] = []
        for i in range(3):
            # Небольшой overlap между ячейками, чтобы не терять цифры на границе.
            left = max(0, i * cell_w - int(cell_w * 0.12))
            right = rw if i == 2 else min(rw, (i + 1) * cell_w + int(cell_w * 0.12))
            cell = red_zone.crop((left, 0, right, rh))
            cell = cell.resize((max(1, cell.width * 5), max(1, cell.height * 5)))
            cell = ImageEnhance.Contrast(cell).enhance(2.2).filter(
                ImageFilter.UnsharpMask(radius=1, percent=280, threshold=2)
            )
            # Доп. вариант с бинаризацией внутри той же ячейки.
            bw_cell = ImageOps.autocontrast(cell.convert("L")).point(
                lambda p: 255 if p > 142 else 0, "L"
            ).convert("RGB")
            # Складываем сначала "естественный", потом бинаризованный — читаем лучший.
            variant_cells.append(_encode_jpeg(cell, quality=95))
            variant_cells.append(_encode_jpeg(bw_cell, quality=95))
        if variant_cells:
            out.append(variant_cells)
    return out


def _read_single_digit(cell_jpeg: bytes) -> tuple[Optional[str], float]:
    try:
        r = _call_openai_vision(
            cell_jpeg,
            mime="image/jpeg",
            system_prompt=DIGIT_CELL_PROMPT,
            user_text="Определи цифру в ячейке.",
        )
    except Exception:
        return None, 0.0
    d = str(r.get("digit") or "").strip()
    if len(d) == 1 and d.isdigit():
        return d, _clamp_confidence(r.get("confidence", 0.0))
    return None, _clamp_confidence(r.get("confidence", 0.0))


def _read_single_digit_paddle(cell_jpeg: bytes) -> tuple[Optional[str], float]:
    ocr = _get_paddle_ocr()
    if ocr is None:
        return None, 0.0
    try:
        img = Image.open(BytesIO(cell_jpeg)).convert("RGB")
        arr = np.array(img)
        result = ocr.ocr(arr, cls=False)
    except Exception:
        return None, 0.0
    lines = result[0] if isinstance(result, list) and result else []
    best_digit = None
    best_conf = 0.0
    for item in lines or []:
        try:
            text = str(item[1][0] or "")
            conf = float(item[1][1] or 0.0)
        except Exception:
            continue
        digits = re.sub(r"[^0-9]", "", text)
        if not digits:
            continue
        d = digits[0]
        if conf > best_conf:
            best_digit = d
            best_conf = conf
    return best_digit, _clamp_confidence(best_conf)


def _read_multi_digits_paddle(img_jpeg: bytes) -> tuple[Optional[str], float]:
    """
    Читает несколько цифр подряд из одного ROI (например, красная зона).
    Возвращает самую уверенную цифровую строку.
    """
    ocr = _get_paddle_ocr()
    if ocr is None:
        return None, 0.0
    try:
        img = Image.open(BytesIO(img_jpeg)).convert("RGB")
        arr = np.array(img)
        result = ocr.ocr(arr, cls=False)
    except Exception:
        return None, 0.0
    lines = result[0] if isinstance(result, list) and result else []
    best_digits = None
    best_conf = 0.0
    for item in lines or []:
        try:
            text = str(item[1][0] or "")
            conf = float(item[1][1] or 0.0)
        except Exception:
            continue
        digits = re.sub(r"[^0-9]", "", text)
        if len(digits) < 2:
            continue
        if conf > best_conf:
            best_digits = digits
            best_conf = conf
    return best_digits, _clamp_confidence(best_conf)


def _read_serial_fallback(image_bytes: bytes, mime: str) -> tuple[Optional[str], float]:
    try:
        r = _call_openai_vision(
            image_bytes,
            mime=mime,
            system_prompt=SERIAL_ONLY_PROMPT,
            user_text="Определи только серийный номер счётчика.",
        )
    except Exception:
        return None, 0.0
    serial = r.get("serial")
    if isinstance(serial, str):
        serial = serial.strip()
    else:
        serial = None
    serial = serial or None
    conf = _clamp_confidence(r.get("confidence", 0.0))
    if not serial:
        return None, conf
    # Валидация: разрешаем только цифры и тире.
    if not re.fullmatch(r"[0-9-]{4,32}", serial):
        return None, min(conf, 0.4)
    return serial, conf


def _classify_digit_template(cell_jpeg: bytes) -> tuple[Optional[str], float]:
    """
    Легкий детерминированный классификатор цифры:
    - бинаризуем ячейку
    - сравниваем с синтетическими шаблонами 0..9 (несколько шрифтов/thickness)
    """
    try:
        arr = np.frombuffer(cell_jpeg, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
        if img is None:
            return None, 0.0
        img = cv2.GaussianBlur(img, (3, 3), 0)
        img = cv2.equalizeHist(img)
        # Цифры темные на светлом фоне -> инвертируем в "белая цифра на черном"
        _, bw = cv2.threshold(img, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        h, w = bw.shape[:2]
        if h < 8 or w < 8:
            return None, 0.0
        # Берем bbox активных пикселей, чтобы убрать пустые поля.
        ys, xs = np.where(bw > 0)
        if len(xs) < 10:
            return None, 0.0
        x1, x2 = max(0, xs.min() - 2), min(w - 1, xs.max() + 2)
        y1, y2 = max(0, ys.min() - 2), min(h - 1, ys.max() + 2)
        roi = bw[y1 : y2 + 1, x1 : x2 + 1]
        roi = cv2.resize(roi, (40, 64), interpolation=cv2.INTER_CUBIC)
    except Exception:
        return None, 0.0

    fonts = [cv2.FONT_HERSHEY_SIMPLEX, cv2.FONT_HERSHEY_DUPLEX, cv2.FONT_HERSHEY_TRIPLEX]
    best_d = None
    best_s = -1.0
    for d in range(10):
        dch = str(d)
        for font in fonts:
            for thickness in (2, 3, 4):
                tpl = np.zeros((64, 40), dtype=np.uint8)
                ((tw, th), _) = cv2.getTextSize(dch, font, 1.5, thickness)
                ox = max(0, (40 - tw) // 2)
                oy = max(th + 2, (64 + th) // 2)
                cv2.putText(tpl, dch, (ox, oy), font, 1.5, 255, thickness, cv2.LINE_AA)
                # Нормированная корреляция
                score = float(cv2.matchTemplate(roi, tpl, cv2.TM_CCOEFF_NORMED)[0][0])
                if score > best_s:
                    best_s = score
                    best_d = dch
    if best_d is None:
        return None, 0.0
    # Преобразуем в "confidence" 0..1
    conf = max(0.0, min(1.0, (best_s + 1.0) / 2.0))
    # Жестко отсечем совсем плохие совпадения
    if conf < 0.52:
        return None, conf
    return best_d, conf


def _refine_red_digits_from_strip(strip_jpeg: bytes) -> tuple[Optional[str], float]:
    variants = _segment_red_cells_from_strip(strip_jpeg)
    if not variants:
        return None, 0.0

    best_digits = None
    best_score = -1.0

    for v in variants:
        # v содержит 6 изображений: по 2 на каждую из 3 позиций
        if len(v) < 6:
            continue
        digits: list[str] = []
        confs: list[float] = []
        valid = True
        for i in range(0, 6, 2):
            options = [v[i], v[i + 1]]
            opt_best_d = None
            opt_best_c = -1.0
            for opt in options:
                # Критично для latency: на ячейках используем только локальный Paddle,
                # без дополнительных внешних LLM-вызовов.
                dp, cp = _read_single_digit_paddle(opt)
                dt, ct = _classify_digit_template(opt)
                if ct > cp and dt is not None:
                    d, c = dt, ct
                else:
                    d, c = dp, cp
                if d is not None and c > opt_best_c:
                    opt_best_d = d
                    opt_best_c = c
            if opt_best_d is None:
                valid = False
                break
            digits.append(opt_best_d)
            confs.append(max(0.0, opt_best_c))
        if not valid or len(digits) != 3:
            continue
        mean_conf = sum(confs) / max(1, len(confs))
        # Легкий бонус за "неплоский" набор цифр (реже бывает 000/111 и т.п.).
        diversity_bonus = 0.04 if len(set(digits)) >= 2 else 0.0
        score = mean_conf + diversity_bonus
        if score > best_score:
            best_score = score
            best_digits = "".join(digits)

    if best_digits is None:
        return None, 0.0
    return best_digits, max(0.0, min(1.0, best_score))


def _refine_red_digits_color_guided(strip_jpeg: bytes) -> tuple[Optional[str], float]:
    """
    Цвето-ориентированное уточнение дробной части:
    - ищем красные пиксели (зона красных барабанов),
    - делим найденную область на 3 ячейки,
    - читаем каждую ячейку (Paddle + template).
    """
    try:
        arr = np.frombuffer(strip_jpeg, dtype=np.uint8)
        bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if bgr is None:
            return None, 0.0
        # Увеличение для стабильности маски/классификации.
        bgr = cv2.resize(bgr, None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC)
        h, w = bgr.shape[:2]
        if h < 30 or w < 30:
            return None, 0.0
    except Exception:
        return None, 0.0

    # Ограничиваемся верхней средней полосой, где обычно находится барабан,
    # чтобы не цеплять красные логотипы/индикаторы ниже.
    yb1 = int(0.16 * h)
    yb2 = int(0.62 * h)
    xb1 = int(0.32 * w)
    xb2 = int(0.98 * w)
    roi = bgr[yb1:yb2, xb1:xb2]
    if roi.size == 0:
        return None, 0.0
    rh, rw = roi.shape[:2]

    hsv = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
    m1 = cv2.inRange(hsv, (0, 30, 25), (14, 255, 255))
    m2 = cv2.inRange(hsv, (158, 30, 25), (179, 255, 255))
    m3 = cv2.inRange(hsv, (0, 12, 18), (24, 255, 255))
    mask = cv2.bitwise_or(cv2.bitwise_or(m1, m2), m3)
    # LAB fallback для бледных красных цифр.
    lab = cv2.cvtColor(roi, cv2.COLOR_BGR2LAB)
    a_ch = lab[:, :, 1]
    _, a_mask = cv2.threshold(a_ch, 142, 255, cv2.THRESH_BINARY)
    mask = cv2.bitwise_or(mask, a_mask)
    kernel = np.ones((3, 3), np.uint8)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=1)
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)

    col_sum = mask.sum(axis=0) / 255.0
    # Отбираем осмысленные красные колонки.
    active_cols = np.where(col_sum > max(3.0, 0.03 * rh))[0]
    if active_cols.size >= 8:
        rx1, rx2 = int(active_cols.min()), int(active_cols.max())
        span = max(1, rx2 - rx1 + 1)
        # Расширяем влево/вправо, чтобы не потерять частично видимый первый красный разряд.
        x1 = max(0, rx1 - int(span * 0.28))
        x2 = min(rw - 1, rx2 + int(span * 0.15))
        # Если область подозрительно узкая — применяем минимальную ширину.
        min_w = int(0.22 * rw)
        if x2 - x1 + 1 < min_w:
            mid = (x1 + x2) // 2
            half = min_w // 2
            x1 = max(0, mid - half)
            x2 = min(rw - 1, x1 + min_w - 1)
    else:
        # Маска неуверенная -> берем типичную правую треть окна одометра.
        x1 = int(0.70 * rw)
        x2 = int(0.985 * rw)
    if x2 - x1 < 18:
        return None, 0.0

    y1 = int(0.02 * rh)
    y2 = int(0.96 * rh)
    zone = roi[y1:y2, x1 : x2 + 1]
    zh, zw = zone.shape[:2]
    if zh < 10 or zw < 18:
        return None, 0.0

    # Дополнительно пытаемся прочитать всю красную зону как цельную строку.
    seq_digits = None
    seq_conf = 0.0
    try:
        ok_z, enc_z = cv2.imencode(".jpg", zone, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
        if ok_z:
            seq_raw, seq_c = _read_multi_digits_paddle(enc_z.tobytes())
            if seq_raw and len(seq_raw) >= 3:
                seq_digits = seq_raw[-3:]
                seq_conf = seq_c
    except Exception:
        seq_digits = None
        seq_conf = 0.0

    cell_w = max(1, zw // 3)
    digits: list[str] = []
    confs: list[float] = []
    for i in range(3):
        lx = max(0, i * cell_w - int(cell_w * 0.14))
        rx = zw if i == 2 else min(zw, (i + 1) * cell_w + int(cell_w * 0.14))
        if rx - lx < 8:
            return None, 0.0
        cell = zone[:, lx:rx]
        gray = cv2.cvtColor(cell, cv2.COLOR_BGR2GRAY)
        gray = cv2.equalizeHist(gray)
        # Два варианта ячейки: "мягкий" и бинаризованный.
        soft = cv2.GaussianBlur(gray, (3, 3), 0)
        _, bw = cv2.threshold(soft, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        opts = []
        for imgv in (soft, bw):
            rgb = cv2.cvtColor(imgv, cv2.COLOR_GRAY2RGB)
            ok, enc = cv2.imencode(".jpg", rgb, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
            if ok:
                opts.append(enc.tobytes())
        if not opts:
            return None, 0.0

        best_d = None
        best_c = -1.0
        for opt in opts:
            dp, cp = _read_single_digit_paddle(opt)
            dt, ct = _classify_digit_template(opt)
            if ct > cp and dt is not None:
                d, c = dt, ct
            else:
                d, c = dp, cp
            if d is not None and c > best_c:
                best_d, best_c = d, c
        if best_d is None:
            return None, 0.0
        digits.append(best_d)
        confs.append(max(0.0, best_c))

    if len(digits) != 3:
        if seq_digits and len(seq_digits) == 3:
            return seq_digits, max(0.0, min(1.0, seq_conf))
        return None, 0.0
    mean_conf = sum(confs) / 3.0
    diversity_bonus = 0.04 if len(set(digits)) >= 2 else 0.0
    cell_digits = "".join(digits)
    cell_conf = max(0.0, min(1.0, mean_conf + diversity_bonus))
    # Если последовательный OCR по всей зоне значительно увереннее — берем его.
    if seq_digits and len(seq_digits) == 3 and seq_conf >= (cell_conf + 0.06):
        return seq_digits, max(0.0, min(1.0, seq_conf))
    return cell_digits, cell_conf


def _refine_red_digits_llm(strip_jpeg: bytes) -> tuple[Optional[str], float]:
    """
    Медленный, но точный fallback:
    отдельный LLM-запрос только по красной зоне дробной части.
    """
    variants = _make_red_zone_variants(strip_jpeg)
    if not variants:
        return None, 0.0
    best_digits = None
    best_conf = 0.0
    for v in variants[:2]:
        try:
            r = _call_openai_vision(
                v,
                mime="image/jpeg",
                system_prompt=WATER_RED_DIGITS_PROMPT,
                user_text="Считай только 3 красные цифры дробной части.",
            )
        except Exception:
            continue
        rd = _normalize_digits(r.get("red_digits"))
        if rd and len(rd) >= 3:
            rd = rd[:3]
        else:
            rd = None
        cf = _clamp_confidence(r.get("confidence", 0.0))
        if rd and cf > best_conf:
            best_digits = rd
            best_conf = cf
    return best_digits, best_conf


def _refine_red_digits_positional(strip_jpeg: bytes) -> tuple[Optional[str], float]:
    """
    Детерминированный fallback без цвета:
    берем правую часть окна барабана и читаем 3 ячейки как дробь.
    """
    try:
        arr = np.frombuffer(strip_jpeg, dtype=np.uint8)
        bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if bgr is None:
            return None, 0.0
        bgr = cv2.resize(bgr, None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC)
        h, w = bgr.shape[:2]
        if h < 30 or w < 60:
            return None, 0.0
    except Exception:
        return None, 0.0

    # Типовая геометрия дробной зоны в правой части окна.
    x1 = int(0.70 * w)
    x2 = int(0.985 * w)
    y1 = int(0.16 * h)
    y2 = int(0.80 * h)
    zone = bgr[y1:y2, x1:x2]
    zh, zw = zone.shape[:2]
    if zh < 12 or zw < 18:
        return None, 0.0

    cell_w = max(1, zw // 3)
    digits: list[str] = []
    confs: list[float] = []
    for i in range(3):
        lx = max(0, i * cell_w - int(cell_w * 0.12))
        rx = zw if i == 2 else min(zw, (i + 1) * cell_w + int(cell_w * 0.12))
        if rx - lx < 8:
            return None, 0.0
        cell = zone[:, lx:rx]
        gray = cv2.cvtColor(cell, cv2.COLOR_BGR2GRAY)
        gray = cv2.equalizeHist(gray)
        soft = cv2.GaussianBlur(gray, (3, 3), 0)
        _, bw = cv2.threshold(soft, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        opts = []
        for imgv in (soft, bw):
            rgb = cv2.cvtColor(imgv, cv2.COLOR_GRAY2RGB)
            ok, enc = cv2.imencode(".jpg", rgb, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
            if ok:
                opts.append(enc.tobytes())
        if not opts:
            return None, 0.0
        best_d = None
        best_c = -1.0
        for opt in opts:
            dt, ct = _classify_digit_template(opt)
            if dt is not None and ct > best_c:
                best_d, best_c = dt, ct
        if best_d is None:
            return None, 0.0
        digits.append(best_d)
        confs.append(best_c)

    if len(digits) != 3:
        return None, 0.0
    mean_conf = sum(confs) / 3.0
    return "".join(digits), max(0.0, min(1.0, mean_conf))


def _pick_best_candidate(candidates: list[dict]) -> tuple[dict, float]:
    best = None
    best_score = -1e9
    for c in candidates:
        t = str(c.get("type") or "unknown")
        reading = c.get("reading")
        conf = float(c.get("confidence") or 0.0)
        provider = str(c.get("provider") or "")
        serial = c.get("serial")
        black_digits = c.get("black_digits")
        red_digits = c.get("red_digits")

        score = conf
        if reading is not None:
            score += 0.20
        if t != "unknown":
            score += 0.04
        if provider == "water_digit":
            score += 0.20
            if black_digits and len(str(black_digits)) >= 3:
                score += 0.12
            if red_digits and len(str(red_digits)) == 3:
                score += 0.10
            elif red_digits and len(str(red_digits)) == 2:
                # Для воды запрещаем финальный выбор по 2 знакам дроби.
                score -= 1.20
            if _digits_overlap_serial(black_digits, serial):
                score -= 0.70
        if provider == "water_serial_anchor":
            score += 0.26
            if black_digits and len(str(black_digits)) >= 3:
                score += 0.12
            if red_digits and len(str(red_digits)) == 3:
                score += 0.12
            elif red_digits:
                # Для воды без 3-х знаков дроби кандидат считаем слабым.
                score -= 1.10
            if _digits_overlap_serial(black_digits, serial):
                score -= 0.85
        if provider == "paddle_seq":
            score += 0.26
            if black_digits and len(str(black_digits)) >= 3:
                score += 0.12
            if red_digits and len(str(red_digits)) == 3:
                score += 0.10
            elif red_digits and len(str(red_digits)) == 2:
                score -= 1.20
            raw_digits = _normalize_digits(c.get("raw_digits"))
            # Частая ошибка: Paddle берет серийник (обычно 8+ цифр без ведущих нулей)
            # вместо барабана показаний. Сильно штрафуем такие кейсы.
            if raw_digits and len(raw_digits) >= 8 and not raw_digits.startswith("00"):
                score -= 1.85
            if raw_digits and len(raw_digits) >= 9:
                score -= 0.60
            if black_digits and len(str(black_digits)) >= 5 and red_digits and len(str(red_digits)) == 3:
                score -= 0.35
            if _digits_overlap_serial(black_digits, serial):
                # Жесткое правило: если чтение совпадает с серийником — кандидат невалиден.
                score = -1e9
            s_norm = _normalize_digits(serial)
            b_norm = _normalize_digits(black_digits)
            if s_norm and b_norm and len(s_norm) >= 7:
                # Доп. защита: paddle часто отрезает серийник как XXXX.YYY
                # (первые 3-4 + последние 3). Такой кандидат дисквалифицируем.
                if s_norm.startswith(b_norm) or s_norm.endswith(b_norm):
                    score = -1e9
        if reading is not None and reading <= 0:
            score -= 0.40

        c["_score"] = score

        if score > best_score:
            best = c
            best_score = score

    if best is None:
        raise HTTPException(status_code=500, detail="openai_empty_response")

    # heuristic: if winner is "too small with leading zeros", but there is a comparable
    # high-confidence candidate with much larger reading, prefer the larger one.
    if str(best.get("provider") or "") == "water_digit" and best.get("reading") is not None:
        bdigits = _normalize_digits(best.get("black_digits"))
        best_reading = float(best.get("reading"))
        best_conf = float(best.get("confidence") or 0.0)
        if bdigits and bdigits.startswith("00") and best_reading < 300.0:
            larger = []
            for c in candidates:
                if str(c.get("provider") or "") != "water_digit":
                    continue
                rv = c.get("reading")
                if rv is None:
                    continue
                try:
                    r = float(rv)
                except Exception:
                    continue
                cf = float(c.get("confidence") or 0.0)
                if cf >= max(0.80, best_conf - 0.08) and r >= best_reading * 3.0 and r <= 5000.0:
                    larger.append((cf, r, c))
            if larger:
                larger.sort(key=lambda x: (x[0], x[1]), reverse=True)
                best = larger[0][2]
                best_score = larger[0][0]

    # Для воды предпочитаем water_digit, если paddle_seq победил с близким score:
    # это уменьшает ложный выбор серийника.
    if str(best.get("provider") or "") == "paddle_seq":
        wd = []
        for c in candidates:
            if str(c.get("provider") or "") != "water_digit":
                continue
            if c.get("reading") is None:
                continue
            rd = _normalize_digits(c.get("red_digits"))
            if not rd or len(rd) != 3:
                continue
            wd.append((float(c.get("_score") or -1e9), c))
        if wd:
            wd.sort(key=lambda x: x[0], reverse=True)
            alt_score, alt = wd[0]
            if alt_score >= (best_score - 0.25):
                best = alt
                best_score = alt_score

    return best, best_score


@app.post("/recognize")
async def recognize(file: UploadFile = File(...)):
    img = await file.read()
    if not img:
        raise HTTPException(status_code=400, detail="empty_file")

    mime = _guess_mime(file.filename, file.content_type)
    candidates = []

    # Water pass first: в simple-режиме это позволяет часто обойтись без base LLM
    # и заметно снизить задержку на типичных фото воды.
    water_variant_bytes: dict[str, bytes] = {}
    water_variants = _make_water_digit_variants(img)
    if water_variants:
        if OCR_WATER_SIMPLE_MODE:
            # Упрощенный режим: минимальный набор кропов (быстрее и меньше ложных веток).
            water_variants = water_variants[:1] if OCR_FAST_MODE else water_variants[:2]
        elif OCR_FAST_MODE:
            water_variants = water_variants[:2]
    for label, wb in water_variants:
        water_variant_bytes[label] = wb
        try:
            wr = _call_openai_vision(
                wb,
                mime="image/jpeg",
                system_prompt=WATER_ODOMETER_PROMPT,
                user_text="Считай только окно цифр воды: black_digits, red_digits, reading.",
            )
        except Exception:
            continue
        t = _sanitize_type(wr.get("type", "unknown"))
        serial = wr.get("serial", None)
        if isinstance(serial, str):
            serial = serial.strip() or None
        conf = _clamp_confidence(wr.get("confidence", 0.0))
        black_digits = _normalize_digits(wr.get("black_digits"))
        red_digits = _normalize_digits(wr.get("red_digits"))
        # Для воды принимаем только 3 знака дроби.
        if red_digits and len(red_digits) >= 3:
            red_digits = red_digits[:3]
        elif red_digits:
            red_digits = None
        reading = _reading_from_digits(black_digits, red_digits)
        if reading is None:
            reading = _normalize_reading(wr.get("reading", None))
        reading, conf, note2 = _plausibility_filter(t, reading, conf)
        candidates.append(
            {
                "type": t,
                "reading": reading,
                "serial": serial,
                "confidence": conf,
                "notes": str(wr.get("notes", "") or ""),
                "note2": note2,
                "variant": label,
                "provider": "water_digit",
                "black_digits": black_digits,
                "red_digits": red_digits,
            }
        )

        # Specialized OCR sequence pass (PaddleOCR) for exact digit reading.
        # В simple-режиме отключаем для воды, чтобы исключить влияние серийника.
        if OCR_ENABLE_PADDLE and not OCR_WATER_SIMPLE_MODE:
            for pc in _paddle_water_candidates(wb):
                candidates.append(
                    {
                        "type": "unknown",
                        "reading": pc.get("reading"),
                        "serial": pc.get("raw_digits"),
                        "confidence": float(pc.get("confidence") or 0.0),
                        "notes": f"paddle_raw={pc.get('raw_text')}",
                        "note2": "",
                        "variant": f"paddle_{label}",
                        "provider": "paddle_seq",
                        "black_digits": pc.get("black_digits"),
                        "red_digits": pc.get("red_digits"),
                        "raw_digits": pc.get("raw_digits"),
                    }
                )

    water_ready = any(
        str(c.get("provider") or "") == "water_digit"
        and _normalize_digits(c.get("black_digits"))
        and c.get("reading") is not None
        for c in candidates
    )
    run_base = not (OCR_WATER_SIMPLE_MODE and water_ready)

    if run_base:
        variants = _make_variants(img)
        if OCR_FAST_MODE and variants:
            variants = variants[:1]
        for label, b in variants:
            resp = _call_openai_vision(b, mime=mime)

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
                    "provider": "base",
                    "black_digits": None,
                    "red_digits": None,
                }
            )

    if not candidates:
        raise HTTPException(status_code=500, detail="openai_empty_response")

    best, _ = _pick_best_candidate(candidates)

    if OCR_WATER_SIMPLE_MODE:
        water_only = [c for c in candidates if str(c.get("provider") or "") == "water_digit" and _normalize_digits(c.get("black_digits"))]
        if water_only:
            best, _ = _pick_best_candidate(water_only)

    # Дополнительный water-pass на serial-anchor кропе:
    # включаем только когда базовый water-кандидат слабый.
    # Для уверенного black-only чтения (например 00999 без красной части)
    # этот проход чаще замедляет ответ и может увести в неверную разрядность.
    best_provider = str(best.get("provider") or "")
    best_black = _normalize_digits(best.get("black_digits"))
    best_red = _normalize_digits(best.get("red_digits"))
    best_conf = float(best.get("confidence") or 0.0)
    if (
        best_provider in ("water_digit", "water_serial_anchor")
        and (not best_red or len(best_red) != 3)
        and ((not best_black) or (len(best_black) < 5) or (best_conf < 0.78))
    ):
        baseline_reading = None
        try:
            if best.get("reading") is not None:
                baseline_reading = float(best.get("reading"))
        except Exception:
            baseline_reading = None
        serial_variants = _make_water_serial_anchor_variants(img)
        for label, sb in serial_variants:
            water_variant_bytes[label] = sb
            try:
                sr = _call_openai_vision(
                    sb,
                    mime="image/jpeg",
                    system_prompt=WATER_ODOMETER_PROMPT,
                    user_text="Считай только окно цифр воды: black_digits, red_digits, reading.",
                )
            except Exception:
                continue
            t = _sanitize_type(sr.get("type", "unknown"))
            serial = sr.get("serial", None)
            if isinstance(serial, str):
                serial = serial.strip() or None
            conf = _clamp_confidence(sr.get("confidence", 0.0))
            black_digits = _normalize_digits(sr.get("black_digits"))
            red_digits = _normalize_digits(sr.get("red_digits"))
            if red_digits and len(red_digits) >= 3:
                red_digits = red_digits[:3]
            elif red_digits:
                red_digits = None
            reading = _reading_from_digits(black_digits, red_digits)
            if reading is None:
                reading = _normalize_reading(sr.get("reading", None))
            # Защита от срыва в неверную разрядность на serial-anchor кропе.
            if black_digits and len(black_digits) > 5:
                continue
            if (reading is not None) and (baseline_reading is not None) and baseline_reading > 0:
                ratio = float(reading) / float(baseline_reading)
                if ratio > 1.8 or ratio < 0.45:
                    continue
            reading, conf, note2 = _plausibility_filter(t, reading, conf)
            candidates.append(
                {
                    "type": t,
                    "reading": reading,
                    "serial": serial,
                    "confidence": conf,
                    "notes": str(sr.get("notes", "") or ""),
                    "note2": note2,
                    "variant": label,
                    "provider": "water_serial_anchor",
                    "black_digits": black_digits,
                    "red_digits": red_digits,
                }
            )

        water_pool = [
            c
            for c in candidates
            if str(c.get("provider") or "") in ("water_digit", "water_serial_anchor")
            and _normalize_digits(c.get("black_digits"))
        ]
        if water_pool:
            best, _ = _pick_best_candidate(water_pool)

    chosen_label = str(best.get("variant") or "orig")

    # Deterministic refinement for red fractional part:
    # читаем 3 красные ячейки по всем water-кропам и выбираем лучший результат.
    if str(best.get("provider") or "") in ("water_digit", "water_serial_anchor", "paddle_seq"):
        bd = _normalize_digits(best.get("black_digits"))
        rd = _normalize_digits(best.get("red_digits"))
        serial_norm = _normalize_digits(best.get("serial"))
        if bd:
            if chosen_label in water_variant_bytes:
                refine_sources = [water_variant_bytes[chosen_label]]
            else:
                refine_sources = list(water_variant_bytes.values())[:1]
            if OCR_WATER_SIMPLE_MODE:
                # В простом режиме используем только локальные (быстрые) методы,
                # без дополнительных LLM-вызовов по дробной части.
                best_red = None
                best_red_conf = -1.0
                best_red_src = None
                for src in refine_sources:
                    red_refined, red_conf = _refine_red_digits_color_guided(src)
                    red_src = "color"
                    if not red_refined or len(red_refined) != 3:
                        red_refined, red_conf = _refine_red_digits_positional(src)
                        red_src = "positional"
                    if red_refined and len(red_refined) == 3 and red_conf > best_red_conf:
                        best_red = red_refined
                        best_red_conf = red_conf
                        best_red_src = red_src

                if best_red is not None:
                    # Жесткая защита: если дробь равна хвосту серийника — отбрасываем.
                    if serial_norm and len(serial_norm) >= 3 and best_red == serial_norm[-3:]:
                        best["note2"] = ((best.get("note2") or "") + "; red_reject=serial_tail").strip("; ")
                    else:
                        should_replace = False
                        if not rd or len(rd) < 3:
                            if best_red_src == "color":
                                should_replace = best_red_conf >= 0.62
                            else:
                                should_replace = best_red_conf >= 0.72
                        elif best_red != rd:
                            # При конфликте ужесточаем порог, чтобы не ухудшать стабильные случаи.
                            should_replace = best_red_conf >= 0.78
                        if should_replace:
                            best["red_digits"] = best_red
                            best["reading"] = _reading_from_digits(bd, best_red)
                            best["confidence"] = max(float(best.get("confidence") or 0.0), best_red_conf)
                            best["note2"] = (
                                (best.get("note2") or "")
                                + f"; red_refined={best_red}@{best_red_conf:.2f}/{best_red_src}"
                            ).strip("; ")
            else:
                best_red = None
                best_red_conf = -1.0
                best_red_src = None
                for src in refine_sources:
                    # 1) color-guided (лучше ловит красные барабаны при плохом свете)
                    red_refined, red_conf = _refine_red_digits_color_guided(src)
                    red_src = "color"
                    # 2) fallback LLM только по зоне красных цифр (если color не дал 3 цифры)
                    if not red_refined or len(red_refined) != 3:
                        red_refined, red_conf = _refine_red_digits_llm(src)
                        red_src = "llm_red"
                    # 3) deterministic positional fallback (без цвета/LLM)
                    if not red_refined or len(red_refined) != 3:
                        red_refined, red_conf = _refine_red_digits_positional(src)
                        red_src = "positional"
                    if red_refined and len(red_refined) == 3 and red_conf > best_red_conf:
                        best_red = red_refined
                        best_red_conf = red_conf
                        best_red_src = red_src

                if best_red is not None:
                    should_replace = False
                    # Было 2 знака дроби (типичный провал): почти всегда нужно заменять на уверенные 3.
                    if not rd or len(rd) < 3:
                        if best_red_src == "positional":
                            should_replace = best_red_conf >= 0.72
                        elif best_red_src == "llm_red":
                            should_replace = best_red_conf >= 0.52
                        else:
                            should_replace = best_red_conf >= 0.45
                    elif best_red != rd:
                        # Для конфликта 3 vs 3:
                        # - color-guided обычно надежнее и может перезаписать;
                        # - llm_red тоже допускаем для сложных кадров.
                        if best_red_src in ("color", "llm_red", "positional"):
                            should_replace = best_red_conf >= 0.58
                        else:
                            should_replace = False
                    if should_replace:
                        best["red_digits"] = best_red
                        best["reading"] = _reading_from_digits(bd, best_red)
                        best["confidence"] = max(float(best.get("confidence") or 0.0), best_red_conf)
                        best["note2"] = (
                            (best.get("note2") or "")
                            + (f"; red_refined={best_red}@{best_red_conf:.2f}/{best_red_src}" if best_red_src else "")
                        ).strip("; ")

            # Жесткое правило: для воды в хранилище либо 3 знака дроби, либо none.
            rd2 = _normalize_digits(best.get("red_digits"))
            if rd2 and len(rd2) != 3:
                rd2 = None
                best["red_digits"] = None
            best["reading"] = _reading_from_digits(bd, rd2)

    t = best["type"]
    reading = best["reading"]
    serial = best["serial"]
    conf = best["confidence"]
    note2 = best.get("note2") or ""

    # Если серийник не считался в победившем кандидате:
    # 1) берем лучший серийник из уже полученных кандидатов;
    # 2) иначе делаем отдельный serial-only проход по исходному фото.
    if not serial:
        serial_pick = None
        serial_pick_conf = 0.0
        for c in candidates:
            s = c.get("serial")
            if not s:
                continue
            cconf = float(c.get("confidence") or 0.0)
            if cconf > serial_pick_conf:
                serial_pick = s
                serial_pick_conf = cconf
        if serial_pick:
            serial = serial_pick
        elif t in ("ХВС", "ГВС", "unknown"):
            s2, s2c = _read_serial_fallback(img, mime)
            if s2:
                serial = s2
                conf = max(conf, min(0.92, s2c))
                note2 = (note2 + "; serial_fallback=1").strip("; ")

    # notes
    notes = str(best.get("notes", "") or "")
    if note2:
        if notes:
            notes = f"{notes}; {note2}"
        else:
            notes = note2
    notes = (notes.strip() or "")
    if best.get("provider") == "water_digit":
        bd = best.get("black_digits")
        rd = best.get("red_digits")
        notes = (notes + f"; digits={bd or 'null'}/{rd or 'null'}").strip()
    notes = (notes + (f"; variant={chosen_label}" if chosen_label else "")).strip()[:200]

    return {
        "type": t,
        "reading": reading if (isinstance(reading, (int, float)) or reading is None) else None,
        "serial": serial,
        "confidence": conf,
        "notes": notes,
        "black_digits": best.get("black_digits"),
        "red_digits": best.get("red_digits"),
        "provider": best.get("provider"),
    }
