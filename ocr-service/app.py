import os
import base64
import json
import requests
from io import BytesIO
from PIL import Image, ImageOps, ImageFilter, ImageEnhance
from typing import Optional, Tuple
from fastapi import FastAPI, UploadFile, File, HTTPException

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OCR_MODEL = os.getenv("OCR_MODEL", "gpt-4o-mini").strip()

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


def _call_openai_vision(image_bytes: bytes, mime: str) -> dict:
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

    b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:{mime};base64,{b64}"

    payload = {
        "model": OCR_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Определи тип счётчика и показание. Верни JSON строго по схеме."},
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


@app.post("/recognize")
async def recognize(file: UploadFile = File(...)):
    img = await file.read()
    if not img:
        raise HTTPException(status_code=400, detail="empty_file")

    mime = _guess_mime(file.filename, file.content_type)
    variants = _make_variants(img)

    best = None
    best_score = -1.0
    chosen_label = "orig"
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

        score = float(conf)
        if reading is not None:
            score += 0.15
        if t != "unknown":
            score += 0.05

        if score > best_score:
            best_score = score
            best = {
                "type": t,
                "reading": reading,
                "serial": serial,
                "confidence": conf,
                "notes": str(resp.get("notes", "") or ""),
                "note2": note2,
            }
            chosen_label = label

        # early exit if strong
        if conf >= 0.85 and reading is not None:
            break

    if best is None:
        raise HTTPException(status_code=500, detail="openai_empty_response")

    t = best["type"]
    reading = best["reading"]
    serial = best["serial"]
    conf = best["confidence"]
    note2 = best.get("note2") or ""

    # notes
    notes = str(best.get("notes", "") or "")
    if note2:
        if notes:
            notes = f"{notes}; {note2}"
        else:
            notes = note2
    notes = (notes.strip() or "")
    notes = (notes + (f"; variant={chosen_label}" if chosen_label else "")).strip()[:200]

    return {
        "type": t,
        "reading": reading if (isinstance(reading, (int, float)) or reading is None) else None,
        "serial": serial,
        "confidence": conf,
        "notes": notes,
    }
