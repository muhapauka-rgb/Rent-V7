import re
import time
from io import BytesIO
from typing import Any, Optional

import cv2
import numpy as np
from PIL import Image, ImageEnhance, ImageFilter

try:
    import pytesseract  # type: ignore
except Exception:  # pragma: no cover - optional runtime dependency
    pytesseract = None

try:
    from water_deterministic import make_water_deterministic_row_variants
except Exception:  # pragma: no cover - optional during standalone import
    make_water_deterministic_row_variants = None


LOCAL_RECOGNIZER_VERSION = "local-shadow-v1"


def _jsonable_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        f = float(v)
        if np.isnan(f) or np.isinf(f):
            return default
        return f
    except Exception:
        return default


def _decode_image(img_bytes: bytes) -> Optional[np.ndarray]:
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is None:
            pil = Image.open(BytesIO(img_bytes)).convert("RGB")
            img = cv2.cvtColor(np.array(pil), cv2.COLOR_RGB2BGR)
        return img
    except Exception:
        return None


def _resize_for_analysis(img: np.ndarray, max_dim: int = 1200) -> tuple[np.ndarray, float]:
    h, w = img.shape[:2]
    m = max(h, w)
    if m <= max_dim:
        return img, 1.0
    scale = float(max_dim) / float(max(1, m))
    out = cv2.resize(
        img,
        (max(1, int(round(w * scale))), max(1, int(round(h * scale)))),
        interpolation=cv2.INTER_AREA,
    )
    return out, scale


def _bbox_dict(x: int, y: int, w: int, h: int, scale: float, image_w: int, image_h: int) -> dict[str, Any]:
    inv = 1.0 / max(1e-6, float(scale))
    ox = int(round(float(x) * inv))
    oy = int(round(float(y) * inv))
    ow = int(round(float(w) * inv))
    oh = int(round(float(h) * inv))
    return {
        "x": ox,
        "y": oy,
        "w": ow,
        "h": oh,
        "rel": {
            "x": round(ox / float(max(1, image_w)), 4),
            "y": round(oy / float(max(1, image_h)), 4),
            "w": round(ow / float(max(1, image_w)), 4),
            "h": round(oh / float(max(1, image_h)), 4),
        },
    }


def _digit_like_components(gray: np.ndarray) -> list[tuple[int, int, int, int, int]]:
    h, w = gray.shape[:2]
    try:
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
        blur = cv2.GaussianBlur(clahe, (3, 3), 0)
        _, th = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        th = cv2.morphologyEx(th, cv2.MORPH_OPEN, np.ones((2, 2), np.uint8), iterations=1)
        num_labels, _labels, stats, _centroids = cv2.connectedComponentsWithStats(th, connectivity=8)
    except Exception:
        return []

    out: list[tuple[int, int, int, int, int]] = []
    image_area = float(max(1, h * w))
    for idx in range(1, int(num_labels)):
        x = int(stats[idx, cv2.CC_STAT_LEFT])
        y = int(stats[idx, cv2.CC_STAT_TOP])
        ww = int(stats[idx, cv2.CC_STAT_WIDTH])
        hh = int(stats[idx, cv2.CC_STAT_HEIGHT])
        area = int(stats[idx, cv2.CC_STAT_AREA])
        if area < image_area * 0.00008 or area > image_area * 0.08:
            continue
        if hh < max(10, h * 0.025) or hh > h * 0.55:
            continue
        if ww < 3 or ww > w * 0.35:
            continue
        aspect = float(ww) / float(max(1, hh))
        if not (0.04 <= aspect <= 1.35):
            continue
        out.append((x, y, ww, hh, area))
    return out


def _group_components_into_rows(
    comps: list[tuple[int, int, int, int, int]],
    *,
    image_shape: tuple[int, int],
) -> list[tuple[int, int, int, int, list[tuple[int, int, int, int, int]], float]]:
    h, w = image_shape
    if not comps:
        return []
    comps_sorted = sorted(comps, key=lambda c: (c[1] + c[3] / 2.0, c[0]))
    rows: list[list[tuple[int, int, int, int, int]]] = []
    for comp in comps_sorted:
        cy = comp[1] + comp[3] / 2.0
        placed = False
        for row in rows:
            ref = sum(c[1] + c[3] / 2.0 for c in row) / float(max(1, len(row)))
            avg_h = sum(c[3] for c in row) / float(max(1, len(row)))
            if abs(cy - ref) <= max(10.0, avg_h * 0.72):
                row.append(comp)
                placed = True
                break
        if not placed:
            rows.append([comp])

    row_items: list[tuple[int, int, int, int, list[tuple[int, int, int, int, int]], float]] = []
    for row in rows:
        if len(row) < 3:
            continue
        xs = [c[0] for c in row]
        ys = [c[1] for c in row]
        x2s = [c[0] + c[2] for c in row]
        y2s = [c[1] + c[3] for c in row]
        x1 = max(0, min(xs))
        y1 = max(0, min(ys))
        x2 = min(w, max(x2s))
        y2 = min(h, max(y2s))
        bw = max(1, x2 - x1)
        bh = max(1, y2 - y1)
        span = bw / float(max(1, w))
        count_score = min(1.0, len(row) / 8.0)
        aspect_score = min(1.0, max(0.0, (bw / float(max(1, bh))) / 7.0))
        height_score = min(1.0, bh / float(max(1, h)) * 8.0)
        score = 0.42 * count_score + 0.30 * span + 0.18 * aspect_score + 0.10 * height_score
        row_items.append((x1, y1, bw, bh, sorted(row, key=lambda c: c[0]), round(float(score), 4)))
    row_items.sort(key=lambda r: (r[5], len(r[4])), reverse=True)
    return row_items


def _red_pixel_ratio(crop_bgr: np.ndarray) -> float:
    if crop_bgr.size == 0:
        return 0.0
    b, g, r = cv2.split(crop_bgr)
    mask = (r.astype(np.int16) > g.astype(np.int16) + 28) & (r.astype(np.int16) > b.astype(np.int16) + 28) & (r > 80)
    return float(mask.sum()) / float(max(1, mask.size))


def _classify_zone_kind(crop_bgr: np.ndarray, comps_count: int, row_score: float) -> str:
    h, w = crop_bgr.shape[:2]
    aspect = float(w) / float(max(1, h))
    red_ratio = _red_pixel_ratio(crop_bgr)
    if red_ratio >= 0.012 and aspect >= 2.4:
        return "water_odometer"
    if aspect >= 3.2 and comps_count >= 4 and red_ratio < 0.012:
        return "electric_display"
    if aspect >= 2.2 and comps_count >= 4 and row_score >= 0.34:
        return "digit_row"
    return "unknown_digit_zone"


def _detect_water_face_odometer_zones(img: np.ndarray) -> list[tuple[np.ndarray, dict[str, Any], float]]:
    """Find round water-meter faces and return practical odometer band crops.

    The legacy deterministic water row crop can lock onto foreground pipes/bars
    on cramped Telegram photos. A cheap circle pass gives us a better physical
    anchor without introducing a heavy detector.
    """
    if img is None or img.size == 0:
        return []
    h, w = img.shape[:2]
    if h < 120 or w < 120:
        return []
    try:
        work, scale = _resize_for_analysis(img, max_dim=1200)
        gray = cv2.cvtColor(work, cv2.COLOR_BGR2GRAY)
        blur = cv2.medianBlur(gray, 5)
        min_dim = min(work.shape[:2])
        circles = cv2.HoughCircles(
            blur,
            cv2.HOUGH_GRADIENT,
            dp=1.2,
            minDist=max(80, int(min_dim * 0.16)),
            param1=80,
            param2=28,
            minRadius=max(55, int(min_dim * 0.08)),
            maxRadius=max(90, int(min_dim * 0.36)),
        )
    except Exception:
        return []
    if circles is None:
        return []

    inv = 1.0 / max(1e-6, float(scale))
    scored: list[tuple[float, int, int, int]] = []
    seen: list[tuple[int, int, int]] = []
    for raw in np.round(circles[0, :]).astype(int).tolist():
        x_s, y_s, r_s = int(raw[0]), int(raw[1]), int(raw[2])
        x = int(round(x_s * inv))
        y = int(round(y_s * inv))
        r = int(round(r_s * inv))
        if r < min(w, h) * 0.08 or r > min(w, h) * 0.42:
            continue
        if not (0.25 * w <= x <= 0.82 * w and 0.30 * h <= y <= 0.95 * h):
            continue
        duplicate = False
        for sx, sy, sr in seen:
            if abs(x - sx) <= max(24, int(0.20 * min(r, sr))) and abs(y - sy) <= max(24, int(0.20 * min(r, sr))):
                duplicate = True
                break
        if duplicate:
            continue
        seen.append((x, y, r))
        x1 = max(0, x - r)
        y1 = max(0, y - r)
        x2 = min(w, x + r)
        y2 = min(h, y + r)
        face = img[y1:y2, x1:x2]
        if face.size == 0:
            continue
        # Odometer band heuristic for round Valtec-like water meters.
        bx1 = max(0, int(round(x - 0.98 * r)))
        by1 = max(0, int(round(y - 0.43 * r)))
        bx2 = min(w, int(round(x + 0.92 * r)))
        by2 = min(h, int(round(y + 0.18 * r)))
        band = img[by1:by2, bx1:bx2]
        if band.size == 0 or band.shape[0] < 18 or band.shape[1] < 90:
            continue
        red_ratio = _red_pixel_ratio(band)
        face_std = float(np.std(cv2.cvtColor(face, cv2.COLOR_BGR2GRAY)))
        gray_band = cv2.cvtColor(band, cv2.COLOR_BGR2GRAY)
        bright_ratio = float((gray_band > 145).sum()) / float(max(1, gray_band.size))
        dark_ratio = float((gray_band < 80).sum()) / float(max(1, gray_band.size))
        band_aspect = float(band.shape[1]) / float(max(1, band.shape[0]))
        score = 0.28 + min(0.22, face_std / 260.0)
        if 2.2 <= band_aspect <= 5.8:
            score += 0.12
        if 0.002 <= red_ratio <= 0.065:
            score += 0.16
        elif red_ratio > 0.12:
            score -= 0.20
        if 0.30 <= bright_ratio <= 0.76:
            score += 0.16
        elif bright_ratio > 0.90 or bright_ratio < 0.04:
            score -= 0.10
        if 0.018 <= dark_ratio <= 0.24:
            score += 0.12
        elif dark_ratio > 0.42:
            score -= 0.14
        band_top_rel = by1 / float(max(1, h))
        if band_top_rel <= 0.58:
            score += 0.09
        elif band_top_rel > 0.66:
            score -= 0.18
        score += 0.05 * (1.0 - abs((y / float(h)) - 0.58))
        scored.append((round(float(min(0.92, max(0.20, score))), 4), bx1, by1, bx2 - bx1, by2 - by1))

    scored.sort(reverse=True, key=lambda item: item[0])
    out: list[tuple[np.ndarray, dict[str, Any], float]] = []
    for score, x, y, bw, bh in scored[:3]:
        crop = img[y : y + bh, x : x + bw]
        bbox = {
            "x": int(x),
            "y": int(y),
            "w": int(bw),
            "h": int(bh),
            "rel": {
                "x": round(x / float(max(1, w)), 4),
                "y": round(y / float(max(1, h)), 4),
                "w": round(bw / float(max(1, w)), 4),
                "h": round(bh / float(max(1, h)), 4),
            },
        }
        out.append((crop, bbox, score))
    return out


def _prepare_crop_for_tesseract(crop_bgr: np.ndarray) -> Image.Image:
    rgb = cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2RGB)
    pil = Image.fromarray(rgb).convert("L")
    pil = ImageEnhance.Contrast(pil).enhance(2.2)
    pil = ImageEnhance.Sharpness(pil).enhance(1.8)
    pil = pil.filter(ImageFilter.UnsharpMask(radius=1, percent=220, threshold=2))
    scale = 2
    pil = pil.resize((max(1, pil.width * scale), max(1, pil.height * scale)), Image.Resampling.LANCZOS)
    return pil


def _tesseract_digits(crop_bgr: np.ndarray, timeout_sec: float) -> tuple[str, str]:
    if pytesseract is None:
        return "", ""
    try:
        pil = _prepare_crop_for_tesseract(crop_bgr)
        cfg = "--psm 7 -c tessedit_char_whitelist=0123456789"
        raw = pytesseract.image_to_string(pil, config=cfg, timeout=max(0.3, float(timeout_sec)))
        digits = re.sub(r"\D+", "", raw or "")
        return digits, str(raw or "").strip()
    except Exception as exc:
        return "", f"error:{type(exc).__name__}"


def _serial_digit_options(raw_text: str) -> list[str]:
    text = str(raw_text or "")
    options: list[str] = []

    def _append(digits: str) -> None:
        if 6 <= len(digits) <= 10 and digits not in options:
            options.append(digits)

    for token in re.findall(r"[0-9][0-9\s\-]{4,}[0-9]", text):
        digits = re.sub(r"\D+", "", token)
        _append(digits)
    all_digits = re.sub(r"\D+", "", text)
    _append(all_digits)
    # Noisy OCR sometimes concatenates serial text with logo/odometer crumbs.
    # Keep vendor-prefixed serial-looking windows as candidates; API-level
    # context matching decides whether any of them is safe enough to use.
    if len(all_digits) > 10:
        for size in (8, 9, 7, 10):
            for idx in range(0, max(0, len(all_digits) - size + 1)):
                chunk = all_digits[idx : idx + size]
                if chunk.startswith(("13", "15")):
                    _append(chunk)
    return options


def _prepare_serial_crop_variants(crop_bgr: np.ndarray) -> list[tuple[str, Image.Image]]:
    if crop_bgr is None or crop_bgr.size == 0:
        return []
    gray = cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2GRAY)
    variants: list[tuple[str, np.ndarray]] = [
        ("gray", gray),
        ("clahe", cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8)).apply(gray)),
    ]
    out: list[tuple[str, Image.Image]] = []
    for name, arr in variants:
        pil = Image.fromarray(arr).convert("L")
        pil = ImageEnhance.Contrast(pil).enhance(2.5)
        pil = ImageEnhance.Sharpness(pil).enhance(2.0)
        pil = pil.filter(ImageFilter.UnsharpMask(radius=1, percent=220, threshold=2))
        scale = 4
        pil = pil.resize((max(1, pil.width * scale), max(1, pil.height * scale)), Image.Resampling.LANCZOS)
        out.append((name, pil))
    return out


def _extract_water_serial_candidates(
    img: np.ndarray,
    zones: list[dict[str, Any]],
    water_candidates: list[dict[str, Any]],
    *,
    timeout_sec: float,
    max_sources: int = 2,
) -> list[dict[str, Any]]:
    if pytesseract is None or img is None or img.size == 0:
        return []
    h, w = img.shape[:2]
    by_zone: dict[str, dict[str, Any]] = {
        str(z.get("id") or ""): z for z in zones if isinstance(z, dict)
    }
    source_zone_ids: list[str] = []
    for cand in water_candidates:
        zone_id = str(cand.get("zone_id") or "").strip()
        if zone_id and zone_id in by_zone and zone_id not in source_zone_ids:
            source_zone_ids.append(zone_id)
        if len(source_zone_ids) >= max(1, int(max_sources)):
            break

    serial_candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source_idx, zone_id in enumerate(source_zone_ids):
        zone = by_zone.get(zone_id) or {}
        bbox = zone.get("bbox") if isinstance(zone.get("bbox"), dict) else None
        if not bbox:
            continue
        try:
            x = int(bbox.get("x") or 0)
            y = int(bbox.get("y") or 0)
            bw = int(bbox.get("w") or 0)
            bh = int(bbox.get("h") or 0)
        except Exception:
            continue
        if bw <= 30 or bh <= 20:
            continue

        crop_specs = [
            (
                "face_top_centered_left",
                int(round(x - bw * 0.01)),
                int(round(y - bh * 0.46)),
                int(round(bw * 1.03)),
                int(round(bh * 0.96)),
                0.84,
            ),
            (
                "face_top_centered",
                int(round(x)),
                int(round(y - bh * 0.46)),
                int(round(bw * 1.03)),
                int(round(bh * 0.96)),
                0.82,
            ),
            (
                "wide_top_band",
                int(round(x - bw * 0.05)),
                int(round(y - bh * 0.46)),
                int(round(bw * 1.10)),
                int(round(bh * 0.96)),
                0.78,
            ),
            (
                "serial_strip",
                int(round(x + bw * 0.18)),
                int(round(y - bh * 0.28)),
                int(round(bw * 0.64)),
                int(round(bh * 0.58)),
                0.70,
            ),
        ]
        for crop_label, cx, cy, cw, ch, geom in crop_specs:
            x1 = max(0, min(w - 1, cx))
            y1 = max(0, min(h - 1, cy))
            x2 = max(x1 + 1, min(w, cx + cw))
            y2 = max(y1 + 1, min(h, cy + ch))
            crop = img[y1:y2, x1:x2]
            if crop.size == 0:
                continue
            for prep_name, pil in _prepare_serial_crop_variants(crop):
                try:
                    raw = pytesseract.image_to_string(
                        pil,
                        config="--psm 6 -c tessedit_char_whitelist=0123456789",
                        timeout=max(0.4, float(timeout_sec)),
                    )
                except Exception:
                    continue
                for digits in _serial_digit_options(raw):
                    if digits in seen:
                        continue
                    seen.add(digits)
                    # Water serials in this project are usually vendor-prefixed
                    # plus a 5-6 digit tail. Penalize very short/noisy reads.
                    length_bonus = 0.10 if 7 <= len(digits) <= 9 else 0.0
                    prefix_bonus = 0.04 if digits.startswith(("13", "15")) else 0.0
                    score = min(0.96, max(0.1, geom + length_bonus + prefix_bonus - 0.03 * source_idx))
                    serial_candidates.append(
                        {
                            "source": "local_tesseract_serial",
                            "serial": digits,
                            "serial_confidence": round(float(score), 4),
                            "zone_id": zone_id,
                            "zone_kind": zone.get("kind_hint"),
                            "crop": crop_label,
                            "preprocess": prep_name,
                            "raw": str(raw or "").strip()[:80],
                            "bbox": {
                                "x": int(x1),
                                "y": int(y1),
                                "w": int(x2 - x1),
                                "h": int(y2 - y1),
                                "rel": {
                                    "x": round(x1 / float(max(1, w)), 4),
                                    "y": round(y1 / float(max(1, h)), 4),
                                    "w": round((x2 - x1) / float(max(1, w)), 4),
                                    "h": round((y2 - y1) / float(max(1, h)), 4),
                                },
                            },
                        }
                    )
    serial_candidates.sort(key=lambda c: float(c.get("serial_confidence") or 0.0), reverse=True)
    return serial_candidates[:6]


def _water_candidate_from_digits(digits: str) -> Optional[dict[str, Any]]:
    d = re.sub(r"\D+", "", digits or "")
    if len(d) < 5:
        return None
    if len(d) >= 7:
        core = d[-7:]
        black = core[:5]
        red = core[5:7]
        flags: list[str] = []
    else:
        black = d[:-2].rjust(5, "0")
        red = d[-2:]
        flags = ["missing_leading_black_digits"]
    try:
        reading = float(f"{int(black)}.{red}")
    except Exception:
        return None
    if reading < 0.0 or reading > 99999.99:
        return None
    return {
        "source": "local_shadow_tesseract",
        "reading": round(reading, 2),
        "integer_digits": black,
        "decimal_digits": red,
        "raw_digits": d,
        "decimal_digits_count": 2,
        "suspicious_flags": flags,
    }


def _electric_candidate_from_digits(digits: str) -> Optional[dict[str, Any]]:
    d = re.sub(r"\D+", "", digits or "")
    if len(d) < 5:
        return None
    if len(d) >= 6:
        core = d[-6:]
        whole = core[:4]
        frac = core[4:6]
    else:
        core = d[-5:]
        whole = core[:3]
        frac = core[3:5]
    try:
        reading = float(f"{int(whole)}.{frac}")
    except Exception:
        return None
    if reading < 0.0 or reading > 99999.99:
        return None
    return {
        "source": "local_shadow_tesseract",
        "reading": round(reading, 2),
        "digits": core,
        "raw_digits": d,
        "suspicious_flags": ["short_electric_digits"] if len(d) < 6 else [],
    }


def run_local_meter_shadow(
    img_bytes: bytes,
    *,
    max_zones: int = 6,
    tesseract_enabled: bool = False,
    tesseract_timeout_sec: float = 0.8,
    include_existing_water_rows: bool = False,
    digit_classifier: Any = None,
    serial_tesseract_enabled: bool = True,
) -> dict[str, Any]:
    started = time.monotonic()
    img = _decode_image(img_bytes)
    if img is None:
        return {
            "version": LOCAL_RECOGNIZER_VERSION,
            "status": "decode_failed",
            "zones": [],
            "water_candidates": [],
            "electric_candidates": [],
        }

    orig_h, orig_w = img.shape[:2]
    work, scale = _resize_for_analysis(img)
    h, w = work.shape[:2]
    gray = cv2.cvtColor(work, cv2.COLOR_BGR2GRAY)
    comps = _digit_like_components(gray)
    rows = _group_components_into_rows(comps, image_shape=(h, w))[: max(1, int(max_zones))]

    zones: list[dict[str, Any]] = []
    water_candidates: list[dict[str, Any]] = []
    electric_candidates: list[dict[str, Any]] = []
    serial_candidates: list[dict[str, Any]] = []
    try:
        digit_classifier_available = bool(
            digit_classifier is not None
            and getattr(digit_classifier, "available", lambda: False)()
            and hasattr(digit_classifier, "predict_sequence")
        )
    except Exception:
        digit_classifier_available = False

    def _prediction_payload(seq: Any) -> dict[str, Any]:
        per_digit = list(getattr(seq, "per_digit", ()) or ())
        return {
            "digits": str(getattr(seq, "digits", "") or "")[:16],
            "confidence": _jsonable_float(getattr(seq, "confidence", None)),
            "model_version": str(getattr(seq, "model_version", "") or ""),
            "digit_confidences": [
                _jsonable_float(getattr(p, "confidence", None)) for p in per_digit[:12]
            ],
        }

    def _append_digit_classifier_candidates(
        *,
        crop: np.ndarray,
        kind_hint: str,
        zone_id: str,
        zone: dict[str, Any],
        geometry_confidence: float,
    ) -> None:
        if not digit_classifier_available:
            return

        predictions: dict[str, Any] = {}

        def _try_water(kind_bonus: float, flags: list[str]) -> None:
            try:
                seq = digit_classifier.predict_sequence(crop, 7)
            except Exception as exc:
                predictions["water_error"] = f"{type(exc).__name__}: {str(exc)[:80]}"
                return
            if seq is None:
                return
            predictions["water"] = _prediction_payload(seq)
            wc = _water_candidate_from_digits(str(getattr(seq, "digits", "") or ""))
            if wc is None:
                return
            seq_conf = _jsonable_float(getattr(seq, "confidence", None))
            candidate_score = 0.18 + 0.46 * seq_conf + 0.24 * float(geometry_confidence) + kind_bonus
            wc.update(
                {
                    "source": "local_template_classifier",
                    "zone_id": zone_id,
                    "zone_kind": kind_hint,
                    "odo_confidence": round(seq_conf, 4),
                    "geometry_confidence": zone["geometry_confidence"],
                    "candidate_score": round(min(0.96, max(0.01, candidate_score)), 4),
                    "model_version": str(getattr(seq, "model_version", "") or ""),
                    "digit_confidences": predictions["water"].get("digit_confidences", []),
                    "suspicious_flags": list(wc.get("suspicious_flags") or []) + flags,
                }
            )
            water_candidates.append(wc)

        def _try_electric(kind_bonus: float, flags: list[str]) -> None:
            try:
                seq = digit_classifier.predict_sequence(crop, 6)
            except Exception as exc:
                predictions["electric_error"] = f"{type(exc).__name__}: {str(exc)[:80]}"
                return
            if seq is None:
                return
            predictions["electric"] = _prediction_payload(seq)
            ec = _electric_candidate_from_digits(str(getattr(seq, "digits", "") or ""))
            if ec is None:
                return
            seq_conf = _jsonable_float(getattr(seq, "confidence", None))
            candidate_score = 0.18 + 0.46 * seq_conf + 0.24 * float(geometry_confidence) + kind_bonus
            ec.update(
                {
                    "source": "local_template_classifier",
                    "zone_id": zone_id,
                    "zone_kind": kind_hint,
                    "display_confidence": round(seq_conf, 4),
                    "geometry_confidence": zone["geometry_confidence"],
                    "candidate_score": round(min(0.96, max(0.01, candidate_score)), 4),
                    "model_version": str(getattr(seq, "model_version", "") or ""),
                    "digit_confidences": predictions["electric"].get("digit_confidences", []),
                    "suspicious_flags": list(ec.get("suspicious_flags") or []) + flags,
                }
            )
            electric_candidates.append(ec)

        if kind_hint == "water_odometer":
            _try_water(0.08, [])
        elif kind_hint == "electric_display":
            _try_electric(0.08, [])
        elif kind_hint == "digit_row":
            _try_water(0.0, ["ambiguous_digit_row"])
            _try_electric(0.0, ["ambiguous_digit_row"])

        if predictions:
            zone["digit_classifier"] = predictions

    def _append_shadow_zone(
        *,
        crop: np.ndarray,
        kind_hint: str,
        source: str,
        bbox: Optional[dict[str, Any]],
        digit_like_components: int = 0,
        geometry_confidence: float = 0.42,
    ) -> None:
        if len(zones) >= max(1, int(max_zones)):
            return
        if crop is None or crop.size == 0:
            return
        zone_id = f"zone_{len(zones)}"
        zone: dict[str, Any] = {
            "id": zone_id,
            "source": source,
            "kind_hint": kind_hint,
            "bbox": bbox,
            "digit_like_components": int(digit_like_components),
            "geometry_confidence": round(min(0.98, max(0.05, float(geometry_confidence))), 4),
            "red_pixel_ratio": round(_red_pixel_ratio(crop), 5),
        }
        zones.append(zone)
        _append_digit_classifier_candidates(
            crop=crop,
            kind_hint=kind_hint,
            zone_id=zone_id,
            zone=zone,
            geometry_confidence=geometry_confidence,
        )
        if not tesseract_enabled:
            return
        digits, raw_text = _tesseract_digits(crop, tesseract_timeout_sec)
        if not digits:
            zone["tesseract"] = {"digits": "", "raw": raw_text[:80] if raw_text else ""}
            return
        zone["tesseract"] = {"digits": digits[:16], "raw": raw_text[:80] if raw_text else ""}
        base_conf = min(0.86, 0.28 + 0.06 * min(8, len(digits)) + 0.28 * float(geometry_confidence))
        if kind_hint in ("water_odometer", "digit_row", "unknown_digit_zone"):
            wc = _water_candidate_from_digits(digits)
            if wc is not None:
                wc.update(
                    {
                        "zone_id": zone_id,
                        "zone_kind": kind_hint,
                        "odo_confidence": round(base_conf, 4),
                        "geometry_confidence": zone["geometry_confidence"],
                        "candidate_score": round(base_conf + (0.05 if kind_hint == "water_odometer" else 0.0), 4),
                    }
                )
                water_candidates.append(wc)
        if kind_hint in ("electric_display", "digit_row", "unknown_digit_zone"):
            ec = _electric_candidate_from_digits(digits)
            if ec is not None:
                ec.update(
                    {
                        "zone_id": zone_id,
                        "zone_kind": kind_hint,
                        "display_confidence": round(base_conf, 4),
                        "geometry_confidence": zone["geometry_confidence"],
                        "candidate_score": round(base_conf + (0.05 if kind_hint == "electric_display" else 0.0), 4),
                    }
                )
                electric_candidates.append(ec)

    for idx, (x, y, bw, bh, row_comps, row_score) in enumerate(rows):
        pad_x = max(4, int(round(bw * 0.06)))
        pad_y = max(3, int(round(bh * 0.26)))
        cx1 = max(0, x - pad_x)
        cy1 = max(0, y - pad_y)
        cx2 = min(w, x + bw + pad_x)
        cy2 = min(h, y + bh + pad_y)
        crop = work[cy1:cy2, cx1:cx2]
        kind_hint = _classify_zone_kind(crop, len(row_comps), row_score)
        red_ratio = round(_red_pixel_ratio(crop), 5)
        zone_id = f"zone_{idx}"
        zone = {
            "id": zone_id,
            "source": "component_row",
            "kind_hint": kind_hint,
            "bbox": _bbox_dict(cx1, cy1, max(1, cx2 - cx1), max(1, cy2 - cy1), scale, orig_w, orig_h),
            "digit_like_components": len(row_comps),
            "geometry_confidence": round(min(0.98, max(0.05, float(row_score))), 4),
            "red_pixel_ratio": red_ratio,
        }
        zones.append(zone)
        _append_digit_classifier_candidates(
            crop=crop,
            kind_hint=kind_hint,
            zone_id=zone_id,
            zone=zone,
            geometry_confidence=row_score,
        )

        if not tesseract_enabled:
            continue
        digits, raw_text = _tesseract_digits(crop, tesseract_timeout_sec)
        if not digits:
            zone["tesseract"] = {"digits": "", "raw": raw_text[:80] if raw_text else ""}
            continue
        zone["tesseract"] = {"digits": digits[:16], "raw": raw_text[:80] if raw_text else ""}
        base_conf = min(0.86, 0.28 + 0.06 * min(8, len(digits)) + 0.28 * float(row_score))
        if kind_hint in ("water_odometer", "digit_row", "unknown_digit_zone"):
            wc = _water_candidate_from_digits(digits)
            if wc is not None:
                wc.update(
                    {
                        "zone_id": zone_id,
                        "zone_kind": kind_hint,
                        "odo_confidence": round(base_conf, 4),
                        "geometry_confidence": zone["geometry_confidence"],
                        "candidate_score": round(base_conf + (0.05 if kind_hint == "water_odometer" else 0.0), 4),
                    }
                )
                water_candidates.append(wc)
        if kind_hint in ("electric_display", "digit_row", "unknown_digit_zone"):
            ec = _electric_candidate_from_digits(digits)
            if ec is not None:
                ec.update(
                    {
                        "zone_id": zone_id,
                        "zone_kind": kind_hint,
                        "display_confidence": round(base_conf, 4),
                        "geometry_confidence": zone["geometry_confidence"],
                        "candidate_score": round(base_conf + (0.05 if kind_hint == "electric_display" else 0.0), 4),
                    }
                )
                electric_candidates.append(ec)

    # Physical water-meter face anchor. This is deliberately placed before the
    # legacy fixed ROIs so exported datasets get useful odometer crops first.
    if len(zones) < max(1, int(max_zones)):
        for crop, bbox, geom_conf in _detect_water_face_odometer_zones(img):
            if len(zones) >= max(1, int(max_zones)):
                break
            _append_shadow_zone(
                crop=crop,
                kind_hint="water_odometer",
                source="hough_face:odometer_band",
                bbox=bbox,
                digit_like_components=0,
                geometry_confidence=geom_conf,
            )

    # Existing production CV already has useful water row crops. Pull them into
    # the local-recognizer contract as zone evidence without making them winner.
    if include_existing_water_rows and make_water_deterministic_row_variants is not None and len(zones) < max(1, int(max_zones)):
        try:
            for label, row_bytes in make_water_deterministic_row_variants(img_bytes, max_variants=3):
                row_img = _decode_image(row_bytes)
                if row_img is None:
                    continue
                _append_shadow_zone(
                    crop=row_img,
                    kind_hint="water_odometer",
                    source=f"water_deterministic:{label}",
                    bbox=None,
                    digit_like_components=0,
                    geometry_confidence=0.62,
                )
        except Exception:
            pass

    # Cheap fixed water/electric ROIs give the future recognizer stable training
    # and debug zones without invoking heavyweight production crop generators.
    if len(zones) < max(1, int(max_zones)):
        roi_specs = [
            ("water_odometer_mid", "water_odometer", (0.08, 0.30, 0.92, 0.62), 0.44),
            ("water_odometer_lower", "water_odometer", (0.08, 0.40, 0.92, 0.74), 0.42),
            ("water_odometer_upper", "water_odometer", (0.08, 0.20, 0.92, 0.52), 0.38),
            ("electric_lcd_tight", "electric_display", (0.10, 0.33, 0.78, 0.66), 0.48),
            ("electric_lcd_digits", "electric_display", (0.14, 0.38, 0.70, 0.62), 0.48),
            ("electric_lcd_wide", "electric_display", (0.05, 0.30, 0.84, 0.70), 0.46),
        ]
        for label, kind_hint, (x1f, y1f, x2f, y2f), geom_conf in roi_specs:
            if len(zones) >= max(1, int(max_zones)):
                break
            x1 = max(0, min(orig_w - 1, int(round(orig_w * x1f))))
            y1 = max(0, min(orig_h - 1, int(round(orig_h * y1f))))
            x2 = max(x1 + 1, min(orig_w, int(round(orig_w * x2f))))
            y2 = max(y1 + 1, min(orig_h, int(round(orig_h * y2f))))
            crop = img[y1:y2, x1:x2]
            bbox = {
                "x": x1,
                "y": y1,
                "w": x2 - x1,
                "h": y2 - y1,
                "rel": {
                    "x": round(x1 / float(max(1, orig_w)), 4),
                    "y": round(y1 / float(max(1, orig_h)), 4),
                    "w": round((x2 - x1) / float(max(1, orig_w)), 4),
                    "h": round((y2 - y1) / float(max(1, orig_h)), 4),
                },
            }
            _append_shadow_zone(
                crop=crop,
                kind_hint=kind_hint,
                source=f"fixed_roi:{label}",
                bbox=bbox,
                digit_like_components=0,
                geometry_confidence=geom_conf,
            )
    water_candidates.sort(key=lambda c: float(c.get("candidate_score") or 0.0), reverse=True)
    electric_candidates.sort(key=lambda c: float(c.get("candidate_score") or 0.0), reverse=True)
    if serial_tesseract_enabled and water_candidates:
        serial_candidates = _extract_water_serial_candidates(
            img,
            zones,
            water_candidates,
            timeout_sec=min(1.2, max(0.4, float(tesseract_timeout_sec))),
        )
    best_water = water_candidates[0] if water_candidates else None
    best_electric = electric_candidates[0] if electric_candidates else None
    winner = None
    if best_water or best_electric:
        if best_water and (
            not best_electric
            or float(best_water.get("candidate_score") or 0.0) >= float(best_electric.get("candidate_score") or 0.0)
        ):
            winner = {"kind": "water", **best_water}
        elif best_electric:
            winner = {"kind": "electric", **best_electric}

    return {
        "version": LOCAL_RECOGNIZER_VERSION,
        "status": "ok",
        "mode": "shadow",
        "image": {"width": int(orig_w), "height": int(orig_h)},
        "tesseract_enabled": bool(tesseract_enabled and pytesseract is not None),
        "digit_classifier_enabled": bool(digit_classifier_available),
        "digit_classifier_version": str(getattr(digit_classifier, "model_version", "") or "")
        if digit_classifier_available
        else "",
        "zones": zones,
        "serial_candidates": serial_candidates[:6],
        "water_candidates": water_candidates[:6],
        "electric_candidates": electric_candidates[:6],
        "winner": winner,
        "elapsed_ms": int((time.monotonic() - started) * 1000),
    }
