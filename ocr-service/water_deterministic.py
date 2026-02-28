from __future__ import annotations

from io import BytesIO
from typing import Optional

import cv2
import numpy as np
from PIL import Image, ImageEnhance, ImageFilter


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


def _encode_jpeg(img: Image.Image, quality: int = 95) -> bytes:
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()


def _unique_circles(circles: list[tuple[int, int, int]]) -> list[tuple[int, int, int]]:
    out: list[tuple[int, int, int]] = []
    for x, y, r in circles:
        is_dup = False
        for ox, oy, orr in out:
            if abs(x - ox) <= max(8, int(orr * 0.10)) and abs(y - oy) <= max(8, int(orr * 0.10)):
                is_dup = True
                break
        if not is_dup:
            out.append((x, y, r))
    return out


def _enhance_row_variants(crop_bgr: np.ndarray, label_prefix: str) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    if crop_bgr.size == 0:
        return out

    pil = Image.fromarray(cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2RGB))
    # x3 is noticeably faster than x4 on dark photos and keeps enough detail for OCR.
    pil = pil.resize((max(1, pil.width * 3), max(1, pil.height * 3)), Image.Resampling.LANCZOS)

    # Base sharpened row.
    base = ImageEnhance.Contrast(pil).enhance(2.1)
    base = ImageEnhance.Sharpness(base).enhance(1.7)
    base = base.filter(ImageFilter.UnsharpMask(radius=1, percent=280, threshold=2))
    out.append((f"{label_prefix}_base", _encode_jpeg(base, quality=95)))

    g = cv2.cvtColor(np.array(base), cv2.COLOR_RGB2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.2, tileGridSize=(8, 8))
    g2 = clahe.apply(g)
    out.append(
        (
            f"{label_prefix}_clahe",
            _encode_jpeg(Image.fromarray(cv2.cvtColor(g2, cv2.COLOR_GRAY2RGB)), quality=95),
        )
    )

    bw = cv2.adaptiveThreshold(g2, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 8)
    out.append(
        (
            f"{label_prefix}_bw",
            _encode_jpeg(Image.fromarray(cv2.cvtColor(bw, cv2.COLOR_GRAY2RGB)), quality=95),
        )
    )
    return out


def make_water_deterministic_row_variants(img_bytes: bytes, max_variants: int = 12) -> list[tuple[str, bytes]]:
    """
    Deterministic geometry-first extractor for dark water-meter photos:
    1) detect main meter circle
    2) crop several fixed odometer-row windows relative to the circle
    3) return enhanced row variants for cell reading.
    """
    out: list[tuple[str, bytes]] = []
    try:
        arr = np.frombuffer(img_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return out
        h, w = im.shape[:2]

        # Hough circle search on full-res dark photos is expensive.
        # Detect circles on a downscaled frame, then map circles back.
        det_max_side = 800
        det_scale = 1.0
        det_im = im
        if max(h, w) > det_max_side:
            det_scale = float(det_max_side) / float(max(h, w))
            dw = max(1, int(round(w * det_scale)))
            dh = max(1, int(round(h * det_scale)))
            det_im = cv2.resize(im, (dw, dh), interpolation=cv2.INTER_AREA)
        else:
            dh, dw = h, w

        gray = cv2.cvtColor(det_im, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 5)
        gray = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)

        circles_all: list[tuple[int, int, int]] = []
        min_r = max(16, min(dw, dh) // 20)
        max_r = max(48, min(dw, dh) // 2)
        for p2 in (22, 26, 30):
            circles = cv2.HoughCircles(
                gray,
                cv2.HOUGH_GRADIENT,
                dp=1.2,
                minDist=max(20, min(dw, dh) // 7),
                param1=90,
                param2=p2,
                minRadius=min_r,
                maxRadius=max_r,
            )
            if circles is None:
                continue
            found = np.round(circles[0, :]).astype(int).tolist()
            found.sort(key=lambda c: int(c[2]), reverse=True)
            for c in found[:24]:
                circles_all.append((int(c[0]), int(c[1]), int(c[2])))
            if len(circles_all) >= 72:
                break
        if not circles_all:
            return out

        if det_scale != 1.0:
            inv = 1.0 / float(det_scale)
            circles_all = [
                (
                    int(round(x * inv)),
                    int(round(y * inv)),
                    int(round(r * inv)),
                )
                for x, y, r in circles_all
            ]

        min_side = float(max(1, min(w, h)))
        filtered_circles = []
        for x, y, r in circles_all:
            rn = float(r) / min_side
            # Drop implausible circles that usually produce huge slow crops.
            if rn < 0.06 or rn > 0.34:
                continue
            filtered_circles.append((x, y, r))
        if filtered_circles:
            circles_all = filtered_circles

        uniq = _unique_circles(circles_all)
        if not uniq:
            return out

        def _circle_score(c: tuple[int, int, int]) -> float:
            x, y, r = c
            y_norm = float(y) / float(max(1, h))
            x_norm = float(x) / float(max(1, w))
            r_norm = float(r) / float(max(1, min(w, h)))
            pos_score = 1.0 - min(1.0, abs(y_norm - 0.64))
            center_score = 1.0 - min(1.0, abs(x_norm - 0.52))
            size_score = 1.0 - min(1.0, abs(r_norm - 0.17) / 0.17)
            return (size_score * 2.6) + (pos_score * 2.0) + (center_score * 0.9)

        uniq = sorted(uniq, key=_circle_score, reverse=True)[:3]

        box_specs = (
            (-0.84, -0.38, 0.86, 0.02),
            (-0.78, -0.30, 0.80, 0.10),
            (-0.92, -0.34, 0.92, 0.08),
        )
        for ci, (x, y, r) in enumerate(uniq, start=1):
            for bi, (lx, ty, rx, by) in enumerate(box_specs, start=1):
                x1, y1, x2, y2 = _clamp_box(
                    int(round(x + r * lx)),
                    int(round(y + r * ty)),
                    int(round(x + r * rx)),
                    int(round(y + r * by)),
                    w,
                    h,
                )
                crop = im[y1:y2, x1:x2]
                if crop.size == 0:
                    continue
                ch, cw = crop.shape[:2]
                if cw > int(w * 0.68) or ch > int(h * 0.30):
                    continue
                # Slight rotation sweep for perspective errors.
                for ai, ang in enumerate((-4.0, 0.0, 4.0), start=1):
                    M = cv2.getRotationMatrix2D((crop.shape[1] / 2.0, crop.shape[0] / 2.0), ang, 1.0)
                    rc = cv2.warpAffine(
                        crop,
                        M,
                        (crop.shape[1], crop.shape[0]),
                        flags=cv2.INTER_CUBIC,
                        borderMode=cv2.BORDER_REPLICATE,
                    )
                    prefix = f"det_row_c{ci}_b{bi}_a{ai}"
                    out.extend(_enhance_row_variants(rc, prefix))
                    if len(out) >= max_variants:
                        return out[:max_variants]
    except Exception:
        return out
    return out[:max_variants]


def make_fixed_cells_sheet_from_row(
    row_bytes: bytes,
    *,
    black_len: int = 5,
    red_len: int = 3,
) -> Optional[tuple[bytes, int]]:
    """
    Deterministic fallback splitter when contour-based cell segmentation fails:
    split row strip into fixed B1..B5 + R1..R3 windows.
    """
    try:
        arr = np.frombuffer(row_bytes, dtype=np.uint8)
        im = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if im is None:
            return None
        h, w = im.shape[:2]
        if w < 24 or h < 24:
            return None

        total = black_len + red_len
        x1 = int(w * 0.05)
        x2 = int(w * 0.97)
        y1 = int(h * 0.08)
        y2 = int(h * 0.92)
        span = max(8, x2 - x1)
        cell_w = max(10, int(span / total))

        cells: list[np.ndarray] = []
        for i in range(total):
            cx1 = x1 + i * cell_w
            cx2 = x1 + (i + 1) * cell_w
            if i == total - 1:
                cx2 = x2
            cx1, _y1, cx2, _y2 = _clamp_box(cx1, y1, cx2, y2, w, h)
            c = im[_y1:_y2, cx1:cx2]
            if c.size == 0:
                return None
            p = Image.fromarray(cv2.cvtColor(c, cv2.COLOR_BGR2RGB))
            p = p.resize((180, 220), Image.Resampling.LANCZOS)
            p = ImageEnhance.Contrast(p).enhance(2.05)
            p = ImageEnhance.Sharpness(p).enhance(1.7)
            p = p.filter(ImageFilter.UnsharpMask(radius=1, percent=280, threshold=2))
            cells.append(np.array(p))
        if len(cells) != total:
            return None

        tile_w = 180
        tile_h = 220
        gap = 16
        margin = 18
        width = margin * 2 + max(black_len, red_len) * tile_w + max(0, max(black_len, red_len) - 1) * gap
        height = margin * 2 + tile_h * 2 + 54
        sheet = Image.new("RGB", (width, height), (245, 245, 245))

        for i in range(black_len):
            x = margin + i * (tile_w + gap)
            y = margin + 24
            sheet.paste(Image.fromarray(cells[i]), (x, y))
        for i in range(red_len):
            x = margin + i * (tile_w + gap)
            y = margin + tile_h + 36
            sheet.paste(Image.fromarray(cells[black_len + i]), (x, y))

        return _encode_jpeg(sheet, quality=95), red_len
    except Exception:
        return None
