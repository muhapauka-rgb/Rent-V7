#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from PIL import Image


BOXES = {
    "full": (0.00, 0.00, 1.00, 1.00),
    "lcd_wide": (0.08, 0.30, 0.84, 0.70),
    "lcd_mid": (0.14, 0.36, 0.72, 0.60),
    "lcd_tight": (0.18, 0.40, 0.70, 0.55),
}


def dhash(img: Image.Image, size: int = 8) -> int:
    gray = img.convert("L").resize((size + 1, size), Image.Resampling.BILINEAR)
    px = list(gray.getdata())
    bits = 0
    bit_idx = 0
    stride = size + 1
    for y in range(size):
        row = y * stride
        for x in range(size):
            if px[row + x] > px[row + x + 1]:
                bits |= (1 << bit_idx)
            bit_idx += 1
    return bits


def crop_rel(img: Image.Image, box: tuple[float, float, float, float]) -> Image.Image:
    w, h = img.size
    x1 = max(0, min(w - 1, int(round(w * box[0]))))
    y1 = max(0, min(h - 1, int(round(h * box[1]))))
    x2 = max(x1 + 1, min(w, int(round(w * box[2]))))
    y2 = max(y1 + 1, min(h, int(round(h * box[3]))))
    return img.crop((x1, y1, x2, y2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build electric template hashes from labeled photos.")
    parser.add_argument("--photos-dir", required=True, help="Directory with electric photos")
    parser.add_argument("--truth-json", required=True, help="JSON map filename->reading")
    parser.add_argument("--output", required=True, help="Output JSON file for template DB")
    args = parser.parse_args()

    photos_dir = Path(args.photos_dir)
    truth = json.loads(Path(args.truth_json).read_text(encoding="utf-8"))

    rows = []
    for filename, reading in sorted(truth.items()):
        p = photos_dir / filename
        if not p.exists():
            continue
        img = Image.open(p).convert("RGB")
        hashes = {k: format(dhash(crop_rel(img, box)), "016x") for k, box in BOXES.items()}
        rows.append(
            {
                "filename": filename,
                "type": "Электро",
                "reading": float(reading),
                "serial": None,
                "hashes": hashes,
            }
        )

    out = {
        "version": 1,
        "hash": "dhash64",
        "count": len(rows),
        "rows": rows,
    }
    Path(args.output).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {len(rows)} templates -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
