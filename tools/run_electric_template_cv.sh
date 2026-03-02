#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/ponch/rent-system/backend-core"
PHOTOS_DIR="$ROOT/tmp/real-photos/Electro"
TRUTH_JSON="$ROOT/tools/electric_truth.json"
CONTAINER="rent_ocr"

if [ ! -d "$PHOTOS_DIR" ]; then
  echo "photos_dir_not_found: $PHOTOS_DIR" >&2
  exit 1
fi
if [ ! -f "$TRUTH_JSON" ]; then
  echo "truth_json_not_found: $TRUTH_JSON" >&2
  exit 1
fi

echo "sync photos/truth into container..."
docker compose -p backend-core exec -T ocr-service mkdir -p /tmp/electro_eval
while IFS= read -r -d '' f; do
  docker cp "$f" "$CONTAINER:/tmp/electro_eval/$(basename "$f")"
done < <(find "$PHOTOS_DIR" -type f -name "*.jpeg" -print0)
docker cp "$TRUTH_JSON" "$CONTAINER:/tmp/electric_truth.json"

echo "run leave-one-out CV..."
docker compose -p backend-core exec -T ocr-service python - <<'PY'
import json
import math
from pathlib import Path

import app
from PIL import Image

app.OCR_ELECTRIC_TEMPLATE_MATCH = True
app.OCR_ELECTRIC_DETERMINISTIC = False
app.OCR_ELECTRIC_TEMPLATE_DB = "/tmp/electric_templates_cv.json"

truth = json.loads(Path("/tmp/electric_truth.json").read_text(encoding="utf-8"))
base = Path("/tmp/electro_eval")

boxes = {
    "full": (0.00, 0.00, 1.00, 1.00),
    "lcd_wide": (0.08, 0.30, 0.84, 0.70),
    "lcd_mid": (0.14, 0.36, 0.72, 0.60),
    "lcd_tight": (0.18, 0.40, 0.70, 0.55),
}


def dhash(img: Image.Image, size: int = 8) -> int:
    g = img.convert("L").resize((size + 1, size), Image.Resampling.BILINEAR)
    px = list(g.getdata())
    bits = 0
    k = 0
    for y in range(size):
        row = y * (size + 1)
        for x in range(size):
            if px[row + x] > px[row + x + 1]:
                bits |= (1 << k)
            k += 1
    return bits


def crop(img: Image.Image, box: tuple[float, float, float, float]) -> Image.Image:
    w, h = img.size
    x1, y1, x2, y2 = box
    return img.crop(
        (
            int(round(w * x1)),
            int(round(h * y1)),
            int(round(w * x2)),
            int(round(h * y2)),
        )
    )


def build_template_file(train_filenames: list[str]) -> None:
    rows = []
    for fn in train_filenames:
        p = base / fn
        if not p.exists():
            continue
        img = Image.open(p).convert("RGB")
        hashes = {k: format(dhash(crop(img, b)), "016x") for k, b in boxes.items()}
        rows.append(
            {
                "filename": fn,
                "type": "Электро",
                "reading": float(truth[fn]),
                "serial": None,
                "hashes": hashes,
            }
        )
    Path("/tmp/electric_templates_cv.json").write_text(
        json.dumps({"version": 1, "rows": rows}, ensure_ascii=False), encoding="utf-8"
    )
    app._ELECTRIC_TEMPLATE_MTIME = -1.0
    app._ELECTRIC_TEMPLATE_ROWS = []


def classify(expected: float, got) -> tuple[str, float]:
    if got is None:
        return "miss", math.inf
    try:
        gv = float(got)
    except Exception:
        return "miss", math.inf
    delta = abs(gv - expected)
    if delta <= 0.05:
        return "exact", delta
    if int(gv) == int(expected):
        return "int", delta
    return "wrong", delta


exact = 0
int_ok = 0
wrong = 0
miss = 0

print("test_file|expected|recognized|status|delta|confidence|variant|train_count")
all_files = sorted(truth.keys())
for test_fn in all_files:
    train_files = [f for f in all_files if f != test_fn]
    build_template_file(train_files)
    b = (base / test_fn).read_bytes()
    cands = app._electric_template_candidates(b)
    best = max(cands, key=lambda c: float(c.get("confidence") or 0.0)) if cands else None
    got = app._normalize_reading(best.get("reading")) if best else None
    conf = float(best.get("confidence") or 0.0) if best else 0.0
    variant = str(best.get("variant") or "") if best else "none"

    status, delta = classify(float(truth[test_fn]), got)
    if status == "exact":
        exact += 1
    elif status == "int":
        int_ok += 1
    elif status == "wrong":
        wrong += 1
    else:
        miss += 1
    delta_str = "inf" if not math.isfinite(delta) else f"{delta:.3f}"
    print(
        f"{test_fn}|{truth[test_fn]}|{got}|{status}|{delta_str}|{conf:.3f}|{variant}|{len(train_files)}"
    )

total = len(all_files)
print("")
print(f"summary total={total} exact={exact} int={int_ok} wrong={wrong} miss={miss}")
PY
