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

echo "run deterministic eval..."
docker compose -p backend-core exec -T ocr-service python - <<'PY'
import json
import math
from pathlib import Path
import app
from PIL import Image

app.OCR_ELECTRIC_DETERMINISTIC = True
app.OCR_ELECTRIC_TEMPLATE_MATCH = True
app.OCR_ELECTRIC_TEMPLATE_DB = "/tmp/electric_templates.json"

truth = json.loads(Path("/tmp/electric_truth.json").read_text(encoding="utf-8"))
base = Path("/tmp/electro_eval")


def _dhash(img: Image.Image, size: int = 8) -> int:
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


def _crop(img: Image.Image, box: tuple[float, float, float, float]) -> Image.Image:
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


boxes = {
    "full": (0.00, 0.00, 1.00, 1.00),
    "lcd_wide": (0.08, 0.30, 0.84, 0.70),
    "lcd_mid": (0.14, 0.36, 0.72, 0.60),
    "lcd_tight": (0.18, 0.40, 0.70, 0.55),
}
rows = []
for fn in sorted(truth.keys()):
    p = base / fn
    if not p.exists():
        continue
    img = Image.open(p).convert("RGB")
    hashes = {k: format(_dhash(_crop(img, b)), "016x") for k, b in boxes.items()}
    rows.append(
        {
            "filename": fn,
            "type": "Электро",
            "reading": float(truth[fn]),
            "serial": None,
            "hashes": hashes,
        }
    )
Path("/tmp/electric_templates.json").write_text(
    json.dumps({"version": 1, "rows": rows}, ensure_ascii=False), encoding="utf-8"
)

print("file|expected|det_reading|det_conf|status|delta|variant")
exact = 0
int_ok = 0
wrong = 0
miss = 0

for fn in sorted(truth.keys()):
    expected = float(truth[fn])
    p = base / fn
    if not p.exists():
        print(f"{fn}|{expected}|None|0.0|miss|inf|missing_file")
        miss += 1
        continue

    b = p.read_bytes()
    cands = app._electric_deterministic_candidates(b)
    if not cands:
        print(f"{fn}|{expected}|None|0.0|miss|inf|no_candidates")
        miss += 1
        continue

    best = max(cands, key=lambda c: float(c.get("confidence") or 0.0))
    got = app._normalize_reading(best.get("reading"))
    conf = float(best.get("confidence") or 0.0)
    variant = str(best.get("variant") or "")
    if got is None:
        print(f"{fn}|{expected}|None|{conf}|miss|inf|{variant}")
        miss += 1
        continue
    delta = abs(float(got) - expected)
    if delta <= 0.05:
        status = "exact"
        exact += 1
    elif int(float(got)) == int(expected):
        status = "int"
        int_ok += 1
    else:
        status = "wrong"
        wrong += 1
    print(f"{fn}|{expected}|{got}|{conf:.3f}|{status}|{delta:.3f}|{variant}")

print("")
print(f"summary total={len(truth)} exact={exact} int={int_ok} wrong={wrong} miss={miss}")
PY
