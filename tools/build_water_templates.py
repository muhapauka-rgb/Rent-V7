#!/usr/bin/env python3
import argparse
import csv
import json
from pathlib import Path

import cv2
import numpy as np


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


def _water_template_hashes(img_bytes: bytes) -> dict[str, int]:
    arr = cv2.imdecode(np.frombuffer(img_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    if arr is None:
        return {}
    h, w = arr.shape[:2]
    if h < 10 or w < 10:
        return {}

    def _crop_hash(box: tuple[float, float, float, float]) -> int:
        x1 = max(0, min(w - 1, int(round(w * box[0]))))
        y1 = max(0, min(h - 1, int(round(h * box[1]))))
        x2 = max(x1 + 1, min(w, int(round(w * box[2]))))
        y2 = max(y1 + 1, min(h, int(round(h * box[3]))))
        g = cv2.cvtColor(arr[y1:y2, x1:x2], cv2.COLOR_BGR2GRAY)
        return _dhash_from_gray(g, size=8)

    return {
        'full': _crop_hash((0.00, 0.00, 1.00, 1.00)),
        'mid': _crop_hash((0.10, 0.22, 0.90, 0.88)),
        'center': _crop_hash((0.18, 0.30, 0.82, 0.78)),
    }


def load_truth_tsv(path: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    with path.open('r', encoding='utf-8') as f:
        rdr = csv.DictReader(f, delimiter='\t')
        for row in rdr:
            fn = str(row.get('filename') or '').strip()
            rv = str(row.get('reading') or '').strip()
            if not fn or not rv:
                continue
            try:
                out[fn] = float(rv.replace(',', '.'))
            except Exception:
                continue
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description='Build water template hashes from labeled photos')
    ap.add_argument('--photos-dir', required=True)
    ap.add_argument('--truth-tsv', required=True)
    ap.add_argument('--output', required=True)
    args = ap.parse_args()

    photos_dir = Path(args.photos_dir)
    truth = load_truth_tsv(Path(args.truth_tsv))

    rows = []
    for filename, reading in sorted(truth.items()):
        p = photos_dir / filename
        if not p.exists():
            continue
        img_bytes = p.read_bytes()
        h = _water_template_hashes(img_bytes)
        if not h:
            continue
        rows.append(
            {
                'filename': filename,
                'type': 'unknown',
                'reading': float(reading),
                'serial': None,
                'hashes': {k: format(v, '016x') for k, v in h.items()},
            }
        )

    out = {
        'version': 1,
        'hash': 'dhash64',
        'count': len(rows),
        'rows': rows,
    }
    Path(args.output).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'wrote {len(rows)} templates -> {args.output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
