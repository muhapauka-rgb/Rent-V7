#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import cv2
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
OCR_SERVICE_DIR = ROOT / "ocr-service"
if str(OCR_SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(OCR_SERVICE_DIR))

from local_digit_classifier import (  # noqa: E402
    LOCAL_DIGIT_CLASSIFIER_CONTRACT_VERSION,
    prepare_digit_template,
    split_digit_cells,
)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception as exc:
                raise SystemExit(f"invalid jsonl at {path}:{line_no}: {exc}") from exc
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _safe_digit_string(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _target_digits(row: dict[str, Any]) -> tuple[Optional[str], str]:
    zone = row.get("zone") if isinstance(row.get("zone"), dict) else {}
    label = row.get("label") if isinstance(row.get("label"), dict) else {}
    kind = str(zone.get("kind_hint") or "")

    integer_digits = _safe_digit_string(label.get("integer_digits"))
    decimal_digits = _safe_digit_string(label.get("decimal_digits"))
    electric_digits = _safe_digit_string(label.get("digits"))

    if kind == "water_odometer" and integer_digits and decimal_digits:
        return (integer_digits[-5:].rjust(5, "0") + decimal_digits[:2].ljust(2, "0"))[:7], "water"
    if kind == "electric_display" and electric_digits:
        return electric_digits[-6:].rjust(6, "0"), "electric"

    # Component rows are useful once they are explicitly labeled by source data.
    # Keep this conservative: do not train water/electric from ambiguous rows
    # unless only one label shape is present.
    if kind == "digit_row":
        if integer_digits and decimal_digits and not electric_digits:
            return (integer_digits[-5:].rjust(5, "0") + decimal_digits[:2].ljust(2, "0"))[:7], "water"
        if electric_digits and not (integer_digits and decimal_digits):
            return electric_digits[-6:].rjust(6, "0"), "electric"
    return None, ""


def _resolve_crop_path(manifest_path: Path, row: dict[str, Any]) -> Optional[Path]:
    raw = row.get("crop_path")
    if not raw:
        return None
    p = Path(str(raw))
    if not p.is_absolute():
        p = manifest_path.parent / p
    return p.resolve()


def _append_templates_from_row(
    *,
    manifest_path: Path,
    row: dict[str, Any],
    input_size: int,
    source_prefixes: list[str],
    templates: list[np.ndarray],
    labels: list[int],
) -> tuple[int, str]:
    if source_prefixes:
        zone = row.get("zone") if isinstance(row.get("zone"), dict) else {}
        source = str(zone.get("source") or "")
        if not any(source.startswith(prefix) for prefix in source_prefixes):
            return 0, "source_filtered"
    digits, label_kind = _target_digits(row)
    if not digits:
        return 0, "unlabeled"
    crop_path = _resolve_crop_path(manifest_path, row)
    if crop_path is None or not crop_path.exists():
        return 0, "missing_crop"
    crop = cv2.imread(str(crop_path), cv2.IMREAD_COLOR)
    if crop is None or crop.size == 0:
        return 0, "decode_failed"
    cells = split_digit_cells(crop, len(digits))
    if len(cells) != len(digits):
        return 0, "split_failed"
    for cell, digit in zip(cells, digits):
        templates.append(prepare_digit_template(cell, input_size=input_size))
        labels.append(int(digit))
    return len(digits), label_kind


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a small template digit classifier from exported local-recognizer crops"
    )
    parser.add_argument("--manifest", required=True, help="Path to dataset manifest.jsonl")
    parser.add_argument("--output", required=True, help="Output .npz model path")
    parser.add_argument("--input-size", type=int, default=28)
    parser.add_argument("--min-samples", type=int, default=10)
    parser.add_argument(
        "--source-prefix",
        action="append",
        default=[],
        help="Only train from zones whose source starts with this prefix; can be repeated",
    )
    args = parser.parse_args()

    manifest_path = Path(args.manifest).expanduser().resolve()
    if not manifest_path.exists():
        raise SystemExit(f"manifest not found: {manifest_path}")
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    templates: list[np.ndarray] = []
    labels: list[int] = []
    status_counts: Counter[str] = Counter()
    label_kind_counts: Counter[str] = Counter()
    rows = _load_jsonl(manifest_path)
    for row in rows:
        added, status = _append_templates_from_row(
            manifest_path=manifest_path,
            row=row,
            input_size=int(args.input_size),
            source_prefixes=[str(v) for v in (args.source_prefix or []) if str(v)],
            templates=templates,
            labels=labels,
        )
        status_counts[status] += 1
        if added > 0:
            label_kind_counts[status] += 1

    summary = {
        "contract_version": LOCAL_DIGIT_CLASSIFIER_CONTRACT_VERSION,
        "model_version": f"template-digits-v1:{manifest_path.parent.name}",
        "manifest": str(manifest_path),
        "output": str(output_path),
        "rows": len(rows),
        "digit_samples": len(labels),
        "crop_samples_used": int(sum(label_kind_counts.values())),
        "status_counts": dict(status_counts),
        "label_kind_counts": dict(label_kind_counts),
        "input_size": int(args.input_size),
        "source_prefixes": [str(v) for v in (args.source_prefix or []) if str(v)],
    }

    if len(labels) < int(args.min_samples):
        print(json.dumps({**summary, "error": "not_enough_labeled_samples"}, ensure_ascii=False))
        return 2

    np.savez_compressed(
        output_path,
        templates=np.stack(templates).astype("float32"),
        labels=np.asarray(labels, dtype="int64"),
        meta=np.asarray(json.dumps(summary, ensure_ascii=False)),
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
