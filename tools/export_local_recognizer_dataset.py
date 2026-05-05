#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional


ROOT = Path(__file__).resolve().parents[1]
OCR_SERVICE_DIRS = [
    Path(os.getenv("OCR_SERVICE_DIR", "")).expanduser() if os.getenv("OCR_SERVICE_DIR") else None,
    ROOT / "ocr-service",
    Path("/app"),
]
for candidate in OCR_SERVICE_DIRS:
    if candidate and candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from PIL import Image  # noqa: E402
from local_recognizer import LOCAL_RECOGNIZER_VERSION, run_local_meter_shadow  # noqa: E402


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_name(value: Any, fallback: str = "item") -> str:
    s = str(value or "").strip()
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    s = s.strip("._-")
    return s[:80] or fallback


def _load_truth_tsv(path: Optional[Path]) -> dict[str, dict[str, Any]]:
    if not path or not path.exists():
        return {}
    out: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as fh:
        rdr = csv.DictReader(fh, delimiter="\t")
        for row in rdr:
            filename = str(row.get("filename") or "").strip()
            reading_raw = str(row.get("reading") or row.get("value") or "").strip().replace(",", ".")
            if not filename or not reading_raw:
                continue
            try:
                reading = float(reading_raw)
            except Exception:
                continue
            meter_type = str(row.get("meter_type") or row.get("type") or "water").strip() or "water"
            out[filename] = {"meter_type": meter_type, "reading": reading, "source": str(path)}
    return out


def _load_truth_json(path: Optional[Path]) -> dict[str, dict[str, Any]]:
    if not path or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, dict[str, Any]] = {}
    if isinstance(payload, dict) and isinstance(payload.get("cases"), dict):
        for case_name, case in payload["cases"].items():
            if not isinstance(case, dict):
                continue
            case_path = Path(str(case.get("path") or case_name)).name
            expected = case.get("ocr") if isinstance(case.get("ocr"), dict) else {}
            reading = expected.get("reading")
            if reading is None and isinstance(case.get("api"), dict):
                reading = case["api"].get("ocr_reading")
            if reading is None:
                continue
            meter_type = expected.get("type") or (case.get("api") or {}).get("meter_kind") or "unknown"
            out[case_path] = {
                "meter_type": meter_type,
                "reading": float(reading),
                "serial": expected.get("serial") or (case.get("api") or {}).get("ocr_serial"),
                "source": str(path),
            }
            out[str(case_name)] = dict(out[case_path])
        return out
    if isinstance(payload, dict):
        for filename, value in payload.items():
            if isinstance(value, dict):
                reading = value.get("reading") or value.get("value") or value.get("ocr_reading")
                meter_type = value.get("meter_type") or value.get("type") or "unknown"
                serial = value.get("serial")
            else:
                reading = value
                meter_type = "electric"
                serial = None
            if reading is None:
                continue
            out[str(filename)] = {
                "meter_type": meter_type,
                "reading": float(reading),
                "serial": serial,
                "source": str(path),
            }
    return out


def _reading_digits(reading: Optional[float], meter_type: str) -> dict[str, Any]:
    if reading is None:
        return {}
    mt = str(meter_type or "").strip().lower()
    try:
        scaled = int(round(float(reading) * 100.0))
    except Exception:
        return {}
    if mt in ("water", "cold", "hot", "хвс", "гвс", "unknown"):
        whole = int(float(reading))
        frac = abs(scaled) % 100
        return {
            "integer_digits": str(whole).rjust(5, "0")[-5:],
            "decimal_digits": f"{frac:02d}",
            "decimal_digits_count": 2,
        }
    return {
        "digits": str(max(0, scaled)).rjust(6, "0")[-6:],
        "decimal_digits_count": 2,
    }


def _resolve_label(path: Path, truth: dict[str, dict[str, Any]]) -> dict[str, Any]:
    keys = [path.name, str(path), path.stem]
    for key in keys:
        if key in truth:
            label = dict(truth[key])
            label.update(_reading_digits(label.get("reading"), str(label.get("meter_type") or "")))
            return label
    return {}


def _crop_image(img: Image.Image, bbox: dict[str, Any]) -> Optional[Image.Image]:
    try:
        x = int(bbox.get("x"))
        y = int(bbox.get("y"))
        w = int(bbox.get("w"))
        h = int(bbox.get("h"))
    except Exception:
        return None
    if w <= 0 or h <= 0:
        return None
    x1 = max(0, min(img.width - 1, x))
    y1 = max(0, min(img.height - 1, y))
    x2 = max(x1 + 1, min(img.width, x + w))
    y2 = max(y1 + 1, min(img.height, y + h))
    return img.crop((x1, y1, x2, y2)).convert("RGB")


def _iter_manifest_paths(path: Path, dataset_root: Optional[Path]) -> list[Path]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases = payload.get("cases") if isinstance(payload, dict) else None
    if not isinstance(cases, dict):
        return []
    out: list[Path] = []
    for case_name, case in cases.items():
        if not isinstance(case, dict):
            continue
        raw = case.get("path") or case_name
        p = Path(str(raw))
        if not p.is_absolute():
            p = ((dataset_root or path.parent) / p).resolve()
        out.append(p)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Export local recognizer zone crops + manifest for digit-model training")
    parser.add_argument("paths", nargs="*", help="Image paths")
    parser.add_argument("--golden-manifest", default="", help="Optional golden manifest; also used as truth source")
    parser.add_argument("--dataset-root", default="", help="Root for manifest-relative paths")
    parser.add_argument("--truth-tsv", default="", help="Optional TSV with filename/reading[/meter_type]")
    parser.add_argument("--truth-json", default="", help="Optional JSON truth map")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-zones", type=int, default=8)
    parser.add_argument("--tesseract", action="store_true")
    parser.add_argument("--existing-water-rows", action="store_true")
    parser.add_argument("--copy-originals", action="store_true")
    args = parser.parse_args()

    output_dir = Path(args.output_dir).expanduser().resolve()
    crops_dir = output_dir / "crops"
    originals_dir = output_dir / "originals"
    output_dir.mkdir(parents=True, exist_ok=True)
    crops_dir.mkdir(parents=True, exist_ok=True)
    if args.copy_originals:
        originals_dir.mkdir(parents=True, exist_ok=True)

    dataset_root = Path(args.dataset_root).expanduser().resolve() if args.dataset_root else None
    manifest_path = Path(args.golden_manifest).expanduser().resolve() if args.golden_manifest else None
    paths = [Path(p).expanduser().resolve() for p in args.paths]
    if manifest_path:
        paths.extend(_iter_manifest_paths(manifest_path, dataset_root))
    paths = list(dict.fromkeys(paths))
    if not paths:
        raise SystemExit("no input paths")

    truth: dict[str, dict[str, Any]] = {}
    if manifest_path:
        truth.update(_load_truth_json(manifest_path))
    truth.update(_load_truth_json(Path(args.truth_json).expanduser().resolve() if args.truth_json else None))
    truth.update(_load_truth_tsv(Path(args.truth_tsv).expanduser().resolve() if args.truth_tsv else None))

    manifest_rows: list[dict[str, Any]] = []
    image_rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for path in paths:
        if not path.exists():
            missing.append(str(path))
            continue
        img_hash = _sha256(path)
        label = _resolve_label(path, truth)
        img_bytes = path.read_bytes()
        recognizer = run_local_meter_shadow(
            img_bytes,
            max_zones=int(args.max_zones),
            tesseract_enabled=bool(args.tesseract),
            include_existing_water_rows=bool(args.existing_water_rows),
        )
        image_row = {
            "image_path": str(path),
            "file_name": path.name,
            "sha256": img_hash,
            "label": label,
            "recognizer_version": recognizer.get("version"),
            "recognizer_status": recognizer.get("status"),
            "zones_count": len(recognizer.get("zones") or []),
        }
        if args.copy_originals:
            original_name = f"{img_hash[:16]}__{_safe_name(path.name)}"
            original_path = originals_dir / original_name
            original_path.write_bytes(img_bytes)
            image_row["original_copy"] = str(original_path.relative_to(output_dir))
        image_rows.append(image_row)

        try:
            pil = Image.open(path).convert("RGB")
        except Exception:
            continue

        for zone in list(recognizer.get("zones") or []):
            if not isinstance(zone, dict):
                continue
            bbox = zone.get("bbox")
            if not isinstance(bbox, dict):
                continue
            crop = _crop_image(pil, bbox)
            if crop is None:
                continue
            zone_id = _safe_name(zone.get("id"), "zone")
            kind = _safe_name(zone.get("kind_hint"), "unknown")
            source = _safe_name(zone.get("source"), "source")
            crop_name = f"{img_hash[:16]}__{zone_id}__{kind}__{source}.jpg"
            crop_path = crops_dir / crop_name
            crop.save(crop_path, format="JPEG", quality=94)
            row = {
                "sample_id": f"{img_hash[:16]}:{zone_id}",
                "image_path": str(path),
                "file_name": path.name,
                "sha256": img_hash,
                "crop_path": str(crop_path.relative_to(output_dir)),
                "zone": zone,
                "label": label,
                "recognizer_version": LOCAL_RECOGNIZER_VERSION,
            }
            manifest_rows.append(row)

    manifest_file = output_dir / "manifest.jsonl"
    manifest_file.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in manifest_rows) + ("\n" if manifest_rows else ""),
        encoding="utf-8",
    )
    images_file = output_dir / "images.jsonl"
    images_file.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in image_rows) + ("\n" if image_rows else ""),
        encoding="utf-8",
    )
    summary = {
        "version": 1,
        "local_recognizer_version": LOCAL_RECOGNIZER_VERSION,
        "images": len(image_rows),
        "crop_samples": len(manifest_rows),
        "missing": missing,
        "output_dir": str(output_dir),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
