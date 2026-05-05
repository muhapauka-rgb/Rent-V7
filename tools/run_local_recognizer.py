#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OCR_SERVICE_DIR = ROOT / "ocr-service"
if str(OCR_SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(OCR_SERVICE_DIR))

from local_digit_classifier import load_digit_classifier  # noqa: E402
from local_recognizer import run_local_meter_shadow  # noqa: E402


def _compact(payload: dict) -> dict:
    zones = list(payload.get("zones") or [])
    winner = payload.get("winner") or {}
    return {
        "version": payload.get("version"),
        "status": payload.get("status"),
        "elapsed_ms": payload.get("elapsed_ms"),
        "tesseract_enabled": payload.get("tesseract_enabled"),
        "digit_classifier_enabled": payload.get("digit_classifier_enabled"),
        "digit_classifier_version": payload.get("digit_classifier_version"),
        "winner": winner,
        "zones": [
            {
                "id": z.get("id"),
                "kind_hint": z.get("kind_hint"),
                "bbox": z.get("bbox"),
                "digit_like_components": z.get("digit_like_components"),
                "geometry_confidence": z.get("geometry_confidence"),
                "red_pixel_ratio": z.get("red_pixel_ratio"),
                "digit_classifier": z.get("digit_classifier"),
                "tesseract": z.get("tesseract"),
            }
            for z in zones[:6]
            if isinstance(z, dict)
        ],
        "top_water": list(payload.get("water_candidates") or [])[:4],
        "top_electric": list(payload.get("electric_candidates") or [])[:4],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run repo-local local meter recognizer shadow on image files")
    parser.add_argument("paths", nargs="+", help="Image paths")
    parser.add_argument("--max-zones", type=int, default=6)
    parser.add_argument("--tesseract", action="store_true", help="Enable local Tesseract digit attempt")
    parser.add_argument("--tesseract-timeout-sec", type=float, default=0.8)
    parser.add_argument("--existing-water-rows", action="store_true", help="Also include heavier existing water row crops")
    parser.add_argument("--digit-model", default="", help="Optional template .npz or ONNX digit classifier")
    parser.add_argument("--full", action="store_true", help="Print full payload instead of compact summary")
    args = parser.parse_args()

    digit_classifier = load_digit_classifier(args.digit_model)
    rc = 0
    for raw_path in args.paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            print(json.dumps({"file": str(path), "error": "missing"}, ensure_ascii=False))
            rc = 1
            continue
        payload = run_local_meter_shadow(
            path.read_bytes(),
            max_zones=args.max_zones,
            tesseract_enabled=bool(args.tesseract),
            tesseract_timeout_sec=float(args.tesseract_timeout_sec),
            include_existing_water_rows=bool(args.existing_water_rows),
            digit_classifier=digit_classifier,
        )
        row = {"file": str(path), **(payload if args.full else _compact(payload))}
        print(json.dumps(row, ensure_ascii=False))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
