#!/usr/bin/env python3
import argparse
import json
import math
import subprocess
from pathlib import Path


def recognize_file(api_url: str, image_path: Path) -> dict:
    cmd = [
        "curl",
        "-sS",
        "--connect-timeout",
        "3",
        "--max-time",
        "70",
        "-X",
        "POST",
        "-F",
        f"file=@{str(image_path)}",
        api_url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        return {"type": "error", "reading": None, "confidence": 0.0, "notes": f"curl_error:{proc.stderr.strip()}"}
    try:
        payload = json.loads(proc.stdout)
    except Exception:
        return {"type": "error", "reading": None, "confidence": 0.0, "notes": "invalid_json_response"}
    return {
        "type": payload.get("type"),
        "reading": payload.get("reading"),
        "confidence": payload.get("confidence", 0.0),
        "notes": payload.get("notes", ""),
        "detail": payload.get("detail"),
    }


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate electric OCR quality against truth values.")
    parser.add_argument("--photos-dir", required=True, help="Directory with electric meter photos")
    parser.add_argument("--truth-json", required=True, help="JSON mapping: filename -> expected reading")
    parser.add_argument("--api-url", default="http://127.0.0.1:8002/recognize", help="OCR recognize endpoint")
    parser.add_argument(
        "--mode",
        choices=["full", "cheap"],
        default="cheap",
        help="cheap: evaluate only 3 key files; full: evaluate all files from truth",
    )
    args = parser.parse_args()

    photos_dir = Path(args.photos_dir)
    truth_path = Path(args.truth_json)
    truth = json.loads(truth_path.read_text(encoding="utf-8"))

    print("file|expected|recognized|status|delta|type|confidence", flush=True)
    exact = 0
    int_ok = 0
    wrong = 0
    miss = 0

    filenames = sorted(truth.keys())
    if args.mode == "cheap":
        preferred = [
            "photo_2026-03-01 01.34.20.jpeg",
            "photo_2026-03-01 01.34.27.jpeg",
            "photo_2026-03-01 01.34.54.jpeg",
        ]
        filenames = [f for f in preferred if f in truth]

    for filename in filenames:
        expected = float(truth[filename])
        image_path = photos_dir / filename
        if not image_path.exists():
            print(f"{filename}|{expected}|null|miss|inf|missing_file|0", flush=True)
            miss += 1
            continue
        res = recognize_file(args.api_url, image_path)
        status, delta = classify(expected, res.get("reading"))
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
            f"{filename}|{expected}|{res.get('reading')}|{status}|{delta_str}|"
            f"{res.get('type')}|{res.get('confidence')}"
        , flush=True)

    total = len(filenames)
    print("", flush=True)
    print(f"summary total={total} exact={exact} int={int_ok} wrong={wrong} miss={miss}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
