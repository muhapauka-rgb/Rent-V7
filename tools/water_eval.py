#!/usr/bin/env python3
import argparse
import csv
import json
from pathlib import Path
from typing import Any

import requests


def load_truth(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    out: dict[str, float] = {}
    with path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        for row in rdr:
            fn = str(row.get("filename") or "").strip()
            raw = str(row.get("reading") or "").strip()
            if not fn or not raw:
                continue
            try:
                out[fn] = float(raw.replace(",", "."))
            except Exception:
                continue
    return out


def status_of(got: float | None, expected: float | None, notes: str) -> str:
    if got is None:
        if "timed out" in notes.lower() or "timeout" in notes.lower():
            return "timeout"
        return "unknown"
    if expected is None:
        return "no_truth"
    if abs(got - expected) <= 0.05:
        return "exact"
    if int(got) == int(expected):
        return "int_only"
    return "wrong"


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate water OCR without context hints")
    ap.add_argument("--photos-dir", required=True)
    ap.add_argument("--endpoint", default="http://127.0.0.1:8000/recognize")
    ap.add_argument("--truth-tsv", default="")
    ap.add_argument("--timeout-sec", type=float, default=45.0)
    args = ap.parse_args()

    photos_dir = Path(args.photos_dir)
    truth_path = Path(args.truth_tsv) if args.truth_tsv else photos_dir / "GROUND_TRUTH.tsv"
    truth = load_truth(truth_path)

    files = sorted(
        [
            p
            for p in photos_dir.iterdir()
            if p.is_file() and p.suffix.lower() in {".jpeg", ".jpg", ".png"}
        ]
    )

    rows: list[dict[str, Any]] = []
    for p in files:
        notes = ""
        row: dict[str, Any]
        try:
            with p.open("rb") as fh:
                r = requests.post(
                    args.endpoint,
                    files={"file": (p.name, fh, "image/jpeg")},
                    data={"trace_id": f"water-eval-{p.stem}"},
                    timeout=args.timeout_sec,
                )
            r.raise_for_status()
            j = r.json()
            notes = str(j.get("notes") or "")
            row = {
                "filename": p.name,
                "type": j.get("type"),
                "reading": j.get("reading"),
                "confidence": j.get("confidence"),
                "provider": j.get("provider"),
                "variant": j.get("variant"),
                "notes": notes,
            }
        except Exception as e:
            notes = f"failed:{e}"
            row = {
                "filename": p.name,
                "type": "unknown",
                "reading": None,
                "confidence": 0.0,
                "provider": None,
                "variant": None,
                "notes": notes,
            }

        expected = truth.get(p.name)
        try:
            got = None if row.get("reading") is None else float(row.get("reading"))
        except Exception:
            got = None
        row["expected"] = expected
        row["status"] = status_of(got, expected, notes)
        rows.append(row)

    print("filename\tstatus\texpected\ttype\treading\tconfidence\tprovider\tvariant\tnotes")
    for r in rows:
        notes = str(r.get("notes") or "").replace("\t", " ")
        print(
            f"{r.get('filename')}\t{r.get('status')}\t{r.get('expected')}\t{r.get('type')}\t{r.get('reading')}\t"
            f"{r.get('confidence')}\t{r.get('provider')}\t{r.get('variant')}\t{notes}"
        )

    summary: dict[str, int] = {}
    for r in rows:
        k = str(r.get("status") or "unknown")
        summary[k] = summary.get(k, 0) + 1
    print("")
    print("summary", json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
