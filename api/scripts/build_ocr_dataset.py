import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

from sqlalchemy import text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.config import engine, logger  # noqa: E402
from core.db import db_ready, ensure_tables  # noqa: E402
from core.integrations import ydisk_ready, ydisk_exists, ydisk_put, ydisk_list, ydisk_delete, ydisk_mkcol  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--rate", type=float, default=0.5)  # seconds between downloads
    parser.add_argument("--ydisk-root", type=str, default=os.getenv("OCR_DATASET_ROOT", "ocr-datasets"))
    parser.add_argument("--keep-months", type=int, default=3)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if not db_ready():
        logger.warning("db not ready")
        return 1
    ensure_tables()

    if not ydisk_ready():
        logger.warning("ydisk not configured")
        return 1

    run_month = datetime.now().strftime("%Y-%m")
    run_date = datetime.now().strftime("%Y-%m-%d")

    with engine.begin() as conn:
        already = conn.execute(
            text("SELECT 1 FROM ocr_training_runs WHERE run_month LIKE :p LIMIT 1"),
            {"p": f"{run_date}%"},
        ).fetchone()
        if already and not args.force:
            logger.info("skip: already ran for %s", run_date)
            return 0

        run_key = run_date if not args.force else datetime.now().strftime("%Y-%m-%d_%H%M%S")
        conn.execute(
            text("INSERT INTO ocr_training_runs(run_month) VALUES(:m)"),
            {"m": run_key},
        )

    labels = []

    processed = 0
    errors = 0
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT s.id, s.apartment_id, s.ym, s.meter_type, s.meter_index,
                       s.photo_event_id, s.ydisk_path, s.ocr_value, s.correct_value, s.source,
                       p.ocr_json, p.diag_json, p.ocr_type, p.ocr_reading, p.meter_kind, p.meter_value, p.stage, p.stage_updated_at
                FROM ocr_training_samples s
                LEFT JOIN photo_events p ON p.id = s.photo_event_id
                WHERE s.processed_at IS NULL
                  AND s.meter_type IN ('cold','hot','electric')
                ORDER BY s.id ASC
                LIMIT :limit
                """
            ),
            {"limit": int(args.limit)},
        ).mappings().all()

    if not rows:
        logger.info("no new samples")
        return 0

    for r in rows:
        try:
            ypath = r.get("ydisk_path")
            if not ypath:
                continue

            # if photo is missing — skip quietly
            if not ydisk_exists(ypath):
                errors += 1
                continue

            ocr_json = r.get("ocr_json") or {}
            if isinstance(ocr_json, str):
                try:
                    ocr_json = json.loads(ocr_json)
                except Exception:
                    ocr_json = {}
            diag_json = r.get("diag_json") or {}
            if isinstance(diag_json, str):
                try:
                    diag_json = json.loads(diag_json)
                except Exception:
                    diag_json = {}

            ocr_val = float(r["ocr_value"]) if r["ocr_value"] is not None else None
            correct_val = float(r["correct_value"])
            delta_abs = None
            delta_rel = None
            if ocr_val is not None:
                delta_abs = abs(correct_val - float(ocr_val))
                if abs(correct_val) > 1e-9:
                    delta_rel = delta_abs / abs(correct_val)

            label_reason = "ocr_missing" if ocr_val is None else "ocr_mismatch"

            label = {
                "image": ypath,  # keep only metadata with ydisk path
                "apartment_id": int(r["apartment_id"]),
                "ym": r["ym"],
                "meter_type": r["meter_type"],
                "meter_index": int(r["meter_index"] or 1),
                "ocr_value": ocr_val,
                "correct_value": correct_val,
                "delta_abs": delta_abs,
                "delta_rel": delta_rel,
                "label_reason": label_reason,
                "source": r.get("source"),
                "photo_event_id": int(r["photo_event_id"] or 0),
                "ocr_type": (r.get("ocr_type") or ocr_json.get("type")),
                "ocr_confidence": (ocr_json.get("confidence") if isinstance(ocr_json, dict) else None),
                "ocr_notes": (ocr_json.get("notes") if isinstance(ocr_json, dict) else None),
                "diag_warnings": (diag_json.get("warnings") if isinstance(diag_json, dict) else None),
                "stage": r.get("stage"),
                "stage_updated_at": (str(r.get("stage_updated_at")) if r.get("stage_updated_at") is not None else None),
            }
            labels.append(label)

            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE ocr_training_samples SET processed_at=now() WHERE id=:id"),
                    {"id": int(r["id"])},
                )

            processed += 1
            time.sleep(max(0.0, float(args.rate)))
        except Exception:
            errors += 1
            continue

    if labels:
        payload = "\n".join(json.dumps(x, ensure_ascii=False) for x in labels).encode("utf-8")
        fname = f"labels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        root = args.ydisk_root.strip("/")
        ydisk_mkcol(root)
        ydisk_mkcol(f"{root}/{run_month}")
        remote_path = f"{root}/{run_month}/{fname}"
        ydisk_put(remote_path, payload)

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE ocr_training_runs SET finished_at=now() WHERE run_month=:m"),
            {"m": run_date},
        )
        msg = f"OCR датасет {run_month} ({run_date}): обработано {processed}, ошибок {errors}"
        conn.execute(
            text(
                """
                INSERT INTO notifications(
                    chat_id, telegram_username, apartment_id, type, message, related, status, created_at
                )
                VALUES(NULL, NULL, NULL, 'ocr_training', :message, NULL, 'unread', now())
                """
            ),
            {"message": msg},
        )

    # keep only last N months on ydisk
    try:
        keep = max(1, int(args.keep_months))
        now = datetime.now()
        keep_set = set()
        for i in range(keep):
            if i == 0:
                dt = now
            else:
                dt = now.replace(day=1)
                for _ in range(i):
                    dt = (dt.replace(day=1) - timedelta(days=1))
            keep_set.add(dt.strftime("%Y-%m"))

        folders = ydisk_list(args.ydisk_root.strip("/"))
        for name in folders:
            if name not in keep_set:
                ydisk_delete(f"{args.ydisk_root.strip('/')}/{name}")
    except Exception:
        pass

    logger.info("done. processed=%s", processed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
