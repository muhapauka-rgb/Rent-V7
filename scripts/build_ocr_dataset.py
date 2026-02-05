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
from core.integrations import ydisk_ready, ydisk_exists, ydisk_put, ydisk_list, ydisk_delete  # noqa: E402


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _should_run_today() -> bool:
    now = datetime.now()
    return now.day == 2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--rate", type=float, default=0.5)  # seconds between downloads
    parser.add_argument("--ydisk-root", type=str, default=os.getenv("OCR_DATASET_ROOT", "ocr-datasets"))
    parser.add_argument("--keep-months", type=int, default=3)
    args = parser.parse_args()

    if not db_ready():
        logger.warning("db not ready")
        return 1
    ensure_tables()

    if not ydisk_ready():
        logger.warning("ydisk not configured")
        return 1

    if not _should_run_today():
        logger.info("skip: not the 2nd day of month")
        return 0

    run_month = datetime.now().strftime("%Y-%m")

    with engine.begin() as conn:
        already = conn.execute(
            text("SELECT 1 FROM ocr_training_runs WHERE run_month=:m LIMIT 1"),
            {"m": run_month},
        ).fetchone()
        if already:
            logger.info("skip: already ran for %s", run_month)
            return 0

        conn.execute(
            text("INSERT INTO ocr_training_runs(run_month) VALUES(:m)"),
            {"m": run_month},
        )

    labels = []

    processed = 0
    errors = 0
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, apartment_id, ym, meter_type, meter_index,
                       photo_event_id, ydisk_path, ocr_value, correct_value
                FROM ocr_training_samples
                WHERE processed_at IS NULL
                ORDER BY id ASC
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

            label = {
                "image": ypath,  # keep only metadata with ydisk path
                "apartment_id": int(r["apartment_id"]),
                "ym": r["ym"],
                "meter_type": r["meter_type"],
                "meter_index": int(r["meter_index"] or 1),
                "ocr_value": float(r["ocr_value"]) if r["ocr_value"] is not None else None,
                "correct_value": float(r["correct_value"]),
                "photo_event_id": int(r["photo_event_id"] or 0),
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
        remote_path = f"{args.ydisk_root.strip('/')}/{run_month}/{fname}"
        ydisk_put(remote_path, payload)

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE ocr_training_runs SET finished_at=now() WHERE run_month=:m"),
            {"m": run_month},
        )
        msg = f"OCR датасет {run_month}: обработано {processed}, ошибок {errors}"
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
