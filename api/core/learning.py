from typing import Optional
from sqlalchemy import text

def capture_training_sample(
    conn,
    *,
    apartment_id: int,
    ym: str,
    meter_type: str,
    meter_index: int,
    correct_value: float,
    source: str,
) -> Optional[int]:
    """
    Capture a training sample based on the latest photo_event for this meter.
    Returns sample id if stored, else None.
    """
    mt = str(meter_type)
    mi = int(meter_index or 1)
    ym = str(ym)

    if mt not in ("cold", "hot", "electric"):
        return None

    row = conn.execute(
        text(
            """
            SELECT id, ydisk_path, ocr_reading, meter_value
            FROM photo_events
            WHERE apartment_id=:aid AND ym=:ym
              AND meter_kind=:mt AND meter_index=:mi
              AND ydisk_path IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {"aid": int(apartment_id), "ym": ym, "mt": mt, "mi": mi},
    ).mappings().first()

    if not row:
        return None

    peid = int(row["id"])
    # avoid duplicates for the same photo_event + meter
    exists = conn.execute(
        text(
            """
            SELECT 1
            FROM ocr_training_samples
            WHERE photo_event_id=:peid AND meter_type=:mt AND meter_index=:mi
            LIMIT 1
            """
        ),
        {"peid": peid, "mt": mt, "mi": mi},
    ).fetchone()
    if exists:
        return None

    ocr_value = row.get("ocr_reading")
    try:
        ocr_val_f = float(ocr_value) if ocr_value is not None else None
    except Exception:
        ocr_val_f = None

    # Only store if OCR is missing or differs from corrected value.
    if (ocr_val_f is not None) and (abs(float(correct_value) - float(ocr_val_f)) <= 1e-6):
        return None

    ydisk_path = row.get("ydisk_path")
    if isinstance(ydisk_path, str):
        ydisk_path = ydisk_path.strip() or None

    res = conn.execute(
        text(
            """
            INSERT INTO ocr_training_samples(
                apartment_id, ym, meter_type, meter_index,
                photo_event_id, ydisk_path, ocr_value, correct_value, source
            )
            VALUES(
                :aid, :ym, :mt, :mi,
                :peid, :path, :ocr, :correct, :source
            )
            RETURNING id
            """
        ),
        {
            "aid": int(apartment_id),
            "ym": ym,
            "mt": mt,
            "mi": int(mi),
            "peid": int(peid),
            "path": ydisk_path,
            "ocr": ocr_val_f,
            "correct": float(correct_value),
            "source": str(source),
        },
    ).fetchone()

    return int(res[0]) if res else None
