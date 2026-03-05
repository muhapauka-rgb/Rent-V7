from io import BytesIO

import cv2
import numpy as np
from PIL import Image

from water_deterministic import make_fixed_cells_sheet_from_row


def _encode_jpeg_from_bgr(arr: np.ndarray) -> bytes:
    ok, enc = cv2.imencode(".jpg", arr)
    assert ok
    return enc.tobytes()


def test_make_fixed_cells_sheet_from_row_basic():
    h, w = 120, 520
    img = np.full((h, w, 3), 35, dtype=np.uint8)
    # Draw 8 bright pseudo-cells to imitate drum windows.
    margin = 22
    span = w - margin * 2
    cell_w = span // 8
    for i in range(8):
        x1 = margin + i * cell_w
        x2 = margin + (i + 1) * cell_w - 4
        cv2.rectangle(img, (x1, 16), (x2, h - 16), (210, 210, 210), thickness=-1)
        cv2.putText(
            img,
            str(i % 10),
            (x1 + 8, h // 2 + 14),
            cv2.FONT_HERSHEY_SIMPLEX,
            1.0,
            (30, 30, 30),
            2,
            cv2.LINE_AA,
        )

    row_bytes = _encode_jpeg_from_bgr(img)
    packed = make_fixed_cells_sheet_from_row(row_bytes, black_len=5, red_len=3)
    assert packed is not None
    sheet_bytes, red_len = packed
    assert red_len == 3
    assert len(sheet_bytes) > 1000

    im = Image.open(BytesIO(sheet_bytes))
    assert im.width > 600
    assert im.height > 300
