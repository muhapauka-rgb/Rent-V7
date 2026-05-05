from io import BytesIO

from PIL import Image, ImageDraw
import numpy as np

from local_digit_classifier import TemplateDigitClassifier, prepare_digit_template
from local_recognizer import (
    LOCAL_RECOGNIZER_VERSION,
    _electric_candidate_from_digits,
    _water_candidate_from_digits,
    run_local_meter_shadow,
)


def _synthetic_digit_row() -> bytes:
    img = Image.new("RGB", (640, 260), "white")
    draw = ImageDraw.Draw(img)
    # Draw blocky digit-like components; the contract test should not depend on
    # any font or external OCR engine.
    x = 90
    for idx in range(7):
        color = (200, 0, 0) if idx >= 5 else (0, 0, 0)
        draw.rectangle((x, 95, x + 28, 160), fill=color)
        draw.rectangle((x + 8, 105, x + 20, 150), fill="white")
        x += 54
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def test_water_candidate_from_digits_keeps_two_decimals():
    candidate = _water_candidate_from_digits("0088438")
    assert candidate is not None
    assert candidate["reading"] == 884.38
    assert candidate["integer_digits"] == "00884"
    assert candidate["decimal_digits"] == "38"
    assert candidate["decimal_digits_count"] == 2


def test_electric_candidate_from_digits_places_decimal():
    candidate = _electric_candidate_from_digits("567473")
    assert candidate is not None
    assert candidate["reading"] == 5674.73
    assert candidate["digits"] == "567473"


def test_local_shadow_contract_without_tesseract():
    payload = run_local_meter_shadow(_synthetic_digit_row(), tesseract_enabled=False)
    assert payload["version"] == LOCAL_RECOGNIZER_VERSION
    assert payload["status"] == "ok"
    assert payload["mode"] == "shadow"
    assert payload["tesseract_enabled"] is False
    assert isinstance(payload["zones"], list)
    assert "water_candidates" in payload
    assert "electric_candidates" in payload


def test_digit_template_shape_and_dtype():
    cell = np.full((30, 18, 3), 240, dtype=np.uint8)
    cell[:, 7:11, :] = 20
    arr = prepare_digit_template(cell)
    assert arr.shape == (28, 28)
    assert arr.dtype == np.float32


def test_template_digit_classifier_predicts_synthetic_digit(tmp_path):
    cell = np.full((30, 18, 3), 240, dtype=np.uint8)
    cell[:, 7:11, :] = 20
    template = prepare_digit_template(cell)
    model_path = tmp_path / "digits.npz"
    np.savez_compressed(
        model_path,
        templates=np.stack([template]).astype("float32"),
        labels=np.asarray([7], dtype="int64"),
        meta=np.asarray('{"model_version":"test-template","input_size":28}'),
    )
    clf = TemplateDigitClassifier(model_path)
    pred = clf.predict_digit(cell)
    assert pred is not None
    assert pred.digit == 7
    assert pred.model_version == "test-template"
