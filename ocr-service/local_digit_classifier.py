from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import cv2
import numpy as np


LOCAL_DIGIT_CLASSIFIER_CONTRACT_VERSION = "digit-classifier-contract-v1"


@dataclass(frozen=True)
class DigitPrediction:
    digit: int
    confidence: float
    probabilities: tuple[float, ...]
    model_version: str


@dataclass(frozen=True)
class DigitSequencePrediction:
    digits: str
    confidence: float
    per_digit: tuple[DigitPrediction, ...]
    model_version: str


def prepare_digit_template(crop_bgr: np.ndarray, *, input_size: int = 28) -> np.ndarray:
    """Normalize a digit cell to a small float template.

    Foreground polarity is normalized so darker/red LCD/odometer strokes tend to
    become brighter than the background. This is intentionally simple and stable;
    it is a bootstrap classifier, not the final CNN.
    """
    if crop_bgr.ndim == 3:
        gray = cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2GRAY)
    else:
        gray = crop_bgr
    gray = cv2.resize(gray, (int(input_size), int(input_size)), interpolation=cv2.INTER_AREA)
    gray = cv2.equalizeHist(gray)
    arr = gray.astype("float32") / 255.0
    # Most meter digits are darker than the plate/LCD background. Invert when
    # the crop is predominantly bright to make strokes high-valued.
    if float(np.mean(arr)) > 0.45:
        arr = 1.0 - arr
    arr = arr - float(np.mean(arr))
    std = float(np.std(arr))
    if std > 1e-6:
        arr = arr / std
    return arr.astype("float32")


def split_digit_cells(crop_bgr: np.ndarray, digits_count: int) -> list[np.ndarray]:
    if crop_bgr is None or crop_bgr.size == 0 or digits_count <= 0:
        return []
    h, w = crop_bgr.shape[:2]
    if h < 4 or w < digits_count * 3:
        return []
    cells: list[np.ndarray] = []
    cell_w = float(w) / float(digits_count)
    pad_x = max(1, int(round(cell_w * 0.08)))
    pad_y = max(1, int(round(h * 0.08)))
    for idx in range(int(digits_count)):
        x1 = int(round(idx * cell_w))
        x2 = int(round((idx + 1) * cell_w))
        x1 = max(0, x1 - pad_x)
        x2 = min(w, max(x1 + 1, x2 + pad_x))
        y1 = min(h - 1, pad_y)
        y2 = max(y1 + 1, h - pad_y)
        cell = crop_bgr[y1:y2, x1:x2]
        if cell.size == 0:
            return []
        cells.append(cell)
    return cells


class LocalDigitClassifier:
    """Optional local digit classifier adapter.

    The production OCR path does not depend on this class until a validated model
    is supplied. The adapter exists so future ONNX/CNN recognizers can be plugged
    into local_recognizer without changing API/business logic.
    """

    model_version = LOCAL_DIGIT_CLASSIFIER_CONTRACT_VERSION

    def available(self) -> bool:
        return False

    def predict_digit(self, crop_bgr: np.ndarray) -> Optional[DigitPrediction]:
        return None

    def predict_sequence(self, crop_bgr: np.ndarray, digits_count: int) -> Optional[DigitSequencePrediction]:
        if not self.available():
            return None
        cells = split_digit_cells(crop_bgr, digits_count)
        if len(cells) != int(digits_count):
            return None
        preds: list[DigitPrediction] = []
        for cell in cells:
            pred = self.predict_digit(cell)
            if pred is None:
                return None
            preds.append(pred)
        if not preds:
            return None
        conf = float(np.prod([max(1e-6, float(p.confidence)) for p in preds]) ** (1.0 / float(len(preds))))
        return DigitSequencePrediction(
            digits="".join(str(p.digit) for p in preds),
            confidence=round(conf, 6),
            per_digit=tuple(preds),
            model_version=self.model_version,
        )


class TemplateDigitClassifier(LocalDigitClassifier):
    def __init__(self, model_path: str | Path) -> None:
        self.model_path = str(model_path)
        payload = np.load(self.model_path, allow_pickle=False)
        self.templates = np.asarray(payload["templates"], dtype="float32")
        self.labels = np.asarray(payload["labels"], dtype="int64")
        meta_raw = payload["meta"]
        try:
            import json

            meta_text = str(meta_raw.tolist() if hasattr(meta_raw, "tolist") else meta_raw)
            self.meta: dict[str, Any] = json.loads(meta_text)
        except Exception:
            self.meta = {}
        self.input_size = int(self.meta.get("input_size") or (self.templates.shape[-1] if self.templates.ndim == 3 else 28))
        self.model_version = str(self.meta.get("model_version") or f"template:{Path(self.model_path).name}")
        if self.templates.ndim != 3 or self.templates.shape[0] != self.labels.shape[0]:
            raise ValueError("invalid template classifier model")
        if self.templates.shape[0] <= 0:
            raise ValueError("empty template classifier model")
        self.label_coverage = len({int(v) for v in self.labels.tolist() if 0 <= int(v) <= 9})

    def available(self) -> bool:
        return True

    def predict_digit(self, crop_bgr: np.ndarray) -> Optional[DigitPrediction]:
        if crop_bgr is None or crop_bgr.size == 0:
            return None
        x = prepare_digit_template(crop_bgr, input_size=self.input_size)
        diffs = self.templates - x.reshape(1, self.input_size, self.input_size)
        mse = np.mean(diffs * diffs, axis=(1, 2))
        best_by_digit: dict[int, float] = {}
        for label, dist in zip(self.labels.tolist(), mse.tolist()):
            d = int(label)
            best_by_digit[d] = min(float(dist), best_by_digit.get(d, float("inf")))
        if not best_by_digit:
            return None
        ranked = sorted(best_by_digit.items(), key=lambda kv: kv[1])
        best_digit, best_dist = ranked[0]
        second_dist = ranked[1][1] if len(ranked) > 1 else best_dist + 1.0
        # Convert distance and margin into a conservative confidence. This must
        # remain a shadow signal until validated on a real dataset.
        margin = max(0.0, float(second_dist) - float(best_dist))
        confidence = 1.0 / (1.0 + float(best_dist))
        confidence *= min(1.0, 0.55 + margin * 0.45)
        # A model that has not seen all digits can still be useful for shadow
        # diagnostics, but its confidence must not look production-grade.
        if self.label_coverage < 10:
            confidence *= min(1.0, 0.30 + 0.07 * float(self.label_coverage))
        confidence = max(0.01, min(0.99, confidence))
        scores = np.zeros(10, dtype="float64")
        inv = {d: 1.0 / (1.0 + dist) for d, dist in best_by_digit.items() if 0 <= int(d) <= 9}
        total = max(1e-12, float(sum(inv.values())))
        for d, val in inv.items():
            scores[int(d)] = float(val) / total
        return DigitPrediction(
            digit=int(best_digit),
            confidence=round(float(confidence), 6),
            probabilities=tuple(round(float(v), 6) for v in scores.tolist()),
            model_version=self.model_version,
        )


class OnnxDigitClassifier(LocalDigitClassifier):
    def __init__(
        self,
        model_path: str | Path,
        *,
        input_size: int = 28,
        input_name: Optional[str] = None,
        output_name: Optional[str] = None,
        model_version: Optional[str] = None,
    ) -> None:
        try:
            import onnxruntime as ort  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("onnxruntime is not installed") from exc
        self.model_path = str(model_path)
        self.input_size = int(input_size)
        self.session = ort.InferenceSession(self.model_path, providers=["CPUExecutionProvider"])
        self.input_name = input_name or self.session.get_inputs()[0].name
        self.output_name = output_name or self.session.get_outputs()[0].name
        self.model_version = model_version or f"onnx:{Path(self.model_path).name}"

    def available(self) -> bool:
        return True

    def _prepare(self, crop_bgr: np.ndarray) -> np.ndarray:
        if crop_bgr.ndim == 3:
            gray = cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2GRAY)
        else:
            gray = crop_bgr
        gray = cv2.resize(gray, (self.input_size, self.input_size), interpolation=cv2.INTER_AREA)
        gray = cv2.equalizeHist(gray)
        arr = gray.astype("float32") / 255.0
        arr = (arr - 0.5) / 0.5
        return arr.reshape(1, 1, self.input_size, self.input_size)

    def predict_digit(self, crop_bgr: np.ndarray) -> Optional[DigitPrediction]:
        if crop_bgr is None or crop_bgr.size == 0:
            return None
        x = self._prepare(crop_bgr)
        raw = self.session.run([self.output_name], {self.input_name: x})[0]
        logits = np.asarray(raw).reshape(-1).astype("float64")
        if logits.size < 10:
            return None
        logits = logits[:10]
        logits = logits - np.max(logits)
        exp = np.exp(logits)
        probs = exp / max(1e-12, float(np.sum(exp)))
        digit = int(np.argmax(probs))
        confidence = float(probs[digit])
        return DigitPrediction(
            digit=digit,
            confidence=round(confidence, 6),
            probabilities=tuple(round(float(v), 6) for v in probs.tolist()),
            model_version=self.model_version,
        )


def load_digit_classifier(model_path: str | Path | None) -> LocalDigitClassifier:
    if not model_path:
        return LocalDigitClassifier()
    p = Path(str(model_path)).expanduser()
    if not p.exists():
        return LocalDigitClassifier()
    if p.suffix.lower() == ".npz":
        try:
            return TemplateDigitClassifier(p)
        except Exception:
            return LocalDigitClassifier()
    try:
        return OnnxDigitClassifier(p)
    except Exception:
        return LocalDigitClassifier()
