#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT="backend-core"
OCR_ENDPOINT="${OCR_ENDPOINT:-http://127.0.0.1:8002/recognize}"
ELECTRO_DIR="${ELECTRO_DIR:-$ROOT/tmp/real-photos/Electro}"
WATER_DIR="${WATER_DIR:-$ROOT/tmp/real-photos}"
ELECTRO_TRUTH="${ELECTRO_TRUTH:-$ROOT/tools/electric_truth.json}"
WATER_TRUTH="${WATER_TRUTH:-$ROOT/tools/water_truth.tsv}"
STRICT="${STRICT:-1}"

if [[ ! -d "$ELECTRO_DIR" ]]; then
  echo "missing electro dir: $ELECTRO_DIR" >&2
  exit 2
fi
if [[ ! -f "$ELECTRO_TRUTH" ]]; then
  echo "missing electro truth: $ELECTRO_TRUTH" >&2
  exit 2
fi
if [[ ! -d "$WATER_DIR" ]]; then
  echo "missing water dir: $WATER_DIR" >&2
  exit 2
fi
if [[ ! -f "$WATER_TRUTH" ]]; then
  echo "missing water truth: $WATER_TRUTH" >&2
  exit 2
fi

cd "$ROOT"
docker compose -p "$PROJECT" up -d --no-deps ocr-service >/dev/null

echo "== Electric regression =="
ELECTRO_OUT="$(mktemp)"
python3 "$ROOT/tools/electric_eval.py" \
  --photos-dir "$ELECTRO_DIR" \
  --truth-json "$ELECTRO_TRUTH" \
  --api-url "$OCR_ENDPOINT" \
  --mode full | tee "$ELECTRO_OUT"

echo ""
echo "== Water regression =="
WATER_STAGE="$(mktemp -d)"
cp "$ROOT/tools/water_eval.py" "$WATER_STAGE/water_eval.py"
cp "$WATER_TRUTH" "$WATER_STAGE/water_truth.tsv"
while IFS=$'\t' read -r filename reading; do
  [[ "$filename" == "filename" ]] && continue
  [[ -z "$filename" ]] && continue
  if [[ ! -f "$WATER_DIR/$filename" ]]; then
    echo "missing water photo from truth: $WATER_DIR/$filename" >&2
    exit 2
  fi
  cp "$WATER_DIR/$filename" "$WATER_STAGE/$filename"
done < "$WATER_TRUTH"

docker cp "$WATER_STAGE/water_eval.py" rent_ocr:/tmp/water_eval.py
docker cp "$WATER_STAGE/." rent_ocr:/tmp/water_reg/
WATER_OUT="$(mktemp)"
docker compose -p "$PROJECT" exec -T ocr-service python /tmp/water_eval.py \
  --photos-dir /tmp/water_reg \
  --truth-tsv /tmp/water_reg/water_truth.tsv \
  --endpoint http://127.0.0.1:8000/recognize \
  --timeout-sec 50 | tee "$WATER_OUT"

if [[ "$STRICT" != "1" ]]; then
  echo ""
  echo "STRICT=0 -> skip fail checks"
  exit 0
fi

electric_summary="$(grep '^summary ' "$ELECTRO_OUT" | tail -n1 || true)"
electric_wrong="$(echo "$electric_summary" | sed -E 's/.* wrong=([0-9]+).*/\1/' || true)"
electric_miss="$(echo "$electric_summary" | sed -E 's/.* miss=([0-9]+).*/\1/' || true)"
electric_wrong="${electric_wrong:-0}"
electric_miss="${electric_miss:-0}"

water_wrong="$(awk -F'\t' 'NR>1 && $1!="summary" {if($2=="wrong" || $2=="unknown" || $2=="timeout") c++} END{print c+0}' "$WATER_OUT")"

echo ""
echo "== Strict summary =="
echo "electric wrong=$electric_wrong miss=$electric_miss"
echo "water bad=$water_wrong"

if [[ "$electric_wrong" != "0" || "$electric_miss" != "0" || "$water_wrong" != "0" ]]; then
  echo "regression failed" >&2
  exit 1
fi

echo "regression passed"
