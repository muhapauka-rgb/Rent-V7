FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# КРИТИЧЕСКИ ВАЖНО:
# --host 0.0.0.0
# порт 8000 (его мы пробрасываем наружу)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
