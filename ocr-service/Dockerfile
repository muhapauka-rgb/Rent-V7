FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    libgl1 \
    libglib2.0-0 \
 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py /app/app.py
COPY local_recognizer.py /app/local_recognizer.py
COPY local_digit_classifier.py /app/local_digit_classifier.py
COPY local_digit_template_model.npz /app/local_digit_template_model.npz
COPY water_deterministic.py /app/water_deterministic.py
COPY electric_templates_seed.json /app/electric_templates_seed.json
COPY water_templates_seed.json /app/water_templates_seed.json
COPY tests /app/tests

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

# OCR тяжелее остальных сервисов, поэтому по умолчанию один worker.
# Если нужен параллелизм на сервере, задаем OCR_WORKERS=2/3 в окружении.
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port 8000 --workers ${OCR_WORKERS:-1}"]
