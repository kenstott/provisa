FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
# wheels/ pre-built by build-dmg.sh for linux/arm64 — no PyPI access needed
COPY wheels/ /wheels/
RUN pip install --no-cache-dir --no-index --find-links /wheels . && rm -rf /wheels

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
