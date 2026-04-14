# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
# wheels/ pre-built for airgapped install — mounted at build time, never written as a layer
RUN --mount=type=bind,source=wheels,target=/wheels \
    pip install --no-cache-dir --no-index --find-links /wheels .

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
