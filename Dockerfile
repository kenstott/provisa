# syntax=docker/dockerfile:1
# Stage 1: install Python deps from pre-built wheels (stays on builder layer only)
FROM python:3.12-slim AS installer
WORKDIR /app
COPY pyproject.toml .
COPY wheels/ /wheels/
RUN pip install --no-cache-dir --no-index --find-links /wheels .

# Stage 2: lean runtime image — no wheels, only app source + installed packages
FROM python:3.12-slim
WORKDIR /app
COPY --from=installer /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=installer /usr/local/bin /usr/local/bin
COPY main.py pyproject.toml ./
COPY provisa/ ./provisa/
COPY config/ ./config/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
