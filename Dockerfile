# syntax=docker/dockerfile:1
# Stage 1: install Python deps from pre-built wheels (stays on builder layer only)
FROM python:3.12-slim AS installer
WORKDIR /app
COPY pyproject.toml .
COPY vendor/ ./vendor/
RUN pip install --no-cache-dir .

# Stage 2: lean runtime image — no wheels, only app source + installed packages
FROM python:3.12-slim
WORKDIR /app
COPY --from=installer /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=installer /usr/local/bin /usr/local/bin
COPY main.py pyproject.toml ./
COPY provisa/ ./provisa/
# static/ contains the built React SPA; may be empty in dev builds
COPY static/ ./static/
# Bake the shipped configs so a demo deploy loads a complete, valid config instead of
# dropping into the first-run wizard (parity with native launch f7289d27): the full
# pet-store + shelter demo (provisa-install.yaml, auth: none) and the minimal wizard
# base skeleton (provisa-install-base.yaml). The demo config resolves its SQLite paths
# via ${env:PROVISA_DEMO_DIR}; stage that sample data under /app/config/demo/files —
# NOT /app/demo, which docker-compose.app.yml bind-mounts (./demo) and would shadow.
COPY config/ ./config/
COPY demo/files/pet_store.sqlite demo/files/inquiries.sqlite ./config/demo/files/

EXPOSE 8000 3000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
