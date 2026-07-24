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
# Bake ONLY the shipped runtime configs — NEVER the whole config/ dir. A blanket copy also
# baked the dev-local config/provisa.yaml (a divergent 909-line config) into the image; a
# secondary that loaded it registered a DIFFERENT source set than the primary's demo config
# and crashed the shared control plane with a duplicate domain+table registration. Every
# cluster node must load the byte-identical baked config, so bake an explicit, minimal set:
# the demo (provisa-install.yaml, auth: none), the wizard base skeleton
# (provisa-install-base.yaml), the engine capability + pg-extension catalogs, the custom
# connector registry (REQ-1177), and the pgbouncer config. The demo config resolves its
# SQLite paths via ${env:PROVISA_DEMO_DIR}; stage that sample data under /app/config/demo/files
# — NOT /app/demo, which docker-compose.app.yml bind-mounts (./demo) and would shadow.
COPY config/capabilities.yaml config/pg_extension_catalog.yaml config/custom_connectors.yaml \
     config/provisa-install.yaml config/provisa-install-base.yaml ./config/
COPY config/pgbouncer/ ./config/pgbouncer/
COPY demo/files/pet_store.sqlite demo/files/inquiries.sqlite ./config/demo/files/

EXPOSE 8000 3000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
