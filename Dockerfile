# syntax=docker/dockerfile:1
# Stage 1: install Python deps from pre-built wheels (stays on builder layer only)
FROM python:3.12-slim AS installer
WORKDIR /app
COPY pyproject.toml .
COPY vendor/ ./vendor/
# [firebase] pulls firebase-admin — the container is the single artifact for cloud deploys where
# Firebase is a first-class IdP (REQ-1266). Without it FirebaseAuthProvider raises ImportError and
# every authenticated request 500s. The desktop wheel keeps firebase optional; the image bakes it in.
# [vector] pulls sentence-transformers — the demo config (provisa-install.yaml) registers a
# huggingface vector_models entry so MCP catalog search (REQ-1008) works out of the box.
# torch is pre-installed from the CPU wheel index: the default amd64 torch is the CUDA build,
# ~2.5 GiB of GPU libraries this CPU-only container never loads — it pushed the core-images
# release tarballs past GitHub's 2 GiB per-asset limit (alpha.308/309 publish failures).
# REQ-1443: PROVISA_EXTRAS is the build-time extra set. The DEFAULT is what the cloud plane ships —
# adding [soda] to the default would put an Elastic-License-2.0 component into the hosted service,
# which its terms forbid. A self-hosted build opts in explicitly:
#   docker build --build-arg PROVISA_EXTRAS=firebase,vector,soda .
ARG PROVISA_EXTRAS=firebase,vector
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir ".[${PROVISA_EXTRAS}]"

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
# Every demo source in provisa-install.yaml must resolve on a demo deploy, and two of them are
# served by Provisa's own mock backends (petstore-mock:8080, graphql-demo:4000). Their only
# runtime deps — starlette/uvicorn/strawberry — are already in this image, so the compose demo
# overlay runs them from this same image rather than pulling two more. Staged under
# /app/config/demo/servers for the same reason as the sample data: /app/demo is bind-mounted
# by docker-compose.app.yml and would shadow whatever is baked there.
COPY demo/petstore_server/server.py demo/petstore_server/openapi.json ./config/demo/servers/petstore/
COPY demo/graphql_server/server.py ./config/demo/servers/graphql/
COPY demo/grpc_server/server.py ./config/demo/servers/grpc/
# The shared lane's queue policy. k8s_provisioner.shared_resource_groups() reads this exact
# file and writes it into the shard's ConfigMap (REQ-1450), so the control-plane image needs
# it even though this node runs no Trino of its own; without it the boot wake dies with
# FileNotFoundError before the app finishes starting.
COPY trino/etc/resource-groups.json ./trino/etc/resource-groups.json

EXPOSE 8000 3000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "620"]
