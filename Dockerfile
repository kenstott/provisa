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
# Every package at the version uv.lock pins — the versions the tests ran on — never pip's newest
# (an unpinned install shipped sqlglot 30.23 while the suite ran 30.9). torch comes from the CPU
# index at the locked version; its CUDA-only dependencies (triton, nvidia-*, cuda-*) are left out.
ARG PROVISA_EXTRAS=firebase,vector
COPY uv.lock .
# REQ-2071: provisa-notebook (the Data Science area's Pyodide) is a dependency that lives beside
# provisa in this repository. Its assets are fetched and built here, with the provisa-client wheel
# they carry, and it is installed as a package (the lock names it as an editable path, which the
# runtime stage would not have).
COPY provisa-notebook/ ./provisa-notebook/
COPY provisa-client/ ./provisa-client/
COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /usr/local/bin/uv
RUN uv export --frozen --no-dev --no-emit-project --no-hashes --no-annotate --no-header \
      $(echo "${PROVISA_EXTRAS}" | tr ',' '\n' | sed 's/^/--extra /') -o /tmp/locked.txt \
 && grep -vE '^(torch|triton|nvidia-|cuda-|-e )' /tmp/locked.txt > /tmp/requirements.txt \
 && pip install --no-cache-dir "$(grep '^torch==' /tmp/locked.txt | cut -d' ' -f1)" \
      --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir --no-deps -r /tmp/requirements.txt \
 && PYTHONPATH=provisa-notebook python -m provisa_notebook.build > /dev/null \
 && pip install --no-cache-dir --no-deps ./provisa-notebook \
 && pip install --no-cache-dir --no-deps . \
 && rm -rf /usr/local/bin/uv /tmp/locked.txt /tmp/requirements.txt provisa-notebook provisa-client

# Stage 2: render_graph's renderer (REQ-1990) — the UI's own Cytoscape stylesheet and layouts,
# bundled with the packages provisa-ui/graph-render's lockfile pins, drawn by Node into a PNG.
# npm and the build tools stay in this stage; the runtime takes node, the bundle and the two
# packages the bundle loads at run time (jsdom, and canvas with its native build for this arch).
FROM node:22.22.3-bookworm-slim@sha256:e21fc383b50d5347dc7a9f1cae45b8f4e2f0d39f7ade28e4eef7d2934522b752 AS graph-render
WORKDIR /build/provisa-ui
COPY provisa-ui/graph-render/package.json provisa-ui/graph-render/package-lock.json ./graph-render/
RUN cd graph-render && npm ci --no-audit --no-fund
COPY provisa-ui/graph-render/src ./graph-render/src
COPY provisa-ui/src/components/graph ./src/components/graph
COPY provisa-ui/src/components/erd/erd-layout-options.ts ./src/components/erd/
RUN cd graph-render && npm run build && npm prune --omit=dev

# Stage 3: lean runtime image — no wheels, only app source + installed packages
FROM python:3.12-slim
WORKDIR /app
# REQ-1992: an org may keep its file folder encrypted (gocryptfs over FUSE). The container needs
# /dev/fuse and CAP_SYS_ADMIN for it (docker-compose.app.yml, the Helm chart); without them the
# setting is refused by name and plain folders are unaffected.
RUN apt-get update \
 && apt-get install -y --no-install-recommends gocryptfs fuse3 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=installer /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=installer /usr/local/bin /usr/local/bin
COPY main.py pyproject.toml ./
COPY provisa/ ./provisa/
# render_graph draws with Node (maintainer, 2026-10-12: "add node to the image and render PNG").
# Labels need a font: fontconfig's sans-serif is DejaVu Sans.
COPY --from=graph-render /usr/local/bin/node /usr/local/bin/node
COPY --from=graph-render /build/provisa-ui/graph-render/dist ./provisa/_graph_render/dist
COPY --from=graph-render /build/provisa-ui/graph-render/node_modules ./provisa/_graph_render/node_modules
RUN apt-get update && apt-get install -y --no-install-recommends fonts-dejavu-core \
 && rm -rf /var/lib/apt/lists/*
# A files source is served by the Calcite pgwire connector server, which runs in a JVM (JPype
# finds it by JAVA_HOME). The connector is built for Java 21 (askamerica-engine, release 21).
COPY --from=eclipse-temurin:21-jre-noble@sha256:000fd431958bc81a24abe1e8e5f0f0fd3ae365a594bd50aadb20696805f9408c \
     /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"
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
