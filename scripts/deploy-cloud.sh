#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# Compile local source and push it straight into the running cloud.provisa.dev
# containers, skipping the image rebuild + terraform re-provision cycle.
#
# TOPOLOGY (REQ-1447..1451): the node is the control plane ONLY. It runs the app, the UI proxy,
# the observability tier and the demo mock backends; it runs no Trino of its own. Every query is
# dispatched to a Trino shard the control plane provisions on GKE (provisa-saas-engine,
# namespace provisa-engines) and scales to zero when idle, and the control plane's registry lives
# on Cloud SQL, not in a container on this node. That is why this script no longer installs the
# Docker-socket isolated-engine overlay and no longer expects a postgres container.
#
# Both compose-provisa-1 (API) and compose-provisa-ui-1 (UI proxy) run the full application image,
# so both need the same /app/static, /app/provisa and /app/config. They get it from ONE host
# directory bind-mounted into each (push_overlay): a container recreate — which this node does on
# every idle-stop — no longer reverts a push, and there are no longer two copies to disagree.
#
#   scripts/deploy-cloud.sh ui     # vite build -> /app/static (no restart needed)
#   scripts/deploy-cloud.sh api    # python package -> /app/provisa (restarts both)
#   scripts/deploy-cloud.sh cfg    # shipped configs -> /app/config (restarts both)
#   scripts/deploy-cloud.sh all    # all three, one restart at the end
#   scripts/deploy-cloud.sh reset  # accounts/orgs -> none, demo data re-seeded
#   scripts/deploy-cloud.sh patch  # the same steps as all
#   scripts/deploy-cloud.sh sandbox-reset  # the sandbox tier alone: its orgs -> none, no build
#
# Every arm that pushes first ensures the observability and demo overlays are installed on the
# node — the ops reports over the OTLP parquet store (traces, queries, metrics, logs) are empty
# without a collector, and the demo config's OpenAPI/GraphQL/gRPC sources are unresolvable without
# the mock backends (REQ-1468). Both are content-compared; already-current is a no-op.
#
# Only `reset` wipes the control plane back to the first-start state (no claimed
# super-admin, no accounts, no tenant orgs, no invites) and lets the restart re-seed
# the bootstrap org's demo data set from the deployment config, and the sandbox org
# with its public invite (provisa/api/sandbox_org.py). Every arm that deploys code
# (ui, api, cfg, all, patch) keeps accounts and orgs: deploying code and returning
# the deployment to its first start are two acts, and an `all` that did both wiped
# cloud-dev on 2026-11-16.
#
# Every arm that pushes the python package to a sandbox VM also resets the sandbox tier there
# (reset_sandbox_state: its sandboxes and every demo template not built under the pushed code's
# table shapes, never the coordinator's accounts or orgs) -- patch included, since the tier's
# tables are rebuilt only from nothing -- and proves a demo opens. `sandbox-reset` does that
# alone, with no build.
#
# Env overrides: NODE, ZONE, PROJECT, CONTAINERS, SITE, SQL_IMAGE.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
# The node, its ssh, the engine and commerce gates, the reset and the verifies: shared with
# scripts/deploy-cloud-release.sh, which installs a release's own artifacts on the same node.
# shellcheck source=cloud-node-lib.sh
. "$REPO/scripts/cloud-node-lib.sh"

TARGET="${1:-}"
case "$TARGET" in
  ui|api|cfg|all|reset|patch|sandbox-reset) ;;
  *) echo "usage: $(basename "$0") {ui|api|cfg|all|reset|patch|sandbox-reset}" >&2; exit 2 ;;
esac

build_ui() {
  local base
  base="$(org_base_domain)" || exit 1
  echo "== building UI (org base domain: $base)"
  # The UI builds on the repository's Node pin (.nvmrc), as CI and the image do.
  local want
  want="v$(cat "$REPO/.nvmrc")"
  export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
  # shellcheck disable=SC1091
  [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use "$want" >/dev/null
  if [ "$(node --version)" != "$want" ]; then
    echo "The UI builds on Node $want (.nvmrc); node on PATH is $(node --version). nvm install ${want#v}" >&2
    exit 1
  fi
  (cd "$REPO/provisa-ui" && VITE_ORG_BASE_DOMAIN="$base" npm run build)
  # index.html carries the hashed bundle names, so it ships with the assets it references.
  # auth-relay.html is the second HTML entry (REQ-1348) and does the same for the relay bundle;
  # left behind, the control plane answers an org subdomain's token request with a stale page or
  # a 502 and sign-in on that subdomain never completes.
  # voyager/ is the vendored GraphQL Voyager bundle the Schema Explorer's SDL iframe loads;
  # it is part of the UI build output, so a patch that left it behind could serve a node whose
  # image predates it and the SDL view would render blank.
  # swagger-ui/ is Swagger UI, copied from the swagger-ui-dist npm package by the build, which the
  # REST docs page loads; left behind, that page renders blank.
  tar czf "$STAGE/ui.tgz" -C "$REPO/provisa-ui/dist" assets index.html auth-relay.html voyager swagger-ui
}

push_ui() {
  echo "== pushing UI ($(du -h "$STAGE/ui.tgz" | cut -f1))"
  scp_node "$STAGE/ui.tgz" /tmp/provisa-ui-deploy.tgz
  # One host directory, bind-mounted into both containers by push_overlay — there is no longer a
  # per-container copy for the two to disagree about.
  # The old assets are removed first: filenames are content-hashed, so extracting
  # over them accumulates every bundle ever deployed and hides which one is live.
  ssh_node "sudo rm -rf $OVERLAY_DIR/static/assets \
    && sudo tar xzf /tmp/provisa-ui-deploy.tgz -C $OVERLAY_DIR/static \
    && echo \"static assets=\$(sudo ls $OVERLAY_DIR/static/assets | wc -l)\""
}

build_api() {
  echo "== packaging python package"
  # provisa/_graph_render is a checkout's symlink to provisa-ui/graph-render (gitignored); the image
  # carries the built renderer there as a directory, which a pushed symlink cannot replace.
  # REQ-2194: provisa's compiled encoders (provisa._encoders), taken from provisa's Linux wheels,
  # which the Dockerfile's provisa-wheel stage builds from this checkout for both architectures.
  # Each module file is given its architecture's name (_encoders.cpython-312-<arch>-linux-gnu.so),
  # so the one pushed tree imports on an amd64 node and an arm64 one alike. A checkout's own
  # (macOS) module stays out.
  echo "== compiling provisa's encoders (linux/amd64, linux/arm64)"
  rm -rf "$STAGE/encoders"
  local platform arch wheel
  for platform in linux/amd64 linux/arm64; do
    arch=x86_64; [ "$platform" = linux/arm64 ] && arch=aarch64
    docker build --quiet --platform "$platform" --target provisa-wheel \
      --output "type=local,dest=$STAGE/encoders/$arch" "$REPO" >/dev/null
    wheel="$(ls "$STAGE/encoders/$arch"/provisa-*-manylinux_2_28_"$arch".whl)"
    unzip -q -o "$wheel" 'provisa/_encoders*.so' -d "$STAGE/encoders/$arch/tree"
    mkdir -p "$STAGE/encoders/tree/provisa"
    mv "$STAGE/encoders/$arch"/tree/provisa/_encoders*.so \
      "$STAGE/encoders/tree/provisa/_encoders.cpython-312-$arch-linux-gnu.so"
  done
  tar czf "$STAGE/api.tgz" -C "$REPO" --exclude='__pycache__' --exclude='*.pyc' \
    --exclude='provisa/_graph_render' --exclude='provisa/_encoders*-darwin.so' provisa \
    -C "$REPO/vendor/buenavista" buenavista \
    -C "$STAGE/encoders/tree" provisa/_encoders.cpython-312-x86_64-linux-gnu.so \
    provisa/_encoders.cpython-312-aarch64-linux-gnu.so
}

push_api() {
  echo "== pushing python package ($(du -h "$STAGE/api.tgz" | cut -f1))"
  scp_node "$STAGE/api.tgz" /tmp/provisa-api-deploy.tgz
  # Extracted over $OVERLAY_DIR/provisa rather than replacing it: the tree is the import root
  # of the running interpreter, and a deleted-then-recreated directory races the restart.
  # The tarball's top-level entry IS `provisa/`, so it unpacks into $OVERLAY_DIR itself.
  ssh_node "sudo tar xzf /tmp/provisa-api-deploy.tgz -C $OVERLAY_DIR \
    && echo \"host provisa=\$(sudo ls $OVERLAY_DIR/provisa | wc -l)\""
}

# The shipped runtime configs, which were previously reachable only through an image rebuild. The
# node is a demo install, so its demo overlay points PROVISA_CONFIG at
# /app/config/provisa-install.yaml (packaging/linux/first-launch.sh, write_demo_overlay; the base
# compose sets provisa-install-base.yaml). A config change that never shipped left the node
# registering a stale source and column-grant set: the pre-REQ-1297 copy still named platform_admin in every column's
# visible_to, and repositories/table.py strips that role at write time, which emptied the grant
# lists and dropped whole tables out of every role's schema.
#
# The set is the SAME explicit list the Dockerfile bakes (Dockerfile:31-34) — never the whole
# config/ dir, which also holds the divergent dev-local config/provisa.yaml.
CONFIG_FILES="capabilities.yaml pg_extension_catalog.yaml custom_connectors.yaml \
provisa-install.yaml provisa-install-base.yaml provisa-sandbox-regions.yaml"

# The sample data provisa-install.yaml's SQLite sources resolve to, via ${env:PROVISA_DEMO_DIR},
# which the demo overlay sets to /app/config/demo/files (Dockerfile:41-45). It ships WITH the config
# because the config is what registers the tables inside it: shipping only the yaml left the node
# registering pet_store's REQ-1586 junction tables against an image-baked sqlite that still held
# `pets` alone, so the schema showed Playgroups and every query through PLAYGROUP_MEMBERS failed
# with TABLE_NOT_FOUND. Same explicit list as the Dockerfile COPY; the paths are relative to the
# repo root so they unpack to demo/files/ under /app/config.
DEMO_DATA="demo/files/pet_store.sqlite demo/files/inquiries.sqlite"

build_cfg() {
  echo "== packaging configs"
  tar czf "$STAGE/cfg.tgz" -C "$REPO/config" $CONFIG_FILES pgbouncer -C "$REPO" $DEMO_DATA
}

push_cfg() {
  echo "== pushing configs ($(du -h "$STAGE/cfg.tgz" | cut -f1))"
  scp_node "$STAGE/cfg.tgz" /tmp/provisa-cfg-deploy.tgz
  # federation_engine is rewritten to the node's own $PROVISA_ENGINE pin
  # (terraform/gcp-saas/main.tf:224 exports it) rather than shipped as a second SaaS config file.
  # The pin already decides which engine boots; without this the persisted selection stayed at the
  # shipped desktop default (duckdb) and /admin/federation-engine reported a saved selection that
  # disagreed with the running engine on every page load. preflight_engine has already asserted the
  # node agrees with this value, so it is written, not discovered.
  local pin="$ENGINE_PIN"
  ssh_node "sudo tar xzf /tmp/provisa-cfg-deploy.tgz -C $OVERLAY_DIR/config \
    && sudo sed -i \"s|^federation_engine:.*|federation_engine: $pin|\" $OVERLAY_DIR/config/provisa-install.yaml \
    && echo \"host config=\$(sudo sh -c 'ls $OVERLAY_DIR/config/*.yaml' | wc -l) \$(sudo grep ^federation_engine: $OVERLAY_DIR/config/provisa-install.yaml)\""
}

restart() {
  # A container restart keeps the image it was created from, so a node whose image push_deps just
  # rebuilt is recreated instead -- the same `systemctl restart provisa` push_app recreates with.
  if [ "$DEPS_RECREATE" = "$NODE" ]; then
    DEPS_RECREATE=""
    echo "== recreating the stack on $NODE's rebuilt image"
    ssh_node "sudo systemctl restart provisa"
    return
  fi
  local containers
  containers="$(node_containers)"
  echo "== restarting"
  ssh_node "sudo docker restart $containers"
}

# The release's Python dependency set. push_api ships the source tree onto the node's image and
# never rebuilds it, so a release that added a package reached the node as code importing what the
# image did not have: 511 (lemminflect, rdflib, pypdf, python-docx, statsmodels,
# pixeltable-pgserver) crash-looped the coordinator and the sandbox VM on 2026-11-23.
# scripts/deploy_deps.py exports the set exactly as the Dockerfile installs it and fingerprints
# pyproject.toml, uv.lock and the Dockerfile's extras; the image records the fingerprint it holds
# (deploy_deps.RECORD_PATH), so a node is rebuilt only when the set changed since its last install.
DEPS_RECORD=/etc/provisa-deps.sha256
DEPS_DIR=/tmp/provisa-deps
# first-launch.sh (load_images) reloads every image from the tarballs staged here on every boot,
# and the GCE startup-script runs it on every instance start: a rebuilt image is saved over its
# tarball, or the node's next wake would bring back the image without the set.
IMAGE_TARBALL=/root/.provisa/images/provisa-local.tar.gz
DEPS_RECREATE=""

build_deps() {
  echo "== staging the release's python dependency set"
  DEPS_WANT="$(python3 "$REPO/scripts/deploy_deps.py" stage "$REPO" "$STAGE/deps")"
  tar czf "$STAGE/deps.tgz" -C "$STAGE/deps" .
}

# Installs the set into the image $API_CONTAINER runs on $NODE when the image's record differs, then
# proves it (verify_deps). Before the restart, so a node that cannot import the pushed code is
# refused while its running containers are still up.
push_deps() {
  local image have action
  image="$(ssh_node "sudo docker inspect -f '{{.Config.Image}}' $API_CONTAINER" | tr -d '\r')"
  [ -n "$image" ] || { echo "dependencies on $NODE: no image for $API_CONTAINER" >&2; exit 1; }
  scp_node "$STAGE/deps.tgz" /tmp/provisa-deps.tgz
  ssh_node "sudo rm -rf $DEPS_DIR && sudo mkdir -p $DEPS_DIR && sudo tar xzf /tmp/provisa-deps.tgz -C $DEPS_DIR"
  # cat exits non-zero when the file is absent: an image no deploy has installed onto records
  # nothing, which deploy_deps plans as "install".
  have="$(ssh_node "sudo docker run --rm --entrypoint cat $image $DEPS_RECORD 2>/dev/null" | tr -d '\r')" || have=""
  action="$(python3 "$REPO/scripts/deploy_deps.py" plan "$DEPS_WANT" "$have")"
  if [ "$action" = install ]; then
    echo "== dependencies on $NODE: installing the set into $image"
    ssh_node "sudo docker build --quiet --build-arg BASE=$image -t $image $DEPS_DIR >/dev/null"
    ssh_node "sudo test -f $IMAGE_TARBALL" \
      || { echo "dependencies on $NODE: no $IMAGE_TARBALL to save the rebuilt image over" >&2; exit 1; }
    ssh_node "sudo sh -c 'docker save $image | gzip > $IMAGE_TARBALL.new && mv $IMAGE_TARBALL.new $IMAGE_TARBALL'"
    DEPS_RECREATE="$NODE"
  else
    echo "== dependencies on $NODE: current"
  fi
  verify_deps "$image"
}

# Every locked requirement at its version, then the app's own import, in throwaway containers on the
# image the restart will run, with the pushed tree mounted as $API_CONTAINER mounts it.
verify_deps() {
  echo "== verifying dependencies on $NODE"
  ssh_node "sudo docker run --rm --volumes-from $API_CONTAINER -v $DEPS_DIR:/deps:ro --entrypoint python $1 /deps/deploy_deps.py check /deps/requirements.txt" \
    || { echo "dependencies on $NODE: $1 does not hold the release's set" >&2; exit 1; }
  ssh_node "sudo docker run --rm --volumes-from $API_CONTAINER -w /app --entrypoint python $1 -c 'import provisa.api.app'" \
    || { echo "dependencies on $NODE: provisa.api.app does not import on $1" >&2; exit 1; }
}

# The telemetry tier the ops reports read. `traces`, `queries`, `metrics` and `logs` are views over
# the OTLP parquet store, so with no collector on the node those four reports return zero rows no
# matter how much traffic the node serves — and docker-compose.app.yml deliberately stopped
# defaulting OTEL_EXPORTER_OTLP_ENDPOINT, so the app exports nothing until a collector exists.
#
# Installed as an extension rather than by flag: scripts/provisa enumerates
# ${PROVISA_HOME}/extensions/*/docker-compose.*.yml into COMPOSE_FILES (scripts/provisa:141-146),
# and the endpoint export is keyed off that resolved list matching *observability*
# (scripts/provisa:427-432), so dropping the overlay in that directory is what both creates the
# collector and points the app at it. The observability config tree it mounts
# (./observability/*.yaml) already ships to ${PROVISA_HOME}/compose/observability.
#
# Adding a compose file needs a stack recreate, which reverts hot-pushed code, so this runs BEFORE
# the pushes in every arm that pushes. Already-installed is the no-op path: no recreate, no loss.
# What makes a push durable. The three push targets used to be written into the CONTAINER
# filesystem, and the container is not durable: provisa.service runs `provisa stop` on shutdown,
# which is `docker compose down` — it REMOVES the containers (scripts/provisa, cmd_stop) — and the
# front door idle-stops this node whenever nobody is using it. Every boot therefore recreated both
# containers from the baked provisa/provisa:local image and silently discarded every patch this
# script had ever pushed; the node kept answering, just with the image's code. The three targets are
# bind-mounted from the host disk instead, so what was deployed outlives the container.
#
# Installed as an extension for the same reason the observability tier is: scripts/provisa
# enumerates ${PROVISA_HOME}/extensions/*/docker-compose.*.yml into COMPOSE_FILES
# (scripts/provisa:141-146), and compose unions `volumes` across files, so the mounts land on the
# services the generated control-plane base already declares without editing that generated file.
# Adding a compose file recreates the stack, so this runs BEFORE the pushes like the other overlays.
# OVERLAY_DIR and OVERLAY_EXT are in scripts/cloud-node-lib.sh: a release install removes the overlay.
# The hub cache of the embedding models the app and the sandbox tier load (a demo's own, REQ-2006).
HF_CACHE_DIR="/root/.provisa/hf-cache"
# The runtime-deps cache: the connector bundles (hundreds of megabytes each) a source reads
# through, mounted over the image's own (govdata's bundle is not in the image) and staged once by
# stage_runtime_deps, before the containers that read it start (provisa/runtime_deps/pgwire_bundles.py).
RUNTIME_DEPS_DIR="/root/.provisa/runtime-deps"

# The rest of the overlay of a node running the regions tier's other nodes: only the sandbox VM,
# whose compose push_compose has brought up to them. An overlay naming a service a node's compose
# does not have stops that node's stack.
REGIONS_NODES_OVERLAY=""

# REQ-294: every connector's bundle (govdata's too, which the image does not hold), into the host
# directory the overlay mounts. Staged by the running image's own Python against the deployed tree,
# so its pins are the deployed ones. Fetches only what is not yet there.
stage_runtime_deps() {
  echo "== runtime deps: staging connector bundles"
  ssh_node "set -e; IMG=\$(sudo docker inspect -f '{{.Config.Image}}' $API_CONTAINER) \
    && sudo mkdir -p $RUNTIME_DEPS_DIR \
    && sudo docker run --rm -w /app -e PROVISA_RUNTIME_DEPS_CACHE=/deps \
         -v $RUNTIME_DEPS_DIR:/deps -v $OVERLAY_DIR/provisa:/app/provisa \
         \$IMG python -m provisa.runtime_deps.stage_pgwire_bundles"
}

push_overlay() {
  local ORG_BASE
  ORG_BASE="$(org_base_domain)" || exit 1
  cat > "$STAGE/docker-compose.overlay.yml" <<YAML
# Installed by scripts/deploy-cloud.sh — the deployed app tree, on the host disk, so that a
# container recreate (every idle-stop on this node) does not revert it.
services:
  provisa:
    # REQ-1276: the API reads an org from a Host only under this base domain. Literal here, not
    # interpolated from the node's provisa.env, so the deploy is what puts it in the container.
    environment:
      VITE_ORG_BASE_DOMAIN: "${ORG_BASE}"
    volumes:
      - ${OVERLAY_DIR}/static:/app/static
      - ${OVERLAY_DIR}/provisa:/app/provisa
      - ${OVERLAY_DIR}/buenavista:/app/buenavista
      - ${OVERLAY_DIR}/config:/app/config
      # A huggingface embedding model is fetched into the hub cache at its first use; on the host,
      # a recreate does not fetch it again.
      - ${HF_CACHE_DIR}:/root/.cache/huggingface
      - ${RUNTIME_DEPS_DIR}:/opt/provisa/runtime-deps
  provisa-ui:
    environment:
      VITE_ORG_BASE_DOMAIN: "${ORG_BASE}"
    volumes:
      - ${OVERLAY_DIR}/static:/app/static
      - ${OVERLAY_DIR}/provisa:/app/provisa
      - ${OVERLAY_DIR}/buenavista:/app/buenavista
      - ${OVERLAY_DIR}/config:/app/config
  # REQ-2006: the sandbox tier runs the same app; without the tree it ran the image's own code
  # beside an API on the deployed one.
  provisa-sandbox:
    volumes:
      - ${OVERLAY_DIR}/static:/app/static
      - ${OVERLAY_DIR}/provisa:/app/provisa
      - ${OVERLAY_DIR}/buenavista:/app/buenavista
      - ${OVERLAY_DIR}/config:/app/config
      - ${HF_CACHE_DIR}:/root/.cache/huggingface
      - ${RUNTIME_DEPS_DIR}:/opt/provisa/runtime-deps
    # Where the tier is published (first-launch.sh's PROVISA_SANDBOX_PUBLISH): on a sandbox VM its
    # VPC address, where the coordinator reaches it. A VM set up by a release from before that
    # variable has no port in its compose, and its systemd unit passes no such variable through.
    ports:
      - "${SANDBOX_PUBLISH}:8000"
    environment:
      VITE_ORG_BASE_DOMAIN: "${ORG_BASE}"
      # The demos cloud.provisa.dev offers (packaging/linux/demo-stores/sandbox-tier-bundles,
      # the list first-launch.sh and terraform's sandbox VM read).
      PROVISA_DEMO_BUNDLES: "$(tr -d '[:space:]' < "$REPO/packaging/linux/demo-stores/sandbox-tier-bundles")"
      # The SQLite sample data of the deployment's demo config, which this tier also builds at
      # startup (first-launch.sh writes the same on a node set up since).
      PROVISA_DEMO_DIR: "/app/config/demo/files"
      # REQ-2006: Polly and every AI role in a sandbox, through OpenRouter (provisa/core/sandbox_ai.py;
      # the model is sandbox.ai_model's, a Claude Haiku). The key is the node's own
      # OPENROUTER_API_KEY (provisa.env, preflight_sandbox_ai), named here, never written here.
      PROVISA_SANDBOX_AI_BASE_URL: "https://openrouter.ai/api/v1"
      PROVISA_SANDBOX_AI_API_KEY: "\$\${env:OPENROUTER_API_KEY}"
      OPENROUTER_API_KEY: "\${OPENROUTER_API_KEY:-}"
  # REQ-2068: the regions sandbox tier runs the same app under its own deployment config
  # (provisa-sandbox-regions.yaml, shipped in CONFIG_FILES); without the tree it ran the image's
  # code and the image's config directory.
  provisa-sandbox-regions:
    environment:
      VITE_ORG_BASE_DOMAIN: "${ORG_BASE}"
    volumes:
      - ${OVERLAY_DIR}/static:/app/static
      - ${OVERLAY_DIR}/provisa:/app/provisa
      - ${OVERLAY_DIR}/buenavista:/app/buenavista
      - ${OVERLAY_DIR}/config:/app/config
      - ${HF_CACHE_DIR}:/root/.cache/huggingface
      - ${RUNTIME_DEPS_DIR}:/opt/provisa/runtime-deps
${REGIONS_NODES_OVERLAY}
YAML
  local want have
  want="$(shasum -a 256 "$STAGE/docker-compose.overlay.yml" | cut -d' ' -f1)"
  # A node that has never had the overlay is the case this function installs, so shasum's failure
  # on a missing file is a probe result, not an abort — the same rule push_obs follows.
  have="$(ssh_node "sudo shasum -a 256 $OVERLAY_EXT 2>/dev/null" | tr -d '\r' | cut -d' ' -f1)" || have=""
  if [ "$want" = "$have" ]; then
    echo "== host overlay: up to date"
    stage_runtime_deps
    return
  fi
  # Seeded from the IMAGE, through a created (never started) container of it. A bind mount hides
  # whatever the image baked at that path, and an empty /app/provisa is an empty import root — the
  # app would not start at all. The pushes ship explicit subsets (CONFIG_FILES, the ui tarball),
  # never the whole tree, so the host copy has to begin as a full copy of the baked one. Never the
  # running container's /app: once an earlier overlay is mounted, that is the host directory this
  # removes first, and the seed copied an empty tree.
  echo "== host overlay: seeding from the image"
  ssh_node "set -e; IMG=\$(sudo docker inspect -f '{{.Config.Image}}' $API_CONTAINER) \
    && SEED=\$(sudo docker create \$IMG) \
    && trap 'sudo docker rm -f \$SEED >/dev/null' EXIT \
    && sudo mkdir -p $OVERLAY_DIR $HF_CACHE_DIR $RUNTIME_DEPS_DIR \
    && for d in static provisa config; do \
         sudo rm -rf $OVERLAY_DIR/\$d \
           && sudo docker cp \$SEED:/app/\$d $OVERLAY_DIR/\$d \
           && echo \"seeded \$d=\$(sudo ls $OVERLAY_DIR/\$d | wc -l)\"; \
       done"
  stage_runtime_deps
  echo "== host overlay: installing overlay"
  scp_node "$STAGE/docker-compose.overlay.yml" /tmp/docker-compose.overlay.yml
  ssh_node "sudo mkdir -p $(dirname $OVERLAY_EXT) \
    && sudo cp /tmp/docker-compose.overlay.yml $OVERLAY_EXT"
  echo "== host overlay: recreating stack"
  ssh_node "sudo systemctl restart provisa"
  # The mount is the whole point of the function, so prove it took rather than assume it: the file
  # is written on the host and read inside the container.
  ssh_node "sudo touch $OVERLAY_DIR/provisa/.overlay-probe \
    && sudo docker exec $API_CONTAINER test -f /app/provisa/.overlay-probe \
    && sudo rm -f $OVERLAY_DIR/provisa/.overlay-probe" \
    || { echo "overlay installed but /app/provisa is not the host directory" >&2; exit 1; }
}

OBS_EXT="/root/.provisa/extensions/observability/docker-compose.observability.yml"

push_obs() {
  # Content-compared, not merely existence-checked: a present-but-stale overlay is the same failure
  # push_app was written to close. The parquet writer's object store lives in this file, so an
  # existence check left otlp2parquet writing to the node's bundled MinIO after the deployment had
  # been pointed at an external store — the writer and the compactor addressed different buckets.
  local want have
  want="$(shasum -a 256 "$REPO/docker-compose.observability.yml" | cut -d' ' -f1)"
  # A node that has never had the overlay is the case this function installs, so shasum's failure
  # on a missing file is a probe result, not an abort — the same rule verify()'s poll follows.
  have="$(ssh_node "sudo shasum -a 256 $OBS_EXT 2>/dev/null" | tr -d '\r' | cut -d' ' -f1)" || have=""
  if [ "$want" = "$have" ]; then
    echo "== observability: up to date"
    return
  fi
  echo "== observability: installing overlay"
  scp_node "$REPO/docker-compose.observability.yml" /tmp/docker-compose.observability.yml
  ssh_node "sudo mkdir -p $(dirname $OBS_EXT) \
    && sudo cp /tmp/docker-compose.observability.yml $OBS_EXT"
  # systemctl, not `docker restart`: the collector container does not exist yet, and the app's
  # OTEL_EXPORTER_OTLP_ENDPOINT is set from the resolved compose set at `provisa start`.
  echo "== observability: recreating stack"
  ssh_node "sudo systemctl restart provisa"
  local otlp
  # printenv exits 1 for an unset variable: that, a failed ssh and an empty value are the same refusal.
  if ! otlp="$(ssh_node "sudo docker exec $API_CONTAINER printenv OTEL_EXPORTER_OTLP_ENDPOINT")" \
    || [ -z "$otlp" ]; then
    echo "observability installed but the app exports no OTLP endpoint" >&2
    exit 1
  fi
}

# REQ-1428: the collector's own config, which the overlay bind-mounts from
# ${PROVISA_HOME}/compose/observability. Only the release installer ever wrote that directory, so a
# change to observability/*.yaml reached the repo and never the node: the node kept running a
# collector without REQ-1425's filter, Trino's per-split/per-task span tree poured into the parquet
# lane, and the compaction job spent every run sweeping tens of thousands of engine-internal files
# for rows no view reads — while the queries report the filter exists to serve stayed empty. Config
# is bind-mounted, so a container restart is enough; no stack recreate, no loss of hot-pushed code.
OBS_CONF_DIR="/root/.provisa/compose/observability"

push_obs_config() {
  local changed=0 name want have
  for f in "$REPO"/observability/*.yaml; do
    name="$(basename "$f")"
    want="$(shasum -a 256 "$f" | cut -d' ' -f1)"
    have="$(ssh_node "sudo shasum -a 256 $OBS_CONF_DIR/$name 2>/dev/null" | tr -d '\r' | cut -d' ' -f1)" || have=""
    [ "$want" = "$have" ] && continue
    scp_node "$f" "/tmp/$name"
    ssh_node "sudo mkdir -p $OBS_CONF_DIR && sudo cp /tmp/$name $OBS_CONF_DIR/$name"
    echo "== collector config: updated $name"
    changed=1
  done
  if [ "$changed" = "0" ]; then
    echo "== collector config: up to date"
    return
  fi
  ssh_node "sudo docker restart compose-otel-collector-1"
}

# The sandbox tier's demo-stores sync (scripts/demo_stores_sync.sh) and its systemd units
# (packaging/linux/demo-stores/). Installed with the node and refreshed by nothing else, so a fix
# to the sync (recreating a store whose first load was cut off) reached no node. Copied when it
# differs; the units are reloaded, and the sync runs again under its own service.
DEMO_STORES_BIN="/root/.provisa/bin/demo_stores_sync.sh"
DEMO_STORES_UNITS="/etc/systemd/system"

# The sandbox VM's demo-stores sync settings (/root/.provisa/demo-stores.env), as
# terraform/gcp-saas/sandbox.tf writes them at boot: packaging/linux/demo-stores/sandbox-vm.env.tpl
# with both tiers' demos (packaging/linux/demo-stores/*-tier-bundles) that have a bundle -- a demo
# is bundled only once it is published (demo.yaml `published: true`).
DEMO_STORES_ENV="/root/.provisa/demo-stores.env"

demo_stores_env() {
  local lists name bundles=()
  lists="$(tr -d '[:space:]' < "$REPO/packaging/linux/demo-stores/sandbox-tier-bundles"),$(tr -d '[:space:]' < "$REPO/packaging/linux/demo-stores/regions-tier-bundles")"
  IFS=',' read -r -a names <<<"$lists"
  for name in "${names[@]}"; do
    [ -n "$name" ] && [ -f "$REPO/demo/named/$name/demo.yaml" ] \
      && grep -qx 'published: true' "$REPO/demo/named/$name/demo.yaml" && bundles+=("$name")
  done
  local joined
  joined="$(IFS=,; echo "${bundles[*]}")"
  printf '%s\n' "$(sed "s|\${bundles}|$joined|" "$REPO/packaging/linux/demo-stores/sandbox-vm.env.tpl")"
}

# A patch refreshes the VM's sync settings: the file is written at boot from the startup script,
# which changes only with a terraform apply, so a demo added to a tier's list would otherwise never
# be pulled. Rewritten only when it differs, then the sync runs at once.
push_demo_stores_env() {
  local want have
  demo_stores_env > "$STAGE/demo-stores.env"
  want="$(shasum -a 256 "$STAGE/demo-stores.env" | cut -d' ' -f1)"
  have="$(ssh_node "sudo shasum -a 256 $DEMO_STORES_ENV 2>/dev/null" | tr -d '\r' | cut -d' ' -f1)" || have=""
  if [ "$want" = "$have" ]; then
    echo "== demo stores: settings up to date"
    return
  fi
  scp_node "$STAGE/demo-stores.env" /tmp/demo-stores.env
  ssh_node "sudo install -m 644 /tmp/demo-stores.env $DEMO_STORES_ENV && sudo systemctl start --no-block provisa-demo-stores.service"
  echo "== demo stores: settings updated ($(grep '^PROVISA_DEMO_BUNDLES=' "$STAGE/demo-stores.env"))"
}

push_demo_stores() {
  local changed=0 f name dest want have
  # A coordinator whose sandbox tier runs on its own VM runs no demo stores: deploy_sandbox_vm
  # installs them there.
  if sandbox_on_own_vm; then
    echo "== demo stores: on $SANDBOX_NODE, not $NODE"
    return
  fi
  for f in "$REPO/scripts/demo_stores_sync.sh" "$REPO/scripts/demo_stores.py" \
      "$REPO/packaging/linux/demo-stores/docker-compose.shared.yml" \
      "$REPO"/packaging/linux/demo-stores/provisa-demo-stores.* \
      "$REPO/packaging/linux/demo-stores/provisa-demo-stores-boot.service"; do
    name="$(basename "$f")"
    case "$name" in
      demo_stores_sync.sh) dest="$DEMO_STORES_BIN" ;;
      demo_stores.py) dest="$(dirname "$DEMO_STORES_BIN")/demo_stores.py" ;;
      docker-compose.shared.yml) dest="$(dirname "$DEMO_STORES_BIN")/demo-stores-shared.yml" ;;
      *) dest="$DEMO_STORES_UNITS/$name" ;;
    esac
    want="$(shasum -a 256 "$f" | cut -d' ' -f1)"
    have="$(ssh_node "sudo shasum -a 256 $dest 2>/dev/null" | tr -d '\r' | cut -d' ' -f1)" || have=""
    [ "$want" = "$have" ] && continue
    scp_node "$f" "/tmp/$name"
    ssh_node "sudo mkdir -p $(dirname "$dest") && sudo install -m $([ "$name" = demo_stores_sync.sh ] && echo 755 || echo 644) /tmp/$name $dest"
    echo "== demo stores: updated $name"
    changed=1
  done
  if [ "$changed" = "0" ]; then
    echo "== demo stores: up to date"
    return
  fi
  ssh_node "sudo systemctl daemon-reload && sudo systemctl enable provisa-demo-stores-boot.service && sudo systemctl start --no-block provisa-demo-stores.service"
}

# The app overlay itself. ${PROVISA_HOME}/compose/docker-compose.app.yml is whatever the release
# installer wrote, and nothing in this script ever refreshed it — so an env passthrough added to the
# repo's overlay (the PROVISA_OTEL_S3_* block the OTLP compactor needs, say) reaches the image but
# never the container, and the fix looks deployed while the node still runs the old environment.
# REQ-2006: the sandbox tier's AI (push_overlay) reads the key from OPENROUTER_API_KEY in the
# provisa.env of the node the tier runs on. The key is the maintainer's to put there; this only
# refuses, by name, a node without it, before anything is pushed to it. No value is printed.
preflight_sandbox_ai() {
  echo "== sandbox AI preflight ($NODE)"
  if ! ssh_node "sudo grep -q '^OPENROUTER_API_KEY=.' $ENV_FILE"; then
    echo "$NODE's $ENV_FILE has no OPENROUTER_API_KEY: Polly in its sandboxes would have no key." >&2
    echo "Add it from the repo .env (the value goes over stdin, never on a command line):" >&2
    echo "  printf 'OPENROUTER_API_KEY=%s\\n' \"\$(sed -n 's/^OPENROUTER_API_KEY=//p' .env | tail -1)\" | gcloud compute ssh $NODE --zone $ZONE --project $PROJECT --command \"sudo sed -i '/^OPENROUTER_API_KEY=/d' $ENV_FILE && sudo tee -a $ENV_FILE >/dev/null\"" >&2
    echo "then re-run." >&2
    exit 1
  fi
  echo "   OPENROUTER_API_KEY=<set>"
}

# Same ordering rule as the extensions: it recreates the stack, so it runs BEFORE the pushes.
APP_YML="/root/.provisa/compose/docker-compose.app.yml"

push_app() {
  local want have
  want="$(shasum -a 256 "$REPO/docker-compose.app.yml" | cut -d' ' -f1)"
  have="$(ssh_node "sudo shasum -a 256 $APP_YML" | tr -d '\r' | cut -d' ' -f1)" || have=""
  # Unlike the extensions, the installer always writes this file: an unreadable one means the node
  # is not a Provisa install, which is why this stays an abort rather than an install.
  [ -n "$have" ] || { echo "cannot read $APP_YML on the node" >&2; exit 1; }
  if [ "$want" = "$have" ]; then
    echo "== app overlay: up to date"
    return
  fi
  echo "== app overlay: updating"
  scp_node "$REPO/docker-compose.app.yml" /tmp/docker-compose.app.yml
  ssh_node "sudo cp $APP_YML $APP_YML.bak-\$(date +%s) && sudo cp /tmp/docker-compose.app.yml $APP_YML"
  echo "== app overlay: recreating stack"
  ssh_node "sudo systemctl restart provisa"
}

# The node's generated control-plane base, the same gap push_app closes for the app overlay.
# first-launch.sh writes ${PROVISA_HOME}/compose-control-plane.yml once, at first boot, and nothing
# here refreshed it: when provisa/ui_server.py came to read PROVISA_SANDBOX_REGIONS_DEMOS as
# region pairs and PROVISA_SANDBOX_REGIONS_NODES as a map, the code reached the node and the
# compose that sets them did not, and the UI container crash-looped on the old value.
#
# Generated, not shipped, like the demo overlay: write_control_plane_compose() holds the only
# copy, so the expected file is that function's own output, run here rather than duplicated. It
# holds none of the node's values: each one is a ${...} that `provisa start` resolves from the
# node's provisa.env (and the launcher's store-credentials.env), so the regenerated file keeps
# them. What it bakes in is the node's PROVISA_HOME and the certificate directory under it.
CP_HOME="/root/.provisa"
CP_YML="$CP_HOME/compose-control-plane.yml"
STORE_CREDENTIALS="$CP_HOME/store-credentials.env"

# Writes write_control_plane_compose's output for the node to $1.
render_control_plane_compose() {
  local fn home="$STAGE/cp-home"
  fn="$(awk '/^write_control_plane_compose\(\) \{$/{f=1} f; f&&/^}$/{exit}' \
    "$REPO/packaging/linux/first-launch.sh")"
  # An empty extraction means the function was restructured and this awk no longer matches.
  grep -q 'compose-control-plane.yml' <<<"$fn" \
    || { echo "cannot extract write_control_plane_compose from first-launch.sh" >&2; exit 1; }
  mkdir -p "$home"
  # In a subshell: ROLE, PROVISA_HOME and CERT_DIR are first-launch's globals, not this script's.
  # PROVISA_HOME is a staging directory (the function writes the file under it) and is put back to
  # the node's in the output.
  (
    ok() { :; }
    ROLE=control-plane PROVISA_HOME="$home" CERT_DIR="$CP_HOME/certs"
    eval "$fn"
    write_control_plane_compose
  )
  sed "s|$home|$CP_HOME|g" "$home/compose-control-plane.yml" > "$1"
  grep -q '^services:' "$1" \
    || { echo "write_control_plane_compose wrote no services" >&2; exit 1; }
}

# The variables a compose file requires (${NAME:?...}) that $2 did not already require and that
# the node's environment ($3: one name per line, each set to a non-empty value) does not set.
# What the installed file requires is already satisfied however the launcher satisfies it (the
# stack resolves it at every start), so only what the shipped file adds is asked of the node.
missing_compose_vars() {
  required() { grep -o '\${[A-Za-z_][A-Za-z0-9_]*:?' "$1" | sed 's/^\${//; s/:?$//' | sort -u; }
  comm -23 <(required "$1") <({ required "$2"; cat "$3"; } | sort -u)
}

push_cp_compose() {
  render_control_plane_compose "$STAGE/compose-control-plane.yml"
  local want have
  want="$(shasum -a 256 "$STAGE/compose-control-plane.yml" | cut -d' ' -f1)"
  have="$(ssh_node "sudo shasum -a 256 $CP_YML" | tr -d '\r' | cut -d' ' -f1)" || have=""
  # Only a control-plane node has one, and every node this script deploys to is one: a missing
  # file is a node that is not, which is an abort, as push_app's is.
  [ -n "$have" ] || { echo "cannot read $CP_YML on $NODE" >&2; exit 1; }
  if [ "$want" = "$have" ]; then
    echo "== control-plane compose: up to date"
    return
  fi
  # A required variable the node does not set stops `provisa start` with the stack down, so it
  # is asked for here, by name, before anything changes. Names only: no value leaves the node.
  ssh_node "sudo cat $CP_YML" | tr -d '\r' > "$STAGE/compose-control-plane.installed.yml"
  ssh_node "sudo sh -c 'cat $ENV_FILE; cat $STORE_CREDENTIALS 2>/dev/null || true'" \
    | tr -d '\r' | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=..*/\1/p' > "$STAGE/node-env-keys"
  local missing
  missing="$(missing_compose_vars "$STAGE/compose-control-plane.yml" \
    "$STAGE/compose-control-plane.installed.yml" "$STAGE/node-env-keys")"
  if [ -n "$missing" ]; then
    echo "the shipped control-plane compose requires what $NODE's $ENV_FILE does not set:" >&2
    sed 's/^/  /' <<<"$missing" >&2
    echo "Set each there and re-run; $CP_YML is unchanged." >&2
    exit 1
  fi
  echo "== control-plane compose: updating"
  diff -u "$STAGE/compose-control-plane.installed.yml" "$STAGE/compose-control-plane.yml" \
    | sed -n 's/^\([-+]\)\([^-+]\)/   \1\2/p' || true
  scp_node "$STAGE/compose-control-plane.yml" /tmp/compose-control-plane.yml
  ssh_node "sudo cp $CP_YML $CP_YML.bak-\$(date +%s) && sudo cp /tmp/compose-control-plane.yml $CP_YML"
  echo "== control-plane compose: recreating stack"
  ssh_node "sudo systemctl restart provisa"
}

# The compose files this script pushes come from the build, while the launcher that interpolates
# them (scripts/provisa) is still the one the node's release installed. A file that requires a
# variable that launcher does not set -- the store address, the OTel object store's account,
# Grafana's admin password -- stops the stack at its next start. Each one missing from the node's
# environment is taken from what the node runs: the address its MinIO is published on, its running
# MinIO's own root account; Grafana's password is generated once. No value is printed.
MINIO_CONTAINER="compose-minio-1"
# What the engine tree no longer holds on disk: the catalog directory an earlier release's file
# catalog store wrote each catalog's properties into, passwords included (the catalogs are in
# Trino's memory now), and the Kafka client properties written before the writer made them 0600
# for Trino alone. The node runs no compose init container that would remove them (its engines are
# pods), so the deploy does.
clear_dead_engine_files() {
  echo "== removing the engine tree's on-disk catalogs"
  # Expanded by root's shell: the ssh user cannot list /root, so its own shell would match nothing.
  ssh_node "sudo sh -c 'rm -rf /root/.provisa/trino/catalog && rm -f /root/.provisa/trino/kafka-client/*.properties'"
}

push_store_env() {
  local have value changed=""
  have="$(ssh_node "sudo cut -d= -f1 $ENV_FILE" | tr -d '\r')"
  if ! grep -qx PROVISA_STORE_BIND_IP <<<"$have"; then
    value="$(ssh_node "sudo sed -n 's/^PROVISA_MINIO_BIND_IP=//p' $ENV_FILE" | tr -d '\r')"
    [ -n "$value" ] || { echo "$ENV_FILE names no PROVISA_MINIO_BIND_IP to publish the stores on" >&2; exit 1; }
    set_node_env PROVISA_STORE_BIND_IP "$value"
    changed=1
  fi
  for pair in "PROVISA_OTEL_S3_ACCESS_KEY MINIO_ROOT_USER" "PROVISA_OTEL_S3_SECRET_KEY MINIO_ROOT_PASSWORD"; do
    set -- $pair
    grep -qx "$1" <<<"$have" && continue
    value="$(ssh_node "sudo docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' $MINIO_CONTAINER" \
      | sed -n "s/^$2=//p" | tr -d '\r')"
    [ -n "$value" ] || { echo "$MINIO_CONTAINER on the node carries no $2" >&2; exit 1; }
    set_node_env "$1" "$value"
    changed=1
  done
  if ! grep -qx PROVISA_GRAFANA_ADMIN_PASSWORD <<<"$have"; then
    set_node_env PROVISA_GRAFANA_ADMIN_PASSWORD "$(openssl rand -hex 24)"
    changed=1
  fi
  if [ -n "$changed" ]; then echo "== store env: reconciled"; else echo "== store env: up to date"; fi
}

push_mail_env() {
  local origin key want_key changed=""
  origin="$(ssh_node "sudo docker exec $API_CONTAINER printenv PROVISA_MAIL_BASE_URL || true" | tr -d '\r')"
  if [ "$origin" = "$SITE" ]; then
    echo "== mail origin: up to date ($SITE)"
  else
    echo "== mail origin: ${origin:-<unset>} -> $SITE"
    set_node_env PROVISA_MAIL_BASE_URL "$SITE"
    changed=1
  fi

  # Read the one key rather than sourcing .env, for the reason verify_engine gives: .env also
  # carries names this script uses, and sourcing it would redirect the deploy at another node.
  want_key="${PROVISA_EMAIL_API_KEY:-$(sed -n 's/^PROVISA_EMAIL_API_KEY=//p' "$REPO/.env" 2>/dev/null | tail -1)}"
  if [ -z "$want_key" ]; then
    echo "PROVISA_EMAIL_API_KEY is not set (repo .env), so this node would accept invitations" >&2
    echo "it cannot send. Set it and re-run." >&2
    exit 1
  fi
  key="$(ssh_node "sudo docker exec $API_CONTAINER printenv PROVISA_EMAIL_API_KEY || true" | tr -d '\r')"
  if [ "$key" = "$want_key" ]; then
    echo "== mail key: up to date"
  else
    echo "== mail key: ${key:+<stale>}${key:-<unset>} -> <set>"
    set_node_env PROVISA_EMAIL_API_KEY "$want_key"
    changed=1
  fi

  [ -n "$changed" ] || return 0
  echo "== mail env: recreating stack"
  ssh_node "sudo systemctl restart provisa"
}

# REQ-1468: the demo overlay, which is what makes the demo sources part of the deployment rather
# than something started by hand. Three of the demo config's sources are served by Provisa's own
# mock backends — petstore-mock:8080 (OpenAPI), graphql-demo:4000 (GraphQL) and grpc-demo:50071 —
# and a node whose overlay predates them registers three sources nothing answers. That is precisely
# how this node ended up with two hand-started containers outside compose: they were not in the
# generated overlay, so nothing recreated them and nothing would have restarted them.
#
# The overlay is generated, not shipped: write_demo_overlay() in packaging/linux/first-launch.sh
# holds the only copy, so the expected content is extracted from that heredoc rather than
# duplicated here — two copies would drift and the installed node would be right by accident.
# Recreates the stack when it changes, so it runs BEFORE the pushes, like the other overlays.
DEMO_EXT="/root/.provisa/extensions/demo/docker-compose.demo.yml"

push_demo() {
  awk '/^  cat > "\$file" <<.YAML.$/{f=1;next} f&&/^YAML$/{exit} f' \
    "$REPO/packaging/linux/first-launch.sh" > "$STAGE/docker-compose.demo.yml"
  # An empty extraction means write_demo_overlay was restructured and this awk no longer matches;
  # installing nothing would take the mocks off the node.
  grep -q 'petstore-mock:' "$STAGE/docker-compose.demo.yml" \
    || { echo "cannot extract the demo overlay from first-launch.sh" >&2; exit 1; }
  local want have
  want="$(shasum -a 256 "$STAGE/docker-compose.demo.yml" | cut -d' ' -f1)"
  have="$(ssh_node "sudo shasum -a 256 $DEMO_EXT 2>/dev/null" | tr -d '\r' | cut -d' ' -f1)" || have=""
  if [ "$want" = "$have" ]; then
    echo "== demo overlay: up to date"
    return
  fi
  echo "== demo overlay: installing"
  scp_node "$STAGE/docker-compose.demo.yml" /tmp/docker-compose.demo.yml
  ssh_node "sudo mkdir -p $(dirname $DEMO_EXT) && sudo cp /tmp/docker-compose.demo.yml $DEMO_EXT"
  # Any hand-started mock is removed first. Compose names its own containers
  # <project>-<service>-1, so a bare `petstore-mock` on the same network does not collide by
  # container name — it collides by DNS: both answer to the service alias the demo config's source
  # URL resolves, and which one a request reaches is then arbitrary.
  for svc in petstore-mock graphql-demo grpc-demo; do
    ssh_node "sudo docker rm -f $svc >/dev/null 2>&1 || true"
  done
  echo "== demo overlay: recreating stack"
  ssh_node "sudo systemctl restart provisa"
  # The mocks are what the demo config's sources resolve to, so their absence is a broken deploy,
  # not a cosmetic gap. Compose names them <project>-<service>-1; match on the service name.
  for svc in petstore-mock graphql-demo grpc-demo; do
    [ -n "$(ssh_node "sudo docker ps --filter name=$svc --format '{{.Names}} {{.Status}}'")" ] \
      || { echo "demo overlay installed but $svc is not running" >&2; exit 1; }
  done
}

# The sandbox tier's own VM (terraform/gcp-saas/sandbox.tf, var.sandbox_vm), when the deployment
# has one: the same overlay (its tree, configs and demo-stores sync) on its disk, so the tier runs
# the code the coordinator runs. It powers itself off when no sandbox is used, and a tree pushed
# to it only while it runs would leave it on the old one at its next start -- so a stopped VM is
# started for the push (it stops itself again once idle).
# The tier's port on the node it runs on: loopback on a node running both profiles, where the UI
# server reaches it by service name (first-launch.sh's default); deploy_sandbox_vm sets the VM's.
SANDBOX_PUBLISH="127.0.0.1:18000"

# The sandbox VM's base compose. first-launch.sh writes it, and runs only when a release changes,
# while every deploy pushes the configs that name what it sets: a config naming a variable the
# node's compose did not (PROVISA_DEMO_LEDGER_ADDRESS) crash-looped the regions tier at startup.
# Rendered by first-launch.sh's own function, as push_demo extracts its overlay, so the two cannot
# drift. Recreates the stack when it changes, so it runs BEFORE the pushes, like the overlays. The
# coordinator's is push_cp_compose's: the UI server reads what it holds (PROVISA_SANDBOX_REGIONS_*).
CONTROL_PLANE_YML="/root/.provisa/compose-control-plane.yml"

push_compose() {
  (
    ROLE=control-plane PROVISA_HOME=/root/.provisa CERT_DIR=/root/.provisa/certs
    ok() { :; }
    eval "$(awk '/^write_control_plane_compose\(\) \{$/,/^}$/' "$REPO/packaging/linux/first-launch.sh")"
    write_control_plane_compose "$STAGE/compose-control-plane.yml"
  )
  # An empty rendering means the function was restructured and this awk no longer matches;
  # installing nothing would take the stack off the node.
  grep -q 'provisa-sandbox-regions:' "$STAGE/compose-control-plane.yml" \
    || { echo "cannot render the control-plane compose from first-launch.sh" >&2; exit 1; }
  local want have
  want="$(shasum -a 256 "$STAGE/compose-control-plane.yml" | cut -d' ' -f1)"
  have="$(ssh_node "sudo shasum -a 256 $CONTROL_PLANE_YML" | tr -d '\r' | cut -d' ' -f1)" || have=""
  if [ "$want" = "$have" ]; then
    echo "== compose: up to date"
    return
  fi
  echo "== compose: updating"
  scp_node "$STAGE/compose-control-plane.yml" /tmp/compose-control-plane.yml
  ssh_node "sudo cp $CONTROL_PLANE_YML $CONTROL_PLANE_YML.bak-\$(date +%s) && sudo cp /tmp/compose-control-plane.yml $CONTROL_PLANE_YML"
  echo "== compose: recreating stack"
  ssh_node "sudo systemctl restart provisa"
}

# The sandbox tier's containers on its VM: the tier and, where they run, the regions tier's nodes.
sandbox_containers() {
  ssh_node "sudo docker ps -a --format '{{.Names}}'" | tr -d '\r' \
    | grep -xE 'compose-provisa-sandbox-1|compose-provisa-sandbox-regions(-warehouse)?-1' | tr '\n' ' '
}

# The sandbox tier back to no orgs of its own but its demo templates still good: every sandbox
# (ids sbx*, REQ-2006) and every other template out of the control plane, their schemas out of it
# and out of the regions tier's two stores, and their folders off the VM. There are no migrations
# (V1): a template built by older code keeps its old tables, and a sandbox opened from it fails on
# a column added since (500 ProgrammingError, "column calendars.day_close does not exist"). So a
# template is kept only when its readiness record names the DDL fingerprint of the pushed code's
# tables (schema_org.ddl_fingerprint, read from that code through the tier's own image and mounts):
# the tier's start trusts it only when its demo version matches too (demo_templates.warm_ready),
# and replaces it otherwise. The coordinator's accounts and orgs are untouched; the tier rebuilds
# each template it does not keep from its bundle at its next start. Runs with NODE set to the
# sandbox VM (deploy_sandbox_vm), its containers stopped so nothing writes while it runs.
reset_sandbox_state() {
  local containers
  containers="$(sandbox_containers)"
  [ -n "$containers" ] || { echo "sandbox VM: no sandbox tier container on $NODE" >&2; exit 1; }
  echo "== sandbox VM: resetting the sandbox tier's orgs (stopping $containers)"
  # A step that fails below ends the script (set -e); the tier is started again on the way out,
  # so a failed reset never leaves it down. Replaced by the stage cleanup alone once it is done.
  trap "echo 'sandbox reset failed: starting $containers again' >&2; ssh_node 'sudo docker start $containers >/dev/null'; rm -rf \"\$STAGE\"" EXIT
  ssh_node "sudo docker stop $containers >/dev/null"
  local tier image ddl kept keep
  tier="${containers%% *}"
  image="$(ssh_node "sudo docker inspect -f '{{.Config.Image}}' $tier" | tr -d '\r')"
  ddl="$(ssh_node "sudo docker run --rm --volumes-from $tier --entrypoint python $image -c 'from provisa.core.schema_org import ddl_fingerprint; print(ddl_fingerprint())'" | tr -d '\r' | tail -1)"
  [[ "$ddl" =~ ^[0-9a-f]{16}$ ]] || { echo "sandbox VM: no DDL fingerprint from the pushed code on $tier: $ddl" >&2; exit 1; }
  cat > "$STAGE/kept_templates.sql" <<SQL
\pset tuples_only on
\pset format unaligned
SELECT template_org FROM public.demo_template_readiness
 WHERE template_org LIKE 'sbxtpl%' AND version LIKE '%@$ddl' ORDER BY template_org;
SQL
  # Only names a template id can be: each is written into the statements below.
  kept="$(run_sql "$STAGE/kept_templates.sql" kept-templates | tr -d '\r' | grep -xE 'sbxtpl[a-z0-9]+' | tr '\n' ' ')" || kept=""
  kept="${kept% }"
  echo "   templates kept (DDL $ddl): ${kept:-none}"
  keep="ARRAY[$(for k in $kept; do printf "'%s'," "$k"; done | sed 's/,$//')]::text[]"
  # One schema per statement, each its own transaction (psql autocommit + \gexec): every
  # template's schema holds hundreds of views, and dropping them all in one transaction runs
  # out of locks (max_locks_per_transaction). Schemas first, then the orgs rows, so a failure
  # part way leaves orgs whose schemas a rerun drops. A kept template's schemas are org_<id> and
  # org_<id>_<more> (environments.org_schema); a template id has no underscore of its own.
  cat > "$STAGE/reset_sandbox.sql" <<SQL
\pset pager off
SELECT format('DROP SCHEMA IF EXISTS %I CASCADE', nspname)
  FROM pg_namespace WHERE nspname LIKE 'org\_sbx%'
   AND NOT EXISTS (SELECT 1 FROM unnest($keep) k
                    WHERE nspname = 'org_' || k OR nspname LIKE 'org\_' || k || '\_%') \gexec
-- Every admin row of an org cascades from its orgs row (provisa/core/schema_admin.py).
DELETE FROM public.orgs WHERE id LIKE 'sbx%' AND id <> ALL($keep);
SELECT count(*) AS sandbox_orgs_left FROM public.orgs WHERE id LIKE 'sbx%';
SQL
  run_sql "$STAGE/reset_sandbox.sql" reset-sandbox
  # The regions tier's stores (first-launch.sh: region-pg-eu, region-pg-us) hold each region's
  # schemas of its orgs, a database per region. Which databases a store holds is the store's own
  # answer, not this script's: the VM's compose comes from its installed release (terraform's
  # provisa_version), which a deploy never rewrites, and a release before REQ-2107's has neither
  # ledger nor warehouse (first-launch.sh's REGION_PG_ALSO).
  cat > "$STAGE/reset_sandbox_store.sql" <<SQL
SELECT format('DROP SCHEMA IF EXISTS %I CASCADE', nspname)
  FROM pg_namespace WHERE nspname LIKE 'org\_sbx%'
   AND NOT EXISTS (SELECT 1 FROM unnest($keep) k
                    WHERE nspname = 'org_' || k OR nspname LIKE 'org\_' || k || '\_%') \gexec
SQL
  local b64 store
  b64="$(base64 < "$STAGE/reset_sandbox_store.sql" | tr -d '\n')"
  local databases database
  for store in $(ssh_node "sudo docker ps --format '{{.Names}}'" | tr -d '\r' | grep -E 'region-pg-(eu|us)'); do
    databases="$(ssh_node "sudo docker exec $store psql -U provisa -d provisa -tAc 'SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname'" | tr -d '\r')"
    [ -n "$databases" ] || { echo "sandbox VM: $store lists no database" >&2; exit 1; }
    for database in $databases; do
      ssh_node "set -e; echo $b64 | base64 -d | sudo docker exec -i $store psql -U provisa -d $database -v ON_ERROR_STOP=1 -q -f -"
      echo "   $store/$database: sandbox schemas dropped"
    done
  done
  # Their folders (provisa.core.org_files: <data dir>/orgs/<org>; first-launch.sh mounts the
  # tiers' data dirs from these), but a kept template's. Only orgs/sbx*: demo-bundles/ beside it is
  # the demo stores sync's. Expanded as root: the login user cannot list /root, where its own shell
  # would expand nothing.
  ssh_node "sudo sh -c 'for d in /root/.provisa/sandbox-data/orgs/sbx* /root/.provisa/sandbox-regions-data/orgs/sbx* /root/.provisa/sandbox-regions-warehouse-data/orgs/sbx*; do case \" $kept \" in *\" \${d##*/} \"*) ;; *) rm -rf \"\$d\" ;; esac; done'"
  trap 'rm -rf "$STAGE"' EXIT
  CONTAINERS="$containers"
}

# A demo opened through the public site, as a visitor opens one, and closed again: the proof the
# sandbox tier serves after a deploy (a template rebuilt from its bundle can take minutes).
verify_sandbox_demo() {
  local demo deadline body code token org
  demo="$(tr -d '[:space:]' < "$REPO/packaging/linux/demo-stores/sandbox-tier-bundles" | cut -d, -f1)"
  echo "== verifying a sandbox opens ($demo)"
  deadline=$(( $(date +%s) + 1800 ))
  while :; do
    body="$(curl -s --max-time 600 -X POST "$SITE/public/demos/$demo")" || body=""
    token="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("token",""))' "$body" 2>/dev/null)" || token=""
    [ -n "$token" ] && break
    code="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("code",""))' "$body" 2>/dev/null)" || code=""
    case "$code" in
      sandbox.demo_not_ready|sandbox.starting|sandbox.demo_starting) ;;
      *) echo "$SITE/public/demos/$demo -> ${body:0:600}" >&2; exit 1 ;;
    esac
    [ "$(date +%s)" -lt "$deadline" ] || { echo "$demo not open in 1800s: ${body:0:600}" >&2; exit 1; }
    sleep 10
  done
  org="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["org_id"])' "$body")"
  echo "   $demo opened as $org"
  curl -s -o /dev/null -w '   closed: %{http_code}\n' -X POST -H "Authorization: Bearer $token" \
    -H "x-org-provisa: $org" "$SITE/sandboxes/closing"
}

# The coordinator, which holds the control plane the sandbox reset changes, up and answering. It
# powers itself off when idle (its front door starts it on a visitor's hit); a reset that reached
# for its database then timed out. Started as deploy_sandbox_vm starts the sandbox VM, then
# awaited on /health, which the API answers only once its lifespan, database included, is up.
ensure_coordinator() {
  local status
  status="$(gcloud compute instances describe "$NODE" --zone "$ZONE" --project "$PROJECT" \
    --format='value(status)')"
  if [ "$status" != "RUNNING" ]; then
    echo "== coordinator $NODE: $status; starting it for the sandbox reset"
    gcloud compute instances start "$NODE" --zone "$ZONE" --project "$PROJECT" >/dev/null
  fi
  # A cold coordinator boots the VM, its database and the API's lifespan (~115s in engine-wake
  # alone): longer than verify_api's budget after a plain restart.
  local i code
  for i in $(seq 1 300); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$SITE/health") || code="unreachable"
    [ "$code" = "200" ] && { echo "== coordinator: $SITE/health -> 200 after $((i * 2))s"; return 0; }
    sleep 2
  done
  echo "coordinator $NODE: $SITE/health -> $code (never reached 200 in 600s)" >&2
  exit 1
}

deploy_sandbox_vm() {
  local status
  status="$(gcloud compute instances describe "$SANDBOX_NODE" --zone "$ZONE" --project "$PROJECT" \
    --format='value(status)' 2>/dev/null)" || status=""
  if [ -z "$status" ]; then
    echo "== sandbox VM: none ($SANDBOX_NODE); the tier runs on $NODE"
    return
  fi
  # Asked of the coordinator before NODE names the sandbox VM below.
  if [ -f "$STAGE/api.tgz" ] || [ "$TARGET" = sandbox-reset ]; then
    ensure_coordinator
  fi
  if [ "$status" != "RUNNING" ]; then
    echo "== sandbox VM: $status; starting it for the push"
    gcloud compute instances start "$SANDBOX_NODE" --zone "$ZONE" --project "$PROJECT" >/dev/null
  fi
  NODE="$SANDBOX_NODE" API_CONTAINER=compose-provisa-sandbox-1 CONTAINERS=compose-provisa-sandbox-1
  SANDBOX_PUBLISH="$(gcloud compute instances describe "$SANDBOX_NODE" --zone "$ZONE" \
    --project "$PROJECT" --format='value(networkInterfaces[0].networkIP)'):8000"
  local i state
  for i in $(seq 1 120); do
    # Empty while the VM boots and docker has not yet listed it.
    state="$(ssh_node "sudo docker inspect -f '{{.State.Status}}' $API_CONTAINER 2>/dev/null" 2>/dev/null \
      | tr -d '\r')" || state=""
    [ "$state" = running ] && break
    # Stopped (a reset that failed before this guard, or a hand stop): nothing else starts it.
    if [ "$state" = exited ] || [ "$state" = created ]; then
      echo "== sandbox VM: $API_CONTAINER is $state; starting it"
      ssh_node "sudo docker start $API_CONTAINER >/dev/null"
    fi
    [ "$i" = 120 ] && { echo "sandbox VM: $API_CONTAINER never came up (last: ${state:-not listed})" >&2; exit 1; }
    sleep 15
  done
  echo "== sandbox VM: $SANDBOX_NODE"
  preflight_sandbox_ai
  # A VM set up by a release from before the regions tier's warehouse node has no such variable.
  grep -qx PROVISA_SANDBOX_REGIONS_WAREHOUSE_PUBLISH <<<"$(ssh_node "sudo cut -d= -f1 $ENV_FILE" | tr -d '\r')" \
    || set_node_env PROVISA_SANDBOX_REGIONS_WAREHOUSE_PUBLISH "${SANDBOX_PUBLISH%:*}:8101"
  REGIONS_NODES_OVERLAY="  provisa-sandbox-regions-warehouse:
    volumes:
      - ${OVERLAY_DIR}/static:/app/static
      - ${OVERLAY_DIR}/provisa:/app/provisa
      - ${OVERLAY_DIR}/buenavista:/app/buenavista
      - ${OVERLAY_DIR}/config:/app/config
      - ${HF_CACHE_DIR}:/root/.cache/huggingface
      - ${RUNTIME_DEPS_DIR}:/opt/provisa/runtime-deps"
  push_demo_stores; push_demo_stores_env; push_compose; push_overlay
  # What this deploy built: the api arm has no configs, the cfg arm no package.
  [ -f "$STAGE/api.tgz" ] && { push_api; push_plugins; push_deps; }
  [ -f "$STAGE/cfg.tgz" ] && push_cfg
  # New code against the tier's old tables fails (no migrations): whenever the package is pushed,
  # or asked for alone (sandbox-reset), the tier starts from no orgs of its own but the templates
  # built under the pushed code's table shapes.
  if [ -f "$STAGE/api.tgz" ] || [ "$TARGET" = sandbox-reset ]; then
    reset_sandbox_state
  fi
  restart
  verify_sandbox_demo
}

case "$TARGET" in
  ui)
    { sandbox_on_own_vm || preflight_sandbox_ai; }; push_overlay; build_ui; push_ui; verify; verify_org_base ;;
  api)
    # Keeps accounts and orgs: only 'reset' wipes them. verify_demo is a statement about a reset.
    { sandbox_on_own_vm || preflight_sandbox_ai; }; preflight_engine; push_commerce_env; push_engine_env; build_api; build_deps; push_store_env; push_app; push_cp_compose; preflight_commerce; push_mail_env; push_obs; push_obs_config; push_demo; push_demo_stores; push_overlay; push_api; push_plugins; push_deps; restart; verify; verify_api; verify_engine; verify_org_base; verify_commerce; deploy_sandbox_vm ;;
  cfg)
    # Restarts: the config is read once at startup, so a pushed file is inert until then.
    { sandbox_on_own_vm || preflight_sandbox_ai; }; preflight_engine; push_commerce_env; push_engine_env; build_cfg; clear_dead_engine_files; push_store_env; push_app; push_cp_compose; preflight_commerce; push_mail_env; push_obs; push_obs_config; push_demo; push_demo_stores; push_overlay; push_cfg; restart; verify; verify_api; verify_engine; verify_org_base; verify_commerce; deploy_sandbox_vm ;;
  all)
    # Keeps accounts and orgs: only 'reset' wipes them. verify_demo is a statement about a reset.
    { sandbox_on_own_vm || preflight_sandbox_ai; }; preflight_engine; push_commerce_env; push_engine_env; build_ui; build_api; build_deps; build_cfg; clear_dead_engine_files; push_store_env; push_app; push_cp_compose; preflight_commerce; push_mail_env; push_obs; push_obs_config; push_demo; push_demo_stores; push_overlay; push_ui; push_api; push_plugins; push_cfg; push_deps; restart; verify; verify_api; verify_engine; verify_org_base; verify_commerce; deploy_sandbox_vm ;;
  sandbox-reset)
    # No build and no push: the sandbox tier's orgs reset and the tier restarted (its VM's
    # overlay, demo stores and settings are synced as on every deploy that reaches it). It needs
    # the VM; with one, the leading node check is the VM's own (deploy_sandbox_vm), as on every arm.
    { sandbox_on_own_vm || preflight_sandbox_ai; }; sandbox_on_own_vm || { echo "no sandbox VM ($SANDBOX_NODE): the tier runs on $NODE, which 'reset' resets" >&2; exit 2; }; deploy_sandbox_vm ;;
  reset)
    # No build: 'ui' deliberately has no reset arm because it never restarts.
    preflight_engine; push_commerce_env; push_engine_env; preflight_commerce; reset_state; restart; verify; verify_api; verify_demo; verify_engine; verify_org_base; verify_commerce ;;
  patch)
    # verify_demo is skipped, not weakened: it asserts zero accounts, which is a statement
    # about the reset, and 'patch' exists precisely to keep the accounts that are there.
    { sandbox_on_own_vm || preflight_sandbox_ai; }; preflight_engine; push_commerce_env; push_engine_env; build_ui; build_api; build_deps; build_cfg; clear_dead_engine_files; push_store_env; push_app; push_cp_compose; preflight_commerce; push_mail_env; push_obs; push_obs_config; push_demo; push_demo_stores; push_overlay; push_ui; push_api; push_plugins; push_cfg; push_deps; restart; verify; verify_api; verify_engine; verify_org_base; verify_commerce; deploy_sandbox_vm ;;
esac
echo "== deployed $TARGET"
