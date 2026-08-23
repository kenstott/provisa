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
# Both compose-provisa-1 (API) and compose-provisa-ui-1 (UI proxy) run the full
# application image and each serve their own copy of /app/static, /app/provisa and
# /app/config, so every target is patched in both or the two disagree about what is
# deployed.
#
#   scripts/deploy-cloud.sh ui     # vite build -> /app/static (no restart needed)
#   scripts/deploy-cloud.sh api    # python package -> /app/provisa (restarts both)
#   scripts/deploy-cloud.sh cfg    # shipped configs -> /app/config (restarts both)
#   scripts/deploy-cloud.sh all    # all three, one restart at the end
#   scripts/deploy-cloud.sh reset  # accounts/orgs -> none, demo data re-seeded
#   scripts/deploy-cloud.sh patch  # like all, but KEEPS accounts and orgs
#
# Every arm that pushes first ensures the observability and demo overlays are installed on the
# node — the ops reports over the OTLP parquet store (traces, queries, metrics, logs) are empty
# without a collector, and the demo config's OpenAPI/GraphQL/gRPC sources are unresolvable without
# the mock backends (REQ-1468). Both are content-compared; already-current is a no-op.
#
# api/all/reset also wipe the control plane back to the first-start state (no
# claimed super-admin, no accounts, no tenant orgs) and let the restart re-seed
# the bootstrap org's demo data set from the deployment config. Use `patch` when
# a sign-in session or a hand-built org under test has to survive the deploy.
#
# Env overrides: NODE, ZONE, PROJECT, CONTAINERS, SITE, SQL_IMAGE.
set -euo pipefail

NODE="${NODE:-provisa-saas-coordinator}"
ZONE="${ZONE:-us-central1-a}"
PROJECT="${PROJECT:-provisa-test-473}"
CONTAINERS="${CONTAINERS:-compose-provisa-1 compose-provisa-ui-1}"
SITE="${SITE:-https://cloud.provisa.dev}"
# The control plane lives on Cloud SQL. Nothing on the node holds it and no container is a
# long-lived psql client, so SQL runs in a throwaway container joined to the app's own network —
# which is the network that reaches the private Cloud SQL address.
SQL_IMAGE="${SQL_IMAGE:-postgres:16}"
API_CONTAINER="${API_CONTAINER:-compose-provisa-1}"

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

TARGET="${1:-}"
case "$TARGET" in
  ui|api|cfg|all|reset|patch) ;;
  *) echo "usage: $(basename "$0") {ui|api|cfg|all|reset|patch}" >&2; exit 2 ;;
esac

ssh_node() { gcloud compute ssh "$NODE" --zone "$ZONE" --project "$PROJECT" --command "$1"; }
scp_node() { gcloud compute scp "$1" "$NODE:$2" --zone "$ZONE" --project "$PROJECT"; }

# The engine contract this node must satisfy before anything is pushed. The control plane runs no
# engine locally: provisioner_settings() (provisa/federation/k8s_provisioner.py:124-141) raises
# unless every one of these is set, provisioning_available() then reports false, and the node
# silently degrades to "routes to an engine somebody else operates" — i.e. no engine at all. Read
# from the RUNNING container, not from provisa.env: the container's environment is what the code
# sees, and a provisa.env edit that was never applied by a stack recreate is exactly the gap this
# is here to catch. PROVISA_ENGINE_SHARD is separate from the provisioner settings —
# engine_wake.boot_shard() (provisa/federation/engine_wake.py:86) raises without it, so boot dies
# after the app is already listening.
ENGINE_KEYS="PROVISA_ENGINE_CLUSTER_PROJECT PROVISA_ENGINE_CLUSTER_LOCATION \
PROVISA_ENGINE_CLUSTER_NAME PROVISA_ENGINE_CLUSTER_ZONE PROVISA_ENGINE_IMAGE \
PROVISA_ZAYCHIK_IMAGE PROVISA_ENGINE_SHARD"

# The SaaS node runs Trino — terraform/gcp-saas pins it and nothing else is supported here, so the
# engine is asserted rather than discovered. Reading the pin and keeping the shipped value when it
# came back empty is what once put duckdb on the node: a regenerated provisa.env dropped
# PROVISA_ENGINE, the read returned nothing, and the deploy quietly shipped the desktop default.
ENGINE_PIN="trino"

preflight_engine() {
  echo "== engine preflight"
  local env_dump running missing=""
  env_dump="$(ssh_node "sudo docker exec $API_CONTAINER printenv | grep -E '^(PROVISA_ENGINE|PROVISA_ZAYCHIK)' || true" | tr -d '\r')"
  running="$(printf '%s\n' "$env_dump" | sed -n 's/^PROVISA_ENGINE=//p')"
  if [ "$running" != "$ENGINE_PIN" ]; then
    echo "$API_CONTAINER runs PROVISA_ENGINE='${running:-<unset>}', expected '$ENGINE_PIN'." >&2
    echo "Fix the node's /root/.provisa/provisa.env and recreate the containers, then re-run." >&2
    exit 1
  fi
  for k in $ENGINE_KEYS; do
    printf '%s\n' "$env_dump" | grep -q "^$k=." || missing="$missing $k"
  done
  if [ -n "$missing" ]; then
    echo "$API_CONTAINER is missing engine settings:$missing" >&2
    echo "Without them the control plane provisions no shard and every query fails." >&2
    exit 1
  fi
  printf '%s\n' "$env_dump" \
    | grep -E '^(PROVISA_ENGINE|PROVISA_ENGINE_SHARD|PROVISA_ENGINE_IMAGE|PROVISA_ZAYCHIK_IMAGE|PROVISA_ENGINE_CLUSTER_NAME|PROVISA_ENGINE_CLUSTER_MODE)=' \
    | sed 's/^/   /'
}

# REQ-1455/REQ-1514: on a HOSTED node the plan catalog is the first thing the signup page asks for,
# and variant_id_for_plan raises rather than inventing an id, so a node missing these answers
# GET /billing/catalog with a 500 and "Get started" cannot create an organization at all. The
# variant ids ARE the billing configuration -- there is no default to fall back to -- so the deploy
# asserts them the way it asserts the engine pin, from the RUNNING container.
COMMERCE_KEYS="LEMONSQUEEZY_VARIANT_STARTER LEMONSQUEEZY_VARIANT_PRO_S \
LEMONSQUEEZY_VARIANT_PRO_M LEMONSQUEEZY_VARIANT_PRO_L \
LEMONSQUEEZY_VARIANT_EGRESS_STARTER LEMONSQUEEZY_VARIANT_EGRESS_PRO_S \
LEMONSQUEEZY_VARIANT_EGRESS_PRO_M LEMONSQUEEZY_VARIANT_EGRESS_PRO_L \
LEMONSQUEEZY_API_KEY LEMONSQUEEZY_STORE_ID LEMONSQUEEZY_SIGNING_SECRET"

preflight_commerce() {
  echo "== commerce preflight"
  local env_dump missing=""
  env_dump="$(ssh_node "sudo docker exec $API_CONTAINER printenv | grep -E '^LEMONSQUEEZY' || true" | tr -d '\r')"
  for k in $COMMERCE_KEYS; do
    printf '%s\n' "$env_dump" | grep -q "^$k=." || missing="$missing $k"
  done
  if [ -n "$missing" ]; then
    echo "$API_CONTAINER is missing commerce settings:$missing" >&2
    echo "Without them /billing/catalog 500s and nobody can sign up." >&2
    echo "Add them to the node's /root/.provisa/provisa.env and recreate the containers." >&2
    exit 1
  fi
  printf '%s\n' "$env_dump" | sed -n 's/^\(LEMONSQUEEZY_[A-Z_]*\)=.*/   \1=<set>/p'
}

build_ui() {
  echo "== building UI"
  (cd "$REPO/provisa-ui" && npm run build)
  # index.html carries the hashed bundle names, so it ships with the assets it references.
  # auth-relay.html is the second HTML entry (REQ-1348) and does the same for the relay bundle;
  # left behind, the control plane answers an org subdomain's token request with a stale page or
  # a 502 and sign-in on that subdomain never completes.
  # voyager/ is the vendored GraphQL Voyager bundle the Schema Explorer's SDL iframe loads;
  # it is part of the UI build output, so a patch that left it behind could serve a node whose
  # image predates it and the SDL view would render blank.
  tar czf "$STAGE/ui.tgz" -C "$REPO/provisa-ui/dist" assets index.html auth-relay.html voyager
}

push_ui() {
  echo "== pushing UI ($(du -h "$STAGE/ui.tgz" | cut -f1))"
  scp_node "$STAGE/ui.tgz" /tmp/provisa-ui-deploy.tgz
  for c in $CONTAINERS; do
    # The old assets are removed first: filenames are content-hashed, so extracting
    # over them accumulates every bundle ever deployed and hides which one is live.
    ssh_node "sudo docker exec $c sh -c 'rm -rf /app/static/assets' \
      && sudo docker cp /tmp/provisa-ui-deploy.tgz $c:/app/static/ui.tgz \
      && sudo docker exec $c sh -c 'cd /app/static && tar xzf ui.tgz && rm ui.tgz && echo \"$c assets=\$(ls assets | wc -l)\"'"
  done
}

build_api() {
  echo "== packaging python package"
  tar czf "$STAGE/api.tgz" -C "$REPO" --exclude='__pycache__' --exclude='*.pyc' provisa
}

push_api() {
  echo "== pushing python package ($(du -h "$STAGE/api.tgz" | cut -f1))"
  scp_node "$STAGE/api.tgz" /tmp/provisa-api-deploy.tgz
  for c in $CONTAINERS; do
    # Extracted over /app/provisa rather than replacing it: the tree is the import root
    # of the running interpreter, and a deleted-then-recreated directory races the restart.
    ssh_node "sudo docker cp /tmp/provisa-api-deploy.tgz $c:/app/api.tgz \
      && sudo docker exec $c sh -c 'cd /app && tar xzf api.tgz && rm api.tgz && echo \"$c provisa=\$(ls provisa | wc -l)\"'"
  done
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
provisa-install.yaml provisa-install-base.yaml"

build_cfg() {
  echo "== packaging configs"
  tar czf "$STAGE/cfg.tgz" -C "$REPO/config" $CONFIG_FILES pgbouncer
}

# Distributions that are not part of this source tree but that the deployed node runs on top of it.
# provisa.core.commerce is the ONE seam they bind to (REQ-1473) and every hook there no-ops when the
# import fails, so a checkout without them deploys a node that runs — it just has no billing. The
# hook is sourced rather than executed: it patches the same containers through ssh_node/scp_node and
# has no business re-deriving the node, the zone or the container list.
push_plugins() {
  local hook="$REPO/.claude/commercial/deploy-plugin.sh"
  if [ ! -f "$hook" ]; then
    echo "== no private plugins in this checkout; the node deploys without them"
    return 0
  fi
  # shellcheck source=/dev/null
  . "$hook"
  push_commercial
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
  for c in $CONTAINERS; do
    ssh_node "sudo docker cp /tmp/provisa-cfg-deploy.tgz $c:/app/config/cfg.tgz \
      && sudo docker exec $c sh -c 'cd /app/config && tar xzf cfg.tgz && rm cfg.tgz \
        && sed -i \"s|^federation_engine:.*|federation_engine: $pin|\" provisa-install.yaml \
        && echo \"$c config=\$(ls *.yaml | wc -l) \$(grep ^federation_engine: provisa-install.yaml)\"'"
  done
}

# Runs SQL against the control plane, which is Cloud SQL — there is no database container on the
# node to exec into. A throwaway psql container is joined to the API container's own network,
# because that network is what carries the route to the private Cloud SQL address; the DSN is read
# out of the API container's env inside the remote shell and passed by file, so the password stays
# out of both the transcript and the node's process list. The +driver part of the SQLAlchemy URL is
# stripped because libpq rejects it.
run_sql() {
  local sql_file="$1" label="$2"
  local b64
  b64="$(base64 < "$sql_file" | tr -d '\n')"
  ssh_node "set -e
    echo $b64 | base64 -d | sudo tee /tmp/provisa-$label.sql >/dev/null
    NET=\$(sudo docker inspect -f '{{range \$k, \$v := .NetworkSettings.Networks}}{{\$k}}{{end}}' $API_CONTAINER)
    [ -n \"\$NET\" ] || { echo 'cannot resolve the network of $API_CONTAINER' >&2; exit 1; }
    DSN=\$(sudo docker exec $API_CONTAINER printenv PLATFORM_DATABASE_URL | sed -E 's|^([a-z]+)\+[a-z0-9]+://|\1://|')
    # ORG_ID names the bootstrap org's schema (provisa/api/app.py:216 — 'default' when unset).
    BOOT=\$(sudo docker exec $API_CONTAINER sh -c 'echo \${ORG_ID:-default}')
    # The DSN is expanded INSIDE the container, not by the node's shell: as a command-line
    # argument it would stand in the node's process list for the life of the query.
    sudo docker run --rm -i --network \"\$NET\" -e PGDSN=\"\$DSN\" -e BOOT=\"\$BOOT\" \
      -v /tmp/provisa-$label.sql:/tmp/q.sql:ro --entrypoint sh $SQL_IMAGE \
      -c 'psql \"\$PGDSN\" -v ON_ERROR_STOP=1 -v boot=\"\$BOOT\" -f /tmp/q.sql'"
}

reset_state() {
  echo "== resetting accounts and orgs"
  cat > "$STAGE/reset.sql" <<'SQL'
\pset pager off
-- set_config, not a psql variable: psql does not interpolate :'boot' inside a dollar-quoted
-- DO body, and the schema sweep below has to be dynamic SQL.
SELECT set_config('provisa.boot_org', :'boot', false);
BEGIN;
-- Every DELETE carries a WHERE: the SQL classifier rejects an unqualified DELETE outright.
DELETE FROM public.superadmin_bootstrap WHERE id IS NOT NULL;
DELETE FROM public.org_invites WHERE org_id IS NOT NULL;
DELETE FROM public.user_org_memberships WHERE user_id IS NOT NULL;
DELETE FROM public.user_profiles WHERE user_id IS NOT NULL;
DELETE FROM public.orgs WHERE id <> current_setting('provisa.boot_org');
DO $$
DECLARE
  s text;
  boot text := current_setting('provisa.boot_org');
BEGIN
  -- The bootstrap org's schema stays: startup re-seeds its registry row unconditionally
  -- (provisa/core/schema_admin.py:236-255), so dropping it only makes it reappear empty.
  -- Its tenant-plane role assignments do go, or the wiped accounts keep their grants.
  EXECUTE format('DELETE FROM %I.user_role_assignments WHERE user_id IS NOT NULL', 'org_' || boot);
  FOR s IN
    SELECT nspname::text FROM pg_namespace
     WHERE nspname LIKE 'org\_%'
       AND nspname <> 'org_' || boot
       AND nspname <> 'org_' || boot || '_mv_cache'
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', s);
  END LOOP;
END $$;
COMMIT;
SQL
  run_sql "$STAGE/reset.sql" reset
}

restart() {
  echo "== restarting"
  ssh_node "sudo docker restart $CONTAINERS"
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
  ssh_node "sudo docker exec $API_CONTAINER printenv OTEL_EXPORTER_OTLP_ENDPOINT" \
    | grep -q . || { echo "observability installed but the app exports no OTLP endpoint" >&2; exit 1; }
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

# The app overlay itself. ${PROVISA_HOME}/compose/docker-compose.app.yml is whatever the release
# installer wrote, and nothing in this script ever refreshed it — so an env passthrough added to the
# repo's overlay (the PROVISA_OTEL_S3_* block the OTLP compactor needs, say) reaches the image but
# never the container, and the fix looks deployed while the node still runs the old environment.
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
    ssh_node "sudo docker ps --filter name=$svc --format '{{.Names}} {{.Status}}'" | grep -q . \
      || { echo "demo overlay installed but $svc is not running" >&2; exit 1; }
  done
}

# The demo data set is not pushed by this script: startup rebuilds the bootstrap org from the
# deployment's own config on every boot (REQ-1296), and dropping the tenant schemas above also
# cascaded away the cross-org org_registry view (REQ-1301). Both are restart-rebuilt, so this
# asserts the rebuild actually happened instead of assuming it.
verify_demo() {
  echo "== verifying demo data set"
  cat > "$STAGE/verify_demo.sql" <<'SQL'
\pset pager off
SELECT set_config('provisa.boot_org', :'boot', false);
DO $$
DECLARE
  boot text := current_setting('provisa.boot_org');
  n_src int; n_tbl int; n_view int; n_org text; n_acct int;
BEGIN
  EXECUTE format('SELECT count(*) FROM %I.sources', 'org_' || boot) INTO n_src;
  EXECUTE format('SELECT count(*) FROM %I.registered_tables', 'org_' || boot) INTO n_tbl;
  SELECT count(*) INTO n_view FROM pg_views
   WHERE schemaname = 'org_' || boot AND viewname = 'org_registry';
  SELECT string_agg(id, ',' ORDER BY id) INTO n_org FROM public.orgs;
  SELECT count(*) INTO n_acct FROM public.user_profiles;
  RAISE INFO 'orgs=% accounts=% sources=% tables=% org_registry=%', n_org, n_acct, n_src, n_tbl, n_view;
  IF n_src = 0 OR n_tbl = 0 THEN
    RAISE EXCEPTION 'demo data set missing after restart: sources=% tables=%', n_src, n_tbl;
  END IF;
  IF n_view = 0 THEN
    RAISE EXCEPTION 'org_registry view was not rebuilt after the reset';
  END IF;
  IF n_acct <> 0 THEN
    RAISE EXCEPTION 'reset left % account(s) behind', n_acct;
  END IF;
END $$;
SQL
  run_sql "$STAGE/verify_demo.sql" verify-demo
}

verify() {
  echo "== verifying $SITE"
  # Accept: text/html marks this a document request; ui_server proxies anything else to
  # the API, where an unauthenticated GET / is a 401 and says nothing about the deploy.
  # 300s. The old 120s budget was shorter than a cold restart on this node, where the API's
  # lifespan spends ~115s in engine-wake alone, so the poll reported 503 on a deploy that then
  # came up seconds later.
  for i in $(seq 1 150); do
    # A refused connection is the expected state while the container restarts, so it is
    # recorded as a probe result instead of tripping `set -e` and aborting the poll. It is
    # never treated as success: the loop still fails loudly below if 200 never arrives.
    code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$SITE/") || code="unreachable"
    [ "$code" = "200" ] && { echo "$SITE/ -> 200 after $((i * 2))s"; return 0; }
    sleep 2
  done
  echo "$SITE/ -> $code (never reached 200)" >&2
  return 1
}

# The document request above is answered by the UI container, which serves static assets the
# moment it restarts — it says nothing about the API, whose lifespan re-seeds the bootstrap org
# and rebuilds the org_registry view. Checking the demo data set on that signal alone read the
# database mid-startup and failed on a view the API had not created yet. /health is served by the
# API itself and only answers once the lifespan has completed, so it is the readiness gate.
verify_api() {
  echo "== verifying API readiness"
  for i in $(seq 1 90); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$SITE/health") || code="unreachable"
    [ "$code" = "200" ] && { echo "$SITE/health -> 200 after $((i * 2))s"; return 0; }
    sleep 2
  done
  echo "$SITE/health -> $code (never reached 200)" >&2
  return 1
}

# The end-to-end signal for this topology: /health only proves the app started, and the app starts
# whether or not it can reach a shard. A query is what wakes the shard, re-establishes the shared
# terminal on the coordinator that answers (REQ-1448), and therefore proves the whole path — control
# plane, GKE provisioner, Trino shard — is live. It runs against the public site with the
# superuser's basic-auth credentials from the repo .env, which is where they live; a query that has
# to cold-start a shard takes north of a minute, so the timeout is generous by design.
# REQ-1562: the env keys preflight_commerce asserts are only half of what signup needs -- the OTHER
# half is the plugin that reads them. provisa.core.commerce imports provisa_commercial inside a try
# and every hook no-ops when it fails, which is right for a self-hosted install and wrong for this
# node: a hosted deployment whose seam is off mounts no /billing routes at all, so the signup page's
# GET /billing/catalog has no route and "Get started" cannot create an organization. The seam is
# silent by design, so the deploy is what has to speak. Asserted AFTER the restart, because
# push_plugins is what lands the tree and the import is resolved once per process.
#
# Only when this checkout carries the plugin: push_plugins already treats a checkout without the
# hook as a deliberate no-billing deploy, and this verify holds to the same rule.
verify_commerce() {
  [ -f "$REPO/.claude/commercial/deploy-plugin.sh" ] || return 0
  echo "== verifying the commercial seam"
  local seam
  seam="$(ssh_node "sudo docker exec $API_CONTAINER python -c \"import provisa.core.commerce as c; print(c.enabled())\"" | tr -d '\r')"
  case "$seam" in
    *True*) echo "   provisa.core.commerce.enabled() -> True" ;;
    *) echo "$API_CONTAINER did not load provisa_commercial: $seam" >&2
       echo "Without it no /billing routes mount and nobody can sign up." >&2
       exit 1 ;;
  esac
}

verify_engine() {
  echo "== verifying the engine path (cold start may take ~90s)"
  # Read the two keys rather than sourcing .env: .env also carries names this script uses
  # (PROJECT, ZONE), and sourcing it would silently redirect the deploy at another node.
  local su_user su_pass
  su_user="${PROVISA_SUPERUSER_USERNAME:-$(sed -n 's/^PROVISA_SUPERUSER_USERNAME=//p' "$REPO/.env" 2>/dev/null | tail -1)}"
  su_pass="${PROVISA_SUPERUSER_PASSWORD:-$(sed -n 's/^PROVISA_SUPERUSER_PASSWORD=//p' "$REPO/.env" 2>/dev/null | tail -1)}"
  if [ -z "$su_user" ] || [ -z "$su_pass" ]; then
    echo "PROVISA_SUPERUSER_USERNAME/PASSWORD are not set (repo .env), so the engine path" >&2
    echo "cannot be exercised. Set them and re-run." >&2
    exit 1
  fi
  local body
  body="$(curl -s --max-time 240 -u "$su_user:$su_pass" \
    -H 'Content-Type: application/json' -d '{"sql":"SELECT 1 AS ok"}' "$SITE/data/sql")" || {
      echo "the engine query never returned" >&2; exit 1; }
  case "$body" in
    *'"ok":1'*) echo "$SITE/data/sql -> $body" ;;
    *) echo "$SITE/data/sql -> $body" >&2; exit 1 ;;
  esac
}

case "$TARGET" in
  ui)
    build_ui; push_ui; verify ;;
  api)
    # reset before restart: the wipe drops the tenant schemas and the org_registry view, and it
    # is the restart that re-seeds the bootstrap org and rebuilds that view.
    preflight_engine; preflight_commerce; build_api; push_app; push_obs; push_obs_config; push_demo; push_api; push_plugins; reset_state; restart; verify; verify_api; verify_demo; verify_engine; verify_commerce ;;
  cfg)
    # Restarts: the config is read once at startup, so a pushed file is inert until then.
    preflight_engine; preflight_commerce; build_cfg; push_app; push_obs; push_obs_config; push_demo; push_cfg; restart; verify; verify_api; verify_engine; verify_commerce ;;
  all)
    preflight_engine; preflight_commerce; build_ui; build_api; build_cfg; push_app; push_obs; push_obs_config; push_demo; push_ui; push_api; push_plugins; push_cfg; reset_state; restart; verify; verify_api; verify_demo; verify_engine; verify_commerce ;;
  reset)
    # No build: 'ui' deliberately has no reset arm because it never restarts.
    preflight_engine; preflight_commerce; reset_state; restart; verify; verify_api; verify_demo; verify_engine; verify_commerce ;;
  patch)
    # verify_demo is skipped, not weakened: it asserts zero accounts, which is a statement
    # about the reset, and 'patch' exists precisely to keep the accounts that are there.
    preflight_engine; preflight_commerce; build_ui; build_api; build_cfg; push_app; push_obs; push_obs_config; push_demo; push_ui; push_api; push_plugins; push_cfg; restart; verify; verify_api; verify_engine; verify_commerce ;;
esac
echo "== deployed $TARGET"
