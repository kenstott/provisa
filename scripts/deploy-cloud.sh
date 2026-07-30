#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# Compile local source and push it straight into the running cloud.provisa.dev
# containers, skipping the image rebuild + terraform re-provision cycle.
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
# api/all/reset also wipe the control plane back to the first-start state (no
# claimed super-admin, no accounts, no tenant orgs) and let the restart re-seed
# the bootstrap org's demo data set from the deployment config. Use `patch` when
# a sign-in session or a hand-built org under test has to survive the deploy.
#
# Env overrides: NODE, ZONE, PROJECT, CONTAINERS, SITE, DB_CONTAINER.
set -euo pipefail

NODE="${NODE:-provisa-saas-coordinator}"
ZONE="${ZONE:-us-central1-a}"
PROJECT="${PROJECT:-provisa-test-473}"
CONTAINERS="${CONTAINERS:-compose-provisa-1 compose-provisa-ui-1}"
SITE="${SITE:-https://cloud.provisa.dev}"
# The control plane lives on Cloud SQL, not in this container; it is only the psql client that
# can reach it from inside the VPC. Its own database holds the demo tables (db/init.sql).
DB_CONTAINER="${DB_CONTAINER:-compose-postgres-1}"
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

build_ui() {
  echo "== building UI"
  (cd "$REPO/provisa-ui" && npm run build)
  # index.html carries the hashed bundle names, so it ships with the assets it references.
  tar czf "$STAGE/ui.tgz" -C "$REPO/provisa-ui/dist" assets index.html
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
# node loads /app/config/provisa-install.yaml (packaging/linux/first-launch.sh:429 sets
# PROVISA_CONFIG), so a config change that never shipped left the node registering a stale source
# and column-grant set: the pre-REQ-1297 copy still named platform_admin in every column's
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

push_cfg() {
  echo "== pushing configs ($(du -h "$STAGE/cfg.tgz" | cut -f1))"
  scp_node "$STAGE/cfg.tgz" /tmp/provisa-cfg-deploy.tgz
  # federation_engine is rewritten to the node's own $PROVISA_ENGINE pin
  # (terraform/gcp-saas/main.tf:224 exports it) rather than shipped as a second SaaS config file.
  # The pin already decides which engine boots; without this the persisted selection stayed at the
  # shipped desktop default (duckdb) and /admin/federation-engine reported a saved selection that
  # disagreed with the running engine on every page load.
  #
  # The pin is read from the API container and applied to every container: only compose-provisa-1
  # gets PROVISA_ENGINE from provisa.env, so reading it per container left compose-provisa-ui-1
  # holding a config that named a different engine than its sibling. An unpinned deployment leaves
  # the shipped value alone — there the config IS the selection.
  local pin
  pin="$(ssh_node "sudo docker exec $API_CONTAINER printenv PROVISA_ENGINE || true" | tr -d '\r\n')"
  echo "== engine pin: ${pin:-<none, keeping shipped selection>}"
  for c in $CONTAINERS; do
    ssh_node "sudo docker cp /tmp/provisa-cfg-deploy.tgz $c:/app/config/cfg.tgz \
      && sudo docker exec $c sh -c 'cd /app/config && tar xzf cfg.tgz && rm cfg.tgz \
        && if [ -n \"$pin\" ]; then \
             sed -i \"s|^federation_engine:.*|federation_engine: $pin|\" provisa-install.yaml; \
           fi \
        && echo \"$c config=\$(ls *.yaml | wc -l) \$(grep ^federation_engine: provisa-install.yaml)\"'"
  done
}

# Runs SQL against the control plane. The DSN is read out of the API container's env inside the
# remote shell and never echoed, so the password stays out of the transcript. The +driver part of
# the SQLAlchemy URL is stripped because libpq rejects it.
run_sql() {
  local sql_file="$1" label="$2"
  local b64
  b64="$(base64 < "$sql_file" | tr -d '\n')"
  ssh_node "set -e
    echo $b64 | base64 -d | sudo tee /tmp/provisa-$label.sql >/dev/null
    sudo docker cp /tmp/provisa-$label.sql $DB_CONTAINER:/tmp/provisa-$label.sql >/dev/null
    DSN=\$(sudo docker exec $API_CONTAINER printenv PLATFORM_DATABASE_URL | sed -E 's|^([a-z]+)\+[a-z0-9]+://|\1://|')
    # ORG_ID names the bootstrap org's schema (provisa/api/app.py:216 — 'default' when unset).
    BOOT=\$(sudo docker exec $API_CONTAINER sh -c 'echo \${ORG_ID:-default}')
    sudo docker exec -i $DB_CONTAINER psql \"\$DSN\" -v ON_ERROR_STOP=1 -v boot=\"\$BOOT\" -f /tmp/provisa-$label.sql"
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
  for i in $(seq 1 60); do
    # A refused connection is the expected state while the container restarts, so it is
    # recorded as a probe result instead of tripping `set -e` and aborting the poll. It is
    # never treated as success: the loop still fails loudly below if 200 never arrives.
    code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$SITE/") || code="unreachable"
    [ "$code" = "200" ] && { echo "$SITE/ -> 200 after ${i}s"; return 0; }
    sleep 2
  done
  echo "$SITE/ -> $code (never reached 200)" >&2
  return 1
}

case "$TARGET" in
  ui)
    build_ui; push_ui; verify ;;
  api)
    # reset before restart: the wipe drops the tenant schemas and the org_registry view, and it
    # is the restart that re-seeds the bootstrap org and rebuilds that view.
    build_api; push_api; reset_state; restart; verify; verify_demo ;;
  cfg)
    # Restarts: the config is read once at startup, so a pushed file is inert until then.
    build_cfg; push_cfg; restart; verify ;;
  all)
    build_ui; build_api; build_cfg; push_ui; push_api; push_cfg; reset_state; restart; verify; verify_demo ;;
  reset)
    # No build: 'ui' deliberately has no reset arm because it never restarts.
    reset_state; restart; verify; verify_demo ;;
  patch)
    # verify_demo is skipped, not weakened: it asserts zero accounts, which is a statement
    # about the reset, and 'patch' exists precisely to keep the accounts that are there.
    build_ui; build_api; build_cfg; push_ui; push_api; push_cfg; restart; verify ;;
esac
echo "== deployed $TARGET"
