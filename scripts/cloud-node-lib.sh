#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# The cloud.provisa.dev node, shared by its two deploys: scripts/deploy-cloud.sh (a source
# hot-patch between releases) and scripts/deploy-cloud-release.sh (REQ-1989: a release installed
# from its own artifacts). Sourced, never executed. The caller sets REPO (the checkout) and STAGE
# (a scratch directory it removes on exit) before sourcing, and `set -euo pipefail` itself.
#
# Env overrides: NODE, ZONE, PROJECT, CONTAINERS, SITE, SQL_IMAGE, API_CONTAINER, SANDBOX_NODE.

NODE="${NODE:-provisa-saas-coordinator}"
ZONE="${ZONE:-us-central1-a}"
PROJECT="${PROJECT:-provisa-test-473}"
CONTAINERS="${CONTAINERS:-compose-provisa-1 compose-provisa-ui-1 compose-provisa-sandbox-1}"
SITE="${SITE:-https://cloud.provisa.dev}"
# The control plane lives on Cloud SQL. Nothing on the node holds it and no container is a
# long-lived psql client, so SQL runs in a throwaway container joined to the app's own network —
# which is the network that reaches the private Cloud SQL address.
SQL_IMAGE="${SQL_IMAGE:-postgres:16}"
API_CONTAINER="${API_CONTAINER:-compose-provisa-1}"

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
  env_dump="$(ssh_node "sudo docker inspect $API_CONTAINER --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E '^(PROVISA_ENGINE|PROVISA_ZAYCHIK)' || true" | tr -d '\r')"
  running="$(printf '%s\n' "$env_dump" | sed -n 's/^PROVISA_ENGINE=//p')"
  if [ "$running" != "$ENGINE_PIN" ]; then
    echo "$API_CONTAINER runs PROVISA_ENGINE='${running:-<unset>}', expected '$ENGINE_PIN'." >&2
    echo "Fix the node's /root/.provisa/provisa.env and recreate the containers, then re-run." >&2
    exit 1
  fi
  for k in $ENGINE_KEYS; do
    grep -q "^$k=." <<<"$env_dump" || missing="$missing $k"
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

# REQ-1455/REQ-1514, REQ-1562: on a HOSTED node the plan catalog is the first thing the signup page
# asks for, and the plugin raises rather than inventing a Stripe price id, so a node missing these
# answers GET /billing/catalog with a 500 and "Get started" cannot create an organization. The
# price ids ARE the billing configuration -- there is no default to fall back to -- so the deploy
# asserts them the way it asserts the engine pin, from the RUNNING container. Each plan has two
# prices on its one subscription: its active hour and its data transfer.
COMMERCE_KEYS="STRIPE_PRICE_STARTER STRIPE_PRICE_PRO_S STRIPE_PRICE_PRO_M STRIPE_PRICE_PRO_L \
STRIPE_PRICE_EGRESS_STARTER STRIPE_PRICE_EGRESS_PRO_S \
STRIPE_PRICE_EGRESS_PRO_M STRIPE_PRICE_EGRESS_PRO_L \
STRIPE_WEBHOOK_SECRET"
# REQ-1564: STRIPE_MODE picks the credentials. Every one of them is carried to the node; which pair
# is asserted is the mode's (commerce_mode_keys).
COMMERCE_CREDENTIAL_KEYS="STRIPE_MODE STRIPE_SECRET_KEY STRIPE_PUBLISHABLE_KEY \
STRIPE_TEST_SECRET_KEY STRIPE_TEST_PUBLISHABLE_KEY"

# REQ-1564: the secret and publishable key STRIPE_MODE selects, from a `printenv` dump. The plugin
# reads "test" (any case, surrounding space ignored) as the sandbox and anything else, unset
# included, as live; this reads the mode the same way.
commerce_mode_keys() {
  local mode
  mode="$(printf '%s\n' "$1" | sed -n 's/^STRIPE_MODE=//p' | tr -d ' \r' | tr '[:upper:]' '[:lower:]')"
  if [ "$mode" = "test" ]; then
    echo "STRIPE_TEST_SECRET_KEY STRIPE_TEST_PUBLISHABLE_KEY"
  else
    echo "STRIPE_SECRET_KEY STRIPE_PUBLISHABLE_KEY"
  fi
}

# REQ-1455/REQ-1514 continued: provisa.env is the systemd EnvironmentFile first-launch.sh writes
# ONCE, at first boot (ENV_FILE's own comment) -- but the GCE instance metadata startup-script
# re-runs that ENTIRE first-boot install on every instance START, not just true first boot (this
# node scales to zero on idle and wakes on the next hit), and that startup-script's own exported
# env only covers the engine/auth/mail keys it was written with. Any key added to this deploy
# AFTER a node's first boot -- exactly what the billing settings are -- gets silently regenerated
# away every time the node idle-stops and restarts, so a preflight that only ASSERTS (and tells a
# human to SSH in and fix it) turns into repeat manual surgery on every cold start. Verified live
# 2026-09-17: a value set by hand via set_node_env was gone after the very next idle-stop/restart
# cycle, mid-deploy. Self-heal from the repo .env instead, the same shape push_mail_env already
# uses for PROVISA_EMAIL_API_KEY -- this is the fix, not a workaround: preflight_commerce below
# stays a hard gate for the case this truly can't heal (the value is missing from .env too).
push_commerce_env() {
  echo "== commerce env"
  local env_dump changed=""
  env_dump="$(ssh_node "sudo docker exec $API_CONTAINER printenv | grep -E '^STRIPE_' || true" | tr -d '\r')"
  for k in $COMMERCE_KEYS $COMMERCE_CREDENTIAL_KEYS; do
    grep -Eq "^$k=.*[^[:space:]]" <<<"$env_dump" && continue
    local want
    want="$(sed -n "s/^$k=//p" "$REPO/.env" 2>/dev/null | tail -1)"
    [ -n "$want" ] || continue
    echo "== commerce env: $k -> <set>"
    set_node_env "$k" "$want"
    changed=1
  done
  if [ -n "$changed" ]; then
    echo "== commerce env: recreating stack"
    ssh_node "sudo systemctl restart provisa"
  else
    echo "== commerce env: up to date"
  fi
}

# REQ-1562, REQ-1564: the eight price ids, the webhook secret, and the secret and publishable key
# the mode selects. Run AFTER push_cp_compose: the container's environment is what the compose file
# names, and a node set up before a setting existed forwards nothing until its compose is current.
# Still before any code is pushed, so a node that cannot bill is refused before it changes.
# A blank value is a missing value: compose forwards every declared variable, so an unset key
# reaches the container as "" and the plugin would refuse it on first use. The publishable key is
# asserted with the secret one because the embedded checkout cannot mount without it. Names and
# "<set>" are printed; values never are.
preflight_commerce() {
  echo "== commerce preflight"
  local env_dump missing="" k
  env_dump="$(ssh_node "sudo docker exec $API_CONTAINER printenv | grep -E '^STRIPE_' || true" | tr -d '\r')"
  for k in $COMMERCE_KEYS; do
    grep -Eq "^$k=.*[^[:space:]]" <<<"$env_dump" || missing="$missing $k"
  done
  local mode
  mode="$(printf '%s\n' "$env_dump" | sed -n 's/^STRIPE_MODE=//p' | tr -d ' \r')"
  for k in $(commerce_mode_keys "$env_dump"); do
    grep -Eq "^$k=.*[^[:space:]]" <<<"$env_dump" \
      || missing="$missing $k(STRIPE_MODE=${mode:-<unset>})"
  done
  if [ -n "$missing" ]; then
    echo "$API_CONTAINER is missing commerce settings:$missing" >&2
    echo "Without them /billing/catalog 500s and nobody can sign up." >&2
    echo "Add them to the node's /root/.provisa/provisa.env and recreate the containers." >&2
    exit 1
  fi
  printf '%s\n' "$env_dump" | grep -E '^STRIPE_[A-Z_]*=.*[^[:space:]]' \
    | sed -n 's/^\(STRIPE_[A-Z_]*\)=.*/   \1=<set>/p'
}

# REQ-1276/REQ-1348: the base domain org subdomains live under. The control plane is
# cloud.<base>, so it is $SITE's host without its `cloud.` label: cloud.provisa.dev declares
# provisa.dev. Every process that reads an org from a Host reads it under this value, as
# VITE_ORG_BASE_DOMAIN: the UI's build and its /deployment-config.js, and the API containers
# (provisa/security/sni.py org_from_host). Unset, no host names an org, so a deploy without it
# would leave every org subdomain unaddressable.
org_base_domain() {
  local base="${SITE#https://cloud.}"
  case "$base" in
    ""|*/*|*:*) echo "SITE=$SITE is not https://cloud.<base domain>; cannot declare the base domain" >&2; exit 1 ;;
  esac
  printf '%s\n' "$base"
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
  CONTAINERS="$(node_containers)" push_commercial
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
    # Read from the container's configuration, not a shell in it: the sandbox reset runs it with
    # the tier stopped.
    ENVS=\$(sudo docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' $API_CONTAINER)
    DSN=\$(printf '%s\n' \"\$ENVS\" | sed -n 's/^PLATFORM_DATABASE_URL=//p' | sed -E 's|^([a-z]+)\+[a-z0-9]+://|\1://|')
    [ -n \"\$DSN\" ] || { echo '$API_CONTAINER names no PLATFORM_DATABASE_URL' >&2; exit 1; }
    # ORG_ID names the bootstrap org's schema (provisa/api/app.py:216 — 'default' when unset).
    BOOT=\$(printf '%s\n' \"\$ENVS\" | sed -n 's/^ORG_ID=//p')
    BOOT=\${BOOT:-default}
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
  boot text := current_setting('provisa.boot_org');
BEGIN
  -- The bootstrap org's schema stays: startup re-seeds its registry row unconditionally
  -- (provisa/core/schema_admin.py:236-255), so dropping it only makes it reappear empty.
  -- Its tenant-plane role assignments do go, or the wiped accounts keep their grants.
  EXECUTE format('DELETE FROM %I.user_role_assignments WHERE user_id IS NOT NULL', 'org_' || boot);
END $$;
COMMIT;
-- One schema per statement, each its own transaction (psql autocommit + \gexec), as the sandbox
-- reset does: a sandbox template's schema holds hundreds of views, and dropping every org's schema
-- in one transaction runs out of locks (max_locks_per_transaction). The orgs rows are already gone,
-- so a failure part way leaves only schemas a rerun drops.
SELECT format('DROP SCHEMA IF EXISTS %I CASCADE', nspname)
  FROM pg_namespace
 WHERE nspname LIKE 'org\_%'
   AND nspname <> 'org_' || current_setting('provisa.boot_org')
   AND nspname <> 'org_' || current_setting('provisa.boot_org') || '_mv_cache' \gexec
SQL
  run_sql "$STAGE/reset.sql" reset
}

# Whether this coordinator's sandbox tier runs on a VM of its own. A describe that finds no VM is
# the answer "no", not a failure.
sandbox_on_own_vm() {
  [ "$NODE" != "$SANDBOX_NODE" ] && gcloud compute instances describe "$SANDBOX_NODE" \
    --zone "$ZONE" --project "$PROJECT" --format='value(name)' >/dev/null 2>&1
}

# The app containers on this node. The sandbox tier's container is on its VM when it has one
# (deploy_sandbox_vm pushes to and restarts it there).
node_containers() {
  if sandbox_on_own_vm; then
    tr ' ' '\n' <<<"$CONTAINERS" | grep -vx compose-provisa-sandbox-1 | tr '\n' ' '
  else
    echo "$CONTAINERS"
  fi
}

# The source hot-patch's host tree (deploy-cloud.sh push_overlay) and the compose extension that
# bind-mounts it over the image's /app. A release install removes the extension, so the node runs
# the release image's own code, UI and configs.
# shellcheck disable=SC2034  # read by the scripts that source this library
OVERLAY_DIR="/root/.provisa/overlay"
# shellcheck disable=SC2034  # read by the scripts that source this library
OVERLAY_EXT="/root/.provisa/extensions/overlay/docker-compose.overlay.yml"

# The systemd EnvironmentFile first-launch.sh writes; the containers' environment comes from it.
ENV_FILE="/root/.provisa/provisa.env"

# REQ-1330: the origin invite links are built from. It is not a per-node knob an operator sets — it
# is $SITE, the address this script already deploys to and verifies against, and terraform derives
# the same value from the DNS zone it creates the record in. But first-launch writes provisa.env
# once, at first boot, so a node booted before the build knew the origin keeps an environment with
# no origin in it and every invite fails to send with nothing to link to. Reconciled here for the
# same reason the overlays are: what the build says the deployment is has to reach the running
# container, not just the next one to be created. Read from the RUNNING container (a provisa.env
# edit nothing recreated is exactly the gap) and, like the overlays, recreate only on a difference.
#
# The provider key is the same shape of problem and gets the same treatment. It is not something an
# operator types into Admin -> Email either: this deployment sends through one Resend account, whose
# key lives in the repo .env.
# A node whose key is empty creates invitations it cannot send, which is the failure this closes.
# The key's VALUE is never printed — only whether it changed.
set_node_env() {
  # first-launch omits the line entirely when the value is empty, so deleting and appending both
  # replaces an existing line and adds a missing one. Value passed on stdin, never in the command
  # line, so a secret does not land in the node's process table or the ssh log.
  printf '%s=%s\n' "$1" "$2" \
    | ssh_node "sudo sed -i '/^$1=/d' $ENV_FILE && sudo tee -a $ENV_FILE >/dev/null"
}

# REQ-2194: the engine image and the Zaychik proxy image must match the release being deployed.
# Release-publish.yml tags both at the same version as the git tag, so the checkout's tag is the
# right pin. Without this, a node provisioned by an older terraform apply keeps its old images
# across deploys: the same idle-stop/restart that resets commerce settings would revert hand-edits
# too, so the deploy sets them here, the same way push_commerce_env heals the Stripe keys.
# After the images are pinned in the env file, the main restart in each arm starts the app with
# the new settings; the app's own converge_boot_shard then applies the updated pod manifest to
# GKE, rolling the engine pod to the new images (engine_wake.py:302).
IMAGE_REGISTRY="ghcr.io/simpleishard-llc"

# $1, when given, is the release to pin (deploy-cloud-release.sh passes the tag it installs);
# without it the checkout's nearest tag is the release.
push_engine_env() {
  echo "== engine image env"
  local ver engine_img zaychik_img changed="" env_dump running_engine running_zaychik
  if [ -n "${1:-}" ]; then
    ver="$1"
  # --abbrev=0 gives the nearest tag without a commit-count suffix, so a worktree that has
  # a local-only commit on top of a release tag still pins to that release's images.
  elif ! ver="$(git -C "$REPO" describe --tags --abbrev=0 HEAD 2>/dev/null)"; then
    echo "   no release tag found; engine image pin skipped"
    return 0
  fi
  # Only v0.1.0-alpha.NNN tags are release tags; a dev branch tag could be anything else.
  case "$ver" in
    v[0-9]*) ;;
    *) echo "   HEAD has no release tag ($ver); engine image pin skipped"; return 0 ;;
  esac
  engine_img="$IMAGE_REGISTRY/provisa-trino-engine:$ver"
  zaychik_img="$IMAGE_REGISTRY/provisa-zaychik:$ver"
  env_dump="$(ssh_node "sudo docker inspect $API_CONTAINER --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E '^PROVISA_(ENGINE_IMAGE|ZAYCHIK_IMAGE)=' || true" | tr -d '\r')"
  running_engine="$(printf '%s\n' "$env_dump" | sed -n 's/^PROVISA_ENGINE_IMAGE=//p')"
  running_zaychik="$(printf '%s\n' "$env_dump" | sed -n 's/^PROVISA_ZAYCHIK_IMAGE=//p')"
  if [ "$running_engine" != "$engine_img" ]; then
    echo "   PROVISA_ENGINE_IMAGE: ${running_engine:-<unset>} -> $engine_img"
    set_node_env PROVISA_ENGINE_IMAGE "$engine_img"
    changed=1
  fi
  if [ "$running_zaychik" != "$zaychik_img" ]; then
    echo "   PROVISA_ZAYCHIK_IMAGE: ${running_zaychik:-<unset>} -> $zaychik_img"
    set_node_env PROVISA_ZAYCHIK_IMAGE "$zaychik_img"
    changed=1
  fi
  if [ -n "$changed" ]; then
    echo "== engine image env: set; engine shard will roll on startup"
  else
    echo "== engine image env: up to date ($ver)"
  fi
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

# What a failed poll says about the node: every container's state, and the last log lines of each
# one that is down. The front door answers 503 for any backend that is not answering, which names
# no container; a UI server crash-looping on its environment looked exactly like a slow start.
#
# failing_containers reads one line per container -- name, state, exit code, restart count, health
# ("-" when it has no check) -- and prints the names of those that are not up: not running (an
# init container that exited 0 and was never restarted is done, not down), health-checked
# unhealthy, or restarted by their policy since they were created. A crash loop is "running" at
# the instant between two of its restarts, so the restart count is what gives it away.
failing_containers() {
  awk '{ down = ($2 != "running" && !($2 == "exited" && $3 == 0 && $4 == 0)) || $4 > 0 || $5 == "unhealthy" }
       down { print $1 }'
}

diagnose_containers() {
  local states c
  # The node may be what is down; that is a finding too, not a reason to lose the poll's.
  states="$(ssh_node "sudo docker inspect -f '{{.Name}} {{.State.Status}} {{.State.ExitCode}} {{.RestartCount}} {{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}' \$(sudo docker ps -aq)" \
    | tr -d '\r' | sed 's|^/||')" || states=""
  if [ -z "$states" ]; then
    echo "   $NODE: no container state (the node or its docker did not answer)" >&2
    return
  fi
  echo "   containers on $NODE (name state exit restarts health):" >&2
  # shellcheck disable=SC2001  # indents every line of a multi-line value
  sed 's/^/     /' <<<"$states" >&2
  for c in $(failing_containers <<<"$states"); do
    echo "   $c is down; its last 40 log lines:" >&2
    ssh_node "sudo docker logs --tail 40 $c 2>&1" | sed 's/^/     | /' >&2 || true
  done
}

verify() {
  echo "== verifying $SITE"
  # Accept: text/html with Sec-Fetch-Dest: document marks this a page load, which the front door and
  # ui_server send to the UI; anything else goes to the API, where an unauthenticated GET / is a
  # 401 and says nothing about the deploy (ui_server proxies anything else to
  # the API, where an unauthenticated GET / is a 401 and says nothing about the deploy.
  # 300s. The old 120s budget was shorter than a cold restart on this node, where the API's
  # lifespan spends ~115s in engine-wake alone, so the poll reported 503 on a deploy that then
  # came up seconds later.
  for i in $(seq 1 150); do
    # A refused connection is the expected state while the container restarts, so it is
    # recorded as a probe result instead of tripping `set -e` and aborting the poll. It is
    # never treated as success: the loop still fails loudly below if 200 never arrives.
    code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Accept: text/html' -H 'Sec-Fetch-Dest: document' -H 'Sec-Fetch-Mode: navigate' "$SITE/") || code="unreachable"
    [ "$code" = "200" ] && { echo "$SITE/ -> 200 after $((i * 2))s"; return 0; }
    sleep 2
  done
  echo "$SITE/ -> $code (never reached 200)" >&2
  diagnose_containers
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
  diagnose_containers
  return 1
}

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

# REQ-1276: the API container must carry the base domain after a deploy. Read from the RUNNING
# container, like the engine's keys: that is what org_from_host sees. Without it every host names
# no org, and the deploy would otherwise report success on a cloud nobody can sign in to.
verify_org_base() {
  echo "== org base domain"
  local want have
  want="$(org_base_domain)" || exit 1
  have="$(ssh_node "sudo docker exec $API_CONTAINER printenv VITE_ORG_BASE_DOMAIN || true" | tr -d '\r')"
  if [ "$have" != "$want" ]; then
    echo "$API_CONTAINER has VITE_ORG_BASE_DOMAIN='${have:-<unset>}', expected '$want'." >&2
    echo "The API reads an org from a Host only under it; without it no host names an org." >&2
    exit 1
  fi
  echo "   VITE_ORG_BASE_DOMAIN=$have"
}

# The end-to-end signal for this topology: /health only proves the app started, and the app starts
# whether or not it can reach a shard. A query is what wakes the shard and proves the path from the
# control plane through the GKE provisioner to the Trino shard is live. verify_engine needs no
# account: a release deploy resets the control plane to zero accounts, so no user exists to ask
# the site as. It runs scripts/cloud_engine_probe.py inside the API container instead, which
# resolves, wakes and dials the engine through the app's own code and proves by the coordinator's
# id that the pinned engine answered (see the probe's docstring). One check serves every arm.
verify_engine() {
  echo "== verifying the engine path (cold start may take ~90s)"
  # The probe runs in the API container as python's stdin, sent base64 so its text needs no
  # quoting through the two shells. It prints the engine, shard, coordinator and query id that
  # answered.
  local probe answer
  probe="$(base64 <"$REPO/scripts/cloud_engine_probe.py" | tr -d '\n')"
  answer="$(ssh_node "echo $probe | base64 -d | sudo docker exec -i $API_CONTAINER python - $ENGINE_PIN" | tr -d '\r')" || {
    echo "the engine probe in $API_CONTAINER failed${answer:+: $answer}" >&2
    exit 1
  }
  echo "   $answer"
}

# The sandbox tier's own VM (terraform/gcp-saas/sandbox.tf, var.sandbox_vm), when the deployment
# has one.
SANDBOX_NODE="${SANDBOX_NODE:-provisa-saas-sandbox}"
