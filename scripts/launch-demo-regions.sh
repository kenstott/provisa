#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Business Source License 1.1 — see LICENSE.
#
# REQ-1922 two-region demo: two Provisa nodes (eu + us) on the embedded local platform, sharing ONE
# model store, so the region-specific admin UX is visible end to end.
#
# The new local platform makes this light and Docker-free:
#   - ONE embedded Postgres (pgserver) is the shared model store AND both regions' stores (per the
#     REQ-1922 amendment, the two regions share one instance; region-qualified schema/cache names
#     keep them from colliding -- that naming is replica-layout-2's work on regions-3c);
#   - each node runs its own DuckDB engine;
#   - ONE shared fakeredis TCP server is the cache for both.
#
# Instances & isolation (CLAUDE.md): this is its OWN instance, "demo-regions". It NEVER touches
# local-dev -- not its ports (API 8001, MCP 8009, pgwire 5439, vite 5173), not ~/.provisa/demo, not
# its processes -- nor demo-snowflake-live (8200/3200). Its ports and data dir are its own.
#
# PROOF IS BLOCKED until replica-layout-2's region-qualified naming lands on regions-3c: until then
# two regions on one Postgres collide. This script is scaffolded and reviewed; do not run the proof
# (or the --test spec harness) before that message. `--check` validates the layout without launching.
#
# Usage:
#   scripts/launch-demo-regions.sh            # start the demo-regions instance (foreground logs tail)
#   scripts/launch-demo-regions.sh --reset    # wipe its data dir first, then start
#   scripts/launch-demo-regions.sh --stop     # stop everything this instance started
#   scripts/launch-demo-regions.sh --test     # spec-owned mode: unique unexposed ports, temp data
#                                              #   dir, print machine-readable PORTS line, stay up
#   scripts/launch-demo-regions.sh --check     # scaffold self-check (paths/tokens), no launch

set -euo pipefail
cd "$(dirname "$0")/.."
REPO="$(pwd)"
PY="$REPO/.venv/bin/python3"
PROVISA="$PY -m provisa.cli"

MODE="start"
for arg in "$@"; do
  case "$arg" in
    --reset) MODE="reset" ;;
    --stop) MODE="stop" ;;
    --test) MODE="test" ;;
    --check) MODE="check" ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

# --- ports & dirs (distinct from local-dev 8001/8009/5439/5173 and demo-snowflake-live 8200/3200) ---
if [[ "$MODE" == "test" ]]; then
  # Spec-owned: unique unexposed ports (high, random-ish by PID) and a throwaway data dir.
  _base=$(( 19000 + (RANDOM % 1000) * 10 ))
  EU_API=$((_base + 1)); EU_UI=$((_base + 2)); US_API=$((_base + 3)); US_UI=$((_base + 4))
  REDIS_PORT=$((_base + 5))
  INSTANCE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/provisa-demo-regions-test.XXXXXX")"
else
  EU_API=8110; EU_UI=8310; US_API=8120; US_UI=8320; REDIS_PORT=6399
  INSTANCE_DIR="$HOME/.provisa/demo-regions"
fi
PIDS_FILE="$INSTANCE_DIR/pids"
CONFIG_OUT="$INSTANCE_DIR/provisa-regions-demo.yaml"
CSV_DIR="$INSTANCE_DIR/csv"
PG_DIR="$INSTANCE_DIR/shared-pg"

_stop() {
  [[ -f "$PIDS_FILE" ]] || return 0
  while read -r pid; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true; done <"$PIDS_FILE"
  # Stop the shared embedded Postgres by its data dir (never a blanket pg kill -- local-dev has its own).
  "$PY" -m provisa.core.control_plane_pg stop "$PG_DIR/control-pg" 2>/dev/null || true
  rm -f "$PIDS_FILE"
}
trap '[[ "$MODE" == "test" ]] && { _stop; rm -rf "$INSTANCE_DIR"; }' EXIT

if [[ "$MODE" == "stop" ]]; then _stop; echo "demo-regions stopped."; exit 0; fi
if [[ "$MODE" == "reset" ]]; then _stop; rm -rf "$INSTANCE_DIR"; fi

mkdir -p "$INSTANCE_DIR" "$CSV_DIR/intake_eu" "$CSV_DIR/intake_us" "$CSV_DIR/breeds" \
         "$INSTANCE_DIR/eu" "$INSTANCE_DIR/us" "$PG_DIR"
: >"$PIDS_FILE"

# Demo CSVs: a `region` column so each region's rows are visible, under their own globbed subdirs.
printf 'id,region,animal\n1,eu,cat\n2,eu,dog\n' >"$CSV_DIR/intake_eu/a.csv"
printf 'id,region,animal\n1,us,cat\n2,us,dog\n' >"$CSV_DIR/intake_us/a.csv"
printf 'name,species\nTabby,cat\nLab,dog\n' >"$CSV_DIR/breeds/a.csv"

if [[ "$MODE" == "check" ]]; then
  test -f "$REPO/config/provisa-regions-demo.yaml.tmpl" || { echo "template missing" >&2; exit 1; }
  echo "demo-regions scaffold OK: ports eu=$EU_API/$EU_UI us=$US_API/$US_UI redis=$REDIS_PORT dir=$INSTANCE_DIR"
  exit 0
fi

# --- demo credentials: generated once per instance, reused across restarts (--reset regenerates) ---
CREDS_FILE="$INSTANCE_DIR/creds.env"
if [[ "$MODE" == "test" ]]; then
  # Fixed, known creds so the spec can log in (the instance is throwaway and unexposed).
  ADMIN_PW="admin-test-pw"; OPERATOR_PW="operator-test-pw"; RESIDENT_PW="resident-test-pw"
  JWT_SECRET="regions-demo-test-jwt-secret-0001234567"
elif [[ -f "$CREDS_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CREDS_FILE"
else
  _rand() { openssl rand -hex "$1" 2>/dev/null || date +%s%N | shasum | head -c $(( $1 * 2 )); }
  ADMIN_PW="admin-$(_rand 6)"
  OPERATOR_PW="operator-$(_rand 6)"
  RESIDENT_PW="resident-$(_rand 6)"
  JWT_SECRET="$(_rand 24)"
  { echo "ADMIN_PW=$ADMIN_PW"; echo "OPERATOR_PW=$OPERATOR_PW"; echo "RESIDENT_PW=$RESIDENT_PW"; echo "JWT_SECRET=$JWT_SECRET"; } >"$CREDS_FILE"
fi

# --- one shared fakeredis TCP server (cache for both regions) ---
"$PY" - "$REDIS_PORT" <<'PY' &
import sys, fakeredis
srv = fakeredis.TcpFakeServer(("127.0.0.1", int(sys.argv[1])), server_type="redis")
srv.serve_forever()
PY
echo $! >>"$PIDS_FILE"
REDIS_URL="redis://127.0.0.1:$REDIS_PORT/0"

# --- one shared embedded Postgres: the model store AND both regions' pg stores ---
# `control_plane_pg start` prints PG_HOST=/PG_PORT= (a unix-socket host dir + port).
_pgout="$("$PY" -m provisa.core.control_plane_pg start "$PG_DIR/control-pg")"
PGHOST="$(printf '%s\n' "$_pgout" | sed -n 's/^PG_HOST=//p')"
PGPORT="$(printf '%s\n' "$_pgout" | sed -n 's/^PG_PORT=//p')"
PG_URL="postgresql+psycopg://provisa:provisa@/provisa?host=$PGHOST&port=$PGPORT"

# --- render the config template for this launch ---
# Rendered in Python, not sed: the pg URL carries & ? = / which sed's replacement treats specially
# (& inserts the matched token), and that mangled a store URL.
EU_ADDRESS="http://127.0.0.1:$EU_UI" US_ADDRESS="http://127.0.0.1:$US_UI" \
EU_ENGINE_URL="duckdb:///$INSTANCE_DIR/eu/engine.duckdb" \
US_ENGINE_URL="duckdb:///$INSTANCE_DIR/us/engine.duckdb" \
PG_URL="$PG_URL" REDIS_URL="$REDIS_URL" DATA_CSV_DIR="$CSV_DIR" JWT_SECRET="$JWT_SECRET" \
  "$PY" - "$REPO/config/provisa-regions-demo.yaml.tmpl" "$CONFIG_OUT" <<'PY'
import os, sys
src, out = sys.argv[1], sys.argv[2]
text = open(src).read()
for tok in ("EU_ADDRESS","US_ADDRESS","EU_ENGINE_URL","US_ENGINE_URL","PG_URL","REDIS_URL","DATA_CSV_DIR","JWT_SECRET"):
    text = text.replace(f"@@{tok}@@", os.environ[tok])
open(out, "w").write(text)
PY

# --- the UI bundle `provisa run` serves (ui_server.py reads provisa/_ui) ---
# A dev worktree ships no bundle, so `provisa run` would serve "Provisa UI not bundled" and none of
# the region admin UX would render. Build it once into provisa/_ui (gitignored, staged exactly as
# scripts/build-wheel.sh does); subsequent launches reuse it.
_ensure_ui_built() {
  [[ -f "$REPO/provisa/_ui/index.html" ]] && return 0
  echo "demo-regions: building the UI bundle once (provisa/_ui)..." >&2
  ( cd "$REPO/provisa-ui" && npm run build ) >&2
  test -f "$REPO/provisa-ui/dist/index.html" || { echo "UI build produced no dist/index.html" >&2; return 1; }
  mkdir -p "$REPO/provisa/_ui"
  cp -r "$REPO/provisa-ui/dist/." "$REPO/provisa/_ui/"
}
_ensure_ui_built

# --- launch a node per region, BOTH on the one shared model store and shared cache ---
# PLATFORM/TENANT_DATABASE_URL and REDIS_URL are exported so the embedded profile (setdefault) keeps
# them: both nodes use the ONE shared pg + fakeredis instead of each starting its own.
launch_node() {
  local region="$1" api="$2" ui="$3" datadir="$4"
  # `provisa run` has no --config flag; it reads PROVISA_CONFIG. REPLACE=true so our regions config
  # is the whole model, not merged onto a bundled default.
  PROVISA_REGION="$region" \
  PLATFORM_DATABASE_URL="$PG_URL" TENANT_DATABASE_URL="$PG_URL" \
  REDIS_URL="$REDIS_URL" \
  PROVISA_CONFIG="$CONFIG_OUT" PROVISA_CONFIG_REPLACE="true" \
  PROVISA_SUPERUSER_USERNAME="admin" PROVISA_SUPERUSER_PASSWORD="$ADMIN_PW" \
    $PROVISA run --region "$region" --api-port "$api" --ui-port "$ui" \
      --data-dir "$datadir" --no-browser \
      >"$INSTANCE_DIR/$region.log" 2>&1 &
  echo $! >>"$PIDS_FILE"
}
launch_node eu "$EU_API" "$EU_UI" "$INSTANCE_DIR/eu"
launch_node us "$US_API" "$US_UI" "$INSTANCE_DIR/us"

# --- first-launch seed: the break-glass superuser (PROVISA_SUPERUSER_*, control-plane only) opens
# invites; /auth/register redeems them as the data-plane basic users operator (org_admin) and
# resident (eu_resident). A second org_admin, `grantor`, then assigns residency_steward to the
# operator through /admin/users -- a grant by ANOTHER principal, because a user cannot assign a role
# to itself (REQ-1308). The operator ends up org_admin + residency_steward (REQ-1921: org_admin
# alone cannot move data across a border). grantor is a seeding artifact, not a demo login. All
# admin data work runs as operator; the superuser holds no data capabilities. Runs once; --reset
# clears the marker so it re-seeds. ---
_seed_users() {
  local marker="$INSTANCE_DIR/.seeded"
  [[ -f "$marker" ]] && return 0
  for _i in $(seq 1 30); do
    [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:$EU_API/auth/provider-type" 2>/dev/null)" == "200" ]] && break
    sleep 5
  done
  if EU_API="$EU_API" ADMIN_PW="$ADMIN_PW" OPERATOR_PW="$OPERATOR_PW" RESIDENT_PW="$RESIDENT_PW" "$PY" - <<'PY'
import os, json, secrets, urllib.request
api = f"http://127.0.0.1:{os.environ['EU_API']}"
def post(path, body, auth=None):
    h = {"content-type": "application/json"}
    if auth: h["Authorization"] = auth
    req = urllib.request.Request(api + path, data=json.dumps(body).encode(), headers=h)
    return json.load(urllib.request.urlopen(req, timeout=20))
# The superuser session token only opens the invites; each user is created by redeeming one.
su = post("/auth/superuser-login", {"username": "admin", "password": os.environ['ADMIN_PW']})["access_token"]
ids = {}
for username, role, pw_env in (("operator", "org_admin", "OPERATOR_PW"), ("resident", "eu_resident", "RESIDENT_PW")):
    inv = post("/admin/invites/", {"org_id": "default", "role_id": role}, auth="Bearer " + su)
    reg = post("/auth/register", {"username": username, "password": os.environ[pw_env], "invite_token": inv["token"]})
    ids[username] = reg["user_id"]
    print(f"seeded {username} -> {role}")
# A second org_admin assigns residency_steward to the operator -- a grant by another principal
# (REQ-1308 forbids assigning a role to yourself). grantor's password is ephemeral (seeding only).
grantor_pw = secrets.token_hex(16)
ginv = post("/admin/invites/", {"org_id": "default", "role_id": "org_admin"}, auth="Bearer " + su)
post("/auth/register", {"username": "grantor", "password": grantor_pw, "invite_token": ginv["token"]})
gtok = post("/auth/login", {"username": "grantor", "password": grantor_pw})["access_token"]
post(f"/admin/users/{ids['operator']}/assignments", {"role_id": "residency_steward", "domain_id": "*"}, auth="Bearer " + gtok)
print("granted operator -> residency_steward (via grantor, /admin/users)")
PY
  then touch "$marker"; else echo "WARN: user seed failed (see logs); admin/residency demo steps unavailable"; fi
}
_seed_users

if [[ "$MODE" == "test" ]]; then
  # Machine-readable lines the Playwright harness parses, then it waits for readiness itself.
  echo "PORTS eu_ui=$EU_UI eu_api=$EU_API us_ui=$US_UI us_api=$US_API"
  echo "CREDS operator=$OPERATOR_PW resident=$RESIDENT_PW admin=$ADMIN_PW"
  wait  # stay up until the spec kills the process group (teardown trap wipes the temp dir)
else
  echo "demo-regions up:"
  echo "  eu:  http://127.0.0.1:$EU_UI   (API http://127.0.0.1:$EU_API)"
  echo "  us:  http://127.0.0.1:$US_UI   (API http://127.0.0.1:$US_API)"
  echo "  shared model store: embedded Postgres at $PGHOST:$PGPORT ; cache: fakeredis :$REDIS_PORT"
  echo "Logins (basic auth):  operator / $OPERATOR_PW  (org_admin)   resident / $RESIDENT_PW  (eu_resident)"
  echo "Break-glass superuser (control plane): admin / $ADMIN_PW  (/auth/superuser-login)"
  echo "Stop with: scripts/launch-demo-regions.sh --stop"
  wait
fi
