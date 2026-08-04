#!/usr/bin/env bash
# Vendor the Splunk Common Information Model add-on (Splunkbase app 1621) into .splunk-cim/,
# which docker-compose.test.yml bind-mounts onto /opt/splunk/etc/apps/Splunk_SA_CIM.
#
# Why vendor it: splunk/splunk:latest ships only the core apps, so a stock container exposes just
# its two sample data models (internal_audit_logs, internal_server). The CIM add-on's 23 models —
# Authentication, Web, Network_Traffic, Endpoint, … — are what a real Splunk deployment queries,
# and one data model is one Calcite/Trino table (TrinoSplunkConnector, no customTables, so the
# adapter's dynamic-discovery path enumerates GET /services/data/models). Vendoring rather than
# setting SPLUNK_APPS_URL keeps container boot off the network and off Splunkbase credentials;
# the already-420s start_period has no room for a licensed download.
#
# CIM models ship UNACCELERATED, which is correct here: acceleration only backs `tstats`, and the
# Calcite adapter issues `| datamodel <model> <object> search`, which reads the raw events.
#
# Credentials come from .env (SPLUNKBASE_USERNAME / SPLUNKBASE_PASSWORD). A free splunk.com
# account is enough, BUT the add-on's license terms must be accepted once, interactively, at
# https://splunkbase.splunk.com/app/1621 — the API serves a 403 for an account that has not,
# and that click-through cannot be scripted.
set -euo pipefail
cd "$(dirname "$0")/.."

APP_ID=1621
DEST=".splunk-cim/Splunk_SA_CIM"
CACHE="${PROVISA_SPLUNK_CIM_CACHE:-$HOME/.cache/provisa-splunk-cim}"

# Idempotence is keyed on the extracted models the tests actually read, never on the directory or
# the tarball existing. A half-extracted tree or an empty mount point must re-fetch, not report
# success — a marker-path probe that answers "present" for a broken tree turns a real failure into
# a silent skip downstream.
if [ -f "$DEST/default/data/models/Authentication.json" ] \
   && [ -f "$DEST/default/data/models/Web.json" ] \
   && [ -f "$DEST/default/data/models/Network_Traffic.json" ]; then
  echo "Splunk_SA_CIM already vendored at $DEST ($(ls "$DEST/default/data/models"/*.json | wc -l | tr -d ' ') data models)"
  exit 0
fi

# .env is the credential home for every other external-provider lane; read it the same way.
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

VERSION="${SPLUNK_CIM_VERSION:?SPLUNK_CIM_VERSION not set (see .env)}"
: "${SPLUNKBASE_USERNAME:?SPLUNKBASE_USERNAME is empty — create a free splunk.com account, accept the app-1621 terms at https://splunkbase.splunk.com/app/${APP_ID}, then fill it in .env}"
: "${SPLUNKBASE_PASSWORD:?SPLUNKBASE_PASSWORD is empty — see .env}"

mkdir -p "$CACHE"
TARBALL="$CACHE/Splunk_SA_CIM-$VERSION.tgz"

if [ ! -s "$TARBALL" ]; then
  echo "Authenticating to Splunkbase as $SPLUNKBASE_USERNAME"
  # The login endpoint answers XML: <feed …><id>TOKEN</id>… . It returns 403 (not 401) for bad
  # credentials AND for an account that has not accepted the app terms, so the two are
  # indistinguishable here — the message below names both.
  LOGIN=$(curl -sS -m 60 -XPOST https://splunkbase.splunk.com/api/account:login/ \
            --data-urlencode "username=$SPLUNKBASE_USERNAME" \
            --data-urlencode "password=$SPLUNKBASE_PASSWORD")
  TOKEN=$(printf '%s' "$LOGIN" | sed -n 's:.*<id>\(.*\)</id>.*:\1:p' | head -1)
  if [ -z "$TOKEN" ]; then
    echo "FAIL: Splunkbase login returned no token. Check SPLUNKBASE_USERNAME/PASSWORD in .env," >&2
    echo "      and confirm the app-$APP_ID terms were accepted at https://splunkbase.splunk.com/app/$APP_ID" >&2
    echo "Response was: $LOGIN" >&2
    exit 1
  fi

  echo "Downloading Splunk_SA_CIM $VERSION"
  curl -sS -L -m 300 -H "X-Auth-Token: $TOKEN" \
    "https://api.splunkbase.splunk.com/api/v2/apps/$APP_ID/releases/$VERSION/download/?origin=sb" \
    -o "$TARBALL.part"
  # A 403/404 body is served with HTTP 200 after the redirect chain in some cases, so verify the
  # bytes are actually a gzip tarball rather than trusting the exit code.
  if ! tar tzf "$TARBALL.part" >/dev/null 2>&1; then
    echo "FAIL: downloaded file is not a gzip tarball — Splunkbase served an error body:" >&2
    head -c 400 "$TARBALL.part" >&2; echo >&2
    echo "If SPLUNK_CIM_VERSION=$VERSION is wrong, list releases with:" >&2
    echo "  curl -H \"X-Auth-Token: \$TOKEN\" https://api.splunkbase.splunk.com/api/v2/apps/$APP_ID/releases/" >&2
    rm -f "$TARBALL.part"
    exit 1
  fi
  mv "$TARBALL.part" "$TARBALL"
fi

echo "Extracting to $DEST"
rm -rf .splunk-cim
mkdir -p .splunk-cim
# The tarball's single top-level directory is the app name; strip nothing, it already unpacks to
# Splunk_SA_CIM/.
tar xzf "$TARBALL" -C .splunk-cim

MODELS="$DEST/default/data/models"
[ -d "$MODELS" ] || { echo "FAIL: no $MODELS in the extracted add-on — tarball layout changed" >&2; exit 1; }
COUNT=$(ls "$MODELS"/*.json 2>/dev/null | wc -l | tr -d ' ')
[ "$COUNT" -gt 0 ] || { echo "FAIL: $MODELS contains no data models" >&2; exit 1; }

# Splunk writes local/ and metadata/local.meta into an app directory at load, so the bind mount is
# read-write; pre-create them here so the container never has to mkdir into the mount root.
mkdir -p "$DEST/local" "$DEST/metadata"

echo "Vendored Splunk_SA_CIM $VERSION — $COUNT data models at $MODELS"
