#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# The Trino-backed UI lanes' stack (.github/workflows/lane.yml `prepare`): the certificate
# Trino's SharePoint catalog mounts, the connector plugins, Provisa's function plugin, the images
# and the core compose services. One copy for the Trino lane and the swap lane, which carried it
# twice.
#
#   trino-stack-prepare.sh sharepoint-cert   the certificate from SP_CERT_P12_BASE64 (required)
#   trino-stack-prepare.sh placeholder-cert  an empty file: no catalog of the lane reads it
#   ... [pull-splunk]                        also pull the image splunk-connector.spec.ts starts
set -euo pipefail

cert="${1:?sharepoint-cert | placeholder-cert}"
pull_splunk="${2:-}"

# A source the engine-swap harness hands to Trino is addressed host.docker.internal (Trino's
# container reaches the host's published ports by that name; docker-compose.e2e-mounts.yml gives it
# host-gateway). Provisa runs natively and reads the same source row -- its direct pool, its
# replica builds -- so the name must resolve on the runner too, as Docker Desktop resolves it on a
# developer's machine. Without it the exasol reboot case's replica never built: "Could not resolve
# IP address of hostname [host.docker.internal]" (swap lane, main 87d09e9c5).
grep -q '[[:space:]]host\.docker\.internal$' /etc/hosts \
  || echo "127.0.0.1 host.docker.internal" | sudo tee -a /etc/hosts >/dev/null
getent hosts host.docker.internal

# The demo sources these lanes provision publish fixed host ports (the specs' own 33xxx-369xx)
# inside Linux's ephemeral range (32768-60999): an outbound connection held 36920 when the druid
# coordinator was to bind it, and the druid source-to-query spec failed both attempts (run
# 44446383071). The band is reserved before any server starts, so none is handed out -- the same
# reservation ui-core-prepare.sh makes for the core lane.
if [ "$(uname -s)" = Linux ]; then
  sudo sysctl -w net.ipv4.ip_local_reserved_ports=33000-39999
fi

# A skip caused by the wrong arch would be a green run that executed nothing.
arch="$(uname -m)"
echo "runner arch: $arch"
[ "$arch" = "x86_64" ] || { echo "::error::ui-e2e lane running on $arch"; exit 1; }

case "$cert" in
  sharepoint-cert)
    # docker-compose.core.yml bind-mounts ./sharepoint.pfx into Trino, and sharepoint-connector
    # .spec.ts registers a certificate-authenticated SharePoint source that Trino authenticates
    # with. The cert is gitignored, so it arrives as a base64 secret; an empty placeholder file
    # would let the mount succeed and then fail the catalog handshake instead.
    if [ -z "${SP_CERT_P12_BASE64:-}" ]; then
      echo "SP_CERT_P12_BASE64 is not set; the sharepoint catalog cannot authenticate." >&2
      exit 1
    fi
    printf '%s' "$SP_CERT_P12_BASE64" | base64 -d > sharepoint.pfx
    # The runner writes this file as uid 1001; Trino reads it as uid 1000 through the bind
    # mount, and a Linux bind mount carries the host's ownership through unchanged. At mode
    # 600 the plugin could not open the file at all, and Calcite reported only "Error
    # instantiating JsonCustomSchema(name=sharepoint)" — the schema list for the catalog
    # then failed forever and the spec's wait for the `sharepoint` schema could never
    # succeed. Hand the file to Trino's uid instead of widening the mode to everyone.
    # Provisa itself reads it too: the source's pgwire server runs on the runner, as the runner's
    # uid, for the source's writes and declared keys (REQ-1946, REQ-2170), and at mode 600 it died
    # with the same "Error instantiating JsonCustomSchema(name=e2e_sharepoint)" (run 42409743565).
    # The lane runs under Trino's group 1000 (lane.yml "Join Trino's group", REQ-2073), so the
    # group bit, and only it, lets that server read the file.
    sudo chown 1000:1000 sharepoint.pfx
    sudo chmod 640 sharepoint.pfx
    ls -l sharepoint.pfx
    ;;
  placeholder-cert)
    # docker-compose.core.yml bind-mounts ./sharepoint.pfx into Trino; Docker creates a DIRECTORY
    # in place of a missing bind-mount source. No catalog in the swap lane reads the file.
    touch sharepoint.pfx
    ;;
  *) echo "unknown certificate mode: $cert" >&2; exit 2 ;;
esac

# The Calcite-derived connector plugins are gitignored (they exist only where built), and
# Trino aborts with "No service providers of type io.trino.spi.Plugin" without them -- a failure
# that surfaces only as an unhealthy container two minutes in. This script fetches none: the
# caller has restored the pinned jars (tests/trino_plugins.py) from the cache one job per workflow
# run primed from Maven Central (.github/actions/trino-plugins). A lane that fetched its own went
# to Central once per job and run, beside every other lane. Here they are only checked, by name,
# before the stack mounts them.
python3 tests/trino_plugins.py verify

# REQ-1494: Provisa's own function plugin (provisa_digest, provisa_fake_method), which the engine's
# compose mounts beside the others. The caller restored it with the jars above: the priming job
# built it once from trino-functions/ (.github/actions/trino-plugins). Built here, every lane ran
# Maven against Maven Central at once (run 44216356740: HTTP 429).
functions=trino/plugins/provisa-functions/provisa-functions-1.jar
[ -s "$functions" ] || { echo "$functions is missing: restore it with .github/actions/trino-plugins" >&2; exit 1; }

# Pull before `up` so the compose wait below spends its budget on Trino's JVM boot rather
# than on a 1 GB image download racing the healthcheck's start_period.  Retry on transient
# registry errors (unexpected EOF, short read, connection reset) up to 3 times.
_pull_with_retry() {
  local max=3 attempt=0 out
  while true; do
    attempt=$((attempt + 1))
    out=$("$@" 2>&1) && return 0
    echo "[compose pull] attempt ${attempt}/${max} failed:"
    echo "${out}"
    if [[ "${out}" != *"unexpected EOF"* && "${out}" != *"short read"* && \
          "${out}" != *"connection reset"* && "${out}" != *"i/o timeout"* ]]; then
      echo "[compose pull] non-transient error — not retrying"
      return 1
    fi
    if [[ "${attempt}" -ge "${max}" ]]; then
      echo "[compose pull] failed after ${max} attempts"
      return 1
    fi
    sleep 5
  done
}

_pull_with_retry docker compose -f docker-compose.core.yml pull postgres trino minio minio-init
if [ "$pull_splunk" = "pull-splunk" ]; then
  # splunk-connector.spec.ts starts this container itself (it needs the core project's
  # network alias), so compose never pulls it. Doing it here keeps the ~2 GB pull off the
  # spec's beforeAll budget, which otherwise has to cover pull + cold boot in one window.
  _pull_with_retry docker pull splunk/splunk:latest
fi

# zaychik is built, not pulled (build: ./zaychik, image: zaychik:latest).
docker compose -f docker-compose.core.yml build zaychik

# trino-worker stays at replicas: 0 — the coordinator alone answers this lane's queries and
# a second JVM would compete for RAM with Trino's 7g limit, the Vite dev server's 4 GB heap
# and two uvicorn backends on a 16 GB runner. minio and zaychik are NOT optional here:
# TrinoBackend.provision_infra -> trino_lifecycle.connect_infra dials Arrow Flight
# (zaychik, 8480) and ensure_results_bucket (minio) with no fallback, so without them the
# app's lifespan aborts with "dial tcp [::1]:8480: connect: connection refused" and
# Playwright reports "Process from config.webServer was not able to start".
# The stack's host mounts (docker-compose.e2e-mounts.yml; no defaults, an unset one fails by
# name): the Hive warehouse, the file lake the specs write under the checkout
# (provisa-ui/e2e/engine-swap-helpers.ts FILE_LAKE_HOST_DIR) and the certificate written above.
# The warehouse is a `local` driver bind, which does not create a missing host directory.
for pair in HIVE_WAREHOUSE:.e2e-hive-warehouse FILE_LAKE:.e2e-file-lake SHAREPOINT_CERT:sharepoint.pfx; do
  var="PROVISA_E2E_${pair%%:*}_HOST"
  value="$GITHUB_WORKSPACE/${pair#*:}"
  # For this script's own commands, and for the lane's later steps.
  export "$var=$value"
  echo "$var=$value" >> "$GITHUB_ENV"
done
mkdir -p "$PROVISA_E2E_HIVE_WAREHOUSE_HOST" "$PROVISA_E2E_FILE_LAKE_HOST"

docker compose -f docker-compose.core.yml -f docker-compose.e2e-mounts.yml up -d --wait \
  postgres trino minio zaychik
docker compose -f docker-compose.core.yml -f docker-compose.e2e-mounts.yml up minio-init
