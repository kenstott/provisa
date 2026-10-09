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
    # succeed. Hand the file to Trino's uid instead of widening the mode.
    sudo chown 1000:1000 sharepoint.pfx
    sudo chmod 600 sharepoint.pfx
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
# Trino aborts with "No service providers of type io.trino.spi.Plugin" without them — a
# failure that surfaces only as an unhealthy container two minutes in. Version pinned to
# the same coordinates tests/conftest.py::_TRINO_PLUGIN_VERSION and build-dmg.yml use; all
# three must move together or a connector behaves differently here than in the shipped
# image. They track the engine release the pgwire bundles pin
# (provisa/runtime_deps/pgwire_bundles.py RELEASE_TAG): trino-splunk before engine-v0.93.0
# rejects the `schema` catalog property TrinoSplunkConnector passes ("Configuration
# property 'schema' was not used"), so the splunk catalog was never created. Source is Maven Central rather than a kenstott/calcite GitHub release: that
# release was deleted mid-run and every download began 404ing, while Central is immutable
# once published and serves anonymously (GitHub Packages 401s even for public artifacts).
# Each jar is shaded and carries its own META-INF/services/io.trino.spi.Plugin, so one jar
# per directory is a complete plugin.
MAVEN="https://repo1.maven.org/maven2/io/simpleishard"
VERSION="0.106.3"
# name:version. trino-file (REQ-1960) is at 0.109.0; trino-salesforce (REQ-1946) and
# trino-cloudops (REQ-1947) are at 0.108.0, the
# first release with what Provisa needs of them; see tests/conftest.py _TRINO_PLUGIN_VERSIONS.
for spec in trino-sharepoint:$VERSION trino-splunk:$VERSION trino-file:0.109.0 \
    trino-salesforce:0.108.0 trino-cloudops:0.108.0; do
  plugin="${spec%%:*}"
  version="${spec##*:}"
  mkdir -p "trino/plugins/$plugin"
  curl -fsSL "$MAVEN/$plugin/$version/$plugin-$version.jar" \
    -o "trino/plugins/$plugin/$plugin-$version.jar"
  ls "trino/plugins/$plugin"/*.jar >/dev/null
done

# REQ-1494: Provisa's own function plugin (provisa_digest, provisa_fake_method), built from
# trino-functions/ in a Maven container; the engine's compose mounts it beside the others.
scripts/build_trino_functions.sh

# Pull before `up` so the compose wait below spends its budget on Trino's JVM boot rather
# than on a 1 GB image download racing the healthcheck's start_period.
docker compose -f docker-compose.core.yml pull postgres trino minio minio-init
if [ "$pull_splunk" = "pull-splunk" ]; then
  # splunk-connector.spec.ts starts this container itself (it needs the core project's
  # network alias), so compose never pulls it. Doing it here keeps the ~2 GB pull off the
  # spec's beforeAll budget, which otherwise has to cover pull + cold boot in one window.
  docker pull splunk/splunk:latest
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
# docker-compose.core.yml's hive_warehouse volume is a `local` driver bind (o: bind), not a
# plain service-level bind mount — unlike a plain bind, it does NOT auto-create a missing
# host directory, it fails the container outright ("no such file or directory"). The
# compose file's own default device path is this repo's maintainer's own machine (matches
# every other *_HOST default in this file, e.g. SP_CERT_PATH), so CI must override it.
mkdir -p "$GITHUB_WORKSPACE/.e2e-hive-warehouse"
echo "PROVISA_E2E_HIVE_WAREHOUSE_HOST=$GITHUB_WORKSPACE/.e2e-hive-warehouse" >> "$GITHUB_ENV"
# This script's own commands need it too (the lane's later steps read it from GITHUB_ENV).
export PROVISA_E2E_HIVE_WAREHOUSE_HOST="$GITHUB_WORKSPACE/.e2e-hive-warehouse"

docker compose -f docker-compose.core.yml up -d --wait \
  postgres trino minio zaychik
docker compose -f docker-compose.core.yml up minio-init
