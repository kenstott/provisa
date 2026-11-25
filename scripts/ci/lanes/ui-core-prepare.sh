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
# The core UI lane's live credentials, before Playwright (lane.yml `prepare`).
#
# Two of the sources the lane registers authenticate with a FILE: BigQuery and Google Sheets
# with a service-account key, SharePoint with a client certificate. Each arrives as a secret in
# the environment and is written here, readable by this user alone, under the runner's temp
# directory; the lane is told the paths (GOOGLE_APPLICATION_CREDENTIALS, SP_CERT_PATH), which
# are not secrets. Fabric needs no file and no `az login`: the product's own Azure credential
# reads AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID from the environment, so no
# secret is ever an argument of a command.
#
# Never prints a value: no xtrace, no echo of a variable, no environment dump.
set -euo pipefail
set +x
umask 077

# The demo sources publish fixed host ports (33030-39200, demo-source-containers.ts and the specs'
# own) inside Linux's ephemeral range (32768-60999): an outbound connection of a backend held
# 37117 when the MongoDB container was to bind it, and the provisioning job ran no test (run
# 40185713445). The band is reserved before any server starts, so none is handed out.
if [ "$(uname -s)" = Linux ]; then
  sudo sysctl -w net.ipv4.ip_local_reserved_ports=33000-39999
fi

dir="${RUNNER_TEMP:?}/lane-credentials"
mkdir -p "$dir"

if [ -n "${GOOGLE_APPLICATION_CREDENTIALS_JSON:-}" ]; then
  printf '%s' "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > "$dir/gcp-service-account.json"
  echo "GOOGLE_APPLICATION_CREDENTIALS=$dir/gcp-service-account.json" >> "$GITHUB_ENV"
  echo "wrote the Google service-account key"
fi

if [ -n "${SP_CERT_P12_BASE64:-}" ]; then
  printf '%s' "$SP_CERT_P12_BASE64" | base64 -d > "$dir/sharepoint.pfx"
  echo "SP_CERT_PATH=$dir/sharepoint.pfx" >> "$GITHUB_ENV"
  echo "wrote the SharePoint client certificate"
fi

# Maintainer 2026-11-10 ("Paid cloud resources") and 2026-11-21 (per-run workspace): the Fabric
# capacity, a workspace on it and a warehouse in that are this run's own, made here and tagged
# with the run; the warehouse's SQL endpoint and database and the workspace id go to $GITHUB_ENV
# (FABRIC_SQL_SERVER / FABRIC_DATABASE / FABRIC_WORKSPACE_ID) for the lane's run and after steps.
# The after-script deletes them whatever happened. Only the warehouse UI job is passed Fabric's
# names.
if [ -n "${FABRIC_RESOURCE_GROUP:-}" ] && [ -n "${FABRIC_CAPACITY_NAME:-}" ]; then
  ( cd "$GITHUB_WORKSPACE" && uv run python scripts/fabric_e2e.py up "$GITHUB_ENV" )
fi

# REQ-294: a running Provisa never fetches a connector bundle, and an AskAmerica source's server is
# the pgwire-govdata bundle (pgwire_bundles.py CONNECTOR_RELEASE["govdata"]). Staged here, from the
# pinned GitHub release, for the job that runs the AskAmerica specs: the provisioning job, the one
# passed FREE_ASKAMERICA_KEY (ui-e2e-core.yml). Unstaged, the govdata case's source never serves
# (run 45471924124).
if [ -n "${FREE_ASKAMERICA_KEY:-}" ]; then
  ( cd "$GITHUB_WORKSPACE" && uv run --no-sync python -m provisa.runtime_deps.stage_pgwire_bundles --only govdata --github-release )
fi

# REQ-294: the files, Splunk and SharePoint sources the core specs register are served by the
# pgwire-file, pgwire-splunk and pgwire-sharepoint bundles: file-connector*.spec.ts and
# kaggle-source.spec.ts in the shards, source-to-query.spec.ts's files, splunk and sharepoint cases in
# the provisioning job. Staged for every job from the pinned GitHub release, as suite-prepare.sh
# stages file and splunk. Unstaged, each source is refused with BundleNotStaged (release 513 run
# 45676271172).
( cd "$GITHUB_WORKSPACE" && uv run --no-sync python -m provisa.runtime_deps.stage_pgwire_bundles --only file --only splunk --only sharepoint --github-release )
