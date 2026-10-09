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
