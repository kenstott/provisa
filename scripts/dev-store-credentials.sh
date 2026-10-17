#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

# Sourced by start-ui.sh and start-ui-install.sh (its Docker path) before the development compose
# stack starts. docker-compose.core.yml sets no credential of its own: this machine's are generated
# once and kept outside the checkout, in PROVISA_HOME (mode 600), as scripts/provisa keeps a
# deployment's. The stores publish on loopback, where the host-run backend dials them.
_dev_creds="${PROVISA_HOME:-$HOME/.provisa}/dev-store-credentials.env"
mkdir -p "$(dirname "$_dev_creds")"
[ -f "$_dev_creds" ] || (umask 077 && : > "$_dev_creds")
for _var in PROVISA_PG_PASSWORD PROVISA_REDIS_PASSWORD PROVISA_MINIO_ROOT_PASSWORD \
            PROVISA_GRAFANA_ADMIN_PASSWORD PROVISA_TRINO_SHARED_SECRET; do
  _val="$(sed -n "s/^${_var}=//p" "$_dev_creds" | head -1)"
  if [ -z "$_val" ]; then
    _val="$(openssl rand -hex 24)"
    printf '%s=%s\n' "$_var" "$_val" >> "$_dev_creds"
  fi
  export "${_var}=${_val}"
done
unset _var _val _dev_creds
export PROVISA_STORE_BIND_IP=127.0.0.1
export PROVISA_MINIO_ROOT_USER=provisa
# What the host-run backend signs in with: the bundled stores' accounts.
export CONFIG_DB_PASSWORD="$PROVISA_PG_PASSWORD"
export PG_PASSWORD="$PROVISA_PG_PASSWORD"
export REDIS_URL="redis://:${PROVISA_REDIS_PASSWORD}@localhost:6379"
export PROVISA_OTEL_S3_ACCESS_KEY=provisa
export PROVISA_OTEL_S3_SECRET_KEY="$PROVISA_MINIO_ROOT_PASSWORD"
export PROVISA_REDIRECT_ACCESS_KEY=provisa
export PROVISA_REDIRECT_SECRET_KEY="$PROVISA_MINIO_ROOT_PASSWORD"
