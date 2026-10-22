#!/bin/bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# provisa-tls-sync: install the deployment's *.provisa.dev certificate from Secret Manager, the
# one place it is kept (secrets.tf). Every VM of the deployment runs it at each boot (before its
# services start: --boot, nothing to restart) and every hour (a renewal reaches a running VM
# within the hour, restarting what serves the certificate). A certificate already installed is
# left alone. The renewal job (provisa-tls-renew.sh) stores each new one.
#
# Its settings (written by the VM's startup script), from PROVISA_TLS_SYNC_ENV:
#   PROJECT       the project holding the secrets
#   CERT_SECRET   the secret holding the certificate chain (PEM)
#   KEY_SECRET    the secret holding its private key (PEM)
#   TARGETS       where to install it: space-separated "<cert path>:<key path>" pairs
#   RESTART       the command restarting what serves it (run only when it changed, never at boot)
set -euo pipefail

# shellcheck disable=SC1090
. "${PROVISA_TLS_SYNC_ENV:-/etc/provisa-tls/sync.env}"
boot=0
[ "${1:-}" = "--boot" ] && boot=1

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fetch() {
  if ! gcloud secrets versions access latest --secret "$1" --project "$PROJECT" > "$2" 2> "$tmp/err"; then
    echo "provisa-tls-sync: cannot read the latest version of $1: $(cat "$tmp/err")" >&2
    echo "provisa-tls-sync: a deployment's certificate is stored by terraform/gcp/scripts/issue-wildcard-cert.sh or the renewal job" >&2
    exit 1
  fi
}
fetch "$CERT_SECRET" "$tmp/crt"
fetch "$KEY_SECRET" "$tmp/key"

# The two are stored one after the other: a read between a renewal's two writes pairs a new
# certificate with the old key. Never installed so; the next run reads the matching pair.
cert_pub="$(openssl x509 -in "$tmp/crt" -noout -pubkey | openssl sha256)"
key_pub="$(openssl pkey -in "$tmp/key" -pubout | openssl sha256)"
if [ "$cert_pub" != "$key_pub" ]; then
  echo "provisa-tls-sync: the stored certificate and key do not match; nothing installed" >&2
  exit 1
fi

changed=0
for target in $TARGETS; do
  crt="${target%%:*}"
  key="${target#*:}"
  if cmp -s "$tmp/crt" "$crt" && cmp -s "$tmp/key" "$key"; then
    continue
  fi
  install -d -m 755 "$(dirname "$crt")"
  install -d -m 700 "$(dirname "$key")"
  install -m 644 "$tmp/crt" "$crt"
  install -m 600 "$tmp/key" "$key"
  changed=1
done

if [ "$changed" = 0 ]; then
  echo "provisa-tls-sync: certificate current ($(openssl x509 -in "$tmp/crt" -noout -enddate))"
  exit 0
fi
echo "provisa-tls-sync: installed $(openssl x509 -in "$tmp/crt" -noout -enddate)"
if [ "$boot" = 0 ] && [ -n "${RESTART:-}" ]; then
  echo "provisa-tls-sync: $RESTART"
  sh -c "$RESTART"
fi
