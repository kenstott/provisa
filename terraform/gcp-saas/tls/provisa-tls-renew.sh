#!/bin/bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# provisa-tls-renew: once a day on the front door (always on), renew the deployment's wildcard
# certificate when fewer than RENEW_DAYS remain on the stored one: Let's Encrypt over DNS-01 on
# Cloudflare, as terraform/gcp/scripts/issue-wildcard-cert.sh issues it by hand. The new
# certificate and key are stored as the secrets' new versions, then installed here at once
# (provisa-tls-sync); every other VM installs it within the hour, or at its next boot.
#
# Its settings (written by the front door's startup script), from PROVISA_TLS_RENEW_ENV:
#   PROJECT, CERT_SECRET, KEY_SECRET  as provisa-tls-sync's
#   TOKEN_SECRET  the secret holding the Cloudflare token (Zone.DNS:Edit on DOMAIN)
#   ACME_EMAIL    the ACME account's contact
#   DOMAIN        the zone: the certificate is *.DOMAIN with DOMAIN beside it
#   RENEW_DAYS    renew when fewer than this many days remain
#   LEGO_PATH     lego's account and certificate directory
set -euo pipefail

# shellcheck disable=SC1090
. "${PROVISA_TLS_RENEW_ENV:-/etc/provisa-tls/renew.env}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

if ! gcloud secrets versions access latest --secret "$CERT_SECRET" --project "$PROJECT" \
    > "$tmp/current.crt" 2> "$tmp/err"; then
  echo "provisa-tls-renew: cannot read the stored certificate: $(cat "$tmp/err")" >&2
  exit 1
fi
if openssl x509 -in "$tmp/current.crt" -noout -checkend $((RENEW_DAYS * 86400)) >/dev/null; then
  echo "provisa-tls-renew: $(openssl x509 -in "$tmp/current.crt" -noout -enddate); renews within ${RENEW_DAYS} days of it"
  exit 0
fi
echo "provisa-tls-renew: $(openssl x509 -in "$tmp/current.crt" -noout -enddate); renewing"

gcloud secrets versions access latest --secret "$TOKEN_SECRET" --project "$PROJECT" > "$tmp/token"
mkdir -p "$LEGO_PATH"
# The wildcard first, so the files are named after it (_.DOMAIN); the apex as a SAN. Propagation
# is checked against public resolvers, as the manual issuance does.
CLOUDFLARE_DNS_API_TOKEN="$(cat "$tmp/token")" lego run \
  --path "$LEGO_PATH" \
  --accept-tos \
  --email "$ACME_EMAIL" \
  --dns cloudflare \
  --dns.resolvers "1.1.1.1:53" \
  --dns.resolvers "8.8.8.8:53" \
  --domains "*.${DOMAIN}" \
  --domains "${DOMAIN}"

crt="$LEGO_PATH/certificates/_.${DOMAIN}.crt"
key="$LEGO_PATH/certificates/_.${DOMAIN}.key"
if ! openssl x509 -in "$crt" -noout -checkend $((RENEW_DAYS * 86400)) >/dev/null; then
  echo "provisa-tls-renew: lego left no certificate valid beyond ${RENEW_DAYS} days at $crt" >&2
  exit 1
fi
# The key first: until the certificate follows, a reader finds a pair that does not match and
# installs nothing (provisa-tls-sync), never a key without its certificate.
gcloud secrets versions add "$KEY_SECRET" --project "$PROJECT" --data-file "$key" >/dev/null
gcloud secrets versions add "$CERT_SECRET" --project "$PROJECT" --data-file "$crt" >/dev/null
echo "provisa-tls-renew: stored $(openssl x509 -in "$crt" -noout -enddate)"
provisa-tls-sync
