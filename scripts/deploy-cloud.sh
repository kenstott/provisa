#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# Compile local source and push it straight into the running cloud.provisa.dev
# containers, skipping the image rebuild + terraform re-provision cycle.
#
# Both compose-provisa-1 (API) and compose-provisa-ui-1 (UI proxy) run the full
# application image and each serve their own copy of /app/static and /app/provisa,
# so every target is patched in both or the two disagree about what is deployed.
#
#   scripts/deploy-cloud.sh ui     # vite build -> /app/static (no restart needed)
#   scripts/deploy-cloud.sh api    # python package -> /app/provisa (restarts both)
#   scripts/deploy-cloud.sh all    # both, one restart at the end
#
# Env overrides: NODE, ZONE, PROJECT, CONTAINERS, SITE.
set -euo pipefail

NODE="${NODE:-provisa-saas-coordinator}"
ZONE="${ZONE:-us-central1-a}"
PROJECT="${PROJECT:-provisa-test-473}"
CONTAINERS="${CONTAINERS:-compose-provisa-1 compose-provisa-ui-1}"
SITE="${SITE:-https://cloud.provisa.dev}"

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

TARGET="${1:-}"
case "$TARGET" in
  ui|api|all) ;;
  *) echo "usage: $(basename "$0") {ui|api|all}" >&2; exit 2 ;;
esac

ssh_node() { gcloud compute ssh "$NODE" --zone "$ZONE" --project "$PROJECT" --command "$1"; }
scp_node() { gcloud compute scp "$1" "$NODE:$2" --zone "$ZONE" --project "$PROJECT"; }

build_ui() {
  echo "== building UI"
  (cd "$REPO/provisa-ui" && npm run build)
  # index.html carries the hashed bundle names, so it ships with the assets it references.
  tar czf "$STAGE/ui.tgz" -C "$REPO/provisa-ui/dist" assets index.html
}

push_ui() {
  echo "== pushing UI ($(du -h "$STAGE/ui.tgz" | cut -f1))"
  scp_node "$STAGE/ui.tgz" /tmp/provisa-ui-deploy.tgz
  for c in $CONTAINERS; do
    # The old assets are removed first: filenames are content-hashed, so extracting
    # over them accumulates every bundle ever deployed and hides which one is live.
    ssh_node "sudo docker exec $c sh -c 'rm -rf /app/static/assets' \
      && sudo docker cp /tmp/provisa-ui-deploy.tgz $c:/app/static/ui.tgz \
      && sudo docker exec $c sh -c 'cd /app/static && tar xzf ui.tgz && rm ui.tgz && echo \"$c assets=\$(ls assets | wc -l)\"'"
  done
}

build_api() {
  echo "== packaging python package"
  tar czf "$STAGE/api.tgz" -C "$REPO" --exclude='__pycache__' --exclude='*.pyc' provisa
}

push_api() {
  echo "== pushing python package ($(du -h "$STAGE/api.tgz" | cut -f1))"
  scp_node "$STAGE/api.tgz" /tmp/provisa-api-deploy.tgz
  for c in $CONTAINERS; do
    # Extracted over /app/provisa rather than replacing it: the tree is the import root
    # of the running interpreter, and a deleted-then-recreated directory races the restart.
    ssh_node "sudo docker cp /tmp/provisa-api-deploy.tgz $c:/app/api.tgz \
      && sudo docker exec $c sh -c 'cd /app && tar xzf api.tgz && rm api.tgz && echo \"$c provisa=\$(ls provisa | wc -l)\"'"
  done
}

restart() {
  echo "== restarting"
  ssh_node "sudo docker restart $CONTAINERS"
}

verify() {
  echo "== verifying $SITE"
  # Accept: text/html marks this a document request; ui_server proxies anything else to
  # the API, where an unauthenticated GET / is a 401 and says nothing about the deploy.
  for i in $(seq 1 60); do
    # A refused connection is the expected state while the container restarts, so it is
    # recorded as a probe result instead of tripping `set -e` and aborting the poll. It is
    # never treated as success: the loop still fails loudly below if 200 never arrives.
    code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$SITE/") || code="unreachable"
    [ "$code" = "200" ] && { echo "$SITE/ -> 200 after ${i}s"; return 0; }
    sleep 2
  done
  echo "$SITE/ -> $code (never reached 200)" >&2
  return 1
}

case "$TARGET" in
  ui)
    build_ui; push_ui; verify ;;
  api)
    build_api; push_api; restart; verify ;;
  all)
    build_ui; build_api; push_ui; push_api; restart; verify ;;
esac
echo "== deployed $TARGET"
