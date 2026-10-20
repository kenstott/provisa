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
# The node's side of the sandbox tier's demos (REQ-2049). For each demo the tier offers
# (PROVISA_DEMO_BUNDLES), on the node that runs the tier:
#   1. pull the version the tier wants (<bundles>/<name>/wanted), else the latest published, and
#      mark it offered (<bundles>/<name>/pulled);
#   2. once the tier has asked for the demo since the node started (<bundles>/<name>/requested:
#      a visitor's open, which builds its template too), bring its stores up
#      (scripts/demo_stores.py): its data loaded into the node's one shared instance of each store
#      type that has one (Postgres, MongoDB, Elasticsearch), and its other services -- a Neo4j
#      store, a Qdrant store, a Kafka broker, a producer -- as its version's own compose project;
#      then write the bundle's stores.env, where each store is and the names of its data;
#   3. mark the version ready (<bundles>/<name>/ready), which the tier then serves;
#   4. take down every other version nothing reads (<bundles>/<name>/in-use): its data dropped
#      from the shared stores, its own services removed.
# Nothing is started at boot: a demo's stores come up the first time it is asked for, and stay
# up, the shared instances with them, until the node stops. Run with --boot (its own unit, before
# docker starts), it clears what the node's last run left: no demo is asked for or ready, since
# none of its stores is running.
# Run at boot, on a timer, and whenever the tier touches <bundles>/.wanted (systemd units in
# packaging/linux/demo-stores/).
#
# Environment:
#   PROVISA_DEMO_BUNDLES   the demos, comma-separated (as the tier is told)
#   DEMO_BUNDLES_DIR       the bundles directory, as this host sees it
#   DEMO_PULL              the command that pulls a bundle: run as
#                          $DEMO_PULL pull <name>[@<version>] --into $DEMO_PULL_INTO, printing the
#                          bundle's directory (scripts/demo_bundle.py, e.g. inside the tier)
#   DEMO_PULL_INTO         the bundles directory as DEMO_PULL sees it
#   DEMO_STORES            scripts/demo_stores.py, as installed beside this script
#   DEMO_STORES_SHARED     the shared stores' compose file (docker-compose.shared.yml)
#   DEMO_STORES_PROJECT    the shared stores' compose project
#   DEMO_STORES_KAFKA_PORT the port the shared Kafka listens, is published and advertised at
#   DEMO_STORES_NETWORK    the docker network the tier is on: each store joins it and is named by
#                          its container and container port. Empty: by 127.0.0.1 and its published
#                          port (a tier on this host, as the test instance runs it).

set -euo pipefail

: "${PROVISA_DEMO_BUNDLES:?the demos to serve}"
: "${DEMO_BUNDLES_DIR:?the bundles directory}"
: "${DEMO_PULL:?the bundle pull command}"
: "${DEMO_PULL_INTO:?the bundles directory as the pull command sees it}"
: "${DEMO_STORES:?scripts/demo_stores.py}"
: "${DEMO_STORES_SHARED:?the shared stores compose file}"
: "${DEMO_STORES_PROJECT:?the shared stores compose project}"
: "${DEMO_STORES_KAFKA_PORT:?the port of the shared Kafka}"
export DEMO_BUNDLES_DIR DEMO_STORES_SHARED DEMO_STORES_PROJECT DEMO_STORES_KAFKA_PORT
export DEMO_STORES_NETWORK="${DEMO_STORES_NETWORK:-}"

write_atomically() { # path text
  local tmp
  tmp="$(dirname "$1")/.$(basename "$1").writing"
  printf '%s\n' "$2" > "$tmp"
  mv -f "$tmp" "$1"
}

sync_demo() { # name
  local name="$1" spec="$1" wanted pulled version dir keep old
  mkdir -p "$DEMO_BUNDLES_DIR/$name"
  if [ -s "$DEMO_BUNDLES_DIR/$name/wanted" ]; then
    wanted="$(tr -d '[:space:]' < "$DEMO_BUNDLES_DIR/$name/wanted")"
    spec="$name@$wanted"
  fi
  pulled="$($DEMO_PULL pull "$spec" --into "$DEMO_PULL_INTO")"
  version="$(basename "$pulled")"
  dir="$DEMO_BUNDLES_DIR/$name/$version"
  [ -d "$dir" ] || { echo "demo $name: $pulled is not $dir on this host" >&2; return 1; }
  # Offered from here; its stores are started only once it is asked for.
  write_atomically "$DEMO_BUNDLES_DIR/$name/pulled" "$version"
  if [ ! -f "$DEMO_BUNDLES_DIR/$name/requested" ]; then
    echo "demo $name: $version pulled; not asked for since the node started"
    return 0
  fi
  if [ -f "$dir/docker-compose.run.yml" ]; then
    python3 "$DEMO_STORES" up "$name" "$version" "$dir"
  fi
  write_atomically "$DEMO_BUNDLES_DIR/$name/ready" "$version"
  echo "demo $name: $version ready"
  keep=" $version "
  if [ -f "$DEMO_BUNDLES_DIR/$name/in-use" ]; then
    keep="$keep$(tr '\n' ' ' < "$DEMO_BUNDLES_DIR/$name/in-use") "
  fi
  # Every other version whose stores this node brought up (its stores.env) and nothing reads: its
  # data is dropped from the shared stores and its own services taken down.
  for old in "$DEMO_BUNDLES_DIR/$name"/*/stores.env; do
    [ -f "$old" ] || continue
    old="$(basename "$(dirname "$old")")"
    case "$keep" in
      *" $old "*) ;;
      *) python3 "$DEMO_STORES" drop "$name" "$old"
         rm -f "$DEMO_BUNDLES_DIR/$name/$old/stores.env"
         echo "demo $name: $old taken down" ;;
    esac
  done
}

if [ "${1:-}" = "--boot" ]; then
  : "${DEMO_BUNDLES_DIR:?the bundles directory}"
  rm -f "$DEMO_BUNDLES_DIR"/*/requested "$DEMO_BUNDLES_DIR"/*/ready
  echo "demo stores: no demo asked for or ready since the node started"
  exit 0
fi

failed=0
IFS=',' read -r -a demos <<< "$PROVISA_DEMO_BUNDLES"
for name in "${demos[@]}"; do
  name="$(echo "$name" | tr -d '[:space:]')"
  [ -n "$name" ] || continue
  # One demo that cannot be synced does not keep the others from theirs; the run still fails. Not
  # `( ... ) || ...`: bash ignores errexit inside a command on the left of ||.
  set +e
  ( set -e; sync_demo "$name" )
  status=$?
  set -e
  if [ "$status" -ne 0 ]; then
    echo "demo $name: not synced" >&2
    failed=1
  fi
done
exit "$failed"
