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
#   1. pull the latest published version and mark it offered (<bundles>/<name>/pulled);
#   2. once the tier has asked for the demo since the node started (<bundles>/<name>/requested:
#      a visitor's open, which builds its template too), bring its stores up
#      (scripts/demo_stores.py): its data loaded into the node's one shared instance of each store
#      type that has one (Postgres, MongoDB, Elasticsearch), and its other services -- a Neo4j
#      store, a Qdrant store, a Kafka broker, a producer -- as its version's own compose project;
#      then write the bundle's stores.env, where each store is and the names of its data;
#   3. mark the version ready (<bundles>/<name>/ready), which the tier then serves; when its stores
#      cannot be brought up, say why in <bundles>/<name>/failed instead, which the tier answers a
#      waiting visitor with (and clears when it asks for the demo again);
#   4. take down every older version nothing reads (<bundles>/<name>/in-use): its data dropped
#      from the shared stores, its own services removed with the images no other container uses,
#      its bundle directory deleted.
# Nothing is started at boot: a demo's stores come up the first time it is asked for, and stay
# up while it is used. When its last sandbox closes, the tier rewrites <bundles>/<name>/requested
# as "until <epoch seconds>" (sandbox.demo_stores_linger after the close); an open before then
# marks it requested again. Once that time has passed, its stores are taken down as an older
# version's are in step 4 (its data dropped from the shared stores, its own services removed) but
# with its images and bundle kept, and requested and ready cleared, so its next open asks for it
# again and it comes back without a pull. Run with
# --boot (its own unit, before docker starts), it clears what the node's last run left: no demo is
# asked for or ready, since none of its stores is running.
# Run at boot, on a timer, and whenever the tier touches <bundles>/.wanted (systemd units in
# packaging/linux/demo-stores/), and again at once when the tier touches it during a run.
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

linger_out() { # name
  # Its last sandbox closed and none opened since: its stores are taken down, its bundle kept.
  local name="$1" ready
  ready=""
  if [ -f "$DEMO_BUNDLES_DIR/$name/ready" ]; then
    ready="$(cat "$DEMO_BUNDLES_DIR/$name/ready")"
  fi
  rm -f "$DEMO_BUNDLES_DIR/$name/ready"
  if [ -n "$ready" ] && [ -f "$DEMO_BUNDLES_DIR/$name/$ready/stores.env" ]; then
    # Its images are kept: the demo is brought back at its next open, not superseded.
    python3 "$DEMO_STORES" stop "$name" "$ready"
    rm -f "$DEMO_BUNDLES_DIR/$name/$ready/stores.env"
  fi
  rm -f "$DEMO_BUNDLES_DIR/$name/requested" "$DEMO_BUNDLES_DIR/$name/failed"
  echo "demo $name: no sandbox open since it lingered; its stores taken down"
}

sync_demo() { # name
  local name="$1" pulled version dir keep old asked errors
  mkdir -p "$DEMO_BUNDLES_DIR/$name"
  pulled="$($DEMO_PULL pull "$name" --into "$DEMO_PULL_INTO")"
  version="$(basename "$pulled")"
  dir="$DEMO_BUNDLES_DIR/$name/$version"
  [ -d "$dir" ] || { echo "demo $name: $pulled is not $dir on this host" >&2; return 1; }
  # Offered from here; its stores are started only once it is asked for.
  write_atomically "$DEMO_BUNDLES_DIR/$name/pulled" "$version"
  if [ ! -f "$DEMO_BUNDLES_DIR/$name/requested" ]; then
    echo "demo $name: $version pulled; not asked for since the node started"
    return 0
  fi
  asked="$(cat "$DEMO_BUNDLES_DIR/$name/requested")"
  case "$asked" in
    "until "*)
      if [ "$(date +%s)" -ge "${asked#until }" ]; then
        linger_out "$name"
        return 0
      fi
      ;;
  esac
  if [ -f "$dir/docker-compose.run.yml" ]; then
    # Its stores not brought up: why, for the tier to answer the visitor waiting on them with,
    # never a demo left starting with nothing said.
    errors="$DEMO_BUNDLES_DIR/$name/.up-errors"
    if ! python3 "$DEMO_STORES" up "$name" "$version" "$dir" 2>"$errors"; then
      cat "$errors" >&2
      write_atomically "$DEMO_BUNDLES_DIR/$name/failed" "$(tail -c 4000 "$errors")"
      rm -f "$errors"
      echo "demo $name: $version could not be brought up" >&2
      return 1
    fi
    cat "$errors" >&2
    rm -f "$errors"
  fi
  rm -f "$DEMO_BUNDLES_DIR/$name/failed"
  write_atomically "$DEMO_BUNDLES_DIR/$name/ready" "$version"
  echo "demo $name: $version ready"
  keep=" $version "
  if [ -f "$DEMO_BUNDLES_DIR/$name/in-use" ]; then
    keep="$keep$(tr '\n' ' ' < "$DEMO_BUNDLES_DIR/$name/in-use") "
  fi
  # Every older version nothing reads: where this node brought its stores up (its stores.env), its
  # data is dropped from the shared stores and its own services taken down with the images no
  # other container uses; then its bundle directory is deleted, whether or not it was ever up.
  for old in "$DEMO_BUNDLES_DIR/$name"/*/; do
    [ -d "$old" ] || continue
    old="$(basename "$old")"
    case "$keep" in
      *" $old "*) continue ;;
    esac
    [[ "$old" < "$version" ]] || continue
    if [ -f "$DEMO_BUNDLES_DIR/$name/$old/stores.env" ]; then
      python3 "$DEMO_STORES" drop "$name" "$old"
      rm -f "$DEMO_BUNDLES_DIR/$name/$old/stores.env"
      echo "demo $name: $old taken down"
    fi
    rm -rf "${DEMO_BUNDLES_DIR:?}/$name/$old"
    echo "demo $name: $old deleted"
  done
}

if [ "${1:-}" = "--boot" ]; then
  : "${DEMO_BUNDLES_DIR:?the bundles directory}"
  # A version an earlier release pinned (<name>/wanted, no longer written) goes too: the latest
  # published is always the one pulled.
  rm -f "$DEMO_BUNDLES_DIR"/*/requested "$DEMO_BUNDLES_DIR"/*/ready "$DEMO_BUNDLES_DIR"/*/wanted \
    "$DEMO_BUNDLES_DIR"/*/failed
  echo "demo stores: no demo asked for or ready since the node started"
  exit 0
fi

failed=0
IFS=',' read -r -a demos <<< "$PROVISA_DEMO_BUNDLES"
# The tier touches <bundles>/.wanted after each ask. One made while a run is going starts no
# other (systemd drops a path unit's trigger while its service runs), so a run takes the signal
# as it starts and goes again while the tier has touched it since: the ask is served now, not at
# the timer's next tick.
signal="$DEMO_BUNDLES_DIR/.wanted"
while :; do
  rm -f "$signal"
  for name in "${demos[@]}"; do
    name="$(echo "$name" | tr -d '[:space:]')"
    [ -n "$name" ] || continue
    # One demo that cannot be synced does not keep the others from theirs; the run still fails.
    # Not `( ... ) || ...`: bash ignores errexit inside a command on the left of ||.
    set +e
    ( set -e; sync_demo "$name" )
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
      echo "demo $name: not synced" >&2
      failed=1
    fi
  done
  [ -e "$signal" ] || break
  echo "demo stores: asked for again during the run; syncing again"
done
exit "$failed"
