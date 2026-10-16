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
#   1. pull the version the tier wants (<bundles>/<name>/wanted), else the latest published;
#   2. bring up the bundle's own stores (its docker-compose.run.yml), each a compose project of
#      its own, provisa-demo-<name>-<version>, shared by every sandbox of that version and
#      read-only to them (the bundle grants Provisa a reader role);
#   3. write the bundle's stores.env -- where each store is, by the environment variables its
#      config.yaml reads (a service's labels name them: com.provisa.demo.host_env,
#      com.provisa.demo.port_env, com.provisa.demo.port);
#   4. mark the version ready (<bundles>/<name>/ready), which the tier then serves;
#   5. take down the stores of every other version nothing reads (<bundles>/<name>/in-use).
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
#   DEMO_STORES_NETWORK    the docker network the tier is on: each store joins it and is named by
#                          its container and container port. Empty: by 127.0.0.1 and its published
#                          port (a tier on this host, as the test instance runs it).

set -euo pipefail

: "${PROVISA_DEMO_BUNDLES:?the demos to serve}"
: "${DEMO_BUNDLES_DIR:?the bundles directory}"
: "${DEMO_PULL:?the bundle pull command}"
: "${DEMO_PULL_INTO:?the bundles directory as the pull command sees it}"
NETWORK="${DEMO_STORES_NETWORK:-}"

write_atomically() { # path text
  local tmp
  tmp="$(dirname "$1")/.$(basename "$1").writing"
  printf '%s\n' "$2" > "$tmp"
  mv -f "$tmp" "$1"
}

stores_env() { # project compose_file
  local project="$1" file="$2"
  docker compose -p "$project" -f "$file" config --format json | python3 -c '
import json, subprocess, sys
project, file, network = sys.argv[1:4]
config = json.load(sys.stdin)
for name, service in sorted(config["services"].items()):
    labels = service.get("labels") or {}
    host_env, port_env = labels.get("com.provisa.demo.host_env"), labels.get("com.provisa.demo.port_env")
    if not host_env:
        continue
    port = labels["com.provisa.demo.port"]
    cid = subprocess.run(["docker", "compose", "-p", project, "-f", file, "ps", "-q", name],
                         check=True, capture_output=True, text=True).stdout.strip()
    if not cid:
        sys.exit(f"{project}: service {name} is not running")
    if network:
        host = subprocess.run(["docker", "inspect", "-f", "{{.Name}}", cid], check=True,
                              capture_output=True, text=True).stdout.strip().lstrip("/")
    else:
        published = subprocess.run(["docker", "compose", "-p", project, "-f", file, "port", name, port],
                                   check=True, capture_output=True, text=True).stdout.strip()
        host, port = "127.0.0.1", published.rsplit(":", 1)[1]
    print(f"{host_env}={host}")
    if port_env:
        print(f"{port_env}={port}")
' "$project" "$file" "$NETWORK"
}

join_network() { # project compose_file
  [ -n "$NETWORK" ] || return 0
  local cid
  for cid in $(docker compose -p "$1" -f "$2" ps -q); do
    if ! docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$cid" \
        | tr ' ' '\n' | grep -qx "$NETWORK"; then
      docker network connect "$NETWORK" "$cid"
    fi
  done
}

sync_demo() { # name
  local name="$1" spec="$1" wanted pulled version dir project keep running
  mkdir -p "$DEMO_BUNDLES_DIR/$name"
  if [ -s "$DEMO_BUNDLES_DIR/$name/wanted" ]; then
    wanted="$(tr -d '[:space:]' < "$DEMO_BUNDLES_DIR/$name/wanted")"
    spec="$name@$wanted"
  fi
  pulled="$($DEMO_PULL pull "$spec" --into "$DEMO_PULL_INTO")"
  version="$(basename "$pulled")"
  dir="$DEMO_BUNDLES_DIR/$name/$version"
  [ -d "$dir" ] || { echo "demo $name: $pulled is not $dir on this host" >&2; return 1; }
  project="provisa-demo-$name-$version"
  if [ -f "$dir/docker-compose.run.yml" ]; then
    # Each version's stores publish no fixed port (several versions run at once): the port
    # variables its services name are left empty, which the run file's ${VAR-default} turns into
    # a port docker picks.
    local blank
    for blank in $(docker compose -p "$project" -f "$dir/docker-compose.run.yml" config --format json \
        | python3 -c 'import json,sys; print("\n".join(s["labels"]["com.provisa.demo.port_env"] for s in json.load(sys.stdin)["services"].values() if (s.get("labels") or {}).get("com.provisa.demo.port_env")))'); do
      export "$blank="
    done
    if ! docker compose -p "$project" -f "$dir/docker-compose.run.yml" up -d --wait; then
      # A store that never turns healthy is one whose first start was cut off before its init
      # wrote its marker (db/init-complete.sh, es-restore.sh): it would never load the rest, as
      # init runs only on an empty volume. The data is the bundle's, read only, so the version's
      # volumes are recreated and its stores started once more.
      echo "demo $name: $version stores not healthy; recreating their volumes" >&2
      docker compose -p "$project" -f "$dir/docker-compose.run.yml" down -v --remove-orphans
      docker compose -p "$project" -f "$dir/docker-compose.run.yml" up -d --wait
    fi
    join_network "$project" "$dir/docker-compose.run.yml"
    write_atomically "$dir/stores.env" "$(stores_env "$project" "$dir/docker-compose.run.yml")"
  fi
  write_atomically "$DEMO_BUNDLES_DIR/$name/ready" "$version"
  echo "demo $name: $version ready"
  [ -f "$dir/docker-compose.run.yml" ] || return 0  # a demo of no stores has none to take down
  keep=" $version "
  if [ -f "$DEMO_BUNDLES_DIR/$name/in-use" ]; then
    keep="$keep$(tr '\n' ' ' < "$DEMO_BUNDLES_DIR/$name/in-use") "
  fi
  for running in $(docker compose ls -a --format json \
      | python3 -c 'import json,sys; print("\n".join(p["Name"] for p in json.load(sys.stdin)))' \
      | grep "^provisa-demo-$name-" || true); do
    case "$keep" in
      *" ${running#provisa-demo-"$name"-} "*) ;;
      *) docker compose -p "$running" down -v --remove-orphans
         echo "demo $name: ${running#provisa-demo-"$name"-} taken down" ;;
    esac
  done
}

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
