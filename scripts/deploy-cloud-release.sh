#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# Deploy cloud.provisa.dev by installing a release from its own artifacts (REQ-1989, amended
# 2026-11-28): the release's AppImage, core image assets and Trino plugins, as a customer receives
# them, for one tag. Nothing from this checkout's source tree reaches the node; the checkout
# supplies only this script, its library and the private plugin hook (push_plugins).
#
#   scripts/deploy-cloud-release.sh <tag>      # e.g. v0.1.0-alpha.513
#
# Steps, in order, each failing by name:
#   check     the tag is a published release with the assets the node's installer downloads,
#             and its engine images are on the registry
#   wake      the node is started by a hit on the site (the front door wakes it), then awaited
#             until GCE reports RUNNING and ssh answers
#   wait      the node's own startup script has finished and the current stack is up
#             (compose-provisa-1 running, /health 200)
#   install   the node's own installer, run for the tag: the instance's startup-script from its
#             metadata, with its release version replaced by the tag, run detached on the node. It
#             downloads the tag's assets, checks them against the release's sha256 manifest, loads
#             the images and writes the tag's compose and config. provisa.env keeps every key the
#             installer does not set (write_env_file). The source hot-patch's overlay is removed
#             first, so the stack runs the release image's own code.
#   recreate  the release's engine images pinned, the stack recreated on the new images and
#             proven to run them
#   reset     accounts and orgs back to the first start, as deploy-cloud.sh reset does
#   verify    as deploy-cloud.sh reset verifies: site, API, demo data set (orgs=, accounts=,
#             sources=, tables=, org_registry view, zero accounts), engine path, org base domain,
#             commercial seam
#
# The sandbox tier's VM is not touched: it installs its release from its own startup script.
#
# Env overrides: those of scripts/cloud-node-lib.sh, and GITHUB_REPO.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="$(mktemp -d)"
KEEPALIVE_PID=""
trap '[ -z "$KEEPALIVE_PID" ] || kill "$KEEPALIVE_PID" 2>/dev/null; rm -rf "$STAGE"' EXIT
# shellcheck source=cloud-node-lib.sh
. "$REPO/scripts/cloud-node-lib.sh"

TAG="${1:-}"
case "$TAG" in
  v[0-9]*) ;;
  *) echo "usage: $(basename "$0") <release tag, e.g. v0.1.0-alpha.513>" >&2; exit 2 ;;
esac

GITHUB_REPO="${GITHUB_REPO:-simpleishard-llc/provisa}"
# Where the tag's installer runs and logs on the node, root-only: the startup script it runs
# carries the deployment's secrets.
RELEASE_DIR="/root/.provisa/release/$TAG"
INSTALL_UNIT="provisa-release-install"
STARTUP_DONE="/var/lib/provisa-startup.sha256"
SENTINEL="/root/.provisa/.first-launch-complete"
IMAGES_MARKER="/root/.provisa/images/.version"
APP_IMAGE="provisa/provisa:local"

# The assets the node's installer downloads for a release (terraform/gcp-saas/main.tf
# base_startup): the AppImage, the core images manifest and every asset it lists, the plugins.
check_release() {
  echo "== check: release $TAG"
  local assets name manifest="provisa-core-images-amd64-$TAG.sha256"
  assets="$(gh release view "$TAG" --repo "$GITHUB_REPO" --json assets --jq '.assets[].name')" \
    || { echo "check: $TAG is not a release of $GITHUB_REPO" >&2; exit 1; }
  for name in "Provisa-$TAG-linux-x86_64.AppImage" "$manifest" "provisa-trino-plugins-$TAG.tar.gz"; do
    grep -qxF "$name" <<<"$assets" || { echo "check: release $TAG has no asset $name" >&2; exit 1; }
  done
  gh release download "$TAG" --repo "$GITHUB_REPO" --pattern "$manifest" --dir "$STAGE" \
    || { echo "check: cannot download $manifest from release $TAG" >&2; exit 1; }
  while read -r _ name; do
    grep -qxF "$name" <<<"$assets" \
      || { echo "check: $manifest lists $name, which release $TAG does not have" >&2; exit 1; }
  done < "$STAGE/$manifest"
  echo "   $(wc -l < "$STAGE/$manifest" | tr -d ' ') core image assets listed, all published"
  # The engine pods' images, which GKE pulls by the tag push_engine_env pins.
  for name in provisa-trino-engine provisa-zaychik; do
    docker manifest inspect "$IMAGE_REGISTRY/$name:$TAG" >/dev/null \
      || { echo "check: $IMAGE_REGISTRY/$name:$TAG is not on the registry" >&2; exit 1; }
  done
  echo "   $IMAGE_REGISTRY/{provisa-trino-engine,provisa-zaychik}:$TAG present"
}

node_status() {
  gcloud compute instances describe "$NODE" --zone "$ZONE" --project "$PROJECT" --format='value(status)'
}

# The front door starts a stopped coordinator on a hit from an address it counts as use, and
# stops it again after its idle window. A hit every 10s until GCE reports RUNNING also covers a
# node caught mid-stop (STOPPING), which a single hit would not restart.
wake_node() {
  echo "== wake: $NODE"
  local status i
  for i in $(seq 1 60); do
    status="$(node_status)" || { echo "wake: cannot describe $NODE" >&2; exit 1; }
    if [ "$status" = RUNNING ]; then
      echo "   $NODE RUNNING after $((i * 10 - 10))s"
      break
    fi
    # The hit is what wakes it; its answer (a waking page, a held or refused connection) is not
    # the result, so it is not checked. RUNNING is.
    curl -s -o /dev/null --max-time 10 "$SITE/health" || true
    [ "$i" = 60 ] && { echo "wake: $NODE still $status after 600s of hits on $SITE" >&2; exit 1; }
    sleep 10
  done
  for i in $(seq 1 60); do
    if ssh_node true >/dev/null 2>&1; then
      echo "   ssh answers after $((i * 5 - 5))s"
      return 0
    fi
    sleep 5
  done
  echo "wake: $NODE is RUNNING but ssh did not answer in 300s" >&2
  exit 1
}

# Keeps the front door from idle-stopping the node while the deploy runs: the install alone
# outlasts a short idle window, and nothing else the deploy does goes through the front door.
keep_awake() {
  ( while :; do curl -s -o /dev/null --max-time 20 "$SITE/health" || true; sleep 60; done ) &
  KEEPALIVE_PID=$!
}

# The stack up: the node's own startup script finished (it recreates the stack itself when it
# runs), compose-provisa-1 running, and /health answering. Right after a boot the container is not
# yet running, and anything that reads its environment then reads nothing.
wait_for_stack() {
  echo "== wait: $1"
  local i state
  for i in $(seq 1 120); do
    # is-active exits non-zero for every state but active; the state it prints is the answer. An
    # empty answer is an ssh that failed, asked again.
    state="$(ssh_node "systemctl is-active google-startup-scripts.service" 2>/dev/null | tr -d '\r')" || true
    [ -n "$state" ] && [ "$state" != activating ] && break
    [ "$i" = 120 ] && { echo "wait: the node's startup script still running after 1800s" >&2; exit 1; }
    sleep 15
  done
  for i in $(seq 1 120); do
    state="$(ssh_node "sudo docker inspect -f '{{.State.Status}}' $API_CONTAINER" 2>/dev/null | tr -d '\r')" || state=""
    [ "$state" = running ] && break
    [ "$i" = 120 ] && {
      echo "wait: $API_CONTAINER never running (last: ${state:-not listed})" >&2
      diagnose_containers
      exit 1
    }
    sleep 5
  done
  verify_api
}

# The release replaces the node's code, so the sandbox tier's settings that only deploy-cloud.sh's
# overlay carries (its AI key, demos, published port) would go with it from a coordinator that runs
# the tier. With the tier on its own VM the coordinator runs the control plane alone.
check_sandbox_tier() {
  sandbox_on_own_vm \
    || { echo "install: the sandbox tier runs on $NODE; this deploy installs only a control-plane-only coordinator (sandbox VM $SANDBOX_NODE)" >&2; exit 1; }
}

# What runs on the node, as root, detached (install_release). Arguments: the tag, the overlay
# extension to remove.
write_node_installer() {
  cat > "$1" <<'NODE'
#!/bin/bash
set -euo pipefail
tag="$1" overlay_ext="$2"
dir="$(dirname "$0")"
# The node's own installer: terraform/gcp-saas/main.tf's startup-script, as the instance holds it.
curl -fsS -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/startup-script \
  > "$dir/startup.metadata.sh" \
  || { echo "install: cannot read the instance's startup-script from metadata" >&2; exit 1; }
old="$(sed -n 's/^[[:space:]]*export PROVISA_VERSION="\([^"]*\)"[[:space:]]*$/\1/p' "$dir/startup.metadata.sh" | head -1)"
[ -n "$old" ] || { echo "install: the startup-script exports no PROVISA_VERSION" >&2; exit 1; }
# Every place the script names its release (the asset URLs and names, PROVISA_VERSION, an engine
# image tag) names the tag instead. A digit may not follow, so alpha.51 never rewrites alpha.513.
OLD="$old" NEW="$tag" perl -pe 's/\Q$ENV{OLD}\E(?![0-9])/$ENV{NEW}/g' \
  "$dir/startup.metadata.sh" > "$dir/startup.sh"
grep -qE "^[[:space:]]*export PROVISA_VERSION=\"$tag\"[[:space:]]*$" "$dir/startup.sh" \
  || { echo "install: the rewritten startup-script does not export PROVISA_VERSION=$tag" >&2; exit 1; }
grep -qF "releases/download/$tag" "$dir/startup.sh" \
  || { echo "install: the rewritten startup-script downloads nothing from release $tag" >&2; exit 1; }
grep -qF -- "--role control-plane" "$dir/startup.sh" \
  || { echo "install: the startup-script does not install a control plane" >&2; exit 1; }
echo "install: startup-script release $old -> $tag"
# The source hot-patch's tree is bind-mounted over the image's /app; the release runs its own.
rm -f "$overlay_ext"
# A redeploy of the tag the images were staged for would reuse tarballs a source deploy rebuilt
# (deploy-cloud.sh push_deps saves over them); the marker gone, they are rebuilt from the assets.
rm -f /root/.provisa/images/.version
# The marker of the last complete run: present and current, the startup-script exits at once.
rm -f /var/lib/provisa-startup.sha256
# Other releases' assets, several GB each, which the boot disk does not hold twice.
find /opt -maxdepth 1 -type f \( -name 'provisa-core-image-*' -o -name 'provisa-core-images-*.sha256' \
  -o -name 'provisa-trino-plugins-*' \) ! -name "*-$tag.*" -print -delete
bash "$dir/startup.sh"
NODE
}

install_release() {
  echo "== install: release $TAG on $NODE"
  write_node_installer "$STAGE/node-install.sh"
  scp_node "$STAGE/node-install.sh" /tmp/provisa-release-install.sh
  ssh_node "sudo install -d -m 700 $RELEASE_DIR \
    && sudo install -m 700 /tmp/provisa-release-install.sh $RELEASE_DIR/node-install.sh \
    && rm -f /tmp/provisa-release-install.sh"
  # Detached under systemd, so a dropped ssh session cannot cut the install off half way. The
  # log is root-only: the startup-script copies the break-glass banner into its output.
  ssh_node "sudo systemctl stop $INSTALL_UNIT >/dev/null 2>&1; sudo systemctl reset-failed $INSTALL_UNIT >/dev/null 2>&1; \
    sudo systemd-run --unit $INSTALL_UNIT -p Type=oneshot -p RemainAfterExit=yes -p UMask=0077 --no-block \
      bash -c 'exec $RELEASE_DIR/node-install.sh $TAG $OVERLAY_EXT > $RELEASE_DIR/install.log 2>&1'" \
    || { echo "install: could not start $INSTALL_UNIT on $NODE" >&2; exit 1; }
  local i state=""
  for i in $(seq 1 240); do
    sleep 15
    # A failed poll (an ssh hiccup) is a probe result, not the install's: the next poll asks again.
    state="$(ssh_node "systemctl show -p ActiveState --value $INSTALL_UNIT" 2>/dev/null | tr -d '\r')" || state=""
    case "$state" in
      active) echo "   installer finished after $((i * 15))s"; break ;;
      failed)
        echo "install: the installer failed on $NODE; the end of $RELEASE_DIR/install.log:" >&2
        ssh_node "sudo tail -n 60 $RELEASE_DIR/install.log | sed '/BREAK-GLASS ACCOUNT/,+4d'" >&2 || true
        exit 1 ;;
    esac
    [ "$i" = 240 ] && { echo "install: the installer still ${state:-unknown} after 3600s; see $RELEASE_DIR/install.log on $NODE" >&2; exit 1; }
  done
  local sentinel staged
  sentinel="$(ssh_node "sudo cat $SENTINEL" | tr -d '\r')"
  staged="$(ssh_node "sudo cat $IMAGES_MARKER" | tr -d '\r')"
  [ "$sentinel" = "$TAG" ] || { echo "install: $SENTINEL reads '$sentinel', expected $TAG" >&2; exit 1; }
  [ "$staged" = "$TAG" ] || { echo "install: $IMAGES_MARKER reads '$staged', expected $TAG" >&2; exit 1; }
  if ssh_node "sudo test -e $OVERLAY_EXT"; then
    echo "install: $OVERLAY_EXT is still on $NODE" >&2
    exit 1
  fi
  echo "   $SENTINEL=$sentinel, images staged for $staged"
}

# The containers' environment is fixed when they are created, so the release's engine images and
# the billing keys are written first and the stack recreated after; then proven: the API runs the
# image the install just loaded, with no host tree mounted over it.
recreate_stack() {
  echo "== recreate: the stack on release $TAG"
  ssh_node "sudo systemctl restart provisa" || { echo "recreate: systemctl restart provisa failed on $NODE" >&2; exit 1; }
  wait_for_stack "the stack on release $TAG"
  local running loaded mounts
  running="$(ssh_node "sudo docker inspect -f '{{.Image}}' $API_CONTAINER" | tr -d '\r')"
  loaded="$(ssh_node "sudo docker image inspect -f '{{.Id}}' $APP_IMAGE" | tr -d '\r')"
  [ -n "$loaded" ] && [ "$running" = "$loaded" ] \
    || { echo "recreate: $API_CONTAINER runs ${running:-nothing}, not $APP_IMAGE ($loaded) from release $TAG" >&2; exit 1; }
  mounts="$(ssh_node "sudo docker inspect -f '{{range .Mounts}}{{println .Source}}{{end}}' $API_CONTAINER" | tr -d '\r')"
  if grep -qF "$OVERLAY_DIR" <<<"$mounts"; then
    echo "recreate: $API_CONTAINER still mounts $OVERLAY_DIR over the release's code" >&2
    exit 1
  fi
  echo "   $API_CONTAINER on $APP_IMAGE ${loaded:7:12}, no overlay mounted"
  # The previous release's images, untagged by the load, are what fills the disk on the next one.
  ssh_node "sudo docker image prune -f" | tail -1
}

# docker restart keeps each container and its filesystem, so the plugin pushed into them survives
# it (a recreate would not).
restart_app() {
  echo "== restarting $(node_containers)"
  ssh_node "sudo docker restart $(node_containers)"
}

check_release
wake_node
keep_awake
wait_for_stack "the current stack on $NODE"
preflight_engine
check_sandbox_tier
install_release
push_engine_env "$TAG"
push_commerce_env
recreate_stack
preflight_engine
preflight_commerce
push_plugins
reset_state
restart_app
verify
verify_api
verify_demo
verify_engine
verify_org_base
verify_commerce
echo "== deployed release $TAG to $SITE"
