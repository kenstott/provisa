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
#   scripts/deploy-cloud-release.sh <tag>                # e.g. v0.1.0-alpha.513
#   scripts/deploy-cloud-release.sh --check-only <tag>   # check, render and asset check; nothing
#                                                        # on the node is touched
#
# Steps, in order, each failing by name:
#   check     the tag is a published release and its engine images are on the registry
#   render    the release's own installer: terraform/gcp-saas's coordinator startup-script at the
#             tag, rendered by a terraform plan of that instance (-refresh=false, -lock=false, on a
#             copy of the deployment's state and variables, $CLOUD_TF_DIR). The plan is read and
#             thrown away: nothing is applied, so the instance's metadata is not changed. Then every
#             asset the rendered script downloads is checked against the release's assets, and one
#             the release lacks is refused by name.
#   wake      the node is started by a hit on the site (the front door wakes it), then awaited
#             until GCE reports RUNNING and ssh answers
#   wait      the node's own startup script has finished and the current stack is up
#             (compose-provisa-1 running, /health 200)
#   install   the rendered startup-script, run detached on the node. It downloads the tag's
#             assets, checks them against the release's sha256 manifest, loads the images and
#             writes the tag's compose and config. provisa.env keeps every key the installer does
#             not set (write_env_file). The source hot-patch's overlay is removed first, so the
#             stack runs the release image's own code.
#   recreate  the release's engine images pinned, the stack recreated on the new images and
#             proven to run them
#   reset     accounts and orgs back to the first start, as deploy-cloud.sh reset does
#   verify    as deploy-cloud.sh reset verifies: site, API, demo data set (orgs=, accounts=,
#             sources=, tables=, org_registry view, zero accounts), engine path, org base domain,
#             commercial seam
#
# The sandbox tier's VM is not touched: it installs its release from its own startup script.
#
# Env overrides: those of scripts/cloud-node-lib.sh, GITHUB_REPO, and CLOUD_TF_DIR: the directory
# holding the deployment's terraform.tfvars, *.auto.tfvars, terraform.tfstate and initialised
# providers (.terraform/providers), by default terraform/gcp-saas in this repository's primary
# checkout, where the deployment is applied from. A variable the tag's module requires that those
# files do not set is given as TF_VAR_<name>, or the render fails naming it.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="$(mktemp -d)"
KEEPALIVE_PID=""
trap '[ -z "$KEEPALIVE_PID" ] || kill "$KEEPALIVE_PID" 2>/dev/null; rm -rf "$STAGE"' EXIT
# shellcheck source=cloud-node-lib.sh
. "$REPO/scripts/cloud-node-lib.sh"

CHECK_ONLY=""
if [ "${1:-}" = --check-only ]; then
  CHECK_ONLY=1
  shift
fi
TAG="${1:-}"
case "$TAG" in
  v[0-9]*) ;;
  *) echo "usage: $(basename "$0") [--check-only] <release tag, e.g. v0.1.0-alpha.513>" >&2; exit 2 ;;
esac

GITHUB_REPO="${GITHUB_REPO:-simpleishard-llc/provisa}"
CLOUD_TF_DIR="${CLOUD_TF_DIR:-$(git -C "$REPO" worktree list --porcelain | sed -n '1s/^worktree //p')/terraform/gcp-saas}"
# The rendered installer and the release's asset list (render_startup, check_startup_assets).
STARTUP="$STAGE/startup.sh"
ASSETS="$STAGE/assets"
# Where the tag's installer runs and logs on the node, root-only: the startup script it runs
# carries the deployment's secrets.
RELEASE_DIR="/root/.provisa/release/$TAG"
INSTALL_UNIT="provisa-release-install"
SENTINEL="/root/.provisa/.first-launch-complete"
IMAGES_MARKER="/root/.provisa/images/.version"
APP_IMAGE="provisa/provisa:local"

# The release published, and the engine pods' images on the registry, which GKE pulls by the tag
# push_engine_env pins. What the installer downloads is checked against $ASSETS after the render.
check_release() {
  echo "== check: release $TAG"
  gh release view "$TAG" --repo "$GITHUB_REPO" --json assets --jq '.assets[].name' > "$ASSETS" \
    || { echo "check: $TAG is not a release of $GITHUB_REPO" >&2; exit 1; }
  echo "   $(wc -l < "$ASSETS" | tr -d ' ') assets published"
  local name
  for name in provisa-trino-engine provisa-zaychik; do
    docker manifest inspect "$IMAGE_REGISTRY/$name:$TAG" >/dev/null \
      || { echo "check: $IMAGE_REGISTRY/$name:$TAG is not on the registry" >&2; exit 1; }
  done
  echo "   $IMAGE_REGISTRY/{provisa-trino-engine,provisa-zaychik}:$TAG present"
}

# The release's own installer: the coordinator's startup-script as the tag's terraform/gcp-saas
# renders it for this deployment. The tag's tree (the module reads files from elsewhere in it), a
# copy of the deployment's variables and state, and a plan of the coordinator alone: -refresh=false
# reads nothing back from GCP, -lock=false takes no lock on a state that is only a copy. The plan
# is the render; it is never applied, so no standing state changes. A plan that would do more than
# update the instance in place (a replace, a create) has values it cannot know yet, and is refused.
render_startup() {
  echo "== render: terraform/gcp-saas startup-script at $TAG"
  local src="$STAGE/src" tf="$STAGE/src/terraform/gcp-saas" f actions
  [ -f "$CLOUD_TF_DIR/terraform.tfstate" ] \
    || { echo "render: no terraform.tfstate in $CLOUD_TF_DIR (CLOUD_TF_DIR)" >&2; exit 1; }
  [ -d "$CLOUD_TF_DIR/.terraform/providers" ] \
    || { echo "render: no initialised providers in $CLOUD_TF_DIR/.terraform/providers (CLOUD_TF_DIR)" >&2; exit 1; }
  git -C "$REPO" fetch --quiet origin "refs/tags/$TAG:refs/tags/$TAG" \
    || { echo "render: cannot fetch tag $TAG" >&2; exit 1; }
  mkdir -p "$src"
  git -C "$REPO" archive "$TAG" | tar -x -C "$src" \
    || { echo "render: cannot extract the tree of $TAG" >&2; exit 1; }
  [ -f "$tf/main.tf" ] || { echo "render: $TAG has no terraform/gcp-saas/main.tf" >&2; exit 1; }
  for f in "$CLOUD_TF_DIR"/terraform.tfvars "$CLOUD_TF_DIR"/*.auto.tfvars "$CLOUD_TF_DIR"/terraform.tfstate; do
    [ -f "$f" ] && install -m 600 "$f" "$tf/"
  done
  ( cd "$tf" && terraform init -input=false -no-color -plugin-dir="$CLOUD_TF_DIR/.terraform/providers" ) \
    > "$STAGE/tf-init.log" 2>&1 \
    || { echo "render: terraform init failed:" >&2; tail -n 30 "$STAGE/tf-init.log" >&2; exit 1; }
  ( cd "$tf" && terraform plan -input=false -no-color -refresh=false -lock=false \
      -target=google_compute_instance.coordinator \
      -var "provisa_version=$TAG" -var "github_repo=$GITHUB_REPO" -out=coordinator.tfplan ) \
    > "$STAGE/tf-plan.log" 2>&1 \
    || { echo "render: terraform plan failed:" >&2; grep -A12 '^Error' "$STAGE/tf-plan.log" >&2; exit 1; }
  ( cd "$tf" && terraform show -json coordinator.tfplan ) > "$STAGE/plan.json" \
    || { echo "render: cannot read the plan" >&2; exit 1; }
  actions="$(jq -r '.resource_changes[] | select(.address == "google_compute_instance.coordinator") | .change.actions | join(",")' "$STAGE/plan.json")"
  case "$actions" in
    update | no-op) ;;
    *) echo "render: the plan would ${actions:-not touch} google_compute_instance.coordinator, not update it in place" >&2; exit 1 ;;
  esac
  jq -e -r '.resource_changes[] | select(.address == "google_compute_instance.coordinator") | .change.after.metadata["startup-script"] // empty' \
    "$STAGE/plan.json" > "$STARTUP" && [ -s "$STARTUP" ] \
    || { echo "render: the plan renders no startup-script for google_compute_instance.coordinator" >&2; exit 1; }
  chmod 600 "$STARTUP"
  bash -n "$STARTUP" || { echo "render: the rendered startup-script does not parse" >&2; exit 1; }
  grep -qE "^[[:space:]]*export PROVISA_VERSION=\"$TAG\"[[:space:]]*$" "$STARTUP" \
    || { echo "render: the rendered startup-script does not export PROVISA_VERSION=$TAG" >&2; exit 1; }
  grep -qF -- "--role control-plane" "$STARTUP" \
    || { echo "render: the rendered startup-script does not install a control plane" >&2; exit 1; }
  echo "   $(wc -l < "$STARTUP" | tr -d ' ') lines, release $TAG"
}

# Every download in the rendered startup-script is an asset of the release. Arguments: the script,
# the release's asset names (one per line), the release (owner/name), the tag. Its downloads are
# "$gh_base/<name>" (gh_base must be the release's download URL) and the manifest loop
# `for asset in $(awk '{print $2}' /opt/<manifest>)`, whose manifest is fetched from the release
# here and read. Any other download, or a name it cannot resolve, is refused by name.
check_startup_assets() {
  local script="$1" assets="$2" repo="$3" tag="$4"
  local want="https://github.com/$repo/releases/download/$tag" base name manifest fetched=0 bad=0
  echo "== check: the startup-script's downloads against release $tag"
  base="$(sed -n 's/^[[:space:]]*gh_base="\([^"]*\)"[[:space:]]*$/\1/p' "$script")"
  [ -n "$base" ] || { echo "check: the startup-script sets no gh_base" >&2; return 1; }
  [ "$(grep -c . <<<"$base")" = 1 ] || { echo "check: the startup-script sets gh_base more than once" >&2; return 1; }
  [ "$base" = "$want" ] || { echo "check: the startup-script downloads from $base, not $want" >&2; return 1; }
  # A release URL written out instead of through gh_base is a download this check would not see.
  if grep -v '^[[:space:]]*gh_base=' "$script" | grep -F "releases/download/" >/dev/null; then
    echo "check: the startup-script downloads from a release URL other than gh_base:" >&2
    grep -v '^[[:space:]]*gh_base=' "$script" | grep -F "releases/download/" >&2
    return 1
  fi
  # The names are the startup-script's own text ("$gh_base/<name>"), matched as written: the
  # single-quoted $ is literal on purpose.
  # shellcheck disable=SC2016
  while read -r name; do
    case "$name" in
      '$asset')
        manifest="$(sed -n "s|^[[:space:]]*for asset in \$(awk '{print \$2}' /opt/\([^)]*\)); do[[:space:]]*$|\1|p" "$script")"
        [ "$(grep -c . <<<"$manifest")" = 1 ] \
          || { echo "check: the startup-script downloads \$gh_base/\$asset outside one manifest loop" >&2; return 1; }
        grep -qF "\"\$gh_base/$manifest\"" "$script" \
          || { echo "check: the startup-script reads /opt/$manifest without downloading it from the release" >&2; return 1; }
        grep -qxF "$manifest" "$assets" || continue  # refused as its own download below
        gh release download "$tag" --repo "$repo" --pattern "$manifest" --dir "$STAGE" --clobber </dev/null \
          || { echo "check: cannot download $manifest from release $tag" >&2; return 1; }
        while read -r _ name; do
          fetched=$((fetched + 1))
          grep -qxF "$name" "$assets" \
            || { echo "check: release $tag has no asset $name (listed in $manifest)" >&2; bad=1; }
        done < "$STAGE/$manifest" ;;
      *'$'*) echo "check: the startup-script downloads \$gh_base/$name, a name this check cannot resolve" >&2; return 1 ;;
      *)
        fetched=$((fetched + 1))
        grep -qxF "$name" "$assets" || { echo "check: release $tag has no asset $name" >&2; bad=1; } ;;
    esac
  done < <(grep -o '"\$gh_base/[^"]*"' "$script" | sed 's|^"\$gh_base/||; s|"$||' | sort -u)
  [ "$fetched" -gt 0 ] || { echo "check: the startup-script downloads nothing from release $tag" >&2; return 1; }
  [ "$bad" = 0 ] || return 1
  echo "   $fetched downloads, all assets of release $tag"
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
  # Before the install the stack being replaced may be the broken one this deploy exists to replace
  # (the 513 deploy, 2026-11-28: an old hot-patch tree failed startup, /health 503, and the deploy
  # stopped here, never installing): its API is not waited for. After the recreate it is.
  [ "${2:-}" = before-install ] && return 0
  verify_api
}

# The release replaces the node's code, so the sandbox tier's settings that only deploy-cloud.sh's
# overlay carries (its AI key, demos, published port) would go with it from a coordinator that runs
# the tier. With the tier on its own VM the coordinator runs the control plane alone.
check_sandbox_tier() {
  sandbox_on_own_vm \
    || { echo "install: the sandbox tier runs on $NODE; this deploy installs only a control-plane-only coordinator (sandbox VM $SANDBOX_NODE)" >&2; exit 1; }
}

# What runs on the node, as root, detached (install_release), beside the rendered startup.sh.
# Arguments: the tag, the overlay extension to remove.
write_node_installer() {
  cat > "$1" <<'NODE'
#!/bin/bash
set -euo pipefail
tag="$1" overlay_ext="$2"
dir="$(dirname "$0")"
# The release's own installer, rendered from the tag's terraform/gcp-saas (render_startup).
[ -s "$dir/startup.sh" ] || { echo "install: no startup.sh beside $0" >&2; exit 1; }
echo "install: release $tag"
# The source hot-patch's tree is bind-mounted over the image's /app; the release runs its own.
rm -f "$overlay_ext"
# A redeploy of the tag the images were staged for would reuse tarballs a source deploy rebuilt
# (deploy-cloud.sh push_deps saves over them); the marker gone, they are rebuilt from the assets.
rm -f /root/.provisa/images/.version
# The marker of the last complete run: present and current, the startup-script exits at once.
rm -f /var/lib/provisa-startup.sha256
# Other releases' assets, several GB each, which the boot disk does not hold twice.
find /opt -maxdepth 1 -type f \( -name 'provisa-core-image-*' -o -name 'provisa-core-images-*' \
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
  # The rendered startup-script carries the deployment's secrets: sent on stdin, straight into the
  # root-only release directory, never through a world-readable /tmp file.
  ssh_node "sudo sh -c 'umask 077 && cat > $RELEASE_DIR/startup.sh'" < "$STARTUP" \
    || { echo "install: could not send the rendered startup-script to $NODE" >&2; exit 1; }
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
render_startup
check_startup_assets "$STARTUP" "$ASSETS" "$GITHUB_REPO" "$TAG" || exit 1
if [ -n "$CHECK_ONLY" ]; then
  echo "== check-only: release $TAG renders an installer whose every download is published"
  exit 0
fi
wake_node
keep_awake
wait_for_stack "the current stack on $NODE" before-install
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
