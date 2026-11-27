#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# REQ-2269: install Provisa with a Trino federation engine on an existing Linux host (a VM or bare
# metal), run on the host or from a workstation over SSH.
#
#   sudo scripts/install-host.sh --version v0.1.0-alpha.511
#   scripts/install-host.sh --version v0.1.0-alpha.511 --ssh admin@db-host-1
#
# Provisa comes from the release's own GitHub assets (its wheel and provisa_notebook); never the
# provisa package from a public registry. Third-party dependencies come from PyPI. The DuckDB
# extensions (provisa_duckdb_ext's version parts among them) are fetched by the stage command at
# first start, as every installer does (REQ-294, provisa.runtime_deps.stage_assets). Trino runs in
# Docker with the release's Trino plugin tarball mounted. A systemd unit runs Provisa. Running the
# script again with a newer --version upgrades in place: a fresh virtualenv per release, the
# `current` link swapped, Trino replaced, the service restarted; the config, the control-plane data
# and the Trino shared secret are kept.
#
# Every function below is callable on its own: tests source this file (main runs only when it is
# executed) and check the arguments and the rendered files.

set -euo pipefail

INSTALL_HOST_DEFAULT_REPO="kenstott/provisa"
INSTALL_HOST_DEFAULT_PREFIX="/opt/provisa"
INSTALL_HOST_DEFAULT_DATA_DIR="/var/lib/provisa"
INSTALL_HOST_DEFAULT_CONFIG_DIR="/etc/provisa"
INSTALL_HOST_DEFAULT_BIND="0.0.0.0"
INSTALL_HOST_DEFAULT_API_PORT=8000
INSTALL_HOST_DEFAULT_UI_PORT=3000
INSTALL_HOST_DEFAULT_TRINO_PORT=8080
INSTALL_HOST_DEFAULT_TRINO_HEAP_GB=5
# The engine every Provisa stack runs (docker-compose.core.yml's trino service).
INSTALL_HOST_DEFAULT_TRINO_IMAGE="trinodb/trino:481"
INSTALL_HOST_DEFAULT_HEALTH_TIMEOUT=300
# uv builds the release's virtualenv and fetches its CPython 3.12 (provisa requires >=3.12,<3.13),
# so the host needs no Python of its own. Pinned; downloaded from uv's GitHub releases.
INSTALL_HOST_UV_VERSION="0.11.19"
INSTALL_HOST_SERVICE="provisa"
INSTALL_HOST_USER="provisa"
INSTALL_HOST_TRINO_CONTAINER="provisa-trino"
# The uid Trino runs as in its image; it writes the fault-tolerant exchange spool, which
# docker-compose.core.yml's trino-exchange-init gives to the same uid (REQ-836).
INSTALL_HOST_TRINO_UID=1000
# Directories Provisa writes Trino catalog side files into (provisa/core/trino_catalog_files.py),
# each mounted into the engine at /etc/trino/<name>, as docker-compose.core.yml mounts them.
INSTALL_HOST_TRINO_SIDE_DIRS="kafka kafka-client codehost-client redis"

info() { printf '[install-host] %s\n' "$*" >&2; }
die() {
  printf '[install-host] ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: install-host.sh --version <tag> [options]

Installs (or upgrades in place) Provisa with a Trino federation engine on this Linux host.

Required:
  --version TAG          Release tag to install, e.g. v0.1.0-alpha.511

Options:
  --ssh USER@HOST        Run the install on HOST over SSH (sudo on the remote side)
  --assets-dir DIR       Read the release assets from DIR instead of downloading them
  --repo OWNER/NAME      GitHub repository the release belongs to (kenstott/provisa)
  --prefix DIR           Install root for virtualenvs, uv and plugins (/opt/provisa)
  --data-dir DIR         Provisa state: control plane, Trino side files (/var/lib/provisa)
  --config-dir DIR       provisa.yaml, provisa.env, Trino config (/etc/provisa)
  --bind ADDR            Address Provisa listens on (0.0.0.0)
  --api-port PORT        Provisa API port (8000)
  --ui-port PORT         Provisa UI port (3000)
  --trino-port PORT      Trino coordinator port on the host (8080)
  --trino-heap-gb N      Trino JVM heap in GB (5)
  --trino-image IMAGE    Trino image (trinodb/trino:481)
  --health-timeout SECS  How long the health check waits for each service (300)
  -h, --help             Show this help
EOF
}

# ── Arguments ────────────────────────────────────────────────────────────────

_require_value() {
  # $1 flag, $2 the number of words left including the flag
  [ "$2" -ge 2 ] || die "$1 needs a value"
}

_require_port() {
  # $1 flag, $2 value
  [[ "$2" =~ ^[0-9]+$ ]] && [ "$2" -ge 1 ] && [ "$2" -le 65535 ] ||
    die "$1 must be a port number between 1 and 65535 (got '$2')"
}

parse_args() {
  # Sets the IH_* globals from the command line. IH_REMOTE_ARGS keeps every argument except
  # --ssh, which is what the remote side runs with.
  IH_VERSION=""
  IH_SSH=""
  IH_ASSETS_DIR=""
  IH_REPO="$INSTALL_HOST_DEFAULT_REPO"
  IH_PREFIX="$INSTALL_HOST_DEFAULT_PREFIX"
  IH_DATA_DIR="$INSTALL_HOST_DEFAULT_DATA_DIR"
  IH_CONFIG_DIR="$INSTALL_HOST_DEFAULT_CONFIG_DIR"
  IH_BIND="$INSTALL_HOST_DEFAULT_BIND"
  IH_API_PORT="$INSTALL_HOST_DEFAULT_API_PORT"
  IH_UI_PORT="$INSTALL_HOST_DEFAULT_UI_PORT"
  IH_TRINO_PORT="$INSTALL_HOST_DEFAULT_TRINO_PORT"
  IH_TRINO_HEAP_GB="$INSTALL_HOST_DEFAULT_TRINO_HEAP_GB"
  IH_TRINO_IMAGE="$INSTALL_HOST_DEFAULT_TRINO_IMAGE"
  IH_HEALTH_TIMEOUT="$INSTALL_HOST_DEFAULT_HEALTH_TIMEOUT"
  IH_REMOTE_ARGS=()
  IH_HELP=false

  while [ "$#" -gt 0 ]; do
    case "$1" in
      -h | --help)
        IH_HELP=true
        shift
        continue
        ;;
      --ssh)
        _require_value "$1" "$#"
        IH_SSH="$2"
        shift 2
        continue
        ;;
      --version | --assets-dir | --repo | --prefix | --data-dir | --config-dir | --bind | \
        --api-port | --ui-port | --trino-port | --trino-heap-gb | --trino-image | --health-timeout)
        _require_value "$1" "$#"
        ;;
      *) die "unknown argument '$1' (see --help)" ;;
    esac
    case "$1" in
      --version) IH_VERSION="$2" ;;
      --assets-dir) IH_ASSETS_DIR="$2" ;;
      --repo) IH_REPO="$2" ;;
      --prefix) IH_PREFIX="$2" ;;
      --data-dir) IH_DATA_DIR="$2" ;;
      --config-dir) IH_CONFIG_DIR="$2" ;;
      --bind) IH_BIND="$2" ;;
      --api-port) IH_API_PORT="$2" ;;
      --ui-port) IH_UI_PORT="$2" ;;
      --trino-port) IH_TRINO_PORT="$2" ;;
      --trino-heap-gb) IH_TRINO_HEAP_GB="$2" ;;
      --trino-image) IH_TRINO_IMAGE="$2" ;;
      --health-timeout) IH_HEALTH_TIMEOUT="$2" ;;
    esac
    IH_REMOTE_ARGS+=("$1" "$2")
    shift 2
  done

  [ "$IH_HELP" = true ] && return 0
  [ -n "$IH_VERSION" ] || die "--version is required (a release tag such as v0.1.0-alpha.511)"
  [[ "$IH_VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-(alpha|beta|rc)\.[0-9]+)?$ ]] ||
    die "--version must be a release tag such as v0.1.0-alpha.511 (got '$IH_VERSION')"
  [[ "$IH_REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || die "--repo must be OWNER/NAME (got '$IH_REPO')"
  _require_port --api-port "$IH_API_PORT"
  _require_port --ui-port "$IH_UI_PORT"
  _require_port --trino-port "$IH_TRINO_PORT"
  [ "$IH_API_PORT" != "$IH_UI_PORT" ] || die "--api-port and --ui-port must differ"
  [ "$IH_API_PORT" != "$IH_TRINO_PORT" ] && [ "$IH_UI_PORT" != "$IH_TRINO_PORT" ] ||
    die "--trino-port must differ from --api-port and --ui-port"
  [[ "$IH_TRINO_HEAP_GB" =~ ^[0-9]+$ ]] && [ "$IH_TRINO_HEAP_GB" -ge 1 ] ||
    die "--trino-heap-gb must be a whole number of GB, at least 1 (got '$IH_TRINO_HEAP_GB')"
  [[ "$IH_HEALTH_TIMEOUT" =~ ^[0-9]+$ ]] && [ "$IH_HEALTH_TIMEOUT" -ge 1 ] ||
    die "--health-timeout must be a whole number of seconds (got '$IH_HEALTH_TIMEOUT')"
  local dir
  for dir in "$IH_PREFIX" "$IH_DATA_DIR" "$IH_CONFIG_DIR"; do
    [[ "$dir" = /* ]] || die "install directories must be absolute paths (got '$dir')"
  done
  if [ -n "$IH_SSH" ]; then
    [[ "$IH_SSH" =~ ^[^[:space:]]+$ ]] || die "--ssh takes USER@HOST (got '$IH_SSH')"
    [ -z "$IH_ASSETS_DIR" ] ||
      die "--assets-dir names a directory on the machine that installs; with --ssh copy it to the host and run there"
  fi
  return 0
}

# ── Release assets ───────────────────────────────────────────────────────────

pep440_version() {
  # v0.1.0-alpha.511 -> 0.1.0a511 (.github/actions/release-metadata's py_version)
  local v="${1#v}"
  v="$(printf '%s' "$v" | sed -e 's/-alpha\.\([0-9]*\)/a\1/' -e 's/-beta\.\([0-9]*\)/b\1/' -e 's/-rc\.\([0-9]*\)/rc\1/')"
  printf '%s\n' "$v"
}

wheel_platform_tag() {
  # $1 machine (uname -m)
  case "$1" in
    x86_64 | amd64) printf 'manylinux_2_28_x86_64\n' ;;
    aarch64 | arm64) printf 'manylinux_2_28_aarch64\n' ;;
    *) die "unsupported machine '$1': Provisa ships Linux wheels for x86_64 and aarch64" ;;
  esac
}

plugins_asset_name() {
  # $1 tag
  printf 'provisa-trino-plugins-%s.tar.gz\n' "$1"
}

select_assets() {
  # $1 tag, $2 machine; stdin: the release's asset names, one per line.
  # Prints the files to install, one per line: the provisa wheel, provisa_notebook and the Trino
  # plugin tarball. Fails naming each one the release lacks. provisa_duckdb_ext is not one of
  # them: it is one project split across version parts (REQ-294), which cannot share a virtualenv,
  # and the stage command fetches every part at first start.
  local tag="$1" plat py names missing="" name
  plat="$(wheel_platform_tag "$2")"
  py="$(pep440_version "$tag")"
  names="$(cat)"
  local want_provisa="provisa-${py}-cp312-cp312-${plat}.whl"
  local want_notebook="provisa_notebook-${py}-py3-none-any.whl"
  local want_plugins
  want_plugins="$(plugins_asset_name "$tag")"
  for name in "$want_provisa" "$want_notebook" "$want_plugins"; do
    if printf '%s\n' "$names" | grep -qxF "$name"; then
      printf '%s\n' "$name"
    else
      missing="${missing} ${name}"
    fi
  done
  [ -z "$missing" ] || die "release ${tag} is missing asset(s):${missing}"
}

release_api_url() {
  # $1 repo, $2 tag
  printf 'https://api.github.com/repos/%s/releases/tags/%s\n' "$1" "$2"
}

release_download_url() {
  # $1 repo, $2 tag, $3 asset name
  printf 'https://github.com/%s/releases/download/%s/%s\n' "$1" "$2" "$3"
}

list_release_assets() {
  # $1 repo, $2 tag. Prints the release's asset names.
  local json
  json="$(curl -fsSL -H 'Accept: application/vnd.github+json' "$(release_api_url "$1" "$2")")" ||
    die "could not read release $2 of $1 from the GitHub API"
  printf '%s\n' "$json" | grep -oE '"browser_download_url"[[:space:]]*:[[:space:]]*"[^"]+"' |
    sed -E 's#.*/([^/"]+)"$#\1#'
}

# ── Rendered files ───────────────────────────────────────────────────────────

render_config_yaml() {
  # $1 trino port. Written on first install only: the setup wizard adds auth to it on first visit
  # (no auth block = the wizard runs) and later edits belong to the operator.
  cat <<EOF
# Provisa configuration written by install-host.sh. The federation engine is the Trino
# coordinator install-host.sh runs in Docker on this host.
federation_engine: trino
federation_engine_host: 127.0.0.1
federation_engine_port: $1
sources: []
domains: []
tables: []
roles: []
EOF
}

render_env_file() {
  # $1 config dir, $2 data dir, $3 trino port
  cat <<EOF
# Written by install-host.sh on every run; edit the script's flags, not this file.
PROVISA_CONFIG=$1/provisa.yaml
# REQ-1296: the bootstrap org id is 'root'; the resolver's own default is not an install's.
ORG_ID=root
TRINO_HOST=127.0.0.1
TRINO_PORT=$3
PROVISA_TRINO_ETC_DIR=$2/trino
PROVISA_FAKE_KEY_DIR=$2/fake-key
EOF
}

render_systemd_unit() {
  # $1 prefix, $2 config dir, $3 data dir, $4 bind, $5 api port, $6 ui port
  cat <<EOF
[Unit]
Description=Provisa (federation engine: Trino)
Documentation=https://github.com/${INSTALL_HOST_DEFAULT_REPO}
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
Type=simple
User=${INSTALL_HOST_USER}
Group=${INSTALL_HOST_USER}
EnvironmentFile=$2/provisa.env
WorkingDirectory=$3
ExecStart=$1/current/bin/provisa run --host $4 --api-port $5 --ui-port $6 --no-browser --data-dir $3/native
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
}

render_trino_config_properties() {
  # $1 trino port. Single coordinator that also schedules work on itself; catalogs are created
  # by Provisa at runtime (dynamic catalog management).
  cat <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=$1
discovery.uri=http://localhost:$1
catalog.management=dynamic
catalog.store=memory
internal-communication.shared-secret=\${ENV:PROVISA_TRINO_SHARED_SECRET}
retry-policy=TASK
fault-tolerant-execution.exchange-encryption-enabled=true
task.low-memory-killer.policy=total-reservation-on-blocked-nodes
fault-tolerant-execution-task-memory=1GB
EOF
}

ensure_trino_exchange_dir() {
  # $1 data dir. The engine's fault-tolerant exchange spool (exchange-manager.properties), a host
  # directory Trino's user owns: a named volume's root is root's, and the first query Provisa runs
  # at boot failed AccessDeniedException there.
  install -d -m 0700 -o "$INSTALL_HOST_TRINO_UID" -g "$INSTALL_HOST_TRINO_UID" "$1/trino-exchange"
}

render_trino_exchange_properties() {
  cat <<'EOF'
exchange-manager.name=filesystem
exchange.base-directories=/data/provisa/exchange
EOF
}

render_trino_node_properties() {
  cat <<'EOF'
node.environment=provisa
node.data-dir=/data/trino
EOF
}

render_trino_jvm_config() {
  # $1 heap GB
  cat <<EOF
-server
-Xmx$1G
-XX:+ExitOnOutOfMemoryError
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-Djdk.attach.allowAttachSelf=true
--add-modules=jdk.incubator.vector
EOF
}

trino_run_args() {
  # $1 config dir, $2 data dir, $3 plugins dir, $4 image, then the plugin directory names.
  # Prints the `docker run` arguments, one per line. Host networking: Trino reaches sources on
  # this host's network exactly as Provisa does, and listens on the configured port.
  local cfg="$1" data="$2" plugins="$3" image="$4" name side
  shift 4
  [ "$#" -gt 0 ] || die "no Trino plugin directories to mount"
  printf '%s\n' -d --name "$INSTALL_HOST_TRINO_CONTAINER" --restart unless-stopped --network host
  printf '%s\n' --env-file "$cfg/trino/trino.env"
  printf '%s\n' -e PROVISA_FAKE_KEY_DIR=/etc/provisa/fake-key
  local f
  for f in config.properties jvm.config node.properties exchange-manager.properties; do
    printf '%s\n' -v "$cfg/trino/$f:/etc/trino/$f:ro"
  done
  for side in $INSTALL_HOST_TRINO_SIDE_DIRS; do
    printf '%s\n' -v "$data/trino/$side:/etc/trino/$side:ro"
  done
  printf '%s\n' -v "$data/fake-key:/etc/provisa/fake-key:ro"
  printf '%s\n' -v "$data/trino-exchange:/data/provisa/exchange"
  for name in "$@"; do
    printf '%s\n' -v "$plugins/$name:/usr/lib/trino/plugin/$name:ro"
  done
  printf '%s\n' "$image"
}

remote_command() {
  # The command the remote host runs: this script on stdin, under sudo, with the arguments
  # given here (every argument but --ssh).
  local cmd="sudo bash -s --" a
  for a in "$@"; do
    cmd="${cmd} $(printf '%q' "$a")"
  done
  printf '%s\n' "$cmd"
}

# ── Install steps (run as root on the host) ─────────────────────────────────

preflight() {
  [ "$(uname -s)" = "Linux" ] || die "install-host.sh installs on Linux; this is $(uname -s)"
  [ "$(id -u)" -eq 0 ] || die "run as root (sudo), or from a workstation with --ssh"
  local tool
  for tool in curl tar systemctl docker useradd install; do
    command -v "$tool" >/dev/null 2>&1 || die "required command '$tool' is not installed"
  done
  docker info >/dev/null 2>&1 || die "Docker is installed but the daemon is not reachable (docker info failed)"
  [ -d /run/systemd/system ] || die "systemd is not running on this host"
  wheel_platform_tag "$(uname -m)" >/dev/null
}

fetch_assets() {
  # $1 destination dir. Sets IH_SELECTED to the selected asset names, one per line. Called
  # directly, never in a command substitution: bash runs a substitution without errexit, so a
  # refusal there was printed and the install went on (release 513's dry run).
  local dest="$1" names selected name
  mkdir -p "$dest"
  if [ -n "$IH_ASSETS_DIR" ]; then
    [ -d "$IH_ASSETS_DIR" ] || die "--assets-dir $IH_ASSETS_DIR does not exist"
    names="$(find "$IH_ASSETS_DIR" -maxdepth 1 -type f -printf '%f\n')"
  else
    names="$(list_release_assets "$IH_REPO" "$IH_VERSION")"
  fi
  selected="$(printf '%s\n' "$names" | select_assets "$IH_VERSION" "$(uname -m)")"
  for name in $selected; do
    if [ -n "$IH_ASSETS_DIR" ]; then
      cp "$IH_ASSETS_DIR/$name" "$dest/$name"
    elif [ ! -s "$dest/$name" ]; then
      info "downloading $name"
      curl -fL --retry 3 --retry-delay 5 -o "$dest/$name.part" \
        "$(release_download_url "$IH_REPO" "$IH_VERSION" "$name")" ||
        die "download of $name from release $IH_VERSION failed"
      mv "$dest/$name.part" "$dest/$name"
    fi
  done
  IH_SELECTED="$selected"
}

ensure_user() {
  id -u "$INSTALL_HOST_USER" >/dev/null 2>&1 ||
    useradd --system --home-dir "$IH_DATA_DIR" --shell /usr/sbin/nologin "$INSTALL_HOST_USER"
}

ensure_uv() {
  local uv="$IH_PREFIX/bin/uv" arch
  if [ -x "$uv" ] && [ "$("$uv" --version | awk '{print $2}')" = "$INSTALL_HOST_UV_VERSION" ]; then
    return 0
  fi
  case "$(uname -m)" in
    x86_64 | amd64) arch=x86_64 ;;
    aarch64 | arm64) arch=aarch64 ;;
  esac
  local tmp
  tmp="$(mktemp -d)"
  info "downloading uv ${INSTALL_HOST_UV_VERSION}"
  curl -fsSL --retry 3 -o "$tmp/uv.tar.gz" \
    "https://github.com/astral-sh/uv/releases/download/${INSTALL_HOST_UV_VERSION}/uv-${arch}-unknown-linux-gnu.tar.gz" ||
    die "download of uv ${INSTALL_HOST_UV_VERSION} failed"
  tar -xzf "$tmp/uv.tar.gz" -C "$tmp"
  install -D -m 0755 "$tmp/uv-${arch}-unknown-linux-gnu/uv" "$uv"
  rm -rf "$tmp"
}

install_provisa() {
  # $1 asset dir, then the selected asset names. Builds the release's virtualenv and points
  # $IH_PREFIX/current at it.
  local assets="$1" name provisa_whl="" others=()
  shift
  for name in "$@"; do
    case "$name" in
      provisa-*.whl) provisa_whl="$assets/$name" ;;
      *.whl) others+=("$assets/$name") ;;
    esac
  done
  local venv="$IH_PREFIX/venvs/$IH_VERSION"
  export UV_PYTHON_INSTALL_DIR="$IH_PREFIX/python"
  export UV_CACHE_DIR="$IH_PREFIX/uv-cache"
  rm -rf "$venv"
  info "creating the Python 3.12 environment for $IH_VERSION"
  "$IH_PREFIX/bin/uv" venv --python 3.12 --python-preference only-managed "$venv"
  info "installing Provisa $IH_VERSION from the release's wheels"
  # The first-party wheels are named as files, so pip takes them from the release and never
  # from a registry; their third-party dependencies resolve from PyPI.
  VIRTUAL_ENV="$venv" "$IH_PREFIX/bin/uv" pip install --python "$venv/bin/python" \
    "${provisa_whl}[embedded]" "${others[@]}"
  ln -sfn "$venv" "$IH_PREFIX/current.new"
  mv -T "$IH_PREFIX/current.new" "$IH_PREFIX/current"
}

install_trino_plugins() {
  # $1 plugin tarball. Replaces $IH_PREFIX/trino/plugins with the release's plugins.
  local plugins="$IH_PREFIX/trino/plugins"
  rm -rf "$plugins.new"
  mkdir -p "$plugins.new"
  tar -xzf "$1" -C "$plugins.new"
  rm -rf "$plugins"
  mv "$plugins.new" "$plugins"
}

plugin_dirs() {
  # $1 plugins dir. Prints each plugin directory name holding at least one jar.
  local d
  for d in "$1"/*/; do
    d="${d%/}"
    compgen -G "$d/*.jar" >/dev/null && basename "$d"
  done
}

write_files() {
  local cfg="$IH_CONFIG_DIR" data="$IH_DATA_DIR" side
  # The setup wizard and the admin UI write provisa.yaml (and its .bak beside it) as the service
  # user; the Trino directory, which holds the engine's shared secret, stays root's.
  install -d -m 0755 -o "$INSTALL_HOST_USER" -g "$INSTALL_HOST_USER" "$cfg"
  install -d -m 0755 "$cfg/trino"
  install -d -m 0755 -o "$INSTALL_HOST_USER" -g "$INSTALL_HOST_USER" "$data" "$data/native" \
    "$data/fake-key" "$data/trino" "$data/trino/etc" "$data/trino/etc/worker"
  for side in $INSTALL_HOST_TRINO_SIDE_DIRS; do
    install -d -m 0755 -o "$INSTALL_HOST_USER" -g "$INSTALL_HOST_USER" "$data/trino/$side"
  done
  ensure_trino_exchange_dir "$data"
  if [ ! -f "$cfg/provisa.yaml" ]; then
    render_config_yaml "$IH_TRINO_PORT" >"$cfg/provisa.yaml"
  else
    info "keeping the existing $cfg/provisa.yaml"
  fi
  chown "$INSTALL_HOST_USER:$INSTALL_HOST_USER" "$cfg/provisa.yaml"
  render_env_file "$cfg" "$data" "$IH_TRINO_PORT" >"$cfg/provisa.env"
  render_trino_config_properties "$IH_TRINO_PORT" >"$cfg/trino/config.properties"
  render_trino_jvm_config "$IH_TRINO_HEAP_GB" >"$cfg/trino/jvm.config"
  render_trino_node_properties >"$cfg/trino/node.properties"
  render_trino_exchange_properties >"$cfg/trino/exchange-manager.properties"
  if [ ! -s "$cfg/trino/trino.env" ]; then
    (
      umask 077
      printf 'PROVISA_TRINO_SHARED_SECRET=%s\n' "$(head -c 48 /dev/urandom | base64 | tr -d '\n/+=')" \
        >"$cfg/trino/trino.env"
    )
  fi
  render_systemd_unit "$IH_PREFIX" "$cfg" "$data" "$IH_BIND" "$IH_API_PORT" "$IH_UI_PORT" \
    >"/etc/systemd/system/${INSTALL_HOST_SERVICE}.service"
}

start_trino() {
  local plugins="$IH_PREFIX/trino/plugins" names args=()
  mapfile -t names < <(plugin_dirs "$plugins")
  [ "${#names[@]}" -gt 0 ] || die "the plugin tarball held no plugin directory with a jar"
  mapfile -t args < <(trino_run_args "$IH_CONFIG_DIR" "$IH_DATA_DIR" "$plugins" "$IH_TRINO_IMAGE" "${names[@]}")
  info "pulling $IH_TRINO_IMAGE"
  docker pull "$IH_TRINO_IMAGE" >/dev/null
  docker rm -f "$INSTALL_HOST_TRINO_CONTAINER" >/dev/null 2>&1 || true
  docker run "${args[@]}" >/dev/null
}

start_provisa() {
  systemctl daemon-reload
  systemctl enable "$INSTALL_HOST_SERVICE" >/dev/null
  systemctl restart "$INSTALL_HOST_SERVICE"
}

prune_old_venvs() {
  local v
  for v in "$IH_PREFIX/venvs"/*; do
    [ "$v" = "$IH_PREFIX/venvs/$IH_VERSION" ] || rm -rf "$v"
  done
}

# ── Health check ─────────────────────────────────────────────────────────────

_wait_for() {
  # $1 timeout seconds, then a command; true when the command succeeds within the timeout.
  local deadline=$((SECONDS + $1))
  shift
  until "$@" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 1
  done
}

_trino_started() {
  curl -fsS "http://127.0.0.1:${IH_TRINO_PORT}/v1/info" | grep -qE '"starting"[[:space:]]*:[[:space:]]*false'
}

_trino_saw_provisa() {
  # The startup probe runs SELECT 1 through the engine as user provisa.
  curl -fsS -H 'X-Trino-User: provisa' "http://127.0.0.1:${IH_TRINO_PORT}/v1/query" |
    grep -qE '"user"[[:space:]]*:[[:space:]]*"provisa"'
}

_provisa_ready() {
  curl -fsS "http://127.0.0.1:${IH_API_PORT}/ready" | grep -q '"ready"'
}

_provisa_health_ok() {
  curl -fsS "http://127.0.0.1:${IH_API_PORT}/health" | grep -qE '"status"[[:space:]]*:[[:space:]]*"ok"'
}

_ui_up() {
  curl -fsS -o /dev/null "http://127.0.0.1:${IH_UI_PORT}/"
}

health_check() {
  # Names each check that failed and returns non-zero when any did.
  local failed=() t="$IH_HEALTH_TIMEOUT"
  [ "$(docker inspect -f '{{.State.Running}}' "$INSTALL_HOST_TRINO_CONTAINER" 2>/dev/null)" = "true" ] ||
    failed+=("Trino container ${INSTALL_HOST_TRINO_CONTAINER} is not running (docker logs ${INSTALL_HOST_TRINO_CONTAINER})")
  _wait_for "$t" _trino_started ||
    failed+=("Trino at http://127.0.0.1:${IH_TRINO_PORT}/v1/info did not report started within ${t}s")
  systemctl is-active --quiet "$INSTALL_HOST_SERVICE" ||
    failed+=("systemd service ${INSTALL_HOST_SERVICE} is not active (journalctl -u ${INSTALL_HOST_SERVICE})")
  _wait_for "$t" _provisa_ready ||
    failed+=("Provisa API at http://127.0.0.1:${IH_API_PORT}/ready did not report ready within ${t}s")
  _provisa_health_ok ||
    failed+=("Provisa API at http://127.0.0.1:${IH_API_PORT}/health does not report status ok")
  _ui_up || failed+=("Provisa UI at http://127.0.0.1:${IH_UI_PORT}/ does not answer")
  _wait_for "$t" _trino_saw_provisa ||
    failed+=("Trino has run no query from Provisa (user provisa): Provisa is not using Trino as its federation engine")
  if [ "${#failed[@]}" -gt 0 ]; then
    local f
    printf '[install-host] health check FAILED:\n' >&2
    for f in "${failed[@]}"; do
      printf '  - %s\n' "$f" >&2
    done
    return 1
  fi
  info "health check passed: Provisa $IH_VERSION on http://${IH_BIND}:${IH_UI_PORT} (API ${IH_API_PORT}), Trino on ${IH_TRINO_PORT}"
}

# ── Entry ────────────────────────────────────────────────────────────────────

install_local() {
  preflight
  local assets="$IH_PREFIX/releases/$IH_VERSION" names tarball
  fetch_assets "$assets"
  mapfile -t names <<<"$IH_SELECTED"
  tarball="$assets/$(plugins_asset_name "$IH_VERSION")"
  ensure_user
  ensure_uv
  install_provisa "$assets" "${names[@]}"
  install_trino_plugins "$tarball"
  write_files
  start_trino
  start_provisa
  health_check
  prune_old_venvs
}

main() {
  parse_args "$@"
  if [ "$IH_HELP" = true ]; then
    usage
    return 0
  fi
  if [ -n "$IH_SSH" ]; then
    command -v ssh >/dev/null 2>&1 || die "--ssh needs the ssh client"
    info "installing on $IH_SSH over SSH"
    # The remote command is built here on purpose: remote_command quotes each argument (%q).
    # shellcheck disable=SC2029
    ssh "$IH_SSH" "$(remote_command "${IH_REMOTE_ARGS[@]}")" <"${BASH_SOURCE[0]}"
    return
  fi
  install_local
}

# Executed (a file, or the script on stdin of `bash -s` over SSH) runs main; sourced does not.
# main reads nothing from stdin, so over SSH no child command can consume the rest of the script.
if [ -z "${BASH_SOURCE[0]:-}" ] || [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@" </dev/null
fi
