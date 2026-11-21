#!/usr/bin/env bash
# Build the pip-installable Provisa wheel (REQ-1126/REQ-1127).
#
# Stages the precompiled React UI and the runtime config/ files INTO the package
# tree (provisa/_ui, provisa/_config) so the wheel is self-contained on a
# Python-only, npm/Node-free host, then builds the sdist and the platform wheel (REQ-2194).
#
#   scripts/build-wheel.sh            # vite build + stage + python -m build
#   PROVISA_SKIP_UI_BUILD=1 ...       # reuse an existing provisa-ui/dist
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

UI_SRC="${REPO_ROOT}/provisa-ui/dist"
UI_DST="${REPO_ROOT}/provisa/_ui"
CONFIG_SRC="${REPO_ROOT}/config"
CONFIG_DST="${REPO_ROOT}/provisa/_config"

# ── 1. Build the React UI (vite) unless a prebuilt dist is reused ────────────
if [ "${PROVISA_SKIP_UI_BUILD:-0}" != "1" ]; then
  echo "[build-wheel] Building UI (vite)…"
  ( cd "${REPO_ROOT}/provisa-ui" && npm ci && npm run build )
fi
if [ ! -d "$UI_SRC" ]; then
  echo "[build-wheel] ERROR: ${UI_SRC} not found — UI build did not produce dist/." >&2
  exit 1
fi

# ── 2. Stage UI + config into the package tree ──────────────────────────────
echo "[build-wheel] Staging UI  -> provisa/_ui"
rm -rf "$UI_DST"; mkdir -p "$UI_DST"
cp -r "${UI_SRC}/." "$UI_DST/"

echo "[build-wheel] Staging config -> provisa/_config"
rm -rf "$CONFIG_DST"; mkdir -p "$CONFIG_DST"
cp "${CONFIG_SRC}/capabilities.yaml" "${CONFIG_SRC}/pg_extension_catalog.yaml" "$CONFIG_DST/"
# Bundle the demo (`provisa run --demo`, REQ-414): the pre-federated pet-store + shelter config plus
# its embedded SQLite sample data. The config resolves the SQLite paths via ${env:PROVISA_DEMO_DIR},
# which cli.py points at provisa/_config/demo/files.
cp "${CONFIG_SRC}/provisa-install.yaml" "$CONFIG_DST/"
# Minimal install skeleton the first-run setup wizard layers `auth` onto (REQ-120): system
# sources/domains + the built-in admin role. ProvisaConfig requires sources/domains/tables/roles,
# so a fileless first-run install has no valid config for _load_and_build until the wizard writes one.
cp "${CONFIG_SRC}/provisa-install-base.yaml" "$CONFIG_DST/"
mkdir -p "$CONFIG_DST/demo/files"
cp "${REPO_ROOT}/demo/files/pet_store.sqlite" "${REPO_ROOT}/demo/files/inquiries.sqlite" \
   "$CONFIG_DST/demo/files/"

# ── 3. Build sdist + wheel ──────────────────────────────────────────────────
# PYTHON overridable so a caller can build with a specific interpreter (e.g. the
# bundled python-build-standalone); defaults to `python` on PATH.
#
# REQ-2194: the wheel carries provisa._encoders, compiled from Rust (rust/encoders) by
# setuptools-rust, so it is a platform wheel for PYTHON's platform. On Linux the Dockerfile's
# provisa-wheel stage builds it, a manylinux_2_28 wheel cross-compiled with zig for this machine's
# architecture (the one Linux recipe: the images and the release build it the same way). The Rust
# toolchain is the one every recipe names (Dockerfile's rust image).
PY="${PYTHON:-python}"
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.95.0}"
sdist=0; wheel=0
for arg in "$@"; do
  case "$arg" in
    --sdist) sdist=1 ;;
    --wheel) wheel=1 ;;
    *) echo "[build-wheel] ERROR: unknown argument $arg (--sdist, --wheel)" >&2; exit 2 ;;
  esac
done
if [ "$sdist" = 0 ] && [ "$wheel" = 0 ]; then sdist=1; wheel=1; fi
mkdir -p "${REPO_ROOT}/dist"

if [ "$sdist" = 1 ]; then
  echo "[build-wheel] ${PY} -m build --sdist"
  "$PY" -m build --sdist
fi
if [ "$wheel" = 1 ]; then
  rm -f "${REPO_ROOT}"/dist/provisa-*.whl
  if [ "$(uname -s)" = "Linux" ]; then
    case "$(uname -m)" in
      x86_64) platform=linux/amd64 ;;
      aarch64 | arm64) platform=linux/arm64 ;;
      *) echo "[build-wheel] ERROR: no Linux wheel for $(uname -m)" >&2; exit 1 ;;
    esac
    echo "[build-wheel] docker build --target provisa-wheel (${platform})"
    docker build --platform "$platform" --target provisa-wheel \
      --output "type=local,dest=${REPO_ROOT}/dist" "$REPO_ROOT"
  else
    # A macOS Python is often universal2, but the module is compiled for PYTHON's own architecture
    # (arm64: Intel Macs are not supported), so the wheel is tagged for that one, at Rust's macOS
    # 11 floor.
    plat=()
    if [ "$(uname -s)" = "Darwin" ]; then
      export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-11.0}"
      arch="$("$PY" -c 'import platform; print(platform.machine())')"
      plat=("-C--build-option=--plat-name=macosx-${MACOSX_DEPLOYMENT_TARGET}-${arch}")
    fi
    echo "[build-wheel] ${PY} -m build --wheel ${plat[*]+${plat[*]}}"
    "$PY" -m build --wheel ${plat[@]+"${plat[@]}"}
  fi
  # One wheel, with the compiled module and its notices in it, or the build failed.
  "$PY" "${REPO_ROOT}/scripts/check-provisa-wheel.py" "${REPO_ROOT}"/dist/provisa-*.whl
fi

echo "[build-wheel] Done. Artifacts in ${REPO_ROOT}/dist/"
