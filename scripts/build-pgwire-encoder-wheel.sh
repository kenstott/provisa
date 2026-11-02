#!/usr/bin/env bash
# REQ-2194: build the provisa-pgwire-encoder wheel (the Rust DataRow encoder) into OUT-DIR for
# PYTHON's platform. An installer's wheelhouse needs it beside provisa's own wheel: provisa pins
# it, and PyPI does not have this version until the release publishes it.
#
# Linux: the Dockerfile's pgwire-encoder-wheel stage, a manylinux_2_28 wheel for this machine's
# architecture (the one Linux recipe: the image, the AppImage and the release build the same way).
# macOS and Windows: maturin, from a throwaway virtualenv, so nothing is left in PYTHON's tree.
# PGWIRE_ENCODER_TARGET names another Rust target this host can build for (x86_64-apple-darwin
# on an arm64 Mac).
# Usage: scripts/build-pgwire-encoder-wheel.sh PYTHON OUT-DIR
set -euo pipefail
PYTHON="$1"
OUT="$2"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
CRATE="$REPO/experiments/pgwire_encoder_rs"
# The toolchain and maturin every recipe uses (Dockerfile and build-sfx.ps1 name the same two).
MATURIN_VERSION="1.15.0"
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.95.0}"
mkdir -p "$OUT"

if [ "$(uname -s)" = "Linux" ]; then
  case "$(uname -m)" in
    x86_64) platform=linux/amd64 ;;
    aarch64 | arm64) platform=linux/arm64 ;;
    *) echo "build-pgwire-encoder-wheel: no Linux wheel for $(uname -m)" >&2; exit 1 ;;
  esac
  docker build --platform "$platform" --target pgwire-encoder-wheel --output "type=local,dest=$OUT" "$REPO"
else
  venv="$(mktemp -d)/maturin"
  trap 'rm -rf "$(dirname "$venv")"' EXIT
  "$PYTHON" -m venv "$venv"
  bin="$venv/bin"
  [ -d "$venv/Scripts" ] && bin="$venv/Scripts"
  "$bin/python" -m pip install --quiet "maturin==$MATURIN_VERSION"
  target=()
  if [ -n "${PGWIRE_ENCODER_TARGET:-}" ]; then
    rustup target add --toolchain "$RUSTUP_TOOLCHAIN" "$PGWIRE_ENCODER_TARGET"
    target=(--target "$PGWIRE_ENCODER_TARGET")
  fi
  (cd "$CRATE" && "$bin/maturin" build --release --locked -i "$PYTHON" ${target[@]+"${target[@]}"} --out "$OUT")
fi

# One wheel, or the build did not make what the installers need.
wheels=("$OUT"/provisa_pgwire_encoder-*.whl)
if [ ! -f "${wheels[0]}" ]; then
  echo "build-pgwire-encoder-wheel: no provisa_pgwire_encoder wheel in $OUT" >&2
  exit 1
fi
