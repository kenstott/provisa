#!/usr/bin/env bash
# REQ-2194: write provisa/_encoders.THIRD_PARTY_NOTICES, the licence notices of every Rust crate
# linked into provisa._encoders, with cargo-about (cargo install cargo-about --locked --version
# 0.8.4) from rust/encoders' Cargo.lock. The file ships in every provisa wheel and installer beside
# the module. Run it after any change to that lock: its first line names the lock it was written
# from, and tests/unit/test_pgwire_encoder_workflow.py fails while the two differ.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
CRATE="$REPO/rust/encoders"
OUT="$REPO/provisa/_encoders.THIRD_PARTY_NOTICES"
lock_sha="$(shasum -a 256 "$CRATE/Cargo.lock" | cut -d' ' -f1)"
body="$(mktemp)"
trap 'rm -f "$body"' EXIT
(cd "$CRATE" && cargo about generate --locked -c about.toml about.hbs -o "$body")
{
  echo "rust/encoders/Cargo.lock sha256: $lock_sha"
  echo
  cat "$body"
} > "$OUT"
echo "wrote $OUT"
