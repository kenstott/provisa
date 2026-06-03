#!/usr/bin/env bash
# Copy the repo's NON-generated files to a target volume (e.g. an SSD), excluding
# everything that is regenerated on the destination (.venv, node_modules, build
# output, caches, logs, demo data files, certs, downloaded jars).
#
# Dry-run by default — prints exactly what would be copied. Pass --go to execute.
# Never deletes the SOURCE; removing the old copy is a deliberate, separate step.
#
# Intended flow:
#   1. git clone the repo onto the SSD first — that captures all TRACKED files (~99%):
#        git clone /Volumes/dev/Users/.../provisa /Volumes/gov/Users/.../provisa
#        (cd DEST && git remote set-url origin <your github url>)   # fix origin off the local path
#   2. Run this script with --update to fill in the gitignored-but-non-generated files
#      (configs, secrets, etc.) that git didn't carry, without re-copying the clone.
#
# Modes (same exclude list — generated dirs are never touched):
#   default   : copy/update changed files onto DEST (additive; never removes on DEST)
#   --update  : only copy files MISSING on DEST or NEWER on SRC (rsync -u). After a fresh
#               clone, cloned files have mtime ~now, so this copies only the gap.
#   --mirror  : also delete files on DEST that no longer exist on SRC, so DEST becomes
#               an exact mirror of SRC's non-generated tree. Use for the FINAL catch-up
#               sync once you're cutting over. Excluded paths on DEST (the rebuilt
#               .venv, node_modules, etc.) are protected and never deleted.
#
# Usage:
#   ./migrate-to-ssd.sh [DEST] --update         # dry run, fill gaps after a clone
#   ./migrate-to-ssd.sh [DEST] --update --go    # apply
#   ./migrate-to-ssd.sh [DEST] --mirror --go    # final exact sync (with deletes)
#
# Default DEST preserves the directory structure: a repo at
#   /Volumes/dev/Users/.../PycharmProjects/provisa
# is copied to
#   /Volumes/gov/Users/.../PycharmProjects/provisa
#
# After a successful --go, regenerate on the destination:
#   cd DEST
#   python3.12 -m venv .venv
#   .venv/bin/pip install -e . -e provisa-client -e vendor/buenavista   # + any extras
#   (cd provisa-ui && npm ci)
#   ./start-ui-install.sh --demo          # regenerates demo files, certs, jar
# Then, once verified, remove the old copy:
#   rm -rf "<this repo path>"

set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"

# Target volume to mirror the structure onto (override DEST_VOL or pass DEST explicitly).
DEST_VOL="${DEST_VOL:-/Volumes/gov}"
# Relative path of the repo under its current /Volumes/<vol> root, preserved on the target.
SRC_REL="${SRC#/Volumes/*/}"
DEFAULT_DEST="$DEST_VOL/$SRC_REL"

DEST="$DEFAULT_DEST"
MODE="dry"
MIRROR=false
UPDATE_ONLY=false
for a in "$@"; do
  case "$a" in
    --go) MODE="go" ;;
    --mirror) MIRROR=true ;;
    --update) UPDATE_ONLY=true ;;
    --*) ;;
    *) DEST="$a" ;;
  esac
done

# Regenerated on the destination — must NOT be copied.
EXCLUDES=(
  ".venv/"
  "node_modules/"
  "__pycache__/"
  "*.pyc"
  "*.pyo"
  "*.egg-info/"
  ".pytest_cache/"
  ".mypy_cache/"
  ".ruff_cache/"
  ".vite/"
  ".vite-temp/"
  "dist/"
  "build/"
  "coverage/"
  ".nyc_output/"
  "playwright-report/"
  "test-results/"
  "/.logs/"
  "demo/files/*.parquet"
  "demo/files/*.sqlite"
  "/config/pgwire.crt"
  "/config/pgwire.key"
  "/config/*.bak"
  "/lib/calcite-govdata-all.jar"
  ".DS_Store"
  "._*"
)

# --- preflight checks ---
# Verify the target VOLUME is mounted; mkdir -p creates the mirrored sub-path.
case "$DEST" in
  /Volumes/*) VOL_ROOT="/Volumes/$(printf '%s' "$DEST" | cut -d/ -f3)" ;;
  *)          VOL_ROOT="/" ;;
esac
if [ ! -d "$VOL_ROOT" ]; then
  echo "ERROR: target volume $VOL_ROOT is not mounted" >&2
  exit 1
fi
case "$DEST" in
  "$SRC"|"$SRC"/*) echo "ERROR: DEST must not be inside SRC ($SRC)" >&2; exit 1 ;;
esac

RSYNC_ARGS=(-a --human-readable --stats)
for e in "${EXCLUDES[@]}"; do RSYNC_ARGS+=(--exclude "$e"); done
# --delete removes DEST files missing from SRC; excluded paths are protected by default
# (no --delete-excluded), so the rebuilt .venv/node_modules on DEST are never deleted.
[ "$MIRROR" = true ] && RSYNC_ARGS+=(--delete)
# -u: skip files that are newer (or same mtime) on DEST — copies only the post-clone gap.
[ "$UPDATE_ONLY" = true ] && RSYNC_ARGS+=(--update)
[ "$MODE" = "dry" ] && RSYNC_ARGS+=(--dry-run --itemize-changes)

echo "SRC : $SRC"
echo "DEST: $DEST"
echo "SYNC: $([ "$MIRROR" = true ] && echo "MIRROR (--delete stale on DEST)" || echo "ADDITIVE (no deletes)")$([ "$UPDATE_ONLY" = true ] && echo " +UPDATE-ONLY (newer/missing)")"
echo "MODE: $([ "$MODE" = go ] && echo "EXECUTE" || echo "DRY RUN (pass --go to apply)")"
echo "Excludes: ${EXCLUDES[*]}"
echo

[ "$MODE" = "go" ] && mkdir -p "$DEST"
# Trailing slash on SRC copies its contents into DEST.
rsync "${RSYNC_ARGS[@]}" "$SRC"/ "$DEST"/

echo
if [ "$MODE" = "dry" ]; then
  echo "Dry run complete. Re-run with --go to perform the copy."
else
  echo "Copy complete → $DEST"
  echo "Next: rebuild .venv (python3.12 -m venv + pip install -e ...), 'npm ci' in provisa-ui,"
  echo "      run ./start-ui-install.sh --demo, verify, then 'rm -rf $SRC'."
fi
