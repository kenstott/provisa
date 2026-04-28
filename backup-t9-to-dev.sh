#!/usr/bin/env bash
# Backs up all data from /Volumes/T9 to /Volumes/dev/T9-backup
# Safe to re-run: rsync only transfers new/changed files.

set -euo pipefail

SRC="/Volumes/T9/"
DEST="/Volumes/dev"
LOG="/Volumes/dev/T9-backup.log"

if [ ! -d "/Volumes/T9" ]; then
  echo "ERROR: /Volumes/T9 is not mounted." >&2
  exit 1
fi
if [ ! -d "/Volumes/dev" ]; then
  echo "ERROR: /Volumes/dev is not mounted." >&2
  exit 1
fi

mkdir -p "$DEST"

echo "Starting backup: $SRC → $DEST"
echo "Log: $LOG"
echo "Started: $(date)" | tee "$LOG"

rsync -avh --progress \
  --exclude='._*' \
  --exclude='.Spotlight-V100' \
  --exclude='.Trashes' \
  --exclude='.fseventsd' \
  --exclude='.DS_Store' \
  --exclude='.venv/' \
  --exclude='node_modules/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  --exclude='Caches/' \
  "$SRC" "$DEST" \
  2>&1 | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "Finished: $(date)" | tee -a "$LOG"
echo "Done. Backup is at $DEST"
