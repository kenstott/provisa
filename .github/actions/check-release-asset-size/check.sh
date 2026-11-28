#!/usr/bin/env bash
# Fail when any given file is too large to be a GitHub release asset: `gh release upload` refuses
# 2147483648 bytes (2 GiB) or more with HTTP 422 "size must be less than 2147483648" (alpha.513's
# core images). Run on every packaged asset before it is uploaded or kept in a dry run, so a dry
# run, which attaches nothing to a release, fails where the release would.
#
# Usage: check.sh <file>...
set -euo pipefail

LIMIT=2147483648

[ $# -gt 0 ] || { echo "usage: $0 <file>..." >&2; exit 2; }
too_big=0
for f in "$@"; do
  [ -f "$f" ] || { echo "::error::no release asset file: $f"; too_big=1; continue; }
  size="$(wc -c < "$f" | tr -d ' ')"
  if [ "$size" -ge "$LIMIT" ]; then
    echo "::error::$f is $size bytes; a GitHub release asset must be under $LIMIT bytes (2 GiB)"
    too_big=1
  else
    printf '%s: %s bytes (under %s)\n' "$f" "$size" "$LIMIT"
  fi
done
exit "$too_big"
