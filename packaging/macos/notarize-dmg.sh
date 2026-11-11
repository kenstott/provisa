#!/usr/bin/env bash
# Notarize and staple one macOS DMG: submit it to Apple, wait for the verdict, read Apple's log,
# staple the ticket. Run by the "Notarize macOS DMGs" workflow (notarize-macos.yml), one DMG per job.
# The DMG build scripts build and sign; they do not notarize, so a notarization that outlasts its
# limit is run again alone without rebuilding the DMG.
#
# Usage: notarize-dmg.sh <path-to.dmg>
#        notarize-dmg.sh --app <path-to.app>   zip the signed .app, notarize it, staple the .app.
#                                              Core and Container notarize the .app first, build the
#                                              DMG from the stapled .app, then notarize the DMG: the
#                                              .app launches offline once copied out of the DMG.
# Env:   APPLE_NOTARYTOOL_APPLE_ID, APPLE_NOTARYTOOL_PASSWORD, APPLE_NOTARYTOOL_TEAM_ID
#        NOTARY_TIMEOUT_MINUTES  longest wait for Apple's verdict (default 150; the job's limit is longer)
#        NOTARY_POLL_SECONDS     seconds between status checks (default 40)
# Requires: xcrun (notarytool, stapler), python3
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { printf "${CYAN}[notarize-dmg]${NC} %s\n" "$*"; }
ok()   { printf "${GREEN}[notarize-dmg]${NC} %s\n" "$*"; }
err()  { printf "${RED}[notarize-dmg]${NC} %s\n" "$*" >&2; }
# shellcheck source=packaging/macos/notary-lib.sh
source "${SCRIPT_DIR}/notary-lib.sh"

NOTARY_TIMEOUT_MINUTES="${NOTARY_TIMEOUT_MINUTES:-150}"
NOTARY_POLL_SECONDS="${NOTARY_POLL_SECONDS:-40}"

notarize_dmg() {
  local dmg="$1" app="${2:-}"
  if [ -z "$app" ] && [ ! -f "$dmg" ]; then
    err "DMG not found: ${dmg}"
    exit 1
  fi
  if [ -n "$app" ] && [ ! -d "$app" ]; then
    err "App bundle not found: ${app}"
    exit 1
  fi
  if [ -z "${APPLE_NOTARYTOOL_APPLE_ID:-}" ]; then
    info "APPLE_NOTARYTOOL_APPLE_ID not set — skipping notarization."
    return
  fi
  local notary_args=(
    --apple-id "${APPLE_NOTARYTOOL_APPLE_ID}"
    --password "${APPLE_NOTARYTOOL_PASSWORD}"
    --team-id  "${APPLE_NOTARYTOOL_TEAM_ID}"
  )
  # notarytool takes a zip, pkg or dmg: an .app goes in zipped with ditto.
  local submit_path="$dmg" zip_dir=""
  if [ -n "$app" ]; then
    zip_dir="$(mktemp -d)"
    submit_path="${zip_dir}/$(basename "$app" .app)-notarize.zip"
    info "Zipping $(basename "$app") for notarization submission..."
    ditto -c -k --keepParent "$app" "$submit_path"
  fi
  info "Submitting $(basename "$submit_path") for notarization..."
  local err_file submit_out submit_err submit_rc
  err_file="$(mktemp)"
  submit_out=$(xcrun notarytool submit "$submit_path" "${notary_args[@]}" --output-format json 2>"$err_file") \
    && submit_rc=0 || submit_rc=$?
  submit_err=$(cat "$err_file" 2>/dev/null || true)
  rm -f "$err_file"
  if [ -n "$zip_dir" ]; then rm -rf "$zip_dir"; fi
  if [ $submit_rc -ne 0 ]; then
    if printf '%s\n%s' "$submit_out" "$submit_err" | grep -qi "required agreement\|403"; then
      info "WARNING: Notarization skipped — Apple Developer agreement missing or expired (HTTP 403). It will be unsigned."
      return
    fi
    err "notarytool submit failed (exit $submit_rc): $submit_err"
    exit 1
  fi
  local submission_id
  submission_id=$(printf '%s' "$submit_out" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  ok "Submission ID: ${submission_id}"

  # Poll with retry — notarytool --wait has no retry on transient network errors.
  local max_polls=$(( NOTARY_TIMEOUT_MINUTES * 60 / NOTARY_POLL_SECONDS ))
  local poll=0 status=""
  while [ $poll -lt $max_polls ]; do
    status=$(xcrun notarytool info "$submission_id" "${notary_args[@]}" \
               --output-format json 2>/dev/null \
             | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" \
             2>/dev/null || echo "network-error")
    case "$status" in
      Accepted)
        ok "Notarization accepted."
        break ;;
      Invalid|Rejected)
        err "Notarization ${status}:"
        notary_log_check "$submission_id" "${notary_args[@]}"
        exit 1 ;;
      network-error)
        info "  Network error — retrying in ${NOTARY_POLL_SECONDS} s (poll $((poll+1))/${max_polls})..." ;;
      *)
        info "  Status: ${status:-unknown} (poll $((poll+1))/${max_polls})" ;;
    esac
    sleep "$NOTARY_POLL_SECONDS"
    poll=$((poll + 1))
  done
  if [ "$status" != "Accepted" ]; then
    err "Notarization of submission ${submission_id} not accepted after ${NOTARY_TIMEOUT_MINUTES} minutes (last status: ${status:-unknown})."
    exit 1
  fi

  notary_log_check "$submission_id" "${notary_args[@]}"
  local stapled="${app:-$dmg}"
  staple_retry "$stapled"
  ok "$(basename "$stapled") notarized and stapled."
}

main() {
  if [ $# -eq 1 ] && [ "$1" != "--app" ]; then
    notarize_dmg "$1"
  elif [ $# -eq 2 ] && [ "$1" = "--app" ]; then
    notarize_dmg "" "$2"
  else
    err "usage: notarize-dmg.sh <path-to.dmg> | --app <path-to.app>"
    exit 2
  fi
}

main "$@"
