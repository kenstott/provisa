# shellcheck shell=bash
# Notarization helpers shared by build-dmg.sh, build-dmg-obs.sh and build-dmg-demo.sh.
# Sourced, not run: the caller defines info/ok/err and runs under `set -euo pipefail`.

# Apple's notarization log for a submission. It is printed for every submission, accepted or
# not, and any issue it lists fails the build: an accepted submission can still carry issues
# (a warning today is a rejection after Apple tightens a rule), and nobody read them before.
# Fetching the log is a network call to Apple; a failed fetch is retried a bounded number of
# times, logged per attempt, and fails the build at the limit.
NOTARY_LOG_ATTEMPTS="${NOTARY_LOG_ATTEMPTS:-5}"
notary_log_check() {
  local submission_id="$1"; shift
  local attempt log_json="" log_rc=1
  for ((attempt = 1; attempt <= NOTARY_LOG_ATTEMPTS; attempt++)); do
    info "Fetching Apple's notarization log for ${submission_id} (attempt ${attempt}/${NOTARY_LOG_ATTEMPTS})..."
    log_json=$(xcrun notarytool log "$submission_id" "$@") && log_rc=0 || log_rc=$?
    if [ "$log_rc" -eq 0 ]; then break; fi
    info "  notarytool log attempt ${attempt} failed (exit ${log_rc})."
    if [ "$attempt" -lt "$NOTARY_LOG_ATTEMPTS" ]; then sleep 20; fi
  done
  if [ "$log_rc" -ne 0 ]; then
    err "notarytool log failed ${NOTARY_LOG_ATTEMPTS} times for ${submission_id} (last exit ${log_rc})."
    exit 1
  fi
  printf '%s\n' "$log_json"
  local issue_count
  # Apple's log JSON carries "issues": null when there are none, a list otherwise.
  issue_count=$(printf '%s' "$log_json" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('issues') or []))")
  if [ "$issue_count" -ne 0 ]; then
    err "Apple's notarization log for ${submission_id} lists ${issue_count} issue(s) (above)."
    exit 1
  fi
  ok "Apple's notarization log for ${submission_id} lists no issues."
}

# `stapler staple` fetches the ticket from Apple's CloudKit ticket-delivery service
# (api.apple-cloudkit.com/.../com.apple.gk.ticket-delivery/.../records/lookup). That connection
# drops on GitHub's macOS runners: NSURLErrorDomain -1005 "The network connection was lost",
# then "The staple and validate action failed! Error 68". It hit the Core DMG on
# v0.1.0-alpha.505 (attempt 1) and the Container DMG on v0.1.0-alpha.506 (attempts 1 and 2),
# each time for a bundle with the same two signed code objects that stapled fine in the
# sibling job, so it is the connection, not the bundle. The ticket exists once notarization is
# accepted; stapling again is safe. Bounded, logged per attempt, fails the build at the limit.
STAPLE_ATTEMPTS="${STAPLE_ATTEMPTS:-5}"
staple_retry() {
  local target="$1"
  local attempt rc=1
  for ((attempt = 1; attempt <= STAPLE_ATTEMPTS; attempt++)); do
    info "Stapling $(basename "$target") (attempt ${attempt}/${STAPLE_ATTEMPTS})..."
    xcrun stapler staple "$target" && rc=0 || rc=$?
    if [ "$rc" -eq 0 ]; then return 0; fi
    info "  stapler attempt ${attempt} failed (exit ${rc})."
    if [ "$attempt" -lt "$STAPLE_ATTEMPTS" ]; then sleep $((attempt * 30)); fi
  done
  err "stapler staple failed ${STAPLE_ATTEMPTS} times for ${target} (last exit ${rc})."
  exit 1
}
