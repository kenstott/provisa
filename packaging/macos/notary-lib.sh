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
  # Apple's log JSON carries "issues": null when there are none, a list otherwise. One class is
  # allowed (maintainer 2026-11-07): a WARNING that an archive inside a bundled Linux container image
  # (<dmg>/images/<image>.tar.gz/...) could not be unpacked. Those files run inside Docker, never on
  # macOS, so notarization does not apply to them. Each allowed warning is printed with that reason;
  # every other issue, and any error, fails the build.
  issue_count=$(printf '%s' "$log_json" | python3 -c "
import json, re, sys
issues = json.load(sys.stdin).get('issues') or []
image_archive = re.compile(r'\.dmg/images/[^/]+\.tar\.gz/')
blocking = 0
for issue in issues:
    allowed = (
        issue.get('severity') == 'warning'
        and image_archive.search(issue.get('path') or '')
        and 'could not be unpacked' in (issue.get('message') or '')
    )
    if allowed:
        print('allowed (Linux container image content, not executed on macOS): ' + issue['path'], file=sys.stderr)
    else:
        blocking += 1
print(blocking)
")
  if [ "$issue_count" -ne 0 ]; then
    err "Apple's notarization log for ${submission_id} lists ${issue_count} blocking issue(s) (above)."
    exit 1
  fi
  ok "Apple's notarization log for ${submission_id} lists no blocking issues."
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

# `notarytool submit` uploads the file and returns the submission id; it has no network timeout of
# its own. On v0.1.0-alpha.511 (run 45059453430) the Demo DMG's submit sent at 20:00:19Z never
# returned and the job's 45-minute limit cancelled it at 20:44Z, leaving an orphan notarytool, while
# the three sibling jobs' submits answered in 2-4 s. A submit that completes takes 3-22 s
# (v0.1.0-alpha.509, run 42951512721: Core 3 s, Demo 9 s, Obs 22 s), so one attempt is capped at
# NOTARY_SUBMIT_TIMEOUT seconds and a stalled attempt is killed and sent again, a bounded number of
# times, logged per attempt; the build fails at the limit. A submit that exits, with any status, is
# returned to the caller as is. Results: NOTARY_SUBMIT_OUT, NOTARY_SUBMIT_ERR, NOTARY_SUBMIT_RC.
# macOS ships no coreutils `timeout`, hence the watchdog.
NOTARY_SUBMIT_TIMEOUT="${NOTARY_SUBMIT_TIMEOUT:-300}"
NOTARY_SUBMIT_ATTEMPTS="${NOTARY_SUBMIT_ATTEMPTS:-3}"
notary_submit() {
  local target="$1"; shift
  local attempt pid waited out_file err_file
  out_file=$(mktemp)
  err_file=$(mktemp)
  for ((attempt = 1; attempt <= NOTARY_SUBMIT_ATTEMPTS; attempt++)); do
    xcrun notarytool submit "$target" "$@" --output-format json >"$out_file" 2>"$err_file" &
    pid=$!
    waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$NOTARY_SUBMIT_TIMEOUT" ]; do
      sleep 2
      waited=$((waited + 2))
    done
    if kill -0 "$pid" 2>/dev/null; then
      pkill -9 -P "$pid" 2>/dev/null || true
      kill -9 "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      info "notarytool submit attempt ${attempt}/${NOTARY_SUBMIT_ATTEMPTS} gave no answer in ${NOTARY_SUBMIT_TIMEOUT}s (stalled connection to Apple), killed."
      continue
    fi
    wait "$pid" && NOTARY_SUBMIT_RC=0 || NOTARY_SUBMIT_RC=$?
    NOTARY_SUBMIT_OUT=$(cat "$out_file")
    NOTARY_SUBMIT_ERR=$(cat "$err_file")
    rm -f "$out_file" "$err_file"
    return 0
  done
  rm -f "$out_file" "$err_file"
  err "notarytool submit gave no answer in ${NOTARY_SUBMIT_ATTEMPTS} attempts of ${NOTARY_SUBMIT_TIMEOUT}s for ${target}."
  exit 1
}
