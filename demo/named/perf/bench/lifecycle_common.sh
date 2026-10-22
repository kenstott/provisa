#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# What bench_lifecycle.sh and seed_lifecycle.sh share, sourced by both: the seed hash and its image,
# each device's setup log, and the wait for every device's ready marker. They set PROJECT, ZONE,
# COMMIT, READY_TIMEOUT, RUN_DIR and DEVICES (the device names, space-separated) first.

# The seed hash at the commit (seed_inputs.py: the files and settings that decide the seeded data)
# and the stored image named after it.
seed_image_for() {  # seed_image_for <repo root> <here>; sets SEED_HASH and SEED_IMAGE
  SEED_HASH="$("$1/.venv/bin/python3" "$2/seed_inputs.py" --commit "$COMMIT")"
  SEED_IMAGE="provisa-perf-seed-${SEED_HASH:0:12}"
}

# present, or missing when GCE says the image was not found; any other answer is an error.
seed_image_state() {
  local described
  if described="$(gcloud compute images describe "$SEED_IMAGE" --project "$PROJECT" \
      --format='value(name)' 2>&1)"; then
    echo present
  elif [[ "$described" == *"was not found"* ]]; then
    echo missing
  else
    echo "seed image $SEED_IMAGE could not be looked up: $described" >&2
    return 1
  fi
}

# Each device's setup log (its startup script's journal) is copied into the run directory before
# destroy, so a device that never became ready names the step that failed.
copy_journals() {
  local device
  for device in $DEVICES; do
    gcloud compute ssh "provisa-perf-$device" --zone "$ZONE" --project "$PROJECT" --quiet \
        --command 'sudo journalctl -t provisa-perf-startup --no-pager' \
        >"$RUN_DIR/startup-$device.log" 2>&1 \
      || echo "provisa-perf-$device: its setup log was not copied (see $RUN_DIR/startup-$device.log)"
  done
}

# Every device's startup script writes the commit to its ready marker when every step has passed
# (on the seed job's backends device, after the seed). Each poll also refreshes the device's idle
# hold (vm_idle_shutdown.sh), so a device that is ready and waiting for the others is not powered
# off.
READY_POLL='sudo touch /var/lib/provisa-idle/hold; cat /var/lib/provisa-perf/ready 2>/dev/null'
wait_ready() {
  local device deadline
  echo "=== waiting for the devices to be ready (up to ${READY_TIMEOUT}s) ==="
  deadline=$(( $(date +%s) + READY_TIMEOUT ))
  for device in $DEVICES; do
    until [ "$(gcloud compute ssh "provisa-perf-$device" --zone "$ZONE" --project "$PROJECT" --quiet \
        --command "$READY_POLL" 2>/dev/null)" = "$COMMIT" ]; do
      if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "device provisa-perf-$device was not ready at $COMMIT within ${READY_TIMEOUT}s;" \
          "its setup log is copied to $RUN_DIR/startup-$device.log before destroy"
        exit 1
      fi
      sleep 30
    done
    echo "provisa-perf-$device ready"
  done
}
