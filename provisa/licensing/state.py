# Copyright (c) 2026 Kenneth Stott
# Canary: a06f8b57-d29c-4e6b-8b11-8a2c7f9d36b0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The unified licensing state: anchors + monotonic clock + license → nag-or-not (REQ-1135–1139).

``evaluate`` is the one call the app makes at startup and each surface consults: it reconciles the
first-use anchors, advances the rollback-resistant high-water mark, checks for a valid matching
license, and decides whether the post-trial nag applies. Pure of protocol concerns — a surface reads
``should_nag``/``nag_text`` and emits through its own out-of-band channel (REQ-1137).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from provisa.licensing import anchors, monotonic
from provisa.licensing.license import load_license
from provisa.licensing.machine_id import stable_machine_id
from provisa.licensing.nag import nag_message


@dataclass(frozen=True)
class LicensingState:  # REQ-1135–1139
    machine_id: str
    first_seen: str
    elapsed_days: float
    trial_expired: bool
    licensed: bool
    license_reason: str

    @property
    def should_nag(self) -> bool:
        """Nag only when the trial has expired AND no valid license is present (REQ-1137/1139)."""
        return self.trial_expired and not self.licensed

    @property
    def nag_text(self) -> str:
        return nag_message(self.machine_id)


def evaluate(
    *,
    now_epoch: float,
    today_iso: str,
    anchor_paths: list[Path] | None = None,
    highwater_path: Path | None = None,
    license_path: Path | None = None,
) -> LicensingState:
    """Reconcile anchors + high-water, check the license, and decide the nag (REQ-1135–1139).

    Fully offline. ``now_epoch``/``today_iso`` are injected (the caller stamps the wall clock) so this
    is deterministic and testable; scripts that forbid ``Date.now`` pass explicit values."""
    machine_id = stable_machine_id()
    first_seen = anchors.reconcile_first_seen(
        machine_id=machine_id, today_iso=today_iso, paths=anchor_paths
    )
    hw_path = highwater_path if highwater_path is not None else monotonic.default_highwater_path()
    high_water = monotonic.update_highwater(hw_path, now_epoch)
    expired = monotonic.trial_expired(
        now_epoch=now_epoch, high_water=high_water, first_seen_iso=first_seen
    )
    lic = load_license(license_path, machine_id=machine_id)
    licensed = lic is not None and lic.valid
    reason = "" if licensed else (lic.reason if lic is not None else "no license present")
    return LicensingState(
        machine_id=machine_id,
        first_seen=first_seen,
        elapsed_days=monotonic.elapsed_days(
            now_epoch=now_epoch, high_water=high_water, first_seen_iso=first_seen
        ),
        trial_expired=expired,
        licensed=licensed,
        license_reason=reason,
    )
