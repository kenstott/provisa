# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3c5e24-af69-4b38-9e0e-5d9f4c6a03fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Rollback-resistant trial elapsed-time evaluation (REQ-1136).

The trial clock must not be extendable by setting the system clock backwards. We persist a monotonic
wall-clock HIGH-WATER MARK (the latest wall-clock ever observed) and evaluate elapsed time as
``max(now, high_water) - first_seen``. Turning the clock back leaves the high-water mark untouched,
so the elapsed measure never shrinks. Fully offline and time-based — no network.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from pathlib import Path

_TRIAL_DAYS = 30
_SECONDS_PER_DAY = 86400


def default_highwater_path() -> Path:
    from provisa.licensing.home import user_home

    return user_home() / "highwater.json"


def advance_default_highwater(now_epoch: float) -> float:
    """Advance the installation's high-water mark and return it.

    In a licensing sandbox (see ``provisa.licensing.home``) the real mark is read, never written:
    the sandbox's own mark is advanced to the latest of the real mark, its own, and now."""
    from provisa.licensing.home import sandbox_dir

    sandbox = sandbox_dir()
    if sandbox is None:
        return update_highwater(default_highwater_path(), now_epoch)
    real = read_highwater(default_highwater_path())
    return update_highwater(sandbox / "highwater.json", max(real, now_epoch))


def read_highwater(path: Path) -> float:
    """The persisted high-water epoch (0.0 if none/unreadable)."""
    try:
        return float(json.loads(path.read_text(encoding="utf-8"))["high_water"])
    except (OSError, ValueError, KeyError, TypeError):
        return 0.0


def update_highwater(path: Path, now_epoch: float) -> float:
    """Advance and persist the high-water mark to ``max(stored, now)``; return the new mark.

    Monotonic non-decreasing: a ``now`` earlier than the stored mark (clock rolled back) does not
    lower it, so the trial cannot be extended by moving the system clock backward (REQ-1136)."""
    hw = max(read_highwater(path), now_epoch)
    path.parent.mkdir(parents=True, exist_ok=True)
    # A temporary file of this writer's own (REQ-1900): every worker of a `--workers N` launch
    # persists the mark at the same moment, and through one shared name a writer's rename took the
    # file out from under the next ("highwater.tmp -> highwater.json: No such file").
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, prefix=path.stem + ".", suffix=".tmp", delete=False
    ) as tmp:
        tmp.write(json.dumps({"high_water": hw}))
    os.replace(tmp.name, path)
    return hw


def _first_seen_epoch(first_seen_iso: str) -> float:
    y, m, d = (int(x) for x in first_seen_iso.split("-"))
    return date(y, m, d).toordinal() * _SECONDS_PER_DAY


def elapsed_days(*, now_epoch: float, high_water: float, first_seen_iso: str) -> float:
    """Rollback-resistant elapsed days since first use: ``max(now, high_water) - first_seen`` (REQ-1136)."""
    effective = max(now_epoch, high_water)
    return (effective - _first_seen_epoch(first_seen_iso)) / _SECONDS_PER_DAY


def trial_expired(*, now_epoch: float, high_water: float, first_seen_iso: str) -> bool:
    """True when ≥ ``_TRIAL_DAYS`` have elapsed (rollback-resistant). Licensing is checked separately
    by the caller; this is purely the time test (REQ-1136)."""
    return (
        elapsed_days(now_epoch=now_epoch, high_water=high_water, first_seen_iso=first_seen_iso)
        >= _TRIAL_DAYS
    )
