# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2b4d13-9f58-4a27-8d0d-4c8e3b5f92ec
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tamper-evident, reinstall-surviving first-use anchors (REQ-1135).

On first startup with no existing anchor, today's date is recorded as ``first_seen``. The payload
``{first_seen, machine_id}`` is Ed25519-signed and written to MULTIPLE locations OUTSIDE the install
directory. On every startup all anchors are read, the EARLIEST valid ``first_seen`` wins, and any
missing / tampered / out-of-date anchor is rewritten with that value. So deleting the app, or one
anchor, does not reset the clock — a surviving anchor re-seeds the rest. Fully airgapped: no network.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from provisa.licensing.keys import sign_anchor, verify_anchor_sig


@dataclass(frozen=True)
class Anchor:  # REQ-1135
    first_seen: str  # ISO date (YYYY-MM-DD)
    machine_id: str


def _canonical(anchor: Anchor) -> bytes:
    """Deterministic bytes signed/verified for an anchor (stable key order)."""
    return json.dumps(
        {"first_seen": anchor.first_seen, "machine_id": anchor.machine_id},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def default_anchor_paths() -> list[Path]:
    """The default anchor locations, all OUTSIDE the install dir (REQ-1135).

    ``~/.provisa/anchor.json`` (primary), the OS-native per-user data dir (secondary store), and a
    dotted secondary marker. Multiple independent locations so no single deletion resets first-use."""
    import platformdirs

    home = Path.home()
    os_store = Path(platformdirs.user_data_dir("provisa", "provisa"))
    return [
        home / ".provisa" / "anchor.json",
        os_store / "anchor.json",
        home / ".provisa" / ".anchor.marker",
    ]


def read_anchor(path: Path, machine_id: str) -> Anchor | None:
    """Read + validate one anchor file: None if absent, unparseable, mis-signed, or wrong machine."""
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        data = json.loads(raw)
        anchor = Anchor(first_seen=data["first_seen"], machine_id=data["machine_id"])
        sig = bytes.fromhex(data["sig"])
    except (ValueError, KeyError, TypeError):
        return None
    if anchor.machine_id != machine_id:
        return None  # an anchor from another machine is not ours — ignore it
    if not verify_anchor_sig(_canonical(anchor), sig):
        return None  # tampered
    return anchor


def write_anchor(path: Path, anchor: Anchor) -> None:
    """Sign and atomically write an anchor to ``path`` (creating parent dirs)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    sig = sign_anchor(_canonical(anchor))
    payload = {
        "first_seen": anchor.first_seen,
        "machine_id": anchor.machine_id,
        "sig": sig.hex(),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    tmp.replace(path)


def reconcile_first_seen(
    *, machine_id: str, today_iso: str, paths: list[Path] | None = None
) -> str:
    """Resolve the effective first-use date across all anchors, healing missing ones (REQ-1135).

    Reads every anchor; the EARLIEST valid ``first_seen`` wins (min), else ``today_iso`` on a truly
    first run. Then rewrites any location whose anchor is absent/tampered/not-earliest so all agree —
    a surviving anchor re-seeds deleted ones, so uninstalling does not reset the trial clock."""
    locations = paths if paths is not None else default_anchor_paths()
    valid = [a for p in locations if (a := read_anchor(p, machine_id)) is not None]

    first_seen = min((a.first_seen for a in valid), default=today_iso)
    canonical = Anchor(first_seen=first_seen, machine_id=machine_id)

    for p in locations:
        existing = read_anchor(p, machine_id)
        if existing is None or existing.first_seen != first_seen:
            write_anchor(p, canonical)
    return first_seen
