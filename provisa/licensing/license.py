# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4d6f35-b07a-4c49-8f0f-6e0a5d7b14fe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Offline Ed25519 license verification and application (REQ-1138, REQ-1139).

A license file is an Ed25519-signed token issued by provisa.dev (which holds the private key). The
app embeds only the public key (keys.py) and verifies ENTIRELY OFFLINE — Provisa never phones home.
The license is a one-time registration capture, the alternative to telemetry; applying it enables no
telemetry, reporting, or ongoing collection.

A valid license requires (a) a signature that verifies against the embedded public key AND (b) a
``machine_id`` equal to the local stable machine id — so a license issued for one machine does not
silence the nag on another. On success it is stored at ``~/.provisa/license.json``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from provisa.licensing.keys import verify_license_sig
from provisa.licensing.machine_id import stable_machine_id

# The payload fields the registration captures (REQ-1137/1138). ``phone`` is optional.
_REQUIRED_FIELDS = (
    "company",
    "position",
    "role",
    "first_name",
    "last_name",
    "email",
    "machine_id",
    "issued_at",
)


@dataclass(frozen=True)
class License:  # REQ-1138
    payload: dict
    valid: bool
    reason: str = ""

    @property
    def machine_id(self) -> str:
        return str(self.payload.get("machine_id", ""))


def default_license_path() -> Path:
    return Path.home() / ".provisa" / "license.json"


def _canonical_payload(payload: dict) -> bytes:
    """Deterministic bytes the issuer signed — payload sans the ``sig`` field, stable key order."""
    body = {k: v for k, v in payload.items() if k != "sig"}
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def verify_license(data: dict, *, machine_id: str | None = None) -> License:
    """Verify a license dict offline: signature + machine_id match (REQ-1138, REQ-1139).

    ``machine_id`` defaults to the local stable id. Returns a ``License`` with ``valid`` False and a
    ``reason`` on any failure (missing fields, bad signature, or machine mismatch) — never raises for
    a merely-invalid license, so callers can keep nagging rather than crash."""
    local_id = machine_id if machine_id is not None else stable_machine_id()
    missing = [f for f in _REQUIRED_FIELDS if f not in data]
    if missing:
        return License(data, False, f"missing fields: {', '.join(missing)}")
    sig_hex = data.get("sig")
    if not sig_hex:
        return License(data, False, "no signature")
    try:
        sig = bytes.fromhex(sig_hex)
    except ValueError:
        return License(data, False, "signature not hex")
    if not verify_license_sig(_canonical_payload(data), sig):
        return License(data, False, "signature does not verify against the embedded public key")
    if data.get("machine_id") != local_id:
        return License(data, False, "license machine_id does not match this installation")
    return License(data, True)


def load_license(path: Path | None = None, *, machine_id: str | None = None) -> License | None:
    """Load + verify the license at ``path`` (default ~/.provisa/license.json). None if absent."""
    p = path if path is not None else default_license_path()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return verify_license(data, machine_id=machine_id)


def apply_license(
    source: Path, *, dest: Path | None = None, machine_id: str | None = None
) -> License:
    """Verify a license file and, if valid, store it at the default location (REQ-1139).

    A signature failure or machine_id mismatch is rejected (returns an invalid ``License``) and NOT
    stored, so the nag continues. A valid, matching license is written to ``dest`` and permanently
    silences the nag."""
    try:
        data = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return License({}, False, f"cannot read license file: {exc}")
    result = verify_license(data, machine_id=machine_id)
    if not result.valid:
        return result
    target = dest if dest is not None else default_license_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(data), encoding="utf-8")
    return result
