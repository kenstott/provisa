#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: f3a8c1e6-2b9d-4a57-8e0c-6d1f4b2a97ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Issue a signed Provisa license (REQ-1138, REQ-1139) — the provisa.dev side of licensing.

Mirrors the exact canonicalization `provisa.licensing.license.verify_license` checks: this is the
only tool that produces a `license.json` a Provisa install will accept. The signing key is the
private half of the Ed25519 keypair whose public half is embedded in `provisa/licensing/keys.py`
(or pinned via `PROVISA_LICENSE_PUBKEY`) — it must never be committed to this repo. Pass it via
`PROVISA_LICENSE_PRIVKEY` (64 hex chars, the 32-byte seed) so it never appears in shell history or
process listings via an argument.

Usage:
    PROVISA_LICENSE_PRIVKEY=<64-hex-seed> python3 scripts/issue_license.py \\
        --company Acme --position "VP Eng" --role admin \\
        --first-name Ada --last-name Lovelace --email ada@acme.test \\
        --machine-id <from their `provisa license status`> \\
        --out ada-license.json

The output round-trips through `provisa.licensing.license.verify_license` before being written, so a
license this script emits is guaranteed valid for the machine id it was issued for.
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from provisa.licensing.license import _canonical_payload, verify_license  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--company", required=True)
    p.add_argument("--position", required=True)
    p.add_argument("--role", required=True)
    p.add_argument("--first-name", required=True)
    p.add_argument("--last-name", required=True)
    p.add_argument("--email", required=True)
    p.add_argument(
        "--machine-id", required=True, help="from the requestor's `provisa license status`"
    )
    p.add_argument("--phone", default=None, help="optional")
    p.add_argument("--issued-at", default=None, help="ISO date; defaults to today")
    p.add_argument("--out", required=True, type=Path, help="path to write the signed license.json")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    import os

    privkey_hex = os.environ.get("PROVISA_LICENSE_PRIVKEY")
    if not privkey_hex:
        print(
            "PROVISA_LICENSE_PRIVKEY is not set — refusing to issue an unsigned license.",
            file=sys.stderr,
        )
        return 1
    priv = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(privkey_hex))

    payload = {
        "company": args.company,
        "position": args.position,
        "role": args.role,
        "first_name": args.first_name,
        "last_name": args.last_name,
        "email": args.email,
        "machine_id": args.machine_id,
        "issued_at": args.issued_at or datetime.date.today().isoformat(),
    }
    if args.phone:
        payload["phone"] = args.phone

    payload["sig"] = priv.sign(_canonical_payload(payload)).hex()

    pub_hex = priv.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()
    check = verify_license(payload, machine_id=args.machine_id)
    if not check.valid:
        print(f"Signed license failed its own verification: {check.reason}", file=sys.stderr)
        return 1

    args.out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {args.out} for machine_id={args.machine_id}")
    print(
        f"Signing public key (confirm it matches the app's embedded/PROVISA_LICENSE_PUBKEY): {pub_hex}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
