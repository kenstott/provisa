#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e9c21-8f3a-4d6e-9c05-1a7d3f8b6e92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generate the ONE production Ed25519 license-signing keypair (REQ-1138, REQ-1793).

Run this ONCE, offline, and never commit its output. It prints three things:

1. The PUBLIC key hex — paste this into provisa/licensing/keys.py's
   _DEFAULT_LICENSE_PUBKEY_HEX. It ships in every Provisa install and is what verifies a license
   offline; it is not secret.
2. The PRIVATE key seed hex — paste into PROVISA_LICENSE_PRIVKEY for local, manual
   scripts/issue_license.py use. Keep this offline (a password manager, not this repo).
3. The PRIVATE key as PKCS8 DER, base64 — paste into the Cloudflare Pages secret
   LICENSE_PRIVATE_KEY_PKCS8_B64 (site/functions/api/register/confirm.js's automated signer):
     wrangler pages secret put LICENSE_PRIVATE_KEY_PKCS8_B64 --project-name provisa-dev

Both (2) and (3) sign for the SAME public key in (1) — verified before printing anything by
round-tripping a throwaway payload through Provisa's own verify_license.
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from provisa.licensing.license import _canonical_payload, verify_license  # noqa: E402


def main() -> int:
    priv = Ed25519PrivateKey.generate()
    pub_hex = priv.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()
    seed_hex = priv.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()).hex()
    pkcs8_b64 = base64.b64encode(
        priv.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
    ).decode()

    payload = {
        "company": "self-test",
        "position": "self-test",
        "role": "self-test",
        "first_name": "self-test",
        "last_name": "self-test",
        "email": "self-test@example.test",
        "machine_id": "self-test",
        "issued_at": "2026-01-01",
    }
    payload["sig"] = priv.sign(_canonical_payload(payload)).hex()
    # verify_license reads the trust anchor from PROVISA_LICENSE_PUBKEY (or the app's embedded
    # default) at call time — pin it to the key just generated so the self-test actually proves
    # this keypair, not whatever key happens to be embedded/exported in this shell already.
    os.environ["PROVISA_LICENSE_PUBKEY"] = pub_hex
    check = verify_license(payload, machine_id="self-test")
    if not check.valid:
        print(f"Self-test failed: {check.reason} — do not use this keypair.", file=sys.stderr)
        return 1

    print("Self-test signature verified OK.\n")
    print("1) PUBLIC key (provisa/licensing/keys.py _DEFAULT_LICENSE_PUBKEY_HEX):")
    print(f"   {pub_hex}\n")
    print("2) PRIVATE key seed hex (PROVISA_LICENSE_PRIVKEY, for manual issue_license.py):")
    print(f"   {seed_hex}\n")
    print("3) PRIVATE key PKCS8 base64 (Cloudflare secret LICENSE_PRIVATE_KEY_PKCS8_B64):")
    print(f"   {pkcs8_b64}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
