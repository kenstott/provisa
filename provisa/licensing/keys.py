# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1a3c92-8e47-4f16-9c0c-3b7e2a4f81db
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Embedded Ed25519 public keys and signing helpers (REQ-1135, REQ-1138).

Two independent keys:

- The LICENSE public key belongs to provisa.dev, which holds the matching private key and issues
  license files. The application embeds ONLY this public key and verifies licenses fully offline
  (REQ-1138). It is overridable via ``PROVISA_LICENSE_PUBKEY`` (hex) so a deployment or a test can
  pin its own issuer key without editing source.
- The ANCHOR keypair is app-local (both halves embedded). It only makes the first-use anchors
  tamper-EVIDENT — casual edits break the signature — not tamper-proof; a fully local, airgapped
  mechanism cannot defend against a determined local attacker, and does not try to (REQ-1135).
"""

from __future__ import annotations

import os

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

# provisa.dev license issuer public key (placeholder until the production key is minted; override in
# a deployment/test via PROVISA_LICENSE_PUBKEY). 32-byte Ed25519 public key, hex-encoded.
_DEFAULT_LICENSE_PUBKEY_HEX = (
    "0000000000000000000000000000000000000000000000000000000000000000"
)

# App-local anchor keypair (tamper-evidence only — not a secret boundary, see module docstring).
_ANCHOR_SEED_HEX = "9d61b19deffe6a07d4b8f6b8b3c2a1f0e5d4c3b2a1908f7e6d5c4b3a29180706"


def license_public_key() -> Ed25519PublicKey:
    """The embedded (or env-overridden) license issuer public key (REQ-1138)."""
    hex_key = os.environ.get("PROVISA_LICENSE_PUBKEY", _DEFAULT_LICENSE_PUBKEY_HEX)
    return Ed25519PublicKey.from_public_bytes(bytes.fromhex(hex_key))


def verify_license_sig(message: bytes, signature: bytes) -> bool:
    """Verify a license signature against the embedded license public key, offline (REQ-1138)."""
    try:
        license_public_key().verify(signature, message)
    except InvalidSignature:
        return False
    return True


def _anchor_private_key() -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(bytes.fromhex(_ANCHOR_SEED_HEX))


def sign_anchor(message: bytes) -> bytes:
    """Sign an anchor payload with the app-local anchor key (REQ-1135)."""
    return _anchor_private_key().sign(message)


def verify_anchor_sig(message: bytes, signature: bytes) -> bool:
    """Verify an anchor payload signature; a broken signature = a tampered anchor (REQ-1135)."""
    try:
        _anchor_private_key().public_key().verify(signature, message)
    except InvalidSignature:
        return False
    return True
