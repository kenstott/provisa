# Copyright (c) 2026 Kenneth Stott
# Canary: bac194ec-19f9-46ec-bfeb-058e69d0c7ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""EncryptionService factory (REQ-684).

Selects the provider from ``provisa.yaml`` ``encryption.provider`` (and
``encryption.key_id``). The cloud KMS providers (aws_kms, azure_key_vault) are
REQ-690-694 and register here as they land; unknown providers fail closed rather
than silently degrading to plaintext.
"""

from __future__ import annotations

from provisa.encryption.envelope import EnvelopeEncryption
from provisa.encryption.providers import LocalKeychain, NullMasterKey
from provisa.encryption.service import EncryptionService, NullEncryption


def build_encryption_service(
    provider: str | None,
    *,
    key_id: str | None = None,
    dek_cache_ttl: float = 300.0,
) -> EncryptionService:
    """Build the configured EncryptionService.

    ``provider`` is one of: ``null``/None (passthrough — dev/test), ``local`` /
    ``local_keychain`` (LocalKeychain master key), or ``null_envelope`` (envelope
    format with an unwrapped DEK — for exercising the envelope path in tests). An
    unrecognized provider is an error (fail closed), never a silent plaintext store.
    """
    name = (provider or "null").lower()
    if name in ("null", "none", "passthrough"):
        return NullEncryption()
    if name in ("local", "local_keychain", "keychain"):
        return EnvelopeEncryption(LocalKeychain.from_config(key_id), dek_cache_ttl=dek_cache_ttl)
    if name == "null_envelope":
        return EnvelopeEncryption(NullMasterKey(), dek_cache_ttl=dek_cache_ttl)
    raise ValueError(
        f"Unknown encryption provider {provider!r}. Supported: null, local. "
        "Cloud KMS providers (aws_kms, azure_key_vault) are REQ-690-694."
    )
