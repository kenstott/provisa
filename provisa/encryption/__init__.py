# Copyright (c) 2026 Kenneth Stott
# Canary: 564a62dc-304f-4688-83a1-2b7bdba4df06
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Encryption core (REQ-684, REQ-685).

An EncryptionService abstraction with pluggable providers (NullEncryption,
LocalKeychain) and AES-256-GCM envelope encryption. Cloud KMS providers and the
per-data-path applications (column/cache/redis/audit encryption) build on top.
"""

from provisa.encryption.envelope import (
    EnvelopeEncryption,
    RingEnvelopeEncryption,
    envelope_key_id,
    split_envelope,
)
from provisa.encryption.factory import build_encryption_service
from provisa.encryption.providers import LocalKeychain, MasterKeyProvider, NullMasterKey
from provisa.encryption.registry import (
    EncryptionProviderSpec,
    encryption_provider_registry,
    get_provider_spec,
    register_encryption_provider,
)
from provisa.encryption.runtime import (
    bind_org_selector,
    configure_encryption,
    deployment_encryption_service,
    encryption_service,
    note_org_rings,
    org_encryption_loaded,
    reset_encryption,
    set_org_encryption,
)
from provisa.encryption.service import EncryptionService, NullEncryption

__all__ = [
    "EncryptionService",
    "NullEncryption",
    "EnvelopeEncryption",
    "RingEnvelopeEncryption",
    "envelope_key_id",
    "split_envelope",
    "MasterKeyProvider",
    "LocalKeychain",
    "NullMasterKey",
    "build_encryption_service",
    "configure_encryption",
    "encryption_service",
    "deployment_encryption_service",
    "bind_org_selector",
    "set_org_encryption",
    "note_org_rings",
    "org_encryption_loaded",
    "reset_encryption",
    # Extension API — enterprises register custom providers (custom KMS/HSM endpoints) with these.
    "register_encryption_provider",
    "EncryptionProviderSpec",
    "encryption_provider_registry",
    "get_provider_spec",
]
