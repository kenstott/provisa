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

Selects the provider from ``provisa.yaml`` ``encryption.provider`` /
``encryption.key_id`` / the per-provider ``encryption.<provider>`` config block,
dispatching through the extensible provider registry (``registry.py``). Built-ins
and cloud KMS (AWS/Vault/Azure) register there; enterprises add custom providers
via the registry's extension hooks. An unknown or unavailable provider fails
closed — it raises rather than silently degrading to plaintext.
"""

from __future__ import annotations

from provisa.encryption.registry import get_provider_spec
from provisa.encryption.service import EncryptionService


def build_encryption_service(
    provider: str | None,
    *,
    key_id: str | None = None,
    config: dict | None = None,
    dek_cache_ttl: float = 300.0,
) -> EncryptionService:
    """Build the configured EncryptionService from the provider registry.

    ``config`` is the per-provider block (e.g. KMS key ARN / Vault address). An
    unrecognized OR not-yet-available provider is an error (fail closed), never a
    silent plaintext store.
    """
    spec = get_provider_spec(provider)
    if spec is None:
        raise ValueError(
            f"Unknown encryption provider {provider!r}. "
            "Register it via provisa.encryption.register_encryption_provider "
            "(or PROVISA_ENCRYPTION_PROVIDER_MODULES)."
        )
    if not spec.available():
        raise ValueError(
            f"Encryption provider {provider!r} is registered but not available "
            "(its SDK/runtime is not installed)."
        )
    return spec.build(config or {}, key_id, dek_cache_ttl)
