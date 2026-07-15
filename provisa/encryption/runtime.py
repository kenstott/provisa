# Copyright (c) 2026 Kenneth Stott
# Canary: 9cfb0cee-666a-4b1c-8128-fbf731a12c8f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Process-configured EncryptionService accessor (REQ-684, REQ-686).

The encryption provider is process-global config (``encryption.provider`` /
``encryption.key_id`` in provisa.yaml, REQ-684), so cross-cutting call sites —
column encryption in repositories, cache payloads — read the one configured
service through ``encryption_service()`` instead of threading it through every
call chain. ``configure_encryption`` is called once at startup; before that (and
in tests) the accessor returns the passthrough NullEncryption, so the encrypted
code path is exercised identically whether or not a provider is configured.

Per-tenant keys (REQ-690-694 / REQ-830 app-layer isolation) would extend this to a
keyed lookup; today the provider is single, process-wide.
"""

from __future__ import annotations

from provisa.encryption.service import EncryptionService, NullEncryption

_service: EncryptionService | None = None


def configure_encryption(
    provider: str | None, *, key_id: str | None = None, config: dict | None = None
) -> EncryptionService:
    """Build and install the process-wide EncryptionService from config. Idempotent.

    ``config`` is the per-provider block (``encryption.<provider>``) carrying e.g.
    the KMS key ARN / Vault address for the selected provider.
    """
    global _service
    from provisa.encryption.factory import build_encryption_service  # noqa: PLC0415

    _service = build_encryption_service(provider, key_id=key_id, config=config)
    return _service


def encryption_service() -> EncryptionService:
    """Return the configured service, or NullEncryption (passthrough) when unconfigured."""
    return _service if _service is not None else NullEncryption()


def reset_encryption() -> None:
    """Clear the configured service (test isolation)."""
    global _service
    _service = None
