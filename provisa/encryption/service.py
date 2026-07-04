# Copyright (c) 2026 Kenneth Stott
# Canary: 1f5af178-be08-438d-8533-f42caa4ec876
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""EncryptionService abstraction (REQ-684).

Every encrypt/decrypt operation in the platform routes through this interface, so
the concrete provider — NullEncryption (dev/test passthrough), LocalKeychain
(AES-256-GCM, key from the OS keychain), or a cloud KMS — is a configuration
choice, not a code change. Providers are selected via ``provisa.yaml``
``encryption.provider`` / ``encryption.key_id`` (see ``factory.py``).
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class EncryptionService(ABC):  # REQ-684
    """Encrypt/decrypt opaque byte payloads.

    Implementations MUST round-trip: ``decrypt(encrypt(x)) == x`` for any bytes x.
    ``encrypt`` output is opaque (provider-framed) and is only meaningful to the
    same provider/key that produced it.
    """

    @abstractmethod
    def encrypt(self, plaintext: bytes) -> bytes: ...

    @abstractmethod
    def decrypt(self, ciphertext: bytes) -> bytes: ...


class NullEncryption(EncryptionService):  # REQ-684
    """Passthrough provider for dev/test. Stores plaintext; performs no encryption.

    Used when ``encryption.provider`` is unset or ``null``. Never certify this for
    production — it exists so encrypted-path code is exercised identically whether
    or not a real provider is configured.
    """

    def encrypt(self, plaintext: bytes) -> bytes:
        return plaintext

    def decrypt(self, ciphertext: bytes) -> bytes:
        return ciphertext
