# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e4c19-8d3a-4f6e-9a1c-5e0d7b3f2a86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A process-local keyring backend for the test instance.

Selected by ``PYTHON_KEYRING_BACKEND`` in ``tests/conftest.py`` so no test reads or writes the
maintainer's OS keychain (the local-dev instance's key store), while a key a test stores can still
be read back in the same process (``keyring.backends.null`` silently drops writes).
"""

from __future__ import annotations

import threading

from keyring.backend import KeyringBackend


class MemoryKeyring(KeyringBackend):
    priority = 1  # type: ignore[assignment]  # selected explicitly by env var, never by priority

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._store: dict[tuple[str, str], str] = {}

    def get_password(self, service: str, username: str) -> str | None:
        with self._lock:
            return self._store.get((service, username))

    def set_password(self, service: str, username: str, password: str) -> None:
        with self._lock:
            self._store[(service, username)] = password

    def delete_password(self, service: str, username: str) -> None:
        with self._lock:
            self._store.pop((service, username), None)
