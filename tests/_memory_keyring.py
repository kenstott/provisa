# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e4c19-8d3a-4f6e-9a1c-5e0d7b3f2a86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The test instance's keyring backend: one key store per host, as a deployment has.

Selected by ``PYTHON_KEYRING_BACKEND`` in ``tests/conftest.py`` so no test reads or writes the
maintainer's OS keychain (the local-dev instance's key store), while a key a test stores can be
read back (``keyring.backends.null`` silently drops writes).

The store is a file under ``$PROVISA_DATA_DIR`` -- the session's own temporary directory, which
every server the session spawns inherits. Workers on one host share that host's key store
(provisa/encryption/providers.py), and the session's processes are one host: a master key an
in-process app mints is the key the session's server subprocesses read. A store held in one
process's memory could not be, and a server started after an in-process app had written vault
secrets refused to boot (VaultKeyError: it held no key and the vault was written under one).
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TypeVar

from keyring.backend import KeyringBackend

T = TypeVar("T")


def _store_path() -> Path:
    # PROVISA_DATA_DIR is set by tests/conftest.py before any keyring use, and read on every call
    # so a test that points it at its own directory gets a store of its own.
    return Path(os.environ["PROVISA_DATA_DIR"]) / "test-keyring.json"


class MemoryKeyring(KeyringBackend):
    priority = 1  # type: ignore[assignment]  # selected explicitly by env var, never by priority

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()

    @contextmanager
    def _locked(self) -> Iterator[Path]:
        path = _store_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, open(path.with_suffix(".lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)  # one writer across the session's processes
            yield path

    @staticmethod
    def _read(path: Path) -> dict[str, str]:
        return json.loads(path.read_text()) if path.exists() else {}

    def _update(self, change: Callable[[dict[str, str]], T]) -> T:
        with self._locked() as path:
            store = self._read(path)
            result = change(store)
            path.write_text(json.dumps(store))
            return result

    def get_password(self, service: str, username: str) -> str | None:
        with self._locked() as path:
            return self._read(path).get(f"{service}\x00{username}")

    def set_password(self, service: str, username: str, password: str) -> None:
        self._update(lambda store: store.__setitem__(f"{service}\x00{username}", password))

    def delete_password(self, service: str, username: str) -> None:
        self._update(lambda store: store.pop(f"{service}\x00{username}", None))
