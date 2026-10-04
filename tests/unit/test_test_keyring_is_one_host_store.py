# Copyright (c) 2026 Kenneth Stott
# Canary: 0c4e9a73-5b21-4d8f-b6e2-7a1f3c9d4e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The test session's keyring is one host's key store, shared by the servers it spawns.

A deployment's workers on one host share that host's key store (provisa/encryption/providers.py).
The test keyring used to live in one process's memory: a master key an in-process app minted was
invisible to a server the session started next, which then refused to boot over the vault the
first had written (VaultKeyError) -- in a batch, never alone."""

from __future__ import annotations

import os
import subprocess
import sys

import keyring


def test_a_key_stored_in_this_process_is_read_by_a_process_it_spawns(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    assert type(keyring.get_keyring()).__name__ == "MemoryKeyring"
    keyring.set_password("provisa-encryption", "master", "a-session-key")
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import keyring; print(keyring.get_password('provisa-encryption', 'master'))",
        ],
        capture_output=True,
        text=True,
        env=dict(os.environ),
        check=True,
    )
    assert child.stdout.strip() == "a-session-key"
    keyring.delete_password("provisa-encryption", "master")
    assert keyring.get_password("provisa-encryption", "master") is None
