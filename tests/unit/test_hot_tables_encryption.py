# Copyright (c) 2026 Kenneth Stott
# Canary: 5e7a9c1b-3d4f-4850-8b6e-2f4a6c8e0b1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-688: hot-table payloads are encrypted at rest in Redis.

Runs against embedded fakeredis (no infra). Verifies the stored blob is ciphertext
(not plaintext JSON), that get_rows decrypts it back, that a wrong key cannot read
it, and that the default (NullEncryption) preserves the current passthrough.
"""

from __future__ import annotations

import os

import pytest

from provisa.cache.hot_tables import HotTableEntry, HotTableManager
from provisa.encryption import NullEncryption
from provisa.encryption.envelope import EnvelopeEncryption
from provisa.encryption.providers import LocalKeychain

pytestmark = pytest.mark.asyncio

_ROWS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


def _mgr(encryption=None):
    return HotTableManager(redis_url=None, auto_threshold=100, max_rows=100, encryption=encryption)


async def _raw(mgr, table_id: int):
    await mgr._connect()
    return await mgr._redis.get(mgr._blob_key(table_id))


async def test_payload_stored_encrypted_and_roundtrips():
    mgr = _mgr(EnvelopeEncryption(LocalKeychain(os.urandom(32))))
    await mgr._store_rows(1, "t", _ROWS, "id", "cat", "sch")
    raw = await _raw(mgr, 1)
    assert "alice" not in raw  # ciphertext at rest — no plaintext row values
    assert await mgr.get_rows(1) == _ROWS


async def test_wrong_key_cannot_read():
    key_a = LocalKeychain(os.urandom(32))
    mgr = _mgr(EnvelopeEncryption(key_a))
    await mgr._store_rows(1, "t", _ROWS, "id", "cat", "sch")
    # A second manager on the shared fakeredis server with a different key.
    mgr2 = _mgr(EnvelopeEncryption(LocalKeychain(os.urandom(32))))
    await mgr2._connect()
    # The table is hot for this process too, with no rows held in it, so the read must go
    # through Redis + decrypt.
    mgr2._hot_tables[1] = HotTableEntry(
        table_id=1, table_name="t", catalog="cat", schema="sch", pk_column="id"
    )
    with pytest.raises(Exception):
        await mgr2.get_rows(1)


async def test_null_encryption_default_passthrough():
    mgr = _mgr(NullEncryption())
    await mgr._store_rows(1, "t", _ROWS, "id", "cat", "sch")
    assert await mgr.get_rows(1) == _ROWS


async def test_default_encryption_is_null():
    # No encryption arg → platform default passthrough, behaviour preserved.
    mgr = _mgr()
    await mgr._store_rows(1, "t", _ROWS, "id", "cat", "sch")
    assert await mgr.get_rows(1) == _ROWS
