# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7f0b58-2a49-4c61-8e05-9b2c4d13a760
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-689: audit-log query text is encrypted before write and decrypted on authorised read.

Pure unit tier — the durable store is mocked (no live Postgres); the crypto is real
(EnvelopeEncryption). Asserts the isolable contract: log_query never binds plaintext to the
query_text_enc column (only ciphertext + a plaintext hash for dedup), and read_query_text
recovers the original only with the writing key.
"""

from __future__ import annotations

import hashlib
import os
from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.audit.query_log import log_query, read_query_text
from provisa.encryption import NullEncryption
from provisa.encryption.envelope import EnvelopeEncryption
from provisa.encryption.providers import LocalKeychain

_QUERY = "SELECT ssn, salary FROM hr.employees WHERE dept = 'exec'"


def _enc():
    return EnvelopeEncryption(LocalKeychain(os.urandom(32)))


def _mock_pool() -> tuple:
    """A pool whose acquire() yields a mocked Connection (the migrated Core path)."""
    conn = AsyncMock()
    acq = MagicMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


async def _capture_insert(enc) -> dict:
    """Run log_query against a mocked pool and return the bound INSERT column values."""
    pool, conn = _mock_pool()
    await log_query(
        pool,
        tenant_id="acme",
        user_id="u1",
        role_id="analyst",
        query_text=_QUERY,
        table_ids=["1"],
        source="graphql",
        status_code=200,
        duration_ms=5,
        encryption=enc,
    )
    conn.execute_core.assert_awaited_once()
    stmt = conn.execute_core.await_args.args[0]
    return stmt.compile().params


def _read_pool(query_text_enc):
    """A pool whose acquire()->conn.execute_core returns a one-row result (row[0]=ciphertext)."""
    pool, conn = _mock_pool()
    result = MagicMock()
    result.fetchone.return_value = None if query_text_enc is _ABSENT else (query_text_enc,)
    conn.execute_core = AsyncMock(return_value=result)
    return pool


_ABSENT = object()  # sentinel: no row at all (vs. a row with NULL ciphertext)


# ---- encrypt-before-write (REQ-689) -----------------------------------------


@pytest.mark.asyncio
async def test_query_text_column_is_ciphertext_not_plaintext():
    params = await _capture_insert(_enc())
    query_text_enc = params["query_text_enc"]
    assert isinstance(query_text_enc, (bytes, bytearray))
    assert _QUERY.encode("utf-8") not in bytes(query_text_enc)  # plaintext never written


@pytest.mark.asyncio
async def test_plaintext_hash_is_kept_for_dedup_not_the_text():
    params = await _capture_insert(_enc())
    assert params["query_hash"] == hashlib.sha256(_QUERY.encode()).hexdigest()


# ---- decrypt-on-read round-trip (REQ-689) -----------------------------------


@pytest.mark.asyncio
async def test_authorised_read_decrypts_back_to_plaintext():
    enc = _enc()
    params = await _capture_insert(enc)
    query_text_enc = params["query_text_enc"]

    assert await read_query_text(_read_pool(query_text_enc), 42, enc) == _QUERY


@pytest.mark.asyncio
async def test_read_with_a_different_key_cannot_decrypt():
    params = await _capture_insert(_enc())  # written with one key
    query_text_enc = params["query_text_enc"]

    with pytest.raises(Exception):  # a foreign key cannot recover the plaintext
        await read_query_text(_read_pool(query_text_enc), 42, _enc())


@pytest.mark.asyncio
async def test_read_returns_none_when_row_or_column_absent():
    assert await read_query_text(_read_pool(_ABSENT), 1, _enc()) is None
    assert await read_query_text(_read_pool(None), 1, _enc()) is None


# ---- NullEncryption passthrough (dev/test) ----------------------------------


@pytest.mark.asyncio
async def test_null_encryption_round_trips_but_is_not_secret():
    enc = NullEncryption()
    params = await _capture_insert(enc)
    query_text_enc = params["query_text_enc"]

    assert await read_query_text(_read_pool(query_text_enc), 1, enc) == _QUERY


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
