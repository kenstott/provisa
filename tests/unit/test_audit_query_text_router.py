# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2a5c14-7b3d-4f60-a9c1-2d6e0f4b7a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statement text behind a row of the ops `queries` report (REQ-1910, REQ-689).

The report is a view over ``query_audit_log`` and never exposes the encrypted text. The text of one
statement is read through this endpoint: gated on the org ``admin`` capability and decrypted by
the audit log's own read path.
"""

# Requirements: REQ-1910, REQ-689

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import audit_query_text_router as router
from provisa.api.errors import ApiError
from provisa.audit.query_log import log_query
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import metadata, query_audit_log


class _Reversing:
    """An encryption service whose ciphertext is visibly not the plaintext."""

    def encrypt(self, data: bytes) -> bytes:
        return data[::-1]

    def decrypt(self, data: bytes) -> bytes:
        return data[::-1]


@pytest.fixture
async def audit_id(tmp_path, monkeypatch) -> int:
    db = Database(create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"), name="audit")
    with db.engine.begin() as conn:
        metadata.create_all(conn, tables=[query_audit_log])
    await log_query(
        db,
        tenant_id="acme",
        user_id="alice",
        role_id="analyst",
        query_text="SELECT id FROM orders WHERE region = 'EU'",
        table_ids=[1],
        source="pgwire",
        status_code=200,
        duration_ms=4,
        encryption=_Reversing(),
    )
    monkeypatch.setattr(router, "_tenant_pool", lambda: db)
    monkeypatch.setattr(router, "encryption_service", lambda: _Reversing())
    async with db.acquire() as conn:
        stored = (
            await conn.execute_core(
                query_audit_log.select().with_only_columns(
                    query_audit_log.c.id, query_audit_log.c.query_text_enc
                )
            )
        ).fetchone()
    assert bytes(stored[1]) != b"SELECT id FROM orders WHERE region = 'EU'"  # stored encrypted
    return stored[0]


def _request():
    return SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id="alice")))


@pytest.mark.asyncio
async def test_an_admin_reads_the_decrypted_text_of_one_statement(audit_id, monkeypatch):
    monkeypatch.setattr(router, "require_capability_request", lambda _r, _c: None)
    result = await router.read_statement_text(_request(), audit_id)
    assert result == {"id": audit_id, "query_text": "SELECT id FROM orders WHERE region = 'EU'"}


@pytest.mark.asyncio
async def test_the_read_is_gated_on_view_governance(audit_id, monkeypatch):
    asked: list[str] = []

    def _refuse(_request, capability):
        asked.append(capability)
        raise ApiError(403, "auth.missing_capability", f"Missing capability: {capability!r}")

    monkeypatch.setattr(router, "require_capability_request", _refuse)
    with pytest.raises(ApiError) as exc:
        await router.read_statement_text(_request(), audit_id)
    assert exc.value.status_code == 403
    assert asked == ["view_governance"]


@pytest.mark.asyncio
async def test_an_unknown_statement_is_404(audit_id, monkeypatch):
    monkeypatch.setattr(router, "require_capability_request", lambda _r, _c: None)
    with pytest.raises(ApiError) as exc:
        await router.read_statement_text(_request(), audit_id + 999)
    assert exc.value.status_code == 404
