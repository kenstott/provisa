# Copyright (c) 2026 Kenneth Stott
# Canary: 9f2c6e8a-4b1d-4a37-8e05-2d7b9c1f6a34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pgwire keeps a prepared statement's $N placeholders and BINDS the client's values all the way to
the source (REQ-589, amended 2026-09-30) — real governance, real Postgres source, real asyncpg."""

# Requirements: REQ-589

from __future__ import annotations

import asyncio

import pytest

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")


def _spy_source(state) -> list[tuple[str, list | None]]:
    """Record the (SQL, params) every DIRECT read hands the Postgres source — through the pool
    (Describe shape, decode/re-encode) or the REQ-1863 raw-DataRow passthrough (the Execute)."""
    sent: list[tuple[str, list | None]] = []
    pool = state.source_pools
    real_stream, real_execute = pool.open_stream, pool.execute
    engine = state.federation_engine
    real_passthrough = engine.execute_pg_passthrough

    def _passthrough(source_pools, source_id, sql, params, result_formats, *, run):
        sent.append((sql, params))
        return real_passthrough(source_pools, source_id, sql, params, result_formats, run=run)

    engine.execute_pg_passthrough = _passthrough

    async def _open_stream(source_id, sql, params=None):
        sent.append((sql, params))
        return await real_stream(source_id, sql, params)

    async def _execute(source_id, sql, params=None):
        sent.append((sql, params))
        return await real_execute(source_id, sql, params)

    pool.open_stream, pool.execute = _open_stream, _execute
    return sent


async def _fetch(port: int, sql: str, *args) -> list[tuple]:
    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=port,
        user="admin",
        password="x",
        database="provisa",
        statement_cache_size=0,
    )
    try:
        return [tuple(r) for r in await conn.fetch(sql, *args)]
    finally:
        await conn.close()


def test_a_bound_value_reaches_the_source_bound_and_every_value_shares_one_sql_text(
    pgwire_pg_backend,
):
    b = pgwire_pg_backend
    sent = _spy_source(b["state"])
    sql = f"SELECT id, region FROM {b['schema']}.{b['table']} WHERE id = $1"
    got = [asyncio.run(_fetch(b["port"], sql, value)) for value in (1, 3)]
    assert got == [[(1, "us-east")], [(3, "eu-west")]]

    executed = [(s, p) for s, p in sent if "WHERE false" not in s]  # drop the Describe shapes
    assert [p for _, p in executed] == [[1], [3]]
    texts = {s for s, _ in executed}
    assert len(texts) == 1  # one SQL text for both values — a server-side prepare can be reused
    (text,) = texts
    predicate = text.split("WHERE", 1)[1]
    assert "$1" in predicate
    assert "= 1" not in predicate and "= 3" not in predicate  # never spliced as a literal


def test_an_rls_predicate_is_applied_identically_with_a_bound_value(pgwire_pg_backend):
    """Governance/RLS: the row filter is part of the governed SQL; the client's value stays bound,
    so it can neither drop nor widen the RLS predicate."""
    from provisa.compiler.rls import build_rls_context

    b = pgwire_pg_backend
    state = b["state"]
    state.rls_contexts = {
        "admin": build_rls_context(
            [{"table_id": 1, "role_id": "admin", "filter_expr": "region = 'us-west'"}], "admin"
        )
    }
    try:
        sent = _spy_source(state)
        sql = f"SELECT id FROM {b['schema']}.{b['table']} WHERE id >= $1 ORDER BY id"
        assert asyncio.run(_fetch(b["port"], sql, 1)) == [(2,)]
        assert asyncio.run(_fetch(b["port"], sql, 3)) == []
        executed = [(s, p) for s, p in sent if "WHERE false" not in s]
        assert all("us-west" in s for s, _ in executed)
        assert [p for _, p in executed] == [[1], [3]]
    finally:
        state.rls_contexts = {}
