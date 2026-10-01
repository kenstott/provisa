# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8a1f6c-7b2e-4c59-a4d1-9e0f6b3c2a85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pgwire prepared statement is governed ONCE and its Describe costs the source nothing (REQ-589,
amended 2026-10-01): the Describe is answered from registered metadata, the Bind/Execute reuses the
Describe's governed statement, and the Postgres passthrough reads on a connection borrowed from the
source's own pool (REQ-1863). Real governance, real Postgres source, real asyncpg."""

# Requirements: REQ-589, REQ-1863

from __future__ import annotations

import asyncio
from contextlib import contextmanager

import pytest

pytestmark = [pytest.mark.integration]

asyncpg = pytest.importorskip("asyncpg")


@contextmanager
def _count_governance():
    """Count the governance transforms the pipeline applies (one per governed statement)."""
    import provisa.compiler.stage2 as stage2

    calls: list[str] = []
    real = stage2.apply_governance

    def _spy(sql, gov_ctx, *a, **kw):
        calls.append(sql)
        return real(sql, gov_ctx, *a, **kw)

    stage2.apply_governance = _spy
    try:
        yield calls
    finally:
        stage2.apply_governance = real


@contextmanager
def _spy_source(state):
    """Record every statement handed to the Postgres source, whichever path carries it."""
    sent: list[tuple[str, list | None]] = []
    pool = state.source_pools
    engine = state.federation_engine
    real_stream, real_execute = pool.open_stream, pool.execute
    real_passthrough = engine.execute_pg_passthrough

    def _passthrough(source_pools, source_id, sql, params, *a, **kw):
        sent.append((sql, params))
        return real_passthrough(source_pools, source_id, sql, params, *a, **kw)

    async def _open_stream(source_id, sql, params=None):
        sent.append((sql, params))
        return await real_stream(source_id, sql, params)

    async def _execute(source_id, sql, params=None):
        sent.append((sql, params))
        return await real_execute(source_id, sql, params)

    engine.execute_pg_passthrough = _passthrough
    pool.open_stream, pool.execute = _open_stream, _execute
    try:
        yield sent
    finally:
        engine.execute_pg_passthrough = real_passthrough
        pool.open_stream, pool.execute = real_stream, real_execute


@contextmanager
def _count_source_connects(source_port: int):
    """Count NEW connections opened to the source by either Postgres driver in this process."""
    import psycopg

    opened: list[str] = []
    real_asyncpg = asyncpg.connect
    real_psycopg = psycopg.Connection.connect.__func__

    async def _asyncpg_connect(*args, **kwargs):
        if kwargs.get("port") == source_port or str(source_port) in str(kwargs.get("dsn", "")):
            opened.append("asyncpg")
        return await real_asyncpg(*args, **kwargs)

    def _psycopg_connect(cls, conninfo="", **kwargs):
        if kwargs.get("port") == source_port or f"port={source_port}" in conninfo:
            opened.append("psycopg")
        return real_psycopg(cls, conninfo, **kwargs)

    asyncpg.connect = _asyncpg_connect
    psycopg.Connection.connect = classmethod(_psycopg_connect)
    try:
        yield opened
    finally:
        asyncpg.connect = real_asyncpg
        psycopg.Connection.connect = classmethod(real_psycopg)


async def _client(port: int, real_connect=None):
    return await (real_connect or asyncpg.connect)(
        host="127.0.0.1",
        port=port,
        user="admin",
        password="x",
        database="provisa",
        statement_cache_size=0,
    )


def test_describe_is_answered_from_metadata_and_execute_reuses_its_governance(pgwire_pg_backend):
    b = pgwire_pg_backend
    sql = f"SELECT id, amount, region FROM {b['schema']}.{b['table']} WHERE id = 1"

    async def _run():
        conn = await _client(b["port"])
        try:
            with _count_governance() as governed, _spy_source(b["state"]) as sent:
                stmt = await conn.prepare(sql)  # Parse + Describe(Statement) + Sync
                described = [(a.name, a.type.name) for a in stmt.get_attributes()]
                after_describe = (len(governed), list(sent))
                rows = [tuple(r) for r in await stmt.fetch()]  # Bind + Execute + Sync
                return described, after_describe, rows, len(governed), list(sent)
        finally:
            await conn.close()

    described, after_describe, rows, governed_total, sent = asyncio.run(_run())
    # Registered column types (integer / double / varchar), not a probe of the source.
    assert described == [("id", "int4"), ("amount", "float8"), ("region", "text")]
    assert after_describe == (1, [])  # governed once; the source was never asked
    assert rows == [(1, 19.98, "us-east")]
    assert governed_total == 1  # the Execute reused the Describe's governed statement
    assert len(sent) == 1  # exactly one statement reached the source: the Execute


def test_a_parameterized_statement_is_governed_once_and_binds_its_value(pgwire_pg_backend):
    b = pgwire_pg_backend
    sql = f"SELECT id, region FROM {b['schema']}.{b['table']} WHERE id = $1"

    async def _run():
        conn = await _client(b["port"])
        try:
            with _count_governance() as governed, _spy_source(b["state"]) as sent:
                stmt = await conn.prepare(sql)
                after_describe = (len(governed), list(sent))
                rows = [tuple(r) for r in await stmt.fetch(3)]
                return after_describe, rows, len(governed), list(sent)
        finally:
            await conn.close()

    after_describe, rows, governed_total, sent = asyncio.run(_run())
    assert after_describe == (1, [])
    assert rows == [(3, "eu-west")]
    assert governed_total == 1
    assert [p for _, p in sent] == [[3]]


def test_aggregate_columns_are_described_without_running_and_read_back_exactly(pgwire_pg_backend):
    b = pgwire_pg_backend
    sql = f"SELECT count(*) AS n, max(id) AS top, min(region) AS first FROM {b['schema']}.{b['table']}"

    async def _run():
        conn = await _client(b["port"])
        try:
            with _spy_source(b["state"]) as sent:
                stmt = await conn.prepare(sql)
                described = [(a.name, a.type.name) for a in stmt.get_attributes()]
                asked_at_describe = list(sent)
                return described, asked_at_describe, [tuple(r) for r in await stmt.fetch()]
        finally:
            await conn.close()

    described, asked_at_describe, rows = asyncio.run(_run())
    assert described == [("n", "int8"), ("top", "int4"), ("first", "text")]
    assert asked_at_describe == []
    assert rows == [(4, 4, "eu-west")]


def test_a_column_whose_type_cannot_be_derived_fails_the_describe_naming_it(pgwire_pg_backend):
    b = pgwire_pg_backend
    sql = f"SELECT undeclared_udf(id) AS mystery FROM {b['schema']}.{b['table']}"

    async def _run():
        conn = await _client(b["port"])
        try:
            with _spy_source(b["state"]) as sent:
                with pytest.raises(asyncpg.PostgresError) as err:
                    await conn.prepare(sql)
                return str(err.value), list(sent)
        finally:
            await conn.close()

    message, sent = asyncio.run(_run())
    assert "mystery" in message and "UNDECLARED_UDF" in message.upper()
    assert sent == []  # no probe query stands in for the missing type


def test_queries_open_no_new_connection_to_the_source(pgwire_pg_backend):
    """Each query reads on a connection borrowed from the source's pool — the passthrough included
    — so a warm server opens none, however many queries run."""
    import os

    b = pgwire_pg_backend
    source_port = int(os.environ.get("PG_PORT", "5432"))
    sql = f"SELECT id, region FROM {b['schema']}.{b['table']} WHERE id = 2"
    real_connect = asyncpg.connect

    async def _run():
        conn = await _client(b["port"], real_connect)
        try:
            assert [tuple(r) for r in await conn.fetch(sql)] == [(2, "us-west")]  # warm
            with _count_source_connects(source_port) as opened, _spy_source(b["state"]) as sent:
                for _ in range(5):
                    assert [tuple(r) for r in await conn.fetch(sql)] == [(2, "us-west")]
                return list(opened), len(sent)
        finally:
            await conn.close()

    opened, statements = asyncio.run(_run())
    assert statements == 5
    assert opened == []


def test_a_repeated_statement_reuses_its_governed_form_until_its_inputs_change(pgwire_pg_backend):
    """REQ-1877: governing a statement is a pure function of its text, the role and the role's
    governance inputs, so the one pipeline keeps the governed statement and a repeat is not
    governed again — until the objects it was governed from are replaced (a schema rebuild)."""
    import copy

    b = pgwire_pg_backend
    state = b["state"]
    sql = f"SELECT id, region FROM {b['schema']}.{b['table']} WHERE id = $1"

    async def _run():
        conn = await _client(b["port"])
        try:
            with _count_governance() as governed:
                first = [tuple(r) for r in await conn.fetch(sql, 1)]
                again = [tuple(r) for r in await conn.fetch(sql, 3)]
                repeats = len(governed)
                # A rebuild publishes a new compilation context for the role.
                state.contexts = {"admin": copy.copy(state.contexts["admin"])}
                rebuilt = [tuple(r) for r in await conn.fetch(sql, 2)]
                return first, again, repeats, rebuilt, len(governed)
        finally:
            await conn.close()

    first, again, repeats, rebuilt, total = asyncio.run(_run())
    assert (first, again, rebuilt) == ([(1, "us-east")], [(3, "eu-west")], [(2, "us-west")])
    assert repeats == 1  # the second execution reused the governed statement
    assert total == 2  # governed again once its context was replaced


def test_a_repeated_statement_is_lowered_and_keyed_once_not_per_execution(pgwire_pg_backend):
    """Lowering the governed statement to catalog-physical SQL and deriving its routing key are
    functions of the statement text and the role's compilation context — not of the bound values —
    so a statement executed again with other values re-derives neither."""
    import provisa.compiler.sql_rewrite as sql_rewrite
    import provisa.observability.stage_trace as stage_trace

    b = pgwire_pg_backend
    sql = f"SELECT id, region FROM {b['schema']}.{b['table']} WHERE id = $1 AND 7 = 7"
    lowered: list[str] = []
    redacted: list[str] = []
    first_run: list[tuple[int, int]] = []
    real_normalize, real_redact = sql_rewrite.normalize_table_refs, stage_trace.redact_sql

    def _normalize(text, ctx):
        lowered.append(text)
        return real_normalize(text, ctx)

    def _redact(text):
        redacted.append(text)
        return real_redact(text)

    async def _run():
        conn = await _client(b["port"])
        try:
            out = [[tuple(r) for r in await conn.fetch(sql, 1)]]
            first_run.append((len(lowered), len(redacted)))
            return out + [[tuple(r) for r in await conn.fetch(sql, n)] for n in (3, 2)]
        finally:
            await conn.close()

    sql_rewrite.normalize_table_refs = _normalize
    stage_trace.redact_sql = _redact
    try:
        rows = asyncio.run(_run())
    finally:
        sql_rewrite.normalize_table_refs = real_normalize
        stage_trace.redact_sql = real_redact
    assert rows == [[(1, "us-east")], [(3, "eu-west")], [(2, "us-west")]]
    # The first execution lowers the statement (its catalog-physical and its direct form) and
    # digests its shape for the routing key; the two that follow, with other values, do neither.
    assert first_run[0][0] == 2 and first_run[0][1] >= 1
    assert [(len(lowered), len(redacted))] == first_run
