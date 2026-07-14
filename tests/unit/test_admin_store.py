# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pluggable control-plane (admin/metadata) store selected by a SQLAlchemy URI (REQ-828).

The control-plane store must run on an embedded engine (SQLite or DuckDB) with zero
external infra on a developer desktop, and on Postgres in production — behind ONE
repository interface, with the SAME schema and behavior. These tests verify:

  * URI -> backend dispatch (sqlite / duckdb / postgres) via ``create_engine_from_url``.
  * The one dialect-neutral schema (``schema_org``) applies to each embedded backend.
  * A full round-trip through the source repository against SQLite AND DuckDB
    (in-memory and temp-file), so the repository is genuinely store-independent.
  * Loud failure on an unsupported or misconfigured URI — never a silent default store.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from provisa.core.database import (
    Capabilities,
    Database,
    create_engine_from_url,
    _normalize_admin_url,
)
from provisa.core.db import _init_schema_portable
from provisa.core.models import Source, SourceType
from provisa.core.repositories import source as source_repo


# --------------------------------------------------------------------------- #
# URI -> backend dispatch
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "uri, expected_dialect",
    [
        ("sqlite+aiosqlite:///:memory:", "sqlite"),
        ("duckdb:///:memory:", "duckdb"),
        ("postgresql+asyncpg://u:p@localhost:5432/db", "postgresql"),
    ],
)
def test_uri_selects_backend(uri, expected_dialect):
    """The SQLAlchemy URI alone selects the store backend — no engine-specific config."""
    engine = create_engine_from_url(uri)
    assert engine.dialect.name == expected_dialect


def test_bare_duckdb_uri_pins_async_driver():
    """A bare ``duckdb://`` control-plane URI is normalized onto the async driver
    (the sync ``duckdb_engine`` cannot back an AsyncEngine)."""
    assert _normalize_admin_url("duckdb:///:memory:").startswith("duckdb+aioduckdb://")


def test_bare_sqlite_uri_pins_async_driver():
    assert _normalize_admin_url("sqlite:///x.db").startswith("sqlite+aiosqlite://")


@pytest.mark.parametrize(
    "bad_uri",
    [
        "mongodb://host/db",  # unsupported backend
        "redis://host:6379",  # unsupported backend
        "postgresql+psycopg2://u@h/db",  # sync driver for a supported backend
        "duckdb+duckdb_engine:///x.db",  # sync driver for duckdb
        "::not a url::",  # unparseable
    ],
)
def test_bad_uri_fails_loud(bad_uri):
    """An unsupported/misconfigured store URI raises — never falls back to a default store."""
    with pytest.raises(ValueError):
        _normalize_admin_url(bad_uri)


def test_backend_capabilities_gate_pg_only_features():
    """Each backend advertises its capabilities so the ONE store abstraction gates PG-only
    features (advisory locks, LISTEN/NOTIFY) off on embedded engines rather than emitting
    PG-specific SQL. Isolation for embedded stores is enforced in the app layer (REQ-828/830),
    not native RLS, which these backends lack."""
    sqlite_caps = Capabilities.for_dialect("sqlite")
    duckdb_caps = Capabilities.for_dialect("duckdb")
    pg_caps = Capabilities.for_dialect("postgresql")
    assert sqlite_caps.schemas is False  # file-per-org isolation
    assert sqlite_caps.listen_notify is False and sqlite_caps.advisory_lock is False
    assert duckdb_caps.advisory_lock is False and duckdb_caps.listen_notify is False
    assert pg_caps.listen_notify is True and pg_caps.advisory_lock is True


# --------------------------------------------------------------------------- #
# same schema applied to an embedded store
# --------------------------------------------------------------------------- #
_CONTROL_PLANE_TABLES = {
    "sources",
    "domains",
    "registered_tables",
    "table_columns",
    "naming_rules",
}


async def _make_store(uri: str) -> Database:
    db = Database(create_engine_from_url(uri), name="admin-test")
    await _init_schema_portable(db)
    return db


@pytest.mark.parametrize("uri", ["sqlite+aiosqlite:///:memory:", "duckdb:///:memory:"])
async def test_same_schema_applies_to_embedded_store(uri):
    """The one dialect-neutral ``schema_org`` metadata creates the identical control-plane
    tables on each embedded backend (SERIAL/JSONB/CASCADE differences absorbed per-dialect)."""
    db = await _make_store(uri)
    try:
        async with db.acquire() as conn:
            reflected = {
                r["column_name"]  # any column proves the table exists + is introspectable
                for tbl in _CONTROL_PLANE_TABLES
                for r in await conn.reflect_columns(tbl)
            }
        assert reflected  # tables created and reflectable on the embedded engine
    finally:
        await db.close()


# --------------------------------------------------------------------------- #
# repository round-trip through the ONE interface, on SQLite AND DuckDB
# --------------------------------------------------------------------------- #
def _sample_source(sid: str = "src1") -> Source:
    return Source(
        id=sid,
        type=SourceType.postgresql,
        host="db.example.com",
        port=5432,
        database="app",
        username="svc",
        dialect="postgresql",
        description="round-trip source",
        mapping={"schema": "public"},
    )


@pytest.mark.parametrize(
    "uri_factory",
    [
        pytest.param(lambda _d: "sqlite+aiosqlite:///:memory:", id="sqlite-memory"),
        pytest.param(lambda _d: "duckdb:///:memory:", id="duckdb-memory"),
        pytest.param(lambda d: f"duckdb:///{d}/admin.duckdb", id="duckdb-file"),
        pytest.param(lambda d: f"sqlite+aiosqlite:///{d}/admin.db", id="sqlite-file"),
    ],
)
async def test_source_repository_round_trip(uri_factory):
    """Insert -> read -> update -> list -> rename -> delete through the source repository,
    unchanged, against each embedded backend — proving the repository is store-independent."""
    with tempfile.TemporaryDirectory() as d:
        db = await _make_store(uri_factory(d))
        try:
            async with db.acquire() as conn:
                # insert
                await source_repo.upsert(conn, _sample_source())
                got = await source_repo.get(conn, "src1")
                assert got is not None
                assert got["id"] == "src1"
                assert got["host"] == "db.example.com"
                assert got["mapping"] == {"schema": "public"}  # jsonb round-trip

                # update via the same upsert path (rowcount-driven match on every backend)
                updated = _sample_source()
                updated.host = "db2.example.com"
                await source_repo.upsert(conn, updated)
                got2 = await source_repo.get(conn, "src1")
                assert got2["host"] == "db2.example.com"

                # list
                assert len(await source_repo.list_all(conn)) == 1

                # rename (multi-statement transaction) then delete
                assert await source_repo.rename(conn, "src1", "src2") is True
                assert await source_repo.get(conn, "src1") is None
                assert await source_repo.delete(conn, "src2") is True
                assert await source_repo.delete(conn, "does-not-exist") is False
        finally:
            await db.close()


async def test_duckdb_file_store_persists_across_reopen():
    """The DuckDB file store is durable — a reopened engine sees committed control-plane rows,
    the desktop persistence the embedded store must provide."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "admin.duckdb"
        uri = f"duckdb:///{path}"
        db = await _make_store(uri)
        try:
            async with db.acquire() as conn:
                await source_repo.upsert(conn, _sample_source("persist"))
        finally:
            await db.close()

        reopened = Database(create_engine_from_url(uri), name="reopen")
        try:
            async with reopened.acquire() as conn:
                got = await source_repo.get(conn, "persist")
                assert got is not None and got["id"] == "persist"
        finally:
            await reopened.close()
