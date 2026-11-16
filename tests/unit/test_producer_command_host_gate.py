# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0a2f4e-3d57-4c1b-9a8e-5f2c7d9e1a43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-861, REQ-2134: a file source's producer command is an argv run on the server's host, so it
is host code. While commands.allow_host_code is off, saving a source that carries one is refused
(a config import and the deployment's config file alike), and a stored one is not run."""

# Requirements: REQ-861, REQ-2134

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

import provisa.api.app  # noqa: F401  (says whether the deployment holds many organizations)
from provisa.core import secrets_store, settings_registry
from provisa.core.config_loader import apply_config, parse_config_dict, seed_config
from provisa.core.database import Database
from provisa.core.host_code import HostCodeRefused
from provisa.core.models import Source, SourceType
from provisa.core.repositories import source as source_repo
from provisa.core.schema_org import metadata, sources
from provisa.events.source_loader import SourceRowLoader
from provisa.executor.result import QueryResult
from provisa.freshness.producer import run_producer

_ENV = "PROVISA_COMMANDS_ALLOW_HOST_CODE"
_ARGV = ["/usr/local/bin/fetch-prices", "--out", "prices.csv"]


@pytest.fixture
def host_code_off(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    settings_registry.bind_config({})


@pytest.fixture
def host_code_on(monkeypatch):
    monkeypatch.setenv(_ENV, "true")


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
def spawned(monkeypatch) -> list:
    """Every argv the producer hands to a subprocess, which is never really spawned."""
    calls: list = []

    def _run(cmd, **_kw):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr("provisa.freshness.producer.subprocess.run", _run)
    return calls


@pytest.fixture
async def db(control_plane) -> Database:
    plane = control_plane.org()
    # The built-in source views are registered on, which the startup seed creates.
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(metadata.tables["sources"]).values(id="__derived__", type="postgresql")
        )
    return plane


class _Engine:
    def __init__(self):
        self.sql: str | None = None
        self.engine = SimpleNamespace(catalog_qualified=True)

    def address_replicas(self, sql):
        return sql

    async def execute_engine(self, sql, *_a, **_k):
        self.sql = sql
        return QueryResult(rows=[(1,)], column_names=["id"], column_types=None)


def _stored_source():
    """A source as the store holds it, read back for a load."""
    return SimpleNamespace(id="prices", type=SimpleNamespace(value="csv"), producer_command=_ARGV)


def _table():
    return SimpleNamespace(schema_name="main", table_name="prices", columns=[])


def _source() -> Source:
    return Source(id="prices", type=SourceType.csv, path="prices.csv", producer_command=_ARGV)


def _raw(producer_command: list[str] | None = _ARGV) -> dict:
    source: dict = {"id": "prices", "type": "csv", "path": "prices.csv"}
    if producer_command is not None:
        source["producer_command"] = producer_command
    return {"sources": [source], "domains": [], "tables": [], "roles": []}


async def _stored_producer(db: Database):
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(sources.c.producer_command).where(sources.c.id == "prices")
            )
        ).fetchall()
    return [row[0] for row in rows]


def _assert_refusal(refused) -> None:
    assert refused.value.code == "sources.host_code_refused"
    assert refused.value.params == {"source_id": "prices"}
    assert "commands.allow_host_code" in str(refused.value)
    assert "REQ-" not in str(refused.value)


# ---- run ------------------------------------------------------------------------------------


async def test_a_stored_producer_command_is_not_run_while_host_code_is_off(host_code_off, spawned):
    with pytest.raises(HostCodeRefused) as refused:
        await run_producer(_stored_source())
    _assert_refusal(refused)
    assert spawned == []


async def test_a_load_refuses_the_producer_and_reads_nothing_while_host_code_is_off(
    host_code_off, spawned
):
    """The refusal leaves the load as a failed producer does: raised, and the file not read."""
    engine = _Engine()
    with pytest.raises(HostCodeRefused) as refused:
        await SourceRowLoader(engine).load(_stored_source(), _table())
    _assert_refusal(refused)
    assert spawned == []
    assert engine.sql is None


async def test_a_load_runs_the_producer_then_reads_while_host_code_is_on(host_code_on, spawned):
    engine = _Engine()
    rows = await SourceRowLoader(engine).load(_stored_source(), _table())
    assert spawned == [_ARGV]
    assert engine.sql == 'SELECT * FROM "prices"."main"."prices"'
    assert rows == [{"id": 1}]


async def test_a_source_with_no_producer_is_read_while_host_code_is_off(host_code_off, spawned):
    engine = _Engine()
    source = SimpleNamespace(id="prices", type=SimpleNamespace(value="csv"), producer_command=None)
    assert await SourceRowLoader(engine).load(source, _table()) == [{"id": 1}]
    assert spawned == []


# ---- save -----------------------------------------------------------------------------------


@pytest.mark.parametrize("save", [source_repo.upsert, source_repo.create])
async def test_saving_a_producer_command_is_refused_while_host_code_is_off(host_code_off, db, save):
    async with db.acquire() as conn:
        with pytest.raises(HostCodeRefused) as refused:
            await save(conn, _source())
    _assert_refusal(refused)
    assert await _stored_producer(db) == []


async def test_a_config_import_with_a_producer_command_is_refused_and_saves_nothing(
    host_code_off, db
):
    """``apply_config`` is what an admin's config import runs (app_loaders.apply_configuration)."""
    async with db.acquire() as conn:
        with pytest.raises(HostCodeRefused) as refused:
            await apply_config(parse_config_dict(_raw()), conn)
    _assert_refusal(refused)
    assert await _stored_producer(db) == []


async def test_a_config_import_with_a_producer_command_is_saved_while_host_code_is_on(
    host_code_on, db
):
    async with db.acquire() as conn:
        await apply_config(parse_config_dict(_raw()), conn)
    assert await _stored_producer(db) == [_ARGV]


# ---- config load at startup -----------------------------------------------------------------


async def test_the_config_file_with_a_producer_command_is_refused_at_startup(host_code_off, db):
    """``seed_config`` is what a first start does with the deployment's file."""
    async with db.acquire() as conn:
        with pytest.raises(HostCodeRefused) as refused:
            await seed_config(parse_config_dict(_raw()), conn)
    _assert_refusal(refused)
    assert await _stored_producer(db) == []


async def test_the_config_file_with_a_producer_command_is_seeded_while_host_code_is_on(
    host_code_on, db
):
    async with db.acquire() as conn:
        assert await seed_config(parse_config_dict(_raw()), conn) is True
    assert await _stored_producer(db) == [_ARGV]


async def test_a_source_with_no_producer_is_saved_while_host_code_is_off(host_code_off, db):
    async with db.acquire() as conn:
        await apply_config(parse_config_dict(_raw(None)), conn)
    assert await _stored_producer(db) == [None]
