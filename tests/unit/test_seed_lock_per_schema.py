# Copyright (c) 2026 Kenneth Stott
# Canary: 2d9e4b71-6c08-4f13-8b5a-c7a10e36f2d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Seeding one org's built-in views keeps no other org waiting (REQ-1900, REQ-2006).

The seed's session advisory lock exists for workers building the SAME schema at once. Its one key
held every org's build, in every process on the control plane, in one line: on 2026-11-01 a retail
sandbox on an idle regions tier waited 115 s of its 125 s build for the other tier's opens and
template builds, and the opens there 250-460 s."""

# Requirements: REQ-1900, REQ-2006

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from provisa.api import startup_seed


class _Conn:
    def __init__(self, held: list[int]) -> None:
        self._held = held

    async def upsert(self, *args, **kwargs):
        return None

    async def execute_core(self, stmt):
        return SimpleNamespace(scalar=lambda: 0)

    @asynccontextmanager
    async def advisory_lock(self, key: int):
        self._held.append(key)
        yield self


class _Db:
    dialect = "postgresql"

    def __init__(self, held: list[int]) -> None:
        self._held = held

    @asynccontextmanager
    async def acquire(self):
        yield _Conn(self._held)


def _keys_of(monkeypatch, org_id: str, env: str | None) -> list[int]:
    held: list[int] = []

    async def _nothing(*args, **kwargs):
        return None

    for step in (
        "_seed_tag_param_values",
        "_seed_meta_domain",
        "_seed_ontology_domain",
        "_seed_ops_pg",
        "_seed_ops_domain",
        "_ensure_ops_steward_grant",
        "_seed_meta_relationships",
    ):
        monkeypatch.setattr(startup_seed, step, _nothing)
    monkeypatch.setattr(startup_seed._source_repo, "remove_where", _nothing)
    monkeypatch.setattr(
        startup_seed,
        "state",
        SimpleNamespace(
            model_db=_Db(held),
            federation_engine=SimpleNamespace(name="duckdb"),
            org_id="default",
        ),
    )
    asyncio.run(startup_seed._seed_built_in_sources("h", 5432, "d", "u", org_id=org_id, env=env))
    return held


def test_two_orgs_seed_under_different_keys(monkeypatch):
    assert _keys_of(monkeypatch, "sbx0aef6d8dbe7d", None) != _keys_of(
        monkeypatch, "sbxfcd0e890bd51", None
    )


def test_one_schema_seeds_under_one_key_on_every_worker(monkeypatch):
    assert _keys_of(monkeypatch, "acme", None) == _keys_of(monkeypatch, "acme", None)


def test_an_environment_is_a_schema_of_its_own(monkeypatch):
    assert _keys_of(monkeypatch, "acme", None) != _keys_of(monkeypatch, "acme", "test")


def test_the_catalog_clients_role_stays_one_at_a_time(monkeypatch):
    """The role is the cluster's, not a schema's: two first GRANTs of one role at once fail with
    'tuple concurrently updated'."""
    held: list[int] = []
    sent: list[str] = []

    class _Client(_Conn):
        async def execute(self, sql: str) -> None:
            sent.append(sql)

    asyncio.run(startup_seed._seed_catalog_client(_Client(held), "org_acme"))
    asyncio.run(startup_seed._seed_catalog_client(_Client(held), "org_other"))
    assert len(held) == 2 and held[0] == held[1]
    assert sum("CREATE ROLE" in sql for sql in sent) == 2


def test_one_schema_bootstraps_under_one_key_and_two_under_two():
    from provisa.core.db import schema_lock_key

    assert schema_lock_key("org_acme") == schema_lock_key("org_acme")
    assert schema_lock_key("org_acme") != schema_lock_key("org_other")
    assert schema_lock_key("org_acme") < 2**63, "a PostgreSQL bigint"


async def test_a_seeded_tables_columns_are_written_in_one_commit(monkeypatch):
    """REQ-2006: a sandbox's build seeds about 270 system tables, and each of their columns
    committed alone, to a control plane on another machine: the silent two minutes of an open."""
    commits = {"open": 0, "columns": 0, "outside": 0}

    class _Writer:
        async def upsert_returning(self, *args, **kwargs):
            return 7

        async def reflect_columns(self, table, schema=None):
            return [
                {"column_name": name, "data_type": "text", "is_primary_key": False}
                for name in ("a", "b", "c")
            ]

        async def execute_core(self, stmt):
            return None

        async def upsert(self, table, row, **kwargs):
            commits["columns"] += 1
            commits["outside"] += 0 if commits["open"] else 1

        @asynccontextmanager
        async def transaction(self):
            commits["open"] += 1
            yield
            commits["open"] -= 1

    monkeypatch.setitem(startup_seed._TBL_DESC, "t", "a table")
    monkeypatch.setitem(startup_seed._COL_DESC, "t", {})
    await startup_seed._register_admin_view(
        _Writer(), "meta", "org_acme", "t", base_table=None, view_name="t"
    )
    assert commits["columns"] == 3
    assert commits["outside"] == 0
