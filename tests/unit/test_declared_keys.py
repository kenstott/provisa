# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9e6c27-1f48-4d5b-9e0a-7c2d8b4f6e13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Discovery records the keys a source declares (REQ-2170).

A table's primary key, its unique (alternate) keys and its foreign keys are read from the
source's own catalog. Registration records the first two into the model, beside what the
registration's own input declares and what its relationships derive; the foreign keys are offered
as relationship candidates. A source that declares no keys records none: nothing is guessed."""

# Requirements: REQ-2170

from __future__ import annotations

import contextlib
import sqlite3
from types import SimpleNamespace

import duckdb
import pytest
from sqlalchemy import insert, select

import provisa.api.app  # noqa: F401  (says whether the deployment holds many organizations)
from provisa.core import secrets_store
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, sources, table_columns
from provisa.discovery.declared_keys import DeclaredKeys, ForeignKey, UniqueKey, declared_keys
from provisa.executor.pool import SourcePool

pytestmark = pytest.mark.asyncio

_PETS_DDL = [
    "CREATE TABLE owners (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
    "CREATE TABLE pets (id INTEGER PRIMARY KEY, tag TEXT UNIQUE, name TEXT,"
    " owner_id INTEGER REFERENCES owners(id))",
]
_PETS = DeclaredKeys(
    primary_key=("id",),
    unique=(UniqueKey("pets_tag_key", ("tag",)),),
    foreign=(ForeignKey("owner_id", "owners", "id"),),
)


def _keys(declared: DeclaredKeys | None) -> tuple:
    """A declaration with each unique key's columns; its name is the source's own, and named."""
    assert declared is not None and all(u.name for u in declared.unique)
    return declared.primary_key, tuple(u.columns for u in declared.unique), declared.foreign


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
async def sqlite_pool(tmp_path):
    path = tmp_path / "pets.db"
    with sqlite3.connect(path) as db:
        for ddl in _PETS_DDL:
            db.execute(ddl)
    pool = SourcePool()
    await pool.add("pets", "sqlite", "", 0, str(path), "", "")
    yield pool
    await pool.close_all()


# --- the scenario --------------------------------------------------------------------------------


async def test_a_sqlite_sources_keys_are_read_from_its_own_catalog(sqlite_pool):
    """pets declares id INTEGER PRIMARY KEY, and no relationship points at pets."""
    assert _keys(await declared_keys(sqlite_pool, "sqlite", "pets", "main", "pets")) == _keys(_PETS)


@pytest.fixture
async def org_plane(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="zoo"))
        await conn.execute_core(insert(sources).values(id="pets", type="sqlite"))
    return db


def _pets_table(*, pk: bool = False, alternate: bool = False) -> Table:
    return Table(
        source_id="pets",
        domain_id="zoo",
        schema_name="main",
        table_name="pets",
        columns=[
            Column(name="id", data_type="integer", visible_to=["admin"], is_primary_key=pk),
            Column(name="tag", data_type="text", visible_to=["admin"], is_alternate_key=alternate),
            Column(name="name", data_type="text", visible_to=["admin"]),
        ],
    )


async def _marks(db) -> dict[str, tuple[bool, bool]]:
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(
                    table_columns.c.column_name,
                    table_columns.c.is_primary_key,
                    table_columns.c.is_alternate_key,
                )
            )
        ).fetchall()
    return {r.column_name: (r.is_primary_key, r.is_alternate_key) for r in rows}


async def test_registration_records_the_keys_the_source_declares(org_plane, sqlite_pool):
    """The registration's input marks nothing; the source's declaration is recorded."""
    declared = await declared_keys(sqlite_pool, "sqlite", "pets", "main", "pets")
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _pets_table(), declared=declared)
    assert await _marks(org_plane) == {
        "id": (True, False),
        "tag": (False, True),
        "name": (False, False),
    }


async def test_a_key_the_source_no_longer_declares_drops_unless_the_input_names_it(org_plane):
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _pets_table(), declared=_PETS)
        # The source dropped its keys; this registration's input still names tag.
        await table_repo.upsert(conn, _pets_table(alternate=True), declared=DeclaredKeys())
    assert await _marks(org_plane) == {
        "id": (False, False),
        "tag": (False, True),
        "name": (False, False),
    }


async def test_a_registration_that_read_no_declaration_records_what_its_input_declares(
    org_plane,
):
    """A source not read (no driver held for it) says nothing of its keys: the marks are the
    input's, as the steward left them -- one the steward cleared is cleared."""
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _pets_table(pk=True), declared=_PETS)
        await table_repo.upsert(conn, _pets_table(alternate=True), declared=None)
    assert await _marks(org_plane) == {
        "id": (False, False),
        "tag": (False, True),
        "name": (False, False),
    }


async def test_a_declared_foreign_key_is_offered_as_a_relationship_candidate(
    org_plane, sqlite_pool
):
    from provisa.discovery.collector import collect_fk_candidates

    async with org_plane.acquire() as conn:
        for name in ("pets", "owners"):
            await conn.execute_core(
                insert(registered_tables).values(
                    source_id="pets", domain_id="zoo", schema_name="main", table_name=name
                )
            )
        candidates = await collect_fk_candidates(
            sqlite_pool, {"pets": "sqlite"}, conn, "domain", "zoo"
        )
        ids = {
            r.table_name: r.id
            for r in (
                await conn.execute_core(
                    select(registered_tables.c.table_name, registered_tables.c.id)
                )
            ).fetchall()
        }
    assert [
        (c.source_table_id, c.source_column, c.target_table_id, c.target_column, c.cardinality)
        for c in candidates
    ] == [(ids["pets"], "owner_id", ids["owners"], "id", "many_to_one")]


# --- each driver ---------------------------------------------------------------------------------


async def test_a_source_that_declares_no_keys_records_none(tmp_path):
    path = tmp_path / "plain.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE notes (body TEXT)")
    pool = SourcePool()
    await pool.add("plain", "sqlite", "", 0, str(path), "", "")
    try:
        assert await declared_keys(pool, "sqlite", "plain", "main", "notes") == DeclaredKeys()
    finally:
        await pool.close_all()


async def test_a_source_not_held_or_of_a_kind_with_no_reader_is_not_read():
    pool = SourcePool()
    assert await declared_keys(pool, "sqlite", "absent", "main", "t") is None
    assert await declared_keys(pool, "openapi", "absent", "main", "t") is None


async def test_duckdb_declares_its_keys_in_duckdb_constraints(tmp_path):
    path = tmp_path / "pets.duckdb"
    with duckdb.connect(str(path)) as db:
        for ddl in _PETS_DDL:
            db.execute(ddl)
    pool = SourcePool()
    await pool.add("pets", "duckdb", "", 0, str(path), "", "")
    try:
        assert _keys(await declared_keys(pool, "duckdb", "pets", "main", "pets")) == _keys(_PETS)
    finally:
        await pool.close_all()


async def test_postgresql_declares_its_keys_in_information_schema(control_plane):
    from tests.embedded_pg import PASSWORD, ROLE

    async with control_plane.platform.acquire() as conn:
        await conn.execute("CREATE SCHEMA zoo")
        for ddl in _PETS_DDL:
            await conn.execute(
                ddl.replace("CREATE TABLE ", "CREATE TABLE zoo.").replace(
                    "REFERENCES owners", "REFERENCES zoo.owners"
                )
            )
    pool = SourcePool()
    await pool.add(
        "pg", "postgresql", "127.0.0.1", control_plane.pg.port, control_plane.name, ROLE, PASSWORD
    )
    try:
        assert await declared_keys(pool, "postgresql", "pg", "zoo", "pets") == _PETS
    finally:
        await pool.close_all()


class _Catalog:
    """A pooled driver answering each catalog statement with the rows given for it."""

    def __init__(self, answers: dict[str, list]) -> None:
        self.answers = answers
        self.statements: list[str] = []

    def has(self, _source_id):
        return True

    async def execute(self, _source_id, sql, _params=None):
        self.statements.append(sql)
        found = [rows for marker, rows in self.answers.items() if marker in sql]
        assert len(found) == 1, sql
        return SimpleNamespace(rows=found[0])


@pytest.mark.parametrize("kind", ["mysql", "mariadb", "sqlserver"])
async def test_mysql_and_sql_server_declare_their_keys_in_information_schema(kind):
    foreign_marker = "referenced_table_name" if kind != "sqlserver" else "referential_constraints"
    catalog = _Catalog(
        {
            "constraint_type IN": [
                ("PRIMARY KEY", "PRIMARY", "id"),
                ("UNIQUE", "pets_tag", "tag"),
                ("UNIQUE", "pets_pair", "a"),
                ("UNIQUE", "pets_pair", "b"),
            ],
            foreign_marker: [("owner_id", "owners", "id")],
        }
    )
    keys = await declared_keys(catalog, kind, "src", "zoo", "pets")
    assert keys == DeclaredKeys(
        primary_key=("id",),
        unique=(UniqueKey("pets_tag", ("tag",)), UniqueKey("pets_pair", ("a", "b"))),
        foreign=(ForeignKey("owner_id", "owners", "id"),),
    )
    # Only a one-column unique key is an alternate key of its own.
    assert keys is not None and keys.alternate_columns == frozenset({"tag"})
    assert all("'zoo'" in s and "'pets'" in s for s in catalog.statements)


async def test_a_composite_primary_key_is_read_in_key_order_and_a_unique_index_is_not_a_key(
    tmp_path,
):
    """A key's columns come in the key's order, not the table's; a CREATE UNIQUE INDEX is an
    index, not a constraint the table declares."""
    path = tmp_path / "visits.db"
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE visits (vet_id INTEGER, pet_id INTEGER, note TEXT,"
            " PRIMARY KEY (pet_id, vet_id))"
        )
        db.execute("CREATE UNIQUE INDEX visits_note ON visits (note)")
    pool = SourcePool()
    await pool.add("vet", "sqlite", "", 0, str(path), "", "")
    try:
        keys = await declared_keys(pool, "sqlite", "vet", "main", "visits")
    finally:
        await pool.close_all()
    assert keys == DeclaredKeys(primary_key=("pet_id", "vet_id"))


async def test_a_composite_foreign_key_keeps_its_pairing_column_by_column():
    """PostgreSQL's catalog query, run over DuckDB's information_schema (which follows the
    standard as PostgreSQL's does): two pairs, never one key paired with another table's."""
    from provisa.discovery.declared_keys import READERS

    con = duckdb.connect()
    con.execute("CREATE SCHEMA s")
    con.execute("CREATE TABLE s.parent (a INT, b INT, PRIMARY KEY (a, b))")
    con.execute("CREATE TABLE s.other (a INT, b INT, PRIMARY KEY (a, b))")
    con.execute(
        "CREATE TABLE s.child (id INT PRIMARY KEY, pb INT, pa INT,"
        " FOREIGN KEY (pa, pb) REFERENCES s.parent (a, b))"
    )

    class _Pool:
        async def execute(self, _source_id, sql, _params=None):
            return SimpleNamespace(rows=con.execute(sql).fetchall())

    keys = await READERS["postgresql"](_Pool(), "src", "s", "child")
    assert keys.primary_key == ("id",)
    assert keys.foreign == (
        ForeignKey("pa", "parent", "a"),
        ForeignKey("pb", "parent", "b"),
    )


async def test_the_admin_save_reads_the_keys_its_source_declares(monkeypatch, sqlite_pool):
    """A table saved from the editor or Register Table is registered with what its source
    declares; a built-in source with no type here (the derived views') is not read."""
    from provisa.api.admin._declared_keys import registration_keys
    from provisa.api.app import state

    monkeypatch.setattr(type(state), "source_types", property(lambda _s: {"pets": "sqlite"}))
    monkeypatch.setattr(type(state), "source_pools", property(lambda _s: sqlite_pool))
    keys = await registration_keys(_pets_table())
    assert keys is not None and keys.primary_key == ("id",)
    assert keys.alternate_columns == frozenset({"tag"})
    derived = _pets_table().model_copy(update={"source_id": "__derived__"})
    assert await registration_keys(derived) is None


async def test_the_boot_pass_records_the_keys_of_a_table_registered_before_its_source_was_read(
    monkeypatch, org_plane, sqlite_pool
):
    """A configuration is applied before the source pools are built, so its tables are registered
    with no declaration read; the pass after the pools records what each source declares."""
    from provisa.api import startup_seed
    from provisa.api.app import state
    from provisa.federation import replica_routing

    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _pets_table(), declared=None)

    async def _none_kept(_state):
        return frozenset()

    monkeypatch.setattr(replica_routing, "kept_sources", _none_kept)
    monkeypatch.setattr(type(state), "source_pools", property(lambda _s: sqlite_pool))
    monkeypatch.setattr(state, "model_db", org_plane, raising=False)
    await startup_seed._resolve_pk_from_sources()
    assert await _marks(org_plane) == {
        "id": (True, False),
        "tag": (False, True),
        "name": (False, False),
    }
