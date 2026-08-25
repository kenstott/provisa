# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9f1a06-72be-4c58-9a41-b5e7c0d82f39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1581: a derived term names the column's BUSINESS name — its alias where one is set.

Aliasing ``usr_nm`` to ``user name`` is the stronger move than renaming the term it produced:
the alias travels with the data to every surface, while a term rename corrects one catalog entry
and leaves the column still reading ``usr_nm``. So the term follows the alias, and re-aliasing
re-derives it — until a curator has worked on the term, at which point their work outranks the
model and pins the ref where it is.

A real SQLite tenant DB through the table repository, because the derivation IS the table upsert.
"""

# Requirements: REQ-1387, REQ-1581

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    roles,
    table_columns,
)

_TABLES = [
    registered_tables,
    table_columns,
    roles,
    glossary_terms,
    glossary_term_refs,
    glossary_term_edges,
    glossary_term_experts,
]


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'alias.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: registered_tables.metadata.create_all(s, tables=_TABLES))
    try:
        async with Database(engine, name="alias").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


def _accounts(alias: str | None) -> Table:
    """One table, one column ``usr_nm``, carrying whatever business name the modeller gave it."""
    return Table(
        source_id="__derived__",
        domain_id="d",
        schema_name="s",
        table_name="accounts",
        columns=[Column(name="usr_nm", data_type="text", visible_to=[], alias=alias)],
        view_sql="SELECT 1",
    )


async def _names(conn) -> set[str]:
    return {t["name"] for t in await glossary_repo.list_terms(conn)}


async def _term(conn, name: str) -> dict | None:
    return next((t for t in await glossary_repo.list_terms(conn) if t["name"] == name), None)


async def _term_for_column(conn, column_name: str) -> str | None:
    """The name of the term the column's ref currently points at."""
    for t in await glossary_repo.list_terms(conn):
        detail = await glossary_repo.get_term(conn, t["id"])
        assert detail is not None
        if any(r["column_name"] == column_name for r in detail["refs"]):
            return detail["name"]
    return None


@pytest.mark.asyncio
async def test_the_term_derives_from_the_alias_not_the_physical_name(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _accounts("user name"))

        assert await _names(conn) == {"user name"}
        # The ref still identifies the column physically — that is what a query compiles against.
        assert await _term_for_column(conn, "usr_nm") == "user name"


@pytest.mark.asyncio
async def test_an_unaliased_column_derives_from_its_physical_name(tmp_path):
    """The alias is the business name where there is one; otherwise the column speaks for itself."""
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _accounts(None))

        assert await _names(conn) == {"usr name"}


@pytest.mark.asyncio
async def test_re_aliasing_a_column_re_derives_its_term(tmp_path):
    """The glossary follows the model: correcting the alias corrects the term it produced."""
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _accounts("user name"))
        await table_repo.upsert(conn, _accounts("login name"))

        assert await _term_for_column(conn, "usr_nm") == "login name"
        # The term the old alias produced was an untouched proposal, so it does not linger.
        assert "user name" not in await _names(conn)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["definition", "relationship", "expert"])
async def test_curation_outranks_the_model_and_pins_the_ref(tmp_path, kind):
    """A definition, a relationship, or a named expert can only have come from a person. Once one
    exists, an alias edit is the weaker statement and must not move the link off their work."""
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _accounts("user name"))
        term = await _term(conn, "user name")
        assert term is not None
        if kind == "definition":
            assert await glossary_repo.set_definition(conn, term["id"], "The sign-in name.")
        elif kind == "expert":
            await glossary_repo.add_expert(conn, term["id"], "alice", kind="author")
        else:
            party = await glossary_repo.create_abstract_term(conn, "party")
            await glossary_repo.add_edge(conn, term["id"], party, "KIND_OF")

        await table_repo.upsert(conn, _accounts("login name"))

        assert await _term_for_column(conn, "usr_nm") == "user name"
        assert "login name" not in await _names(conn)


@pytest.mark.asyncio
async def test_dropping_the_alias_returns_the_term_to_the_physical_name(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _accounts("user name"))
        await table_repo.upsert(conn, _accounts(None))

        assert await _term_for_column(conn, "usr_nm") == "usr name"
