# Copyright (c) 2026 Kenneth Stott
# Canary: c5294733-3e32-47ff-9e33-f0fa3c066892
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A deploy, an undo or the load of a commit that lacks an object's file retires it (REQ-2248,
amended 2026-11-15: "The config loader, metric removal and deploy/undo retire a model object;
they no longer delete it"), and so does a person's delete of a glossary term."""

# Requirements: REQ-2248

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.core import model_change
from provisa.core.database import Database
from provisa.core.env_deploy import land
from provisa.core.env_project import project
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    glossary_term_edges,
    glossary_terms,
    registered_tables,
    roles,
    sources,
    table_columns,
)
from tests.unit.gate_identity import serve_every_column

SCHEMA = "org_default"
ORDERS = "sales/tables/orders.yaml"


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="seller", capabilities=[], domain_access=["sales"])
        )
        for name in ("orders", "customers"):
            await table_repo.upsert(conn, _table(name, "id", "amount"))
    return db


def _table(name: str, *columns: str) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=[Column(name=c, visible_to=["seller"], data_type="integer") for c in columns],
    )


async def _orders(conn) -> dict[str, Any]:
    row = (
        await conn.execute_core(
            select(registered_tables).where(registered_tables.c.table_name == "orders")
        )
    ).one()
    return dict(row._mapping)


async def _deploy(conn, tree: dict[str, Any]) -> None:
    current = await project(conn, SCHEMA)
    await land(conn, "default", None, tree, current, "deadbeef", False, None, apply=True)


async def test_a_deploy_of_a_tree_without_a_tables_file_retires_the_table(plane):
    async with model_change.scope("deploy", actor=lambda: "ada"), plane.acquire() as conn:
        before = await _orders(conn)
        tree = await project(conn, SCHEMA)
        del tree[ORDERS]
        await _deploy(conn, tree)
        after = await _orders(conn)
        tree_after = await project(conn, SCHEMA)
    assert after["id"] == before["id"]
    assert (after["retired"], after["status_by"]) == (True, "ada")
    assert after["status_reason"] == "removed by a deploy of deadbeef"
    assert tree_after[ORDERS]["retired"] is True


async def test_a_deploy_of_a_tables_file_without_a_column_retires_the_column(plane):
    async with model_change.scope("deploy", actor=lambda: "ada"), plane.acquire() as conn:
        tree = await project(conn, SCHEMA)
        tree[ORDERS] = {
            **tree[ORDERS],
            "columns": [c for c in tree[ORDERS]["columns"] if c["column_name"] != "amount"],
        }
        await _deploy(conn, tree)
        held = {
            r.column_name: r.retired
            for r in (
                await conn.execute_core(
                    select(table_columns.c.column_name, table_columns.c.retired).where(
                        table_columns.c.table_id == (await _orders(conn))["id"]
                    )
                )
            ).fetchall()
        }
    assert held == {"id": False, "amount": True}


async def test_an_undo_to_a_commit_holding_the_file_restores_the_same_table(plane):
    """The undo of a deploy that retired a table: its file comes back unretired, and the same
    row (identity is the path) is served again."""
    async with model_change.scope("deploy", actor=lambda: "ada"), plane.acquire() as conn:
        before = await _orders(conn)
        tree = await project(conn, SCHEMA)
        without = {p: b for p, b in tree.items() if p != ORDERS}
        await _deploy(conn, without)
        await _deploy(conn, tree)
        after = await _orders(conn)
    assert after["id"] == before["id"]
    assert after["retired"] is False


async def test_deleting_a_glossary_term_retires_it_with_who_and_why(plane):
    """A person's delete of a term retires it through retire_term (REQ-2248): the row, its name
    and its edges are kept; a second delete finds nothing to do."""
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        party = await glossary_repo.create_abstract_term(conn, "party", domains={"sales"})
        person = await glossary_repo.create_abstract_term(conn, "person", domains={"sales"})
        await glossary_repo.add_edge(conn, person, party, "KIND_OF")
        assert await glossary_repo.delete_term(conn, party, reason="merged into person") is True
        assert await glossary_repo.delete_term(conn, party) is False
        row = dict(
            (await conn.execute_core(select(glossary_terms).where(glossary_terms.c.id == party)))
            .one()
            ._mapping
        )
        edges = (
            await conn.execute_core(
                select(glossary_term_edges.c.from_term_id, glossary_term_edges.c.to_term_id)
            )
        ).fetchall()
    assert (row["name"], row["retired"], row["status_by"]) == ("party", True, "ada")
    assert row["status_reason"] == "merged into person"
    assert (person, party) in [tuple(e) for e in edges]
