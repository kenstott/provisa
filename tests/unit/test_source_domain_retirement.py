# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f7c2e-5a41-4d8e-9f13-7e2c4a9b1d60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deleting a source or a domain retires it (REQ-2248, amended 2026-11-15).

One test per sentence of the rule, for a source and for a domain:
- deleting one is refused only while it holds a live table (or another live object names it);
- otherwise the delete retires it together with the retired tables it holds;
- a retired one is hidden, is not counted against a one-per-kind limit, and leaves its name free;
- it is listed with the other retired objects and restored from there with its tables;
- a restore is refused while a live object holds its name;
- nothing is removed.
"""

# Requirements: REQ-2248

from __future__ import annotations

import types
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy import insert, select

from provisa.core import model_change
from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import domain as domain_repo
from provisa.core.repositories import source as source_repo
from provisa.core.repositories import table as table_repo
from provisa.core.repositories.domain import DomainDeleteRefused
from provisa.core.repositories.retirement import NameHeld
from provisa.core.repositories.source import SourceDeleteRefused
from provisa.core.schema_org import (
    domains,
    registered_tables,
    roles,
    sources,
    table_columns,
    tag_assignments,
    tracked_functions,
)
from tests.unit.gate_identity import serve_every_column

if TYPE_CHECKING:
    from provisa.api.admin.types import SourceInput

WHY = "moved to the new warehouse"


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(control_plane) -> Database:
    """Source ``old`` and domain ``archive`` hold one table, ``ledger``, with two columns and a
    tag on the source. ``live`` (on source ``pg`` in domain ``sales``) stays in service."""
    db = control_plane.org()
    async with db.acquire() as conn:
        for source_id in ("old", "pg"):
            await conn.execute_core(insert(sources).values(id=source_id, type="postgresql"))
        for domain_id in ("archive", "sales"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        await conn.execute_core(
            insert(roles).values(id="seller", capabilities=[], domain_access=["sales"])
        )
        await table_repo.upsert(conn, _table("ledger", "old", "archive"))
        await table_repo.upsert(conn, _table("live", "pg", "sales"))
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="pii",
                base_tag_id="pii",
                object_type="source",
                object_key="old",
                source_id="old",
            )
        )
    return db


def _table(name: str, source_id: str, domain_id: str) -> Table:
    return Table(
        source_id=source_id,
        domain_id=domain_id,
        schema_name="public",
        table_name=name,
        columns=[Column(name=c, visible_to=["seller"], data_type="integer") for c in ("id", "n")],
    )


async def _ledger(conn) -> dict[str, Any]:
    return dict(
        (
            await conn.execute_core(
                select(registered_tables).where(registered_tables.c.table_name == "ledger")
            )
        )
        .one()
        ._mapping
    )


async def _retire_ledger(conn) -> int:
    ledger = (await _ledger(conn))["id"]
    assert await table_repo.delete(conn, ledger, reason="superseded") is True
    return ledger


REPOS = {"source": source_repo, "domain": domain_repo}
HOLDER = {"source": "old", "domain": "archive"}
COLUMN = {"source": "source_id", "domain": "domain_id"}
REFUSED = {"source": SourceDeleteRefused, "domain": DomainDeleteRefused}
KINDS = ["source", "domain"]


async def _retired(conn, kind: str) -> dict[str, Any]:
    [row] = await REPOS[kind].list_all(conn, retired=True)
    return row


# --- refused only while it holds a live table, or another live object names it ------------------


@pytest.mark.parametrize("kind", KINDS)
async def test_deleting_one_that_holds_a_live_table_is_refused_naming_it(plane, kind):
    async with plane.acquire() as conn:
        ledger = (await _ledger(conn))["id"]
        with pytest.raises(REFUSED[kind]) as refused:
            await REPOS[kind].delete(conn, HOLDER[kind])
        assert refused.value.reason == "dependents"
        assert [(d.ref.kind, d.ref.id) for d in refused.value.dependents] == [("table", ledger)]
        assert await REPOS[kind].get(conn, HOLDER[kind]) is not None
        assert await REPOS[kind].list_all(conn, retired=True) == []


async def test_deleting_a_source_a_live_command_names_is_refused(plane):
    async with plane.acquire() as conn:
        await _retire_ledger(conn)
        await conn.execute_core(
            insert(tracked_functions).values(name="refund", source_id="old", domain_id="sales")
        )
        with pytest.raises(SourceDeleteRefused) as refused:
            await source_repo.delete(conn, "old")
        assert [(d.ref.kind, d.ref.id) for d in refused.value.dependents] == [("command", "refund")]


async def test_deleting_a_domain_a_live_role_names_is_refused(plane):
    async with plane.acquire() as conn:
        await _retire_ledger(conn)
        await conn.execute_core(
            roles.update().where(roles.c.id == "seller").values(domain_access=["archive"])
        )
        with pytest.raises(DomainDeleteRefused) as refused:
            await domain_repo.delete(conn, "archive")
        assert [(d.ref.kind, d.ref.id) for d in refused.value.dependents] == [("role", "seller")]


# --- otherwise the delete retires it together with the retired tables it holds -------------------


@pytest.mark.parametrize("kind", KINDS)
async def test_deleting_one_holding_only_retired_tables_retires_it_with_them(plane, kind):
    async with model_change.scope("delete", actor=lambda: "ada"), plane.acquire() as conn:
        ledger = await _retire_ledger(conn)
        assert await REPOS[kind].delete(conn, HOLDER[kind], reason=WHY) is True
        row = await _retired(conn, kind)
        assert row["retired"] is True
        assert (row["status_by"], row["status_reason"]) == ("ada", WHY)
        assert row["status_at"] is not None
        assert row["retired_id"] == HOLDER[kind]
        # Its retired table is retired with it: still retired, held by the retired row.
        table = await _ledger(conn)
        assert table["id"] == ledger and table["retired"] is True
        assert table[COLUMN[kind]] == row["id"]
        # Retired already: nothing is left to delete at the id it had.
        assert await REPOS[kind].delete(conn, HOLDER[kind]) is False


# --- hidden, not counted against a one-per-kind limit, its name free -----------------------------


@pytest.mark.parametrize("kind", KINDS)
async def test_a_retired_one_is_hidden(plane, kind):
    async with plane.acquire() as conn:
        await _retire_ledger(conn)
        await REPOS[kind].delete(conn, HOLDER[kind])
        assert await REPOS[kind].get(conn, HOLDER[kind]) is None
        assert HOLDER[kind] not in {r["id"] for r in await REPOS[kind].list_all(conn)}
        assert all(not r["retired"] for r in await REPOS[kind].list_all(conn))


async def test_a_retired_source_is_not_counted_against_the_one_askamerica_source(plane):
    from provisa.api.admin.schema_common import _another_govdata_source

    async with plane.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="gov", type="govdata"))
    second = cast("SourceInput", types.SimpleNamespace(id="gov2", type="govdata"))
    refused = await _another_govdata_source(plane, second)
    assert refused is not None and "'gov'" in refused.message
    async with plane.acquire() as conn:
        assert await source_repo.delete(conn, "gov") is True
    assert await _another_govdata_source(plane, second) is None


async def test_a_retired_source_is_not_counted_against_the_plans_ceiling(plane):
    async with plane.acquire() as conn:
        before = await source_repo.count_billable(conn)
        await _retire_ledger(conn)
        await source_repo.delete(conn, "old")
        assert await source_repo.count_billable(conn) == before - 1


async def test_a_retired_sources_id_is_free_for_a_new_source_and_its_tables(plane):
    """The id is free in the database too: a new source is written at it, and the same physical
    table is registered on it in a domain of its own (the retired table keeps its model name in
    its own domain)."""
    async with plane.acquire() as conn:
        await _retire_ledger(conn)
        await source_repo.delete(conn, "old")
        await conn.execute_core(insert(sources).values(id="old", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="fresh"))
        assert await table_repo.upsert(conn, _table("ledger", "old", "fresh")) is not None
        assert (await source_repo.get(conn, "old") or {}).get("retired") is False


async def test_a_retired_domains_id_is_free_for_a_new_domain_and_its_tables(plane):
    """The id is free in the database too: a new domain is written at it, and a table at the
    retired table's model name is registered in it."""
    async with plane.acquire() as conn:
        await _retire_ledger(conn)
        await domain_repo.delete(conn, "archive")
        await conn.execute_core(insert(domains).values(id="archive"))
        await conn.execute_core(insert(sources).values(id="other", type="postgresql"))
        assert await table_repo.upsert(conn, _table("ledger", "other", "archive")) is not None
        assert (await domain_repo.get(conn, "archive") or {}).get("retired") is False


# --- restored with its tables; refused while a live object holds its name ------------------------


@pytest.mark.parametrize("kind", KINDS)
async def test_a_restore_brings_it_back_at_its_id_with_its_tables(plane, kind):
    async with plane.acquire() as conn:
        ledger = await _retire_ledger(conn)
        await REPOS[kind].delete(conn, HOLDER[kind])
        key = (await _retired(conn, kind))["id"]
        assert await REPOS[kind].restore(conn, key) is True
        row = await REPOS[kind].get(conn, HOLDER[kind])
        assert row is not None
        assert (row["retired"], row["status_by"], row["retired_id"]) == (False, None, None)
        assert await REPOS[kind].list_all(conn, retired=True) == []
        table = await _ledger(conn)
        assert (table["id"], table["retired"], table[COLUMN[kind]]) == (
            ledger,
            False,
            HOLDER[kind],
        )
        assert await REPOS[kind].restore(conn, key) is False


@pytest.mark.parametrize("kind", KINDS)
async def test_a_restore_is_refused_while_a_live_one_holds_its_name(plane, kind):
    async with plane.acquire() as conn:
        await _retire_ledger(conn)
        await REPOS[kind].delete(conn, HOLDER[kind])
        key = (await _retired(conn, kind))["id"]
        table = sources if kind == "source" else domains
        values = {"id": HOLDER[kind], **({"type": "postgresql"} if kind == "source" else {})}
        await conn.execute_core(insert(table).values(**values))
        with pytest.raises(NameHeld) as held:
            await REPOS[kind].restore(conn, key)
        assert (held.value.kind, held.value.name) == (kind, HOLDER[kind])
        assert (await _retired(conn, kind))["id"] == key
        assert (await _ledger(conn))["retired"] is True


# --- nothing is removed --------------------------------------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
async def test_nothing_is_removed(plane, kind):
    async with plane.acquire() as conn:
        ledger = await _retire_ledger(conn)
        await REPOS[kind].delete(conn, HOLDER[kind])
        key = (await _retired(conn, kind))["id"]
        columns = await conn.execute_core(
            select(table_columns.c.column_name, table_columns.c.domain_id).where(
                table_columns.c.table_id == ledger
            )
        )
        domain_of_columns = "archive" if kind == "source" else key
        assert sorted(columns.fetchall()) == [("id", domain_of_columns), ("n", domain_of_columns)]
        tag_source = (await conn.execute_core(select(tag_assignments.c.source_id))).scalar_one()
        assert tag_source == (key if kind == "source" else "old")
