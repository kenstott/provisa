# Copyright (c) 2026 Kenneth Stott
# Canary: 712f0b5a-3b24-4a96-b5f7-fcae4ed0e0a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a column rename or drop would break, as its caller can read it (REQ-1484, REQ-2152).

The dependents scan answers a caller holding ``table_registration``; the right opens the
feature, never more of the model. A dependent the caller cannot read is left out: a view or MV,
a data-quality contract or metric view on a table it is not served, a metric not granted to its
acting role, a relationship whose other end it is not served, a term it cannot read, a command
not assigned to it. A column it is not served is answered as one the table does not have.

The model: ``sales.orders`` (id, total, secret) and ``finance.ledger`` (id, total);
``orders.total`` feeds the views ``sales.big_orders`` and ``finance.fin_view``, the metrics
``open_total`` (everyone) and ``fin_total`` (``finance_role``), the relationship
``orders-ledger`` (orders.total -> ledger.total) and the term registration derived for it
(in sales). The reader is served orders(id, total) and big_orders, and reads no term of sales.
"""

# Requirements: REQ-1484, REQ-2152

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.api.admin.column_dependents import dependents_for
from provisa.api.admin.read_rights import SERVICE_READER, DependentsReader
from provisa.core.models import DERIVED_SOURCE_ID, Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    glossary_term_refs,
    metrics,
    relationships,
    sources,
)

pytestmark = pytest.mark.asyncio


def _table(source: str, domain: str, name: str, columns: list[str], view_sql=None) -> Table:
    return Table(
        source_id=source,
        domain_id=domain,
        schema_name="public",
        table_name=name,
        view_sql=view_sql,
        columns=[Column(name=c, visible_to=[], data_type="integer") for c in columns],
    )


@pytest.fixture
async def model(control_plane, monkeypatch):
    import provisa.api.app as appmod

    monkeypatch.setattr(appmod.state, "mv_registry", None, raising=False)
    monkeypatch.setattr(appmod.state, "tracked_functions", {}, raising=False)
    db = control_plane.org()
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="trino"))
        for domain_id in ("sales", "finance"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        for key, table in (
            ("orders", _table("pg", "sales", "orders", ["id", "total", "secret"])),
            ("ledger", _table("pg", "finance", "ledger", ["id", "total"])),
            (
                "big_orders",
                _table(
                    DERIVED_SOURCE_ID,
                    "sales",
                    "big_orders",
                    ["total"],
                    view_sql="SELECT total FROM sales.orders WHERE total > 100",
                ),
            ),
            (
                "fin_view",
                _table(
                    DERIVED_SOURCE_ID,
                    "finance",
                    "fin_view",
                    ["total"],
                    view_sql="SELECT total FROM sales.orders",
                ),
            ),
        ):
            table_id = await table_repo.upsert(conn, table)
            assert table_id is not None
            ids[key] = table_id
        await conn.execute_core(
            insert(metrics).values(
                [
                    {"name": "open_total", "expression": "SUM(total)", "visible_to": ["*"]},
                    {
                        "name": "fin_total",
                        "expression": "SUM(total)",
                        "visible_to": ["finance_role"],
                    },
                ]
            )
        )
        await conn.execute_core(
            insert(relationships).values(
                id="orders-ledger",
                source_table_id=ids["orders"],
                target_table_id=ids["ledger"],
                source_column="total",
                target_column="total",
                cardinality="many-to-one",
            )
        )
        # Registering a table derives a term for each of its columns (REQ-1387), in its domain.
        ids["term"] = (
            await conn.execute_core(
                select(glossary_term_refs.c.term_id).where(
                    glossary_term_refs.c.table_id == ids["orders"],
                    glossary_term_refs.c.column_name == "total",
                )
            )
        ).scalar_one()
    return db, ids


def _reader(ids: dict[str, int]) -> DependentsReader:
    served = {ids["orders"]: {"id", "total"}, ids["big_orders"]: {"total"}}
    return DependentsReader(
        column=lambda table_id, column: (
            table_id in served and (column is None or column in served[table_id])
        ),
        metric=lambda row: "*" in row["visible_to"],
        term=lambda term_domains: "sales" not in term_domains,
        command=lambda name: False,
    )


def _names(found) -> set[tuple[str, str]]:
    return {(d.kind, d.name) for deps in found.values() for d in deps}


async def test_a_reader_is_answered_only_the_dependents_it_can_read(model):
    db, ids = model
    async with db.acquire() as conn:
        found = await dependents_for(
            conn, ids["orders"], renamed=[], removed=["total"], reader=_reader(ids)
        )
    names = _names(found)
    assert ("view", "sales.big_orders") in names
    assert ("metric", "open_total") in names
    for hidden in ("fin_view", "fin_total", "orders-ledger"):
        assert not any(hidden in name for _kind, name in names), hidden
    assert not any(kind == "glossary" for kind, _name in names)


async def test_the_unnarrowed_scan_answers_every_dependent(model):
    db, ids = model
    async with db.acquire() as conn:
        found = await dependents_for(
            conn, ids["orders"], renamed=[], removed=["total"], reader=SERVICE_READER
        )
    names = _names(found)
    for expected in (
        ("view", "sales.big_orders"),
        ("view", "finance.fin_view"),
        ("metric", "open_total"),
        ("metric", "fin_total"),
        ("relationship", "orders-ledger"),
    ):
        assert expected in names, expected
    assert any(kind == "glossary" for kind, _name in names)


async def test_a_column_the_reader_is_not_served_is_answered_as_one_the_table_lacks(model):
    db, ids = model
    async with db.acquire() as conn:
        with pytest.raises(ValueError) as unserved:
            await dependents_for(
                conn, ids["orders"], renamed=[], removed=["secret"], reader=_reader(ids)
            )
        with pytest.raises(ValueError) as missing:
            await dependents_for(
                conn, ids["orders"], renamed=[], removed=["nope"], reader=_reader(ids)
            )
    assert str(unserved.value).replace("secret", "X") == str(missing.value).replace("nope", "X")
