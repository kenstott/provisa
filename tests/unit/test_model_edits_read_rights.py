# Copyright (c) 2026 Kenneth Stott
# Canary: ea88b85f-53df-4ec8-82e2-1ac61e6b9a27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Model and governance edits reach only what their caller reads (REQ-2152).

The rights that change a table's settings, its write grants, its row rules and its relationships
open those features; they do not reach past the caller's grants. A table it is served no column
of is answered as one that does not exist, and a grant, a filter or a join naming a column it is
not served is refused naming none.

The model: ``orders`` (id served, secret not) and ``payroll`` (nothing served), in ``sales``.
"""

# Requirements: REQ-2152

from __future__ import annotations

import types
from typing import Any, cast

import pytest
from sqlalchemy import insert

from provisa.api.admin import schema_mutation
from provisa.api.admin._table_settings import write_grant_names
from provisa.api.admin.catalog_scope import CatalogScope
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources

pytestmark = pytest.mark.asyncio

_INFO = cast(Any, types.SimpleNamespace(context={}))


@pytest.fixture
async def model(control_plane, monkeypatch):
    db = control_plane.org()
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        for name, columns in (("orders", ["id", "secret"]), ("payroll", ["salary"])):
            table_id = await table_repo.upsert(
                conn,
                Table(
                    source_id="pg",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                    columns=[Column(name=c, visible_to=[], data_type="integer") for c in columns],
                ),
            )
            assert table_id is not None
            ids[name] = table_id

    async def _pool():
        return db

    scope = CatalogScope(
        whole=False,
        reach=None,
        served={ids["orders"]: {"id"}},
        governance=False,
        role="seller",
        compiled=frozenset(ids.values()),
    )
    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)
    return ids


async def test_a_table_the_caller_is_served_nothing_of_is_answered_as_missing(model):
    assert await schema_mutation._table_unread(_INFO, model["orders"]) is None
    refused = await schema_mutation._table_unread(_INFO, model["payroll"])
    assert refused is not None
    assert (refused.code, refused.message) == (
        "schema.table_not_found",
        f"Table {model['payroll']} not found",
    )


async def test_a_row_rule_filter_names_only_columns_the_caller_reads(model):
    from provisa.core.repositories import table as repo

    async def _targets(name):
        pool = await schema_mutation._get_pool()
        async with pool.acquire() as conn:
            return [t for t in await repo.list_all(conn) if t["table_name"] == name]

    rule = types.SimpleNamespace(table_id="orders", domain_id=None, filter_expr="id > 0")
    assert await schema_mutation._rule_beyond_reads(_INFO, rule, await _targets("orders")) is None
    rule.filter_expr = "secret = 'x'"
    refused = await schema_mutation._rule_beyond_reads(_INFO, rule, await _targets("orders"))
    assert refused is not None and refused.code == "schema.rls_rule_invalid"
    assert "secret" not in refused.message
    # One that does not parse is left to the save's parse check (REQ-1676), which follows.
    rule.filter_expr = "secret = = 'x'"
    assert await schema_mutation._rule_beyond_reads(_INFO, rule, await _targets("orders")) is None
    on_payroll = types.SimpleNamespace(table_id="payroll", domain_id=None, filter_expr="1 = 1")
    refused = await schema_mutation._rule_beyond_reads(_INFO, on_payroll, await _targets("payroll"))
    assert refused is not None and refused.code == "schema.table_not_found"


async def test_a_relationship_joins_only_columns_the_caller_reads(model):
    orders = {"id": model["orders"]}
    payroll = {"id": model["payroll"]}

    def _rel(source_column: str, target_column: str) -> Any:
        return types.SimpleNamespace(
            source_table_id="orders",
            target_table_id="orders",
            source_column=source_column,
            target_column=target_column,
        )

    assert (
        await schema_mutation._relationship_beyond_reads(_INFO, _rel("id", "id"), orders, orders)
        is None
    )
    refused = await schema_mutation._relationship_beyond_reads(
        _INFO, _rel("secret", "id"), orders, orders
    )
    assert refused is not None and refused.code == "schema.relationship_column_beyond_reads"
    assert "secret" not in refused.message
    # A column the table lacks is refused the same way: no answer tells which of the two it is.
    missing = await schema_mutation._relationship_beyond_reads(
        _INFO, _rel("nope", "id"), orders, orders
    )
    assert missing is not None and missing.message == refused.message
    to_payroll: Any = types.SimpleNamespace(
        source_table_id="orders",
        target_table_id="payroll",
        source_column="id",
        target_column="salary",
    )
    refused = await schema_mutation._relationship_beyond_reads(_INFO, to_payroll, orders, payroll)
    assert refused is not None and refused.code == "schema.relationship_unknown_target"


def test_a_write_grant_names_its_columns_presets_and_predicates():
    grant = types.SimpleNamespace(
        columns=["id"],
        presets=[types.SimpleNamespace(column="region")],
        rows="owner = current_user",
        check="amount > 0",
    )
    assert write_grant_names(grant) == {"id", "region", "owner", "amount"}
