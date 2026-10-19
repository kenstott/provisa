# Copyright (c) 2026 Kenneth Stott
# Canary: 0c830783-4bac-4cab-8645-7d56aa1f8da2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-086 / REQ-085: an MV with expose_in_sdl is registered as a governed table, and the MV
rewrite keeps the row-level-security predicate and its parameters."""

from __future__ import annotations

import time
import types

from provisa.api import app_loaders
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus, TableIdentity
from provisa.mv.registry import MVRegistry
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.mv.view_reinject import reinject


class _Engine:
    # The engine it stands for names a catalog in its SQL (REQ-1730).
    engine = types.SimpleNamespace(catalog_qualified=True)

    def materialize_store_target(self, org_id):
        return "mat_store", "mv_cache"


def _load(monkeypatch, raw_config: dict) -> tuple[list[MVDefinition], MVRegistry]:
    registry = MVRegistry()
    st = types.SimpleNamespace(
        mv_registry=registry, federation_engine=_Engine(), org_id="root", active_org_id="root"
    )
    monkeypatch.setattr("provisa.api.app.state", st, raising=False)
    return app_loaders._load_mv_and_views_config(raw_config), registry


def _mv_config(**extra) -> dict:
    return {
        "id": "mv-sales",
        "sql": "SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
        "target_table": "sales_by_region",
        **extra,
    }


def test_an_exposed_mv_is_appended_as_a_table_with_its_own_domain_and_columns(
    monkeypatch,
):  # REQ-086
    raw = {
        "tables": [],
        "materialized_views": [
            _mv_config(
                expose_in_sdl=True,
                sdl_config={
                    "domain_id": "sales",
                    "columns": [{"name": "region", "visible_to": ["analyst"]}, {"name": "total"}],
                },
            )
        ],
    }
    loaded, _ = _load(monkeypatch, raw)
    assert loaded[0].expose_in_sdl is True
    assert raw["tables"] == [
        {
            "source_id": "mat_store",
            "domain_id": "sales",
            "schema": "mv_cache",
            "table": "sales_by_region",
            "columns": [{"name": "region", "visible_to": ["analyst"]}, {"name": "total"}],
        }
    ]


def test_an_mv_not_marked_exposed_adds_no_table_to_the_schema(monkeypatch):  # REQ-086
    raw = {"tables": [], "materialized_views": [_mv_config(sdl_config={"domain_id": "sales"})]}
    loaded, registry = _load(monkeypatch, raw)
    assert loaded[0].expose_in_sdl is False
    assert raw["tables"] == []
    assert registry.get("mv-sales") is not None


def _fresh_mv() -> MVDefinition:
    mv = MVDefinition(
        id="mv-orders-customers",
        source_tables=["orders", "customers"],
        inputs=[
            TableIdentity("pg", "public", "orders"),
            TableIdentity("pg", "public", "customers"),
        ],
        target_catalog="postgresql",
        target_schema="mv_cache",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        refresh_interval=300,
    )
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = time.time() - 5
    return mv


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    for i, name in enumerate(("orders", "customers")):
        ctx.tables[name] = TableMeta(
            table_id=i, field_name=name, type_name=name.title(), source_id="pg",
            catalog_name="pg", schema_name="public", table_name=name, domain_id="sales",
        )  # fmt: skip
    return ctx


def test_the_rewrite_keeps_the_rls_predicate_and_its_parameters():  # REQ-085
    """Governance runs first: the role's row filter is in the statement the view is put into,
    and it is carried onto the view's column with its placeholder."""
    sql = (
        'SELECT "o"."id", "c"."name" FROM "pg"."public"."orders" AS "o" '
        'LEFT JOIN "pg"."public"."customers" AS "c" ON "o"."customer_id" = "c"."id" '
        'WHERE ("o"."region" = $1)'
    )
    hit = reinject(sql, [_fresh_mv()], _ctx())
    assert hit is not None
    assert "mv_cache" in hit.sql and "JOIN" not in hit.sql
    assert 'WHERE ("o"."region" = $1)' in hit.sql
