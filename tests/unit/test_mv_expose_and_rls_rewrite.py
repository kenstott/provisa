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
from provisa.compiler.sql_gen import ColumnRef, CompiledQuery
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.mv.rewriter import rewrite_if_mv_match


class _Engine:
    def materialize_store_target(self, org_id):
        return "mat_store", "mv_cache"


def _load(monkeypatch, raw_config: dict) -> tuple[list[MVDefinition], MVRegistry]:
    registry = MVRegistry()
    st = types.SimpleNamespace(mv_registry=registry, federation_engine=_Engine(), org_id="root")
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


def test_the_rewrite_keeps_the_rls_predicate_and_its_parameters():  # REQ-085
    sql = (
        'SELECT "t0"."id", "t1"."name" FROM "public"."orders" "t0" '
        'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
        'WHERE ("t0"."region" = $1)'
    )
    compiled = CompiledQuery(
        sql=sql,
        params=["us-east"],
        root_field="orders",
        columns=[
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
        ],
        sources={"pg"},
    )
    result = rewrite_if_mv_match(compiled, [_fresh_mv()])
    assert "mv_cache" in result.sql and "JOIN" not in result.sql
    # The alias is rewritten onto the MV target; the predicate and its placeholder are not dropped.
    assert 'WHERE ("region" = $1)' in result.sql
    assert result.params == ["us-east"]
