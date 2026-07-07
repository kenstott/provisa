# Copyright (c) 2026 Kenneth Stott
# Canary: d5775c6d-bce5-4694-b044-8b68ab788cd7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for JSONB field promotions (Phase U / REQ-119)."""

import pytest

from provisa.api_source.promotions import (
    apply_promotions,
    dot_path_to_pg_expression,
    generate_promotion_ddl,
)
from provisa.api_source.models import PromotionConfig


# --- Dot-path to PG expression ---


def test_single_level_path():
    expr = dot_path_to_pg_expression("data", "city")
    assert expr == "(data->>'city')"


def test_two_level_path():
    expr = dot_path_to_pg_expression("data", "address.city")
    assert expr == "(data->'address'->>'city')"


def test_three_level_path():
    expr = dot_path_to_pg_expression("data", "a.b.c")
    assert expr == "(data->'a'->'b'->>'c')"


def test_empty_path_raises():
    with pytest.raises(ValueError):
        dot_path_to_pg_expression("data", "")


# --- DDL generation ---


def test_generate_promotion_ddl_integer():
    promotions = [
        PromotionConfig(
            jsonb_column="data",
            field="user.age",
            target_column="user_age",
            target_type="integer",
        ),
    ]
    stmts = generate_promotion_ddl("api_cache_users", promotions)
    assert len(stmts) == 1
    assert "ALTER TABLE api_cache_users" in stmts[0]
    assert "ADD COLUMN IF NOT EXISTS user_age INTEGER" in stmts[0]
    assert "GENERATED ALWAYS AS" in stmts[0]
    assert "(data->'user'->>'age')::integer" in stmts[0]
    assert "STORED" in stmts[0]


def test_generate_promotion_ddl_text():
    """Text type has no cast suffix."""
    promotions = [
        PromotionConfig(
            jsonb_column="meta",
            field="name",
            target_column="meta_name",
            target_type="text",
        ),
    ]
    stmts = generate_promotion_ddl("api_cache_items", promotions)
    assert len(stmts) == 1
    # text has empty cast
    assert "(meta->>'name')" in stmts[0]
    assert "::text" not in stmts[0] or "::timestamptz" not in stmts[0]


def test_generate_promotion_ddl_multiple():
    promotions = [
        PromotionConfig(jsonb_column="d", field="a", target_column="col_a", target_type="integer"),
        PromotionConfig(
            jsonb_column="d", field="b.c", target_column="col_bc", target_type="boolean"
        ),
    ]
    stmts = generate_promotion_ddl("tbl", promotions)
    assert len(stmts) == 2
    assert "col_a" in stmts[0]
    assert "col_bc" in stmts[1]
    assert "::boolean" in stmts[1]


def test_generate_promotion_ddl_timestamptz():
    promotions = [
        PromotionConfig(
            jsonb_column="data",
            field="created_at",
            target_column="created_ts",
            target_type="timestamptz",
        ),
    ]
    stmts = generate_promotion_ddl("tbl", promotions)
    assert "::timestamptz" in stmts[0]


# --- REQ-119: end-to-end wiring (execution helper + registration) ---


class _RecordingConn:
    def __init__(self):
        self.executed: list[str] = []

    async def execute(self, sql, *args):
        self.executed.append(sql)


@pytest.mark.asyncio
async def test_apply_promotions_executes_ddl():
    conn = _RecordingConn()
    promotions = [
        PromotionConfig(jsonb_column="data", field="a.b", target_column="ab", target_type="text"),
        PromotionConfig(
            jsonb_column="data", field="n", target_column="n_int", target_type="integer"
        ),
    ]
    n = await apply_promotions(conn, "orders", promotions)
    assert n == 2
    assert len(conn.executed) == 2
    assert all("GENERATED ALWAYS AS" in s and "IF NOT EXISTS" in s for s in conn.executed)


@pytest.mark.asyncio
async def test_apply_promotions_noop_when_empty():
    conn = _RecordingConn()
    assert await apply_promotions(conn, "orders", []) == 0
    assert conn.executed == []


def test_dot_path_cast_source_for_varchar_json():
    # REQ-119: the Trino api-cache stores JSON as varchar, so the source column is
    # cast to jsonb before extraction.
    expr = dot_path_to_pg_expression("data", "addr.city", cast_source=True)
    assert "data::jsonb" in expr
    assert expr.endswith("->>'city')")


@pytest.mark.asyncio
async def test_apply_promotions_cast_source_emits_jsonb_cast():
    conn = _RecordingConn()
    promotions = [
        PromotionConfig(jsonb_column="data", field="city", target_column="city", target_type="text")
    ]
    await apply_promotions(conn, "cache_tbl", promotions, cast_source=True)
    assert "::jsonb" in conn.executed[0]


def test_table_config_parses_promotions():
    # REQ-119: a steward declares promotions on a table in YAML config.
    from provisa.core.models import Table

    t = Table(
        source_id="s",
        domain_id="d",
        schema="public",
        table="t",
        columns=[],
        promotions=[
            {
                "jsonb_column": "data",
                "field": "addr.city",
                "target_column": "city",
                "target_type": "text",
            }
        ],
    )
    assert t.promotions[0]["target_column"] == "city"


def test_register_api_columns_includes_promoted_columns():
    # REQ-119: promoted columns are registered as first-class schema columns.
    from provisa.api_source.models import ApiEndpoint
    from provisa.api_source.schema_integration import register_api_columns

    ep = ApiEndpoint(id=1, source_id="s", path="/x", table_name="people", columns=[])
    tables: list[dict] = []
    col_types: dict = {}
    promotions_map = {
        "people": [
            PromotionConfig(
                jsonb_column="data", field="addr.city", target_column="city", target_type="text"
            )
        ]
    }
    register_api_columns(
        tables, col_types, [ep], domain_id="api", role_ids=["admin"], promotions_map=promotions_map
    )
    assert len(tables) == 1
    col_names = [c["column_name"] for c in tables[0]["columns"]]
    assert "city" in col_names
