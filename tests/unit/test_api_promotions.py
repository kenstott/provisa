# Copyright (c) 2025 Kenneth Stott
# Canary: d5775c6d-bce5-4694-b044-8b68ab788cd7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for JSONB field promotions (Phase U)."""

import pytest

from provisa.api_source.promotions import dot_path_to_pg_expression, generate_promotion_ddl
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
        PromotionConfig(jsonb_column="d", field="b.c", target_column="col_bc", target_type="boolean"),
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
