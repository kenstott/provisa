# Copyright (c) 2025 Kenneth Stott
# Canary: 37f5755c-04e9-4d70-b61f-360af05aaa66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for sampling mode."""

from provisa.compiler.sampling import apply_sampling
from provisa.compiler.sql_gen import ColumnRef, CompiledQuery


def _compiled(sql):
    return CompiledQuery(
        sql=sql, params=[], root_field="orders",
        columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources={"pg"},
    )


class TestApplySampling:
    def test_adds_limit_when_none(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders"'), 100)
        assert "LIMIT 100" in result.sql

    def test_caps_existing_large_limit(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders" LIMIT 10000'), 100)
        assert "LIMIT 100" in result.sql
        assert "10000" not in result.sql

    def test_keeps_existing_small_limit(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders" LIMIT 5'), 100)
        assert "LIMIT 5" in result.sql

    def test_preserves_where_clause(self):
        sql = 'SELECT "id" FROM "public"."orders" WHERE "region" = $1'
        result = apply_sampling(_compiled(sql), 50)
        assert "WHERE" in result.sql
        assert "LIMIT 50" in result.sql

    def test_preserves_order_by(self):
        sql = 'SELECT "id" FROM "public"."orders" ORDER BY "id"'
        result = apply_sampling(_compiled(sql), 50)
        assert "ORDER BY" in result.sql
        assert "LIMIT 50" in result.sql

    def test_preserves_params(self):
        c = CompiledQuery(
            sql='SELECT "id" FROM "public"."orders"',
            params=["val"],
            root_field="orders",
            columns=[],
            sources={"pg"},
        )
        result = apply_sampling(c, 100)
        assert result.params == ["val"]

    def test_custom_sample_size(self):
        result = apply_sampling(_compiled('SELECT 1 FROM t'), 25)
        assert "LIMIT 25" in result.sql
