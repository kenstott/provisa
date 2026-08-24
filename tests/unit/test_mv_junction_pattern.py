# Copyright (c) 2026 Kenneth Stott
# Canary: 0d9da60a-a199-4890-a8cc-1cb2f7295a2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Junction-backed relationships as MV hints (REQ-1586).

A junction edge is two hops through an associative table, so the join pattern it
materializes carries the via leg and its discriminator. These are pure calls — the refresh
SQL builder and the rewriter — with a fake engine standing in for column introspection.
"""

# Requirements: REQ-1586

from __future__ import annotations

import time

import pytest

from provisa.compiler.sql_gen import CompiledQuery
from provisa.executor.result import QueryResult
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.refresh import _build_refresh_sql
from provisa.mv.rewriter import rewrite_if_mv_match


class _FakeEngine:
    """SHOW COLUMNS answers per table from a mapping."""

    def __init__(self, columns: dict[str, list[str]]):
        self._columns = columns

    async def execute_engine(self, sql, *a, **k):
        assert sql.startswith("SHOW COLUMNS FROM ")
        table = sql.split('"')[1]
        return QueryResult(rows=[(c,) for c in self._columns[table]], column_names=[])


def _junction_jp(type_value: str | None = "bonded pair") -> JoinPattern:
    return JoinPattern(
        left_table="pets",
        left_column="id",
        right_table="pets",
        right_column="id",
        via_table="pet_companions",
        via_left_column="pet_id",
        via_right_column="companion_pet_id",
        via_type_column="relation_type" if type_value else None,
        via_type_value=type_value,
    )


def _junction_mv(type_value: str | None = "bonded pair") -> MVDefinition:
    mv = MVDefinition(
        id="auto-mv-pets-bonded-pair",
        source_tables=["pets", "pet_companions", "pets"],
        target_catalog="iceberg",
        target_schema="mv",
        target_table="mv_bonded_pair",
        join_pattern=_junction_jp(type_value),
    )
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = time.time() - 5
    return mv


def _compiled(sql: str) -> CompiledQuery:
    return CompiledQuery(sql=sql, params=[], root_field="pets", columns=[], sources={"pet-store"})


# The shape the traversal compiles to: pets -> pet_companions -> pets, discriminated.
_TRAVERSAL_SQL = (
    'SELECT "t0"."name", "t2"."name" '
    'FROM "pet_store"."pets" "t0" '
    'LEFT JOIN "pet_store"."pet_companions" "t1" ON "t0"."id" = "t1"."pet_id" '
    'LEFT JOIN "pet_store"."pets" "t2" ON "t1"."companion_pet_id" = "t2"."id" '
    'WHERE "t1"."relation_type" = \'bonded pair\''
)


class TestJoinPatternDeclaration:
    def test_via_table_requires_both_key_columns(self):
        with pytest.raises(ValueError, match="via_left_column and via_right_column"):
            JoinPattern(
                left_table="pets",
                left_column="id",
                right_table="pets",
                right_column="id",
                via_table="pet_companions",
                via_left_column="pet_id",
            )

    def test_via_columns_require_a_via_table(self):
        with pytest.raises(ValueError, match="require via_table"):
            JoinPattern(
                left_table="pets",
                left_column="id",
                right_table="pets",
                right_column="id",
                via_left_column="pet_id",
                via_right_column="companion_pet_id",
            )

    def test_discriminator_column_and_value_are_declared_together(self):
        with pytest.raises(ValueError, match="declared together"):
            JoinPattern(
                left_table="pets",
                left_column="id",
                right_table="pets",
                right_column="id",
                via_table="pet_companions",
                via_left_column="pet_id",
                via_right_column="companion_pet_id",
                via_type_column="relation_type",
            )

    def test_is_junction_reads_via_table(self):
        assert _junction_jp().is_junction is True
        assert (
            JoinPattern(
                left_table="orders",
                left_column="customer_id",
                right_table="customers",
                right_column="id",
            ).is_junction
            is False
        )


class TestJunctionRefreshSQL:
    @pytest.mark.asyncio
    async def test_refresh_sql_joins_through_the_junction(self):
        engine = _FakeEngine(
            {"pets": ["id", "name"], "pet_companions": ["id", "pet_id", "since", "note"]}
        )
        sql = await _build_refresh_sql(_junction_mv(), engine)

        assert 'LEFT JOIN "pet_companions" ON "pets"."id" = "pet_companions"."pet_id"' in sql
        assert 'LEFT JOIN "pets" ON "pet_companions"."companion_pet_id" = "pets"."id"' in sql

    @pytest.mark.asyncio
    async def test_refresh_sql_carries_the_edge_attributes(self):
        engine = _FakeEngine(
            {"pets": ["id", "name"], "pet_companions": ["id", "pet_id", "since", "note"]}
        )
        sql = await _build_refresh_sql(_junction_mv(), engine)

        assert '"pet_companions"."since" AS "pet_companions__since"' in sql
        assert '"pet_companions"."note" AS "pet_companions__note"' in sql

    @pytest.mark.asyncio
    async def test_refresh_sql_restricts_to_the_discriminator_value(self):
        engine = _FakeEngine(
            {"pets": ["id", "name"], "pet_companions": ["id", "pet_id", "relation_type"]}
        )
        sql = await _build_refresh_sql(_junction_mv(), engine)

        assert sql.endswith('WHERE "pet_companions"."relation_type" = \'bonded pair\'')

    @pytest.mark.asyncio
    async def test_undiscriminated_junction_has_no_where_clause(self):
        engine = _FakeEngine({"pets": ["id", "name"], "pet_companions": ["id", "pet_id"]})
        sql = await _build_refresh_sql(_junction_mv(type_value=None), engine)

        assert "WHERE" not in sql


class TestJunctionRewrite:
    def test_traversal_reads_the_mv(self):
        result = rewrite_if_mv_match(_compiled(_TRAVERSAL_SQL), [_junction_mv()])

        assert "mv_bonded_pair" in result.sql
        assert result.sources == {"iceberg"}

    def test_both_hops_are_removed(self):
        result = rewrite_if_mv_match(_compiled(_TRAVERSAL_SQL), [_junction_mv()])

        assert "JOIN" not in result.sql

    def test_target_columns_take_the_mv_prefix(self):
        result = rewrite_if_mv_match(_compiled(_TRAVERSAL_SQL), [_junction_mv()])

        assert '"pets__name"' in result.sql

    def test_a_different_edge_type_does_not_read_this_mv(self):
        sql = _TRAVERSAL_SQL.replace("'bonded pair'", "'littermate'")

        result = rewrite_if_mv_match(_compiled(sql), [_junction_mv()])

        assert result.sql == sql
        assert result.sources == {"pet-store"}

    def test_direct_two_table_join_does_not_read_a_junction_mv(self):
        sql = (
            'SELECT "t0"."name" '
            'FROM "pet_store"."pets" "t0" '
            'LEFT JOIN "pet_store"."pets" "t1" ON "t0"."id" = "t1"."id"'
        )

        result = rewrite_if_mv_match(_compiled(sql), [_junction_mv()])

        assert result.sources == {"pet-store"}

    def test_unchained_pair_does_not_match(self):
        # Both tables appear, but the second hop starts from the root rather than the
        # junction — that is a different query, not this edge.
        sql = (
            'SELECT "t0"."name" '
            'FROM "pet_store"."pets" "t0" '
            'LEFT JOIN "pet_store"."pet_companions" "t1" ON "t0"."id" = "t1"."pet_id" '
            'LEFT JOIN "pet_store"."pets" "t2" ON "t0"."id" = "t2"."id" '
            'WHERE "t1"."relation_type" = \'bonded pair\''
        )

        result = rewrite_if_mv_match(_compiled(sql), [_junction_mv()])

        assert result.sources == {"pet-store"}
