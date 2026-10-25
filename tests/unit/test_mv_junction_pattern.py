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
materializes carries the via leg and its discriminator. These are pure calls -- the build's
join in the model's terms, as handed to the one pipeline (REQ-1922) -- with the model's names and
the registry's columns standing in for a server. The rewrite onto such a view is
test_view_reinject.py's.
"""

# Requirements: REQ-1586

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from tests.helpers import src_table
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.refresh import _build_refresh_sql


async def _built_sql(monkeypatch, mv: MVDefinition, columns: dict[str, list[str]]) -> str:
    """The statement a build of ``mv`` hands the one pipeline, with each table the model names in
    domain ``zoo`` and registers with ``columns``."""
    from provisa.api.app import state
    from provisa.federation import query_residency, registry_view
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route

    def _meta(table: str) -> SimpleNamespace:
        return SimpleNamespace(
            source_id="src",
            schema_name="public",
            table_name=table,
            original_table_name="",
            domain_id="zoo",
            display_name=table,
            field_name=table,
        )

    monkeypatch.setattr(
        state, "view_context", SimpleNamespace(tables={t: _meta(t) for t in columns}), raising=False
    )

    async def _tables(_state):
        return [
            SimpleNamespace(
                source_id="src",
                schema_name="public",
                table_name=t,
                columns=[SimpleNamespace(name=c) for c in cols],
            )
            for t, cols in columns.items()
        ]

    routed: list[str] = []

    async def _govern_and_route(sql, _role, **_kw):
        routed.append(sql)
        return SimpleNamespace(route=Route.ENGINE, physical_sql=sql, exec_params=None, columns=None)

    async def _resident(_state, _plan):
        return None

    monkeypatch.setattr(registry_view, "registered_tables", _tables)
    monkeypatch.setattr(_pipeline, "_govern_and_route", _govern_and_route)
    monkeypatch.setattr(query_residency, "prepare_engine_residency", _resident)
    await _build_refresh_sql(mv, object())
    [sql] = routed
    return sql


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
        inputs=[src_table(t) for t in ("pets", "pet_companions", "pets")],
        target_catalog="iceberg",
        target_schema="mv",
        target_table="mv_bonded_pair",
        join_pattern=_junction_jp(type_value),
    )
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = time.time() - 5
    return mv


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
    async def test_refresh_sql_joins_through_the_junction(self, monkeypatch):
        sql = await _built_sql(
            monkeypatch,
            _junction_mv(),
            {"pets": ["id", "name"], "pet_companions": ["id", "pet_id", "since", "note"]},
        )

        # Each table is named as the model names it, under its registered name as alias.
        assert 'FROM "zoo"."pets" AS "pets" ' in sql
        assert (
            'LEFT JOIN "zoo"."pet_companions" AS "pet_companions" '
            'ON "pets"."id" = "pet_companions"."pet_id"'
        ) in sql
        assert (
            'LEFT JOIN "zoo"."pets" AS "pets" ON "pet_companions"."companion_pet_id" = "pets"."id"'
        ) in sql

    @pytest.mark.asyncio
    async def test_refresh_sql_carries_the_edge_attributes(self, monkeypatch):
        sql = await _built_sql(
            monkeypatch,
            _junction_mv(),
            {"pets": ["id", "name"], "pet_companions": ["id", "pet_id", "since", "note"]},
        )

        assert '"pet_companions"."since" AS "pet_companions__since"' in sql
        assert '"pet_companions"."note" AS "pet_companions__note"' in sql

    @pytest.mark.asyncio
    async def test_refresh_sql_restricts_to_the_discriminator_value(self, monkeypatch):
        sql = await _built_sql(
            monkeypatch,
            _junction_mv(),
            {"pets": ["id", "name"], "pet_companions": ["id", "pet_id", "relation_type"]},
        )

        assert sql.endswith('WHERE "pet_companions"."relation_type" = \'bonded pair\'')

    @pytest.mark.asyncio
    async def test_undiscriminated_junction_has_no_where_clause(self, monkeypatch):
        sql = await _built_sql(
            monkeypatch,
            _junction_mv(type_value=None),
            {"pets": ["id", "name"], "pet_companions": ["id", "pet_id"]},
        )

        assert "WHERE" not in sql
