# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1d0b57-4f0e-4b3b-9d52-0a1f3e7c9a11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1586: a junction relationship with a composite key is approved as every column pair of each
leg, in order; a partial key or a mismatched pair is refused; keys of unequal length are a
malformed declaration."""

from __future__ import annotations

import dataclasses

import pytest
import sqlglot

from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_types import JunctionMeta
from provisa.compiler.sql_validator import (
    approved_joins,
    computed_joins,
    tables_outside_relationships,
)
from provisa.compiler.stage2 import GovernanceContext

_COLS = {
    1: ["a", "b", "name"],
    2: ["x", "y", "name"],
    3: ["sa", "sb", "p", "q"],
}


def _meta(table_id: int, name: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name=name,
        domain_id="d",
    )


def _model(via: JunctionMeta | None = None, **join) -> tuple[CompilationContext, GovernanceContext]:
    left, right, link = _meta(1, "left_t"), _meta(2, "right_t"), _meta(3, "link")
    ctx = CompilationContext()
    ctx.tables = {"left_t": left, "right_t": right, "link": link}
    via = via or JunctionMeta(table=link, source_columns=("sa", "sb"), target_columns=("p", "q"))
    ctx.joins = {
        ("Left_t", "rights"): JoinMeta(
            source_column=join.get("source_column", "a,b"),
            target_column=join.get("target_column", "x,y"),
            source_column_type="integer",
            target_column_type="integer",
            target=right,
            cardinality="many-to-many",
            via=via,
        )
    }
    gov = GovernanceContext()
    gov.table_map = {"left_t": 1, "right_t": 2, "link": 3}
    gov.all_columns = {tid: [(c, "integer") for c in cols] for tid, cols in _COLS.items()}
    return ctx, gov


def _guard(sql: str, ctx, gov) -> list[str]:
    computed, constants = computed_joins(ctx)
    return [
        v.message
        for v in tables_outside_relationships(
            sqlglot.parse_one(sql, read="postgres"),
            gov,
            approved_joins(ctx),
            {m.table_id: m for m in ctx.tables.values()},
            computed=computed,
            constants=constants,
        )
    ]


def test_every_pair_of_each_composite_leg_is_approved_in_both_directions():
    ctx, _ = _model()
    approved = approved_joins(ctx)
    assert {
        (1, 3, "a", "sa"),
        (1, 3, "b", "sb"),
        (3, 1, "sa", "a"),
        (3, 1, "sb", "b"),
        (3, 2, "p", "x"),
        (3, 2, "q", "y"),
        (2, 3, "x", "p"),
        (2, 3, "y", "q"),
        (1, 2, "a", "x"),
        (1, 2, "b", "y"),
    } <= approved
    assert not any("," in col for *_, c1, c2 in approved for col in (c1, c2))


def test_sql_joining_through_the_junction_on_the_whole_key_is_approved():
    ctx, gov = _model()
    sql = (
        "SELECT l.name FROM left_t l JOIN link k ON k.sa = l.a AND k.sb = l.b "
        "JOIN right_t r ON r.x = k.p AND r.y = k.q"
    )
    assert _guard(sql, ctx, gov) == []


def test_a_mismatched_pair_is_refused_by_name():
    ctx, gov = _model()
    sql = "SELECT l.name FROM left_t l JOIN link k ON k.sa = l.b AND k.sb = l.a"
    (said,) = _guard(sql, ctx, gov)
    assert "left_t" in said and "link" in said, said


def test_a_partial_key_is_refused_by_name():
    ctx, gov = _model()
    (said,) = _guard("SELECT l.name FROM left_t l JOIN link k ON k.sa = l.a", ctx, gov)
    assert "link" in said, said


def test_keys_of_unequal_length_are_refused():
    ctx, _ = _model(
        via=JunctionMeta(table=_meta(3, "link"), source_columns=("sa",), target_columns=("p", "q"))
    )
    with pytest.raises(ValueError):
        approved_joins(ctx)


def test_a_single_column_junction_is_unchanged():
    link = _meta(3, "link")
    ctx, _ = _model(
        via=dataclasses.replace(
            JunctionMeta(table=link, source_columns=("sa",), target_columns=("p",))
        ),
        source_column="a",
        target_column="x",
    )
    assert approved_joins(ctx) == {
        (1, 2, "a", "x"),
        (2, 1, "x", "a"),
        (1, 3, "a", "sa"),
        (3, 1, "sa", "a"),
        (3, 2, "p", "x"),
        (2, 3, "x", "p"),
    }
