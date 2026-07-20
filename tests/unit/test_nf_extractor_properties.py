# Copyright (c) 2026 Kenneth Stott
# Canary: 29131f4d-61d7-42aa-bf00-9a84fc494d86
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for nf_extractor SQL surgery (REQ-264, REQ-599).

These functions rewrite SQL to enforce governance — dropping a JOIN whose native
source is unavailable, pruning UNION branches for an unsatisfiable remote table.
A bug drops the wrong table, corrupts the SQL, or leaves the target behind (a
governance bypass), silently. Generate SQL with a KNOWN set of joined / unioned
tables and assert the surgery does exactly what it claims for every shape:

  * left_join_table_names reports exactly the tables joined;
  * drop_joined_table removes its target and only its target, keeps the SQL valid,
    preserves the projection (dropped columns become NULL, not removed), and is
    idempotent;
  * drop_union_branches_for_table removes the target branch, keeps the other, stays
    valid, and is idempotent.
"""

from __future__ import annotations

import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st

from provisa.compiler.nf_extractor import (
    drop_joined_table,
    drop_union_branches_for_table,
    find_api_table_names,
    left_join_table_names,
)

_BASE = "base"
_TABLES = ["ta", "tb", "tc", "td"]


@st.composite
def _joined_case(draw) -> tuple[str, set[str], str]:
    """A SELECT LEFT JOINing >=2 distinct tables (each projecting a column), plus a
    drop target chosen from them. Returns (sql, joined_tables, target)."""
    joins = draw(st.lists(st.sampled_from(_TABLES), min_size=2, max_size=4, unique=True))
    target = draw(st.sampled_from(joins))
    proj = [f"{_BASE}.id"] + [f"{t}.a AS {t}_a" for t in joins]
    join_sql = " ".join(f"LEFT JOIN {t} ON {t}.id = {_BASE}.id" for t in joins)
    sql = f"SELECT {', '.join(proj)} FROM {_BASE} {join_sql}"
    return sql, set(joins), target


def _proj_count(sql: str) -> int:
    tree = sqlglot.parse_one(sql, dialect="postgres")
    return len(tree.selects) if isinstance(tree, sqlglot.exp.Select) else -1


@settings(max_examples=200, deadline=None)
@given(case=_joined_case())
def test_left_join_names_are_exactly_the_joined_tables(case) -> None:
    sql, joined, _ = case
    assert left_join_table_names(sql) == joined
    # every joined table is also a FROM/JOIN table, alongside the base.
    api = set(find_api_table_names(sql))
    assert joined <= api and _BASE in api


@settings(max_examples=200, deadline=None)
@given(case=_joined_case())
def test_drop_joined_table_removes_only_its_target(case) -> None:
    sql, joined, target = case
    out = drop_joined_table(sql, target)
    assert sqlglot.parse_one(out, dialect="postgres") is not None
    remaining = left_join_table_names(out)
    assert target not in remaining, "target JOIN survived — governance bypass"
    assert (joined - {target}) <= remaining, "an unrelated JOIN was dropped"


@settings(max_examples=200, deadline=None)
@given(case=_joined_case())
def test_drop_joined_table_preserves_projection_and_is_idempotent(case) -> None:
    sql, _, target = case
    out = drop_joined_table(sql, target)
    # Dropped columns are NULL-ed, not removed — the caller's result shape is stable.
    assert _proj_count(out) == _proj_count(sql)
    assert drop_joined_table(out, target) == out


@st.composite
def _union_case(draw) -> tuple[str, str, str]:
    """A two-branch UNION over distinct tables, plus which table's branch to drop.
    Returns (sql, keep_table, drop_table)."""
    keep, drop = draw(st.lists(st.sampled_from(_TABLES), min_size=2, max_size=2, unique=True))
    op = draw(st.sampled_from(["UNION", "UNION ALL"]))
    sql = f"SELECT a FROM {keep} {op} SELECT a FROM {drop}"
    return sql, keep, drop


@settings(max_examples=200, deadline=None)
@given(case=_union_case())
def test_drop_union_branch_removes_only_its_target(case) -> None:
    sql, keep, drop = case
    out = drop_union_branches_for_table(sql, drop)
    assert sqlglot.parse_one(out, dialect="postgres") is not None
    names = set(find_api_table_names(out))
    assert drop not in names, "target union branch survived — governance bypass"
    assert keep in names, "the surviving branch's table was lost"
    assert drop_union_branches_for_table(out, drop) == out
