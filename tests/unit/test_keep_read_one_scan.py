# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's governed relations are pruned to the outputs it reads (REQ-039), in one scan of
the statement: an unlabelled Cypher MATCH is a union of one arm per table, every arm a governed
relation, and scanning the whole statement once per relation spent the request's 60 seconds in
this pass (run 42527446894, e2e/cypher-union-all-email)."""

# Requirements: REQ-039

from __future__ import annotations

import sys

import pytest
import sqlglot.expressions as exp

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from tests.helpers import governed_for_role

pytestmark = pytest.mark.unit

_T = TableMeta(
    table_id=1,
    field_name="t",
    type_name="T",
    source_id="pg",
    catalog_name="pg",
    schema_name="public",
    table_name="t",
)
_COLUMNS = {1: [("id", "integer"), ("email", "varchar"), ("region", "varchar")]}


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {"t": _T}
    ctx.joins = {}
    return ctx


def _union(arms: int) -> str:
    return " UNION ALL ".join('SELECT n."email" FROM "public"."t" AS n' for _ in range(arms))


def _whole_statement_scans(monkeypatch, sql: str) -> int:
    """How many times the pruning pass walks the whole statement."""
    seen = {"walks": 0}
    walk = exp.Expr.walk

    def counting(self, *args, **kwargs):
        frame = sys._getframe(1)
        for _ in range(3):
            if frame is None:
                break
            if frame.f_globals.get("__name__") == "provisa.compiler.governed_relation":
                if self.parent is None:
                    seen["walks"] += 1
                break
            frame = frame.f_back
        return walk(self, *args, **kwargs)

    monkeypatch.setattr(exp.Expr, "walk", counting)
    governed_for_role(sql, _ctx(), "clerk", column_types=_COLUMNS)
    return seen["walks"]


def test_a_union_of_many_relations_is_scanned_once(monkeypatch):
    assert _whole_statement_scans(monkeypatch, _union(30)) <= 2


def test_the_scan_count_does_not_grow_with_the_relations(monkeypatch):
    few = _whole_statement_scans(monkeypatch, _union(3))
    many = _whole_statement_scans(monkeypatch, _union(30))
    assert many == few


def test_each_relation_still_serves_only_what_the_statement_reads():
    governed = governed_for_role(_union(2), _ctx(), "clerk", column_types=_COLUMNS)
    assert '"email"' in governed
    assert "region" not in governed
    assert '"id"' not in governed
