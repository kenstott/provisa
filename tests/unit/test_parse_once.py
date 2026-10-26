# Copyright (c) 2026 Kenneth Stott
# Canary: c455491d-68f7-49cb-8191-93263f9d7dbf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement is parsed once per request (REQ-913): within a request's scope a text already
parsed is answered from its kept tree, each caller handed its own copy; a request's run on its
connection loop is that scope; outside one, each call parses."""

# Requirements: REQ-913

from __future__ import annotations

from types import SimpleNamespace

import sqlglot
import sqlglot.expressions as exp

from provisa.core import parse_once as po
from tests.unit.test_governed_plan_stages import pipeline  # noqa: F401

_SQL = 'SELECT "t"."a" FROM "s"."t" AS "t" WHERE "t"."b" = 1'


def _counting(monkeypatch) -> list[str]:
    parses: list[str] = []
    real = sqlglot.parse_one

    def _parse_one(sql, *args, **kwargs):
        parses.append(sql)
        return real(sql, *args, **kwargs)

    # parse_once's own parses: a dialect's set-up parses through sqlglot.parse_one too.
    monkeypatch.setattr(po, "sqlglot", SimpleNamespace(parse_one=_parse_one))
    return parses


def test_a_text_is_parsed_once_in_a_scope(monkeypatch):
    parses = _counting(monkeypatch)
    with po.parse_scope():
        trees = [po.parse_once(_SQL, "postgres") for _ in range(4)]
    assert parses == [_SQL]
    assert all(t.sql(dialect="postgres") == trees[0].sql(dialect="postgres") for t in trees)


def test_each_caller_changes_its_own_copy(monkeypatch):
    _counting(monkeypatch)
    with po.parse_scope():
        first = po.parse_once(_SQL, "postgres")
        first.find(exp.Where).pop()  # what one stage does to the tree it was handed
        assert po.parse_once(_SQL, "postgres").find(exp.Where) is not None


def test_a_dialect_is_its_own_parse(monkeypatch):
    parses = _counting(monkeypatch)
    with po.parse_scope():
        po.parse_once(_SQL, "postgres")
        po.parse_once(_SQL, "duckdb")
    assert parses == [_SQL, _SQL]


def test_a_scope_keeps_its_last_texts(monkeypatch):
    parses = _counting(monkeypatch)
    texts = [f"SELECT {i}" for i in range(po.KEPT + 1)]
    with po.parse_scope():
        for text in texts:
            po.parse_once(text, "postgres")
        po.parse_once(texts[-1], "postgres")  # kept
        po.parse_once(texts[0], "postgres")  # the oldest, no longer kept
    assert parses == [*texts, texts[0]]


def test_outside_a_scope_each_call_parses(monkeypatch):
    parses = _counting(monkeypatch)
    po.parse_once(_SQL, "postgres")
    po.parse_once(_SQL, "postgres")
    assert parses == [_SQL, _SQL]


async def test_the_raw_sql_stage_parses_each_text_it_passes_once(pipeline, monkeypatch):  # noqa: F811
    """REQ-913: one statement through the raw-SQL stage -- validation, governance, the row limit,
    the lowering and the transpile -- reparsed the text each step was handed, twelve parses of
    five texts. Each text is parsed once per request now, and the next request parses its own."""
    from collections import Counter

    from sqlglot import parser as sg_parser

    seen: Counter = Counter()
    real = sg_parser.Parser.parse

    def _counting(self, raw_tokens, sql=None):
        seen[" ".join((sql or "").split())] += 1
        return real(self, raw_tokens, sql)

    monkeypatch.setattr(sg_parser.Parser, "parse", _counting)
    for value in (7, 8):
        seen.clear()
        await pipeline.mod._govern_and_route(
            f"SELECT o.id FROM sales.orders o WHERE o.id = {value}", "analyst"
        )
        assert len(seen) >= 4, seen
        assert set(seen.values()) == {1}, f"a text was parsed more than once: {seen}"
