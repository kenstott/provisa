# Copyright (c) 2026 Kenneth Stott
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
from provisa.core.connection_loop import connection_loop

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


def test_a_request_s_run_is_its_scope_and_ends_with_it(monkeypatch):
    parses = _counting(monkeypatch)

    async def _request() -> None:
        po.parse_once(_SQL, "postgres")
        po.parse_once(_SQL, "postgres")

    with connection_loop() as cl:
        cl.run(_request())
        cl.run(_request())
    # Once per request: the next request parses its own.
    assert parses == [_SQL, _SQL]
