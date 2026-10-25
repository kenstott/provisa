# Copyright (c) 2026 Kenneth Stott
# Canary: 6c6c8a62-d7bc-41bd-91b0-648d758282f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every statement's relationship guard compares its conditions with every registered
relationship's side (REQ-603, sql_validator.computed_joins); each side's form was a parse per
statement. A form is its side's text alone, so it is kept by that text: a relationship saved with
another side is another text, and the next statement's guard compares with its form."""

# Requirements: REQ-603

from __future__ import annotations

from dataclasses import replace

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler import join_guard
from provisa.compiler.sql_validator import computed_joins
from tests.unit.test_cypher_computed_relationships import _model


def test_a_saved_relationship_s_side_is_the_form_the_next_statement_compares(monkeypatch):
    ctx, _gov, _lm, _edge = _model("source_expr")
    join_guard.registered_form.cache_clear()
    first, _ = computed_joins(ctx)
    assert any("LOWER" in side for _a, _b, side, _t in first)

    # The relationship is saved with another computed side: the next statement sees it.
    ((key, meta),) = ctx.joins.items()
    ctx.joins = {key: replace(meta, source_expr='UPPER({alias}."customer_code")')}
    second, _ = computed_joins(ctx)
    assert any("UPPER" in side for _a, _b, side, _t in second)
    assert not any("LOWER" in side for _a, _b, side, _t in second)


def test_a_form_is_parsed_once_however_many_statements_ask(monkeypatch):
    ctx, _gov, _lm, _edge = _model("source_expr")
    join_guard.registered_form.cache_clear()
    parses: list[str] = []
    real = sqlglot.parse_one

    def _counting(sql, *args, **kwargs):
        parses.append(sql)
        return real(sql, *args, **kwargs)

    monkeypatch.setattr(sqlglot, "parse_one", _counting)
    computed_joins(ctx)
    once = len(parses)
    assert once > 0
    for _ in range(5):
        computed_joins(ctx)
    assert len(parses) == once


def test_a_kept_form_is_text_no_caller_can_change():
    # The kept value is the form's SQL text, immutable: a caller transforming what it was handed
    # changes its own copy, never the next statement's.
    join_guard.registered_form.cache_clear()
    form = join_guard.registered_form('LOWER({alias}."customer_code")')
    assert isinstance(form, str)
    rewritten = sqlglot.parse_one(form, read="postgres").transform(
        lambda n: exp.Literal.string("x") if isinstance(n, exp.Column) else n
    )
    assert rewritten.sql(dialect="postgres") != form
    assert join_guard.registered_form('LOWER({alias}."customer_code")') == form
