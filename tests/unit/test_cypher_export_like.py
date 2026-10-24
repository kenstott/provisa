# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL read's LIKE and ILIKE conditions exported as Cypher (REQ-351): ``=~`` over the regex
matching the same values, so the Cypher answers on DuckDB what the read's SQL does, for each
shape a pattern takes. A LIKE with no exact regex is refused, never written approximately."""

# Requirements: REQ-351, REQ-2135

import pytest

from provisa.cypher.sql_to_cypher_helpers import (
    UntranslatableSubquery,
    _sql_to_cypher_expr,
    like_regex,
)
from tests.unit import permits_on_duckdb as permits

_ADDRESSES = [
    "12 Oak St",
    "120 Oak Street",
    "1.2 Main",
    "102 Main",
    "a%b",
    "axb",
    "it's here",
    "OAK lane",
    "two\nlines oak",
    "(x) [y] {z} ^$|?*+",
    "back\\slash",
    "",
]


@pytest.fixture(scope="module")
def model():
    return permits.model()


@pytest.mark.parametrize(
    ("op", "pattern"),
    [
        ("like", "12%"),  # a prefix
        ("like", "%Street"),  # a suffix
        ("like", "%Oak%"),  # a substring
        ("like", "1_2 Main"),  # one character, the dot among them
        ("like", "1.2%"),  # a regex metacharacter, itself
        ("like", "a\\%b"),  # an escaped wildcard, itself
        ("like", "it's%"),  # a quote
        ("like", "%(x) [y] {z} ^$|?*+"),  # every other metacharacter
        ("like", "back\\\\slash"),  # an escaped escape character
        ("like", "two%oak"),  # % across a newline
        ("like", "%"),  # every value
        ("like", ""),  # only the empty value
        ("like", "12 Oak St"),  # no wildcard: equality
        ("ilike", "%oak%"),  # case-insensitive
        ("ilike", "OAK_LANE"),
    ],
)
def test_an_exported_like_answers_as_its_sql_does(model, op, pattern):
    from graphql import parse

    from provisa.compiler.sql_gen import compile_query
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher

    ctx, lm, _view_sql = model
    rows = [{"address": a} for a in _ADDRESSES]
    document = parse(
        "query($p: String!) { c__permits(where: {address: {%s: $p}}) { address } }" % op
    )
    [own] = compile_query(document, ctx, {"p": pattern})
    expected = sorted(permits.answer(own.sql, own.params, model, rows))
    cypher = semantic_sql_to_cypher(make_semantic_sql(own.sql, ctx), lm, ctx, params=own.params)
    assert cypher is not None and "=~" in cypher, cypher
    assert sorted(permits.cypher_answer(cypher, model, rows)) == expected, cypher


def test_the_regex_of_each_pattern_character():
    assert like_regex("a%b_c", case_insensitive=False) == "(?s)^a.*b.c$"
    assert like_regex("a.b(c)", case_insensitive=True) == "(?is)^a\\.b\\(c\\)$"
    assert like_regex("50\\%\\_", case_insensitive=False) == "(?s)^50%_$"


def test_a_like_with_no_exact_regex_is_refused():
    with pytest.raises(UntranslatableSubquery, match="ESCAPE"):
        _sql_to_cypher_expr("t.a LIKE 'x!%' ESCAPE '!'")
    with pytest.raises(UntranslatableSubquery, match="no literal"):
        _sql_to_cypher_expr("t.a LIKE t.b")
    with pytest.raises(UntranslatableSubquery, match="ending in its escape"):
        _sql_to_cypher_expr("t.a LIKE 'x\\'")


def test_a_string_literal_is_written_as_cypher_writes_it():
    # Its characters are its value: a quote escaped as Cypher escapes it, and nothing in it read
    # as SQL (TRUE stays TRUE, a quoted word keeps its quotes).
    assert _sql_to_cypher_expr("t.\"a\" = 'it''s TRUE \"x\"'") == "t.a = 'it\\'s TRUE \"x\"'"
