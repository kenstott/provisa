# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9a1c62-7b0e-4d85-9e21-c4a8d6f0b317
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view body's mark comes only from a view the server expands (REQ-039, REQ-603).

Governance reads a marked derived table as the administrator's view body. A view reference
written inside a comment, a string literal or a quoted identifier is no reference: expanding it
there would paste the body into that text, where the body's own newline, ``*/``, quote or closing
parenthesis ends it and wraps the client's own SELECT in the mark.
"""

# Requirements: REQ-039, REQ-603, REQ-135

from __future__ import annotations

import sqlglot
from sqlglot import exp

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.compiler.view_expand import VIEW_MARK, expand_view_refs, marked_view, reads_view

_ROLE = {"id": "teller", "capabilities": [], "domain_access": ["*"]}

#: Bodies whose text ends what a client wraps a reference in: a newline (a line comment), a
#: ``*/`` (a block comment), a quote (a string literal), a double quote (a quoted identifier).
_BODY = (
    "SELECT c.name /* the 'customer' \"name\" */\n"
    "FROM bank.customer c -- it's the customer\n"
    "WHERE c.name <> 'x'"
)
_VIEWS = {"v": _BODY}

_FORGES = {
    "line_comment": (
        "SELECT x FROM ((SELECT provisa_digest(name) AS x -- FROM bank.v /*\n"
        "FROM bank.customer c) AS q) v -- */"
    ),
    "block_comment": (
        "SELECT x FROM ((SELECT provisa_digest(name) AS x /* FROM bank.v */ "
        "FROM bank.customer c) AS q) v"
    ),
    "string_literal": (
        "SELECT x FROM ((SELECT provisa_digest(name) AS x, 'FROM bank.v' AS s "
        "FROM bank.customer c) AS q) v"
    ),
    "quoted_identifier": (
        'SELECT x FROM ((SELECT provisa_digest(name) AS "FROM bank.v" FROM bank.customer c) AS q) v'
    ),
}


def _gov() -> GovernanceContext:
    return GovernanceContext(
        rls_rules={}, masking_rules={}, visible_columns={}, all_columns={}, table_map={}
    )


def _violations(sql: str) -> list[str]:
    return [
        f"{v.code} {v.message}"
        for v in validate_sql(
            sql, CompilationContext(), _gov(), _ROLE, [], bypass_relationship_guard=True
        )
    ]


def _validated(sql: str) -> str:
    """As mv.view_read.semantic_statement builds the statement governance validates."""
    return expand_view_refs(expand_view_refs(sql, _VIEWS, mark=True), _VIEWS, mark=True)


@pytest.mark.parametrize("forge", sorted(_FORGES))
def test_a_reference_a_client_hides_in_text_is_not_expanded(forge):
    sql = _validated(_FORGES[forge])
    assert VIEW_MARK not in sql, sql
    tree = sqlglot.parse_one(sql, read="postgres")
    assert not [s for s in tree.find_all(exp.Subquery) if marked_view(s.alias) is not None]


@pytest.mark.parametrize("forge", sorted(_FORGES))
def test_the_clients_own_call_stays_refused(forge):
    said = _violations(_validated(_FORGES[forge]))
    assert any(v.startswith("V007") and "'provisa_digest'" in v for v in said), said


@pytest.mark.parametrize("forge", sorted(_FORGES))
def test_a_reference_in_text_is_no_read_of_the_view(forge):
    assert not reads_view(_FORGES[forge], "v")
    assert expand_view_refs(_FORGES[forge], _VIEWS) == _FORGES[forge]


def test_a_real_reference_beside_one_in_text_is_expanded_and_marked():
    sql = expand_view_refs(
        "SELECT t.name, 'FROM bank.v' AS s FROM bank.v t -- FROM bank.v\n", _VIEWS, mark=True
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    marked = [s for s in tree.find_all(exp.Subquery) if marked_view(s.alias) is not None]
    assert [marked_view(s.alias) for s in marked] == ["v"]
    assert tree.find(exp.Literal, bfs=False).this == "FROM bank.v"
    assert reads_view("SELECT * FROM bank.v -- FROM bank.w", "v")


def test_an_unmarked_reference_after_a_comment_is_expanded():
    sql = expand_view_refs("SELECT 1 -- FROM bank.v\nFROM bank.v", {"v": "SELECT 1 AS one"})
    assert sql == "SELECT 1 -- FROM bank.v\nFROM (SELECT 1 AS one) v"


def test_a_marked_body_keeps_its_alias_and_qualified_columns_resolve():
    sql = expand_view_refs('SELECT f.name FROM "bank"."v" AS f', _VIEWS, mark=True)
    tree = sqlglot.parse_one(sql, read="postgres")
    outer = next(s for s in tree.find_all(exp.Subquery) if s.alias == "f")
    inner = outer.this.args["from_"].this
    assert marked_view(inner.alias) == "v"
    assert sqlglot.parse_one(_BODY, read="postgres") == inner.this
