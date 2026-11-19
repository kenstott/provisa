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
_VIEWS = {"bank.v": _BODY}

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
    assert not reads_view(_FORGES[forge], "bank.v", unqualified=True)
    assert expand_view_refs(_FORGES[forge], _VIEWS) == _FORGES[forge]


def test_a_real_reference_beside_one_in_text_is_expanded_and_marked():
    sql = expand_view_refs(
        "SELECT t.name, 'FROM bank.v' AS s FROM bank.v t -- FROM bank.v\n", _VIEWS, mark=True
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    marked = [s for s in tree.find_all(exp.Subquery) if marked_view(s.alias) is not None]
    assert [marked_view(s.alias) for s in marked] == ["bank.v"]
    assert tree.find(exp.Literal, bfs=False).this == "FROM bank.v"
    assert reads_view("SELECT * FROM bank.v -- FROM bank.w", "bank.v", unqualified=True)


def test_an_unmarked_reference_after_a_comment_is_expanded():
    sql = expand_view_refs("SELECT 1 -- FROM bank.v\nFROM bank.v", {"bank.v": "SELECT 1 AS one"})
    assert sql == 'SELECT 1 -- FROM bank.v\nFROM (SELECT 1 AS one) "v"'


def test_a_marked_body_keeps_its_alias_and_qualified_columns_resolve():
    sql = expand_view_refs('SELECT f.name FROM "bank"."v" AS f', _VIEWS, mark=True)
    tree = sqlglot.parse_one(sql, read="postgres")
    outer = next(s for s in tree.find_all(exp.Subquery) if s.alias == "f")
    inner = outer.this.args["from_"].this
    assert marked_view(inner.alias) == "bank.v"
    assert sqlglot.parse_one(_BODY, read="postgres") == inner.this


# --- Second review (REQ-039, REQ-135) -------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT E'it\\'s FROM bank.v' AS s FROM bank.customer c",
        "SELECT $q$ FROM bank.v $q$ AS s FROM bank.customer c",
        "SELECT $$ FROM bank.v $$ AS s FROM bank.customer c",
        "SELECT 1 /* outer /* FROM bank.v */ FROM bank.v */ FROM bank.customer c",
        "SELECT U&'FROM bank.v' AS s FROM bank.customer c",
    ],
    ids=["escape_string", "dollar_tag", "dollar_bare", "nested_comment", "unicode_string"],
)
def test_a_reference_in_any_literal_or_comment_form_is_no_read(sql):
    assert not reads_view(sql, "bank.v", unqualified=True)
    assert expand_view_refs(sql, _VIEWS) == sql
    assert VIEW_MARK not in expand_view_refs(sql, _VIEWS, mark=True)


def test_a_clients_alias_shaped_like_the_mark_is_not_a_view_body():
    forged = VIEW_MARK[:7] + "0" * (len(VIEW_MARK) - 9) + "__"
    assert forged != VIEW_MARK and len(forged) == len(VIEW_MARK)
    sql = (
        f"SELECT x FROM (SELECT * FROM (SELECT provisa_digest(name) AS x FROM bank.customer c) "
        f'AS "{forged}v") v'
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    assert not [s for s in tree.find_all(exp.Subquery) if marked_view(s.alias) is not None]
    said = _violations(sql)
    assert any(v.startswith("V007") and "'provisa_digest'" in v for v in said), said


def _no_part_of_the_mark(text: str) -> None:
    secret = VIEW_MARK[len("__view_") : -2]  # the random hex
    for k in range(len(secret) - 3):
        assert secret[k : k + 4] not in text, text


def test_a_parse_error_of_a_marked_statement_names_no_mark():
    sql = f'SELECT * FROM (SELECT 1 AS one) AS "{VIEW_MARK}v" WHERE ((('
    said = _violations(sql)
    assert said and said[0].startswith("V000"), said
    _no_part_of_the_mark(said[0])


def test_a_governance_refusal_names_no_mark():
    from provisa.compiler.sql_validator import GovernanceRefused, ValidationViolation

    refused = GovernanceRefused([ValidationViolation("V003", f"no column of {VIEW_MARK}v")])
    assert VIEW_MARK not in str(refused)
    assert VIEW_MARK not in str(refused.params)
    assert all(VIEW_MARK not in v.message for v in refused.violations)


def test_an_error_governing_a_marked_statement_names_no_mark():
    from provisa.compiler.view_expand import marks_withheld

    with pytest.raises(PermissionError) as raised:
        with marks_withheld():
            try:
                raise ValueError(f'"{VIEW_MARK}v"')
            except ValueError as inner:
                raise PermissionError(f"no column of {VIEW_MARK}v") from inner
    assert VIEW_MARK not in str(raised.value)
    assert VIEW_MARK not in str(raised.value.__cause__)


@pytest.mark.parametrize(
    "sql, clause",
    [
        ("SELECT * FROM bank.v TABLESAMPLE SYSTEM (10)", "sample"),
        ("SELECT * FROM bank.v FOR SYSTEM_TIME AS OF 1", "version"),
        ("SELECT * FROM bank.v WITH ORDINALITY", "ordinality"),
    ],
)
def test_a_clause_a_view_cannot_carry_is_refused_naming_it(sql, clause):
    from provisa.compiler.view_expand import ViewClauseNotSupported

    with pytest.raises(ViewClauseNotSupported) as refused:
        expand_view_refs(sql, _VIEWS, mark=True)
    assert refused.value.clause == clause
    assert refused.value.view == "bank.v"


def test_a_view_in_a_parenthesised_join_keeps_its_join():
    sql = expand_view_refs(
        "SELECT * FROM (bank.v AS a JOIN bank.account t ON true) j", _VIEWS, mark=True
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    assert "account" in {t.name for t in tree.find_all(exp.Table)}, sql
    outer = next(s for s in tree.find_all(exp.Subquery) if s.alias == "a")
    assert [j.this.name for j in outer.args["joins"]] == ["account"]


def test_only_on_a_view_reads_the_view():
    sql = expand_view_refs("SELECT * FROM ONLY bank.v", _VIEWS, mark=True)
    tree = sqlglot.parse_one(sql, read="postgres")
    assert [marked_view(s.alias) for s in tree.find_all(exp.Subquery) if marked_view(s.alias)] == [
        "bank.v"
    ]


def test_a_cte_named_like_a_view_is_the_cte():
    sql = "WITH v AS (SELECT 1 AS one) SELECT * FROM v"
    assert expand_view_refs(sql, _VIEWS, mark=True) == sqlglot.parse_one(sql, read="postgres").sql(
        dialect="postgres"
    )
    assert expand_view_refs(sql, _VIEWS) == sql
    assert not reads_view(sql, "bank.v", unqualified=True)


def test_a_non_recursive_cte_reads_the_view_it_shadows_in_its_own_body():
    sql = expand_view_refs("WITH v AS (SELECT * FROM v) SELECT * FROM v", _VIEWS, mark=True)
    tree = sqlglot.parse_one(sql, read="postgres")
    marked = [marked_view(s.alias) for s in tree.find_all(exp.Subquery) if marked_view(s.alias)]
    assert marked == ["bank.v"], sql
    assert tree.find(exp.CTE).find(exp.Subquery) is not None  # inside the CTE's body


def test_an_unquoted_name_folds_to_lower_case():
    for sql in ("SELECT * FROM bank.V", "select * from V", 'SELECT * FROM "bank"."v"'):
        marked = expand_view_refs(sql, _VIEWS, mark=True)
        assert VIEW_MARK in marked, sql
        assert reads_view(sql, "bank.v", unqualified=True), sql
    for sql in ('SELECT * FROM "V"', 'SELECT * FROM bank."V"'):
        assert VIEW_MARK not in expand_view_refs(sql, _VIEWS, mark=True), sql
        assert not reads_view(sql, "bank.v", unqualified=True), sql


def test_a_schema_keyed_view_is_read_only_under_its_schema_folded():
    views = {"ontology.thing": "SELECT 1 AS one"}
    assert VIEW_MARK in expand_view_refs("SELECT * FROM ONTOLOGY.Thing", views, mark=True)
    assert VIEW_MARK not in expand_view_refs("SELECT * FROM other.thing", views, mark=True)
    # An ontology Class is never read by its name alone (REQ-2163).
    assert VIEW_MARK not in expand_view_refs("SELECT * FROM thing", views, mark=True)
    assert reads_view("SELECT * FROM ONTOLOGY.Thing", "ontology.thing", unqualified=False)
    assert not reads_view("SELECT * FROM other.thing", "ontology.thing", unqualified=False)
