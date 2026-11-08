# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8e51d2-6a47-4f19-9b0e-d27a64c1f8b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A qualifier the statement binds is in scope (REQ-039).

The unknown-qualifier rule (J609) refuses a column qualified by a name that matches no relation in
scope. Every name the statement binds is one: a join's alias, whichever join the statement
relates it by -- a recursive CTE's step joining a table to itself (a Cypher walk's ``_nxt``), a
junction joined between two reads of one table (``e__via``), a second read of a table (``t``) --
and a column an UNNEST binds (``UNNEST($1) AS _uw0(__b)``), whose fields ``"__b"."n"`` reads.
A qualifier the statement binds nowhere is still refused.
"""

# Requirements: REQ-039

from __future__ import annotations

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import ValidationViolation, validate_sql
from tests.unit.test_v003_identifier_case import _ROLE, _gov


def _violations(sql: str) -> list[ValidationViolation]:
    # The relationship guard is not what these statements are judged by (a role that bypasses it
    # sends them, as org_admin's Cypher does); every other rule is.
    return validate_sql(
        sql, CompilationContext(), _gov(), _ROLE, [], bypass_relationship_guard=True
    )


def _messages(sql: str) -> list[str]:
    return [f"{v.code}: {v.message}" for v in _violations(sql)]


# -- each alias shape the statement binds is in scope ----------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(
            # A Cypher walk's recursive step: the table joined to itself, the step read by _nxt.
            "WITH RECURSIVE r AS (SELECT c.id AS cur_id FROM bank.customer AS c "
            "UNION ALL SELECT _nxt.id FROM r AS t "
            "INNER JOIN bank.customer AS _cur ON _cur.id = t.cur_id "
            "INNER JOIN bank.customer AS _nxt ON _cur.id = _nxt.id "
            "WHERE _nxt.id > 0) SELECT cur_id FROM r",
            id="recursive-cte-self-reference",
        ),
        pytest.param(
            # A junction-backed edge between two reads of one table (sonnets' ECHOES).
            "SELECT a.id, b.id FROM bank.customer AS a "
            "INNER JOIN bank.customer AS e__via ON a.id = e__via.id "
            "INNER JOIN bank.customer AS b ON e__via.id = b.id "
            "WHERE e__via.name > 'x'",
            id="junction-alias",
        ),
        pytest.param(
            # reviews' BOUGHT_WITH: a second read of a table, filtered by its alias.
            "SELECT p.id, t.id FROM bank.customer AS p "
            "INNER JOIN bank.customer AS t ON p.id = t.id "
            "WHERE t.name > 'x'",
            id="reviews-second-read",
        ),
        pytest.param(
            # REQ-2176's rows unwound: __b is the UNNEST's column, "__b"."n" a field of it.
            "SELECT n.id FROM bank.customer AS n CROSS JOIN UNNEST($1) AS _uw0(__b) "
            'WHERE n.id = "__b"."n"',
            id="unnest-column-alias",
        ),
        pytest.param(
            "SELECT d.x FROM (SELECT c.id AS x FROM bank.customer AS c) AS d WHERE d.x > 0",
            id="derived-table-alias",
        ),
        pytest.param(
            "SELECT c.id, l.n FROM bank.customer AS c "
            "CROSS JOIN LATERAL (SELECT c.name AS n) AS l WHERE l.n > 'x'",
            id="lateral-alias",
        ),
    ],
)
def test_a_column_qualified_by_an_alias_the_statement_binds_is_served(sql):
    assert _messages(sql) == []


# -- a pruned join's alias is still judged as the alias it is ---------------------------------------


def test_a_masked_column_filtered_through_a_second_read_is_refused_as_masked():
    # email is masked: filtering on it infers the plaintext (V005), however the table is joined.
    codes = [
        v.code
        for v in _violations(
            "SELECT p.id FROM bank.customer AS p "
            "INNER JOIN bank.customer AS t ON p.id = t.id WHERE t.email = 'a@b'"
        )
    ]
    assert codes == ["V005"]


@pytest.mark.parametrize("column", ["id", "name", "email", "ssn"])
def test_a_second_read_is_judged_as_one_read_is(column):
    # The second read's alias is judged as the only read's would be, column for column.
    one = [
        v.code for v in _violations(f"SELECT c.id FROM bank.customer AS c WHERE c.{column} = '1'")
    ]
    two = [
        v.code
        for v in _violations(
            "SELECT p.id FROM bank.customer AS p "
            f"INNER JOIN bank.customer AS t ON p.id = t.id WHERE t.{column} = '1'"
        )
    ]
    assert two == one


# -- a qualifier bound nowhere is still refused ----------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # J609's shape: no relation is named x
        "SELECT x.id FROM bank.customer AS c",
        # an UNNEST binds __b, not __c
        "SELECT n.id FROM bank.customer AS n CROSS JOIN UNNEST($1) AS _uw0(__b) "
        'WHERE n.id = "__c"."n"',
        # a column alias of another scope's UNNEST is not in this one
        'SELECT n.id FROM bank.customer AS n WHERE n.id = "__b"."n" '
        "AND EXISTS (SELECT 1 FROM UNNEST($1) AS _uw0(__b))",
        # a join's alias in another branch of a union is not in this one
        "SELECT t.id FROM bank.customer AS p UNION ALL "
        "SELECT p.id FROM bank.customer AS a INNER JOIN bank.customer AS t ON a.id = t.id",
    ],
)
def test_a_qualifier_the_statement_binds_nowhere_in_scope_is_refused(sql):
    assert "names no relation in scope" in " ".join(_messages(sql))
