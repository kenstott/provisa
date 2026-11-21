# Copyright (c) 2026 Kenneth Stott
# Canary: f69109f5-f7cb-491f-8990-f9c54398d310
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A hidden column cannot be reached by the case of a name (REQ-039, amended 2026-11-04).

Identifiers are compared as PostgreSQL folds them: an unquoted name in lower case, a quoted one
exactly. ``SELECT C.ssn FROM bank.customer c`` reads ``c.ssn`` on the engine, so it is refused as
``c.ssn`` is. A column qualified by a name that matches no relation in scope is refused. Stage 2
matches names as written, so a statement is served only when governance holds both as written
and as PostgreSQL reads it: no spelling names a column one reading governs and the other misses.
"""

# Requirements: REQ-039

from __future__ import annotations

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.security.masking import MaskingRule, MaskType

_ROLE = {"id": "teller", "capabilities": [], "domain_access": ["*"]}
_CUSTOMER = [("id", "integer"), ("name", "varchar"), ("ssn", "varchar"), ("email", "varchar")]
_MIXED = [("id", "integer"), ("secret", "varchar")]


def _gov() -> GovernanceContext:
    return GovernanceContext(
        rls_rules={},
        masking_rules={
            (1, "email"): (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
        },
        visible_columns={1: frozenset({"id", "name", "email"}), 3: frozenset({"id"})},
        table_map={"bank.customer": 1, "customer": 1, "bank.MixedCase": 3, "MixedCase": 3},
        all_columns={1: list(_CUSTOMER), 3: list(_MIXED)},
        limit_ceiling=None,
        sample_size=None,
    )


def _codes(sql: str) -> list[str]:
    return [v.code for v in validate_sql(sql, CompilationContext(), _gov(), _ROLE, [])]


# -- H4: a qualifier in any case reads the column it names on the engine --------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT C.ssn FROM bank.customer c",
        "SELECT CUSTOMER.ssn FROM bank.customer",
        "SELECT c.ssn FROM bank.customer AS C",
        "SELECT Customer.ssn FROM bank.customer",
        "SELECT id FROM bank.customer c WHERE EXISTS (SELECT 1 WHERE C.ssn LIKE '1%') "
        "AND C.ssn IS NOT NULL",
        "SELECT (SELECT C.ssn) FROM bank.customer c",
        "SELECT x FROM (SELECT C.ssn AS x FROM bank.customer c) AS d",
    ],
)
def test_an_unquoted_qualifier_is_read_in_lower_case(sql):
    assert "V003" in _codes(sql)


@pytest.mark.parametrize(
    "sql",
    [
        # stage 2 expands no star it cannot resolve; PostgreSQL reads C.* as c.*, ssn included
        "SELECT C.* FROM bank.customer c",
        "SELECT CUSTOMER.* FROM bank.customer",
        'SELECT "C".* FROM bank.customer c',
        "SELECT x.* FROM bank.customer c",
    ],
)
def test_a_star_qualified_by_no_relation_in_scope_is_refused(sql):
    assert "V003" in _codes(sql)


def test_a_star_qualified_as_written_is_the_served_columns():
    assert _codes("SELECT c.* FROM bank.customer c") == []
    governed = apply_governance("SELECT c.* FROM bank.customer c", _gov())
    assert "ssn" not in governed


# -- a qualifier that names no relation in scope is refused ----------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # PostgreSQL: missing FROM-clause entry for table "C" -- never served
        'SELECT "C".ssn FROM bank.customer c',
        "SELECT x.ssn FROM bank.customer c",
        # an alias hides the table's own name
        "SELECT customer.ssn FROM bank.customer c",
        'SELECT "C".id FROM bank.customer c',
    ],
)
def test_a_qualifier_naming_no_relation_in_scope_is_refused(sql):
    assert "V003" in _codes(sql)


# -- the shapes already refused stay refused --------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT c.SSN FROM bank.customer c",
        "SELECT SSN FROM bank.customer",
        'SELECT "SSN" FROM bank.customer',
        "SELECT customer.ssn FROM bank.customer",
        "SELECT bank.customer.ssn FROM bank.customer",
        "SELECT c.ssn FROM bank.customer c",
        "SELECT ssn FROM bank.customer",
    ],
)
def test_a_hidden_column_is_refused_in_every_spelling(sql):
    assert "V003" in _codes(sql)


# -- names stage 2 matches as written: a spelling PostgreSQL folds is not served past it ----------


@pytest.mark.parametrize(
    "sql",
    [
        # email is masked: stage 2 resolves C to no relation and would serve it in clear
        "SELECT C.email FROM bank.customer c",
        "SELECT CUSTOMER.email FROM bank.customer",
        "SELECT c.email FROM bank.customer AS C",
        # an unregistered spelling of the table: stage 2 would apply none of its rules
        "SELECT id FROM BANK.CUSTOMER",
    ],
)
def test_a_name_that_resolves_only_when_folded_is_refused(sql):
    assert _codes(sql), "refused: stage 2 would not govern the relation the engine reads"


def test_no_served_spelling_reads_a_masked_column_in_clear():
    spellings = [
        "SELECT c.email FROM bank.customer c",
        "SELECT C.email FROM bank.customer c",
        "SELECT c.email FROM bank.customer AS C",
        "SELECT C.email FROM bank.customer AS C",
        "SELECT CUSTOMER.email FROM bank.customer",
        "SELECT customer.email FROM bank.customer",
        "SELECT email FROM bank.customer",
        "SELECT EMAIL FROM bank.customer",
    ]
    served = [sql for sql in spellings if not _codes(sql)]
    assert "SELECT c.email FROM bank.customer c" in served
    for sql in served:
        governed = apply_governance(sql, _gov())
        assert "'***'" in governed, f"{sql!r} served unmasked: {governed}"


# -- a quoted mixed-case table that is mixed case resolves exactly ----------------------------------


def test_a_quoted_mixed_case_table_resolves():
    assert _codes('SELECT m.id FROM bank."MixedCase" m') == []
    assert _codes('SELECT "MixedCase".id FROM bank."MixedCase"') == []


def test_a_quoted_mixed_case_table_still_hides_its_columns():
    assert "V003" in _codes('SELECT m.secret FROM bank."MixedCase" m')
    assert "V003" in _codes('SELECT "MixedCase".secret FROM bank."MixedCase"')
    assert "V003" in _codes('SELECT M.secret FROM bank."MixedCase" m')


def test_an_unquoted_mixed_case_table_is_not_the_mixed_case_one():
    # PostgreSQL reads bank.MixedCase as bank.mixedcase, which is not registered
    assert "V006" in _codes("SELECT id FROM bank.MixedCase")


# -- what PostgreSQL resolves, and stage 2 governs, is still served ---------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT c.id, c.name FROM bank.customer c",
        "SELECT customer.id FROM bank.customer",
        "SELECT bank.customer.id FROM bank.customer",
        "SELECT C.id FROM bank.customer AS C",
        "SELECT id FROM bank.customer c WHERE EXISTS (SELECT 1 WHERE c.id > 1)",
        "SELECT c.id, (SELECT c.name) FROM bank.customer c",
        "SELECT d.id FROM (SELECT id FROM bank.customer) AS d",
        "SELECT g.x FROM generate_series(1, 3) AS g(x)",
        "SELECT c.id, l.n FROM bank.customer c, LATERAL (SELECT c.name AS n) AS l",
        "WITH k AS (SELECT id FROM bank.customer) SELECT k.id FROM k",
    ],
)
def test_a_resolvable_qualified_visible_column_is_served(sql):
    assert _codes(sql) == []
