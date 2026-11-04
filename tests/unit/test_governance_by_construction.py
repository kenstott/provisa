# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1f9a52-7e04-4b8d-a6e2-91d5c0f8b7a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hidden columns are kept out by construction (REQ-039, amended 2026-11-04).

Every reference to a registered relation becomes the role's governed relation, which exposes only
the columns the role is served, masked, with its row rule applied: a hidden column is in no
relation the engine reads, whatever the scoping. Each statement of the RV597 and RV597b reviews
runs through the validator and stage 2, and then on PostgreSQL, twice: with the column check
(V003) on, and with it bypassed, so the construction is tested on its own. Each runs against two
tables that differ only in the hidden ``ssn``: no answer holds an ``ssn`` value, a row the row
rule excludes or a value derived from either, and no answer differs between the two tables (the
hidden column is no oracle). A refusal -- by the validator, by stage 2 or by PostgreSQL as an
unknown column -- returns nothing, which is what the role is owed.
"""

# Requirements: REQ-039, REQ-040, REQ-603

from __future__ import annotations

import re
from typing import Any

import pytest

from provisa.compiler.sql_types import CompilationContext
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import KEY_MARK, GovernanceContext, apply_governance
from provisa.compiler.view_expand import VIEW_MARK, expand_view_refs
from provisa.pgwire.catalog_route import CLIENT_TABLE_FUNCTIONS

psycopg = pytest.importorskip("psycopg")

_CUSTOMER, _ACCT = 1, 2
_COLUMNS = [("id", "integer"), ("name", "varchar"), ("ssn", "varchar"), ("email", "varchar")]
_ACCT_COLUMNS = [("id", "integer"), ("cust_ssn", "varchar"), ("bal", "integer")]

# Two tables alike in every served value; only the hidden ssn differs, in value and in order.
_ROWS = [(1, "ann", "a@x"), (2, "bob", "b@x"), (3, "zed", "z@x")]
_SSN_A = ["111-SECRET", "222-SECRET", "333-SECRET"]
_SSN_B = ["999-OTHER", "000-OTHER", "555-OTHER"]
# Row 3 ("zed") is the row the role's row rule excludes.
_LEAKS = re.compile(r"SECRET|OTHER|zed|z@x")


def _gov(**writes: Any) -> GovernanceContext:
    gov = GovernanceContext(
        table_map={
            "bank.customer": _CUSTOMER,
            "customer": _CUSTOMER,
            "bank.acct": _ACCT,
            "acct": _ACCT,
        },
        all_columns={_CUSTOMER: list(_COLUMNS), _ACCT: list(_ACCT_COLUMNS)},
        visible_columns={
            _CUSTOMER: frozenset({"id", "name", "email"}),
            _ACCT: frozenset({"id", "bal"}),
        },
        rls_rules={_CUSTOMER: "id < 3"},
        join_keys={(_CUSTOMER, _ACCT, "ssn", "cust_ssn"), (_ACCT, _CUSTOMER, "cust_ssn", "ssn")},
        pk_columns={_CUSTOMER: ["id"], _ACCT: ["id"]},
    )
    if writes:
        gov.can_write = True
        gov.role_id = "clerk"
        columns = ["id", "name", "email"]
        gov.write_grants = {
            _CUSTOMER: {
                op: {"operation": op, "role": "clerk", "columns": columns}
                for op in ("insert", "update", "delete")
            }
        }
        gov.write_ops = {_CUSTOMER: frozenset({"insert", "update", "delete"})}
        gov.write_returns_rows = {_CUSTOMER: True}
        gov.write_refused_forms = {_CUSTOMER: frozenset()}
    return gov


@pytest.fixture(scope="module")
def dsn(embedded_postgres):
    admin = psycopg.connect(embedded_postgres.dsn(), autocommit=True)
    admin.execute("DROP DATABASE IF EXISTS governance_by_construction")
    admin.execute("CREATE DATABASE governance_by_construction")
    admin.close()
    url = embedded_postgres.dsn("governance_by_construction")
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA bank")
        conn.execute(
            "CREATE TABLE bank.customer (id int PRIMARY KEY, name text, ssn text, email text)"
        )
        conn.execute("CREATE TABLE bank.acct (id int PRIMARY KEY, cust_ssn text, bal int)")
    return url


def _load(conn: Any, ssn: list[str]) -> None:
    conn.execute("DELETE FROM bank.customer")
    conn.execute("DELETE FROM bank.acct")
    for (i, name, email), s in zip(_ROWS, ssn):
        conn.execute("INSERT INTO bank.customer VALUES (%s, %s, %s, %s)", (i, name, s, email))
        conn.execute("INSERT INTO bank.acct VALUES (%s, %s, %s)", (10 + i, s, 100 * i))


def _governed(sql: str, gov: GovernanceContext, *, check: bool) -> str | None:
    """What stage 2 hands the engine for ``sql``; None when governance refuses it."""
    if check:
        violations = validate_sql(
            sql,
            CompilationContext(),
            gov,
            {"domain_access": ["*"]},
            [],
            bypass_relationship_guard=True,
            table_functions=CLIENT_TABLE_FUNCTIONS,
        )
        if violations:
            return None
    try:
        return apply_governance(sql, gov)
    except (PermissionError, ValueError):
        return None


def _run(dsn: str, governed: str, ssn: list[str]) -> tuple[Any, ...]:
    """The answer PostgreSQL gives ``governed`` over the table holding ``ssn``, and the table's
    served values afterwards (a write's effect); an error is an answer of its own."""
    with psycopg.connect(dsn) as conn:
        try:
            _load(conn, ssn)
            cur = conn.execute(governed)
            rows = cur.fetchall() if cur.description else []
            count = cur.rowcount
            after = conn.execute("SELECT id, name, email FROM bank.customer ORDER BY id").fetchall()
            return ("ok", rows, count, after)
        except psycopg.Error as exc:
            return ("error", type(exc).__name__)
        finally:
            conn.rollback()


def _assert_kept_out(dsn: str, sql: str, gov: GovernanceContext, *, check: bool) -> None:
    governed = _governed(sql, gov, check=check)
    if governed is None:
        return
    assert VIEW_MARK not in governed
    first, second = _run(dsn, governed, _SSN_A), _run(dsn, governed, _SSN_B)
    answer = first[1] if first[0] == "ok" else []
    assert not _LEAKS.search(repr(answer)), f"{sql}\n  ran as {governed}\n  answered {answer}"
    if first[0] == "ok":
        assert first[3][2] == (3, "zed", "z@x"), f"{sql} wrote a row its role does not read"
    assert first == second, f"{sql}\n  ran as {governed}\n  answers depend on ssn: {first} {second}"


# -- RV597 -----------------------------------------------------------------------------------------

_RV597 = [
    # C1: an outer reference to a hidden column through VALUES or a record function
    "SELECT id, (SELECT ssn FROM (VALUES (1)) v) AS leak FROM bank.customer c",
    "SELECT id, ARRAY(SELECT ssn FROM (VALUES (1)) v) AS leak FROM bank.customer c",
    "SELECT id, (SELECT ssn FROM json_each('{}') j) AS leak FROM bank.customer c",
    "SELECT id, (SELECT ssn FROM jsonb_each('{}') j) FROM bank.customer c",
    "SELECT id, (SELECT ssn FROM (VALUES (1)) v UNION SELECT 'a') AS leak FROM bank.customer c",
    "SELECT (SELECT ssn FROM (SELECT * FROM (VALUES (1)) v) d) FROM bank.customer c",
    # C2: a LATERAL derived table renames the hidden column
    "SELECT x FROM bank.customer c, LATERAL (SELECT ssn AS x FROM (VALUES (1)) v) l",
    "SELECT l.x FROM bank.customer c CROSS JOIN LATERAL (SELECT ssn AS x FROM (VALUES (1)) v) l",
    # H1: a name no relation registers beside a source with unknown columns
    "SELECT id, unregistered FROM bank.customer c, (VALUES (1)) v",
    "SELECT unregistered FROM bank.customer c, json_each('{}') j",
    # H2: a hidden column passed as a FROM-function argument
    "SELECT x FROM bank.customer c, unnest(ARRAY[c.ssn]) x",
    "SELECT x FROM bank.customer c, unnest(ARRAY[ssn]) x",
    "SELECT x FROM bank.customer c, LATERAL unnest(ARRAY[c.ssn]) x",
    # H3: table-function admission
    "SELECT 1 FROM evil.unnest(ARRAY[1]) x",
    "SELECT 1 FROM public.unnest(ARRAY[1]) x",
    "SELECT 1 FROM cat.evil.unnest(ARRAY[1]) x",
    "SELECT 1 FROM explode(ARRAY[1]) x",
    "SELECT 1 FROM pg_catalog.explode(ARRAY[1]) x",
    "SELECT 1 FROM pg_catalog.posexplode(ARRAY[1]) x",
    # H4: an unquoted uppercase alias
    "SELECT C.ssn FROM bank.customer c",
    "SELECT CUSTOMER.ssn FROM bank.customer",
    # H5: a star over a NATURAL JOIN
    "SELECT * FROM bank.customer NATURAL JOIN (VALUES ('x')) v(ssn)",
    # M1: hidden columns outside the select list
    "SELECT id FROM bank.customer WHERE ssn LIKE '1%'",
    "SELECT id FROM bank.customer ORDER BY ssn",
    "SELECT count(*) FROM bank.customer GROUP BY ssn",
    "SELECT id FROM bank.customer c WHERE EXISTS "
    "(SELECT 1 FROM (VALUES (1)) v WHERE c.ssn LIKE '1%')",
    # shapes the review found refused
    "SELECT c FROM bank.customer c",
    "SELECT row_to_json(c) FROM bank.customer c",
    "SELECT to_jsonb(c.*) FROM bank.customer c",
    "SELECT ROW(c.*) FROM bank.customer c",
    "SELECT (c).ssn FROM bank.customer c",
    "SELECT c.SSN FROM bank.customer c",
    'SELECT "SSN" FROM bank.customer c',
    "SELECT customer.ssn FROM bank.customer",
    "SELECT bank.customer.ssn FROM bank.customer",
    "SELECT a FROM (SELECT ssn FROM bank.customer) AS d(a)",
    "WITH d(a) AS (SELECT ssn FROM bank.customer) SELECT a FROM d",
    "SELECT ssn FROM bank.customer JOIN (VALUES (1)) v(id) USING (id)",
    "SELECT rank() OVER (ORDER BY ssn) FROM bank.customer",
    "SELECT * FROM (VALUES ((SELECT ssn FROM bank.customer LIMIT 1))) v",
    "SELECT * FROM pg_catalog.unnest((SELECT array_agg(ssn) FROM bank.customer)) u",
]

# -- RV597b ----------------------------------------------------------------------------------------

_RV597B = [
    # C1: a name resolved to a source PostgreSQL cannot see there
    "SELECT (SELECT u FROM unnest(ARRAY[ssn]) u, (VALUES ('x')) v(ssn)) FROM bank.customer c",
    "SELECT (SELECT d.x FROM (VALUES ('x')) v(ssn), (SELECT ssn AS x) d) FROM bank.customer c",
    "SELECT id, (SELECT d.x FROM (SELECT ssn AS x) d, (VALUES ('x')) v(ssn)) FROM bank.customer c",
    "SELECT x FROM bank.customer c, "
    "LATERAL (SELECT d.x FROM (SELECT ssn AS x) d, (VALUES ('x')) v(ssn)) l",
    "SELECT (SELECT x FROM (VALUES ('a')) v(ssn), (SELECT ssn AS x UNION SELECT 'z') d LIMIT 1) "
    "FROM bank.customer c",
    "SELECT (WITH w AS (SELECT ssn AS x) SELECT w.x FROM w, (VALUES ('x')) v(ssn)) "
    "FROM bank.customer c",
    "SELECT id FROM bank.customer c WHERE EXISTS (SELECT 1 FROM (VALUES (1)) a "
    "JOIN (VALUES (1)) b ON ssn LIKE '1%', (VALUES ('x')) v(ssn))",
    "WITH d AS (SELECT 'a' AS ssn) SELECT (WITH d AS (SELECT 1 AS z) SELECT ssn FROM d) "
    "FROM bank.customer c",
    # C2: wrongly known output names
    "SELECT (SELECT ssn FROM (SELECT 'ssn') d) FROM bank.customer c",
    "SELECT (SELECT ssn FROM (SELECT * FROM (SELECT 'ssn') e) d) FROM bank.customer c",
    "SELECT (SELECT ssn FROM unnest(ARRAY[1], ARRAY[2]) ssn) FROM bank.customer c",
    # C3: a parenthesized join
    "SELECT ssn FROM (bank.customer CROSS JOIN (VALUES (1)) v)",
    "SELECT j.ssn FROM (bank.customer c CROSS JOIN (VALUES (1)) v) AS j",
    "SELECT * FROM (bank.customer c CROSS JOIN (VALUES (1)) v)",
    "SELECT id FROM (bank.customer c CROSS JOIN (VALUES (1)) v) WHERE c.ssn LIKE '1%'",
    "SELECT id FROM (bank.customer CROSS JOIN (VALUES (1)) v)",
    "SELECT count(*) FROM (bank.customer JOIN (VALUES ('111-SECRET')) v(ssn) USING (ssn))",
    "SELECT count(*) FROM (bank.customer NATURAL JOIN (VALUES ('111-SECRET')) v(ssn))",
    "SELECT c.id FROM bank.customer c JOIN (bank.customer d CROSS JOIN (VALUES (1)) v) "
    "ON c.id = d.id WHERE d.ssn LIKE '1%'",
    # C4: a data-modifying CTE
    "WITH u AS (UPDATE bank.customer SET name = name RETURNING ssn) SELECT * FROM u",
    "WITH u AS (UPDATE bank.customer SET name = name WHERE ssn LIKE '1%' RETURNING id) "
    "SELECT * FROM u",
    "WITH u AS (DELETE FROM bank.customer WHERE ssn LIKE '9%' RETURNING ssn) SELECT ssn FROM u",
    # H1: an output name written as a string literal
    "SELECT 'ssn', id FROM bank.customer ORDER BY ssn DESC",
    "SELECT 'ssn', id FROM bank.customer ORDER BY ssn ASC",
    # H2: a CTE named like a registered table
    "WITH customer AS (SELECT 1 AS x) SELECT id FROM bank.customer WHERE ssn LIKE '1%'",
    "WITH customer AS (SELECT 1 AS x) SELECT id, ssn FROM bank.customer ORDER BY ssn",
    # H3: top-level DML columns outside RETURNING
    "UPDATE bank.customer SET name = ssn RETURNING name",
    "UPDATE bank.customer SET name = name WHERE ssn LIKE '1%' RETURNING id",
    "DELETE FROM bank.customer WHERE ssn LIKE '9%' RETURNING id",
    "INSERT INTO bank.customer (id, name) VALUES (1,'x') ON CONFLICT (id) "
    "DO UPDATE SET name = bank.customer.ssn",
    # M1: a table-function admission that does not check schema or case
    "SELECT 1 FROM evil.generate_series(1, 2) g",
    "SELECT 1 FROM \"JSON_EACH\"('{}') x",
    # M2
    "SELECT explode(ARRAY[name]) FROM bank.customer",
    # shapes the review found refused
    "SELECT 'ssn' AS x, id FROM bank.customer ORDER BY ssn",
    "SELECT name AS ssn FROM bank.customer GROUP BY ssn",
    "SELECT count(*) FILTER (WHERE ssn LIKE '1%') FROM bank.customer",
    "SELECT DISTINCT ON (ssn) id FROM bank.customer",
    "SELECT string_agg(name, ',' ORDER BY ssn) FROM bank.customer",
    "SELECT id, rank() OVER w FROM bank.customer WINDOW w AS (PARTITION BY ssn ORDER BY id)",
    'SELECT (SELECT ssn FROM (SELECT 1 AS "SSN") d) FROM bank.customer c',
    "SELECT id FROM bank.customer c, LATERAL (SELECT ssn) l WHERE false",
    "SELECT id FROM bank.customer WHERE (SELECT ssn FROM (VALUES (1)) v(column1)) LIKE '1%'",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 2) "
    "SELECT (SELECT ssn FROM r, (VALUES ('x')) v(ssn) LIMIT 1) FROM bank.customer c",
]


@pytest.mark.parametrize("check", [True, False], ids=["v003", "v003-bypassed"])
@pytest.mark.parametrize("sql", _RV597 + _RV597B)
def test_no_reviewed_statement_reaches_a_hidden_value(dsn, sql, check):
    _assert_kept_out(dsn, sql, _gov(writes=True), check=check)


def test_the_row_rule_holds_in_a_parenthesised_join(dsn):
    """RV597b C3: the governed table in a parenthesised join carries its row rule."""
    governed = apply_governance("SELECT id FROM (bank.customer CROSS JOIN (VALUES (1)) v)", _gov())
    assert _run(dsn, governed, _SSN_A)[1] == [(1,), (2,)]


# -- a relationship whose key the role is not served ---------------------------------------------


def test_a_hidden_join_key_joins_and_is_never_read(dsn):
    sql = "SELECT c.name, a.bal FROM bank.customer c JOIN bank.acct a ON c.ssn = a.cust_ssn ORDER BY 1"
    governed = apply_governance(sql, _gov())
    assert KEY_MARK in governed
    for ssn in (_SSN_A, _SSN_B):
        assert _run(dsn, governed, ssn)[1] == [("ann", 100), ("bob", 200)]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM bank.customer c JOIN bank.acct a ON c.ssn = a.cust_ssn",
        "SELECT c.* FROM bank.customer c JOIN bank.acct a ON c.ssn = a.cust_ssn",
        "SELECT c FROM bank.customer c JOIN bank.acct a ON c.ssn = a.cust_ssn",
        "SELECT row_to_json(c), to_jsonb(a.*) FROM bank.customer c "
        "JOIN bank.acct a ON c.ssn = a.cust_ssn",
        "SELECT * FROM (SELECT * FROM bank.customer c JOIN bank.acct a ON c.ssn = a.cust_ssn) d",
        "SELECT c.ssn, a.cust_ssn FROM bank.customer c JOIN bank.acct a ON c.ssn = a.cust_ssn",
        "SELECT * FROM bank.customer c JOIN bank.acct a ON c.ssn = a.cust_ssn NATURAL JOIN "
        "(VALUES (1)) v(x)",
    ],
)
def test_a_carried_key_is_in_no_star_whole_row_or_named_read(dsn, sql):
    for check in (True, False):
        _assert_kept_out(dsn, sql, _gov(), check=check)
    governed = _governed(sql, _gov(), check=False)
    if governed is not None:
        answer = _run(dsn, governed, _SSN_A)
        assert KEY_MARK not in repr(answer)


def test_a_key_join_the_role_writes_without_a_relationship_fails(dsn):
    """A join on a hidden column that no relationship approves names a column that is not there."""
    gov = _gov()
    gov.join_keys = set()
    governed = apply_governance(
        "SELECT c.name FROM bank.customer c JOIN bank.acct a ON c.ssn = a.cust_ssn", gov
    )
    assert _run(dsn, governed, _SSN_A)[0] == "error"


# -- writes ----------------------------------------------------------------------------------------


def _write(dsn: str, sql: str) -> tuple[Any, ...]:
    return _run(dsn, apply_governance(sql, _gov(writes=True)), _SSN_A)


def test_an_update_returns_the_served_columns_masked():
    gov = _gov(writes=True)
    from provisa.security.masking import MaskingRule, MaskType

    gov.masking_rules = {
        (_CUSTOMER, "email"): (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
    }
    governed = apply_governance("UPDATE bank.customer SET name = 'x' WHERE id = 1 RETURNING *", gov)
    assert "ssn" not in governed and "'***'" in governed


def test_a_plain_update_writes_the_rows_it_names(dsn):
    status, rows, count, after = _write(
        dsn, "UPDATE bank.customer SET name = upper(name) WHERE id = 1 RETURNING *"
    )
    assert status == "ok" and count == 1
    assert rows == [(1, "ANN", "a@x")]
    assert after[0] == (1, "ANN", "a@x")


def test_a_keyed_update_reads_its_subquery_over_the_governed_relation(dsn):
    status, rows, _count, after = _write(
        dsn,
        "UPDATE bank.customer SET name = (SELECT max(name) FROM bank.customer) "
        "WHERE id IN (SELECT id FROM bank.customer WHERE name = 'ann') RETURNING id, name",
    )
    assert status == "ok"
    # max(name) is the governed relation's: 'zed' is excluded by the row rule.
    assert rows == [(1, "bob")]
    assert after == [(1, "bob", "a@x"), (2, "bob", "b@x"), (3, "zed", "z@x")]


def test_a_keyed_update_never_reaches_a_row_its_role_does_not_read(dsn):
    status, rows, _count, after = _write(
        dsn, "UPDATE bank.customer SET name = 'q' WHERE id IN (SELECT 3) RETURNING id"
    )
    assert status == "ok" and rows == [] and after[2] == (3, "zed", "z@x")


def test_a_keyed_delete_reads_its_using_over_the_governed_relation(dsn):
    status, rows, _count, after = _write(
        dsn,
        "DELETE FROM bank.customer c USING bank.acct a WHERE a.id = c.id + 10 AND a.bal > 100 "
        "RETURNING c.id",
    )
    assert status == "ok"
    assert rows == [(2,)]
    assert [r[0] for r in after] == [1, 3]


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE bank.customer SET name = (SELECT ssn) WHERE id = 1 RETURNING name",
        "UPDATE bank.customer SET name = 'q' WHERE id IN (SELECT id WHERE ssn LIKE '1%') "
        "RETURNING id",
        "UPDATE bank.customer SET name = 'q' WHERE id = 1 RETURNING (SELECT ssn)",
        "DELETE FROM bank.customer WHERE EXISTS (SELECT 1 WHERE ssn LIKE '1%') RETURNING id",
        "UPDATE bank.customer c SET name = a.cust_ssn FROM bank.acct a WHERE a.id = c.id + 10 "
        "RETURNING c.name",
    ],
)
def test_a_write_reads_no_hidden_value(dsn, sql):
    for check in (True, False):
        _assert_kept_out(dsn, sql, _gov(writes=True), check=check)


def test_a_write_in_a_cte_is_admitted_and_its_returning_governed(dsn):
    status, rows, _count, _after = _write(
        dsn,
        "WITH w AS (UPDATE bank.customer SET name = 'q' WHERE id IN (SELECT 1) RETURNING *) "
        "SELECT * FROM w",
    )
    assert status == "ok" and rows == [(1, "q", "a@x")]


@pytest.mark.parametrize(
    "sql",
    [
        "WITH a AS (UPDATE bank.customer SET name = 'a' RETURNING id), "
        "b AS (DELETE FROM bank.customer RETURNING id) SELECT * FROM a, b",
        "WITH w AS (UPDATE bank.customer SET name = 'a') SELECT 1",
        "SELECT * FROM (WITH w AS (UPDATE bank.customer SET name = 'a' RETURNING id) "
        "SELECT * FROM w) d",
        "WITH x AS (SELECT 1), w AS (WITH v AS (DELETE FROM bank.customer RETURNING id) "
        "SELECT * FROM v) SELECT * FROM w",
    ],
)
def test_a_statement_holds_one_write_at_its_root_or_in_a_returning_cte(sql):
    with pytest.raises(PermissionError, match="V006"):
        apply_governance(sql, _gov(writes=True))


# -- view bodies -----------------------------------------------------------------------------------


def test_a_view_body_reads_a_hidden_column_its_select_list_does_not_return(dsn):
    body = "SELECT id, name, ssn FROM bank.customer WHERE ssn LIKE '1%'"
    sql = expand_view_refs("SELECT * FROM bank.v", {"v": body}, mark=True)
    governed = apply_governance(sql, _gov())
    assert VIEW_MARK not in governed
    assert _run(dsn, governed, _SSN_A)[1] == [(1, "ann", None)]
    assert _run(dsn, governed, _SSN_B)[1] == []


def test_the_readers_own_clauses_over_a_view_do_not_read_its_hidden_column(dsn):
    body = "SELECT id, name, ssn FROM bank.customer"
    sql = expand_view_refs("SELECT id FROM bank.v WHERE ssn LIKE '1%'", {"v": body}, mark=True)
    governed = apply_governance(sql, _gov())
    first, second = _run(dsn, governed, _SSN_A), _run(dsn, governed, _SSN_B)
    assert first == second and first[1] == []
