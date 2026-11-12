# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1d8c4f-2a7b-4b95-b3e0-9f5a17c2d846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Type operators on Trino answer as PostgreSQL does or are refused by name (REQ-2171, AN ANSWER
THAT DIFFERS IS REFUSED): the wrong answers the function support measure found on Trino (run
43264139919), each as the statement it sent, against postgres:17.11's answer."""

# Requirements: REQ-2171, REQ-2167

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.pgwire.server import _sql_type_to_bvtype
from buenavista.core import BVType
from buenavista.postgres import BVTYPE_TO_OID
from provisa.transpiler.transpile import transpile

_BYTEA = 17


# --- a send function's bytes: Trino's varbinary is PostgreSQL's bytea -------------------------


@pytest.mark.parametrize("served", ["varbinary", "VARBINARY"])
def test_trinos_varbinary_is_sent_as_bytea(served):
    """int4send and every other send function, byteain and sha256 answer varbinary on Trino:
    described as text, a client read PostgreSQL's bytea as text (oid 25 for 17)."""
    assert _sql_type_to_bvtype(served) is BVType.BYTES
    assert BVTYPE_TO_OID[_sql_type_to_bvtype(served)] == _BYTEA


@pytest.mark.parametrize(
    "pg_sql",
    [
        'SELECT pg_catalog."int4send"(NULL::integer) AS v',
        'SELECT pg_catalog."textsend"(NULL::text) AS v',
        'SELECT pg_catalog."byteain"(NULL::cstring) AS v',
    ],
)
def test_a_null_send_is_a_null_of_varbinary_on_trino(pg_sql):
    """What Trino is sent: a NULL of the type the wire describes as bytea."""
    tree = sqlglot.parse_one(transpile(pg_sql, "trino"), read="trino")
    (cast,) = tree.find_all(exp.Cast)
    assert isinstance(cast.this, exp.Null)
    assert cast.to.is_type(exp.DataType.Type.VARBINARY)


# --- a NULL of a type Trino has no wire form of: refused by name -------------------------------


@pytest.mark.parametrize(
    ("pg_sql", "function"),
    [
        (
            'SELECT pg_catalog."interval_in"(NULL::cstring, NULL::oid, NULL::integer) AS v',
            "interval_in",
        ),
        ('SELECT pg_catalog."jsonb_in"(NULL::cstring) AS v', "jsonb_in"),
        ('SELECT pg_catalog."xid8in"(NULL::cstring) AS v', "xid8in"),
    ],
)
@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
def test_a_null_of_a_type_the_engine_does_not_hold_is_refused_by_name(pg_sql, function, dialect):
    """PostgreSQL answers a NULL of interval, jsonb or xid8: the engine's NULL would be described as
    text. It is refused by the message the call of a value gets, not answered as text."""
    tree = sqlglot.parse_one(transpile(pg_sql, dialect), read=dialect)
    said = {s.this for s in tree.find_all(exp.Literal) if s.is_string}
    assert any(f"{function} does not read this value" in s for s in said), said
    assert not [
        c for c in tree.find_all(exp.Cast) if isinstance(c.this, exp.Null) and c.to.is_type("text")
    ]


@pytest.mark.parametrize(
    ("pg_sql", "kind"),
    [
        (
            'SELECT pg_catalog."interval_in"(NULL::cstring, NULL::oid, NULL::integer) AS v',
            exp.DataType.Type.INTERVAL,
        ),
        ('SELECT pg_catalog."jsonb_in"(NULL::cstring) AS v', exp.DataType.Type.JSON),
    ],
)
def test_duckdb_still_answers_the_null_of_its_own_type(pg_sql, kind):
    tree = sqlglot.parse_one(transpile(pg_sql, "duckdb"), read="duckdb")
    (cast,) = tree.find_all(exp.Cast)
    assert isinstance(cast.this, exp.Null) and cast.to.is_type(kind)


# --- cash_out: strict, so a NULL money is NULL -------------------------------------------------


def _null_guarded(tree: exp.Expr) -> bool:
    """Whether a CASE of ``tree`` answers NULL first for a value that IS NULL."""
    for case in tree.find_all(exp.Case):
        first = case.args["ifs"][0]
        test, answer = first.this, first.args["true"]
        nulled = answer.this if isinstance(answer, exp.Cast) else answer
        if isinstance(test, exp.Is) and isinstance(test.expression, exp.Null):
            if isinstance(nulled, exp.Null):
                return True
    return False


@pytest.mark.parametrize(
    "pg_sql",
    [
        'SELECT pg_catalog."cash_out"(NULL::money) AS v',
        'SELECT pg_catalog."cash_out"(c) AS v FROM t',
        "SELECT CAST(NULL::money AS text) AS v",
    ],
)
def test_money_text_of_a_null_is_null_on_trino(pg_sql):
    """Trino's format('$%,.2f', NULL) is '$nu': Java writes null, cut to the two digits asked for.
    PostgreSQL's cash_out is strict."""
    written = transpile(pg_sql, "trino")
    tree = sqlglot.parse_one(written, read="trino")
    assert _null_guarded(tree), written
