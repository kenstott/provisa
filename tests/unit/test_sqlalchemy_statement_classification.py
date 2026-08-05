# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3e05b9-84a1-4d26-b0f7-92e5c14a3d68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1223: which statements may run behind a server-side cursor.

The classification is not cosmetic. ``stream_results`` makes psycopg2 issue ``DECLARE ... CURSOR
FOR <sql>`` at execute time, which is a syntax error for DDL and DML — so misreading an INSERT as
row-returning fails the statement outright, and misreading a SELECT as non-row-returning buffers
the whole result the streaming path exists to bound (REQ-1222).

The classifier is tested directly rather than through a live engine: what it decides is a pure
function of the SQL text, and a database would only re-prove that psycopg2 rejects DECLARE CURSOR
FOR INSERT.
"""

# Requirements: REQ-1222, REQ-1223

from __future__ import annotations

import pytest

from provisa.federation.sqlalchemy_runtime import _ROW_RETURNING, _is_row_returning


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "select id from orders",
        "  \n\t SELECT id FROM orders",
        "WITH t AS (SELECT 1) SELECT * FROM t",
        "VALUES (1), (2)",
        "TABLE orders",
        "SHOW search_path",
        "EXPLAIN SELECT 1",
    ],
)
def test_row_returning_statements_stream(sql):
    assert _is_row_returning(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO orders VALUES (1)",
        "UPDATE orders SET amount = 1",
        "DELETE FROM orders",
        "CREATE TABLE t (id int)",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN c int",
        "TRUNCATE orders",
        "COMMIT",
        "SET search_path TO public",
    ],
)
def test_ddl_and_dml_run_buffered(sql):
    """A server-side cursor over these is a syntax error, not a slower query."""
    assert _is_row_returning(sql) is False


@pytest.mark.parametrize(
    "sql",
    [
        "-- the governed scan\nSELECT id FROM orders",
        "--no space after the dashes\nSELECT 1",
        "/* block */ SELECT id FROM orders",
        "/* one */ /* two */\nSELECT 1",
        "-- line\n/* block */\n  SELECT 1",
    ],
)
def test_a_leading_comment_does_not_hide_the_keyword(sql):
    """Compiled SQL arrives with a provenance comment on it, so the keyword is rarely first."""
    assert _is_row_returning(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "-- only a comment\n",
        "/* only a block */",
        "",
        "   ",
        "/* unterminated",
        "--",
    ],
)
def test_a_statement_with_no_keyword_is_not_row_returning(sql):
    """Nothing to stream, and the buffered path reports the driver's own error on execute —
    which is a better failure than DECLARE CURSOR FOR a comment."""
    assert _is_row_returning(sql) is False


def test_the_keyword_is_read_whole_rather_than_by_prefix():
    """``SELECTED`` is not ``SELECT``: matching on a prefix would send an unknown statement down
    the server-side cursor path."""
    assert _is_row_returning("SELECTED_ROWS()") is False
    assert _is_row_returning("WITHOUT OIDS") is False


def test_the_row_returning_set_is_the_documented_one():
    assert _ROW_RETURNING == {"SELECT", "WITH", "VALUES", "TABLE", "SHOW", "EXPLAIN"}
