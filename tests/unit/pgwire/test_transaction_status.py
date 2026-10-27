# Copyright (c) 2026 Kenneth Stott
# Canary: bcdd0671-3777-4987-94a1-9868e9e1258a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ReadyForQuery reports 'T' between BEGIN and COMMIT/ROLLBACK. Transactions stay a no-op by
design; drivers read the status byte (pgspec TestSimpleQuery/TransactionStatus)."""

from __future__ import annotations

import pytest

from provisa.pgwire.catalog import classify
from provisa.pgwire.server import ProvisaSession, _transaction_open


@pytest.mark.parametrize(
    ("sql", "before", "after"),
    [
        ("BEGIN", False, True),
        ("begin transaction isolation level serializable", False, True),
        ("START TRANSACTION", False, True),
        ("COMMIT", True, False),
        ("END", True, False),
        ("ROLLBACK", True, False),
        ("ABORT", True, False),
        ("ROLLBACK TO SAVEPOINT s1", True, True),
        ("SAVEPOINT s1", True, True),
        ("SELECT 1", True, True),
        ("SET search_path TO public", False, False),
    ],
)
def test_statement_moves_the_transaction(sql, before, after):
    assert _transaction_open(sql, before) is after


@pytest.mark.parametrize("sql", ["END", "ABORT"])
def test_end_and_abort_are_answered_like_commit(sql):
    assert classify(sql) == "INTERCEPT"


def test_the_session_reports_the_open_transaction():
    session = ProvisaSession.__new__(ProvisaSession)
    session.transaction_open = False
    assert session.in_transaction() is False
    session.transaction_open = True
    assert session.in_transaction() is True


@pytest.mark.parametrize(
    ("sql", "kind", "rowcount", "tag"),
    [
        ("INSERT INTO t(val) VALUES ('a')", "insert", 1, "INSERT 0 1"),
        ("UPDATE t SET val = 'b'", "rewrite", 2, "UPDATE 2"),
        ("DELETE FROM t", "rewrite", 3, "DELETE 3"),
        ("CREATE TEMP TABLE t (id serial)", "create", 0, "CREATE TABLE"),
        ("DROP TABLE t", "drop", 0, "DROP TABLE"),
    ],
)
def test_a_temporary_table_statement_is_answered_with_its_tag(sql, kind, rowcount, tag):
    from types import SimpleNamespace

    from provisa.executor.result import QueryResult
    from provisa.pgwire.server import _status_of

    plan = SimpleNamespace(writes_tables=False, temp=SimpleNamespace(kind=kind))
    result = QueryResult(rows=[], column_names=[], rowcount=rowcount)
    assert _status_of(sql, result, plan) == tag
