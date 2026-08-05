# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0d38b7-1a94-4c62-9d75-84fb2c07e153
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1186 / REQ-1189: the pgwire result adapter pulls rows as the wire asks for them.

The whole point of the streaming ENGINE terminal is that a large user result never sits in
Provisa's memory. The adapter is where that is won or lost: buenavista asks for the column
description before any DataRow, and an adapter that answered by draining the stream would
materialize the result at exactly the moment it claimed to be streaming it.

Two things are therefore asserted per case — the value on the wire, and how much of the stream
had to be pulled to produce it. A counting stream is the only way to see the second, which is why
these are unit tests rather than a psql round-trip.
"""

# Requirements: REQ-1186, REQ-1189

from __future__ import annotations

import pytest

from provisa.pgwire.server import ProvisaQueryResult


class _CountingStream:
    """A ResultStream whose batches are produced on demand, counting what was pulled."""

    def __init__(self, batches, column_names, column_types=None):
        self._batches = list(batches)
        self.column_names = column_names
        self.column_types = column_types
        self.pulled = 0

    def batches(self):
        for batch in self._batches:
            self.pulled += 1
            yield batch


def _stream(**kwargs):
    return _CountingStream(
        [[[1, "a"], [2, "b"]], [[3, "c"]], [[4, "d"]]],
        ["id", "name"],
        **kwargs,
    )


def test_declared_column_types_are_used_without_touching_the_stream():
    """The engine already knows its types, so nothing needs to be read to describe the row."""
    stream = _stream(column_types=["integer", "text"])

    result = ProvisaQueryResult(stream, "SELECT id, name FROM orders")

    assert result.column_count() == 2
    assert result.column(0)[0] == "id"
    assert result.column(1)[0] == "name"
    assert stream.pulled == 0


def test_missing_types_buffer_exactly_one_batch_to_infer_them():
    """RowDescription precedes DataRow on the wire, so a type that is not declared has to be
    inferred from data — from ONE batch, not from the result."""
    stream = _stream()

    ProvisaQueryResult(stream, "SELECT id, name FROM orders")

    assert stream.pulled == 1


def test_a_partially_typed_result_still_peeks_only_one_batch():
    stream = _stream(column_types=["integer", None])

    ProvisaQueryResult(stream, "SELECT id, name FROM orders")

    assert stream.pulled == 1


def test_rows_are_pulled_one_batch_at_a_time_as_the_wire_consumes_them():
    stream = _stream(column_types=["integer", "text"])
    result = ProvisaQueryResult(stream, "SELECT id, name FROM orders")

    rows = result.rows()
    assert next(rows) == [1, "a"]
    # The first batch had to be pulled to answer; the two behind it did not.
    assert stream.pulled == 1
    assert next(rows) == [2, "a".replace("a", "b")]
    assert stream.pulled == 1

    assert next(rows) == [3, "c"]
    assert stream.pulled == 2


def test_a_peeked_batch_is_emitted_rather_than_dropped():
    """The batch read to infer types is part of the result — losing it silently truncates."""
    stream = _stream()
    result = ProvisaQueryResult(stream, "SELECT id, name FROM orders")

    assert list(result.rows()) == [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]
    assert stream.pulled == 3


def test_a_result_with_no_columns_reports_no_results():
    """A DML statement returns a status line, not a row description."""
    stream = _CountingStream([], [], column_types=[])

    result = ProvisaQueryResult(stream, "INSERT INTO orders VALUES (1)")

    assert result.has_results() is False
    assert result.column_count() == 0
    assert list(result.rows()) == []


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SET search_path TO public", "SET"),
        ("BEGIN", "BEGIN"),
        ("start transaction", "START"),
        ("COMMIT", "COMMIT"),
        ("ROLLBACK", "ROLLBACK"),
        ("DISCARD ALL", "DISCARD"),
    ],
)
def test_transaction_control_statements_answer_with_their_own_tag(sql, expected):
    """A driver reads the command tag to know its BEGIN took; answering OK to every statement
    leaves it unable to tell a started transaction from an ignored one."""
    result = ProvisaQueryResult(_CountingStream([], ["id"], column_types=["integer"]), sql)

    assert result.status() == expected


@pytest.mark.parametrize("sql", ["SELECT 1", "INSERT INTO orders VALUES (1)", ""])
def test_every_other_statement_reports_a_plain_ok(sql):
    result = ProvisaQueryResult(_CountingStream([], ["id"], column_types=["integer"]), sql)

    assert result.status() == "OK"
