# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2a6d13-4b9c-4e57-a0d8-3c1e7b5f9a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A subtype keeps its native semantics on the Trino engine (REQ-2171, amended 2026-10-27).

A range is emulated from its bounds; a geometry is Trino's own. Each statement is written as
Provisa writes it for Trino and run there; the expected answers are PostgreSQL's. An operation
Trino neither has nor emulates -- an inet's, its IPADDRESS holding no mask -- is refused before it
is sent."""

# Requirements: REQ-2171

from __future__ import annotations

import pytest

from provisa.transpiler.native_subtype import SubtypeOperationRefused
from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.requires_trino, pytest.mark.integration]


def _rows(conn, pg_sql: str) -> list:
    cur = conn.cursor()
    cur.execute(transpile(pg_sql, "trino"))
    return cur.fetchall()


def _values(column: str, values: list[str]) -> str:
    return "(VALUES " + ", ".join(f"('{v}')" for v in values) + f") AS h({column})"


def test_an_inet_reads_as_its_text_and_an_operation_on_it_is_refused_by_name(trino_conn):
    """Trino's IPADDRESS holds no mask; no emulation from Trino 481's functions answered as
    PostgreSQL does, so an inet's order or containment is refused before anything is sent, and
    the column read as a value is its text form."""
    addresses = ["10.0.0.10", "10.0.0.9", "10.0.0.1/24", "::1"]
    rows = _rows(trino_conn, f"SELECT h.ip FROM {_values('ip', addresses)}")
    assert sorted(r[0] for r in rows) == sorted(addresses)
    for statement in (
        f"SELECT h.ip FROM {_values('ip', addresses)} ORDER BY provisa_subtype(h.ip, 'inet')",
        f"SELECT h.ip FROM {_values('ip', addresses)}"
        " WHERE provisa_subtype(h.ip, 'inet') <<= '10.0.0.0/8'",
    ):
        with pytest.raises(SubtypeOperationRefused) as refused:
            transpile(statement, "trino")
        assert (refused.value.params["subtype"], refused.value.params["dialect"]) == (
            "inet",
            "trino",
        )


def test_range_overlap_answers_as_postgresql_does(trino_conn):
    rows = _rows(
        trino_conn,
        "SELECT h.span FROM "
        + _values("span", ["[1,5)", "[5,10)", "(1,5]", "empty", "[,3)"])
        + " WHERE provisa_subtype(h.span, 'int4range') && '[4,6)'",
    )
    # '(1,5]' is '[2,6)'; '[,3)' ends before 4; empty overlaps nothing.
    assert sorted(r[0] for r in rows) == ["(1,5]", "[1,5)", "[5,10)"]


def test_a_spatial_predicate_runs_on_trinos_own_geometry(trino_conn):
    rows = _rows(
        trino_conn,
        "SELECT h.area FROM "
        + _values(
            "area", ["POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))", "POLYGON((5 5, 5 6, 6 6, 6 5, 5 5))"]
        )
        + " WHERE ST_Contains(provisa_subtype(h.area, 'geometry'), 'POINT(1 1)')",
    )
    assert [r[0] for r in rows] == ["POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))"]


def test_an_operator_trino_neither_has_nor_emulates_is_refused_by_name():
    with pytest.raises(SubtypeOperationRefused) as refused:
        transpile(
            "SELECT 1 FROM (VALUES ('a')) AS h(v)"
            " WHERE provisa_subtype(h.v, 'tsvector') @@ to_tsquery('a')",
            "trino",
        )
    assert refused.value.params == {"subtype": "tsvector", "operation": "@@", "dialect": "trino"}
