# Copyright (c) 2026 Kenneth Stott
# Canary: a1a264ac-de18-40e5-a25d-51908ce90b9d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The geometric forms over columns on Trino, and a shape not finite (REQ-2171). conformance-functions measures every literal statement there."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from typing import Any

import pytest

from provisa.transpiler.transpile import transpile
from tests.integration.geometric_engines import on_columns
from tests.unit.pgwire.geometric_replay import BY_FUNCTION, outcome, same

pytestmark = [pytest.mark.integration]


@pytest.mark.requires_trino
@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_geometric_columns_on_trino(trino_conn, function: str):
    def rows(sql: str) -> list[Any]:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return [r[0] for r in cursor.fetchall()]

    on_columns(function, "trino", rows)


#: PostgreSQL 17's answers of a shape holding NaN or an infinity, for the forms whose literal
#: statements Trino's measure reaches only in conformance-functions: poly_contain and
#: poly_contained from the recording, and these (geo_ops.c's make_bound_box, lseg_length and
#: path_length).
_NOT_FINITE = {
    "SELECT spg_poly_quad_compress('((0,0),(NaN,1),(1,1))'::polygon) AS v": "(NaN,1),(0,0)",
    "SELECT spg_poly_quad_compress('((0,0),(Infinity,1),(1,1))'::polygon) AS v": "(Infinity,1),(0,0)",
    "SELECT length('[(0,0),(NaN,1)]'::lseg) AS v": "NaN",
    "SELECT length('[(-Infinity,0),(1,1)]'::lseg) AS v": "Infinity",
    "SELECT length('((0,0),(NaN,1),(1,1))'::path) AS v": "NaN",
    "SELECT length('[(0,0),(inf,1)]'::path) AS v": "Infinity",
}


@pytest.mark.requires_trino
def test_a_shape_not_finite_answers_postgresql_on_trino(trino_conn):
    def run(sql: str) -> Any:
        cursor = trino_conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()[0][0]

    contained = [
        sql
        for sql in (*BY_FUNCTION["poly_contain"], *BY_FUNCTION["poly_contained"])
        if "NaN" in sql or "Infinity" in sql
    ]
    wrong = [s for s in (outcome(sql, "trino", run) for sql in contained) if s != "ok"]
    for sql, want in _NOT_FINITE.items():
        got = run(transpile(sql, "trino", generated=False))
        if not same(got, want):
            wrong.append(f"{sql}: {got!r}, PostgreSQL {want}")
    assert len(contained) == 12 and not wrong, wrong
