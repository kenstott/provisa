# Copyright (c) 2026 Kenneth Stott
# Canary: 7c0f4a19-2d63-4e58-8b41-9e2a5d0c8f37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1165: the SQL-pushdown translator for SQL-expressible preflight checks.

A single quantified row assertion translates to a governed-PostgreSQL WHERE fragment evaluated
as a count probe; anything outside the subset returns None (→ the Python+Arrow streaming path).
The translated predicate is verified against a real engine (DuckDB) to prove Python↔SQL parity.
"""

from __future__ import annotations

import duckdb
import pytest

from provisa.mv.preflight import Decision
from provisa.mv.preflight_sql import translate


def _check(pred_body: str) -> str:
    return f"def preflight(rows, ctx):\n{pred_body}"


def test_translates_any_abort() -> None:
    sp = translate(
        _check("    if any(r['qty'] < 0 for r in rows):\n        return ctx.abort('neg')\n    return ctx.ok()")
    )
    assert sp is not None
    assert sp.quantifier == "any"
    assert sp.predicate_sql == '("qty" < 0)'
    assert sp.violation.decision is Decision.ABORT and sp.violation.reason == "neg"
    assert sp.passing.decision is Decision.CONTINUE
    assert sp.verdict_for(3).decision is Decision.ABORT
    assert sp.verdict_for(0).decision is Decision.CONTINUE


def test_translates_all_quarantine() -> None:
    sp = translate(
        _check(
            "    if all(r['ok'] == True for r in rows):\n"
            "        return ctx.quarantine('all flagged')\n    return ctx.ok()"
        )
    )
    assert sp is not None
    assert sp.quantifier == "all"
    # all() fires when NO row violates → count of violators == 0
    assert sp.verdict_for(0).decision is Decision.QUARANTINE
    assert sp.verdict_for(2).decision is Decision.CONTINUE


def test_translates_boolean_arithmetic_and_null() -> None:
    sp = translate(
        _check(
            "    if any((r['a'] + 1) > r['b'] and r['c'] != None for r in rows):\n"
            "        return ctx.abort('x')\n    return ctx.ok()"
        )
    )
    assert sp is not None
    assert sp.predicate_sql == '((("a" + 1) > "b") AND ("c" IS NOT NULL))'


@pytest.mark.parametrize(
    "body",
    [
        # cross-row state (len) — not a single quantified assertion
        "    if len(rows) > 5:\n        return ctx.abort('big')\n    return ctx.ok()",
        # helper call inside the predicate — not translatable
        "    if any(bad(r) for r in rows):\n        return ctx.abort('x')\n    return ctx.ok()",
        # extra sibling statement — not the canonical two-statement shape
        "    n = 1\n    if any(r['q'] < n for r in rows):\n        return ctx.abort('x')\n    return ctx.ok()",
        # iterates something other than rows
        "    if any(r < 0 for r in [1, 2]):\n        return ctx.abort('x')\n    return ctx.ok()",
    ],
)
def test_untranslatable_returns_none(body: str) -> None:
    assert translate(_check(body)) is None


def test_sql_matches_python_on_a_real_engine() -> None:
    # REQ-964: the pushed-down predicate must agree with the Python check for the same dataset.
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1,10),(2,-3),(3,7)) AS v(id, qty)")
    select_sql = "SELECT id, qty FROM t"
    sp = translate(
        _check("    if any(r['qty'] < 0 for r in rows):\n        return ctx.abort('neg')\n    return ctx.ok()")
    )
    assert sp is not None
    count = con.execute(sp.count_sql(select_sql)).fetchone()[0]
    assert count == 1  # one negative row
    assert sp.verdict_for(count).decision is Decision.ABORT

    # Python evaluation over the same rows reaches the same verdict.
    rows = [{"id": r[0], "qty": r[1]} for r in con.execute(select_sql).fetchall()]
    py_fires = any(r["qty"] < 0 for r in rows)
    assert py_fires is (sp.verdict_for(count).decision is Decision.ABORT)
