# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2ce5ff-7f1a-46d2-a85f-9301046c3d97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Differential e2e proof that the governance pipeline is AST-based (REQ-913).

Runs the FULL path — compile → govern (compiler/stage2) → transpile → execute — against in-process
DuckDB and inspects the RETURNED ROWS, not the SQL text. The two tests share one governor,
one dataset, and one execution harness; they differ ONLY in the shape of the projection:

  - control:  a plain projection — the SSN cell comes back masked. Proves the harness works.
  - defeating: a projected string literal containing the word ``from``. A text scan for the
              projection's end would match that keyword INSIDE the literal, move the SELECT
              boundary, and never reach "ssn" — returning the real SSN to the caller.

The pipeline is AST-based, so the literal's contents cannot move a clause boundary and both
mask identically.
"""

from __future__ import annotations

import pytest

from provisa.compiler.mask_inject import MaskingRules
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.security.masking import MaskType, MaskingRule
from provisa.transpiler.transpile import transpile
from tests.helpers import governed_for_role

pytestmark = pytest.mark.integration

duckdb = pytest.importorskip("duckdb")

_SSN = "111-11-1111"
_ROWS = [(1, _SSN, "us"), (2, "222-22-2222", "eu")]


def _con():
    con = duckdb.connect()
    con.execute("CREATE TABLE customers (id INTEGER, ssn VARCHAR, region VARCHAR)")
    con.executemany("INSERT INTO customers VALUES (?, ?, ?)", _ROWS)
    return con


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "customers": TableMeta(
            table_id=2,
            field_name="customers",
            type_name="Customers",
            source_id="duck",
            catalog_name="duck",
            schema_name="main",
            table_name="customers",
        )
    }
    ctx.joins = {}
    return ctx


def _ssn_rule() -> MaskingRules:
    return {
        (2, "analyst"): {
            "ssn": (MaskingRule(mask_type=MaskType.constant, value="HIDDEN"), "varchar")
        }
    }


def _govern_and_execute(pg_sql: str) -> list[tuple]:
    """compile → govern → transpile(duckdb) → execute → rows."""
    governed = governed_for_role(pg_sql, _ctx(), "analyst", masking_rules=_ssn_rule())
    return _con().execute(transpile(governed, "duckdb")).fetchall()


def test_control_plain_projection_masks_ssn_in_executed_rows():
    """Baseline: a well-shaped projection masks the SSN cell — the harness works."""
    rows = _govern_and_execute('SELECT "ssn", "region" FROM "customers"')
    flat = [str(c) for row in rows for c in row]
    assert _SSN not in flat, "control: SSN should be masked"
    assert "HIDDEN" in flat, "control: mask expression should reach the rows"


def test_keyword_in_literal_projection_leaks_ssn_end_to_end():
    """REQ-913: a projected literal containing ``from`` no longer defeats masking — the AST projection
    rewrite masks ``ssn`` regardless of a scalar-subquery/keyword-bearing literal in the SELECT list."""
    rows = _govern_and_execute(
        'SELECT \'shipped from warehouse\' AS "note", "ssn", "region" FROM "customers"'
    )
    flat = [str(c) for row in rows for c in row]
    assert _SSN not in flat, "SSN leaked unmasked to executed result"
