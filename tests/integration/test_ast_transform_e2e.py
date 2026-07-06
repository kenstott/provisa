# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Differential e2e proof that the governance pipeline is AST-based (REQ-913).

Runs the FULL path — compile → mask-inject → transpile → execute — against in-process
DuckDB and inspects the RETURNED ROWS, not the SQL text. The two tests share one governor,
one dataset, and one execution harness; they differ ONLY in the shape of the projection:

  - control:  a plain projection — the string masker's regex boundary happens to be right,
              so the SSN cell comes back masked. Proves the harness works.
  - defeating: a projected string literal containing the word ``from``. `_find_select_end`
              matches that keyword INSIDE the literal, moves the SELECT boundary, and the
              masker never reaches "ssn" — the real SSN is returned to the caller.

If the pipeline were AST-based, the literal's contents could not move a clause boundary and
both would mask identically. The defeating case is xfail(strict): it leaks today and flips to
a hard failure (forcing marker removal) once transforms operate on the tree.
"""

from __future__ import annotations

import pytest

from provisa.compiler.mask_inject import MaskingRules, inject_masking
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
)
from provisa.security.masking import MaskType, MaskingRule
from provisa.transpiler.transpile import transpile

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
    """compile → mask-inject → transpile(duckdb) → execute → rows."""
    compiled = CompiledQuery(
        sql=pg_sql,
        params=[],
        root_field="customers",
        columns=[ColumnRef(alias=None, column="ssn", field_name="ssn", nested_in=None)],
        sources={"duck"},
    )
    governed = inject_masking(compiled, _ctx(), _ssn_rule(), "analyst")
    return _con().execute(transpile(governed.sql, "duckdb")).fetchall()


def test_control_plain_projection_masks_ssn_in_executed_rows():
    """Baseline: a well-shaped projection masks the SSN cell — the harness works."""
    rows = _govern_and_execute('SELECT "ssn", "region" FROM "customers"')
    flat = [str(c) for row in rows for c in row]
    assert _SSN not in flat, "control: SSN should be masked"
    assert "HIDDEN" in flat, "control: mask expression should reach the rows"


@pytest.mark.xfail(
    strict=True,
    reason="REQ-913: keyword in string literal moves the regex SELECT boundary → SSN leaks",
)
def test_keyword_in_literal_projection_leaks_ssn_end_to_end():
    """A projected literal containing ``from`` defeats the regex boundary — SSN reaches the caller."""
    rows = _govern_and_execute(
        'SELECT \'shipped from warehouse\' AS "note", "ssn", "region" FROM "customers"'
    )
    flat = [str(c) for row in rows for c in row]
    assert _SSN not in flat, "SSN leaked unmasked to executed result"
