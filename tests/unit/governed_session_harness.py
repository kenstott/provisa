# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e94b1-5c3a-4f86-a0e7-19b6c4d8f3a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph session whose every read is governed as the server governs it (REQ-2176, REQ-039):
each SQL statement a Cypher read is translated to is judged by ``validate_sql`` with the
session's tables bound, before DuckDB runs it. A refusal raises :class:`Refused` with the
violations, as the server answers a refused statement."""

from __future__ import annotations

from typing import Any

from provisa.compiler import temp_tables
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import ValidationViolation, validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.cypher.parser import parse_cypher
from provisa.ephemeral import seam
from provisa.gql.assembler import assemble_rows, take_overrun, to_serializable
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.temporal import bindable
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.ephemeral_harness import Session

_ROLE = {"id": "analyst", "capabilities": [], "domain_access": ["*"]}


class Refused(AssertionError):
    """A read governance refused: its SQL and the violations."""

    def __init__(self, sql: str, violations: list[ValidationViolation]) -> None:
        self.sql = sql
        self.violations = violations
        found = "; ".join(f"[{v.code}] {v.message}" for v in violations)
        super().__init__(f"{found}\n{sql}")


def _empty_model() -> GovernanceContext:
    return GovernanceContext(
        rls_rules={},
        masking_rules={},
        visible_columns={},
        table_map={},
        all_columns={},
        limit_ceiling=None,
        sample_size=None,
    )


class GovernedSession(Session):
    """An empty graph's session (the TCK's ``Given an empty graph``), its reads governed."""

    def __init__(self, monkeypatch: Any) -> None:
        super().__init__(monkeypatch, {})
        #: Each read's SQL as governance judged it, in order.
        self.judged: list[str] = []

    async def read(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """A read as the server runs it: translated, governed, run, a walk with no upper bound
        that reached the depth limit refused by name (take_overrun), its rows assembled."""
        lm = seam.label_map(self.model, self.session)
        ast, ordered, graph_vars = cypher_to_sql(parse_cypher(query, lm), lm, params)
        ast = apply_graph_rewrites(ast, graph_vars, lm)
        sql = seam.expand(ast, lm, self.model, self.session).sql(dialect="postgres")
        self.judged.append(sql)
        token = temp_tables.bind(self.session)
        try:
            found = validate_sql(sql, CompilationContext(), _empty_model(), _ROLE, [])
        finally:
            temp_tables.unbind(token)
        if found:
            raise Refused(sql, found)
        result = self.duck.execute(transpile_to_duckdb(sql), [bindable(params[n]) for n in ordered])
        columns = [d[0] for d in result.description]
        rows = take_overrun([dict(zip(columns, r)) for r in result.fetchall()])
        rows = assemble_rows(rows, graph_vars)
        return [to_serializable(r, keep_values=True) for r in rows]
