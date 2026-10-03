# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e1d92-5c48-4f06-a1e3-8d2f6c9b0e57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The governance a write is admitted by, for tests of the statements a surface compiles.

A surface's compiled write (a GraphQL mutation, a Cypher CREATE/SET/DELETE) is admitted, and
given the role's row filter, by the one admission in the governance stage
(provisa/compiler/write_admission.py). These helpers build the governance for the table the
statement writes, so a test asserts what that stage does to the statement it compiled."""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.stage2 import GovernanceContext, apply_governance


def _ref(table: exp.Table) -> str:
    return f"{table.db}.{table.name}" if table.db else table.name


def write_governance(
    tables: dict[str, tuple[int, list[str]]],
    *,
    rls: dict[int, str] | None = None,
    writable: dict[int, list[str]] | None = None,
    can_write: bool = True,
    role_id: str = "writer",
    write_ops: dict[int, set[str]] | None = None,
) -> GovernanceContext:
    """Governance for ``role_id`` over ``tables`` — ``{"schema.table": (table_id, columns)}`` —
    holding the write right (unless ``can_write`` is False), named on every column unless
    ``writable`` says which, with ``rls`` as its row filters."""
    gov = GovernanceContext()
    gov.role_id = role_id
    gov.can_write = can_write
    gov.table_map = {ref: tid for ref, (tid, _) in tables.items()}
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in tables.values()}
    named = writable if writable is not None else {tid: cols for tid, cols in tables.values()}
    gov.writable_columns = {tid: frozenset(cols) for tid, cols in named.items()}
    gov.rls_rules = dict(rls or {})
    # Each table's source takes every write unless ``write_ops`` says which
    # (executor/write_capability.py).
    every = {"insert", "update", "delete"}
    offered = write_ops if write_ops is not None else {tid: every for tid, _ in tables.values()}
    gov.write_ops = {tid: frozenset(ops) for tid, ops in offered.items()}
    return gov


def target_ref(sql: str) -> str:
    """``schema.table`` of the table ``sql`` writes."""
    tree = sqlglot.parse_one(sql, read="postgres")
    target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
    assert isinstance(target, exp.Table), sql
    return _ref(target)


def admitted(sql: str, gov: GovernanceContext, params: list | None = None) -> str:
    """``sql`` as the governance stage admits it (raises when it does not)."""
    return apply_governance(sql, gov, {}, params)
