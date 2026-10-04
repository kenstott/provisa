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


def run_after_write(table_id: int, table_name: str, source_id: str) -> dict[str, list]:
    """Finalize a successful write plan through the pipeline's terminal and record what the
    steps after a write did."""
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import patch

    from provisa.kafka import change_events as _change_mod
    from provisa.kafka import sink_executor as _sink_mod
    from provisa.pgwire._pipeline import _Plan, finalize_audit
    from provisa.transpiler.router import Route

    calls: dict[str, list] = {"invalidated": [], "stale": [], "events": [], "sinks": []}

    class _Store:
        async def invalidate_by_table(self, tid, tenant_id=None):
            calls["invalidated"].append(tid)
            return 1

    async def _sinks(name, _state):
        calls["sinks"].append(name)
        return 0

    state = SimpleNamespace(
        response_cache_store=_Store(),
        model_db="fake",
        tenant_db="fake",
        org_id="org-a",
        model_stamp=1,
        contexts={
            "writer": SimpleNamespace(
                tables={
                    table_name: SimpleNamespace(
                        table_id=table_id, table_name=table_name, source_id=source_id
                    )
                }
            )
        },
        mv_registry=SimpleNamespace(mark_stale=calls["stale"].append),
        hot_manager=None,
    )
    plan = _Plan(
        route=Route.DIRECT,
        sql="INSERT",
        source_id=source_id,
        dialect="postgres",
        role_id="writer",
        table_ids=(table_id,),
        writes_tables=True,
        written_table_id=table_id,
    )

    async def _no_replica(_state, _table_id, _source_id, _reason):
        return False  # no replica store here: the build request has its own tests

    with (
        patch.object(_change_mod, "emit_change_event", lambda *a: calls["events"].append(a)),
        patch.object(_sink_mod, "trigger_sinks_for_table", _sinks),
        patch("provisa.federation.replica_builds.request_if_replicated", _no_replica),
    ):

        async def _finalize():
            await finalize_audit(plan, 200, state)
            await asyncio.sleep(0)  # the sink run is spawned in the background

        asyncio.run(_finalize())
    return calls
