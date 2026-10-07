# Copyright (c) 2026 Kenneth Stott
# Canary: ed4571f3-e956-4c5c-845c-4d4a0a922037
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A mutation kept in a Reversible environment's change log (REQ-1942).

The governed, admitted mutation is never run on the source. What it would write is read first,
through the one pipeline as the same role -- so the row filter and the environment's already kept
mutations apply -- and kept in the table's change log (:mod:`provisa.core.env_changes`):

* INSERT: each row it supplies, as a new version of its key;
* UPDATE: each row its WHERE reaches, with its SET expressions applied, as a new version;
* DELETE: each key its WHERE reaches, as a deletion;
* TRUNCATE: one marker -- every row read before it is deleted (its admission requires the write
  right and no row filter on the table);
* MERGE: what each WHEN clause would do, read in clause order -- a matched row takes the first
  matched clause whose condition holds (an update as a new version, a delete as a deletion), an
  unmatched source row the first unmatched clause (an insert as a new version).

A table written this way needs a primary key, by which the versions are applied.
"""

# Requirements: REQ-1942

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp


@dataclass(frozen=True)
class KeptMutation:
    statement: str  # the governed, admitted mutation, in the governed dialect
    table_id: int
    role_id: str
    params: list | None


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _reads(tree: Any, columns: list[str], key: list[str]) -> tuple[str, str, list[str]]:
    """The read that says what ``tree`` writes: (its statement, the kept op, the columns it
    gives)."""
    from provisa.core.env_changes import DELETE, TRUNCATE, UPSERT

    if isinstance(tree, exp.TruncateTable):
        return "", TRUNCATE, []  # one marker: nothing to read
    if isinstance(tree, exp.Insert):
        target = tree.this
        listed = [c.name for c in target.expressions] if isinstance(target, exp.Schema) else columns
        missing = [k for k in key if k not in listed]
        if missing:
            raise PermissionError(
                "a kept INSERT names every key column, so its row can be applied by its key: "
                + ", ".join(missing)
            )
        source = tree.expression
        names = ", ".join(_q(c) for c in listed)
        return f"SELECT * FROM ({source.sql(dialect='postgres')}) AS __v({names})", UPSERT, listed
    if isinstance(tree, exp.Update):
        table = tree.this
        sets = {e.this.name: e.expression for e in tree.expressions}
        unknown = sorted(set(sets) - set(columns))
        if unknown:
            raise PermissionError(f"UPDATE sets no column of the table: {', '.join(unknown)}")
        items = ", ".join(
            f"{sets[c].sql(dialect='postgres')} AS {_q(c)}" if c in sets else _q(c) for c in columns
        )
        where = tree.args.get("where")
        tail = f" {where.sql(dialect='postgres')}" if where is not None else ""
        return f"SELECT {items} FROM {table.sql(dialect='postgres')}{tail}", UPSERT, columns
    if isinstance(tree, exp.Delete):
        table = tree.this
        where = tree.args.get("where")
        tail = f" {where.sql(dialect='postgres')}" if where is not None else ""
        items = ", ".join(_q(k) for k in key)
        return f"SELECT {items} FROM {table.sql(dialect='postgres')}{tail}", DELETE, key
    raise PermissionError(
        f"{type(tree).__name__.upper()} is not kept in an environment's change log; use INSERT, "
        "UPDATE, DELETE, MERGE or TRUNCATE"
    )


def _sql(node: Any) -> str:
    return node.sql(dialect="postgres")


def _merge_steps(tree: Any, columns: list[str], key: list[str]) -> list[tuple[str, str, list[str]]]:
    """The reads that say what a MERGE writes, one per WHEN clause, in clause order: (statement,
    kept op, columns it gives)."""
    from provisa.core.env_changes import DELETE, UPSERT

    target, source, on = tree.this, tree.args["using"], tree.args["on"]
    alias = target.alias_or_name
    steps: list[tuple[str, str, list[str]]] = []
    earlier = {True: [], False: []}  # each side's earlier conditions: a row takes the first
    for when in tree.args["whens"].expressions:
        if when.args.get("source"):
            raise PermissionError(
                "MERGE ... WHEN NOT MATCHED BY SOURCE is not kept in an environment's change log"
            )
        matched = bool(when.args.get("matched"))
        condition = when.args.get("condition")
        holds = _sql(condition) if condition is not None else "TRUE"
        guard = " AND ".join([f"({holds})", *(f"NOT ({c})" for c in earlier[matched])])
        earlier[matched].append(holds)
        then = when.args["then"]
        if matched:
            joined = f"{_sql(target)} JOIN {_sql(source)} ON {_sql(on)} WHERE {guard}"
            if isinstance(then, exp.Update):
                sets = {e.this.name: e.expression for e in then.expressions}
                unknown = sorted(set(sets) - set(columns))
                if unknown:
                    raise PermissionError(
                        f"MERGE sets no column of the table: {', '.join(unknown)}"
                    )
                items = ", ".join(
                    f"{_sql(sets[c])} AS {_q(c)}" if c in sets else f"{_q(alias)}.{_q(c)}"
                    for c in columns
                )
                steps.append((f"SELECT {items} FROM {joined}", UPSERT, columns))
            else:  # DELETE
                items = ", ".join(f"{_q(alias)}.{_q(k)}" for k in key)
                steps.append((f"SELECT {items} FROM {joined}", DELETE, key))
            continue
        if not isinstance(then, exp.Insert):
            raise PermissionError("MERGE ... WHEN NOT MATCHED takes an INSERT")
        listed = [c.name for c in then.this.expressions] if then.this is not None else columns
        missing = [k for k in key if k not in listed]
        if missing:
            raise PermissionError(
                "a kept MERGE inserts every key column, so its row can be applied by its key: "
                + ", ".join(missing)
            )
        values = ", ".join(
            f"{_sql(v)} AS {_q(c)}"
            for c, v in zip(listed, then.expression.expressions, strict=True)
        )
        unmatched = f"NOT EXISTS (SELECT 1 FROM {_sql(target)} WHERE {_sql(on)})"
        steps.append(
            (f"SELECT {values} FROM {_sql(source)} WHERE {unmatched} AND {guard}", UPSERT, listed)
        )
    return steps


async def keep(mutation: KeptMutation, state: Any) -> Any:
    """Keep ``mutation`` in its table's change log; the result a write gives: the rows affected."""
    from provisa.api.app import _rebuild_schemas
    from provisa.core import env_changes
    from provisa.core.environments import org_schema
    from provisa.core.ir_types import to_ir
    from provisa.core.request_context import active_env, require_current_org
    from provisa.executor.result import QueryResult
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    table = next(t for t in state.tables if t["id"] == mutation.table_id)
    columns = [c["column_name"] for c in table["columns"]]
    key = [c["column_name"] for c in table["columns"] if c.get("is_primary_key")]
    if not key:
        raise PermissionError(
            f"{table['table_name']!r} declares no primary key, and a mutation kept in a "
            f"Reversible environment is applied by it (REQ-1942)"
        )
    tree = sqlglot.parse_one(mutation.statement, read="postgres")
    steps = (
        _merge_steps(tree, columns, key)
        if isinstance(tree, exp.Merge)
        else [_reads(tree, columns, key)]
    )
    # Every read before any write: the statement sees the table as it stood.
    found: list[tuple[str, list[dict[str, Any]]]] = []
    for read, op, given in steps:
        rows: list[dict[str, Any]] = []
        if read:
            plan = await _govern_and_route(read, mutation.role_id, params=mutation.params)
            result = await _execute_plan(plan, state)
            rows = [dict(zip(given, r, strict=True)) for r in result.rows]
        found.append((op, rows))
    types = [(c["column_name"], to_ir(c["data_type"])) for c in table["columns"]]
    schema = org_schema(require_current_org(), active_env())
    first = mutation.table_id not in state._active_runtime().kept
    kept = 0
    async with state.tenant_db.acquire() as conn:
        for op, rows in found:
            kept += await env_changes.keep(
                conn, schema, mutation.table_id, types, op, rows, by=mutation.role_id
            )
    if first:
        # The table's reads now apply its log: rebuilt, so every compiled read reads through it.
        await _rebuild_schemas()
    return QueryResult(rows=[], column_names=[], rowcount=kept)
