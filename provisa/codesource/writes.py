# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governed write to a code table, carried out (REQ-2100): its command found by binding
(provisa/codesource/mutations.py), its rows or the keys it touches read, and the command called
with them in batches. The pipeline plans it (:func:`plan`) and runs it at its terminal
(:func:`run`); the key read is an ordinary read the pipeline routes."""

# Requirements: REQ-2100
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from sqlglot import exp

from provisa.codesource.mutations import (
    BATCH,
    DELETE,
    INSERT,
    UPDATE,
    Binding,
    MutationRefused,
    bindings,
    inserted_rows,
    key_read,
    keys_of,
    operation_of,
)


@dataclass
class CodeWrite:
    """What a write to a code table does, decided when it is planned."""

    table: dict  # its state.tables row
    operation: str
    binding: Binding
    #: Each registered column's name as the code declares it, by its registered name.
    declared: dict[str, str]
    #: The key columns, as registered (what the key read returns) and as the code declares them.
    key: list[str]
    #: An INSERT ... VALUES's rows, by registered column; None when the rows are read.
    rows: list[dict[str, Any]] | None = None
    #: The read the write needs: an INSERT ... SELECT's SELECT, or an UPDATE's or a DELETE's keys.
    read_sql: str | None = None
    #: The columns of an INSERT ... SELECT's rows, or an UPDATE's SET, in order, as registered.
    columns: list[str] = field(default_factory=list)


def is_code_table(state: Any, table_id: int | None) -> bool:
    from provisa.codesource.source import CODE_SOURCE_TYPES

    table = _table(state, table_id)
    return (
        table is not None
        and (getattr(state, "source_types", {}) or {}).get(table["source_id"]) in CODE_SOURCE_TYPES
    )


def _table(state: Any, table_id: int | None) -> dict | None:
    return next((t for t in getattr(state, "tables", None) or [] if t.get("id") == table_id), None)


async def _prepared(state: Any, table: dict) -> Any:
    from provisa.codesource.source import context_for, prepare
    from provisa.federation.registry_view import registered_sources

    found = [s for s in await registered_sources(state) if s.id == table["source_id"]]
    if not found:
        raise MutationRefused(
            table["table_name"], f"its source {table['source_id']!r} is not registered"
        )
    source = found[0]
    return await prepare(source.id, source.type.value, source.mapping, await context_for(state))


async def plan(
    state: Any, table_id: int, tree: exp.Expr, params: list | None, dialect: str = "postgres"
) -> CodeWrite:
    """The write ``tree`` (governed) makes on the code table ``table_id``: refused by name when
    the table has no command bound for it, or an UPDATE or a DELETE is made on a table with no
    primary key."""
    from provisa.codesource.names import name_tables
    from provisa.codesource.table import export_of

    table = _table(state, table_id)
    assert table is not None  # a registered table, as written_table_id resolved it
    name = table["table_name"]
    operation = operation_of(tree)
    prepared = await _prepared(state, table)
    function = export_of(table)
    forms = name_tables(prepared.manifest).get(function, {}).get("columns", [])
    declared = {f["sql"]: f["name"] for f in forms} | {f["name"]: f["name"] for f in forms}
    registered_key = [c["column_name"] for c in table["columns"] if c.get("is_primary_key")]
    if operation in (UPDATE, DELETE) and not registered_key:
        raise MutationRefused(
            name, f"an {operation.upper()} names its rows by the primary key, and it has none"
        )
    key = [declared.get(k, k) for k in registered_key]
    bound = bindings(prepared.manifest, function, key).get(operation)
    if bound is None:
        raise MutationRefused(name, f"no command of its source takes {operation} on {function}")
    write = CodeWrite(table, operation, bound, declared, registered_key)
    if operation == INSERT:
        assert isinstance(tree, exp.Insert)
        write.rows = inserted_rows(tree, params)
        if write.rows is None:
            schema = tree.this
            write.columns = (
                [c.name for c in schema.expressions] if isinstance(schema, exp.Schema) else []
            )
            write.read_sql = tree.expression.sql(dialect=dialect)
        return write
    write.read_sql, write.columns = key_read(tree, registered_key, dialect)
    return write


def _call(prepared: Any, binding: Binding, args: list) -> None:
    prepared.worker().command(binding.command, args, list(binding.names))


async def run(state: Any, write: CodeWrite, read_rows: list[tuple] | None) -> int:
    """Carry the write out: the command called with its rows or keys in batches of
    :data:`BATCH`. The affected count: the rows written, or the keys given."""
    prepared = await _prepared(state, write.table)
    code = [write.declared.get(k, k) for k in write.key]

    def _as_declared(row: dict[str, Any]) -> dict[str, Any]:
        return {write.declared.get(c, c): v for c, v in row.items()}

    def _go() -> int:
        if write.operation == INSERT:
            rows = write.rows
            if rows is None:
                assert read_rows is not None
                rows = [dict(zip(write.columns, r, strict=True)) for r in read_rows]
            rows = [_as_declared(r) for r in rows]
            for at in range(0, len(rows), BATCH):
                _call(prepared, write.binding, [rows[at : at + BATCH]])
            return len(rows)
        assert read_rows is not None  # the key read's rows
        keys = keys_of(read_rows, code, write.binding.bare_keys)
        if write.operation == DELETE:
            for at in range(0, len(keys), BATCH):
                _call(prepared, write.binding, [keys[at : at + BATCH]])
            return len(keys)
        # An UPDATE: the keys grouped by the changes their SET gives them (one group when the SET
        # is literals; one a key when it is computed per row), each group's call given its own.
        width = len(write.key)
        groups: dict[tuple, list[Any]] = {}
        for k, row in zip(keys, read_rows, strict=True):
            changes = tuple(zip(write.columns, row[width:], strict=True))
            groups.setdefault(changes, []).append(k)
        for changes, grouped in groups.items():
            given = {write.declared.get(c, c): v for c, v in changes}
            for at in range(0, len(grouped), BATCH):
                _call(prepared, write.binding, [grouped[at : at + BATCH], given])
        return len(keys)

    return await asyncio.to_thread(_go)
