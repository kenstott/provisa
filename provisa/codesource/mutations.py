# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL and GraphQL mutations on a code table, routed to its source's commands (REQ-2100).

BINDING. A ``@command`` is bound to a table's operation by its input type, read from the manifest:

- insert takes ``rows``: one parameter, a list of the table's row type;
- update takes ``keys`` and ``changes``: a list of the table's primary-key values (objects whose
  fields are its key columns, or the values themselves for a one-column key), then an object whose
  fields are some of the row type's, each optional (a partial row);
- delete takes ``keys``.

A tag line ``@insert <table>`` (``@update``, ``@delete``) on a command resolves two that fit; it
binds nothing on its own. At most one command a table and operation: two that fit with no tag are
refused by name at registration, and so is a tag naming a command that does not fit. ``<table>`` is
the table's ``@table`` function.

THE WRITE (provisa/pgwire/_pipeline.py): a mutation is governed as any mutation is -- its columns'
write grants, the role's row rules added to an UPDATE's or a DELETE's WHERE. An INSERT's typed rows
are passed to the insert command in batches. An UPDATE or a DELETE first reads the rows it touches
(:func:`key_read`): an ordinary SELECT of the table's key columns, and an UPDATE's SET values
computed per row, over the statement's own FROM and WHERE, routed by the usual gates; then the
command is called with the keys (an update with its changes, one call per set of changes). The
affected count is the number of rows written or keys given. There is no transaction across the
read and the call.
"""

# Requirements: REQ-2100
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp

from provisa.codesource.manifest import (
    COMMAND,
    Anonymous,
    Export,
    ListOf,
    Manifest,
    Named,
    Scalar,
    Shape,
)

INSERT, UPDATE, DELETE = "insert", "update", "delete"
OPERATIONS = (INSERT, UPDATE, DELETE)
#: The column a key read names an UPDATE's computed SET value by: ``__set_<column>``.
SET_PREFIX = "__set_"
#: How many rows or keys one call of a command is given.
BATCH = 500


class MutationRefused(ValueError):
    """A mutation on a code table its source cannot take: named, with why."""

    def __init__(self, table: str, reason: str) -> None:
        super().__init__(f"code table {table!r}: {reason}")
        self.table = table


@dataclass(frozen=True)
class Binding:
    """The command a table's operation is routed to, and its parameters' names."""

    operation: str
    command: str
    names: tuple[str, ...]
    #: Its keys are the key column's values themselves (a one-column key typed as such), not
    #: objects of the key columns.
    bare_keys: bool = False


def _fields(manifest: Manifest, shape: Shape) -> dict[str, Any] | None:
    if isinstance(shape, Named) and shape.name in manifest.types:
        return {f.name: f for f in manifest.types[shape.name].fields}
    if isinstance(shape, Anonymous):
        return {f.name: f for f in shape.fields}
    return None


def _is_keys(manifest: Manifest, shape: Shape, key: list[str], columns: dict[str, Any]) -> bool:
    """Whether ``shape`` is a list of the table's key values: objects whose fields are its key
    columns, each of the column's type, or for a one-column key the values themselves."""
    if not isinstance(shape, ListOf) or not key or any(k not in columns for k in key):
        return False
    fields = _fields(manifest, shape.element)
    if fields is not None:
        return sorted(fields) == sorted(key) and all(
            fields[k].shape == columns[k].shape for k in key
        )
    return len(key) == 1 and shape.element == columns[key[0]].shape


def _fits(manifest: Manifest, command: Export, table: Export, key: list[str]) -> list[str]:
    """The operations ``command``'s input type fits on ``table``."""
    params = command.parameters
    row = table.returns
    columns = _fields(manifest, row) if row is not None else None
    fits: list[str] = []
    if row is not None and len(params) == 1 and params[0].shape == ListOf(row):
        fits.append(INSERT)
    if columns is None:
        return fits
    if len(params) == 1 and _is_keys(manifest, params[0].shape, key, columns):
        fits.append(DELETE)
    if len(params) == 2:
        changed = _fields(manifest, params[1].shape)
        if (
            _is_keys(manifest, params[0].shape, key, columns)
            and changed is not None
            and changed
            and set(changed) <= set(columns)
            and all(f.nullable for f in changed.values())
        ):
            fits.append(UPDATE)
    return fits


def bindings(manifest: Manifest, table: str, key: list[str]) -> dict[str, Binding]:
    """The commands ``table``'s operations are routed to (``table`` its ``@table`` function,
    ``key`` its primary-key columns), by operation; refused by name when two fit one operation
    with no tag between them, or a tag names a command that does not fit."""
    target = next((e for e in manifest.exports if e.name == table and e.kind == "table"), None)
    if target is None:
        raise MutationRefused(table, "its source's code has no @table function of that name")
    candidates: dict[str, list[Export]] = {op: [] for op in OPERATIONS}
    tagged: dict[str, list[str]] = {op: [] for op in OPERATIONS}
    for command in (e for e in manifest.exports if e.kind == COMMAND):
        fits = _fits(manifest, command, target, key)
        for op in fits:
            candidates[op].append(command)
        for op, named in command.binds:
            if named != table:
                continue
            if op not in fits:
                raise MutationRefused(
                    table,
                    f"@{op} {table} on {command.name}: its input type is not what {op} takes",
                )
            tagged[op].append(command.name)
    out: dict[str, Binding] = {}
    for op in OPERATIONS:
        fitting = candidates[op]
        chosen = [c for c in fitting if c.name in tagged[op]] if tagged[op] else fitting
        if len(chosen) > 1:
            names = ", ".join(sorted(c.name for c in chosen))
            raise MutationRefused(
                table,
                f"the commands {names} all fit {op}: tag the one it is with @{op} {table}",
            )
        if chosen:
            (command,) = chosen
            keys = command.parameters[0].shape if op != INSERT else None
            bare = isinstance(keys, ListOf) and isinstance(keys.element, Scalar)
            out[op] = Binding(op, command.name, tuple(p.name for p in command.parameters), bare)
    return out


# -- the statement --------------------------------------------------------------------------------


def operation_of(tree: exp.Expr) -> str:
    if isinstance(tree, exp.Insert):
        return INSERT
    if isinstance(tree, exp.Update):
        return UPDATE
    if isinstance(tree, exp.Delete):
        return DELETE
    raise MutationRefused("?", f"a {type(tree).__name__.upper()} is no write a code table takes")


def key_read(tree: exp.Expr, key: list[str], dialect: str = "postgres") -> tuple[str, list[str]]:
    """An UPDATE's or a DELETE's key read: a SELECT of the target's key columns -- and an UPDATE's
    SET values, each as ``__set_<column>`` -- over the statement's own FROM and WHERE. Its text,
    and the columns its SET values are of, in order."""
    if isinstance(tree, exp.Update):
        target = tree.this
        sets = [(e.this.name, e.expression) for e in tree.expressions]
        joined = tree.args.get("from_") or None  # sqlglot's key for an UPDATE's FROM
    elif isinstance(tree, exp.Delete):
        target = tree.this
        sets = []
        joined = tree.args.get("using") or None  # sqlglot leaves False where none is written
    else:
        raise MutationRefused("?", "only an UPDATE or a DELETE reads the keys it touches")
    alias = target.alias_or_name
    items: list[exp.Expr] = [exp.column(k, table=alias, quoted=True) for k in key]
    items += [
        exp.alias_(value.copy(), f"{SET_PREFIX}{column}", quoted=True) for column, value in sets
    ]
    select = exp.select(*items).from_(target.copy())
    if isinstance(joined, exp.From):
        select = select.join(joined.this.copy(), join_type="cross")
    elif joined is not None:
        for extra in joined if isinstance(joined, list) else [joined]:
            select = select.join(extra.copy(), join_type="cross")
    where = tree.args.get("where")
    if where is not None:
        select = select.where(where.this.copy())
    return select.sql(dialect=dialect), [column for column, _ in sets]


def inserted_rows(tree: exp.Insert, params: list | None) -> list[dict[str, Any]] | None:
    """An INSERT ... VALUES's rows, by column, each value a literal or a bound parameter; None for
    an INSERT ... SELECT, whose rows are read."""
    source = tree.expression
    if not isinstance(source, exp.Values):
        return None
    schema = tree.this
    columns = [c.name for c in schema.expressions] if isinstance(schema, exp.Schema) else []
    if not columns:
        raise MutationRefused(
            schema.name if hasattr(schema, "name") else "?",
            "an INSERT into a code table names its columns",
        )
    rows: list[dict[str, Any]] = []
    for row in source.expressions:
        values = row.expressions if isinstance(row, exp.Tuple) else [row]
        rows.append({c: _value(v, params) for c, v in zip(columns, values, strict=True)})
    return rows


def _value(node: exp.Expr, params: list | None) -> Any:
    if isinstance(node, (exp.Placeholder, exp.Parameter)):
        index = int(str(node.this.this if isinstance(node.this, exp.Expr) else node.this))
        assert params is not None  # a bound statement's parameter
        return params[index - 1]
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Boolean):
        return node.this
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        text = str(node.this)
        return float(text) if any(c in text for c in ".eE") else int(text)
    if isinstance(node, exp.Neg):
        return -_value(node.this, params)
    if isinstance(node, exp.Cast):
        return _value(node.this, params)
    raise MutationRefused(
        "?",
        f"an INSERT into a code table gives each value as a literal or a parameter, not {node.sql()}",
    )


def keys_of(rows: list[tuple], key: list[str], bare: bool) -> list[Any]:
    """The keys of a key read's rows (its first ``len(key)`` columns), as the command takes them:
    objects of the key columns (``key``, named as the code declares them), or for a one-column key
    typed as such the values themselves."""
    if bare:
        return [r[0] for r in rows]
    return [dict(zip(key, r[: len(key)], strict=True)) for r in rows]


def parse(sql: str, dialect: str = "postgres") -> exp.Expr:
    return sqlglot.parse_one(sql, read=dialect)
