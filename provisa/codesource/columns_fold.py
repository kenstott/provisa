# Copyright (c) 2026 Kenneth Stott
# Canary: f9bb89ca-00f4-45bc-9458-868875ba86a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered column transform in a statement (REQ-2131): ``clean_address(raw_address)`` as an
item of the select list stands for the function's output columns (``street, city, country``).

Provisa applies it; no source or engine sees the call. After governance (:func:`fold`) each call
is admitted as any command is -- the role is assigned it and reaches its domain -- and its item is
replaced by the call's input expressions, each under a name of its own, so the statement reads
the inputs. Once the rows are read (:func:`apply`), the inputs are handed to the function in
batches across the source's workers and each call's inputs are replaced, in place, by the output
columns the function returned for each row. The output columns take the function's field names;
an outer projection renames them.

A call anywhere but as an item of the statement's own select list -- a WHERE, a JOIN, a GROUP BY,
inside another expression -- is refused by name: the rows it needs are not read there. So is an
aliased call (its outputs are several columns, named by the function) and one given another number
of arguments than the function's input columns.
"""

# Requirements: REQ-2131

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import sqlglot
import sqlglot.expressions as exp

#: The name a call's input column takes in the folded statement: call ``i``, input ``k``.
_INPUT = "__provisa_columns_{call}_{input}"


@dataclass(frozen=True)
class ColumnsCall:
    """One select-list call: the command, its folded input columns and its output columns."""

    command: str
    inputs: tuple[str, ...]  # the names its input columns take in the folded statement
    outputs: tuple[tuple[str, str], ...]  # (name, IR type), the function's output columns


@dataclass(frozen=True)
class Folded:
    """A governed statement with its column transforms' items replaced by their inputs."""

    sql: str
    calls: tuple[ColumnsCall, ...] = ()


def mentions(sql: str, transforms: Collection[str]) -> bool:
    """Whether ``sql`` may call one of ``transforms`` (their names) -- a cheap check before
    parsing."""
    flat = sql.lower().replace(" ", "")
    return any(f"{name.lower()}(" in flat for name in transforms)


def called(sql: str, transforms: Collection[str]) -> list[str]:
    """The registered column transforms (of ``transforms``, their names) ``sql`` calls, by name,
    in first use order."""
    if not mentions(sql, transforms):
        return []
    by_name = {name.lower(): name for name in transforms}
    names: list[str] = []
    for node in sqlglot.parse_one(sql, read="postgres").find_all(exp.Anonymous):
        name = by_name.get(node.name.lower())
        if name is not None and name not in names:
            names.append(name)
    return names


def _output_select(tree: exp.Expr) -> exp.Select | None:  # pyright: ignore[reportPrivateImportUsage]
    if isinstance(tree, exp.Select):
        return tree
    if isinstance(tree, exp.Insert) and isinstance(tree.expression, exp.Select):
        return tree.expression
    return None


def _refused(name: str, reason: str) -> Exception:
    from provisa.codesource.columns import ColumnTransformRefused

    return ColumnTransformRefused(name, reason)


def fold(
    governed_sql: str,
    transforms: Mapping[str, dict],
    admit: Callable[[dict, str], None],
) -> Folded:
    """``governed_sql`` with each select-list call of a registered column transform replaced by its
    input columns (see the module docstring). ``transforms`` are the registered column transforms by
    name; ``admit(command, name)`` refuses a call the role may not make."""
    if not mentions(governed_sql, transforms):
        return Folded(governed_sql)
    tree = sqlglot.parse_one(governed_sql, read="postgres")
    by_name = {name.lower(): name for name in transforms}
    found = [n for n in tree.find_all(exp.Anonymous) if n.name.lower() in by_name]
    if not found:
        return Folded(governed_sql)
    output = _output_select(tree)
    calls: list[ColumnsCall] = []
    for index, call in enumerate(found):
        name = by_name[call.name.lower()]
        command = transforms[name]
        admit(command, name)
        if output is None or not any(item is call for item in output.expressions):
            if isinstance(call.parent, exp.Alias):
                raise _refused(
                    name,
                    "its output columns are named by the function; rename them in an outer "
                    "projection, not with an alias on the call",
                )
            raise _refused(
                name,
                "it is applied to the rows once they are read: write it as an item of the "
                "statement's select list",
            )
        arguments = command["arguments"]
        if len(call.expressions) != len(arguments):
            raise _refused(
                name,
                f"it takes {len(arguments)} input columns "
                f"({', '.join(a['name'] for a in arguments)}), not {len(call.expressions)}",
            )
        inputs = tuple(_INPUT.format(call=index, input=k) for k in range(len(arguments)))
        position = next(i for i, item in enumerate(output.expressions) if item is call)
        items = [
            exp.Alias(this=expression.copy(), alias=exp.to_identifier(alias))
            for expression, alias in zip(call.expressions, inputs)
        ]
        output.set(
            "expressions",
            [*output.expressions[:position], *items, *output.expressions[position + 1 :]],
        )
        outputs = tuple((c["name"], c["type"]) for c in command["output_columns"])
        calls.append(ColumnsCall(command=name, inputs=inputs, outputs=outputs))
    return Folded(tree.sql(dialect="postgres"), tuple(calls))


def reshape(shape: Sequence[tuple[str, Any]], calls: Sequence[ColumnsCall]) -> list:
    """A result shape of the folded statement (``(name, type)`` per column) as the statement's
    own: each call's input columns replaced by its output columns."""
    out = list(shape)
    for call in calls:
        names = [n for n, _ in out]
        at = names.index(call.inputs[0])
        out[at : at + len(call.inputs)] = list(call.outputs)
    return out


async def apply(result: Any, calls: Sequence[ColumnsCall], state: Any) -> Any:
    """``result`` with each call's input columns replaced by the output columns its function
    returned for each row (:func:`provisa.codesource.columns.transform`)."""
    from provisa.codesource.columns import transform
    from provisa.executor.result import QueryResult

    materialized = result.materialize()
    names = list(materialized.column_names)
    types: list[str] | None = (
        None if materialized.column_types is None else list(materialized.column_types)
    )
    data = [list(r) for r in materialized.rows]
    for call in calls:
        at = names.index(call.inputs[0])
        width = len(call.inputs)
        inputs = [[row[at + k] for row in data] for k in range(width)]
        command = state.column_transforms[call.command]
        results = await transform(state, command, inputs)
        out_names = [n for n, _ in call.outputs]
        for row, answered in zip(data, results):
            row[at : at + width] = [answered[n] for n in out_names]
        names[at : at + width] = out_names
        if types is not None:
            types[at : at + width] = [t for _, t in call.outputs]
    return QueryResult(
        rows=[tuple(r) for r in data],
        column_names=names,
        column_types=types,
        rowcount=materialized.rowcount,
    )
