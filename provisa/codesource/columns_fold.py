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
the inputs. As the rows are read (:func:`apply`), the inputs are handed to the function in
batches of 1000 rows across the source's workers, streamed, and each call's inputs are replaced,
in place, by the output columns the function returned for each row. The output columns take the function's field names;
an outer projection renames them.

A call as an item of a common table expression's select list is a staged step
(:mod:`provisa.codesource.columns_stage`): the CTE is run on its own and the call folded there. A
call anywhere else -- a WHERE, a JOIN, a GROUP BY, inside another expression -- is refused by name:
the rows it needs are not read there. So is an
aliased call (its outputs are several columns, named by the function) and one given another number
of arguments than the function's input columns.
"""

# Requirements: REQ-2131

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Collection, Generator, Iterator, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
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
            if (
                isinstance(call.parent, exp.Select)
                and isinstance(call.parent.parent, exp.CTE)
                and any(item is call for item in call.parent.expressions)
            ):
                # A SELECT's CTE holding one is staged (columns_stage); this statement is not one.
                raise _refused(
                    name,
                    "a common table expression holding it is run as a staged step only in a "
                    "SELECT, not in an INSERT ... SELECT: write it in the SELECT's own select list",
                )
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


def _cut(batches: Iterator[list[tuple]]) -> Generator[list[tuple], None, None]:
    """The rows of ``batches`` (record batches of any size, as read) cut again into batches of
    :data:`~provisa.codesource.columns.BATCH` rows, each passed on as soon as it is full, the last
    when the rows end: the unit a worker takes, whatever its call costs to start."""
    from provisa.codesource.columns import BATCH

    held: list[tuple] = []
    for batch in batches:
        held.extend(batch)
        while len(held) >= BATCH:
            yield held[:BATCH]
            held = held[BATCH:]
    if held:
        yield held


@dataclass(frozen=True)
class _Step:
    """One call applied to a batch: where its inputs sit in the row as the calls before it left
    it, how many, its runner and its output columns' names."""

    at: int
    width: int
    runner: Any
    outputs: tuple[str, ...]


def _enriched(steps: Sequence[_Step], index: int, rows: list[tuple]) -> list[tuple]:
    """Batch ``index``'s rows with each call's input columns replaced by its outputs. Batch ``i``
    runs on worker ``i`` of each source, modulo its workers."""
    data = [list(r) for r in rows]
    for step in steps:
        inputs = [[row[step.at + k] for row in data] for k in range(step.width)]
        answered = step.runner.run(index % step.runner.workers, inputs)
        for row, result in zip(data, answered):
            row[step.at : step.at + step.width] = [result[n] for n in step.outputs]
    return [tuple(r) for r in data]


async def apply(result: Any, calls: Sequence[ColumnsCall], state: Any) -> Any:
    """``result`` with each call's input columns replaced by the output columns its function
    returned for each row, streamed (REQ-2131, "COLUMN TRANSFORMS STREAM"): the read's record
    batches are cut into batches of 1000 rows (:func:`_cut`), each sent to a worker as soon as it is
    full, at most as many in flight as the source has workers, and each answered batch passed on
    at once, in the read's order. The result is never held whole: the first rows reach the caller
    before the last are read."""
    from provisa.codesource.columns import runner
    from provisa.executor.result import StreamingQueryResult

    names = list(result.column_names)
    types: list[str] | None = None if result.column_types is None else list(result.column_types)
    steps: list[_Step] = []
    for call in calls:
        at = names.index(call.inputs[0])
        width = len(call.inputs)
        outputs = tuple(n for n, _ in call.outputs)
        ready = await runner(state, state.column_transforms[call.command])
        steps.append(_Step(at, width, ready, outputs))
        names[at : at + width] = outputs
        if types is not None:
            types[at : at + width] = [t for _, t in call.outputs]
    # Consecutive batches run on distinct workers of every source, so each worker runs one call at
    # a time: the window is the fewest workers any of the calls' sources runs.
    window = min(step.runner.workers for step in steps)
    pool = ThreadPoolExecutor(max_workers=window, thread_name_prefix="columns")

    def answered() -> Iterator[list[tuple]]:
        source = _cut(iter(result.batches()))
        running: deque[Future] = deque()
        try:
            for index, rows in enumerate(source):
                if len(running) == window:
                    yield running.popleft().result()
                running.append(pool.submit(_enriched, steps, index, rows))
            while running:
                yield running.popleft().result()
        finally:
            source.close()
            pool.shutdown(wait=True, cancel_futures=True)

    return StreamingQueryResult(
        answered(), names, types, on_release=lambda: pool.shutdown(wait=False)
    )
