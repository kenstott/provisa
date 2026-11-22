# Copyright (c) 2026 Kenneth Stott
# Canary: e67f373e-cbec-4ec1-9475-323bbb8d1928
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every form the SDK allows: pyright reports no error here (tests/test_typing.py)."""

import dataclasses
from collections.abc import AsyncIterator, Iterator, Mapping
from typing import Any, Literal

from provisa_sdk import (
    AsyncRows,
    AsyncStream,
    Batch,
    Results,
    Rows,
    Stream,
    change,
    columns,
    command,
    health,
    on_start,
    on_stop,
    query,
    requires,
    stream,
    table,
)
from provisa_services import Filter, ReadContext, TableRef


@dataclasses.dataclass
class Inputs:
    region: str = "EU"


@dataclasses.dataclass
class Row:
    id: int


@table
def no_parameters() -> list[Row]:
    return []


@table
def inputs_only(inputs: Inputs) -> Rows[Row]:
    return []


@table
def context_alone(context: ReadContext) -> Rows[Row]:
    first: Filter = context.filters[0]
    return [Row(len(first.column))]


@table()
def both(inputs: Inputs, context: ReadContext) -> Iterator[Row]:
    yield Row(context.limit or 0)


@table
async def awaited(inputs: Inputs) -> list[Row]:
    return []


@table
async def produced(inputs: Inputs) -> AsyncRows[Row]:
    for row in ():
        yield row


@table(loader=True)
def loader(inputs: Inputs, context: ReadContext) -> list[Row]:
    return []


@table(loader=True)
def loader_keyed(inputs: Inputs, context: ReadContext) -> list[Row] | dict[str, list[Row]]:
    return {} if context.keys else []


@table(loader=50)
def loader_sized(context: ReadContext) -> list[Row]:
    return []


@stream
def ticking(inputs: Inputs) -> Stream[Row]:
    yield Row(1)


@stream
async def ticking_async(inputs: Inputs, context: ReadContext) -> AsyncStream[Row]:
    yield Row(1)


@stream
async def generator(inputs: Inputs) -> AsyncIterator[Row]:
    yield Row(1)


@command
def cancel(order_id: int) -> bool:
    return True


@command(insert="orders", update="orders")
def upsert(id: int) -> bool:
    return True


@command
def reads_a_table(rows: TableRef[Row]) -> int:
    return len(rows.columns)


@query
def lookup(name: str) -> list[Row]:
    return []


@columns
def scores(a: Batch[float]) -> Results[Row]:
    return []


@columns
def eight(
    a: list[int],
    b: list[int],
    c: list[int],
    d: list[int],
    e: list[int],
    f: list[int],
    g: list[int],
    h: list[int],
) -> list[Row]:
    return []


@columns
async def awaited_columns(a: list[str]) -> list[Row]:
    return []


@change(table="orders", key=("id",), removed="gone", cursor="seq")
def refresh(since: str | None) -> list[Row]:
    return []


@on_start
def start(values: Mapping[str, Any]) -> None:
    pass


@on_start
def start_bare() -> None:
    pass


@on_stop
def stop() -> None:
    pass


@health
def status() -> Literal["ok", "degraded"]:
    return "ok"


@health
async def status_async() -> str:
    return "ok"


@requires("API_KEY", secret=True)
@requires("REGION")
@table
def secured(inputs: Inputs) -> list[Row]:
    return []


called: Rows[Row] = inputs_only(Inputs())
