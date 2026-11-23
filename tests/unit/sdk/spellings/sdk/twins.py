# Copyright (c) 2026 Kenneth Stott
# Canary: a2416ffa-3386-44c1-ac00-62b21cb86c43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SDK spelling of every decorator: each function's twin is in ../doc/twins.py."""

import dataclasses

from provisa_sdk import (
    AsyncStream,
    Batch,
    BigInt,
    Boolean,
    Date,
    Double,
    Json,
    Numeric,
    Results,
    Rows,
    Stream,
    Text,
    Timestamp,
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
from provisa_sdk import table as t
from provisa_services import ReadContext


@dataclasses.dataclass
class Inputs:
    region: Text = "EU"


@dataclasses.dataclass
class Row:
    id: BigInt
    name: Text
    amount: Numeric
    at: Timestamp
    day: Date
    ok: Boolean
    ratio: Double
    extra: Json


@dataclasses.dataclass
class Scored:
    score: Double


@table
def plain() -> list[Row]:
    """Every row."""
    return []


@table()
def called(inputs: Inputs) -> Rows[Row]:
    """Rows of a region."""
    return []


@table
def with_context(inputs: Inputs, context: ReadContext) -> Rows[Row]:
    return []


@table
async def context_only(context: ReadContext) -> list[Row]:
    return []


@table(loader=True)
def loaded(inputs: Inputs, context: ReadContext) -> list[Row]:
    """Batched by key."""
    return []


@table(loader=100)
def loaded_n(context: ReadContext) -> list[Row]:
    return []


@stream
def ticks(inputs: Inputs) -> Stream[Row]:
    yield from ()


@stream
async def async_ticks(inputs: Inputs, context: ReadContext) -> AsyncStream[Row]:
    """Live rows."""
    for row in ():
        yield row


@command
def cancel(order_id: BigInt) -> Boolean:
    """Cancel one order."""
    return True


@command(insert="orders")
def add_order(id: BigInt, name: Text) -> Boolean:
    return True


@command(update="orders")
def edit_order(id: BigInt, name: Text) -> Boolean:
    return True


@command(delete="orders")
def drop_order(id: BigInt) -> Boolean:
    return True


@query
def lookup(name: Text) -> list[Row]:
    return []


@columns
def scores(amount: Batch[Double], ratio: Batch[Double]) -> Results[Scored]:
    """One score per row."""
    return []


@change(table="orders", key=["id", "name"], removed="ok", cursor="at")
def refresh(since: Text | None) -> list[Row]:
    return []


@on_start
def start(values: dict[str, Json]) -> None:
    pass


@on_stop
def stop() -> None:
    pass


@health
def status() -> Text:
    return "ok"


@requires("API_KEY", secret=True)
@requires("REGION")
@table
def secured(inputs: Inputs) -> list[Row]:
    """Needs setup values."""
    return []


@table
def twice() -> list[Row]:
    """Tagged in both spellings.

    @table
    """
    return []


@table
def undocumented() -> list[Row]:
    return []


@table
def paragraphs() -> list[Row]:
    """First line.

    More text, indented
    as a docstring is.
    """
    return []


@t
def aliased() -> list[Row]:
    return []
