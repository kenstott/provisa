# Copyright (c) 2026 Kenneth Stott
# Canary: 74369375-9e4d-4524-b28c-3a9fbd42cffc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The docstring spelling of every SDK decorator: each function's twin is in ../sdk/twins.py."""

import dataclasses
import datetime
import decimal
from collections.abc import AsyncIterator, Iterable, Iterator
from typing import Any

from provisa_services import ReadContext


@dataclasses.dataclass
class Inputs:
    region: str = "EU"


@dataclasses.dataclass
class Row:
    id: int
    name: str
    amount: decimal.Decimal
    at: datetime.datetime
    day: datetime.date
    ok: bool
    ratio: float
    extra: Any


@dataclasses.dataclass
class Scored:
    score: float


def plain() -> list[Row]:
    """Every row.

    @table
    """
    return []


def called(inputs: Inputs) -> Iterable[Row]:
    """Rows of a region.

    @table
    """
    return []


def with_context(inputs: Inputs, context: ReadContext) -> Iterable[Row]:
    """@table"""
    return []


async def context_only(context: ReadContext) -> list[Row]:
    """@table"""
    return []


def loaded(inputs: Inputs, context: ReadContext) -> list[Row]:
    """Batched by key.

    @table
    @loader
    """
    return []


def loaded_n(context: ReadContext) -> list[Row]:
    """@table
    @loader 100
    """
    return []


def ticks(inputs: Inputs) -> Iterator[Row]:
    """@stream"""
    yield from ()


async def async_ticks(inputs: Inputs, context: ReadContext) -> AsyncIterator[Row]:
    """Live rows.

    @stream
    """
    for row in ():
        yield row


def cancel(order_id: int) -> bool:
    """Cancel one order.

    @command
    """
    return True


def add_order(id: int, name: str) -> bool:
    """@command
    @insert orders
    """
    return True


def edit_order(id: int, name: str) -> bool:
    """@command
    @update orders
    """
    return True


def drop_order(id: int) -> bool:
    """@command
    @delete orders
    """
    return True


def lookup(name: str) -> list[Row]:
    """@query"""
    return []


def scores(amount: list[float], ratio: list[float]) -> list[Scored]:
    """One score per row.

    @columns
    """
    return []


def refresh(since: str | None) -> list[Row]:
    """@change table=orders
    @change key=id,name
    @change removed=ok
    @change cursor=at
    """
    return []


def start(values: dict[str, Any]) -> None:
    """@on-start"""


def stop() -> None:
    """@on-stop"""


def status() -> str:
    """@health"""
    return "ok"


def secured(inputs: Inputs) -> list[Row]:
    """Needs setup values.

    @table
    @requires REGION
    @requires API_KEY:secret
    """
    return []


def twice() -> list[Row]:
    """Tagged in both spellings.

    @table
    """
    return []


def undocumented() -> list[Row]:
    """@table"""
    return []


def paragraphs() -> list[Row]:
    """First line.

    More text, indented
    as a docstring is.

    @table
    """
    return []


def aliased() -> list[Row]:
    """@table"""
    return []
