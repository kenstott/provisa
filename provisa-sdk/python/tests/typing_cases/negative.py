# Copyright (c) 2026 Kenneth Stott
# Canary: df69caae-c440-4a3e-b3ec-a9ccf1f1fdcd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Mistakes the SDK makes type errors: pyright reports an error on each line carrying an
error comment and on no other line (tests/test_typing.py)."""

import dataclasses
from collections.abc import Iterator

from provisa_sdk import (
    change,
    columns,
    command,
    health,
    on_start,
    on_stop,
    requires,
    stream,
    table,
)
from provisa_services import ReadContext


@dataclasses.dataclass
class Inputs:
    region: str = "EU"


@dataclasses.dataclass
class Row:
    id: int


@table  # error: a table returning one object
def one_row() -> Row:
    return Row(1)


@table  # error: three parameters
def three(inputs: Inputs, context: ReadContext, more: int) -> list[Row]:
    return []


@table  # error: the context first
def context_first(context: ReadContext, inputs: Inputs) -> list[Row]:
    return []


@table(loader=True)  # error: a loader with no read context
def loader_without_context(inputs: Inputs) -> list[Row]:
    return []


@table(loader="100")  # error: a loader size that is not a number
def loader_text(context: ReadContext) -> list[Row]:
    return []


@stream  # error: a stream returning a list
def not_a_generator(inputs: Inputs) -> list[Row]:
    return []


@command(inserts="orders")  # error: a misspelt binding
def misspelt(id: int) -> bool:
    return True


@command(insert=1)  # error: a table named by a number
def numbered(id: int) -> bool:
    return True


@columns  # error: a column parameter that is not a list
def not_a_list(a: int) -> list[Row]:
    return []


@columns  # error: a columns result that is not a list
def not_a_list_result(a: list[int]) -> Iterator[Row]:
    yield Row(1)


@on_start  # error: on-start taking two parameters
def start(a: dict[str, str], b: int) -> None:
    pass


@on_stop  # error: on-stop taking a parameter
def stop(a: int) -> None:
    pass


@health  # error: health returning a number
def status() -> int:
    return 1


@requires("API_KEY", secret="yes")  # error: secret not a bool
@table
def secret_text() -> list[Row]:
    return []


@change(table="orders", key="id", removed="gone", cursor="seq")  # error: key one string
def refresh(since: str | None) -> list[Row]:
    return []


@table
def reads_context(context: ReadContext) -> list[Row]:
    return [Row(len(context.filter))]  # error: a ReadContext field misspelt
