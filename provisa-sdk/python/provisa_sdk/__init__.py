# Copyright (c) 2026 Kenneth Stott
# Canary: 25962a7d-ec82-4b5f-ac33-1ecdce752f0c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's optional SDK for Python code sources (REQ-1984, REQ-1993).

Each decorator is a docstring tag, spelled so a type checker can hold the function to its tag's
rules. It writes its tag line into the function's docstring and returns the same function, so
Provisa reads a decorated function exactly as it reads one whose docstring carries the tag:

    from provisa_sdk import table, Rows
    from provisa_services import ReadContext

    @table
    def orders(inputs: OrderInputs, context: ReadContext) -> Rows[Order]:
        '''Open orders.'''

is the same function to Provisa as

    def orders(inputs: OrderInputs, context: ReadContext) -> Iterable[Order]:
        '''Open orders.

        @table
        '''

The SDK does nothing when the function is called, and nothing is served because it is imported:
Provisa still reads the code. ``ReadContext``, ``Filter``, ``Order``, ``TableRef`` and
``TableConnection`` stay ``provisa_services``'s; this package carries their type stubs only.
"""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import datetime as _datetime
import decimal as _decimal
import inspect as _inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypeVar, overload

if TYPE_CHECKING:
    from provisa_services import ReadContext

    _RowsOut: TypeAlias = Iterable[Any] | AsyncIterable[Any]
    _ReadOut: TypeAlias = _RowsOut | Awaitable[_RowsOut]
    #: ``(inputs, context)``, either left out (REQ-2100 READ CONTEXT).
    _ReadFn: TypeAlias = (
        Callable[[], _ReadOut] | Callable[[Any], _ReadOut] | Callable[[Any, ReadContext], _ReadOut]
    )
    #: A ``@loader`` is given the join's keys in its read context, so it takes one.
    _LoaderFn: TypeAlias = (
        Callable[[ReadContext], _ReadOut] | Callable[[Any, ReadContext], _ReadOut]
    )
    _StreamOut: TypeAlias = Iterator[Any] | AsyncIterator[Any]
    _StreamFn: TypeAlias = (
        Callable[[], _StreamOut]
        | Callable[[Any], _StreamOut]
        | Callable[[Any, ReadContext], _StreamOut]
    )
    _ColumnsOut: TypeAlias = list[Any] | Awaitable[list[Any]]
    _L: TypeAlias = list[Any]
    #: Each parameter one input column, a list of the batch's values.
    _ColumnsFn: TypeAlias = (
        Callable[[_L], _ColumnsOut]
        | Callable[[_L, _L], _ColumnsOut]
        | Callable[[_L, _L, _L], _ColumnsOut]
        | Callable[[_L, _L, _L, _L], _ColumnsOut]
        | Callable[[_L, _L, _L, _L, _L], _ColumnsOut]
        | Callable[[_L, _L, _L, _L, _L, _L], _ColumnsOut]
        | Callable[[_L, _L, _L, _L, _L, _L, _L], _ColumnsOut]
        | Callable[[_L, _L, _L, _L, _L, _L, _L, _L], _ColumnsOut]
    )
    #: Nothing, or the setup values as one mapping of name to value.
    _OnStartFn: TypeAlias = Callable[[], object] | Callable[[dict[str, Any]], object]
    _OnStopFn: TypeAlias = Callable[[], object]
    _HealthFn: TypeAlias = Callable[[], str] | Callable[[], Awaitable[str]]

__all__ = [
    "SPELLING",
    "AsyncRows",
    "AsyncStream",
    "Batch",
    "BigInt",
    "Boolean",
    "Date",
    "Double",
    "Json",
    "Numeric",
    "Results",
    "Rows",
    "Stream",
    "Text",
    "Timestamp",
    "change",
    "columns",
    "command",
    "health",
    "on_start",
    "on_stop",
    "query",
    "requires",
    "stream",
    "table",
]

#: The forms this SDK spells. It changes only when a tag or form is added.
SPELLING = 1

R = TypeVar("R")
T = TypeVar("T")

# -- result shapes: the hints written out ---------------------------------------------------------
# Old-style generic aliases: a PEP 695 ``type`` alias reaches the reader as a TypeAliasType,
# which it does not read.

#: A ``@table``'s rows.
Rows: TypeAlias = Iterable[R]
#: A ``@table``'s rows, produced asynchronously.
AsyncRows: TypeAlias = AsyncIterable[R]
#: A ``@stream``'s rows, each delivered as it is yielded.
Stream: TypeAlias = Iterator[R]
#: A ``@stream``'s rows, yielded asynchronously.
AsyncStream: TypeAlias = AsyncIterator[R]
#: A ``@columns`` parameter: one input column's values for the batch.
Batch: TypeAlias = list[T]
#: A ``@columns`` result: one result row per input row.
Results: TypeAlias = list[R]

# -- column data types: aliases of the hints Provisa already maps ---------------------------------

Text: TypeAlias = str
BigInt: TypeAlias = int
Double: TypeAlias = float
Numeric: TypeAlias = _decimal.Decimal
Timestamp: TypeAlias = _datetime.datetime
Date: TypeAlias = _datetime.date
Boolean: TypeAlias = bool
Json: TypeAlias = Any

# -- the decorators -------------------------------------------------------------------------------

_F = TypeVar("_F", bound="_ReadFn")
_LF = TypeVar("_LF", bound="_LoaderFn")
_SF = TypeVar("_SF", bound="_StreamFn")
_CF = TypeVar("_CF", bound="_ColumnsFn")
_StartF = TypeVar("_StartF", bound="_OnStartFn")
_StopF = TypeVar("_StopF", bound="_OnStopFn")
_HealthF = TypeVar("_HealthF", bound="_HealthFn")
_AnyF = TypeVar("_AnyF", bound=Callable[..., Any])


def _mark(fn: _AnyF, *lines: str) -> _AnyF:
    """``fn`` itself, its docstring ending with each tag line it does not hold yet."""
    doc = _inspect.cleandoc(fn.__doc__) if fn.__doc__ else ""
    held = doc.splitlines()
    fn.__doc__ = "\n".join([doc, *(line for line in lines if line not in held)]).lstrip("\n")
    return fn


@overload
def table(fn: _F, /) -> _F: ...
@overload
def table(*, loader: None = None) -> Callable[[_F], _F]: ...
@overload
def table(*, loader: Literal[True] | int) -> Callable[[_LF], _LF]: ...
def table(fn: Any = None, /, *, loader: Literal[True] | int | None = None) -> Any:
    """``@table``: a table whose rows the function returns. ``loader=True`` adds ``@loader`` and
    ``loader=N`` adds ``@loader N``: a table given a join's keys in its read context, ``N`` at a
    time."""
    lines = ["@table"]
    if loader is True:
        lines.append("@loader")
    elif loader is not None:
        lines.append(f"@loader {loader}")
    if fn is None:
        return lambda f: _mark(f, *lines)
    return _mark(fn, *lines)


def stream(fn: _SF, /) -> _SF:
    """``@stream``: a live table, each row delivered as the generator yields it."""
    return _mark(fn, "@stream")


@overload
def command(fn: _AnyF, /) -> _AnyF: ...
@overload
def command(
    *, insert: str | None = None, update: str | None = None, delete: str | None = None
) -> Callable[[_AnyF], _AnyF]: ...
def command(
    fn: Any = None,
    /,
    *,
    insert: str | None = None,
    update: str | None = None,
    delete: str | None = None,
) -> Any:
    """``@command``: a mutation. ``insert="orders"`` (or ``update=``, ``delete=``) adds
    ``@insert orders``: the command a write to that table binds to."""
    lines = ["@command"]
    for operation, name in (("insert", insert), ("update", update), ("delete", delete)):
        if name is not None:
            lines.append(f"@{operation} {name}")
    if fn is None:
        return lambda f: _mark(f, *lines)
    return _mark(fn, *lines)


def query(fn: _AnyF, /) -> _AnyF:
    """``@query``: a command whose kind is query."""
    return _mark(fn, "@query")


def columns(fn: _CF, /) -> _CF:
    """``@columns``: a batch column transform; each parameter is one input column's values, and
    the result holds one row per input row."""
    return _mark(fn, "@columns")


def change(
    *, table: str, key: list[str] | tuple[str, ...], removed: str, cursor: str
) -> Callable[[_AnyF], _AnyF]:
    """``@change``: the function that keeps the landed table ``table`` fresh, given the cursor of
    its last run: ``key`` the columns identifying a match, ``removed`` the column marking one as
    removed, ``cursor`` the column carrying each change's cursor value."""
    lines = [
        f"@change table={table}",
        f"@change key={','.join(key)}",
        f"@change removed={removed}",
        f"@change cursor={cursor}",
    ]
    return lambda f: _mark(f, *lines)


def on_start(fn: _StartF, /) -> _StartF:
    """``@on-start``: called once when the code starts, with nothing or the setup values."""
    return _mark(fn, "@on-start")


def on_stop(fn: _StopF, /) -> _StopF:
    """``@on-stop``: called once when the code stops."""
    return _mark(fn, "@on-stop")


def health(fn: _HealthF, /) -> _HealthF:
    """``@health``: the code's status, as text."""
    return _mark(fn, "@health")


def requires(name: str, *, secret: bool = False) -> Callable[[_AnyF], _AnyF]:
    """``@requires NAME`` (``@requires NAME:secret`` with ``secret=True``): a value whoever sets
    the code up gives it. Stack one per value."""
    line = f"@requires {name}:secret" if secret else f"@requires {name}"
    return lambda f: _mark(f, line)
