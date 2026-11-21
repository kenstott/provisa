# Copyright (c) 2026 Kenneth Stott
# Canary: 17d5d651-53f5-4ade-948c-f27cadb73ec6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one statement the table viewer reads a table with, written by the server from what the
viewer chose: its column filters, sorts, grouping and page.

The viewer of a table in service writes this statement itself and sends it as any statement is
sent. A WITHDRAWN table is read by no statement a caller sends (admin/table_read.py), so its viewer sends
the choices and the server writes the statement — over that one table, naming only columns the
table is registered with, with every value bound. Nothing a caller sends is written into the
text: a column is checked against the registration and quoted, an operator is one of a fixed
set, a sort direction is ``asc`` or ``desc``, a page and its size are integers.

The predicates mean what the viewer's own mean (provisa-ui/src/components/columnFilterSql.ts,
REQ-1937).
"""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field

from provisa.api.errors import ApiError

#: The checklist's stand-in for a null or blank cell (columnFilter.ts EMPTY_VALUE).
EMPTY_VALUE = "\u0000empty"

_TEXT_OPS = {"contains", "equals", "startsWith", "endsWith", "notContains"}
_NUMBER_OPS = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_DATE_OPS = {
    "before",
    "after",
    "between",
    "lastDays",
    "today",
    "thisWeek",
    "thisMonth",
    "lastMonth",
    "thisYear",
}
_ANY_KIND_OPS = {"isEmpty", "notEmpty", "in"}
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
_NUMBER = re.compile(r"^\s*-?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*$")

#: The most rows one page may ask for: the viewer's export chunk.
MAX_PAGE_SIZE = 5000


class ViewerFilter(BaseModel):
    """One column's filter, as the viewer holds it. ``a``/``b`` are operands as typed and
    ``values`` the ticked checklist; a date filter carries the bounds it resolved to (``from_`` /
    ``to``, wall-clock timestamps) since "today" is the viewer's today."""

    col: str
    kind: Literal["text", "number", "date"]
    op: str
    a: str | None = None
    b: str | None = None
    values: list[str] | None = None
    from_: str | None = Field(default=None, alias="from")
    to: str | None = None

    model_config = {"populate_by_name": True}


class ViewerSort(BaseModel):
    col: str
    dir: Literal["asc", "desc"] = "asc"


class ViewerParam(BaseModel):
    """A value for a parameter column of the table (a source that cannot be read without one)."""

    column: str
    value: str | int | float


class ViewerRequest(BaseModel):
    params: list[ViewerParam] = []
    filters: list[ViewerFilter] = []
    sorts: list[ViewerSort] = []
    group_by: list[str] = Field(default=[], alias="groupBy")
    page: int = Field(default=0, ge=0)
    page_size: int = Field(default=50, ge=1, le=MAX_PAGE_SIZE, alias="pageSize")

    model_config = {"populate_by_name": True}


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _refused(code: str, message: str, **params: Any) -> ApiError:
    return ApiError(400, code, message, **params)


class _Statement:
    def __init__(self, table: dict) -> None:
        self.table = table
        self.params: list[str | int | float] = []
        # The names a statement may use for the table's columns: each one's alias, else its name.
        self.columns = {c.get("alias") or c["column_name"] for c in table["columns"]}

    def bind(self, value: str | int | float) -> str:
        self.params.append(value)
        return f"${len(self.params)}"

    def column(self, name: str) -> str:
        """``name`` quoted, once it is one of the table's registered columns."""
        if name not in self.columns:
            raise _refused(
                "data.viewer_column_unknown",
                f"{name!r} is not a column of table {self.table['table_name']!r}",
                column=name,
                table=self.table["table_name"],
            )
        return _quoted(name)


def _number(text: str | None, column: str) -> int | float:
    if text is None or not _NUMBER.match(text):
        raise _refused(
            "data.viewer_value_invalid",
            f"the filter on {column!r} needs a number, not {text!r}",
            column=column,
            value=str(text),
        )
    value = float(text)
    return (
        int(value) if value.is_integer() and "." not in text and "e" not in text.lower() else value
    )


def _in_list(c: str, f: ViewerFilter, st: _Statement) -> str:
    insensitive = f.a == "ci"
    chosen = f.values or []
    values = [v for v in chosen if v != EMPTY_VALUE]
    parts: list[str] = []
    if values:
        listed = ", ".join(
            f"CAST({st.bind(v.lower() if insensitive else v)} AS VARCHAR)" for v in values
        )
        left = f"LOWER(CAST({c} AS VARCHAR))" if insensitive else f"CAST({c} AS VARCHAR)"
        parts.append(f"{left} IN ({listed})")
    if EMPTY_VALUE in chosen:
        parts += [f"{c} IS NULL", f"CAST({c} AS VARCHAR) = ''"]
    return parts[0] if len(parts) == 1 else f"({' OR '.join(parts)})"


def _unknown_operator(f: ViewerFilter) -> ApiError:
    return _refused(
        "data.viewer_filter_unknown",
        f"{f.op!r} is not a filter of a {f.kind} column ({f.col!r})",
        operator=f.op,
        column=f.col,
    )


def _predicate(f: ViewerFilter, st: _Statement) -> str:  # noqa: PLR0911, PLR0912 -- one branch per operator, as the viewer's own
    c = st.column(f.col)
    if f.op == "isEmpty":
        return f"({c} IS NULL OR CAST({c} AS VARCHAR) = '')" if f.kind == "text" else f"{c} IS NULL"
    if f.op == "notEmpty":
        return (
            f"({c} IS NOT NULL AND CAST({c} AS VARCHAR) <> '')"
            if f.kind == "text"
            else f"{c} IS NOT NULL"
        )
    if f.op == "in":
        if not f.values:
            raise _unknown_operator(f)
        return _in_list(c, f, st)
    if f.kind == "number":
        if f.op == "between":
            low, high = sorted([_number(f.a, f.col), _number(f.b, f.col)])
            return f"{c} BETWEEN {st.bind(low)} AND {st.bind(high)}"
        if f.op not in _NUMBER_OPS:
            raise _unknown_operator(f)
        return f"{c} {_NUMBER_OPS[f.op]} {st.bind(_number(f.a, f.col))}"
    if f.kind == "date":
        if f.op not in _DATE_OPS:
            raise _unknown_operator(f)
        bounds = [(">=", f.from_), ("<", f.to)]
        given = [(op, at) for op, at in bounds if at is not None]
        if not given or any(not _TIMESTAMP.match(at) for _op, at in given):
            raise _refused(
                "data.viewer_value_invalid",
                f"the filter on {f.col!r} needs its bounds as timestamps",
                column=f.col,
                value=" .. ".join(str(at) for _op, at in bounds),
            )
        parts = [
            f"CAST({c} AS TIMESTAMP) {op} CAST({st.bind(at)} AS TIMESTAMP)" for op, at in given
        ]
        return f"({' AND '.join(parts)})"
    if f.op not in _TEXT_OPS or not f.a:
        raise _unknown_operator(f)
    text = f"LOWER(CAST({c} AS VARCHAR))"

    def operand() -> str:
        return f"CAST({st.bind((f.a or '').lower())} AS VARCHAR)"

    if f.op == "contains":
        return f"STRPOS({text}, {operand()}) > 0"
    if f.op == "notContains":
        return f"({c} IS NULL OR STRPOS({text}, {operand()}) = 0)"
    if f.op == "equals":
        return f"{text} = {operand()}"
    x = operand()
    if f.op == "startsWith":
        return f"SUBSTR({text}, 1, LENGTH({x})) = {x}"
    return (
        f"(LENGTH({text}) >= LENGTH({x}) AND "
        f"SUBSTR({text}, LENGTH({text}) - LENGTH({x}) + 1) = {x})"
    )


def viewer_statement(
    table: dict, relation: str, request: ViewerRequest
) -> tuple[str, list[str | int | float]]:
    """``(sql, params)``: one page of ``table`` (named ``relation`` in a statement) under the
    viewer's choices. One row more than the page is asked for: it says a next page exists.

    Group columns lead the ORDER BY, so a group stays together across pages; the primary key is
    appended as a tiebreaker so paging is stable under a chosen order. With no sort and no
    grouping there is no ORDER BY at all."""
    st = _Statement(table)
    predicates = [f"{st.column(p.column)} = {st.bind(p.value)}" for p in request.params]
    predicates += [_predicate(f, st) for f in request.filters]
    where = f" WHERE {' AND '.join(predicates)}" if predicates else ""

    ordered: list[str] = []
    order: list[str] = []
    for name in request.group_by:
        order.append(f"{st.column(name)} ASC")
        ordered.append(name)
    for sort in request.sorts:
        if sort.col not in ordered:
            order.append(f"{st.column(sort.col)} {'DESC' if sort.dir == 'desc' else 'ASC'}")
            ordered.append(sort.col)
    if order:
        for column in table["columns"]:
            name = column.get("alias") or column["column_name"]
            if column.get("is_primary_key") and name not in ordered:
                order.append(f"{_quoted(name)} ASC")
    order_by = f" ORDER BY {', '.join(order)}" if order else ""

    limit, offset = request.page_size + 1, request.page * request.page_size
    return f"SELECT * FROM {relation}{where}{order_by} LIMIT {limit} OFFSET {offset}", st.params
