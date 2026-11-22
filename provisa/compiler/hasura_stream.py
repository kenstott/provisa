# Copyright (c) 2026 Kenneth Stott
# Canary: b4e17c92-3a5d-4f08-9c6e-7d2a1f5b8e43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hasura's streaming subscription of a Hasura-preset table (REQ-194, maintainer 2026-11-22).

``<table>_stream(batch_size, cursor, where)`` streams the table's rows past a cursor, in cursor
order, at most ``batch_size`` at a time; after each batch the cursor moves to the last row sent.
Each batch is the table's own read, ``<table>(where: {_and: [where, {<col>: {_gt: cursor}}]},
order_by: [{<col>: asc}], limit: batch_size)``, answered through the one query pipeline, so row
rules, masks and visibility apply to it exactly as to a query. ``DESC`` reads ``_lt`` and
``desc``. As Hasura does, the stream takes one cursor on one column, and a cursor without an
ordering is ascending.
"""

# Requirements: REQ-194

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable

from graphql import (
    ArgumentNode,
    EnumValueNode,
    FieldNode,
    GraphQLEnumType,
    GraphQLInputObjectType,
    GraphQLScalarType,
    GraphQLSchema,
    IntValueNode,
    ListValueNode,
    NameNode,
    ObjectFieldNode,
    ObjectValueNode,
    OperationDefinitionNode,
    SelectionSetNode,
    ValueNode,
    ast_from_value,
    get_named_type,
    print_ast,
)
from graphql.execution.values import get_argument_values, get_variable_values

from provisa.compiler.hasura_dialect import table_hasura
from provisa.compiler.naming import hasura_argument_name, hasura_enum_value, hasura_operator
from provisa.compiler.sql_types import TableMeta

#: The response key the cursor column is read back under, beside the client's selection, and
#: removed from each row before the row is sent.
CURSOR_KEY = "provisa_stream_cursor"


class StreamRefused(ValueError):
    """A stream request Hasura's streaming subscription does not take, refused by name."""


@dataclass(frozen=True)
class StreamRequest:
    """One ``<table>_stream`` field, its arguments read."""

    table: TableMeta
    convention: str
    response_key: str
    batch_size: int
    column: str
    ascending: bool
    initial: Any
    where: Any
    where_type: GraphQLInputObjectType
    column_type: GraphQLScalarType | GraphQLEnumType
    selection: SelectionSetNode


def stream_request(
    schema: GraphQLSchema,
    operation: OperationDefinitionNode,
    field: FieldNode,
    table: TableMeta,
    variables: dict | None,
) -> StreamRequest:
    """``field``, a ``<table>_stream`` field of ``operation``, read against ``schema`` with the
    request's ``variables``."""
    conv = table_hasura(table)
    if conv is None:
        raise StreamRefused(f"{field.name.value!r} is not a Hasura-preset table's stream")
    sub = schema.subscription_type
    assert sub is not None and field.name.value in sub.fields  # the document was validated
    field_def = sub.fields[field.name.value]
    coerced = get_variable_values(schema, operation.variable_definitions or (), variables or {})
    if isinstance(coerced, list):
        raise StreamRefused("; ".join(e.message for e in coerced))
    args = get_argument_values(field_def, field, coerced)

    def arg(name: str) -> str:
        return hasura_argument_name(conv, name)

    batch_size = args[arg("batch_size")]
    if batch_size < 1:
        raise StreamRefused(f"{arg('batch_size')} must be at least 1, got {batch_size}")
    cursors = [c for c in args[arg("cursor")] if c is not None]
    if len(cursors) != 1:
        raise StreamRefused(f"a stream takes exactly one {arg('cursor')}, got {len(cursors)}")
    (cursor,) = cursors
    initial = cursor[arg("initial_value")]
    if len(initial) != 1:
        raise StreamRefused(
            f"a stream cursor's {arg('initial_value')} names exactly one column, got "
            f"{sorted(initial)}"
        )
    ((column, value),) = initial.items()
    # Hasura's cursor_ordering is ascending when the cursor names none (REQ-194 serves Hasura's
    # streaming subscription as Hasura does).
    ascending = cursor.get(arg("ordering"), "ASC") == "ASC"
    where_type = _where_type(schema, table)
    _check_comparable(where_type, conv, column, ascending)
    value_type = get_named_type(field_def.args[arg("cursor")].type)
    assert isinstance(value_type, GraphQLInputObjectType)
    initial_type = get_named_type(value_type.fields[arg("initial_value")].type)
    assert isinstance(initial_type, GraphQLInputObjectType)
    column_type = get_named_type(initial_type.fields[column].type)
    assert isinstance(column_type, (GraphQLScalarType, GraphQLEnumType))
    if value is None:
        raise StreamRefused(f"the stream cursor on {column!r} starts at null")
    if field.selection_set is None or any(
        isinstance(s, FieldNode) and (s.alias or s.name).value == CURSOR_KEY
        for s in field.selection_set.selections
    ):
        raise StreamRefused(f"a stream's selection may not answer under {CURSOR_KEY!r}")
    return StreamRequest(
        table=table,
        convention=conv,
        response_key=(field.alias or field.name).value,
        batch_size=batch_size,
        column=column,
        ascending=ascending,
        initial=value,
        where=args.get(arg("where")),
        where_type=where_type,
        column_type=column_type,
        selection=field.selection_set,
    )


def _where_type(schema: GraphQLSchema, table: TableMeta) -> GraphQLInputObjectType:
    query = schema.query_type
    assert query is not None
    read = query.fields[table.field_name]
    where = read.args[hasura_argument_name(table_hasura(table) or "", "where")]
    named = get_named_type(where.type)
    assert isinstance(named, GraphQLInputObjectType)
    return named


def _check_comparable(
    where_type: GraphQLInputObjectType, conv: str, column: str, ascending: bool
) -> None:
    """The cursor column's comparison expression takes the operator a batch reads past it by."""
    op = hasura_operator(conv, "gt" if ascending else "lt")
    comparison = where_type.fields.get(column)
    if comparison is None or op not in get_named_type(comparison.type).fields:  # type: ignore[union-attr]
        raise StreamRefused(f"the stream cursor column {column!r} cannot be compared by {op}")


def batch_query(req: StreamRequest, cursor: Any) -> str:
    """The query text of the batch past ``cursor`` (a value of the cursor column's type)."""
    conv = req.convention
    past = {req.column: {hasura_operator(conv, "gt" if req.ascending else "lt"): cursor}}
    condition = past if req.where is None else {"_and": [req.where, past]}
    where = _literal(condition, req.where_type)
    direction = hasura_enum_value(conv, "asc" if req.ascending else "desc", "order_by")
    order_by = ListValueNode(
        values=(
            ObjectValueNode(
                fields=(
                    ObjectFieldNode(
                        name=NameNode(value=req.column), value=EnumValueNode(value=direction)
                    ),
                )
            ),
        )
    )
    cursor_field = FieldNode(alias=NameNode(value=CURSOR_KEY), name=NameNode(value=req.column))
    read = FieldNode(
        alias=NameNode(value=req.response_key),
        name=NameNode(value=req.table.field_name),
        arguments=(
            ArgumentNode(name=NameNode(value=hasura_argument_name(conv, "where")), value=where),
            ArgumentNode(
                name=NameNode(value=hasura_argument_name(conv, "order_by")), value=order_by
            ),
            ArgumentNode(
                name=NameNode(value="limit"), value=IntValueNode(value=str(req.batch_size))
            ),
        ),
        selection_set=SelectionSetNode(selections=(*req.selection.selections, cursor_field)),
    )
    return "query " + print_ast(SelectionSetNode(selections=(read,)))


def _literal(value: Any, type_: GraphQLInputObjectType) -> ValueNode:
    node = ast_from_value(value, type_)
    assert node is not None  # a where condition of its own type always has a literal
    return node


async def stream_batches(
    req: StreamRequest,
    run: Callable[[str], Awaitable[dict]],
    idle: Callable[[], Awaitable[bool]],
) -> AsyncIterator[dict]:
    """Each batch's answer, as Hasura sends it (``{"data": {<key>: rows}}``), the cursor moving to
    each batch's last row. ``run`` answers a query text through the query pipeline; ``idle`` waits
    before reading again after a batch that was not full, and says whether to go on. A batch with
    no rows sends nothing. An answer with errors is sent and ends the stream."""
    cursor = req.initial
    while True:
        answer = await run(batch_query(req, cursor))
        if answer.get("errors"):
            yield answer
            return
        rows = answer["data"][req.response_key]
        if rows:
            cursor = _cursor_of(req, rows[-1])
            yield {"data": {req.response_key: [_sent(r) for r in rows]}}
        if len(rows) < req.batch_size and not await idle():
            return


def _cursor_of(req: StreamRequest, row: dict) -> Any:
    """The cursor column's value in ``row``, as the cursor column's type reads it."""
    value = row[CURSOR_KEY]
    if value is None:
        raise StreamRefused(f"the stream cursor column {req.column!r} is null in a row")
    return req.column_type.parse_value(value)


def _sent(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != CURSOR_KEY}
