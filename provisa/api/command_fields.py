# Copyright (c) 2026 Kenneth Stott
# Canary: 782510f0-e425-49c3-b5c0-68348208310a
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The commands a role's GraphQL schema offers, read for every other surface (REQ-1929).

A command is offered to a role exactly when the role's GraphQL schema has its field: a field on
Query is a query-kind command, a read; a field on Mutation is a write. REST, OpenAPI and JSON:API
take their command operations from these fields (:func:`command_fields`), never from the raw
command list, so no surface offers a command GraphQL does not.

A read over REST or JSON:API is lowered to the GraphQL command field (:func:`read_command`): the
field, its declared arguments and its where/order_by/limit/offset, executed by the GraphQL
endpoint's own action-field execution, so filtering, ordering, paging and governance are GraphQL's.
"""

# Requirements: REQ-1929

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from graphql import (
    GraphQLArgument,
    GraphQLBoolean,
    GraphQLField,
    GraphQLFloat,
    GraphQLID,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    parse,
    validate,
)
from graphql.execution.values import get_variable_values
from graphql.language import FieldNode, OperationDefinitionNode

from provisa.api.errors import ApiError
from provisa.core.command_bindings import binding_name

# A query-kind command field's paging and filtering arguments (actions_schema), which every
# surface's read maps its own parameters onto.
FILTER_ARGS = ("where", "order_by", "limit", "offset")

_TEXT_TYPES = (GraphQLString, GraphQLID)
_COLUMN_PARSERS = {GraphQLInt: int, GraphQLFloat: float}


@dataclass(frozen=True)
class CommandField:
    """One command field of a role's GraphQL schema."""

    field_name: str  # the root field, as GraphQL names it
    root: str  # "Query" (a read) or "Mutation" (a write)
    field: GraphQLField

    @property
    def definition(self) -> dict:
        return self.field.extensions["provisa_command_def"]

    @property
    def command(self) -> str:
        """The command's registered name, the one its admission and executor know."""
        return self.field.extensions["provisa_command"]

    @property
    def domain_id(self) -> str:
        return self.definition["domain_id"]

    @property
    def is_read(self) -> bool:
        return self.root == "Query"

    def name_in(self, language: str) -> str | None:
        """The command's name in ``language`` (REQ-2168), None when excluded from it."""
        return binding_name(self.definition, language)

    @property
    def arguments(self) -> dict[str, GraphQLArgument]:
        """The command's declared arguments, without a read's paging and filtering ones."""
        return {
            name: arg
            for name, arg in self.field.args.items()
            if not (self.is_read and name in FILTER_ARGS)
        }

    @property
    def row_type(self) -> GraphQLObjectType | None:
        """The object type of the rows the command returns, None for a scalar answer."""
        inner = self.field.type
        while isinstance(inner, (GraphQLNonNull, GraphQLList)):
            inner = inner.of_type
        return inner if isinstance(inner, GraphQLObjectType) else None


def command_fields(schema: GraphQLSchema) -> list[CommandField]:
    """Every command field of ``schema``: its query-kind commands, then its mutation-kind ones."""
    found: list[CommandField] = []
    for root, obj in (("Query", schema.query_type), ("Mutation", schema.mutation_type)):
        if obj is None:
            continue
        for name, field in obj.fields.items():
            if "provisa_command" in (field.extensions or {}):
                found.append(CommandField(name, root, field))
    return found


def command_at(
    schema: GraphQLSchema, domain_id: str, name: str, language: str
) -> CommandField | None:
    """The command field offered in ``domain_id`` under ``name`` in ``language``, or None."""
    for cf in command_fields(schema):
        if cf.domain_id == domain_id and cf.name_in(language) == name:
            return cf
    return None


def _refused(command: str, message: str) -> ApiError:
    return ApiError(
        400,
        "data.command_parameter_refused",
        f"{command}: {message}",
        command=command,
        reason=message,
    )


def _named(gql_type: Any) -> Any:
    while isinstance(gql_type, (GraphQLNonNull, GraphQLList)):
        gql_type = gql_type.of_type
    return gql_type


def argument_values(cf: CommandField, given: Mapping[str, str]) -> dict[str, Any]:
    """The declared arguments a URL's query string gives, each read as its GraphQL type reads it:
    text as written, any other type as the JSON literal it is written as."""
    values: dict[str, Any] = {}
    for name, arg in cf.arguments.items():
        if name not in given:
            continue
        text = given[name]
        if _named(arg.type) in _TEXT_TYPES and not isinstance(arg.type, GraphQLList):
            values[name] = text
            continue
        try:
            values[name] = json.loads(text)
        except json.JSONDecodeError as exc:
            raise _refused(
                cf.command, f"argument {name!r} is not a {arg.type} value: {exc}"
            ) from exc
    return values


def column_value(cf: CommandField, column: str, text: str) -> Any:
    """A filter value written as text, read as the type of the returned column it compares with.
    A column the rows do not have keeps the text; the filter refuses that column by name."""
    row_type = cf.row_type
    column_field = row_type.fields.get(column) if row_type is not None else None
    if column_field is None:
        return text
    named = _named(column_field.type)
    if named is GraphQLBoolean:
        if text not in ("true", "false"):
            raise _refused(cf.command, f"filter on {column!r} takes true or false, got {text!r}")
        return text == "true"
    parser = _COLUMN_PARSERS.get(named)
    if parser is None:
        return text
    try:
        return parser(text)
    except ValueError as exc:
        raise _refused(
            cf.command, f"filter on {column!r} takes a {named.name}, got {text!r}"
        ) from exc


def _document(cf: CommandField, inputs: dict[str, Any]) -> str:
    """The GraphQL operation calling ``cf`` with ``inputs``, each passed as a variable."""
    var_defs = ", ".join(f"${name}: {cf.field.args[name].type}" for name in inputs)
    call_args = ", ".join(f"{name}: ${name}" for name in inputs)
    row_type = cf.row_type
    selection = ""
    if row_type is not None:
        scalars = [
            n
            for n, f in row_type.fields.items()
            if not isinstance(_named(f.type), GraphQLObjectType)
        ]
        selection = " { " + " ".join(scalars) + " }"
    operation = "query" if cf.is_read else "mutation"
    head = f"{operation} Command({var_defs})" if var_defs else operation
    call = f"{cf.field_name}({call_args})" if call_args else cf.field_name
    return f"{head} {{ {call}{selection} }}"


async def read_command(  # REQ-1929
    state: Any,
    surface: Any,
    cf: CommandField,
    role_id: str,
    *,
    arguments: dict[str, Any],
    where: dict[str, dict[str, Any]],
    order_by: list[str],
    limit: int | None,
    offset: int | None,
) -> list[dict]:
    """A query-kind command read, lowered to its GraphQL field and executed as GraphQL executes
    it: the field validated against the role's schema, the call admitted and run by the one
    command executor, then the field's where/order_by/limit/offset applied to its rows."""
    from provisa.api.data.mutations import _execute_action_field

    if not cf.is_read:
        raise ValueError(f"command {cf.command!r} is a write, not a read")
    inputs: dict[str, Any] = dict(arguments)
    for name, value in (("where", where), ("order_by", order_by)):
        if value:
            inputs[name] = value
    for name, value in (("limit", limit), ("offset", offset)):
        if value is not None:
            inputs[name] = value
    document = parse(_document(cf, inputs))
    errors = validate(surface.schema, document)
    if errors:
        raise _refused(cf.command, "; ".join(e.message for e in errors))
    operation = document.definitions[0]
    assert isinstance(operation, OperationDefinitionNode)
    coerced = get_variable_values(surface.schema, operation.variable_definitions or (), inputs)
    if isinstance(coerced, list):
        raise _refused(cf.command, "; ".join(e.message for e in coerced))
    (field_node,) = operation.selection_set.selections
    assert isinstance(field_node, FieldNode)
    # The registered name: the executor's lookup and admission know every command by it.
    return await _execute_action_field(
        cf.command, field_node, state, coerced, ctx=surface.context, role_id=role_id
    )
