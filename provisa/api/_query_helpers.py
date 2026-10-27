# Copyright (c) 2026 Kenneth Stott
# Canary: 9f58cd0e-b512-4600-a8b5-484b7ed78880
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

# Requirements: REQ-256, REQ-1986

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from graphql import (
    GraphQLArgument,
    GraphQLBoolean,
    GraphQLEnumType,
    GraphQLError,
    GraphQLField,
    GraphQLFloat,
    GraphQLInputObjectType,
    GraphQLInputType,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLSchema,
)
from graphql.utilities import coerce_input_value

from provisa.compiler.type_map import BigInt
from provisa.core.parameters import PARAMETER_EXTENSION

log = logging.getLogger(__name__)


def get_scalar_fields(schema: GraphQLSchema, table: str) -> list[str]:  # REQ-256
    """Get only scalar (non-object) field names for a root query type."""
    query_type = schema.query_type
    if query_type is None:
        return []
    field_map = query_type.fields
    if table not in field_map:
        return []
    gql_field = field_map[table]
    return_type = gql_field.type
    while hasattr(return_type, "of_type"):
        return_type = return_type.of_type
    if not isinstance(return_type, GraphQLObjectType):
        return []
    scalars = []
    for name, f in return_type.fields.items():
        inner = f.type
        while isinstance(inner, (GraphQLNonNull, GraphQLList)):
            inner = inner.of_type
        if isinstance(inner, (GraphQLScalarType, GraphQLEnumType)):
            scalars.append(name)
    return scalars


class RequestRefused(ValueError):
    """A request's filter or parameters the field does not take, or a value its type refuses;
    answered with a 400."""


def native_arguments(root: GraphQLField) -> dict[str, tuple[str, GraphQLArgument]]:  # REQ-1986
    """A root field's parameters, by parameter name: the argument each fills, and its name (which
    is ``_<name>`` when the field has a ``<name>`` of its own)."""
    return {
        arg.extensions[PARAMETER_EXTENSION]: (name, arg)
        for name, arg in root.args.items()
        if PARAMETER_EXTENSION in (arg.extensions or {})
    }


@dataclass(frozen=True)
class RequestVariables:  # REQ-1986
    """A request's filter and parameters as GraphQL variables: the text names them, the values
    travel beside it, so no value is ever written into the text."""

    declarations: tuple[str, ...] = ()
    arguments: tuple[str, ...] = ()
    values: dict[str, Any] = field(default_factory=dict)

    def operation(self, selection: str) -> str:
        """``selection`` (``{ field(...) { ... } }``) as an operation declaring the variables."""
        if not self.declarations:
            return selection
        return f"query({', '.join(self.declarations)}) {selection}"


NO_VARIABLES = RequestVariables()

_SCALAR_READERS: dict[str, Any] = {
    GraphQLInt.name: int,
    BigInt.name: int,
    GraphQLFloat.name: float,
    GraphQLBoolean.name: {"true": True, "false": False}.__getitem__,
}


def _typed(value: Any, type_: GraphQLInputType) -> Any:
    """A filter value read from the request's text, as the type it is compared as. A value that
    does not read as that type is left as given, for the type's coercion to refuse naming it."""
    nullable = type_.of_type if isinstance(type_, GraphQLNonNull) else type_
    if isinstance(nullable, GraphQLList):
        if isinstance(value, list):
            return [_typed(v, nullable.of_type) for v in value]
        return value
    if isinstance(nullable, GraphQLInputObjectType):
        if not isinstance(value, dict):
            return value
        out = {}
        for key, item in value.items():
            input_field = nullable.fields.get(key)
            out[key] = item if input_field is None else _typed(item, input_field.type)
        return out
    reader = _SCALAR_READERS.get(getattr(nullable, "name", ""))
    if reader is None or not isinstance(value, str):
        return value
    try:
        return reader(value)
    except (KeyError, ValueError):
        return value


def _coerced(value: Any, type_: GraphQLInputType, what: str) -> Any:
    try:
        return coerce_input_value(value, type_)
    except GraphQLError as err:
        raise RequestRefused(f"{what}: {err.message}") from err


def request_variables(  # REQ-1986
    schema: GraphQLSchema,
    field_name: str,
    where: dict[str, dict[str, Any]],
    params: dict[str, Any] | None = None,
    travel: dict[str, str] | None = None,
) -> RequestVariables:
    """The request's filter (``where``), parameters (``params``, by argument name) and time travel
    (``travel``: ``as_of`` / ``as_recorded``, REQ-1163) as variables of root field ``field_name``,
    each declared with the type of the argument it fills.

    A parameter must be a native argument of the field; any other name is refused naming it. A
    time the field's relation does not travel by is refused naming the field."""
    query_type = schema.query_type
    root = query_type.fields.get(field_name) if query_type is not None else None
    if root is None:
        raise RequestRefused(f"no field {field_name!r}")
    declarations: list[str] = []
    arguments: list[str] = []
    values: dict[str, Any] = {}
    if where:
        where_arg = root.args.get("where")
        if where_arg is None:
            raise RequestRefused(f"{field_name} takes no filter")
        values["where"] = _coerced(_typed(where, where_arg.type), where_arg.type, "filter")
        declarations.append(f"$where: {where_arg.type}")
        arguments.append("where: $where")
    parameters = native_arguments(root)
    for name, value in (params or {}).items():
        if name not in parameters:
            raise RequestRefused(f"{field_name} takes no parameter {name!r}")
        arg_name, arg = parameters[name]
        values[arg_name] = _coerced(value, arg.type, f"parameter {name!r}")
        declarations.append(f"${arg_name}: {arg.type}")
        arguments.append(f"{arg_name}: ${arg_name}")
    for name, value in (travel or {}).items():
        travel_arg = root.args.get(name)
        if travel_arg is None:
            raise RequestRefused(
                f"{field_name} takes no {name}: as_of and as_recorded apply to bitemporal tables "
                "and views, and as_recorded to Iceberg and Delta tables"
            )
        from provisa.mv.bitemporal import parse_as_of, parse_as_recorded

        try:
            (parse_as_of if name == "as_of" else parse_as_recorded)(value)
        except ValueError as err:
            raise RequestRefused(f"invalid {name}: {err}") from err
        values[name] = value
        declarations.append(f"${name}: {travel_arg.type}")
        arguments.append(f"{name}: ${name}")
    return RequestVariables(tuple(declarations), tuple(arguments), values)


def parse_params(raw: str | None) -> dict[str, Any]:  # REQ-1986
    """The ``params`` query parameter: a JSON object of parameter values by parameter name."""
    if raw is None or not raw.strip():
        return {}
    try:
        params = json.loads(raw)
    except json.JSONDecodeError as err:
        raise RequestRefused(f"params is not JSON: {err.msg}") from err
    if not isinstance(params, dict):
        raise RequestRefused("params must be a JSON object of parameter values by name")
    return params


def build_graphql_query(  # REQ-256, REQ-1986
    table: str,
    fields: list[str],
    variables: RequestVariables,
    order_by: list[dict[str, str]],
    limit: int | None,
    offset: int | None,
) -> str:
    """Build a GraphQL query string from table/field parameters; the filter and parameters are
    the ``variables`` it names."""
    args_parts = []

    if limit is not None:
        args_parts.append(f"limit: {limit}")
    if offset:
        args_parts.append(f"offset: {offset}")

    args_parts.extend(variables.arguments)

    if order_by:
        ob_parts = [f"{o['field']}: {o['dir']}" for o in order_by]
        args_parts.append("order_by: {" + ", ".join(ob_parts) + "}")

    args_str = f"({', '.join(args_parts)})" if args_parts else ""
    fields_str = " ".join(fields)
    return variables.operation(f"{{ {table}{args_str} {{ {fields_str} }} }}")
