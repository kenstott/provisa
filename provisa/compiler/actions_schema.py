# Copyright (c) 2026 Kenneth Stott
# Canary: fe7dee37-1a51-4599-a719-d5e9249736c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL action-field builders (REQ-205–210, REQ-304, REQ-360–362).

Mutation-name derivation, JSON-Schema->GraphQL type conversion, and tracked
function/webhook action fields. Extracted from schema_gen.py; leaf module.
"""

from typing import cast

from graphql import (
    GraphQLArgument,
    GraphQLBoolean as _GraphQLBoolean,
    GraphQLField,
    GraphQLFloat as _GraphQLFloat,
    GraphQLInt as _GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString as _GraphQLString,
)

from provisa.compiler.naming import domain_to_sql_name, mutation_style
from provisa.compiler.type_map import JSONScalar
from provisa.compiler.schema_types import SchemaInput, _TableInfo  # noqa: F401  (annotations)

GraphQLString: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLString)
GraphQLInt: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLInt)
GraphQLBoolean: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLBoolean)
GraphQLFloat: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLFloat)

# A qualified "schema.table" return type has this many dot-separated parts.
_SCHEMA_TABLE_PARTS = 2

_ACTION_SCALAR_MAP: dict[str, GraphQLScalarType] = {
    "String": GraphQLString,
    "Int": GraphQLInt,
    "Float": GraphQLFloat,
    "Boolean": GraphQLBoolean,
    "DateTime": GraphQLString,
    "Date": GraphQLString,
    "BigInt": GraphQLString,
    "JSON": GraphQLString,
}


def _mutation_name(op: str, field_name: str, convention: str = "apollo_graphql") -> str:
    """Build mutation field name respecting the naming convention.

    apollo_graphql: 'insert' + 'orders' → 'insertOrders'
    snake/hasura_graphql: 'insert' + 'orders' → 'insert_orders'
    Domain-prefixed: 'insert' + 'sa__orders' → 'sa__insertOrders' or 'sa__insert_orders'
    """
    style = mutation_style(convention)
    if "__" in field_name:
        domain, table = field_name.split("__", 1)
        if style == "snake":
            return f"{domain}__{op}_{table}"
        return f"{domain}__{op}{table[0].upper()}{table[1:]}"
    if style == "snake":
        return f"{op}_{field_name}"
    return f"{op}{field_name[0].upper()}{field_name[1:]}"


def _json_schema_to_gql_type(schema: dict, type_name: str):
    """Convert a JSON Schema object/array definition to a GraphQL return type."""
    if not schema:
        return None
    top = schema.get("type", "object")
    if top == "array":
        props = (schema.get("items") or {}).get("properties") or {}
    else:
        props = schema.get("properties") or {}
    if not props:
        return None
    _JS_MAP = {
        "string": GraphQLString,
        "integer": GraphQLInt,
        "number": GraphQLFloat,
        "boolean": GraphQLBoolean,
    }
    gql_fields = {
        k: GraphQLField(
            _JS_MAP.get(
                (v.get("type") if isinstance(v, dict) else "string") or "string", GraphQLString
            )
        )
        for k, v in props.items()
    }
    obj = cast(GraphQLObjectType, GraphQLObjectType(type_name, lambda f=gql_fields: f))
    if top == "array":
        return GraphQLList(GraphQLNonNull(obj))
    return obj


def _build_action_fields(  # REQ-205, REQ-206, REQ-207, REQ-208, REQ-209, REQ-210, REQ-304, REQ-305, REQ-360, REQ-361, REQ-362
    si: SchemaInput,
    obj_types: dict[int, GraphQLObjectType],
    tables: list["_TableInfo"],
    domain_alias_map: dict[str, str] | None = None,
) -> tuple[dict[str, GraphQLField], dict[str, GraphQLField]]:
    """Build query/mutation fields for tracked functions and webhooks.

    Returns (extra_query_fields, extra_mutation_fields).
    """
    # Build a lookup: (schema_name, table_name) → GraphQL object type
    table_type_lookup: dict[tuple[str, str], GraphQLObjectType] = {
        (t.schema_name, t.table_name): obj_types[t.table_id]
        for t in tables
        if t.table_id in obj_types
    }

    extra_query: dict[str, GraphQLField] = {}
    extra_mutation: dict[str, GraphQLField] = {}

    role_id = si.role["id"]
    accessible = set(si.role["domain_access"])
    all_access = "*" in accessible

    def _gql_scalar(type_str: str):
        return _ACTION_SCALAR_MAP.get(type_str, GraphQLString)

    def _build_args(
        arguments: list[dict], response_fields: set[str] | None = None
    ) -> dict[str, GraphQLArgument]:
        result = {}
        for a in arguments:
            if not a.get("name"):
                continue
            name = a["name"]
            if response_fields and name in response_fields:
                name = f"_{name}"
            result[name] = GraphQLArgument(_gql_scalar(a.get("type", "String")))
        return result

    def _build_callable_fields(
        items: list[dict],
        extra_query: dict[str, GraphQLField],
        extra_mutation: dict[str, GraphQLField],
        resolve_return_and_args,
    ) -> None:
        """Emit query/mutation fields for a list of callable items (functions or webhooks).

        Args:
            items: List of function or webhook dicts.
            extra_query: Accumulator for query fields (mutated in place).
            extra_mutation: Accumulator for mutation fields (mutated in place).
            resolve_return_and_args: Callable(item) → (gql_return, args) that computes
                the GraphQL return type and argument dict for one item.
        """
        for item in items:
            visible_to = item.get("visible_to") or []
            if visible_to and role_id not in visible_to:
                continue
            domain_id = item.get("domain_id", "")
            if not all_access and domain_id and domain_id not in accessible:
                continue

            gql_return, args = resolve_return_and_args(item)

            if item.get("kind", "mutation") == "query":
                args["limit"] = GraphQLArgument(GraphQLInt)
                args["offset"] = GraphQLArgument(GraphQLInt)
                args["where"] = GraphQLArgument(cast(GraphQLScalarType, JSONScalar))
                args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(GraphQLString)))

            gql_field = GraphQLField(gql_return, args=args, description=item.get("description"))
            field_key = item["name"]
            if si.domain_prefix and domain_id:
                alias = (domain_alias_map or {}).get(domain_id) or domain_to_sql_name(domain_id)
                field_key = f"{alias}__{field_key}"

            if item.get("kind", "mutation") == "query":
                extra_query[field_key] = gql_field
            else:
                extra_mutation[field_key] = gql_field

    def _resolve_function(func: dict):
        returns_str = func.get("returns", "")
        # returns_str is "schema.table" — look up the GraphQL object type
        parts = returns_str.split(".", 1) if returns_str else []
        if len(parts) == _SCHEMA_TABLE_PARTS:
            ret_type = table_type_lookup.get((parts[0], parts[1]))
            gql_return = GraphQLList(GraphQLNonNull(ret_type)) if ret_type else GraphQLString
        else:
            return_schema = func.get("return_schema")
            if return_schema:
                type_name = "".join(p.capitalize() for p in func["name"].split("_")) + "ReturnType"
                gql_return = _json_schema_to_gql_type(return_schema, type_name) or GraphQLString
            else:
                gql_return = GraphQLString
            ret_type = None
        # Detect collision between function arg names and return type field names
        ret_fields: set[str] = (
            set(ret_type.fields.keys()) if ret_type and hasattr(ret_type, "fields") else set()
        )
        args = _build_args(
            func["arguments"] if isinstance(func["arguments"], list) else [],
            response_fields=ret_fields,
        )
        return gql_return, args

    def _resolve_webhook(wh: dict):
        returns_str = wh.get("returns") or ""
        inline = wh.get("inline_return_type") or []
        if returns_str:
            # normalize "source.schema.table" → look up by (schema, table)
            rparts = returns_str.split(".")
            if len(rparts) >= _SCHEMA_TABLE_PARTS:
                schema_part, table_part = rparts[-2], rparts[-1]
                ret_type = table_type_lookup.get((schema_part, table_part))
            else:
                ret_type = None
            gql_return = GraphQLList(GraphQLNonNull(ret_type)) if ret_type else GraphQLString
        elif inline:
            wh_type_name = "".join(p.capitalize() for p in wh["name"].split("_")) + "ReturnType"
            inline_fields = {
                f["name"]: GraphQLField(_gql_scalar(f.get("type", "String")))
                for f in inline
                if f.get("name")
            }
            wh_obj = cast(
                GraphQLObjectType, GraphQLObjectType(wh_type_name, lambda f=inline_fields: f)
            )
            gql_return = GraphQLList(GraphQLNonNull(wh_obj))
            ret_type = None
        else:
            ret_type = None
            gql_return = GraphQLString
        wh_ret_fields: set[str] = (
            set(ret_type.fields.keys()) if ret_type and hasattr(ret_type, "fields") else set()
        )
        args = _build_args(
            wh["arguments"] if isinstance(wh["arguments"], list) else [],
            response_fields=wh_ret_fields,
        )
        return gql_return, args

    _build_callable_fields(si.functions, extra_query, extra_mutation, _resolve_function)
    _build_callable_fields(si.webhooks, extra_query, extra_mutation, _resolve_webhook)

    return extra_query, extra_mutation
