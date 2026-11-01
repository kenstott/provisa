# Copyright (c) 2026 Kenneth Stott
# Canary: bed8019e-98fe-48f9-8a39-459b8f68d26c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a role reads over OData, from the surface it is served through (REQ-2193).

As a role's proto is built from what the role sees, its model is read from its own surface: the
GraphQL schema and the compilation context hold only that role's tables, columns and
relationships, so nothing here filters by role and a table the role cannot see cannot appear.
"""

# Requirements: REQ-2193

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from graphql import GraphQLInputObjectType, GraphQLNonNull, GraphQLList, is_required_argument

from provisa.api._query_helpers import get_scalar_fields
from provisa.api.odata.edm import EdmType, edm_type
from provisa.api.role_surface import RoleSurface
from provisa.compiler.sql_types import JoinMeta, key_list

_SINGLE_VALUED = ("many-to-one", "one-to-one")
# The synthetic join every table has to the catalog's record of it: not a relationship of the model.
_CATALOG_JOIN = "_meta"


@dataclass(frozen=True)
class Property:
    name: str
    type: EdmType
    column: str  # the physical name a statement reads it by
    data_type: str  # as registered


@dataclass(frozen=True)
class Navigation:
    name: str
    target_set: str
    target_type: str
    collection: bool
    constraints: tuple[tuple[str, str], ...]  # (property, referenced property)


@dataclass(frozen=True)
class EntitySet:
    name: str
    type_name: str
    key: tuple[str, ...]
    properties: tuple[Property, ...]
    navigations: tuple[Navigation, ...]
    ordering: tuple[str, ...]  # the properties that give a read one order across its pages

    def property(self, name: str) -> Property | None:
        return next((p for p in self.properties if p.name == name), None)


def _unwrapped(gql_type: Any) -> Any:  # noqa: ANN401 — a graphql-core type
    while isinstance(gql_type, (GraphQLNonNull, GraphQLList)):
        gql_type = gql_type.of_type
    return gql_type


def _constraints(join: JoinMeta, source_id: int, ctx: Any) -> tuple[tuple[str, str], ...]:  # noqa: ANN401
    """The relationship's constraint, when it is one: equal columns the role sees on both ends of
    a single-valued property. CSDL gives a collection-valued property none; the to-one side
    of the relationship states it. An expression, a constant or a junction is no column pair."""
    if join.cardinality not in _SINGLE_VALUED or join.via is not None:
        return ()
    if join.source_constant is not None or join.source_json_key or join.source_expr:
        return ()
    if join.target_expr:
        return ()
    pairs = []
    for source, target in zip(key_list(join.source_column), key_list(join.target_column)):
        # physical_to_gql holds the columns this role sees: a key it does not is no property.
        name = ctx.physical_to_gql.get((source_id, source))
        referenced = ctx.physical_to_gql.get((join.target.table_id, target))
        if name is None or referenced is None:
            return ()
        pairs.append((name, referenced))
    return tuple(pairs)


def _navigations(surface: RoleSurface, field_name: str) -> tuple[Navigation, ...]:
    ctx = surface.context
    meta = ctx.tables[field_name]
    fields = _unwrapped(surface.schema.query_type.fields[field_name].type).fields
    navigations = []
    for (type_name, name), join in ctx.joins.items():
        if type_name != meta.type_name or name == _CATALOG_JOIN or name not in fields:
            continue
        if entity_set(surface, join.target.field_name) is None:
            continue
        navigations.append(
            Navigation(
                name,
                join.target.field_name,
                join.target.type_name,
                join.cardinality not in _SINGLE_VALUED,
                _constraints(join, meta.table_id, ctx),
            )
        )
    return tuple(sorted(navigations, key=lambda n: n.name))


def _ordering(root: Any, key: tuple[str, ...], properties: tuple[Property, ...]) -> tuple[str, ...]:  # noqa: ANN401
    """The key, or with no key every property that sorts: rows equal in all of them are
    interchangeable, so a page boundary between them loses none."""
    if key:
        return key
    argument = root.args.get("order_by")
    if argument is None:
        return ()
    order_type = _unwrapped(argument.type)
    assert isinstance(order_type, GraphQLInputObjectType)
    return tuple(p.name for p in properties if p.name in order_type.fields)


def entity_set(surface: RoleSurface, name: str, *, navigations: bool = False) -> EntitySet | None:
    """The entity set ``name`` for this role, or None when the role has none. A table read only
    with arguments it must be given has none: an entity set is read with no arguments."""
    if surface.schema is None or surface.schema.query_type is None:
        return None
    root = surface.schema.query_type.fields.get(name)
    if root is None or name not in surface.table_path_map:
        return None
    if any(is_required_argument(argument) for argument in root.args.values()):
        return None
    ctx = surface.context
    meta = ctx.tables[name]
    scalars = set(get_scalar_fields(surface.schema, name))
    properties = tuple(
        Property(
            ctx.physical_to_gql[(meta.table_id, column)], edm_type(data_type), column, data_type
        )
        for column, data_type in ctx.aggregate_columns.get(meta.table_id, [])
        if ctx.physical_to_gql[(meta.table_id, column)] in scalars
    )
    if not properties:
        return None
    served = {p.name for p in properties}
    key = tuple(
        ctx.physical_to_gql[(meta.table_id, c)] for c in ctx.pk_columns.get(meta.table_id, [])
    )
    key = key if all(k in served for k in key) else ()
    return EntitySet(
        name,
        meta.type_name,
        key,
        properties,
        _navigations(surface, name) if navigations else (),
        _ordering(root, key, properties),
    )


def entity_sets(surface: RoleSurface) -> list[EntitySet]:
    """Every entity set this role reads, by name."""
    found = (entity_set(surface, name, navigations=True) for name in surface.table_path_map or {})
    return sorted((s for s in found if s is not None), key=lambda s: s.name)
