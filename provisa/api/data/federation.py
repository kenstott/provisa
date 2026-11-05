# Copyright (c) 2026 Kenneth Stott
# Canary: d4ff0394-3b79-4f6e-8f99-4f9b10c247ab
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa as an Apollo Federation v2 subgraph (REQ-259), served on /data/graphql.

A supergraph cannot pass its caller's identity, so the subgraph is an endpoint with a declared
role: the org's ``federation.role``. The router authenticates as an ordinary client acting as that
role, and that role's schema -- and no other's -- carries ``_service`` and ``_entities``.

``_entities`` is not read with a query of its own. Each batch of representations of one type is
rewritten into an ordinary query of that type's table, filtered to the representations' keys and
selecting what the router asked of the type; that query runs through the same governed path as any
other read by the role (``endpoint._handle_query``), so the role's row rules, masks and visibility
hold. A representation whose row the role cannot see resolves to null.
"""

# Requirements: REQ-259

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from graphql import (
    ArgumentNode,
    DocumentNode,
    FieldNode,
    FragmentDefinitionNode,
    FragmentSpreadNode,
    GraphQLInputObjectType,
    GraphQLObjectType,
    GraphQLSchema,
    InlineFragmentNode,
    IntValueNode,
    ListValueNode,
    NameNode,
    ObjectFieldNode,
    ObjectValueNode,
    OperationDefinitionNode,
    OperationType,
    SelectionNode,
    SelectionSetNode,
    VariableDefinitionNode,
    VariableNode,
    get_named_type,
    parse_type,
    validate,
    value_from_ast_untyped,
    visit,
    Visitor,
)

from provisa.api.errors import ApiError
from provisa.compiler.federation import build_federation_schema, generate_federation_sdl
from provisa.core.models import FederationConfig

ENTITIES = "_entities"
SERVICE = "_service"


def subgraph(state: Any) -> FederationConfig:
    """The bound org's subgraph: its own ``federation`` setting (org_settings) where it has one,
    which replaces the config file's as every org-owned key does (REQ-1349)."""
    overrides = state.settings_overrides
    if "federation" in overrides:
        return FederationConfig.model_validate(overrides["federation"])
    if state.config is None:
        # A first start with no config file yet: nothing declares a subgraph.
        return FederationConfig()
    return state.config.federation


@dataclass(frozen=True)
class EntityType:
    """A type the subgraph resolves by key: its table's list field and its designated primary
    key, as the type's field names."""

    type_name: str
    root_field: str
    keys: tuple[str, ...]


def entity_types(schema: GraphQLSchema, ctx: Any) -> dict[str, EntityType]:
    """Each table of the role's surface with a designated primary key (REQ-393) whose key fields
    the role sees, by type name. A table whose key the role cannot see is not an entity of it."""
    found: dict[str, EntityType] = {}
    for field_name, meta in ctx.tables.items():
        if field_name != meta.field_name or meta.type_name in found:
            continue  # another root field of the same table (an alias of it)
        designated = ctx.pk_columns.get(meta.table_id)
        gql_type = schema.type_map.get(meta.type_name)
        if not designated or not isinstance(gql_type, GraphQLObjectType):
            continue
        keys = tuple(ctx.physical_to_gql.get((meta.table_id, col)) for col in designated)
        if all(k is not None and k in gql_type.fields for k in keys):
            found[meta.type_name] = EntityType(meta.type_name, field_name, keys)  # type: ignore[arg-type]
    return found


@dataclass
class Federated:
    """The declared role's surface, as a subgraph: its schema with ``_service`` and
    ``_entities``, its SDL with ``@key`` on each entity type, and its entity types."""

    surface: Any
    schema: GraphQLSchema
    sdl: str
    types: dict[str, EntityType]


# (org, role) -> the federated form of that role's surface. A surface is replaced whole at each
# build (role_surface.install_surface), so a kept form is reused only for the very surface it was
# made from.
_federated: dict[tuple[str | None, str], Federated] = {}


def federated(surface: Any, org_id: str | None, role_id: str) -> Federated:
    kept = _federated.get((org_id, role_id))
    if kept is not None and kept.surface is surface:
        return kept
    base = surface.schema
    types = entity_types(base, surface.context)
    tables = [{"id": i, "_type_name": t.type_name} for i, t in enumerate(types.values())]
    keys = {i: list(t.keys) for i, t in enumerate(types.values())}
    made = Federated(
        surface,
        build_federation_schema(base, tables, keys),
        generate_federation_sdl(base, {t.type_name: " ".join(t.keys) for t in types.values()}),
        types,
    )
    _federated[(org_id, role_id)] = made
    return made


# -- a request's federation field ------------------------------------------------------------------


@dataclass
class ServiceRead:
    """``{ _service { sdl } }``: answered with the subgraph's SDL."""

    alias: str
    selection: SelectionSetNode | None

    def answer(self, sdl: str) -> dict:
        service: dict[str, Any] = {}
        for sel in self.selection.selections if self.selection else ():
            if isinstance(sel, FieldNode):
                key = sel.alias.value if sel.alias else sel.name.value
                service[key] = sdl if sel.name.value == "sdl" else "_Service"
        return {"data": {self.alias: service}}


@dataclass
class _Group:
    alias: str
    type: EntityType
    positions: list[int] = field(default_factory=list)
    keys: list[tuple[str, ...]] = field(default_factory=list)


@dataclass
class EntitiesRead:
    """``_entities``, rewritten into one governed read per entity type (``document``,
    ``variables``), and how each answer goes back to its representation."""

    alias: str
    document: DocumentNode
    variables: dict[str, Any]
    groups: list[_Group]
    count: int

    def answer(self, body: dict) -> dict:
        """The ``_entities`` answer from the governed read's ``body``: each representation's row
        with its ``__typename``, in the order given; null where the role sees no row for it."""
        if "redirect" in body or "redirects" in body:
            raise ApiError(
                413,
                "data.entities_redirected",
                "The entities asked for are more than an inline answer holds; the router is to "
                "ask for fewer at a time",
            )
        data = body["data"]
        results: list[dict | None] = [None] * self.count
        for group in self.groups:
            by_key: dict[tuple[str, ...], dict] = {}
            for row in data[group.alias] or []:
                # The designated key is informational (REQ-393): the first row of a key answers it.
                by_key.setdefault(tuple(str(row[k]) for k in group.type.keys), row)
            for position, key in zip(group.positions, group.keys):
                row = by_key.get(key)
                if row is not None:
                    results[position] = {"__typename": group.type.type_name, **row}
        return {"data": {self.alias: results}}


def federation_read(
    document: DocumentNode, variables: dict | None, fed: Federated
) -> ServiceRead | EntitiesRead | None:
    """What a request asks of the subgraph: ``_service``, ``_entities``, or neither (None). A
    federation field is asked alone, as a router asks it."""
    operation = next(d for d in document.definitions if isinstance(d, OperationDefinitionNode))
    roots = operation.selection_set.selections
    names = [s.name.value for s in roots if isinstance(s, FieldNode)]
    if not {ENTITIES, SERVICE} & set(names):
        return None
    if len(roots) != 1:
        raise ApiError(
            400,
            "data.federation_field_alone",
            "_service and _entities are each asked alone, as a router asks them",
        )
    root = roots[0]
    assert isinstance(root, FieldNode)
    alias = root.alias.value if root.alias else root.name.value
    if root.name.value == SERVICE:
        return ServiceRead(alias, root.selection_set)
    return _entities_read(document, operation, root, alias, variables or {}, fed)


def _entities_read(
    document: DocumentNode,
    operation: OperationDefinitionNode,
    root: FieldNode,
    alias: str,
    variables: dict,
    fed: Federated,
) -> EntitiesRead:
    fragments = {
        d.name.value: d for d in document.definitions if isinstance(d, FragmentDefinitionNode)
    }
    (argument,) = [a for a in root.arguments if a.name.value == "representations"]
    representations = value_from_ast_untyped(argument.value, variables)
    # What the router asks of each type: its inline fragments' (or named fragments') selections.
    asked: dict[str, list[SelectionNode]] = {}
    assert root.selection_set is not None
    for sel in root.selection_set.selections:
        if isinstance(sel, FragmentSpreadNode):
            fragment = fragments[sel.name.value]
            asked.setdefault(fragment.type_condition.name.value, []).extend(
                fragment.selection_set.selections
            )
        elif isinstance(sel, InlineFragmentNode) and sel.type_condition is not None:
            asked.setdefault(sel.type_condition.name.value, []).extend(sel.selection_set.selections)
        # A bare __typename: every entity answers its own.

    groups: dict[str, _Group] = {}
    for position, rep in enumerate(representations):
        type_name = rep.get("__typename") if isinstance(rep, dict) else None
        entity = fed.types.get(type_name) if isinstance(type_name, str) else None
        if entity is None:
            raise ApiError(
                400,
                "data.entity_type_unknown",
                f"Representation {position} names no entity of this subgraph: {type_name!r}",
                position=str(position),
            )
        missing = [k for k in entity.keys if k not in rep]
        if missing:
            raise ApiError(
                400,
                "data.entity_key_missing",
                f"Representation {position} of {type_name} has no {', '.join(missing)}",
                position=str(position),
            )
        group = groups.setdefault(entity.type_name, _Group(f"e{len(groups)}", entity))
        group.positions.append(position)
        group.keys.append(tuple(str(rep[k]) for k in entity.keys))

    fields: list[FieldNode] = []
    values: dict[str, Any] = {}
    definitions: list[VariableDefinitionNode] = []
    for group in groups.values():
        entity = group.type
        root_field = fed.surface.schema.query_type.fields[entity.root_field]
        where_type = get_named_type(root_field.args["where"].type)
        assert isinstance(where_type, GraphQLInputObjectType)
        reps = [representations[p] for p in group.positions]
        where = _key_filter(group.alias, entity, reps, where_type, values, definitions)
        distinct = len(set(group.keys))
        selections = [s for s in asked.get(entity.type_name, []) if not _is_typename(s)]
        selected = {s.name.value for s in selections if isinstance(s, FieldNode) and not s.alias}
        selections += [_field(k) for k in entity.keys if k not in selected]
        fields.append(
            FieldNode(
                alias=NameNode(value=group.alias),
                name=NameNode(value=entity.root_field),
                arguments=(
                    ArgumentNode(name=NameNode(value="where"), value=where),
                    ArgumentNode(
                        name=NameNode(value="limit"), value=IntValueNode(value=str(distinct))
                    ),
                ),
                directives=(),
                selection_set=SelectionSetNode(selections=tuple(selections)),
            )
        )

    body = SelectionSetNode(selections=tuple(fields))
    used_fragments = _fragments_used(body, fragments)
    used_variables = _variables_used([body, *used_fragments])
    kept = [
        d
        for d in operation.variable_definitions
        if d.variable.name.value in used_variables and d.variable.name.value != "representations"
    ]
    for d in kept:
        name = d.variable.name.value
        if name in variables:
            values[name] = variables[name]
    rewritten = DocumentNode(
        definitions=(
            OperationDefinitionNode(
                operation=OperationType.QUERY,
                name=None,
                variable_definitions=tuple(kept + definitions),
                directives=(),
                selection_set=body,
            ),
            *used_fragments,
        )
    )
    errors = validate(fed.surface.schema, rewritten)
    if errors:
        raise ApiError(
            400,
            "data.entities_invalid",
            "The entities asked for cannot be read as this role: "
            + "; ".join(e.message for e in errors),
        )
    return EntitiesRead(alias, rewritten, values, list(groups.values()), len(representations))


def _key_filter(
    alias: str,
    entity: EntityType,
    reps: list[dict],
    where_type: GraphQLInputObjectType,
    values: dict[str, Any],
    definitions: list[VariableDefinitionNode],
) -> ObjectValueNode:
    """The ``where`` that reads exactly the rows of ``reps``' keys, its values as variables
    typed as the role's filter types them: ``{k: {in: $..}}`` for a one-column key, an ``_or``
    of each representation's ``{k: {eq: $..}}`` columns for a composite one."""

    def variable(name: str, op: str, key: str, value: Any) -> VariableNode:
        comparison = get_named_type(where_type.fields[key].type)
        assert isinstance(comparison, GraphQLInputObjectType)
        gql_type = comparison.fields[op].type
        definitions.append(
            VariableDefinitionNode(
                variable=VariableNode(name=NameNode(value=name)),
                type=parse_type(str(gql_type)),
                directives=(),
            )
        )
        values[name] = value
        return VariableNode(name=NameNode(value=name))

    if len(entity.keys) == 1:
        (key,) = entity.keys
        in_values = list({json.dumps(r[key], sort_keys=True): r[key] for r in reps}.values())
        return _object({key: _object({"in": variable(f"{alias}_{key}", "in", key, in_values)})})
    each = []
    for i, rep in enumerate(reps):
        each.append(
            _object(
                {
                    k: _object({"eq": variable(f"{alias}_{i}_{k}", "eq", k, rep[k])})
                    for k in entity.keys
                }
            )
        )
    return _object({"_or": ListValueNode(values=tuple(each))})


def _object(fields: dict[str, Any]) -> ObjectValueNode:
    return ObjectValueNode(
        fields=tuple(ObjectFieldNode(name=NameNode(value=k), value=v) for k, v in fields.items())
    )


def _field(name: str) -> FieldNode:
    return FieldNode(
        alias=None, name=NameNode(value=name), arguments=(), directives=(), selection_set=None
    )


def _is_typename(selection: SelectionNode) -> bool:
    return isinstance(selection, FieldNode) and selection.name.value == "__typename"


def _fragments_used(
    node: Any, fragments: dict[str, FragmentDefinitionNode]
) -> list[FragmentDefinitionNode]:
    """The fragment definitions ``node`` spreads, and those they spread, in a stable order."""
    found: dict[str, FragmentDefinitionNode] = {}
    pending = [node]
    while pending:
        current = pending.pop()
        for name in _spread_names(current):
            if name not in found:
                found[name] = fragments[name]
                pending.append(fragments[name])
    return list(found.values())


def _spread_names(node: Any) -> list[str]:
    names: list[str] = []

    class _Spreads(Visitor):
        def enter_fragment_spread(self, spread: FragmentSpreadNode, *_args: Any) -> None:
            names.append(spread.name.value)

    visit(node, _Spreads())
    return names


def _variables_used(nodes: list[Any]) -> set[str]:
    names: set[str] = set()

    class _Variables(Visitor):
        def enter_variable(self, variable: VariableNode, *_args: Any) -> None:
            names.add(variable.name.value)

    for node in nodes:
        visit(node, _Variables())
    return names


__all__ = [
    "EntitiesRead",
    "EntityType",
    "Federated",
    "ServiceRead",
    "entity_types",
    "federated",
    "federation_read",
    "subgraph",
]
