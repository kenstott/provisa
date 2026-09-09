# Copyright (c) 2026 Kenneth Stott
# Canary: 5a8c1e7f-3d9b-4b6e-a2c4-8f1d0e7b3a59
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Land a Hasura v2 remote schema as tables (REQ-1681).

Hasura proxies a remote schema and governs it with a per-role SDL subset. Provisa lands remote
GraphQL data as replicas and governs it like any table, so the import registers one table per
Query root field the role SDLs expose. Without introspection the SDLs are the only statement of
the remote's shape the export carries, which is why a remote schema with no permissions yields
no tables and a warning.
"""

# Requirements: REQ-417, REQ-1681

from __future__ import annotations

from typing import Any

from graphql import (
    EnumTypeDefinitionNode,
    FieldDefinitionNode,
    ListTypeNode,
    NamedTypeNode,
    NonNullTypeNode,
    ObjectTypeDefinitionNode,
    TypeNode,
    parse,
)
from graphql.error import GraphQLSyntaxError

from provisa.core.models import Column, Table
from provisa.hasura_v2.models import HasuraRemoteSchema
from provisa.import_shared.warnings import WarningCollector
from provisa.security.rights import ORG_ADMIN_ROLE

_SCALAR_TO_IR = {
    "Int": "integer",
    "Float": "double",
    "String": "varchar",
    "ID": "varchar",
    "Boolean": "boolean",
}


def _base_name(t: TypeNode) -> str:
    while isinstance(t, (NonNullTypeNode, ListTypeNode)):
        t = t.type
    assert isinstance(t, NamedTypeNode)
    return t.name.value


def _is_list(t: TypeNode) -> bool:
    while isinstance(t, NonNullTypeNode):
        t = t.type
    return isinstance(t, ListTypeNode)


def _root_and_types(sdl: str) -> tuple[ObjectTypeDefinitionNode | None, dict[str, Any]]:
    doc = parse(sdl)
    objects: dict[str, ObjectTypeDefinitionNode] = {}
    enums: set[str] = set()
    query_name = "Query"
    for d in doc.definitions:
        if isinstance(d, ObjectTypeDefinitionNode):
            objects[d.name.value] = d
        elif isinstance(d, EnumTypeDefinitionNode):
            enums.add(d.name.value)
        elif getattr(d, "kind", "") == "schema_definition":
            for op in d.operation_types:  # pyright: ignore[reportAttributeAccessIssue]
                if op.operation.value == "query":
                    query_name = op.type.name.value
    return objects.get(query_name), {"objects": objects, "enums": enums}


def _column_type(field: FieldDefinitionNode, enums: set[str]) -> str | None:
    """The IR type for a scalar/enum field, or None when the field is an object or a list."""
    if _is_list(field.type):
        return None
    name = _base_name(field.type)
    if name in _SCALAR_TO_IR:
        return _SCALAR_TO_IR[name]
    if name in enums:
        return "varchar"
    return None


def land_remote_schema(
    rs: HasuraRemoteSchema, domain_id: str, collector: WarningCollector
) -> list[Table]:
    """One table per Query root field across the remote schema's role SDLs.

    A column's ``visible_to`` is every role whose SDL exposes the field on the returned type. A
    non-null root-field argument becomes a ``_nf_<arg>`` native-filter column (query_param) so the
    argument is supplied at query time (the same shape the demo's graphql_remote tables declare).
    """
    if not rs.permissions:
        collector.warn(
            "remote_schemas",
            f"Remote schema {rs.name!r} carries no role permissions; without an introspected "
            "schema the importer cannot land its fields as tables",
        )
        return []

    # table name → {"columns": {name: (type, roles)}, "args": {name: type}, "skipped": set}
    tables: dict[str, dict[str, Any]] = {}
    for perm in rs.permissions:
        role = perm.get("role", "")
        sdl = ((perm.get("definition") or {}).get("schema")) or ""
        if not role or not sdl.strip():
            continue
        try:
            query, ctx = _root_and_types(sdl)
        except GraphQLSyntaxError as exc:
            collector.warn(
                "remote_schemas",
                f"Remote schema {rs.name!r} role {role!r} SDL does not parse: {exc.message}",
            )
            continue
        if query is None:
            continue
        objects, enums = ctx["objects"], ctx["enums"]
        for root in query.fields:
            fname = root.name.value
            ret = objects.get(_base_name(root.type))
            if ret is None:
                continue  # a root field returning a scalar has no rows to land
            entry = tables.setdefault(
                fname, {"columns": {}, "args": {}, "skipped": set(), "roles": set()}
            )
            entry["roles"].add(role)
            for f in ret.fields:
                ctype = _column_type(f, enums)
                if ctype is None:
                    entry["skipped"].add(f.name.value)
                    continue
                cur = entry["columns"].setdefault(f.name.value, (ctype, set()))
                cur[1].add(role)
            for arg in root.arguments:
                if isinstance(arg.type, NonNullTypeNode):
                    entry["args"][arg.name.value] = _column_type(arg, enums) or "varchar"  # pyright: ignore[reportArgumentType]

    out: list[Table] = []
    for fname, entry in sorted(tables.items()):
        columns: list[Column] = []
        for arg_name, arg_type in sorted(entry["args"].items()):
            columns.append(
                Column(
                    name=f"_nf_{arg_name}",
                    data_type=arg_type,
                    native_filter_type="query_param",
                    visible_to=[],
                    description=f"{arg_name} argument of {rs.name}.{fname}, passed through at query time",
                )
            )
        for cname, (ctype, roles) in entry["columns"].items():
            # REQ-1684: org_admin is Hasura's implicit admin on every column.
            columns.append(
                Column(name=cname, data_type=ctype, visible_to=sorted(roles | {ORG_ADMIN_ROLE}))
            )
        if entry["skipped"]:
            collector.warn(
                "remote_schemas",
                f"Remote schema {rs.name!r} field {fname!r}: nested fields not landed as columns: "
                + ", ".join(sorted(entry["skipped"])),
            )
        out.append(
            Table(
                source_id=rs.name,
                domain_id=domain_id,
                schema_name="graphql",
                table_name=fname,
                columns=columns,
                description=f"{rs.name}.{fname} (Hasura remote schema, landed)",
            )
        )
    return out
