# Copyright (c) 2026 Kenneth Stott
# Canary: 8fcae53e-8f0e-496d-941b-a04721eae752
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Physical ↔ GraphQL name translation for the JSON:API surface.

JSON:API is an API over the SQL plane, so every name it accepts in a query parameter and every
key it emits in a document is the *physical* column or relationship name. The GQL naming
convention (camelCase under ``apollo_graphql``) is the GraphQL surface's alone — REQ-471 keeps the
two authorities separate — and JSON:API only borrows it for the GraphQL text it synthesizes.
Requests translate physical → GQL at that emit boundary and responses translate back, which is the
rule ``?groupBy=`` (REQ-1361) and gRPC's ``include`` (REQ-1408) already follow.
"""

# Requirements: REQ-471, REQ-1361, REQ-1408, REQ-1417

from __future__ import annotations

from typing import Any

from provisa.compiler.naming import apply_sql_name


def scalar_name_maps(
    ctx: Any, table_id: int, gql_scalars: list[str]
) -> tuple[dict[str, str], dict[str, str]]:
    """``(gql → physical, physical → gql)`` for one table's scalar columns.

    ``ctx.exposed_to_physical`` is the compiler's own record of the rename, so it is the authority
    here rather than a second application of the convention. It records a column only when the
    exposed name differs (compiler/context.py::_register_columns ``if gql != phys``), so a column
    absent from it is one the convention left alone and its physical name *is* its GQL name.
    """
    gql_to_physical = {g: ctx.exposed_to_physical.get((table_id, g), g) for g in gql_scalars}
    return gql_to_physical, {p: g for g, p in gql_to_physical.items()}


def physical_rel_name(gql_name: str) -> str:
    """The physical spelling of one relationship field name.

    A relationship field name is minted by ``naming.rel_field_name``, which joins its words with
    ``_`` and then applies the GQL convention, so the physical spelling is that same name under the
    SQL plane's convention (REQ-471) — identity when the two conventions agree. Every surface that
    needs this name calls here rather than transliterating the casing itself; the admin API exposes
    it as ``RelationshipType.physical_name`` so the UI never has to.
    """
    return apply_sql_name(gql_name)


def relationship_name_maps(gql_rels: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """``(gql → physical, physical → gql)`` for relationship field names."""
    gql_to_physical = {g: physical_rel_name(g) for g in gql_rels}
    return gql_to_physical, {p: g for g, p in gql_to_physical.items()}


def relationship_scalar_maps(
    ctx: Any, type_name: str, rel_scalars: dict[str, list[str]]
) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    """Per-relationship column maps: ``(gql → physical, physical → gql)`` keyed by GQL rel name.

    The related table's id comes from the join the compiler registered for that field
    (``ctx.joins[(type_name, rel)].target``, compiler/context.py::_register_relationship_joins), so
    the same ``exposed_to_physical`` authority covers a sideloaded table's columns as covers the
    base table's. A relationship field the schema exposes always has that join — it is what
    generated the field — so a missing key is a broken context, not a case to paper over.
    """
    gql_to_physical: dict[str, dict[str, str]] = {}
    physical_to_gql: dict[str, dict[str, str]] = {}
    for rel, columns in rel_scalars.items():
        target_id = ctx.joins[(type_name, rel)].target.table_id
        gql_to_physical[rel], physical_to_gql[rel] = scalar_name_maps(ctx, target_id, columns)
    return gql_to_physical, physical_to_gql


def rename_row_keys(
    row: dict[str, Any],
    gql_to_physical: dict[str, str],
    rel_gql_to_physical: dict[str, str],
    rel_scalar_maps: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """Rewrite one serialized row's GQL keys to their physical names, nested objects included.

    ``rel_scalar_maps`` is the related table's gql → physical column map per relationship field,
    so a sideloaded ``pet { breedName }`` comes back as ``pet: {"breed_name": ...}``.
    """
    out: dict[str, Any] = {}
    for key, value in row.items():
        if key in rel_gql_to_physical:
            nested = rel_scalar_maps.get(key, {})
            out[rel_gql_to_physical[key]] = _rename_nested(value, nested)
        else:
            out[gql_to_physical.get(key, key)] = value
    return out


def _rename_nested(value: Any, column_map: dict[str, str]) -> Any:
    """Apply a column map to a relationship's value — an object, a list of them, or null."""
    if isinstance(value, dict):
        return {column_map.get(k, k): v for k, v in value.items()}
    if isinstance(value, list):
        return [_rename_nested(v, column_map) for v in value]
    return value
