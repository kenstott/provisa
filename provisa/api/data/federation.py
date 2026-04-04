# Copyright (c) 2025 Kenneth Stott
# Canary: d4ff0394-3b79-4f6e-8f99-4f9b10c247ab
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Apollo Federation entity resolver (Phase AJ).

Resolves _entities queries by extracting __typename and key fields from
representations, grouping by type, and executing batched queries through
the existing pipeline (RLS + masking applied).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class EntityResolutionPlan:
    """Batch plan: one SQL query per entity type."""

    type_name: str
    table_id: int
    key_columns: list[str]
    key_values: list[dict[str, object]]
    # Index positions in the original representations list
    positions: list[int]


def group_representations(
    representations: list[dict],
    type_to_table: dict[str, int],
    type_to_keys: dict[str, list[str]],
) -> list[EntityResolutionPlan]:
    """Group entity representations by __typename for batch resolution.

    Args:
        representations: List of dicts with __typename and key fields.
        type_to_table: Maps GraphQL type name -> table_id.
        type_to_keys: Maps GraphQL type name -> list of PK column names.

    Returns:
        List of EntityResolutionPlan, one per distinct type in representations.
    """
    plans: dict[str, EntityResolutionPlan] = {}

    for idx, rep in enumerate(representations):
        type_name = rep.get("__typename")
        if not type_name:
            raise ValueError(
                f"Representation at index {idx} missing __typename"
            )
        if type_name not in type_to_table:
            raise ValueError(
                f"Unknown entity type: {type_name!r}"
            )

        if type_name not in plans:
            plans[type_name] = EntityResolutionPlan(
                type_name=type_name,
                table_id=type_to_table[type_name],
                key_columns=type_to_keys[type_name],
                key_values=[],
                positions=[],
            )

        plan = plans[type_name]
        key_vals = {}
        for col in plan.key_columns:
            if col not in rep:
                raise ValueError(
                    f"Representation for {type_name} missing key field: {col!r}"
                )
            key_vals[col] = rep[col]

        plan.key_values.append(key_vals)
        plan.positions.append(idx)

    return list(plans.values())


def compile_entity_query(
    catalog: str,
    schema_name: str,
    table_name: str,
    key_columns: list[str],
    key_values: list[dict[str, object]],
) -> tuple[str, list[object]]:
    """Compile a batched SELECT for entity resolution.

    Generates: SELECT * FROM "catalog"."schema"."table"
               WHERE (pk1, pk2) IN ((v1, v2), (v3, v4))
    Or for single PK: WHERE pk IN ($1, $2, ...)

    Args:
        catalog: Trino catalog name.
        schema_name: DB schema name.
        table_name: DB table name.
        key_columns: PK column names.
        key_values: List of dicts mapping column -> value.

    Returns:
        (sql, params) tuple.
    """
    fqn = f'"{catalog}"."{schema_name}"."{table_name}"'
    params: list[object] = []

    if len(key_columns) == 1:
        col = key_columns[0]
        placeholders = []
        for kv in key_values:
            params.append(kv[col])
            placeholders.append(f"${len(params)}")
        where = f'"{col}" IN ({", ".join(placeholders)})'
    else:
        # Composite key: WHERE (col1, col2) IN ((v1, v2), ...)
        col_list = ", ".join(f'"{c}"' for c in key_columns)
        tuples = []
        for kv in key_values:
            vals = []
            for c in key_columns:
                params.append(kv[c])
                vals.append(f"${len(params)}")
            tuples.append(f"({', '.join(vals)})")
        where = f"({col_list}) IN ({', '.join(tuples)})"

    sql = f"SELECT * FROM {fqn} WHERE {where}"
    return sql, params


async def resolve_entities(
    representations: list[dict],
    type_to_table: dict[str, int],
    type_to_keys: dict[str, list[str]],
    table_meta: dict[int, dict],
    execute_fn,
    rls_context=None,
) -> list[dict | None]:
    """Resolve _entities representations through the existing pipeline.

    Args:
        representations: Raw representations from the _entities query.
        type_to_table: type_name -> table_id.
        type_to_keys: type_name -> PK column names.
        table_meta: table_id -> {catalog, schema_name, table_name}.
        execute_fn: async (sql, params, rls_context) -> list[dict].
        rls_context: Optional RLS context for security enforcement.

    Returns:
        Ordered list matching input representations. None for unresolved.
    """
    plans = group_representations(representations, type_to_table, type_to_keys)
    results: list[dict | None] = [None] * len(representations)

    for plan in plans:
        meta = table_meta.get(plan.table_id)
        if meta is None:
            continue

        sql, params = compile_entity_query(
            catalog=meta["catalog"],
            schema_name=meta["schema_name"],
            table_name=meta["table_name"],
            key_columns=plan.key_columns,
            key_values=plan.key_values,
        )

        rows = await execute_fn(sql, params, rls_context)

        # Index rows by key for O(1) lookup
        row_index: dict[tuple, dict] = {}
        for row in rows:
            key = tuple(row.get(c) for c in plan.key_columns)
            row_index[key] = row

        # Map results back to original positions
        for i, kv in enumerate(plan.key_values):
            key = tuple(kv[c] for c in plan.key_columns)
            row = row_index.get(key)
            if row is not None:
                row["__typename"] = plan.type_name
                results[plan.positions[i]] = row

    return results
