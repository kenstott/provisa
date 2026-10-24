# Copyright (c) 2026 Kenneth Stott
# Canary: 2b9e4f1a-7c3d-4a8b-9e0f-5d1c6a2b7e3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Shared asyncpg query helpers for schema rebuild."""

# Requirements: REQ-155, REQ-194, REQ-351, REQ-393, REQ-399

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.core.database import Connection


# inflect (~0.8s import) lazy-loaded — paid once on first singularization, never at cold start.
def _as_list(v: Any) -> list:
    """A JSON list column. On PostgreSQL the jsonb codec deserializes it to a list; on a SQLite
    control plane (no codec) a raw-SQL read returns the JSON as TEXT, so decode a string here —
    ``list("[...]")`` would otherwise explode it into characters. Null → empty list."""
    if isinstance(v, str):
        return json.loads(v) if v else []
    return list(v or [])


def _as_dict(v: Any) -> dict:
    """A JSON object column — same PostgreSQL-codec-vs-SQLite-TEXT split as ``_as_list``. Null (no
    live-delivery config) → empty dict."""
    if isinstance(v, str):
        return json.loads(v) if v else {}
    return dict(v or {})


async def fetch_tables(  # REQ-155, REQ-393, REQ-399
    conn: "Connection", *, draft: bool = False
) -> list[dict]:
    """Fetch registered tables with columns — the data plane's: those in service. ``draft``
    True fetches the ones out of service instead (REQ-1921), which no schema offers and no
    statement reads: fetched to be refused by name, for their steward's preview
    (admin/table_read.py) and for the replica such a preview lands."""
    rows = await conn.fetch(
        "SELECT id, source_id, domain_id, schema_name, table_name, "
        "alias, description, unique_constraints, l1_cluster, l2_cluster, l3_cluster, "
        "enable_aggregates, enable_group_by, view_sql, dq_contract, profiler_source_id, "  # REQ-1443/1934
        "live, push_debounce_quiet, push_debounce_max_delay, cache_ttl, "  # REQ-1733, REQ-1730
        "row_materialize, role_ttl, replicate, load_protected, region, draft, "  # REQ-1865/1907/826/1141/1921
        "off_peak_window, off_peak_tz, "  # REQ-1141: when a load-protected table is refreshed
        "file_glob, source_file_column, delta, table_format, "  # REQ-788/874/1981
        "change_signal, watermark_column, probe_type, "  # REQ-929/924/982: the table's own
        "pagination, "  # REQ-318
        "landing_filter, "  # REQ-2062: what of the table its replica holds
        "query_template, "  # REQ-1668/1683/1980: the query a query-API or pipeline table runs
        # REQ-2109: what a bitemporal table maps, and the refresh interval and allowed lateness
        # of the version log of one updated in place (bitemporal_capture).
        "business_key, valid_time, recorded_time, mv_refresh_interval, mv_allowed_lateness "
        "FROM registered_tables WHERE draft = $1 ORDER BY id",
        draft,
    )
    from provisa.core.repositories import write_grant as write_grant_repo

    grants = write_grant_repo.grouped(await conn.fetch(write_grant_repo.BY_TABLE_SQL))
    tables = []
    for row in rows:
        table = dict(row)
        col_rows = await conn.fetch(
            "SELECT column_name, data_type, visible_to, masks, "
            "alias, description, path, is_primary_key, object_fields, native_filter_type, "
            "native_filter_required, required_by_steward, "  # REQ-1986
            "epoch_unit, fake, fake_stable, fake_stable_version, synthetic_rule, "  # REQ-1908, REQ-1494
            "fake_why, "  # REQ-2094
            "array_depth, element_type, native_type, native_platform, "  # REQ-1965
            "scope, "  # REQ-1959: who is served the column is decided from it (rights.column_served)
            "embedding, embedding_model "  # REQ-421, REQ-423: what a similarity search governs
            "FROM table_columns WHERE table_id = $1 ORDER BY id",
            row["id"],
        )
        table["write_grants"] = grants.get(row["id"], [])  # REQ-2074
        table["unique_constraints"] = _as_list(row.get("unique_constraints"))  # REQ-1093
        table["live"] = _as_dict(row.get("live"))  # REQ-1733
        table["delta"] = row.get("delta")  # REQ-874: dict or None, reconstructed on the registry
        table["role_ttl"] = _as_dict(row["role_ttl"])  # REQ-1907
        # REQ-2109: NULL = not bitemporal
        table["business_key"] = (
            None if row["business_key"] is None else _as_list(row["business_key"])
        )
        table["pagination"] = (  # REQ-318: NULL = not paged here
            None if row["pagination"] is None else _as_dict(row["pagination"])
        )
        table["columns"] = [
            {
                "column_name": r["column_name"],
                "data_type": r.get("data_type"),
                "visible_to": _as_list(r["visible_to"]),
                "masks": _as_list(r["masks"]),  # REQ-2059
                "alias": r["alias"],
                "description": r["description"],
                "path": r["path"],
                "is_primary_key": bool(r.get("is_primary_key") or False),
                "object_fields": _as_list(r.get("object_fields")),
                "native_filter_type": r.get("native_filter_type"),
                "native_filter_required": r.get("native_filter_required"),
                "required_by_steward": bool(r.get("required_by_steward") or False),
                "scope": r["scope"],  # REQ-1959
                "epoch_unit": r.get("epoch_unit"),  # REQ-1908
                "array_depth": r.get("array_depth") or 0,  # REQ-1965
                "element_type": r.get("element_type"),
                "native_type": r.get("native_type"),
                "native_platform": r.get("native_platform"),
                "fake": r.get("fake"),  # REQ-1494
                "fake_stable": bool(r.get("fake_stable") or False),
                "fake_stable_version": r.get("fake_stable_version"),
                "synthetic_rule": r.get("synthetic_rule"),
                "fake_why": r.get("fake_why"),  # REQ-2094
                "embedding": bool(r["embedding"]),  # REQ-421
                "embedding_model": r["embedding_model"],  # REQ-423
            }
            for r in col_rows
        ]
        tables.append(table)
    return tables


async def fetch_relationships(
    conn: "Connection",
) -> list[dict]:  # REQ-155, REQ-194, REQ-351, REQ-399
    """Fetch relationships, each with its effective names as stored on its row
    (core.relationship_names): ``graphql_alias`` the pinned one or the stored GraphQL field,
    ``computed_cypher_alias`` the stored Cypher type where no alias is pinned."""
    rows = await conn.fetch(
        "SELECT r.id, r.source_table_id, r.target_table_id, r.source_column, "
        "r.target_column, r.cardinality, r.materialize, r.refresh_interval, "
        "r.target_function_name, r.function_arg, r.alias, r.graphql_alias, r.disable_cypher, "
        "r.same_entity, "  # REQ-2141
        # REQ-1586: the junction declaration travels with the edge — the compiler reads it to build
        # the two-hop join, and label_map reads via_label_source to name the exposed type.
        "r.via_table_id, r.via_source_column, r.via_target_column, r.via_type_column, "
        "r.via_type_value, r.via_label_source, "
        # REQ-2099: the target's parameters the edge feeds, read by the materialize stage.
        "r.parameters, "
        # REQ-351: the effective names, derived and stored once (core.relationship_names).
        "r.cypher_type, r.graphql_field, "
        "t.table_name AS target_table_name, "
        "v.table_name AS via_table_name "
        "FROM relationships r "
        "LEFT JOIN registered_tables t ON t.id = r.target_table_id "
        "LEFT JOIN registered_tables v ON v.id = r.via_table_id "
        "ORDER BY r.id"
    )
    result = []
    for r in rows:
        d = dict(r)
        d["graphql_alias"] = d["graphql_alias"] or d["graphql_field"]
        d["computed_cypher_alias"] = None if d["alias"] else d["cypher_type"]
        result.append(d)
    return result
