# Copyright (c) 2026 Kenneth Stott
# Canary: 8a2d47c1-93be-4f05-b7e6-51c0d9f3a284
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registry scan for a column's dependents (REQ-1484).

Advisory, not a gate: the answer is shown to the administrator before a save that renames a SQL alias
or drops a column, and they decide. Because it only ever informs, the scans over free SQL text
(metric expressions, RLS predicates, DQ contracts) match the column name as an identifier token
rather than resolving scope — over-reporting a candidate is the safe direction for a warning, and a
missed one is what the administrator would have got anyway.

Two reference styles, per :mod:`provisa.lineage.dependents`: artifacts authored against the EXPOSED
name (alias, else the snake_case default) break on a rename as well as a removal; artifacts storing
the PHYSICAL ``column_name`` break only on removal.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from sqlalchemy import or_, select

from provisa.compiler.naming import apply_sql_name, domain_to_sql_name
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import (
    glossary_term_refs,
    glossary_terms,
    metrics,
    registered_tables,
    relationships,
    rls_rules,
    tag_assignments,
    table_columns,
)
from provisa.lineage.dependents import Dependent, graph_dependents, relation_candidates

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _mentions(sql_text: str | None, name: str) -> bool:
    """Whether ``name`` appears in ``sql_text`` as a whole identifier token."""
    if not sql_text:
        return False
    return re.search(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", sql_text) is not None


async def _table_row(conn: "Connection", table_id: int) -> dict:
    res = await conn.execute_core(
        select(registered_tables).where(registered_tables.c.id == table_id)
    )
    row = res.fetchone()
    if row is None:
        raise ValueError(f"table {table_id} is not registered")
    return dict(row._mapping)


async def _exposed_names(conn: "Connection", table_id: int) -> dict[str, str]:
    """physical ``column_name`` → exposed name (``alias`` or the snake_case default).

    Mirrors ``schema_helpers.computed_sql_alias``, which is the only name the SQL and GraphQL
    surfaces ever show."""
    res = await conn.execute_core(
        select(table_columns.c.column_name, table_columns.c.alias).where(
            table_columns.c.table_id == table_id
        )
    )
    return {r.column_name: (r.alias or apply_sql_name(r.column_name)) for r in res.fetchall()}


async def _federation_graph(conn: "Connection"):
    """The merged column-level federation graph over every registered view definition.

    Same inputs as ``GET /admin/lineage/federation`` (``lineage_router._fetch_view_rows`` /
    ``_registry_views``), read here directly off the connection the caller already holds."""
    from provisa.api.app import state
    from provisa.lineage.merge import build_federation_graph_incremental

    res = await conn.execute_core(
        select(
            registered_tables.c.domain_id,
            registered_tables.c.table_name,
            registered_tables.c.view_sql,
        ).where(
            registered_tables.c.source_id == DERIVED_SOURCE_ID,
            registered_tables.c.view_sql.is_not(None),
        )
    )
    from provisa.api.admin.lineage_router import _registry_views

    views, mats = _registry_views(
        [dict(r._mapping) for r in res.fetchall()], getattr(state, "mv_registry", None)
    )
    commands = getattr(state, "tracked_functions", None) or {}
    merged = build_federation_graph_incremental(
        views, commands=commands, materialized_relations=mats
    )
    return merged.graph


async def _exposed_name_dependents(
    conn: "Connection", row: dict, exposed: str, graph
) -> list[Dependent]:
    """Artifacts authored against the column's exposed name — they break on rename OR removal."""
    table_sql = apply_sql_name(row["alias"] or row["table_name"])
    domain_sql = domain_to_sql_name(row["domain_id"]) if row["domain_id"] else ""
    out = graph_dependents(graph, relation_candidates(domain_sql, table_sql), exposed)

    mres = await conn.execute_core(select(metrics.c.name, metrics.c.expression))
    out += [
        Dependent("metric", r.name, f"expression references {exposed}", "rename")
        for r in mres.fetchall()
        if _mentions(r.expression, exposed)
    ]

    rres = await conn.execute_core(
        select(rls_rules.c.role_id, rls_rules.c.filter_expr).where(
            or_(
                rls_rules.c.table_id == row["id"],
                rls_rules.c.domain_id == row["domain_id"],
            )
        )
    )
    from provisa.encryption import encryption_service

    out += [
        Dependent("rls", f"{r.role_id}", f"filter references {exposed}", "rename")
        for r in rres.fetchall()
        if _mentions(encryption_service().decrypt(bytes(r.filter_expr)).decode("utf-8"), exposed)
    ]

    dres = await conn.execute_core(
        select(registered_tables.c.table_name, registered_tables.c.dq_contract).where(
            registered_tables.c.dq_contract.is_not(None)
        )
    )
    out += [
        Dependent("dq_contract", r.table_name, f"contract references {exposed}", "rename")
        for r in dres.fetchall()
        if _mentions(r.dq_contract, exposed)
    ]

    vres = await conn.execute_core(
        select(registered_tables.c.table_name, registered_tables.c.view_metrics).where(
            registered_tables.c.view_metrics.is_not(None)
        )
    )
    for r in vres.fetchall():
        spec = r.view_metrics or {}
        refs = list(spec.get("dimensions") or []) + list(spec.get("filters") or [])
        if any(_mentions(ref, exposed) for ref in refs):
            out.append(
                Dependent("metric_view", r.table_name, f"grain references {exposed}", "rename")
            )

    # The table's own MV row identity names its OUTPUT columns, i.e. exposed names.
    for field, label in (("mv_primary_key", "MV primary key"), ("mv_bitemporal_key", "MV key")):
        if exposed in (row.get(field) or []):
            out.append(Dependent("mv_key", row["table_name"], label, "rename"))
    return out


async def _physical_name_dependents(
    conn: "Connection", row: dict, column_name: str
) -> list[Dependent]:
    """Artifacts storing the physical ``column_name`` — they break only on removal."""
    out: list[Dependent] = []
    tid = row["id"]

    rres = await conn.execute_core(
        select(relationships.c.id, relationships.c.source_column, relationships.c.target_column)
        .where(
            or_(
                relationships.c.source_table_id == tid,
                relationships.c.target_table_id == tid,
            )
        )
    )
    for r in rres.fetchall():
        if column_name in (r.source_column, r.target_column):
            out.append(Dependent("relationship", r.id, "join column", "remove"))

    gres = await conn.execute_core(
        select(glossary_terms.c.name)
        .select_from(
            glossary_term_refs.join(
                glossary_terms, glossary_term_refs.c.term_id == glossary_terms.c.id
            )
        )
        .where(
            glossary_term_refs.c.table_id == tid,
            glossary_term_refs.c.column_name == column_name,
        )
    )
    out += [Dependent("glossary", r.name, "bound term", "remove") for r in gres.fetchall()]

    tres = await conn.execute_core(
        select(tag_assignments.c.tag_id).where(
            tag_assignments.c.table_id == tid,
            tag_assignments.c.column_name == column_name,
        )
    )
    out += [Dependent("tag", r.tag_id, "column tag", "remove") for r in tres.fetchall()]

    if row.get("watermark_column") == column_name:
        out.append(Dependent("watermark", row["table_name"], "watermark column", "remove"))
    if any(p.get("column") == column_name for p in (row.get("column_presets") or [])):
        out.append(Dependent("column_preset", row["table_name"], "preset target", "remove"))
    return out


async def dependents_for(
    conn: "Connection", table_id: int, *, renamed: list[str], removed: list[str]
) -> dict[str, list[Dependent]]:
    """physical column name → the artifacts a pending edit would break (REQ-1484).

    ``renamed`` names columns whose SQL alias is changing (their CURRENT exposed name is what
    dependents were authored against); ``removed`` names columns being dropped, which breaks both
    reference styles."""
    row = await _table_row(conn, table_id)
    exposed = await _exposed_names(conn, table_id)
    graph = await _federation_graph(conn)

    out: dict[str, list[Dependent]] = {}
    for name in dict.fromkeys([*renamed, *removed]):
        if name not in exposed:
            raise ValueError(f"column {name!r} is not a column of table {table_id}")
        found = await _exposed_name_dependents(conn, row, exposed[name], graph)
        if name in removed:
            found += await _physical_name_dependents(conn, row, name)
        if found:
            out[name] = found
    return out
