# Copyright (c) 2026 Kenneth Stott
# Canary: 3a97c1e0-4d2b-41f7-8c65-b20e96d7f418
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's privilege facts (REQ-2190): what has_table_privilege and has_column_privilege read that
no view can give, from the schema build a statement of that role is compiled against. A table the
role lists and is not served holds no privilege, so those functions answer false for it."""

# Requirements: REQ-2190, REQ-127

from __future__ import annotations

import logging
import time
from typing import Any

from provisa.compiler.naming import apply_sql_name
from provisa.security.rights import SQL_CATALOG_TABLES, Capability, has_capability
from provisa.transpiler.metadata_facts import CatalogFacts, catalog_facts, role_privileges
from provisa.transpiler.metadata_oids import metric_oid, table_oid

log = logging.getLogger(__name__)

#: SHOW STATS FOR's summary row holds the row count here.
_ROW_COUNT_COLUMN = 4


def _unserved(ctx: Any, raw_tables: list[dict]) -> list[tuple[int, str]]:
    """Each column of a table served to the role that it is not served, by its SQL name."""
    p2s: dict = getattr(ctx, "physical_to_sql", None) or {}
    served = {tm.table_id for tm in ctx.tables.values()}
    if not p2s:
        return []
    return [
        (table["id"], apply_sql_name(column["column_name"]))
        for table in raw_tables
        if table["id"] in served
        for column in table.get("columns") or ()
        if (table["id"], column["column_name"]) not in p2s
    ]


def role_facts(role_id: str, state: Any) -> CatalogFacts:
    """``role_id``'s facts, of the model as it stands."""
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return catalog_facts(role_id, {})
    raw_tables: list[dict] = state.schema_build_cache.get("tables") or []
    served = {
        tm.table_id
        for tm in ctx.tables.values()
        if (tm.domain_id, tm.original_table_name or tm.table_name) not in SQL_CATALOG_TABLES
    }
    write_grants = {
        table["id"]: {
            grant["operation"]: grant
            for grant in table.get("write_grants") or ()
            if grant["role"] == role_id
        }
        for table in raw_tables
    }
    write_ops = {table["id"]: frozenset(table.get("write_ops") or ()) for table in raw_tables}
    role = state.roles.get(role_id)
    can_write = role is not None and has_capability(role, Capability.WRITE)
    config = getattr(state, "config", None)
    metrics = getattr(config, "metrics", None)
    readable_metrics = sorted(
        m.name for m in (metrics if isinstance(metrics, list) else ()) if role_id in m.visible_to
    )
    return catalog_facts(
        role_id,
        role_privileges(served, write_grants, write_ops, can_write),
        _unserved(ctx, raw_tables),
        [metric_oid(name) for name in readable_metrics],
    )


#: A failed or slow estimate is paid at most once per role per this long (seconds).
_ROW_COUNT_TTL = 300
#: What a statement waits for the engine's statistics, in all.
_ROW_COUNT_BUDGET_S = 2.0
_row_counts: dict[tuple[str, str], tuple[float, dict[int, float]]] = {}


def _engine_row_counts(ctx: Any, engine_conn: Any) -> dict[int, float]:
    """Row count estimates by relation oid, through SHOW STATS FOR. An estimate is best effort: one
    the engine does not give stays 0, PostgreSQL's own answer before a table is analyzed."""
    if engine_conn is None:
        return {}
    result: dict[int, float] = {}
    deadline = time.monotonic() + _ROW_COUNT_BUDGET_S
    for tm in ctx.tables.values():
        if time.monotonic() >= deadline:
            break
        qualified = f'"{tm.catalog_name}"."{tm.schema_name}"."{tm.table_name}"'
        try:
            cursor = engine_conn.cursor()
            cursor.execute(f"SHOW STATS FOR {qualified}")
            for row in cursor.fetchall():
                if row[0] is None and len(row) > _ROW_COUNT_COLUMN and row[_ROW_COUNT_COLUMN]:
                    result[table_oid(tm.table_id)] = float(row[_ROW_COUNT_COLUMN])
        except Exception:  # noqa: BLE001 -- an estimate is best effort; 0 is PostgreSQL's own answer
            log.debug("no row count for %s", qualified, exc_info=True)
    return result


def row_counts(role_id: str, state: Any) -> dict[int, float]:
    """The role's row-count estimates, kept by org and role: a role's id is its org's."""
    from provisa.core.request_context import require_current_org

    key = (require_current_org(), role_id)
    now = time.monotonic()
    kept = _row_counts.get(key)
    if kept is not None and now - kept[0] < _ROW_COUNT_TTL:
        return kept[1]
    ctx = state.contexts.get(role_id)
    counts = _engine_row_counts(ctx, getattr(state, "engine_conn", None)) if ctx else {}
    _row_counts[key] = (now, counts)
    return counts
