# Copyright (c) 2026 Kenneth Stott
# Canary: 2ae8ef6d-2550-4cb3-bd42-e938c6f76e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin GraphQL schema helpers — pool, openapi/govdata columns, conflict checks.

Module-level helpers extracted from schema.py; called by the Query/Mutation
resolvers. state / _rebuild_schemas are imported lazily inside functions.
"""

# complexity-gate: allow-ble=4 reason="admin helpers relocated verbatim from schema.py; the broad excepts make openapi-column discovery / sqlite-migration / conflict-probe best-effort (fall back to a safe default, never fail the admin request)"

from __future__ import annotations


import logging
from typing import TYPE_CHECKING

from sqlalchemy import func, or_, select

from provisa.core.schema_org import (
    registered_tables,
    sources,
    table_columns,
)

if TYPE_CHECKING:
    from provisa.core.database import Database

from provisa.federation.strategy import engine_attaches
from provisa.core.config_loader import _normalize_op_id
from provisa.api.admin.types import (
    AvailableColumnType,
    ColumnPresetType,
    RegisteredTableType,
    UniqueConstraintType,
    TableColumnType,
)

from provisa.api.admin._row_mappers import _live_type_from_row

log = logging.getLogger(__name__)


async def _get_pool() -> "Database":
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


async def _dynamic_openapi_columns(base_url: str, query) -> list[dict]:
    """Call a no-input GET endpoint and infer columns from the response keys."""
    import httpx
    from provisa.openapi.register import _openapi_to_provisa_type

    url = base_url.rstrip("/") + query.path
    params = {p["name"]: "" for p in query.query_params}
    try:
        r = httpx.get(url, params=params, timeout=10, follow_redirects=True)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    if isinstance(data, list):
        sample = data[0] if data else {}
    elif isinstance(data, dict):
        sample = data
    else:
        return []
    if not isinstance(sample, dict):
        return []
    additional_type = (query.response_schema or {}).get("additionalProperties", {})
    value_type = _openapi_to_provisa_type(
        additional_type.get("type") if isinstance(additional_type, dict) else None
    )
    return [{"name": k, "type": value_type} for k in sample]


async def _ensure_openapi_spec(source_id: str) -> bool:
    """Lazy-load an OpenAPI spec into state from the DB source record if missing."""
    from provisa.api.app import state

    if getattr(state, "openapi_specs", {}).get(source_id):
        return True
    pool = await _get_pool()
    if not pool:
        return False
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(sources.c.type, sources.c.path).where(sources.c.id == source_id)
        )
        _r = result.fetchone()
    row = dict(_r._mapping) if _r is not None else None
    if not row or row["type"] != "openapi" or not row["path"]:
        return False
    try:
        from provisa.core.secrets import resolve_secrets as _resolve_secrets
        from provisa.openapi.loader import load_spec

        resolved_path = _resolve_secrets(row["path"])
        spec = load_spec(resolved_path)
        servers = spec.get("servers", [])
        base_url = servers[0].get("url", "") if servers else ""
        if (
            base_url
            and not base_url.startswith(("http://", "https://"))
            and resolved_path.startswith(("http://", "https://"))
        ):
            from urllib.parse import urljoin

            base_url = urljoin(resolved_path, base_url)
        if not hasattr(state, "openapi_specs"):
            state.openapi_specs = {}
        state.openapi_specs[source_id] = {
            "spec_path": row["path"],
            "spec": spec,
            "base_url": base_url,
            "domain_id": "",
            "auth_config": None,
            "cache_ttl": 300,
        }
        return True
    except Exception:
        return False


async def _govdata_columns(  # pyright: ignore[reportUnusedParameter]
    source_id: str,
    schema_name: str,
    table_name: str,
    _config_conn,  # noqa: ARG001
) -> list["AvailableColumnType"]:
    import asyncio as _asyncio
    import logging as _logging

    from provisa.core.models import GovDataSource, GovDataSubject
    from provisa.core.secrets import resolve_secrets as _resolve_secrets
    from provisa.govdata.source import (
        fetch_columns as _fetch_columns,
        fetch_primary_keys as _fetch_pks,
    )

    schema_lower = schema_name.lower()
    table_lower = table_name.lower()

    pool = await _get_pool()
    async with pool.acquire() as _conn:
        _res = await _conn.execute_core(select(sources.c.username).where(sources.c.id == source_id))
        row = _res.fetchone()
    api_key = _resolve_secrets((row.username or "") if row else "")

    gds = GovDataSource(
        id=source_id,
        subject=GovDataSubject.all,
        govdata_schemas=[schema_lower],
        domain_id="default",
        api_key=api_key,
    )

    try:
        loop = _asyncio.get_running_loop()
        cols_fut = loop.run_in_executor(None, _fetch_columns, gds, schema_lower, table_lower)
        pks_fut = loop.run_in_executor(None, _fetch_pks, gds, schema_lower, table_lower)
        rows, pk_cols = await _asyncio.gather(cols_fut, pks_fut)
        return [
            AvailableColumnType(
                name=col, data_type=typ, comment=rem or None, is_primary_key=col in pk_cols
            )
            for col, typ, rem in rows
        ]
    except Exception as _e:
        _logging.getLogger(__name__).error(
            "govdata _govdata_columns failed for %s.%s: %s",
            schema_name,
            table_name,
            _e,
            exc_info=True,
        )
        return []


async def _rebuild_schemas():
    import logging

    logging.getLogger(__name__).warning("[DEBUG] _rebuild_schemas called")
    from provisa.api.app import _rebuild_schemas as rebuild

    await rebuild()
    logging.getLogger(__name__).warning("[DEBUG] _rebuild_schemas completed")


def _compute_can_deploy_to_db(
    view_sql: str,
    all_tables: list,
) -> bool:
    """Return True iff view_sql references tables from exactly one source that has an active pool."""
    from provisa.api.app import state
    from provisa.compiler.naming import domain_to_sql_name

    replacements: list[tuple[str, str, str]] = []
    for t in all_tables:
        domain_sql = domain_to_sql_name(t["domain_id"])
        alias_or_name = t["alias"] or t["table_name"]
        virtual_ref = f'"{domain_sql}"."{alias_or_name}"'
        replacements.append((virtual_ref, t["source_id"], t["schema_name"]))

    hit_sources: dict[str, str] = {}
    for virtual_ref, source_id, schema_name in sorted(
        replacements, key=lambda x: len(x[0]), reverse=True
    ):
        if virtual_ref in view_sql:
            hit_sources[source_id] = schema_name

    if not hit_sources or len(hit_sources) != 1:
        return False

    target_source_id = next(iter(hit_sources))
    return state.source_pools.has(target_source_id)


async def _fetch_table_with_columns(
    conn, row, all_tables: list | None = None, user_can_deploy: bool = True
) -> RegisteredTableType:
    _col_res = await conn.execute_core(
        select(table_columns)
        .where(table_columns.c.table_id == row["id"])
        .order_by(table_columns.c.id)
    )
    col_rows = [dict(r._mapping) for r in _col_res.fetchall()]
    from provisa.compiler.naming import apply_sql_name

    columns = [
        TableColumnType(
            id=r["id"],
            column_name=r["column_name"],
            visible_to=list(r["visible_to"]),
            writable_by=list(r.get("writable_by") or []),
            unmasked_to=list(r.get("unmasked_to") or []),
            mask_type=r.get("mask_type"),
            mask_pattern=r.get("mask_pattern"),
            mask_replace=r.get("mask_replace"),
            mask_value=r.get("mask_value"),
            mask_precision=r.get("mask_precision"),
            alias=r.get("alias"),
            computed_sql_alias=r.get("alias") or apply_sql_name(r["column_name"]),
            description=r.get("description"),
            data_type=r.get("data_type"),
            native_filter_type=r.get("native_filter_type"),
            is_primary_key=bool(r.get("is_primary_key") or False),
            is_foreign_key=bool(r.get("is_foreign_key") or False),
            is_alternate_key=bool(r.get("is_alternate_key") or False),
            scope=r.get("scope") or "domain",
        )
        for r in col_rows
    ]
    presets = [
        ColumnPresetType(
            column=p["column"],
            source=p["source"],
            name=p.get("name"),
            value=p.get("value"),
            data_type=p.get("data_type"),
        )
        for p in (row.get("column_presets") or [])
    ]
    unique_constraints = [
        UniqueConstraintType(name=u["name"], columns=list(u["columns"]))
        for u in (row.get("unique_constraints") or [])
    ]  # REQ-1093

    api_endpoint = None
    if await _ensure_openapi_spec(row["source_id"]):
        try:
            from provisa.api.app import state
            from provisa.openapi.mapper import parse_spec

            spec_info = state.openapi_specs.get(row["source_id"], {})
            spec = spec_info.get("spec", {})
            base_url = spec_info.get("base_url", "")
            queries, _ = parse_spec(spec)

            table_name = row["table_name"]
            q = next(
                (
                    q
                    for q in queries
                    if _normalize_op_id(q.operation_id) == _normalize_op_id(table_name)
                ),
                None,
            )
            if q:
                api_endpoint = f"[{q.method.upper()}] {base_url.rstrip('/')}{q.path}"
        except Exception:
            pass

    view_sql = row.get("view_sql")
    can_deploy = False
    if (
        user_can_deploy
        and row["source_id"] == "__provisa__"
        and view_sql
        and all_tables is not None
    ):
        can_deploy = _compute_can_deploy_to_db(view_sql, all_tables)

    return RegisteredTableType(
        id=row["id"],
        source_id=row["source_id"],
        domain_id=row["domain_id"],
        schema_name=row["schema_name"],
        table_name=row["table_name"],
        alias=row.get("alias"),
        description=row.get("description"),
        cache_ttl=row.get("cache_ttl"),
        prefer_materialized=row.get("prefer_materialized"),
        load_protected=row.get("load_protected"),  # REQ-1141
        off_peak_window=row.get("off_peak_window"),  # REQ-1141
        off_peak_tz=row.get("off_peak_tz"),  # REQ-1141
        gql_naming_convention=row.get("gql_naming_convention"),
        watermark_column=row.get("watermark_column"),
        columns=columns,
        column_presets=presets,
        unique_constraints=unique_constraints,  # REQ-1093
        api_endpoint=api_endpoint,
        view_sql=view_sql,
        change_signal=row.get("change_signal"),
        probe_query=row.get("probe_query"),
        probe_type=row.get("probe_type"),
        materialize=bool(row.get("materialize", False)),
        mv_refresh_interval=int(row.get("mv_refresh_interval") or 300),
        mv_debounce_quiet=float(row.get("mv_debounce_quiet") or 0.0),  # REQ-963
        mv_debounce_max_delay=float(row.get("mv_debounce_max_delay") or 5.0),  # REQ-963
        mv_consistency=row.get("mv_consistency") or "shared",  # REQ-879
        mv_preprocess=row.get("mv_preprocess"),  # REQ-957
        data_product=bool(row.get("data_product", False)),
        enable_aggregates=bool(row.get("enable_aggregates", False)),
        enable_group_by=bool(row.get("enable_group_by", False)),
        can_deploy_to_db=can_deploy,
        live=_live_type_from_row(row.get("live")),
    )


async def _call_llm(prompt: str, operation: str, max_tokens: int = 256) -> str:
    from provisa.llm.client import ProviasLLMClient

    client = ProviasLLMClient(operation)
    return await client.complete(
        prompt, system="You are a data catalog assistant.", max_tokens=max_tokens
    )


async def _maybe_migrate_sqlite(
    src_row, conn, source_id: str, table_name: str, schema_name: str, table_id: int | None = None
) -> None:
    if src_row and src_row["type"] == "sqlite" and src_row["path"]:
        from provisa.api.app import state

        # An ATTACH engine (DuckDB) reads the sqlite file in place — never materialize it (REQ-947).
        if engine_attaches(getattr(state, "federation_engine", None), "sqlite"):
            return
        from provisa.file_source.pg_migrate import migrate_sqlite_table, record_mtime

        _log = logging.getLogger(__name__)
        try:
            await migrate_sqlite_table(src_row["path"], table_name, conn, schema_name, table_name)
            if table_id is not None:
                await record_mtime(table_id, src_row["path"], conn)
        except Exception as _e:
            _log.warning("SQLite → PG migration failed for %s.%s: %s", source_id, table_name, _e)


from provisa.api.admin._table_ops import (  # noqa: E402
    _build_column_models,  # noqa: F401  (re-export: tests + steps import from schema)
    _ensure_view_column_types,  # noqa: F401  (re-export: tests import from schema)
)


async def _domain_table_conflict(  # REQ-432
    conn,
    domain_id: str,
    table_name: str,
    source_id: str,
    schema_name: str,
    alias: str | None = None,
) -> str | None:
    """Return an error message if the effective name (alias or table_name) is already
    registered in the domain from a DIFFERENT physical table.

    Re-registering the same physical table (same source+schema) is allowed (upsert).
    Providing an alias that differs from a conflicting table_name resolves the conflict."""
    effective_name = alias or table_name
    _res = await conn.execute_core(
        select(registered_tables.c.source_id, registered_tables.c.schema_name)
        .where(
            registered_tables.c.domain_id == domain_id,
            func.coalesce(registered_tables.c.alias, registered_tables.c.table_name)
            == effective_name,
            or_(
                registered_tables.c.source_id != source_id,
                registered_tables.c.schema_name != schema_name,
            ),
        )
        .limit(1)
    )
    row = _res.fetchone()
    if row:
        return (
            f"Name {effective_name!r} is already used in domain {domain_id!r} "
            f"from {row.source_id}.{row.schema_name} — effective name (alias or table_name) must be unique."
        )
    return None


def _normalize_dataset_name(name: str) -> str:
    """Snake-case + lowercase normalization for dataset ownership comparison (REQ-433)."""
    from provisa.compiler.naming import apply_sql_name

    return apply_sql_name(name, "snake").lower()


async def _dataset_ownership_conflict(  # REQ-433
    conn, source_id: str, table_name: str, domain_id: str
) -> str | None:
    """Return an error if this dataset is already claimed by a DIFFERENT domain (REQ-433).

    First-come ownership: a physical dataset — identified by (source_id, normalized
    table name) — may be registered by only one domain. Re-registration by the owning
    domain is allowed. Virtual Provisa views (``__provisa__``) are exempt: they are not
    datasource claims and many domains legitimately share that source id.
    """
    if source_id == "__provisa__":
        return None
    from provisa.core import domain_policy

    target_domain = domain_policy.resolve_domain_id(domain_id)
    norm = _normalize_dataset_name(table_name)
    _res = await conn.execute_core(
        select(registered_tables.c.domain_id, registered_tables.c.table_name).where(
            registered_tables.c.source_id == source_id
        )
    )
    for r in _res.fetchall():
        if _normalize_dataset_name(r.table_name) == norm and r.domain_id != target_domain:
            return (
                f"Table {table_name!r} on source {source_id!r} is already claimed by "
                f"domain {r.domain_id!r} (first-come ownership)."
            )
    return None


# --- Creation-request queue (REQ-434/063/366) -------------------------------
