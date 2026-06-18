# Copyright (c) 2026 Kenneth Stott
# Canary: 2ae8ef6d-2550-4cb3-bd42-e938c6f76e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin GraphQL schema — queries + mutations for all config entities."""

from __future__ import annotations

import logging
from typing import Optional, cast

import asyncpg
import strawberry
from strawberry.types.info import Info as StrawberryInfo

from provisa.compiler.naming import source_to_catalog
from provisa.api.admin._config_io import config_path as _config_path, read_config
from provisa.api.admin.db_queries import derive_graphql_alias as _derive_graphql_alias_fn
from provisa.cypher.label_map import _to_rel_type as _to_cypher_rel_type
from provisa.core.config_loader import _normalize_op_id
from provisa.api.admin.types import (
    AvailableColumnType,
    AvailableTableType,
    CacheStatsType,
    ColumnAliasType,
    ColumnPresetType,
    CompileQueryInput,
    CompileQueryResult,
    DomainInput,
    DomainType,
    EnforcementType,
    MutationResult,
    MVType,
    RegisteredTableType,
    RelationshipInput,
    RelationshipType,
    RLSRuleInput,
    RLSRuleType,
    RoleInput,
    RoleType,
    ScheduledTaskType,
    SourceInput,
    SourceType,
    SystemHealthType,
    TableColumnType,
    TableInput,
)


async def _get_pool() -> asyncpg.Pool:
    from provisa.api.app import state

    assert state.pg_pool is not None
    return state.pg_pool


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
        row = await conn.fetchrow("SELECT type, path FROM sources WHERE id = $1", source_id)
    if not row or row["type"] != "openapi" or not row["path"]:
        return False
    try:
        from provisa.core.secrets import resolve_secrets as _resolve_secrets
        from provisa.openapi.loader import load_spec

        resolved_path = _resolve_secrets(row["path"])
        spec = load_spec(resolved_path)
        servers = spec.get("servers", [])
        base_url = servers[0].get("url", "") if servers else ""
        if base_url and not base_url.startswith(("http://", "https://")) and resolved_path.startswith(("http://", "https://")):
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


async def _govdata_columns(
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
        row = await _conn.fetchrow("SELECT username FROM sources WHERE id = $1", source_id)
    api_key = _resolve_secrets((row["username"] or "") if row else "")

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


def _source_from_row(row) -> SourceType:
    return SourceType(
        id=row["id"],
        type=row["type"],
        host=row["host"],
        port=row["port"],
        database=row["database"],
        username=row["username"],
        dialect=row["dialect"],
        cache_enabled=row.get("cache_enabled", True),
        cache_ttl=row.get("cache_ttl"),
        gql_naming_convention=row.get("gql_naming_convention"),
        path=row.get("path"),
        allowed_domains=list(row.get("allowed_domains") or []),
        description=row.get("description") or "",
    )


def _domain_from_row(row) -> DomainType:
    return DomainType(
        id=row["id"], description=row["description"], graphql_alias=row["graphql_alias"]
    )


def _role_from_row(row) -> RoleType:
    return RoleType(
        id=row["id"],
        capabilities=list(row["capabilities"]),
        domain_access=list(row["domain_access"]),
    )


def _derive_graphql_alias(
    target_table_name: str, cardinality: str, _alias: str | None, convention: str = "apollo_graphql"
) -> str | None:
    return _derive_graphql_alias_fn(target_table_name, cardinality, convention)


def _rel_from_row(row, convention: str = "apollo_graphql") -> RelationshipType:
    cardinality = row["cardinality"]
    target_table_name = row.get("target_table_name") or ""
    source_column = row.get("source_column") or ""
    alias = row.get("alias")
    persisted_graphql_alias = row.get("graphql_alias") or None
    graphql_alias = persisted_graphql_alias or _derive_graphql_alias(
        target_table_name, cardinality, alias, convention
    )
    computed_cypher_alias = None if alias else _to_cypher_rel_type(graphql_alias or target_table_name or "", cardinality)
    return RelationshipType(
        id=row["id"],
        source_table_id=row["source_table_id"],
        target_table_id=row.get("target_table_id"),
        source_table_name=row.get("source_table_name", ""),
        source_domain_id=row.get("source_domain_id") or "",
        target_table_name=target_table_name,
        source_column=source_column,
        target_column=row.get("target_column"),
        cardinality=cardinality,
        materialize=row.get("materialize", False),
        refresh_interval=row.get("refresh_interval", 300),
        target_function_name=row.get("target_function_name"),
        function_arg=row.get("function_arg"),
        alias=alias,
        graphql_alias=graphql_alias,
        computed_cypher_alias=computed_cypher_alias,
        disable_cypher=row.get("disable_cypher", False),
    )


def _rls_from_row(row) -> RLSRuleType:
    return RLSRuleType(
        id=row["id"],
        table_id=row["table_id"],
        domain_id=row["domain_id"],
        role_id=row["role_id"],
        filter_expr=row["filter_expr"],
    )


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


async def _fetch_table_with_columns(conn, row, all_tables: list | None = None, user_can_deploy: bool = True) -> RegisteredTableType:
    col_rows = await conn.fetch(
        "SELECT id, column_name, visible_to, writable_by, unmasked_to, "
        "mask_type, mask_pattern, mask_replace, mask_value, mask_precision, "
        "alias, description, data_type, native_filter_type, is_primary_key, is_foreign_key, is_alternate_key, scope "
        "FROM table_columns WHERE table_id = $1 ORDER BY id",
        row["id"],
    )
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
    if user_can_deploy and row["source_id"] == "__provisa__" and view_sql and all_tables is not None:
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
        gql_naming_convention=row.get("gql_naming_convention"),
        watermark_column=row.get("watermark_column"),
        columns=columns,
        column_presets=presets,
        api_endpoint=api_endpoint,
        view_sql=view_sql,
        materialize=bool(row.get("materialize", False)),
        mv_refresh_interval=int(row.get("mv_refresh_interval") or 300),
        data_product=bool(row.get("data_product", False)),
        can_deploy_to_db=can_deploy,
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
        import logging as _logging
        from provisa.file_source.pg_migrate import migrate_sqlite_table, record_mtime

        _log = _logging.getLogger(__name__)
        try:
            await migrate_sqlite_table(src_row["path"], table_name, conn, schema_name, table_name)
            if table_id is not None:
                await record_mtime(table_id, src_row["path"], conn)
        except Exception as _e:
            _log.warning("SQLite → PG migration failed for %s.%s: %s", source_id, table_name, _e)


def _build_column_models(columns: list) -> list:
    from provisa.core.models import Column as ColumnModel

    return [
        ColumnModel(
            name=c.name,
            visible_to=c.visible_to,
            writable_by=c.writable_by,
            unmasked_to=c.unmasked_to,
            mask_type=c.mask_type,
            mask_pattern=c.mask_pattern,
            mask_replace=c.mask_replace,
            mask_value=c.mask_value,
            mask_precision=c.mask_precision,
            alias=c.alias,
            description=c.description,
            native_filter_type=c.native_filter_type,
            is_primary_key=c.is_primary_key,
            is_foreign_key=c.is_foreign_key,
            is_alternate_key=c.is_alternate_key,
            scope=getattr(c, "scope", "domain"),
        )
        for c in columns
    ]


async def _introspect_view_columns(conn, view_sql: str, default_roles: list[str]) -> list:
    """Derive a view's columns from its SQL when the caller supplies none.

    Output column names come from the SELECT projection (SQLGlot). Each column's
    data_type is resolved from the stored columns of the registered tables the view
    references (by name match), falling back to varchar for expressions/aggregates the
    type can't be traced to. visible_to defaults to all roles. This makes a view's
    schema self-describing — the view SQL is the source of truth for its columns.
    """
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.core.models import Column as ColumnModel

    try:
        tree = sqlglot.parse_one(view_sql, read="postgres")
    except Exception:
        return []
    output_names = list(getattr(tree, "named_selects", []) or [])
    if not output_names:
        return []

    # Resolve types from the registered tables referenced in the view (by table name/alias).
    ref_names = {t.name for t in tree.find_all(exp.Table) if t.name}
    type_map: dict[str, str] = {}
    if ref_names:
        rows = await conn.fetch(
            """SELECT tc.column_name, tc.data_type
               FROM table_columns tc JOIN registered_tables rt ON rt.id = tc.table_id
               WHERE (rt.table_name = ANY($1::text[]) OR rt.alias = ANY($1::text[]))
                 AND tc.data_type IS NOT NULL""",
            list(ref_names),
        )
        for r in rows:
            type_map.setdefault(r["column_name"], r["data_type"])

    return [
        ColumnModel(name=n, data_type=type_map.get(n, "varchar"), visible_to=list(default_roles))
        for n in output_names
    ]


async def _domain_table_conflict(
    conn, domain_id: str, table_name: str, source_id: str, schema_name: str, alias: str | None = None
) -> str | None:
    """Return an error message if the effective name (alias or table_name) is already
    registered in the domain from a DIFFERENT physical table.

    Re-registering the same physical table (same source+schema) is allowed (upsert).
    Providing an alias that differs from a conflicting table_name resolves the conflict."""
    effective_name = alias or table_name
    row = await conn.fetchrow(
        "SELECT source_id, schema_name FROM registered_tables "
        "WHERE domain_id = $1 AND COALESCE(alias, table_name) = $2 "
        "AND NOT (source_id = $3 AND schema_name = $4) LIMIT 1",
        domain_id,
        effective_name,
        source_id,
        schema_name,
    )
    if row:
        return (
            f"Name {effective_name!r} is already used in domain {domain_id!r} "
            f"from {row['source_id']}.{row['schema_name']} — effective name (alias or table_name) must be unique."
        )
    return None


def _normalize_dataset_name(name: str) -> str:
    """Snake-case + lowercase normalization for dataset ownership comparison (REQ-433)."""
    from provisa.compiler.naming import apply_sql_name

    return apply_sql_name(name, "snake").lower()


async def _dataset_ownership_conflict(
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
    rows = await conn.fetch(
        "SELECT domain_id, table_name FROM registered_tables WHERE source_id = $1",
        source_id,
    )
    for r in rows:
        if _normalize_dataset_name(r["table_name"]) == norm and r["domain_id"] != target_domain:
            return (
                f"Table {table_name!r} on source {source_id!r} is already claimed by "
                f"domain {r['domain_id']!r} (first-come ownership)."
            )
    return None


async def _ensure_view_column_types(conn, view_sql: str, columns: list) -> list:
    """Fill any null/empty data_type on caller-supplied view columns.

    The admin UI snapshots a view's columns by running its SQL; a column whose type
    can't be traced (e.g. it references a source not yet introspected) arrives with
    data_type=None. introspect_tables requires every SQL-catalog column to have a
    type, so resolve nulls the same way _introspect_view_columns does — from the
    referenced tables, else varchar — so a view can never be persisted schema-broken.
    """
    if not any(getattr(c, "data_type", None) in (None, "") for c in columns):
        return columns
    import sqlglot
    import sqlglot.expressions as exp

    try:
        tree = sqlglot.parse_one(view_sql, read="postgres")
    except Exception:
        tree = None
    type_map: dict[str, str] = {}
    ref_names = {t.name for t in tree.find_all(exp.Table) if t.name} if tree else set()
    if ref_names:
        rows = await conn.fetch(
            """SELECT tc.column_name, tc.data_type
               FROM table_columns tc JOIN registered_tables rt ON rt.id = tc.table_id
               WHERE (rt.table_name = ANY($1::text[]) OR rt.alias = ANY($1::text[]))
                 AND tc.data_type IS NOT NULL""",
            list(ref_names),
        )
        for r in rows:
            type_map.setdefault(r["column_name"], r["data_type"])
    for c in columns:
        if getattr(c, "data_type", None) in (None, ""):
            c.data_type = type_map.get(c.name, "varchar")
    return columns


@strawberry.type
class Query:
    @strawberry.field
    async def schema_version(self) -> str:
        """Returns SHA256 hash of current schema state for cache validation."""
        import hashlib
        import json

        pool = await _get_pool()
        async with pool.acquire() as conn:
            domains = await conn.fetch(
                "SELECT id, description, graphql_alias FROM domains ORDER BY id"
            )
            tables = await conn.fetch("SELECT id FROM registered_tables ORDER BY id")
            rels = await conn.fetch(
                "SELECT id FROM relationships WHERE id NOT LIKE 'gql_auto__%' ORDER BY id"
            )

        # Hash the schema state
        schema_data = {
            "domains": [dict(d) for d in domains],
            "tables": [dict(t) for t in tables],
            "relationships": [dict(r) for r in rels],
        }
        schema_json = json.dumps(schema_data, sort_keys=True, default=str)
        version_hash = hashlib.sha256(schema_json.encode()).hexdigest()
        return version_hash

    @strawberry.field
    async def sources(self) -> list[SourceType]:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM sources ORDER BY id")
            return [_source_from_row(r) for r in rows]

    @strawberry.field
    async def source(self, id: str) -> Optional[SourceType]:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM sources WHERE id = $1", id)
            return _source_from_row(row) if row else None

    @strawberry.field
    async def domains(self) -> list[DomainType]:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM domains WHERE id != '' ORDER BY id")
            return [_domain_from_row(r) for r in rows]

    @strawberry.field
    async def tables(self, info: StrawberryInfo) -> list[RegisteredTableType]:
        from provisa.api.admin.capabilities import _identity_from_info, _resolved_capabilities
        from provisa.api.app import state as _state

        identity = _identity_from_info(info)
        if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
            user_can_deploy = True  # dev mode — no auth, allow all
        else:
            caps = _resolved_capabilities(identity, _state)
            user_can_deploy = bool(caps & {"table_registration", "admin", "superadmin"})

        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM registered_tables ORDER BY id")
            all_tables = await conn.fetch(
                """SELECT rt.source_id, rt.domain_id, rt.schema_name, rt.table_name, rt.alias
                   FROM registered_tables rt
                   WHERE rt.source_id != '__provisa__'""",
            )
            return [
                await _fetch_table_with_columns(conn, r, list(all_tables), user_can_deploy)
                for r in rows
            ]

    @strawberry.field
    async def relationships(self) -> list[RelationshipType]:
        from provisa.api.app import state

        convention = state.global_gql_naming_convention
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT r.*, "
                "st.table_name AS source_table_name, "
                "st.domain_id AS source_domain_id, "
                "tt.table_name AS target_table_name "
                "FROM relationships r "
                "JOIN registered_tables st ON r.source_table_id = st.id "
                "LEFT JOIN registered_tables tt ON r.target_table_id = tt.id "
                "WHERE r.id NOT LIKE 'gql_auto__%' "
                "AND r.id NOT LIKE 'meta:%' "
                "ORDER BY r.id"
            )
            return [_rel_from_row(r, convention) for r in rows]

    @strawberry.field
    async def roles(self) -> list[RoleType]:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM roles ORDER BY id")
            return [_role_from_row(r) for r in rows]

    @strawberry.field
    async def rls_rules(self) -> list[RLSRuleType]:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM rls_rules ORDER BY id")
            return [_rls_from_row(r) for r in rows]

    @strawberry.field
    async def available_schemas(self, source_id: str) -> list[str]:
        """List schemas available in a source."""
        from provisa.api.app import state
        from provisa.api.admin.introspect import native_schemas, _PROVISA_INTERNAL_SCHEMAS
        from provisa.core.models import SOURCE_TO_CONNECTOR

        source_type = state.source_types.get(source_id, "")
        if source_type == "openapi":
            await _ensure_openapi_spec(source_id)
        pool = await _get_pool()
        async with pool.acquire() as config_conn:
            result = await native_schemas(source_id, source_type, state.source_pools, config_conn)
        if result is not None:
            return [s for s in result if s not in _PROVISA_INTERNAL_SCHEMAS]
        # native_schemas returns None only for Trino-backed connector sources
        # that have no cheaper direct pool path. Use Trino for those.
        if source_type not in SOURCE_TO_CONNECTOR:
            return []
        catalog = source_to_catalog(source_id)
        hidden = {"information_schema", "pg_catalog"} | _PROVISA_INTERNAL_SCHEMAS
        try:
            assert state.trino_conn is not None
            cursor = state.trino_conn.cursor()
            cursor.execute(
                f'SELECT schema_name FROM "{catalog}".information_schema.schemata '
                f"ORDER BY schema_name"
            )
            return [row[0].lower() for row in cursor.fetchall() if row[0].lower() not in hidden]
        except Exception:
            return []

    @strawberry.field
    async def available_tables(
        self, source_id: str, schema_name: str = "public"
    ) -> list[AvailableTableType]:
        """List tables available in a source, using native introspection first.

        Returns table names with comments from the physical database.
        Filters out Provisa admin/platform tables.
        For OpenAPI sources, returns GET operations whose response is array or pagination wrapper.
        For GraphQL sources, returns query fields returning a list type.
        For gRPC sources, returns server-streaming RPCs or RPCs with repeated response fields.
        """
        from provisa.api.app import state
        from provisa.api.admin.introspect import native_tables

        source_type = state.source_types.get(source_id, "")
        if source_type == "openapi":
            await _ensure_openapi_spec(source_id)
        pool = await _get_pool()
        async with pool.acquire() as config_conn:
            try:
                result = await native_tables(
                    source_id,
                    source_type,
                    schema_name,
                    state.source_pools,
                    config_conn,
                    state,
                )
            except Exception:
                result = None
        if result is not None:
            return result
        # Trino fallback
        from provisa.api.admin.introspect import _PROVISA_INTERNAL_TABLES
        catalog = source_to_catalog(source_id)
        skip = _PROVISA_INTERNAL_TABLES if schema_name.lower() == "public" else frozenset()
        try:
            assert state.trino_conn is not None
            cursor = state.trino_conn.cursor()
            cursor.execute(
                f'SELECT table_name FROM "{catalog}".information_schema.tables '
                f"WHERE lower(table_schema) = lower('{schema_name}') "
                f"AND table_type = 'BASE TABLE' "
                f"ORDER BY table_name"
            )
            return [
                AvailableTableType(name=row[0], comment=None)
                for row in cursor.fetchall()
                if row[0].lower() not in skip
            ]
        except Exception:
            return []

    @strawberry.field
    async def available_functions(
        self, source_id: str, schema_name: str = "openapi"
    ) -> list[AvailableTableType]:
        """List available functions/mutations for a source.

        For OpenAPI sources: returns non-GET operations (POST/PUT/PATCH/DELETE).
        """
        from provisa.api.app import state

        if schema_name == "openapi" and await _ensure_openapi_spec(source_id):
            from provisa.openapi.mapper import parse_spec

            spec = state.openapi_specs[source_id]["spec"]
            _, mutations = parse_spec(spec)
            return [
                AvailableTableType(
                    name=m.operation_id,
                    comment=f"[{m.method}] {m.path}" + (f" — {m.summary}" if m.summary else ""),
                )
                for m in mutations
            ]
        return []

    @strawberry.field
    async def available_columns(
        self, source_id: str, schema_name: str, table_name: str
    ) -> list[str]:
        """List columns for a table in a source's Trino catalog."""
        from provisa.api.app import state

        source_type = state.source_types.get(source_id, "")
        if source_type == "govdata":
            cols = await _govdata_columns(source_id, schema_name, table_name, None)
            return [c.name for c in cols]
        catalog = source_to_catalog(source_id)
        try:
            assert state.trino_conn is not None
            cursor = state.trino_conn.cursor()
            cursor.execute(
                f'SELECT column_name FROM "{catalog}".information_schema.columns '
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_name = '{table_name}' "
                f"ORDER BY ordinal_position"
            )
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []

    @strawberry.field
    async def available_columns_metadata(
        self, source_id: str, schema_name: str, table_name: str
    ) -> list[AvailableColumnType]:
        """List columns with data types and comments from the physical database.

        For OpenAPI sources: derives columns from the operation's response schema + params.
        """
        from provisa.api.app import state

        source_type = state.source_types.get(source_id, "")
        if source_type == "govdata":
            return await _govdata_columns(source_id, schema_name, table_name, None)
        if schema_name == "openapi" and await _ensure_openapi_spec(source_id):
            from provisa.openapi.mapper import parse_spec
            from provisa.openapi.register import _schema_to_columns

            spec = state.openapi_specs[source_id]["spec"]
            queries, _ = parse_spec(spec)
            q = next((q for q in queries if q.operation_id == table_name), None)
            if q is None:
                return []
            cols = _schema_to_columns(q.response_schema)
            if not cols and not q.path_params and (q.response_schema or {}).get("additionalProperties"):
                cols = await _dynamic_openapi_columns(
                    state.openapi_specs[source_id]["base_url"], q
                )
            existing = {c["name"] for c in cols}
            for p in q.path_params:
                nf_name = f"_nf_{p['name']}"
                if nf_name not in existing:
                    cols.append(
                        {
                            "name": nf_name,
                            "type": p.get("type", "string"),
                            "native_filter_type": "path_param",
                        }
                    )
            for p in q.query_params:
                nf_name = f"_nf_{p['name']}"
                if nf_name not in existing:
                    cols.append(
                        {
                            "name": nf_name,
                            "type": p.get("type", "string"),
                            "native_filter_type": "query_param",
                        }
                    )
            return [
                AvailableColumnType(
                    name=c["name"],
                    data_type=c["type"],
                    comment=c.get("description"),
                    native_filter_type=c.get("native_filter_type"),
                )
                for c in cols
            ]
        catalog = source_to_catalog(source_id)
        try:
            from provisa.compiler.introspect import introspect_pk_columns

            assert state.trino_conn is not None
            pk_cols = introspect_pk_columns(state.trino_conn, catalog, schema_name, table_name)
            cursor = state.trino_conn.cursor()
            cursor.execute(
                f"SELECT column_name, data_type, comment "
                f'FROM "{catalog}".information_schema.columns '
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_name = '{table_name}' "
                f"ORDER BY ordinal_position"
            )
            return [
                AvailableColumnType(
                    name=row[0], data_type=row[1], comment=row[2], is_primary_key=row[0] in pk_cols
                )
                for row in cursor.fetchall()
            ]
        except Exception:
            return []

    @strawberry.field
    async def suggest_table_alias(
        self, table_name: str, domain_id: str, source_id: str
    ) -> str:
        """Return the alias to use when registering table_name in domain_id from source_id.

        Returns a plain snake_case alias when no conflict exists, or a source-prefixed
        alias (e.g. sqlite_b_orders) when the effective name already exists in the domain
        from a different source.
        """
        from provisa.compiler.naming import apply_convention

        pool = await _get_pool()
        async with pool.acquire() as conn:
            convention = await conn.fetchval(
                "SELECT gql_naming_convention FROM sources WHERE id = $1", source_id
            ) or "apollo_graphql"
            candidate: str = apply_convention(table_name, convention)
            conflict = await conn.fetchrow(
                "SELECT 1 FROM registered_tables "
                "WHERE domain_id = $1 AND COALESCE(alias, table_name) = $2 "
                "AND source_id != $3 LIMIT 1",
                domain_id, candidate, source_id,
            )
            if not conflict:
                return candidate
            # Prefix with source_id to disambiguate
            prefixed: str = apply_convention(f"{source_id}_{table_name}", convention)
            conflict2 = await conn.fetchrow(
                "SELECT 1 FROM registered_tables "
                "WHERE domain_id = $1 AND COALESCE(alias, table_name) = $2 "
                "AND source_id != $3 LIMIT 1",
                domain_id, prefixed, source_id,
            )
            if not conflict2:
                return prefixed
            # Last resort: add numeric suffix
            for i in range(1, 100):
                suffixed: str = apply_convention(f"{source_id}_{table_name}_{i}", convention)
                taken = await conn.fetchrow(
                    "SELECT 1 FROM registered_tables "
                    "WHERE domain_id = $1 AND COALESCE(alias, table_name) = $2 "
                    "AND source_id != $3 LIMIT 1",
                    domain_id, suffixed, source_id,
                )
                if not taken:
                    return suffixed
            return prefixed

    # ── Admin: Materialized Views ──

    @strawberry.field
    async def mv_list(self) -> list[MVType]:
        """List all materialized views with status."""
        from provisa.api.app import state

        return [
            MVType(
                id=mv.id,
                source_tables=mv.source_tables,
                target_table=mv.target_table or "",
                refresh_interval=mv.refresh_interval,
                enabled=mv.enabled,
                status=mv.status.value,
                last_refresh_at=mv.last_refresh_at,
                row_count=mv.row_count,
                last_error=mv.last_error,
            )
            for mv in state.mv_registry._mvs.values()
        ]

    # ── Admin: Cache Stats ──

    @strawberry.field
    async def cache_stats(self) -> CacheStatsType:
        """Return cache statistics."""
        from provisa.api.app import state
        from provisa.cache.store import RedisCacheStore

        store = state.response_cache_store
        if isinstance(store, RedisCacheStore):
            try:
                assert store._redis is not None
                info = await store._redis.info("stats")
                return CacheStatsType(
                    total_keys=await store._redis.dbsize(),
                    hit_count=info.get("keyspace_hits", 0),
                    miss_count=info.get("keyspace_misses", 0),
                    store_type="redis",
                )
            except Exception:
                pass
        return CacheStatsType(total_keys=0, hit_count=0, miss_count=0, store_type="noop")

    # ── Admin: System Health ──

    @strawberry.field
    async def system_health(self) -> SystemHealthType:
        """Return system component health status."""
        from provisa.api.app import state
        from provisa.cache.store import RedisCacheStore

        trino_ok = False
        trino_worker_count = 0
        trino_active_workers = 0
        if state.trino_conn is not None:
            try:
                cursor = state.trino_conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                trino_ok = True
                cursor.execute("SELECT state, count(*) FROM system.runtime.nodes GROUP BY state")
                for row in cursor.fetchall():
                    node_state, cnt = row[0], int(row[1])
                    trino_worker_count += cnt
                    if node_state == "active":
                        trino_active_workers = cnt
            except Exception:
                pass

        pg_size, pg_free = 0, 0
        if state.pg_pool is not None:
            pg_size = state.pg_pool.get_size()
            pg_free = state.pg_pool.get_idle_size()

        cache_ok = False
        if isinstance(state.response_cache_store, RedisCacheStore):
            try:
                assert state.response_cache_store._redis is not None
                await state.response_cache_store._redis.ping()
                cache_ok = True
            except Exception:
                pass

        flight_ok = state._flight_server is not None if hasattr(state, "_flight_server") else False

        return SystemHealthType(
            trino_connected=trino_ok,
            trino_worker_count=trino_worker_count,
            trino_active_workers=trino_active_workers,
            pg_pool_size=pg_size,
            pg_pool_free=pg_free,
            cache_connected=cache_ok,
            flight_server_running=flight_ok,
            mv_refresh_loop_running=hasattr(state, "_mv_refresh_task")
            and state._mv_refresh_task is not None,
        )

    # ── Admin: Scheduled Tasks ──

    @strawberry.field
    async def scheduled_tasks(self) -> list[ScheduledTaskType]:
        """List scheduled triggers from config with runtime state."""
        path = _config_path()
        if not path.exists():
            return []

        cfg = read_config()
        triggers = cfg.get("scheduled_triggers", [])

        # Try to get runtime info from the APScheduler instance
        from apscheduler.job import Job as _APSJob

        job_map: dict[str, _APSJob] = {}
        try:
            from provisa.api.app import state

            scheduler = getattr(state, "scheduler", None)
            if scheduler is not None:
                for job in scheduler.get_jobs():
                    job_map[job.id] = job
        except Exception:
            pass

        result = []
        for t in triggers:
            tid = t["id"]
            job = job_map.get(tid)
            next_run = None
            if job is not None and job.next_run_time is not None:
                next_run = job.next_run_time.isoformat()
            result.append(
                ScheduledTaskType(
                    id=tid,
                    name=t.get("name", tid),
                    cron_expression=t["cron"],
                    webhook_url=t.get("url"),
                    enabled=t.get("enabled", True),
                    last_run_at=None,
                    next_run_at=next_run,
                )
            )
        return result

    # ── AI: Generate table description ──

    @strawberry.field
    async def generate_table_description(self, table_id: str) -> str:
        """Use LLM to generate a description for a registered table."""
        import sys

        print(
            f"[DEBUG] generate_table_description called: table_id={table_id}",
            file=sys.stderr,
            flush=True,
        )
        try:
            pool = await _get_pool()
            tid = int(table_id)
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM registered_tables WHERE id = $1", tid)
                if row is None:
                    print(
                        f"[DEBUG] table {table_id} not found — save the view/table first",
                        file=sys.stderr,
                        flush=True,
                    )
                    return "Save the view first before generating descriptions"
                col_rows = await conn.fetch(
                    "SELECT column_name FROM table_columns WHERE table_id = $1 ORDER BY id", tid
                )
            table_name = row["table_name"]
            schema_name = row["schema_name"]
            source_id = row["source_id"]
            columns = [r["column_name"] for r in col_rows]
            prompt = (
                f"You are a data catalog assistant. Write a concise one-to-two sentence description "
                f"for a database table named '{table_name}' in schema '{schema_name}' "
                f"from source '{source_id}'. "
                f"Columns: {', '.join(columns)}. "
                f"Respond with only the description text, no preamble."
            )
            result = await _call_llm(prompt, "table_description", max_tokens=256)
            logging.getLogger(__name__).info(
                "generateTableDescription result: %s", result[:100] if result else "(empty)"
            )
            return result
        except Exception as e:
            logging.getLogger(__name__).exception("generateTableDescription failed: %s", e)
            return ""

    @strawberry.field
    async def generate_column_description(self, table_id: str, column_name: str) -> str:
        """Use LLM to generate a description for a single column."""
        import sys

        print(
            f"[DEBUG] generate_column_description called: table_id={table_id}, column_name={column_name}",
            file=sys.stderr,
            flush=True,
        )
        try:
            pool = await _get_pool()
            tid = int(table_id)
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM registered_tables WHERE id = $1", tid)
                if row is None:
                    print(
                        f"[DEBUG] table {table_id} not found — save the view/table first",
                        file=sys.stderr,
                        flush=True,
                    )
                    return "Save the view first before generating descriptions"
                col_rows = await conn.fetch(
                    "SELECT column_name FROM table_columns WHERE table_id = $1 ORDER BY id", tid
                )
            table_name = row["table_name"]
            schema_name = row["schema_name"]
            source_id = row["source_id"]
            all_columns = [r["column_name"] for r in col_rows]
            prompt = (
                f"You are a data catalog assistant. Write a concise one-sentence description "
                f"for the column '{column_name}' in table '{table_name}' (schema '{schema_name}', "
                f"source '{source_id}'). Other columns in this table: {', '.join(c for c in all_columns if c != column_name)}. "
                f"Respond with only the description text, no preamble."
            )
            result = await _call_llm(prompt, "column_description", max_tokens=128)
            print(
                f"[DEBUG] _call_llm result: {result[:100] if result else '(empty)'}",
                file=sys.stderr,
                flush=True,
            )
            return result
        except Exception as e:
            print(f"[DEBUG] Exception: {e}", file=sys.stderr, flush=True)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return ""


async def _validate_govdata_api_key(input: SourceInput) -> Optional[MutationResult]:
    """Return a failure MutationResult if the govdata API key is invalid, else None."""
    if not input.username:
        return MutationResult(success=False, message="AskAmerica API Key is required")
    import asyncio as _asyncio
    import logging as _vlog
    from provisa.core.models import GovDataSource as _GDS, GovDataSubject as _GDSubj
    from provisa.core.secrets import resolve_secrets as _rs_v
    from provisa.govdata.source import connect as _gd_v

    def _validate() -> None:
        gds = _GDS(
            id=input.id,
            subject=_GDSubj.all,
            govdata_schemas=["fec"],
            domain_id="default",
            api_key=_rs_v(input.username),
        )
        conn = _gd_v(gds)
        conn.getMetaData().getDatabaseProductName()

    try:
        loop = _asyncio.get_running_loop()
        await loop.run_in_executor(None, _validate)
    except Exception as _ve:
        _vlog.getLogger(__name__).warning("govdata API key validation failed: %s", _ve)
        return MutationResult(success=False, message=f"Invalid AskAmerica API Key: {_ve}")
    return None


async def _upsert_source_with_domains(pool, model, input: SourceInput) -> None:
    """Upsert the source model and update allowed_domains in the DB."""
    from provisa.core.repositories import source as source_repo

    async with pool.acquire() as conn:
        await source_repo.upsert(conn, model)
        _domains = [d for d in (input.allowed_domains or []) if d.strip()]
        if _domains:
            await conn.execute(
                "UPDATE sources SET allowed_domains = $1 WHERE id = $2",
                _domains,
                input.id,
            )


def _configure_govdata_env(input: SourceInput) -> None:
    """Set AWS environment variables required for govdata access."""
    import os as _os
    from provisa.core.secrets import resolve_secrets as _rs

    _os.environ.setdefault("AWS_ACCESS_KEY_ID", _rs(input.username))
    if input.password:
        _os.environ.setdefault("AWS_SECRET_ACCESS_KEY", _rs(input.password))
    if input.host:
        _os.environ["AWS_ENDPOINT_OVERRIDE"] = _rs(input.host)


async def _add_source_pool(state, input: SourceInput) -> None:
    """Register a direct connection pool for the source if a driver exists."""
    from provisa.executor.drivers.registry import has_driver
    from provisa.core.secrets import resolve_secrets

    if not has_driver(input.type):
        return
    # REQ-012: a failed direct connection must surface (no silent swallow), so the
    # caller can reject registration instead of persisting a dead source.
    await state.source_pools.add(
        source_id=input.id,
        source_type=input.type,
        host=resolve_secrets(input.host) if input.host else "localhost",
        port=input.port,
        database=input.database,
        user=input.username,
        password=resolve_secrets(input.password),
    )


def _create_trino_catalog(state, model, input: SourceInput) -> None:
    """Create a Trino catalog for the source (mirrors config_loader path)."""
    from provisa.core.catalog import create_catalog
    from provisa.core.secrets import resolve_secrets

    try:
        create_catalog(
            state.trino_conn,
            model,
            resolve_secrets(input.password) if input.password else "",
        )
    except Exception as _cat_err:
        logging.getLogger(__name__).warning(
            "Trino catalog creation for %r failed: %s", input.id, _cat_err
        )


async def _analyze_trino_tables(state, pool, model, input: SourceInput) -> None:
    """Fire ANALYZE on all registered tables for this source (errors swallowed)."""
    from provisa.core.catalog import analyze_source_tables

    class _TblRef:
        def __init__(self, source_id: str, schema_name: str, table_name: str) -> None:
            self.source_id = source_id
            self.schema_name = schema_name
            self.table_name = table_name

    async with pool.acquire() as _conn:
        rows = await _conn.fetch(
            "SELECT schema_name, table_name FROM registered_tables WHERE source_id = $1",
            input.id,
        )
    table_refs = [_TblRef(input.id, r["schema_name"], r["table_name"]) for r in rows]
    if table_refs:
        analyze_source_tables(state.trino_conn, model, table_refs)


def _prime_govdata_cache(input: SourceInput) -> None:
    """Schedule a background task to prime the govdata metadata cache."""
    import asyncio as _asyncio
    from provisa.core.models import GovDataSource as _GDS, GovDataSubject as _GDSubj
    from provisa.core.secrets import resolve_secrets as _rs2
    from provisa.govdata.source import prime_source as _prime

    _gds = _GDS(
        id=input.id,
        subject=_GDSubj.all,
        govdata_schemas=[s.strip().lower() for s in input.database.split(",") if s.strip()],
        domain_id="default",
        api_key=_rs2(input.username),
    )
    _schemas = [s.strip().lower() for s in input.database.split(",") if s.strip()]

    async def _prime_task() -> None:
        loop = _asyncio.get_running_loop()
        await loop.run_in_executor(None, _prime, _gds, _schemas)

    _asyncio.create_task(_prime_task())


def _fire_catalog_indexing(state, pool, input: SourceInput) -> None:
    """Schedule background catalog indexing for NL table search (REQ-464)."""
    import asyncio as _asyncio
    from provisa.discovery.catalog_cache import index_source as _index_source

    _asyncio.create_task(
        _index_source(
            input.id,
            pool,
            state.trino_conn,
            state.source_pools,
            state.source_types,
            state,
        )
    )


def _sync_view_mv(table_name: str, view_sql: str, refresh_interval: int) -> None:
    """Register or update an MVDefinition for a materialized user-defined view."""
    from provisa.api.app import state
    from provisa.mv.models import MVDefinition, MVStatus

    mv_id = f"view-{table_name}"
    existing = state.mv_registry.get(mv_id)
    mv = MVDefinition(
        id=mv_id,
        source_tables=[],
        target_catalog="postgresql",
        target_schema="mv_cache",
        target_table=f"mv_{table_name}",
        refresh_interval=refresh_interval,
        enabled=True,
        sql=view_sql,
        expose_in_sdl=False,
        status=existing.status if existing is not None else MVStatus.STALE,
    )
    state.mv_registry.register(mv)


def _remove_view_mv(table_name: str) -> None:
    """Remove a materialized view definition when materialize is toggled off."""
    from provisa.api.app import state

    state.mv_registry.unregister(f"view-{table_name}")


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def rebuild_schemas(self) -> MutationResult:
        """Rebuild in-memory schema from DB state. Useful after external DB changes."""
        await _rebuild_schemas()
        return MutationResult(success=True, message="Schemas rebuilt")

    @strawberry.mutation
    async def create_source(
        self, info: StrawberryInfo, input: SourceInput
    ) -> MutationResult:
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "source_registration")
        from provisa.core.models import Source as SourceModel, SourceType as SourceTypeEnum

        if input.type == "govdata":
            _err = await _validate_govdata_api_key(input)
            if _err is not None:
                return _err

        pool = await _get_pool()
        model = SourceModel(
            id=input.id,
            type=SourceTypeEnum(input.type),
            host=input.host,
            port=input.port,
            database=input.database,
            username=input.username,
            password=input.password,
            path=input.path,
            description=input.description,
        )
        from provisa.api.app import state

        # REQ-012: validate the direct connection before persisting; reject on failure
        # rather than leaving a half-registered source behind a swallowed error.
        try:
            await _add_source_pool(state, input)
        except Exception as _conn_err:
            return MutationResult(
                success=False,
                message=f"Source {input.id!r}: connection validation failed: {_conn_err}",
            )

        await _upsert_source_with_domains(pool, model, input)

        if input.type == "govdata" and input.username:
            _configure_govdata_env(input)

        _domains = [d for d in (input.allowed_domains or []) if d.strip()]
        if _domains:
            state.source_allowed_domains[input.id] = _domains
        state.source_types[input.id] = input.type
        state.source_dialects[input.id] = ""

        if state.trino_conn is not None:
            _create_trino_catalog(state, model, input)
            await _analyze_trino_tables(state, pool, model, input)

        if input.type == "govdata" and input.database and input.username:
            _prime_govdata_cache(input)

        _fire_catalog_indexing(state, pool, input)

        return MutationResult(success=True, message=f"Source {input.id!r} created")

    @strawberry.mutation
    async def update_source(
        self, info: StrawberryInfo, input: SourceInput
    ) -> MutationResult:
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "source_registration")
        from provisa.core.models import Source as SourceModel, SourceType as SourceTypeEnum
        from provisa.core.repositories import source as source_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            _conn = cast(asyncpg.Connection, conn)
            existing = await source_repo.get(_conn, input.id)
            if existing is None:
                return MutationResult(success=False, message=f"Source {input.id!r} not found")
            model = SourceModel(
                id=input.id,
                type=SourceTypeEnum(input.type),
                host=input.host,
                port=input.port,
                database=input.database,
                username=input.username,
                password=input.password,
                path=input.path,
                description=input.description,
            )
            await source_repo.upsert(_conn, model)
            if input.allowed_domains is not None:
                await conn.execute(
                    "UPDATE sources SET allowed_domains = $1 WHERE id = $2",
                    input.allowed_domains,
                    input.id,
                )

        if input.type == "govdata" and input.username:
            import os as _os
            from provisa.core.secrets import resolve_secrets as _rs

            _os.environ["AWS_ACCESS_KEY_ID"] = _rs(input.username)
            if input.password:
                _os.environ["AWS_SECRET_ACCESS_KEY"] = _rs(input.password)
            if input.host:
                _os.environ["AWS_ENDPOINT_OVERRIDE"] = _rs(input.host)

        from provisa.api.app import state
        from provisa.executor.drivers.registry import has_driver
        from provisa.core.secrets import resolve_secrets

        if has_driver(input.type):
            await state.source_pools.remove(input.id)
            try:
                await state.source_pools.add(
                    source_id=input.id,
                    source_type=input.type,
                    host=resolve_secrets(input.host) if input.host else "localhost",
                    port=input.port,
                    database=input.database,
                    user=input.username,
                    password=resolve_secrets(input.password),
                )
            except Exception as _pool_err:
                import logging as _log

                _log.getLogger(__name__).warning(
                    "Direct pool for %r failed: %s — Trino-routed queries still work.",
                    input.id,
                    _pool_err,
                )
        state.source_types[input.id] = input.type
        state.source_dialects[input.id] = ""
        if input.allowed_domains is not None:
            state.source_allowed_domains[input.id] = list(input.allowed_domains)

        # Invalidate and re-index catalog cache (REQ-464)
        import asyncio as _asyncio
        from provisa.discovery.catalog_cache import (
            invalidate_source as _invalidate,
            index_source as _index_source,
        )

        async def _reindex():
            await _invalidate(pool, input.id)
            await _index_source(
                input.id,
                pool,
                state.trino_conn,
                state.source_pools,
                state.source_types,
                state,
            )

        _asyncio.create_task(_reindex())

        return MutationResult(success=True, message=f"Source {input.id!r} updated")

    @strawberry.mutation
    async def rename_source(self, old_id: str, new_id: str) -> MutationResult:
        from provisa.core.repositories import source as source_repo

        if not new_id.strip():
            return MutationResult(success=False, message="New ID must not be empty")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            renamed = await source_repo.rename(cast(asyncpg.Connection, conn), old_id, new_id)
        if renamed:
            return MutationResult(success=True, message=f"Source renamed {old_id!r} → {new_id!r}")
        return MutationResult(success=False, message=f"Source {old_id!r} not found")

    @strawberry.mutation
    async def delete_source(self, id: str) -> MutationResult:
        from provisa.core.repositories import source as source_repo
        from provisa.api.app import state

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await source_repo.delete(cast(asyncpg.Connection, conn), id)
        if deleted:
            state.graphql_remote_sources.pop(id, None)
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Source {id!r} deleted")
        return MutationResult(success=False, message=f"Source {id!r} not found")

    @strawberry.mutation
    async def create_domain(self, input: DomainInput) -> MutationResult:
        from provisa.core.models import Domain as DomainModel
        from provisa.core.repositories import domain as domain_repo

        pool = await _get_pool()
        model = DomainModel(
            id=input.id, description=input.description, graphql_alias=input.graphql_alias or None
        )
        async with pool.acquire() as conn:
            await domain_repo.upsert(cast(asyncpg.Connection, conn), model)
        return MutationResult(success=True, message=f"Domain {input.id!r} created")

    @strawberry.mutation
    async def delete_domain(self, id: str) -> MutationResult:
        from provisa.core.repositories import domain as domain_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await domain_repo.delete(cast(asyncpg.Connection, conn), id)
        if deleted:
            return MutationResult(success=True, message=f"Domain {id!r} deleted")
        return MutationResult(success=False, message=f"Domain {id!r} not found")

    @strawberry.mutation
    async def create_role(self, input: RoleInput) -> MutationResult:
        from provisa.core.models import Role as RoleModel
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        model = RoleModel(
            id=input.id,
            capabilities=input.capabilities,
            domain_access=input.domain_access,
        )
        async with pool.acquire() as conn:
            await role_repo.upsert(cast(asyncpg.Connection, conn), model)
        return MutationResult(success=True, message=f"Role {input.id!r} created")

    @strawberry.mutation
    async def register_table(
        self, info: StrawberryInfo, input: TableInput
    ) -> MutationResult:
        import logging

        logging.getLogger(__name__).warning(
            "[DEBUG] register_table called: table_name=%s, source_id=%s, domain_id=%s",
            input.table_name,
            input.source_id,
            input.domain_id,
        )
        from provisa.api.admin.capabilities import require_capability

        if input.view_sql:
            # view registration: create_view or query_development suffice
            from provisa.api.admin.capabilities import _identity_from_info, _resolved_capabilities
            from provisa.api.app import state as _cap_state

            identity = _identity_from_info(info)
            if identity is not None and getattr(identity, "user_id", "anonymous") != "anonymous":
                caps = _resolved_capabilities(identity, _cap_state)
                if not (caps & {"create_view", "query_development", "admin", "superadmin"}):
                    raise PermissionError(
                        "Missing capability: 'create_view' or 'query_development'"
                    )
        else:
            require_capability(info, "table_registration", domain_id=input.domain_id)
        from provisa.core.models import (
            Table as TableModel,
        )
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        columns = _build_column_models(input.columns)
        # A view's columns are defined by its SQL — if the caller supplied none
        # (e.g. the SQL was edited after the column snapshot), derive them from the
        # view_sql so the view is never registered with an empty schema.
        if input.view_sql and not columns:
            async with pool.acquire() as _vc:
                _roles = [r["id"] for r in await _vc.fetch("SELECT id FROM roles")]
                columns = await _introspect_view_columns(
                    _vc, input.view_sql, _roles or ["admin"]
                )
        elif input.view_sql and columns:
            async with pool.acquire() as _vc:
                columns = await _ensure_view_column_types(_vc, input.view_sql, columns)
        alias = input.alias or None
        if not alias:
            from provisa.compiler.naming import apply_convention

            async with pool.acquire() as conn:
                src = await conn.fetchrow(
                    "SELECT gql_naming_convention FROM sources WHERE id = $1", input.source_id
                )
            convention = (src["gql_naming_convention"] if src else None) or "apollo_graphql"
            alias = apply_convention(input.table_name, convention)

        from provisa.core.models import ColumnPreset as ColumnPresetModel

        presets = [
            ColumnPresetModel(
                column=cp.column,
                source=cp.source,
                name=cp.name,
                value=cp.value,
                data_type=cp.data_type,
            )
            for cp in input.column_presets
        ]
        model = TableModel(
            source_id=input.source_id,
            domain_id=input.domain_id,
            schema_name=input.schema_name,
            table_name=input.table_name,
            alias=alias,
            description=input.description,
            columns=columns,
            watermark_column=input.watermark_column,
            column_presets=presets,
            view_sql=input.view_sql or None,
            materialize=input.materialize,
            mv_refresh_interval=input.mv_refresh_interval,
            data_product=input.data_product,
        )
        async with pool.acquire() as conn:
            _conn = cast(asyncpg.Connection, conn)
            _conflict = await _domain_table_conflict(
                _conn, model.domain_id, model.table_name, model.source_id, model.schema_name, alias
            )
            if _conflict:
                return MutationResult(success=False, message=_conflict)
            _owner_conflict = await _dataset_ownership_conflict(
                _conn, model.source_id, model.table_name, model.domain_id
            )
            if _owner_conflict:
                return MutationResult(success=False, message=_owner_conflict)
            if input.source_id == "__provisa__":
                await _conn.execute(
                    """
                    INSERT INTO sources (id, type, description)
                    VALUES ('__provisa__', 'trino', 'Provisa-managed virtual views — cross-source SQL views defined and published by the data team as governed data products')
                    ON CONFLICT (id) DO NOTHING
                    """
                )
            table_id = await table_repo.upsert(_conn, model)
            src_row = await _conn.fetchrow(
                "SELECT type, path FROM sources WHERE id = $1", input.source_id
            )
            await _maybe_migrate_sqlite(
                src_row, _conn, input.source_id, input.table_name, input.schema_name
            )
            if input.domain_id != "meta":
                meta_rt_id = await _conn.fetchval(
                    "SELECT id FROM registered_tables WHERE source_id = 'provisa-admin' AND domain_id = 'meta' AND table_name = 'registered_tables'"
                )
                if meta_rt_id:
                    await _conn.execute(
                        "INSERT INTO table_meta_links (source_table_id, target_table_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        table_id,
                        meta_rt_id,
                    )

            import os as _os

            if _os.environ.get("PROVISA_AUTO_TRACK_FK", "true").lower() not in ("0", "false", "no"):
                from provisa.discovery.fk_introspect import auto_register_fk_relationships
                from provisa.api.app import state as _state

                source_type = (src_row["type"] if src_row else None) or ""
                _naming_cfg = getattr(getattr(_state, "config", None), "naming", None)
                _v2_style = bool(getattr(_naming_cfg, "hasura_v2_relationship_style", False))
                fk_count = await auto_register_fk_relationships(
                    _state.source_pools,
                    source_type,
                    input.source_id,
                    input.schema_name,
                    input.table_name,
                    _conn,
                    hasura_v2_relationship_style=_v2_style,
                )
                if fk_count:
                    import logging as _logging

                    _logging.getLogger(__name__).info(
                        "Auto-tracked %d FK relationship(s) for %s.%s",
                        fk_count,
                        input.schema_name,
                        input.table_name,
                    )

        if input.view_sql and input.materialize:
            _sync_view_mv(input.table_name, input.view_sql, input.mv_refresh_interval)

        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Table {input.table_name!r} registered (id={table_id})",
        )

    @strawberry.mutation
    async def update_table(self, info: StrawberryInfo, input: TableInput) -> MutationResult:
        """Update an existing table's alias, description, and column metadata."""
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "table_registration", domain_id=input.domain_id)
        from provisa.core.models import (
            Table as TableModel,
        )
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        columns = _build_column_models(input.columns)
        # Re-derive a view's columns from its SQL when none are supplied (e.g. the SQL
        # was edited without re-running it), so an edit can't leave the view schema-less.
        if input.view_sql and not columns:
            async with pool.acquire() as _vc:
                _roles = [r["id"] for r in await _vc.fetch("SELECT id FROM roles")]
                columns = await _introspect_view_columns(
                    _vc, input.view_sql, _roles or ["admin"]
                )
        elif input.view_sql and columns:
            async with pool.acquire() as _vc:
                columns = await _ensure_view_column_types(_vc, input.view_sql, columns)
        from provisa.core.models import ColumnPreset as ColumnPresetModel

        presets = [
            ColumnPresetModel(
                column=cp.column,
                source=cp.source,
                name=cp.name,
                value=cp.value,
                data_type=cp.data_type,
            )
            for cp in input.column_presets
        ]
        model = TableModel(
            source_id=input.source_id,
            domain_id=input.domain_id,
            schema_name=input.schema_name,
            table_name=input.table_name,
            alias=input.alias,
            description=input.description,
            columns=columns,
            watermark_column=input.watermark_column,
            column_presets=presets,
            view_sql=input.view_sql or None,
            materialize=input.materialize,
            mv_refresh_interval=input.mv_refresh_interval,
            data_product=input.data_product,
        )
        async with pool.acquire() as conn:
            _conn = cast(asyncpg.Connection, conn)
            _conflict = await _domain_table_conflict(
                _conn, model.domain_id, model.table_name, model.source_id, model.schema_name, input.alias
            )
            if _conflict:
                return MutationResult(success=False, message=_conflict)
            _owner_conflict = await _dataset_ownership_conflict(
                _conn, model.source_id, model.table_name, model.domain_id
            )
            if _owner_conflict:
                return MutationResult(success=False, message=_owner_conflict)
            table_id = await table_repo.upsert(_conn, model)
            src_row = await _conn.fetchrow(
                "SELECT type, path FROM sources WHERE id = $1", input.source_id
            )
            await _maybe_migrate_sqlite(
                src_row, _conn, input.source_id, input.table_name, input.schema_name
            )
        if input.view_sql and input.materialize:
            _sync_view_mv(input.table_name, input.view_sql, input.mv_refresh_interval)
        elif not input.materialize:
            _remove_view_mv(input.table_name)
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Table {input.table_name!r} updated (id={table_id})",
        )

    @strawberry.mutation
    async def delete_table(self, id: int) -> MutationResult:
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await table_repo.delete(cast(asyncpg.Connection, conn), id)
        if deleted:
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Table {id} deleted")
        return MutationResult(success=False, message=f"Table {id} not found")

    @strawberry.mutation
    async def delete_role(self, id: str) -> MutationResult:
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await role_repo.delete(cast(asyncpg.Connection, conn), id)
        if deleted:
            return MutationResult(success=True, message=f"Role {id!r} deleted")
        return MutationResult(success=False, message=f"Role {id!r} not found")

    @strawberry.mutation
    async def upsert_rls_rule(self, input: RLSRuleInput) -> MutationResult:
        from provisa.core.models import RLSRule as RLSRuleModel
        from provisa.core.repositories import rls as rls_repo

        pool = await _get_pool()
        model = RLSRuleModel(
            table_id=input.table_id or None,
            domain_id=input.domain_id or None,
            role_id=input.role_id,
            filter=input.filter_expr,
        )
        try:
            async with pool.acquire() as conn:
                await rls_repo.upsert(cast(asyncpg.Connection, conn), model)
        except ValueError as e:
            return MutationResult(success=False, message=str(e))
        target = f"domain {input.domain_id!r}" if input.domain_id else f"table {input.table_id!r}"
        return MutationResult(
            success=True,
            message=f"RLS rule for {target} / role {input.role_id!r} saved",
        )

    @strawberry.mutation
    async def delete_rls_rule(
        self,
        role_id: str,
        table_id: Optional[int] = None,
        domain_id: Optional[str] = None,
    ) -> MutationResult:
        from provisa.core.repositories import rls as rls_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await rls_repo.delete(cast(asyncpg.Connection, conn), role_id, table_id=table_id, domain_id=domain_id)
        if deleted:
            return MutationResult(success=True, message="RLS rule deleted")
        return MutationResult(success=False, message="RLS rule not found")

    @strawberry.mutation
    async def upsert_relationship(
        self, info: StrawberryInfo, input: RelationshipInput
    ) -> MutationResult:
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "create_relationship")
        from provisa.core.models import Relationship as RelModel, Cardinality
        from provisa.core.repositories import relationship as rel_repo

        pool = await _get_pool()
        try:
            Cardinality(input.cardinality)
        except ValueError:
            return MutationResult(
                success=False,
                message=f"Invalid cardinality: {input.cardinality!r}",
            )
        model = RelModel(
            id=input.id,
            source_table_id=input.source_table_id,
            target_table_id=input.target_table_id or "",
            source_column=input.source_column,
            target_column=input.target_column or "",
            cardinality=Cardinality(input.cardinality),
            materialize=input.materialize,
            refresh_interval=input.refresh_interval,
            target_function_name=input.target_function_name or None,
            function_arg=input.function_arg or None,
            alias=input.alias or None,
            graphql_alias=getattr(input, "graphql_alias", None) or None,
            disable_cypher=getattr(input, "disable_cypher", False),
        )
        async with pool.acquire() as conn:
            _conn = cast(asyncpg.Connection, conn)
            await rel_repo.upsert(_conn, model)
            if input.record_candidate and not input.target_function_name:
                rel_row = await _conn.fetchrow(
                    "SELECT source_table_id, target_table_id FROM relationships WHERE id = $1",
                    input.id,
                )
                if rel_row and rel_row["target_table_id"] is not None:
                    await _conn.execute(
                        """
                        INSERT INTO relationship_candidates
                            (source_table_id, target_table_id, source_column, target_column,
                             cardinality, confidence, reasoning, suggested_name, scope, status)
                        VALUES ($1, $2, $3, $4, $5, 1.0, 'SQL modeling (admin)', $6, 'admin', 'accepted')
                        ON CONFLICT (source_table_id, source_column, target_table_id, target_column)
                        DO UPDATE SET status = 'accepted', confidence = 1.0,
                                      reasoning = 'SQL modeling (admin)'
                        """,
                        rel_row["source_table_id"],
                        rel_row["target_table_id"],
                        input.source_column,
                        input.target_column or None,
                        input.cardinality,
                        input.id,
                    )
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Relationship {input.id!r} saved",
        )

    @strawberry.mutation
    async def delete_relationship(self, id: str) -> MutationResult:
        from provisa.core.repositories import relationship as rel_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await rel_repo.delete(cast(asyncpg.Connection, conn), id)
        if deleted:
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Relationship {id!r} deleted")
        return MutationResult(success=False, message=f"Relationship {id!r} not found")

    # ── Admin: Cache Configuration ──

    @strawberry.mutation
    async def update_source_cache(
        self, source_id: str, cache_enabled: bool, cache_ttl: int | None = None
    ) -> MutationResult:
        """Update cache settings for a source."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE sources SET cache_enabled = $1, cache_ttl = $2 WHERE id = $3",
                cache_enabled,
                cache_ttl,
                source_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Source {source_id!r} not found")
        return MutationResult(
            success=True, message=f"Cache settings updated for source {source_id!r}"
        )

    @strawberry.mutation
    async def update_table_cache(
        self, table_id: int, cache_ttl: int | None = None
    ) -> MutationResult:
        """Update cache TTL for a registered table."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE registered_tables SET cache_ttl = $1 WHERE id = $2",
                cache_ttl,
                table_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Table {table_id} not found")
        return MutationResult(success=True, message=f"Cache TTL updated for table {table_id}")

    # ── Admin: Naming Convention ──

    @strawberry.mutation
    async def update_gql_naming_convention(self, convention: str) -> MutationResult:
        """Set the global naming convention and rebuild schemas for all roles."""
        from provisa.api.app import state

        from provisa.compiler import naming as _naming

        state.global_gql_naming_convention = convention
        _naming.configure(gql=convention, sql=state.global_sql_naming_convention)
        await _rebuild_schemas()
        return MutationResult(success=True, message=f"Naming convention set to {convention!r}")

    @strawberry.mutation
    async def update_source_naming(
        self, source_id: str, gql_naming_convention: Optional[str] = None
    ) -> MutationResult:
        """Update naming convention for a source."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE sources SET gql_naming_convention = $1 WHERE id = $2",
                gql_naming_convention,
                source_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Source {source_id!r} not found")
        await _rebuild_schemas()
        return MutationResult(
            success=True, message=f"Naming convention updated for source {source_id!r}"
        )

    @strawberry.mutation
    async def update_source_allowed_domains(
        self, source_id: str, allowed_domains: list[str]
    ) -> MutationResult:
        """Set the allowed domain list for a source (empty list = unrestricted)."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE sources SET allowed_domains = $1 WHERE id = $2",
                allowed_domains,
                source_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Source {source_id!r} not found")
        from provisa.api.app import state

        if allowed_domains:
            state.source_allowed_domains[source_id] = list(allowed_domains)
        else:
            state.source_allowed_domains.pop(source_id, None)
        await _rebuild_schemas()
        return MutationResult(
            success=True, message=f"Allowed domains updated for source {source_id!r}"
        )

    @strawberry.mutation
    async def update_table_naming(
        self, table_id: int, gql_naming_convention: Optional[str] = None
    ) -> MutationResult:
        """Update naming convention for a registered table."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE registered_tables SET gql_naming_convention = $1 WHERE id = $2",
                gql_naming_convention,
                table_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Table {table_id} not found")
        await _rebuild_schemas()
        return MutationResult(
            success=True, message=f"Naming convention updated for table {table_id}"
        )

    # ── Admin: MV Management ──

    @strawberry.mutation
    async def refresh_mv(self, mv_id: str) -> MutationResult:
        """Trigger a manual refresh of a materialized view."""
        from provisa.api.app import state

        mv = state.mv_registry.get(mv_id)
        if mv is None:
            return MutationResult(success=False, message=f"MV {mv_id!r} not found")
        try:
            from provisa.mv.refresh import refresh_mv

            assert state.trino_conn is not None
            await refresh_mv(state.trino_conn, mv, state.mv_registry)
            return MutationResult(success=True, message=f"MV {mv_id!r} refreshed")
        except Exception as e:
            return MutationResult(success=False, message=str(e))

    @strawberry.mutation
    async def toggle_mv(self, mv_id: str, enabled: bool) -> MutationResult:
        """Enable or disable a materialized view."""
        from provisa.api.app import state
        from provisa.mv.models import MVStatus

        mv = state.mv_registry.get(mv_id)
        if mv is None:
            return MutationResult(success=False, message=f"MV {mv_id!r} not found")
        mv.enabled = enabled
        if not enabled:
            mv.status = MVStatus.DISABLED
        elif mv.status == MVStatus.DISABLED:
            mv.status = MVStatus.STALE
        return MutationResult(
            success=True, message=f"MV {mv_id!r} {'enabled' if enabled else 'disabled'}"
        )

    # ── Admin: Cache Management ──

    @strawberry.mutation
    async def purge_cache(self) -> MutationResult:
        """Purge all cached query results."""
        from provisa.api.app import state

        try:
            count = await state.response_cache_store.invalidate_by_pattern("provisa:cache:*")
            return MutationResult(success=True, message=f"Purged {count} cache entries")
        except Exception as e:
            return MutationResult(success=False, message=str(e))

    @strawberry.mutation
    async def purge_cache_by_table(self, table_id: int) -> MutationResult:
        """Purge cached results for a specific table."""
        from provisa.api.app import state

        try:
            count = await state.response_cache_store.invalidate_by_table(table_id)
            return MutationResult(
                success=True, message=f"Purged {count} cache entries for table {table_id}"
            )
        except Exception as e:
            return MutationResult(success=False, message=str(e))

    @strawberry.mutation
    async def invalidate_file_source(self, table_id: int) -> MutationResult:
        """Force re-migration of a file-backed (SQLite) table into PG."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _conn = cast(asyncpg.Connection, conn)
            row = await _conn.fetchrow(
                """SELECT rt.table_name, rt.schema_name, s.type, s.path, s.id as source_id
                   FROM registered_tables rt
                   JOIN sources s ON s.id = rt.source_id
                   WHERE rt.id = $1""",
                table_id,
            )
            if not row:
                return MutationResult(success=False, message=f"Table {table_id} not found")
            if row["type"] != "sqlite":
                return MutationResult(
                    success=False, message=f"Source type {row['type']!r} is not sqlite"
                )
            from provisa.file_source.pg_migrate import migrate_sqlite_table, record_mtime

            try:
                await _conn.execute("DELETE FROM file_source_mtimes WHERE table_id = $1", table_id)
                await migrate_sqlite_table(
                    row["path"], row["table_name"], _conn, row["schema_name"], row["table_name"]
                )
                await record_mtime(table_id, row["path"], _conn)
                return MutationResult(
                    success=True, message=f"Re-migrated {row['source_id']}.{row['table_name']}"
                )
            except Exception as e:
                return MutationResult(success=False, message=str(e))

    # ── Admin: Scheduled Task Management ──

    @strawberry.mutation
    async def toggle_scheduled_task(self, task_id: str, enabled: bool) -> MutationResult:
        """Enable or disable a scheduled trigger in the config."""
        import yaml

        path = _config_path()
        if not path.exists():
            return MutationResult(success=False, message="Config file not found")

        cfg = read_config()
        triggers = cfg.get("scheduled_triggers", [])
        found = False
        for t in triggers:
            if t["id"] == task_id:
                t["enabled"] = enabled
                found = True
                break

        if not found:
            return MutationResult(success=False, message=f"Task {task_id!r} not found")

        with open(path, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

        return MutationResult(
            success=True,
            message=f"Task {task_id!r} {'enabled' if enabled else 'disabled'}",
        )

    async def refresh_source_statistics(self, source_id: str) -> MutationResult:
        """Run ANALYZE on all registered tables for a source (Phase AL).

        Triggers Trino to collect fresh table statistics, which improves the
        quality of join-order and broadcast decisions for federated queries.
        """
        from provisa.api.app import state

        if state.trino_conn is None:
            return MutationResult(success=False, message="Trino connection not available")

        pool = await _get_pool()
        if pool is None:
            return MutationResult(success=False, message="Database pool not available")

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT schema_name, table_name FROM registered_tables WHERE source_id = $1",
                source_id,
            )

        if not rows:
            return MutationResult(
                success=False,
                message=f"No tables registered for source {source_id!r}",
            )

        analyzed: list[str] = []
        errors: list[str] = []
        source_catalog = source_to_catalog(source_id)

        for row in rows:
            full_name = f"{source_catalog}.{row['schema_name']}.{row['table_name']}"
            try:
                cur = state.trino_conn.cursor()
                cur.execute(f"ANALYZE {full_name}")
                analyzed.append(full_name)
            except Exception as exc:
                errors.append(f"{full_name}: {exc}")

        if errors:
            return MutationResult(
                success=False,
                message=f"ANALYZE completed with errors. OK={len(analyzed)} errors={errors}",
            )
        return MutationResult(
            success=True,
            message=f"ANALYZE completed for {len(analyzed)} table(s) on source {source_id!r}",
        )

    @strawberry.mutation
    async def compile_query(self, input: CompileQueryInput) -> list[CompileQueryResult]:
        from provisa.api.admin import dev_queries

        variables = cast(dict, input.variables) if input.variables else None
        results = await dev_queries.compile_query(
            input.role,
            input.query,
            variables,
            flat_sql=input.flat_sql,
            flat_cypher=input.flat_cypher,
            node_only_cypher=input.node_only_cypher,
        )
        out = []
        for r in results:
            enf = r["enforcement"]
            out.append(
                CompileQueryResult(
                    sql=r["sql"],
                    semantic_sql=r["semantic_sql"],
                    trino_sql=r.get("trino_sql"),
                    direct_sql=r.get("direct_sql"),
                    route=r["route"],
                    route_reason=r["route_reason"],
                    sources=r["sources"],
                    root_field=r["root_field"],
                    canonical_field=r["canonical_field"],
                    column_aliases=[
                        ColumnAliasType(field_name=a["field_name"], column=a["column"])
                        for a in r["column_aliases"]
                    ],
                    enforcement=EnforcementType(
                        rls_filters_applied=enf.rls_filters_applied,
                        columns_excluded=enf.columns_excluded,
                        schema_scope=enf.schema_scope,
                        masking_applied=enf.masking_applied,
                        ceiling_applied=enf.ceiling_applied,
                        route=enf.route,
                    ),
                    optimizations=r["optimizations"],
                    warnings=r["warnings"],
                    compiled_cypher=r.get("compiled_cypher"),
                    cypher_error=r.get("cypher_error"),
                )
            )
        return out

    @strawberry.mutation
    async def deploy_view_to_db(self, info: StrawberryInfo, table_id: int) -> MutationResult:
        """Promote a virtual Provisa view to a real database view on its underlying native source."""
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "table_registration")

        from provisa.api.app import state
        from provisa.compiler.naming import domain_to_sql_name
        from provisa.transpiler.transpile import transpile

        pool = await _get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, source_id, domain_id, schema_name, table_name, alias, view_sql FROM registered_tables WHERE id = $1",
                table_id,
            )
        if not row:
            return MutationResult(success=False, message=f"Table {table_id} not found")
        if row["source_id"] != "__provisa__":
            return MutationResult(
                success=False,
                message="Table is not a virtual Provisa view (source_id != '__provisa__')",
            )
        if not row["view_sql"]:
            return MutationResult(success=False, message="Table has no view_sql")

        view_sql = row["view_sql"]
        view_name = row["alias"] or row["table_name"]

        # Fetch all non-provisa registered tables with domain_id, source info
        async with pool.acquire() as conn:
            all_tables = await conn.fetch(
                """SELECT rt.id, rt.source_id, rt.domain_id, rt.schema_name, rt.table_name, rt.alias,
                          s.type as source_type
                   FROM registered_tables rt
                   JOIN sources s ON s.id = rt.source_id
                   WHERE rt.source_id != '__provisa__'""",
            )

        # Build replacement dict: virtual ref → physical ref, tracking source_ids hit
        # Sort by length descending so longest match wins
        replacements: list[
            tuple[str, str, str, str]
        ] = []  # (virtual_ref, physical_ref, source_id, schema_name)
        for t in all_tables:
            domain_sql = domain_to_sql_name(t["domain_id"])
            alias_or_name = t["alias"] or t["table_name"]
            virtual_ref = f'"{domain_sql}"."{alias_or_name}"'
            physical_ref = f'"{t["schema_name"]}"."{t["table_name"]}"'
            replacements.append((virtual_ref, physical_ref, t["source_id"], t["schema_name"]))

        # Apply replacements (longest virtual_ref first), track which sources are hit
        physical_sql = view_sql
        hit_sources: dict[str, str] = {}  # source_id → schema_name
        for virtual_ref, physical_ref, source_id, schema_name in sorted(
            replacements, key=lambda x: len(x[0]), reverse=True
        ):
            if virtual_ref in physical_sql:
                physical_sql = physical_sql.replace(virtual_ref, physical_ref)
                hit_sources[source_id] = schema_name

        if not hit_sources:
            return MutationResult(success=False, message="no recognized table references")
        if len(hit_sources) > 1:
            return MutationResult(
                success=False,
                message=f"view spans multiple sources: {', '.join(sorted(hit_sources))}",
            )

        target_source_id = next(iter(hit_sources))
        target_schema = hit_sources[target_source_id]

        if not state.source_pools.has(target_source_id):
            return MutationResult(
                success=False, message=f"source {target_source_id!r} has no active connection"
            )

        dialect = state.source_pools.dialect_for(target_source_id) or "postgres"
        native_sql = transpile(physical_sql, dialect)

        ddl = f'CREATE OR REPLACE VIEW "{target_schema}"."{view_name}" AS {native_sql}'
        await state.source_pools.execute_ddl(target_source_id, ddl)

        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE registered_tables SET source_id=$1, schema_name=$2, view_sql=NULL WHERE id=$3",
                target_source_id,
                target_schema,
                table_id,
            )

        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"View '{view_name}' deployed to {target_source_id!r} schema '{target_schema}'",
        )


admin_schema = strawberry.Schema(query=Query, mutation=Mutation)
