# Copyright (c) 2026 Kenneth Stott
# Canary: 2ae8ef6d-2550-4cb3-bd42-e938c6f76e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""Admin GraphQL Query type — read-side resolvers for all config entities."""

from __future__ import annotations


import logging
from typing import TYPE_CHECKING, Optional, cast

import strawberry
from sqlalchemy import func, or_, select
from strawberry.types.info import Info as StrawberryInfo

from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    roles,
    sources,
    table_columns,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

from provisa.compiler.naming import source_to_catalog
from provisa.core.repositories import rls as rls_repo
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.api.admin._config_io import config_path as _config_path, read_config
from provisa.api.admin.types import (
    AvailableColumnType,
    AvailableTableType,
    CacheStatsType,
    CacheTableStatType,
    DomainType,
    HotTableStatType,
    MaterializeStoreInfoType,
    MVType,
    RegisteredTableType,
    RelationshipType,
    RLSRuleType,
    RoleType,
    ScheduledTaskType,
    SourceType,
    SystemHealthType,
)

from provisa.api.admin.schema_helpers import (
    _call_llm,
    _dynamic_openapi_columns,
    _ensure_openapi_spec,
    _fetch_table_with_columns,
    _get_pool,
    _govdata_columns,
)

_tracer = _get_tracer(__name__)


from provisa.api.admin.discovery_resilience import discovery_fallback  # noqa: E402
from provisa.api.admin._row_mappers import (  # noqa: E402
    _source_from_row,
    _domain_from_row,
    _role_from_row,
    _rel_from_row,
    _rls_from_row,
)
from provisa.api.admin.schema_common import (  # noqa: E402
    CreationRequestType,
    _resolve_admin_context,
)


@strawberry.type
class Query:  # REQ-021, REQ-042
    @strawberry.field
    async def creation_requests(
        self, info: StrawberryInfo
    ) -> list[CreationRequestType]:  # REQ-434, REQ-063  # pyright: ignore[reportUnusedParameter]
        """REQ-434/063: pending creation requests, for users holding a create capability."""
        import json as _json

        from provisa.core.repositories import creation_request as cr_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await cr_repo.list_pending(cast("Connection", conn))
        return [
            CreationRequestType(
                id=r["id"],
                request_type=r["request_type"],
                capability=r["capability"],
                requested_by=r.get("requested_by"),
                status=r["status"],
                rejection_reason=r.get("rejection_reason"),
                payload_json=_json.dumps(r["payload"]),
            )
            for r in rows
        ]

    @strawberry.field
    async def schema_version(self) -> str:
        """Returns SHA256 hash of current schema state for cache validation."""
        import hashlib
        import json

        pool = await _get_pool()
        async with pool.acquire() as conn:
            _dres = await conn.execute_core(
                select(domains.c.id, domains.c.description, domains.c.graphql_alias).order_by(
                    domains.c.id
                )
            )
            domain_rows = _dres.fetchall()
            _tres = await conn.execute_core(
                select(registered_tables.c.id).order_by(registered_tables.c.id)
            )
            table_rows = _tres.fetchall()
            _rres = await conn.execute_core(
                select(relationships.c.id)
                .where(relationships.c.id.not_like("gql_auto__%"))
                .order_by(relationships.c.id)
            )
            rel_rows = _rres.fetchall()

        # Hash the schema state
        schema_data = {
            "domains": [dict(d._mapping) for d in domain_rows],
            "tables": [dict(t._mapping) for t in table_rows],
            "relationships": [dict(r._mapping) for r in rel_rows],
        }
        schema_json = json.dumps(schema_data, sort_keys=True, default=str)
        version_hash = hashlib.sha256(schema_json.encode()).hexdigest()
        return version_hash

    @strawberry.field
    async def sources(self) -> list[SourceType]:  # REQ-012, REQ-013
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _res = await conn.execute_core(select(sources).order_by(sources.c.id))
            return [_source_from_row(dict(r._mapping)) for r in _res.fetchall()]

    @strawberry.field
    async def source(self, id: str) -> Optional[SourceType]:  # REQ-012, REQ-013
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _res = await conn.execute_core(select(sources).where(sources.c.id == id))
            row = _res.fetchone()
            return _source_from_row(dict(row._mapping)) if row else None

    @strawberry.field
    async def domains(self, info: StrawberryInfo) -> list[DomainType]:  # REQ-021, REQ-042
        active_org_id, is_admin = _resolve_admin_context(info)
        pool = await _get_pool()
        async with pool.acquire() as conn:
            if is_admin:
                _res = await conn.execute_core(
                    select(domains).where(domains.c.id != "").order_by(domains.c.id)
                )
            else:
                _res = await conn.execute_core(
                    select(domains)
                    .where(domains.c.id != "", domains.c.org_id == active_org_id)
                    .order_by(domains.c.id)
                )
            return [_domain_from_row(dict(r._mapping)) for r in _res.fetchall()]

    @strawberry.field
    async def tables(
        self, info: StrawberryInfo
    ) -> list[RegisteredTableType]:  # REQ-016, REQ-021, REQ-042
        with _tracer.start_as_current_span("admin.schema_introspect"):
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
                _res = await conn.execute_core(
                    select(registered_tables).order_by(registered_tables.c.id)
                )
                rows = [dict(r._mapping) for r in _res.fetchall()]
                _ares = await conn.execute_core(
                    select(
                        registered_tables.c.source_id,
                        registered_tables.c.domain_id,
                        registered_tables.c.schema_name,
                        registered_tables.c.table_name,
                        registered_tables.c.alias,
                    ).where(registered_tables.c.source_id != "__provisa__")
                )
                all_tables = [dict(r._mapping) for r in _ares.fetchall()]
                return [
                    await _fetch_table_with_columns(conn, r, all_tables, user_can_deploy)
                    for r in rows
                ]

    @strawberry.field
    async def relationships(self) -> list[RelationshipType]:  # REQ-019, REQ-020
        from provisa.api.app import state

        convention = state.global_gql_naming_convention
        pool = await _get_pool()
        _st = registered_tables.alias("st")
        _tt = registered_tables.alias("tt")
        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(
                    relationships,
                    _st.c.table_name.label("source_table_name"),
                    _st.c.domain_id.label("source_domain_id"),
                    _tt.c.table_name.label("target_table_name"),
                )
                .select_from(
                    relationships.join(_st, relationships.c.source_table_id == _st.c.id).join(
                        _tt, relationships.c.target_table_id == _tt.c.id, isouter=True
                    )
                )
                .where(
                    relationships.c.id.not_like("gql_auto__%"),
                    relationships.c.id.not_like("meta:%"),
                )
                .order_by(relationships.c.id)
            )
            return [_rel_from_row(dict(r._mapping), convention) for r in _res.fetchall()]

    @strawberry.field
    async def all_relationships(self) -> list[RelationshipType]:  # REQ-019, REQ-020
        """All relationships including system-generated meta:% entries (used by ERD)."""
        from provisa.api.app import state

        convention = state.global_gql_naming_convention
        pool = await _get_pool()
        _st = registered_tables.alias("st")
        _tt = registered_tables.alias("tt")
        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(
                    relationships,
                    _st.c.table_name.label("source_table_name"),
                    _st.c.domain_id.label("source_domain_id"),
                    _tt.c.table_name.label("target_table_name"),
                )
                .select_from(
                    relationships.join(_st, relationships.c.source_table_id == _st.c.id).join(
                        _tt, relationships.c.target_table_id == _tt.c.id, isouter=True
                    )
                )
                .where(relationships.c.id.not_like("gql_auto__%"))
                .order_by(relationships.c.id)
            )
            return [_rel_from_row(dict(r._mapping), convention) for r in _res.fetchall()]

    @strawberry.field
    async def roles(
        self, info: StrawberryInfo
    ) -> list[RoleType]:  # REQ-042, REQ-059, REQ-060, REQ-215
        active_org_id, is_admin = _resolve_admin_context(info)
        pool = await _get_pool()
        async with pool.acquire() as conn:
            if is_admin:
                _res = await conn.execute_core(select(roles).order_by(roles.c.id))
            else:
                _res = await conn.execute_core(
                    select(roles)
                    .where(or_(roles.c.org_id.is_(None), roles.c.org_id == active_org_id))
                    .order_by(roles.c.id)
                )
            return [_role_from_row(dict(r._mapping)) for r in _res.fetchall()]

    @strawberry.field
    async def rls_rules(self) -> list[RLSRuleType]:  # REQ-041, REQ-402, REQ-686

        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await rls_repo.list_all(conn)  # repo decrypts filter_expr at the boundary
            return [_rls_from_row(r) for r in rows]

    @strawberry.field
    async def available_schemas(self, source_id: str) -> list[str]:
        """List schemas available in a source."""
        from provisa.api.app import state
        from provisa.api.admin.introspect import is_provisa_internal, native_schemas

        source_type = state.source_types.get(source_id, "")
        if source_type == "openapi":
            await _ensure_openapi_spec(source_id)
        pool = await _get_pool()
        async with pool.acquire() as config_conn:
            result = await native_schemas(source_id, source_type, state.source_pools, config_conn)
        if result is not None:
            return [s for s in result if not is_provisa_internal(s)]
        # native_schemas returns None only for the engine-backed connector sources that have no
        # cheaper direct pool path. Use the engine for those — reach is the engine's OWN connector
        # registry (REQ-947), not a parallel map: unreachable ⇒ no engine schemas.
        if not state.federation_engine.reachable(source_type):
            return []
        catalog = source_to_catalog(source_id)
        schemas: list[str] = []
        with discovery_fallback(f"engine schemata for {source_id!r}"):
            res = await state.federation_engine.execute_engine(
                f'SELECT schema_name FROM "{catalog}".information_schema.schemata '
                f"ORDER BY schema_name"
            )
            schemas = [
                row[0].lower() for row in res.rows if not is_provisa_internal(row[0].lower())
            ]
        return schemas

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
        result = None
        async with pool.acquire() as config_conn:
            with discovery_fallback(f"native tables for {source_id!r}"):
                result = await native_tables(
                    source_id,
                    source_type,
                    schema_name,
                    state.source_pools,
                    config_conn,
                    state,
                )
        if result is not None:
            return result
        # the engine fallback
        from provisa.api.admin.introspect import PROVISA_INTERNAL_TABLES

        catalog = source_to_catalog(source_id)
        skip = PROVISA_INTERNAL_TABLES if schema_name.lower() == "public" else frozenset()
        tables: list[AvailableTableType] = []
        with discovery_fallback(f"engine tables for {source_id!r}"):
            res = await state.federation_engine.execute_engine(
                f'SELECT table_name FROM "{catalog}".information_schema.tables '
                f"WHERE lower(table_schema) = lower('{schema_name}') "
                f"AND table_type = 'BASE TABLE' "
                f"ORDER BY table_name"
            )
            tables = [
                AvailableTableType(name=row[0], comment=None)
                for row in res.rows
                if row[0].lower() not in skip
            ]
        return tables

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
        """List columns for a table in a source's the engine catalog."""
        from provisa.api.app import state

        source_type = state.source_types.get(source_id, "")
        if source_type == "govdata":
            cols = await _govdata_columns(source_id, schema_name, table_name, None)
            return [c.name for c in cols]
        catalog = source_to_catalog(source_id)
        columns: list[str] = []
        with discovery_fallback(f"engine columns for {source_id!r}.{schema_name}.{table_name}"):
            res = await state.federation_engine.execute_engine(
                f'SELECT column_name FROM "{catalog}".information_schema.columns '
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_name = '{table_name}' "
                f"ORDER BY ordinal_position"
            )
            columns = [row[0] for row in res.rows]
        return columns

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
            if (
                not cols
                and not q.path_params
                and (q.response_schema or {}).get("additionalProperties")
            ):
                cols = await _dynamic_openapi_columns(state.openapi_specs[source_id]["base_url"], q)
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
        cols_meta: list[AvailableColumnType] = []
        with discovery_fallback(
            f"engine column metadata for {source_id!r}.{schema_name}.{table_name}"
        ):
            # PK columns + column metadata vithe engine terminal (information_schema).
            pk_res = await state.federation_engine.execute_engine(
                f"SELECT kcu.column_name "
                f'FROM "{catalog}".information_schema.table_constraints tc '
                f'JOIN "{catalog}".information_schema.key_column_usage kcu '
                f"  ON tc.constraint_name = kcu.constraint_name "
                f"  AND tc.table_schema = kcu.table_schema "
                f"  AND tc.table_name = kcu.table_name "
                f"WHERE tc.table_schema = '{schema_name}' AND tc.table_name = '{table_name}' "
                f"  AND tc.constraint_type = 'PRIMARY KEY'"
            )
            pk_cols = {row[0] for row in pk_res.rows}
            col_res = await state.federation_engine.execute_engine(
                f"SELECT column_name, data_type, comment "
                f'FROM "{catalog}".information_schema.columns '
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_name = '{table_name}' "
                f"ORDER BY ordinal_position"
            )
            cols_meta = [
                AvailableColumnType(
                    name=row[0], data_type=row[1], comment=row[2], is_primary_key=row[0] in pk_cols
                )
                for row in col_res.rows
            ]
        return cols_meta

    @strawberry.field
    async def suggest_table_alias(self, table_name: str, domain_id: str, source_id: str) -> str:
        """Return the alias to use when registering table_name in domain_id from source_id.

        Returns a plain snake_case alias when no conflict exists, or a source-prefixed
        alias (e.g. sqlite_b_orders) when the effective name already exists in the domain
        from a different source.
        """
        from provisa.compiler.naming import apply_convention

        pool = await _get_pool()

        def _conflict_stmt(name: str):
            return (
                select(registered_tables.c.id)
                .where(
                    registered_tables.c.domain_id == domain_id,
                    func.coalesce(registered_tables.c.alias, registered_tables.c.table_name)
                    == name,
                    registered_tables.c.source_id != source_id,
                )
                .limit(1)
            )

        async with pool.acquire() as conn:
            _cres = await conn.execute_core(
                select(sources.c.gql_naming_convention).where(sources.c.id == source_id)
            )
            convention = _cres.scalar() or "apollo_graphql"
            candidate: str = apply_convention(table_name, convention)
            conflict = (await conn.execute_core(_conflict_stmt(candidate))).fetchone()
            if not conflict:
                return candidate
            # Prefix with source_id to disambiguate
            prefixed: str = apply_convention(f"{source_id}_{table_name}", convention)
            conflict2 = (await conn.execute_core(_conflict_stmt(prefixed))).fetchone()
            if not conflict2:
                return prefixed
            # Last resort: add numeric suffix
            for i in range(1, 100):
                suffixed: str = apply_convention(f"{source_id}_{table_name}_{i}", convention)
                taken = (await conn.execute_core(_conflict_stmt(suffixed))).fetchone()
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
            with discovery_fallback("redis cache stats"):
                # Lazily connect: _redis is None until the first cache op, so a fresh
                # zero-traffic instance would otherwise mislabel a live store as "noop".
                await store._connect()
                assert store._redis is not None
                total_keys = await store._redis.dbsize()
                # Embedded fakeredis (REQ-829) has no INFO command — report the keys it
                # can and label it "memory" so the UI shows it as an enabled store, not "noop".
                if type(store._redis).__module__.startswith("fakeredis"):
                    return CacheStatsType(
                        total_keys=total_keys, hit_count=0, miss_count=0, store_type="memory"
                    )
                # Default INFO covers the memory, clients, and stats sections in one round-trip.
                info = await store._redis.info()
                return CacheStatsType(
                    total_keys=total_keys,
                    hit_count=info.get("keyspace_hits", 0),
                    miss_count=info.get("keyspace_misses", 0),
                    store_type="redis",
                    used_memory_bytes=info.get("used_memory"),
                    # Redis reports maxmemory=0 when unbounded; surface that as "no cap" (None).
                    max_memory_bytes=info.get("maxmemory") or None,
                    evicted_keys=info.get("evicted_keys"),
                    expired_keys=info.get("expired_keys"),
                    connected_clients=info.get("connected_clients"),
                    ops_per_sec=info.get("instantaneous_ops_per_sec"),
                )
        return CacheStatsType(total_keys=0, hit_count=0, miss_count=0, store_type="noop")

    @strawberry.field
    async def cache_table_stats(self) -> list[CacheTableStatType]:
        """Per-table cached-entry counts (empty when no cache store is configured)."""
        from provisa.api.app import state

        counts = await state.response_cache_store.table_entry_counts()
        return [CacheTableStatType(table_id=tid, cached_entries=n) for tid, n in counts.items()]

    @strawberry.field
    async def hot_tables(self) -> list[HotTableStatType]:
        """Hot-tier lookup tables mirrored into Redis/fakeredis for JOIN inlining."""
        from provisa.api.app import state

        mgr = getattr(state, "hot_manager", None)
        if mgr is None:
            return []
        return [
            HotTableStatType(
                table_name=e["table_name"],
                catalog=e["catalog"],
                schema_name=e["schema"],
                row_count=e["row_count"],
                is_api=e["is_api"],
                loaded=e["loaded"],
            )
            for e in mgr.snapshot()
        ]

    @strawberry.field
    async def materialize_store_info(self) -> MaterializeStoreInfoType:
        """Identity of the durable materialization store (landed sources + MV cache)."""
        from provisa.api.app import state

        engine = state.federation_engine
        return MaterializeStoreInfoType(
            engine_name=engine.name,
            store_ref=engine.materialize_store(),
            mv_count=len(state.mv_registry._mvs),
        )

    # ── Admin: System Health ──

    @strawberry.field
    async def system_health(self) -> SystemHealthType:
        """Return system component health status."""
        from provisa.api.admin.system_health import collect_system_health

        return await collect_system_health()

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
        with discovery_fallback("scheduler jobs"):
            from provisa.api.app import state

            scheduler = getattr(state, "scheduler", None)
            if scheduler is not None:
                for job in scheduler.get_jobs():
                    job_map[job.id] = job

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
                _res = await conn.execute_core(
                    select(registered_tables).where(registered_tables.c.id == tid)
                )
                row = _res.fetchone()
                if row is None:
                    print(
                        f"[DEBUG] table {table_id} not found — save the view/table first",
                        file=sys.stderr,
                        flush=True,
                    )
                    return "Save the view first before generating descriptions"
                _cres = await conn.execute_core(
                    select(table_columns.c.column_name)
                    .where(table_columns.c.table_id == tid)
                    .order_by(table_columns.c.id)
                )
                col_rows = _cres.fetchall()
            table_name = row.table_name
            schema_name = row.schema_name
            source_id = row.source_id
            columns = [r.column_name for r in col_rows]
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
                _res = await conn.execute_core(
                    select(registered_tables).where(registered_tables.c.id == tid)
                )
                row = _res.fetchone()
                if row is None:
                    print(
                        f"[DEBUG] table {table_id} not found — save the view/table first",
                        file=sys.stderr,
                        flush=True,
                    )
                    return "Save the view first before generating descriptions"
                _cres = await conn.execute_core(
                    select(table_columns.c.column_name)
                    .where(table_columns.c.table_id == tid)
                    .order_by(table_columns.c.id)
                )
                col_rows = _cres.fetchall()
            table_name = row.table_name
            schema_name = row.schema_name
            source_id = row.source_id
            all_columns = [r.column_name for r in col_rows]
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
            logging.getLogger(__name__).exception("generateColumnDescription failed: %s", e)
            return ""
