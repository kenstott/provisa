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

from typing import Optional

import strawberry

from provisa.api.admin.types import (
    AvailableColumnType,
    AvailableTableType,
    CacheStatsType,
    ColumnInput,
    DomainInput,
    DomainType,
    MutationResult,
    MVType,
    PersistedQueryType,
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


async def _get_pool():
    from provisa.api.app import state
    return state.pg_pool


async def _rebuild_schemas():
    from provisa.api.app import _rebuild_schemas as rebuild
    await rebuild()


def _source_from_row(row) -> SourceType:
    return SourceType(
        id=row["id"], type=row["type"], host=row["host"],
        port=row["port"], database=row["database"],
        username=row["username"], dialect=row["dialect"],
        cache_enabled=row.get("cache_enabled", True),
        cache_ttl=row.get("cache_ttl"),
        naming_convention=row.get("naming_convention"),
    )


def _domain_from_row(row) -> DomainType:
    return DomainType(id=row["id"], description=row["description"])


def _role_from_row(row) -> RoleType:
    return RoleType(
        id=row["id"], capabilities=list(row["capabilities"]),
        domain_access=list(row["domain_access"]),
    )


def _rel_from_row(row) -> RelationshipType:
    return RelationshipType(
        id=row["id"], source_table_id=row["source_table_id"],
        target_table_id=row["target_table_id"],
        source_table_name=row.get("source_table_name", ""),
        target_table_name=row.get("target_table_name", ""),
        source_column=row["source_column"],
        target_column=row["target_column"],
        cardinality=row["cardinality"],
        materialize=row.get("materialize", False),
        refresh_interval=row.get("refresh_interval", 300),
    )


def _rls_from_row(row) -> RLSRuleType:
    return RLSRuleType(
        id=row["id"], table_id=row["table_id"],
        role_id=row["role_id"], filter_expr=row["filter_expr"],
    )


async def _fetch_table_with_columns(conn, row) -> RegisteredTableType:
    col_rows = await conn.fetch(
        "SELECT id, column_name, visible_to, writable_by, unmasked_to, "
        "mask_type, mask_pattern, mask_replace, mask_value, mask_precision, "
        "alias, description "
        "FROM table_columns WHERE table_id = $1 ORDER BY id", row["id"],
    )
    columns = [
        TableColumnType(
            id=r["id"], column_name=r["column_name"],
            visible_to=list(r["visible_to"]),
            writable_by=list(r.get("writable_by") or []),
            unmasked_to=list(r.get("unmasked_to") or []),
            mask_type=r.get("mask_type"),
            mask_pattern=r.get("mask_pattern"),
            mask_replace=r.get("mask_replace"),
            mask_value=r.get("mask_value"),
            mask_precision=r.get("mask_precision"),
            alias=r.get("alias"), description=r.get("description"),
        )
        for r in col_rows
    ]
    return RegisteredTableType(
        id=row["id"], source_id=row["source_id"],
        domain_id=row["domain_id"], schema_name=row["schema_name"],
        table_name=row["table_name"], governance=row["governance"],
        alias=row.get("alias"), description=row.get("description"),
        cache_ttl=row.get("cache_ttl"),
        naming_convention=row.get("naming_convention"),
        columns=columns,
    )


@strawberry.type
class Query:
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
            rows = await conn.fetch("SELECT * FROM domains ORDER BY id")
            return [_domain_from_row(r) for r in rows]

    @strawberry.field
    async def tables(self) -> list[RegisteredTableType]:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM registered_tables ORDER BY id"
            )
            return [await _fetch_table_with_columns(conn, r) for r in rows]

    @strawberry.field
    async def relationships(self) -> list[RelationshipType]:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT r.*, "
                "st.table_name AS source_table_name, "
                "tt.table_name AS target_table_name "
                "FROM relationships r "
                "JOIN registered_tables st ON r.source_table_id = st.id "
                "JOIN registered_tables tt ON r.target_table_id = tt.id "
                "ORDER BY r.id"
            )
            return [_rel_from_row(r) for r in rows]

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
    async def persisted_queries(self) -> list[PersistedQueryType]:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM persisted_queries ORDER BY id")
            return [
                PersistedQueryType(
                    id=r["id"], query_text=r["query_text"],
                    compiled_sql=r["compiled_sql"] or "",
                    status=r["status"], stable_id=r.get("stable_id"),
                    developer_id=r.get("developer_id"),
                    approved_by=r.get("approved_by"),
                    sink_topic=r.get("sink_topic"),
                    sink_trigger=r.get("sink_trigger"),
                    sink_key_column=r.get("sink_key_column"),
                    business_purpose=r.get("business_purpose"),
                    use_cases=r.get("use_cases"),
                    data_sensitivity=r.get("data_sensitivity"),
                    refresh_frequency=r.get("refresh_frequency"),
                    expected_row_count=r.get("expected_row_count"),
                    owner_team=r.get("owner_team"),
                    expiry_date=str(r["expiry_date"]) if r.get("expiry_date") else None,
                )
                for r in rows
            ]

    @strawberry.field
    async def available_schemas(self, source_id: str) -> list[str]:
        """List schemas available in a source's Trino catalog."""
        from provisa.api.app import state
        catalog = source_id.replace("-", "_")
        # Admin/platform schemas to hide from data UI
        _HIDDEN_SCHEMAS = {"information_schema", "pg_catalog"}
        try:
            cursor = state.trino_conn.cursor()
            cursor.execute(
                f"SELECT schema_name FROM \"{catalog}\".information_schema.schemata "
                f"ORDER BY schema_name"
            )
            return [
                row[0] for row in cursor.fetchall()
                if row[0] not in _HIDDEN_SCHEMAS
            ]
        except Exception:
            return []

    @strawberry.field
    async def available_tables(self, source_id: str, schema_name: str = "public") -> list[AvailableTableType]:
        """List tables available in a source's Trino catalog (for registration UI).

        Returns table names with comments from the physical database.
        Filters out Provisa admin/platform tables.
        """
        from provisa.api.app import state
        catalog = source_id.replace("-", "_")
        # Admin tables managed by Provisa — hide from data registration
        _ADMIN_TABLES = {
            "sources", "domains", "naming_rules", "registered_tables",
            "table_columns", "relationships", "roles", "rls_rules",
            "materialized_views", "mv_refresh_log", "column_masking_rules",
            "persisted_queries", "approval_log", "relationship_candidates",
            "kafka_sources", "kafka_topics", "kafka_sinks",
            "api_sources", "api_endpoints", "api_endpoint_candidates",
        }
        try:
            cursor = state.trino_conn.cursor()
            cursor.execute(
                f"SELECT table_name, comment FROM \"{catalog}\".information_schema.tables "
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_type = 'BASE TABLE' "
                f"ORDER BY table_name"
            )
            return [
                AvailableTableType(name=row[0], comment=row[1])
                for row in cursor.fetchall()
                if row[0] not in _ADMIN_TABLES
            ]
        except Exception:
            return []

    @strawberry.field
    async def available_columns(
        self, source_id: str, schema_name: str, table_name: str
    ) -> list[str]:
        """List columns for a table in a source's Trino catalog."""
        from provisa.api.app import state
        catalog = source_id.replace("-", "_")
        try:
            cursor = state.trino_conn.cursor()
            cursor.execute(
                f"SELECT column_name FROM \"{catalog}\".information_schema.columns "
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
        """List columns with data types and comments from the physical database."""
        from provisa.api.app import state
        catalog = source_id.replace("-", "_")
        try:
            cursor = state.trino_conn.cursor()
            cursor.execute(
                f"SELECT column_name, data_type, comment "
                f"FROM \"{catalog}\".information_schema.columns "
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_name = '{table_name}' "
                f"ORDER BY ordinal_position"
            )
            return [
                AvailableColumnType(name=row[0], data_type=row[1], comment=row[2])
                for row in cursor.fetchall()
            ]
        except Exception:
            return []

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
        store = state.cache_store
        if isinstance(store, RedisCacheStore):
            try:
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
        if state.trino_conn is not None:
            try:
                cursor = state.trino_conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                trino_ok = True
            except Exception:
                pass

        pg_size, pg_free = 0, 0
        if state.pg_pool is not None:
            pg_size = state.pg_pool.get_size()
            pg_free = state.pg_pool.get_idle_size()

        cache_ok = False
        if isinstance(state.cache_store, RedisCacheStore):
            try:
                await state.cache_store._redis.ping()
                cache_ok = True
            except Exception:
                pass

        flight_ok = state.flight_server is not None if hasattr(state, "flight_server") else False

        return SystemHealthType(
            trino_connected=trino_ok,
            pg_pool_size=pg_size,
            pg_pool_free=pg_free,
            cache_connected=cache_ok,
            flight_server_running=flight_ok,
            mv_refresh_loop_running=hasattr(state, "_mv_task") and state._mv_task is not None,
        )


    # ── Admin: Scheduled Tasks ──

    @strawberry.field
    async def scheduled_tasks(self) -> list[ScheduledTaskType]:
        """List scheduled triggers from config with runtime state."""
        import os
        from pathlib import Path

        import yaml

        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        if not path.exists():
            return []

        with open(path) as f:
            cfg = yaml.safe_load(f)

        triggers = cfg.get("scheduled_triggers", [])

        # Try to get runtime info from the APScheduler instance
        job_map: dict[str, object] = {}
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
            result.append(ScheduledTaskType(
                id=tid,
                name=t.get("name", tid),
                cron_expression=t["cron"],
                webhook_url=t.get("url"),
                enabled=t.get("enabled", True),
                last_run_at=None,
                next_run_at=next_run,
            ))
        return result


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def create_source(self, input: SourceInput) -> MutationResult:
        from provisa.core.models import Source as SourceModel
        from provisa.core.repositories import source as source_repo

        pool = await _get_pool()
        model = SourceModel(
            id=input.id, type=input.type, host=input.host,
            port=input.port, database=input.database,
            username=input.username, password=input.password,
        )
        async with pool.acquire() as conn:
            await source_repo.upsert(conn, model)

        # AL1: fire ANALYZE on all registered tables for this source (errors swallowed)
        from provisa.api.app import state
        if state.trino_conn is not None:
            from provisa.core.catalog import analyze_source_tables

            class _TblRef:
                def __init__(self, source_id, schema_name, table_name):
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

        return MutationResult(success=True, message=f"Source {input.id!r} created")

    @strawberry.mutation
    async def update_source(self, input: SourceInput) -> MutationResult:
        from provisa.core.models import Source as SourceModel
        from provisa.core.repositories import source as source_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            existing = await source_repo.get(conn, input.id)
            if existing is None:
                return MutationResult(success=False, message=f"Source {input.id!r} not found")
            model = SourceModel(
                id=input.id, type=input.type, host=input.host,
                port=input.port, database=input.database,
                username=input.username, password=input.password,
            )
            await source_repo.upsert(conn, model)
        return MutationResult(success=True, message=f"Source {input.id!r} updated")

    @strawberry.mutation
    async def delete_source(self, id: str) -> MutationResult:
        from provisa.core.repositories import source as source_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await source_repo.delete(conn, id)
        if deleted:
            return MutationResult(success=True, message=f"Source {id!r} deleted")
        return MutationResult(success=False, message=f"Source {id!r} not found")

    @strawberry.mutation
    async def create_domain(self, input: DomainInput) -> MutationResult:
        from provisa.core.models import Domain as DomainModel
        from provisa.core.repositories import domain as domain_repo

        pool = await _get_pool()
        model = DomainModel(id=input.id, description=input.description)
        async with pool.acquire() as conn:
            await domain_repo.upsert(conn, model)
        return MutationResult(success=True, message=f"Domain {input.id!r} created")

    @strawberry.mutation
    async def create_role(self, input: RoleInput) -> MutationResult:
        from provisa.core.models import Role as RoleModel
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        model = RoleModel(
            id=input.id, capabilities=input.capabilities,
            domain_access=input.domain_access,
        )
        async with pool.acquire() as conn:
            await role_repo.upsert(conn, model)
        return MutationResult(success=True, message=f"Role {input.id!r} created")

    @strawberry.mutation
    async def register_table(self, input: TableInput) -> MutationResult:
        from provisa.core.models import (
            Column as ColumnModel,
            GovernanceLevel,
            Table as TableModel,
        )
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        try:
            governance = GovernanceLevel(input.governance)
        except ValueError:
            return MutationResult(
                success=False,
                message=f"Invalid governance level: {input.governance!r}",
            )
        columns = [
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
            )
            for c in input.columns
        ]
        model = TableModel(
            source_id=input.source_id,
            domain_id=input.domain_id,
            schema_name=input.schema_name,
            table_name=input.table_name,
            governance=governance,
            alias=input.alias,
            description=input.description,
            columns=columns,
        )
        async with pool.acquire() as conn:
            table_id = await table_repo.upsert(conn, model)
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Table {input.table_name!r} registered (id={table_id})",
        )

    @strawberry.mutation
    async def update_table(self, input: TableInput) -> MutationResult:
        """Update an existing table's alias, description, and column metadata."""
        from provisa.core.models import (
            Column as ColumnModel,
            GovernanceLevel,
            Table as TableModel,
        )
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        try:
            governance = GovernanceLevel(input.governance)
        except ValueError:
            return MutationResult(
                success=False,
                message=f"Invalid governance level: {input.governance!r}",
            )
        columns = [
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
            )
            for c in input.columns
        ]
        model = TableModel(
            source_id=input.source_id,
            domain_id=input.domain_id,
            schema_name=input.schema_name,
            table_name=input.table_name,
            governance=governance,
            alias=input.alias,
            description=input.description,
            columns=columns,
        )
        async with pool.acquire() as conn:
            table_id = await table_repo.upsert(conn, model)
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
            deleted = await table_repo.delete(conn, id)
        if deleted:
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Table {id} deleted")
        return MutationResult(success=False, message=f"Table {id} not found")

    @strawberry.mutation
    async def delete_role(self, id: str) -> MutationResult:
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await role_repo.delete(conn, id)
        if deleted:
            return MutationResult(success=True, message=f"Role {id!r} deleted")
        return MutationResult(success=False, message=f"Role {id!r} not found")

    @strawberry.mutation
    async def upsert_rls_rule(self, input: RLSRuleInput) -> MutationResult:
        from provisa.core.models import RLSRule as RLSRuleModel
        from provisa.core.repositories import rls as rls_repo

        pool = await _get_pool()
        model = RLSRuleModel(
            table_id=input.table_id,
            role_id=input.role_id,
            filter=input.filter_expr,
        )
        try:
            async with pool.acquire() as conn:
                await rls_repo.upsert(conn, model)
        except ValueError as e:
            return MutationResult(success=False, message=str(e))
        return MutationResult(
            success=True,
            message=f"RLS rule for table {input.table_id!r} / role {input.role_id!r} saved",
        )

    @strawberry.mutation
    async def delete_rls_rule(self, table_id: int, role_id: str) -> MutationResult:
        from provisa.core.repositories import rls as rls_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await rls_repo.delete(conn, table_id, role_id)
        if deleted:
            return MutationResult(success=True, message="RLS rule deleted")
        return MutationResult(success=False, message="RLS rule not found")

    @strawberry.mutation
    async def upsert_relationship(self, input: RelationshipInput) -> MutationResult:
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
            target_table_id=input.target_table_id,
            source_column=input.source_column,
            target_column=input.target_column,
            cardinality=Cardinality(input.cardinality),
            materialize=input.materialize,
            refresh_interval=input.refresh_interval,
        )
        async with pool.acquire() as conn:
            await rel_repo.upsert(conn, model)
        await _rebuild_schemas()
        return MutationResult(
            success=True, message=f"Relationship {input.id!r} saved",
        )

    @strawberry.mutation
    async def delete_relationship(self, id: str) -> MutationResult:
        from provisa.core.repositories import relationship as rel_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await rel_repo.delete(conn, id)
        if deleted:
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Relationship {id!r} deleted")
        return MutationResult(success=False, message=f"Relationship {id!r} not found")

    @strawberry.mutation
    async def approve_query(self, query_id: int, approver_id: str = "admin") -> MutationResult:
        from provisa.registry.store import approve
        pool = await _get_pool()
        async with pool.acquire() as conn:
            try:
                stable_id = await approve(conn, query_id, approver_id)
                return MutationResult(
                    success=True,
                    message=f"Query approved with stable ID: {stable_id}",
                )
            except Exception as e:
                return MutationResult(success=False, message=str(e))


    # ── Admin: Cache Configuration ──

    @strawberry.mutation
    async def update_source_cache(self, source_id: str, cache_enabled: bool, cache_ttl: int | None = None) -> MutationResult:
        """Update cache settings for a source."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE sources SET cache_enabled = $1, cache_ttl = $2 WHERE id = $3",
                cache_enabled, cache_ttl, source_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Source {source_id!r} not found")
        return MutationResult(success=True, message=f"Cache settings updated for source {source_id!r}")

    @strawberry.mutation
    async def update_table_cache(self, table_id: int, cache_ttl: int | None = None) -> MutationResult:
        """Update cache TTL for a registered table."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE registered_tables SET cache_ttl = $1 WHERE id = $2",
                cache_ttl, table_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Table {table_id} not found")
        return MutationResult(success=True, message=f"Cache TTL updated for table {table_id}")

    # ── Admin: Naming Convention ──

    @strawberry.mutation
    async def update_source_naming(self, source_id: str, naming_convention: Optional[str] = None) -> MutationResult:
        """Update naming convention for a source."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE sources SET naming_convention = $1 WHERE id = $2",
                naming_convention, source_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Source {source_id!r} not found")
        await _rebuild_schemas()
        return MutationResult(success=True, message=f"Naming convention updated for source {source_id!r}")

    @strawberry.mutation
    async def update_table_naming(self, table_id: int, naming_convention: Optional[str] = None) -> MutationResult:
        """Update naming convention for a registered table."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE registered_tables SET naming_convention = $1 WHERE id = $2",
                naming_convention, table_id,
            )
            if result == "UPDATE 0":
                return MutationResult(success=False, message=f"Table {table_id} not found")
        await _rebuild_schemas()
        return MutationResult(success=True, message=f"Naming convention updated for table {table_id}")

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
            await refresh_mv(mv, state)
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
        return MutationResult(success=True, message=f"MV {mv_id!r} {'enabled' if enabled else 'disabled'}")

    # ── Admin: Cache Management ──

    @strawberry.mutation
    async def purge_cache(self) -> MutationResult:
        """Purge all cached query results."""
        from provisa.api.app import state
        try:
            count = await state.cache_store.invalidate_by_pattern("provisa:cache:*")
            return MutationResult(success=True, message=f"Purged {count} cache entries")
        except Exception as e:
            return MutationResult(success=False, message=str(e))

    @strawberry.mutation
    async def purge_cache_by_table(self, table_id: int) -> MutationResult:
        """Purge cached results for a specific table."""
        from provisa.api.app import state
        try:
            count = await state.cache_store.invalidate_by_table(table_id)
            return MutationResult(success=True, message=f"Purged {count} cache entries for table {table_id}")
        except Exception as e:
            return MutationResult(success=False, message=str(e))


    # ── Admin: Scheduled Task Management ──

    @strawberry.mutation
    async def toggle_scheduled_task(self, task_id: str, enabled: bool) -> MutationResult:
        """Enable or disable a scheduled trigger in the config."""
        import os
        from pathlib import Path

        import yaml

        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        if not path.exists():
            return MutationResult(success=False, message="Config file not found")

        with open(path) as f:
            cfg = yaml.safe_load(f)

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
        source_catalog = source_id.replace("-", "_")

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


admin_schema = strawberry.Schema(query=Query, mutation=Mutation)
