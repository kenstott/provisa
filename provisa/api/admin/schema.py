# Copyright (c) 2025 Kenneth Stott
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
    ColumnInput,
    DomainInput,
    DomainType,
    MutationResult,
    PersistedQueryType,
    RegisteredTableType,
    RelationshipInput,
    RelationshipType,
    RLSRuleInput,
    RLSRuleType,
    RoleInput,
    RoleType,
    SourceInput,
    SourceType,
    TableColumnType,
    TableInput,
)


async def _get_pool():
    from provisa.api.app import state
    return state.pg_pool


def _source_from_row(row) -> SourceType:
    return SourceType(
        id=row["id"], type=row["type"], host=row["host"],
        port=row["port"], database=row["database"],
        username=row["username"], dialect=row["dialect"],
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
        "SELECT id, column_name, visible_to, alias, description "
        "FROM table_columns WHERE table_id = $1 ORDER BY id", row["id"],
    )
    columns = [
        TableColumnType(
            id=r["id"], column_name=r["column_name"],
            visible_to=list(r["visible_to"]),
            alias=r.get("alias"), description=r.get("description"),
        )
        for r in col_rows
    ]
    return RegisteredTableType(
        id=row["id"], source_id=row["source_id"],
        domain_id=row["domain_id"], schema_name=row["schema_name"],
        table_name=row["table_name"], governance=row["governance"],
        alias=row.get("alias"), description=row.get("description"),
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
    async def available_tables(self, source_id: str, schema_name: str = "public") -> list[str]:
        """List tables available in a source's Trino catalog (for registration UI).

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
                f"SELECT table_name FROM \"{catalog}\".information_schema.tables "
                f"WHERE table_schema = '{schema_name}' "
                f"AND table_type = 'BASE TABLE' "
                f"ORDER BY table_name"
            )
            return [
                row[0] for row in cursor.fetchall()
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
        return MutationResult(success=True, message=f"Source {input.id!r} created")

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


admin_schema = strawberry.Schema(query=Query, mutation=Mutation)
