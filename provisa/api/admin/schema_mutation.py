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
"""Admin GraphQL Mutation type — write-side resolvers for all config entities."""

from __future__ import annotations


import logging
from typing import TYPE_CHECKING, Optional, cast

import strawberry
from sqlalchemy import delete as _delete, select, update
from strawberry.types.info import Info as StrawberryInfo

from provisa.core.schema_org import (
    file_source_mtimes,
    registered_tables,
    relationship_candidates,
    relationships,
    sources,
    tracked_webhooks,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

from provisa.compiler.naming import source_to_catalog
from provisa.core.repositories import rls as rls_repo
from provisa.federation.strategy import engine_attaches
from provisa.api.admin._config_io import config_path as _config_path, read_config
from provisa.api.admin.types import (
    ColumnAliasType,
    CompileQueryInput,
    CompileQueryResult,
    DomainInput,
    EnforcementType,
    MutationResult,
    RelationshipInput,
    RLSRuleInput,
    RoleInput,
    SourceInput,
    TableInput,
)

from provisa.api.admin.schema_helpers import (
    _dataset_ownership_conflict,
    _domain_table_conflict,
    _get_pool,
    _maybe_migrate_sqlite,
    _rebuild_schemas,
)
from provisa.api.admin._live_mappers import table_model_from_input as _table_model_from_input
from provisa.api.admin._table_ops import _build_columns_for_input
from provisa.api.admin import schema_mutation_ops as _ops


from provisa.api.admin._row_mappers import (  # noqa: E402
    _parse_mapping_json,
    _cdc_model_from_input,
)
from provisa.api.admin.schema_common import (  # noqa: E402
    _add_source_pool,
    _analyze_source_on_engine,
    _configure_govdata_env,
    _fire_catalog_indexing,
    _prime_govdata_cache,
    _queue_creation_request,
    _rebuild_relationship_input,
    _rebuild_table_input,
    _register_source_on_engine,
    _remove_view_mv,
    _sync_view_mv,
    _upsert_source_with_domains,
    _validate_govdata_api_key,
)


@strawberry.type
class Mutation:  # REQ-012, REQ-013, REQ-016, REQ-042
    @strawberry.mutation
    async def rebuild_schemas(self) -> MutationResult:
        """Rebuild in-memory schema from DB state. Useful after external DB changes."""
        await _rebuild_schemas()
        return MutationResult(success=True, message="Schemas rebuilt")

    @strawberry.mutation
    async def create_source(
        self, info: StrawberryInfo, input: SourceInput
    ) -> MutationResult:  # REQ-012, REQ-013
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
            mapping=_parse_mapping_json(input.mapping_json),
            change_signal=input.change_signal,
            cdc=_cdc_model_from_input(input),
        )
        from provisa.api.app import state

        # REQ-012: validate the direct connection before persisting; reject on failure
        # rather than leaving a half-registered source behind a swallowed error.
        try:
            await _add_source_pool(state, input)
        except Exception as _conn_err:
            logging.getLogger(__name__).exception(
                "create_source: connection validation failed for %r", input.id
            )
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

        # Provision on the bound engine (the engine makes a catalog; native engines no-op / attach lazily).
        _register_source_on_engine(state, model, input)
        await _analyze_source_on_engine(state, pool, model, input)

        if input.type == "govdata" and input.database and input.username:
            _prime_govdata_cache(input)

        _fire_catalog_indexing(state, pool, input)

        return MutationResult(success=True, message=f"Source {input.id!r} created")

    @strawberry.mutation
    async def update_source(
        self, info: StrawberryInfo, input: SourceInput
    ) -> MutationResult:  # REQ-012
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "source_registration")
        from provisa.core.models import Source as SourceModel, SourceType as SourceTypeEnum
        from provisa.core.repositories import source as source_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
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
                mapping=_parse_mapping_json(input.mapping_json),
                change_signal=input.change_signal,
                cdc=_cdc_model_from_input(input),
            )
            await source_repo.upsert(_conn, model)
            if input.allowed_domains is not None:
                await conn.execute_core(
                    update(sources)
                    .where(sources.c.id == input.id)
                    .values(allowed_domains=input.allowed_domains)
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
            except Exception:
                logging.getLogger(__name__).exception(
                    "Direct pool for %r failed — the engine-routed queries still work.",
                    input.id,
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
                state.federation_engine,
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
            renamed = await source_repo.rename(cast("Connection", conn), old_id, new_id)
        if renamed:
            return MutationResult(success=True, message=f"Source renamed {old_id!r} → {new_id!r}")
        return MutationResult(success=False, message=f"Source {old_id!r} not found")

    @strawberry.mutation
    async def delete_source(self, id: str) -> MutationResult:
        from provisa.core.repositories import source as source_repo
        from provisa.api.app import state

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await source_repo.delete(cast("Connection", conn), id)
        if deleted:
            state.graphql_remote_sources.pop(id, None)
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Source {id!r} deleted")
        return MutationResult(success=False, message=f"Source {id!r} not found")

    @strawberry.mutation
    async def create_domain(self, input: DomainInput) -> MutationResult:  # REQ-021
        from provisa.core.models import Domain as DomainModel
        from provisa.core.repositories import domain as domain_repo

        pool = await _get_pool()
        model = DomainModel(
            id=input.id, description=input.description, graphql_alias=input.graphql_alias or None
        )
        async with pool.acquire() as conn:
            await domain_repo.upsert(cast("Connection", conn), model)
        return MutationResult(success=True, message=f"Domain {input.id!r} created")

    @strawberry.mutation
    async def delete_domain(self, id: str) -> MutationResult:
        from provisa.core.repositories import domain as domain_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await domain_repo.delete(cast("Connection", conn), id)
        if deleted:
            return MutationResult(success=True, message=f"Domain {id!r} deleted")
        return MutationResult(success=False, message=f"Domain {id!r} not found")

    @strawberry.mutation
    async def create_role(
        self, input: RoleInput
    ) -> MutationResult:  # REQ-042, REQ-059, REQ-060, REQ-215
        from provisa.core.models import Role as RoleModel
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        model = RoleModel(
            id=input.id,
            capabilities=input.capabilities,
            domain_access=input.domain_access,
        )
        async with pool.acquire() as conn:
            await role_repo.upsert(cast("Connection", conn), model)
        return MutationResult(success=True, message=f"Role {input.id!r} created")

    @strawberry.mutation
    async def register_table(
        self, info: StrawberryInfo, input: TableInput
    ) -> MutationResult:  # REQ-013, REQ-016, REQ-252, REQ-366, REQ-413, REQ-432, REQ-433, REQ-434
        return await _ops.register_table(info, input)

    @strawberry.mutation
    async def update_table(
        self, info: StrawberryInfo, input: TableInput
    ) -> MutationResult:  # REQ-016, REQ-020, REQ-155, REQ-156
        """Update an existing table's alias, description, and column metadata."""
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "table_registration", domain_id=input.domain_id)
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        columns, _ = await _build_columns_for_input(pool, input)
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
        model = _table_model_from_input(input, columns, presets, input.alias)
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            _conflict = await _domain_table_conflict(
                _conn,
                model.domain_id,
                model.table_name,
                model.source_id,
                model.schema_name,
                input.alias,
            )
            if _conflict:
                return MutationResult(success=False, message=_conflict)
            _owner_conflict = await _dataset_ownership_conflict(
                _conn, model.source_id, model.table_name, model.domain_id
            )
            if _owner_conflict:
                return MutationResult(success=False, message=_owner_conflict)
            table_id = await table_repo.upsert(_conn, model)
            if table_id is not None:
                await _conn.execute_core(
                    update(registered_tables)
                    .where(registered_tables.c.id == table_id)
                    .values(
                        enable_aggregates=input.enable_aggregates,
                        enable_group_by=input.enable_group_by,
                    )
                )
            # REQ-020: a column change may invalidate a relationship's join field — flag
            # any relationship whose join column on this table is no longer present.
            from provisa.core.repositories import relationship as _rel_repo

            if table_id is not None:
                await _rel_repo.mark_relationships_for_review(
                    _conn, table_id, [c.name for c in model.columns]
                )
            _sres = await _conn.execute_core(
                select(sources.c.type, sources.c.path).where(sources.c.id == input.source_id)
            )
            _srow = _sres.fetchone()
            src_row = dict(_srow._mapping) if _srow is not None else None
            await _maybe_migrate_sqlite(
                src_row, _conn, input.source_id, input.table_name, input.schema_name
            )
        if input.view_sql and input.materialize:
            try:
                _sync_view_mv(
                    input.table_name,
                    input.view_sql,
                    input.mv_refresh_interval,
                    input.change_signal,
                    debounce_quiet=input.mv_debounce_quiet,  # REQ-963
                    debounce_max_delay=input.mv_debounce_max_delay,  # REQ-963
                )
            except ValueError as _det_err:  # REQ-964: reject non-deterministic MV SQL
                return MutationResult(success=False, message=str(_det_err))
        elif not input.materialize:
            _remove_view_mv(input.table_name)
        await _rebuild_schemas()
        # Materialize + wire a (re)materialized view immediately — FRESH now, not STALE-until-restart.
        if input.view_sql and input.materialize:
            from provisa.api.admin.schema_common import activate_view_mv

            await activate_view_mv(input.table_name)
        return MutationResult(
            success=True,
            message=f"Table {input.table_name!r} updated (id={table_id})",
        )

    @strawberry.mutation
    async def delete_table(self, id: int) -> MutationResult:
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await table_repo.delete(cast("Connection", conn), id)
        if deleted:
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Table {id} deleted")
        return MutationResult(success=False, message=f"Table {id} not found")

    @strawberry.mutation
    async def delete_role(self, id: str) -> MutationResult:
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await role_repo.delete(cast("Connection", conn), id)
        if deleted:
            return MutationResult(success=True, message=f"Role {id!r} deleted")
        return MutationResult(success=False, message=f"Role {id!r} not found")

    @strawberry.mutation
    async def upsert_rls_rule(self, input: RLSRuleInput) -> MutationResult:  # REQ-041, REQ-402
        from provisa.core.models import RLSRule as RLSRuleModel

        pool = await _get_pool()
        model = RLSRuleModel(
            table_id=input.table_id or None,
            domain_id=input.domain_id or None,
            role_id=input.role_id,
            filter=input.filter_expr,
        )
        try:
            async with pool.acquire() as conn:
                await rls_repo.upsert(cast("Connection", conn), model)
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

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await rls_repo.delete(
                cast("Connection", conn), role_id, table_id=table_id, domain_id=domain_id
            )
        if deleted:
            return MutationResult(success=True, message="RLS rule deleted")
        return MutationResult(success=False, message="RLS rule not found")

    @strawberry.mutation
    async def execute_creation_request(  # REQ-434, REQ-063
        self, info: StrawberryInfo, request_id: int
    ) -> MutationResult:
        """REQ-434: a rights-holder executes a queued creation request."""
        from provisa.api.admin.capabilities import _identity_from_info, require_capability
        from provisa.core.repositories import creation_request as cr_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            req = await cr_repo.get(cast("Connection", conn), request_id)
        if req is None or req["status"] != "pending":
            return MutationResult(success=False, message="Request not found or already resolved")
        try:
            require_capability(info, req["capability"])
        except PermissionError as e:
            return MutationResult(success=False, message=str(e))

        if req["request_type"] == "relationship":
            result = await self.upsert_relationship(
                info,
                _rebuild_relationship_input(req["payload"]),  # pyright: ignore[reportCallIssue]
            )
        elif req["request_type"] == "view":
            result = await self.register_table(info, _rebuild_table_input(req["payload"]))  # pyright: ignore[reportCallIssue]
        elif req["request_type"] == "webhook":
            # REQ-209: approving a webhook only requires marking this request executed (done
            # below) — the schema-build gate then exposes the webhook whose latest request is
            # executed. Verify the webhook still exists, then rebuild.
            wh_name = req["payload"]["name"]
            async with pool.acquire() as conn:
                _ex = await conn.execute_core(
                    select(tracked_webhooks.c.id).where(tracked_webhooks.c.name == wh_name)
                )
                exists = _ex.scalar()
            if not exists:
                return MutationResult(success=False, message=f"Webhook {wh_name!r} not found")
            from provisa.api.app import _rebuild_schemas

            await _rebuild_schemas()
            result = MutationResult(success=True, message=f"Approved webhook {wh_name!r}")
        else:
            return MutationResult(
                success=False, message=f"Unknown request type {req['request_type']!r}"
            )
        if not result.success:
            return result

        identity = _identity_from_info(info)
        resolved_by = getattr(identity, "user_id", None) if identity is not None else None
        async with pool.acquire() as conn:
            await cr_repo.mark_executed(cast("Connection", conn), request_id, resolved_by)
        return MutationResult(success=True, message=f"Executed creation request #{request_id}")

    @strawberry.mutation
    async def reject_creation_request(  # REQ-434, REQ-063
        self, info: StrawberryInfo, request_id: int, reason: str
    ) -> MutationResult:
        """REQ-434/063: a rights-holder rejects a queued request with an actionable reason."""
        from provisa.api.admin.capabilities import _identity_from_info, require_capability
        from provisa.core.repositories import creation_request as cr_repo

        if not reason or not reason.strip():
            return MutationResult(success=False, message="A rejection reason is required")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            req = await cr_repo.get(cast("Connection", conn), request_id)
            if req is None or req["status"] != "pending":
                return MutationResult(
                    success=False, message="Request not found or already resolved"
                )
            try:
                require_capability(info, req["capability"])
            except PermissionError as e:
                return MutationResult(success=False, message=str(e))
            identity = _identity_from_info(info)
            resolved_by = getattr(identity, "user_id", None) if identity is not None else None
            await cr_repo.mark_rejected(
                cast("Connection", conn), request_id, reason.strip(), resolved_by
            )
        return MutationResult(success=True, message=f"Rejected creation request #{request_id}")

    @strawberry.mutation
    async def upsert_relationship(  # REQ-019, REQ-020, REQ-366, REQ-434
        self, info: StrawberryInfo, input: RelationshipInput
    ) -> MutationResult:
        from provisa.api.admin.capabilities import has_capability

        # REQ-434/366: a user lacking create_relationship queues a request instead of erroring.
        if not has_capability(info, "create_relationship"):
            return await _queue_creation_request(info, "relationship", "create_relationship", input)
        from provisa.core.models import Relationship as RelModel, Cardinality
        from provisa.core.repositories import relationship as rel_repo
        from provisa.api.admin.capabilities import _identity_from_info

        pool = await _get_pool()
        try:
            Cardinality(input.cardinality)
        except ValueError:
            return MutationResult(
                success=False,
                message=f"Invalid cardinality: {input.cardinality!r}",
            )
        # REQ-020: record the defining steward as owner.
        _identity = _identity_from_info(info)
        _owner = getattr(_identity, "user_id", None) if _identity is not None else None
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
            owner=_owner,
        )
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            await rel_repo.upsert(_conn, model)
            if input.record_candidate and not input.target_function_name:
                _rres = await _conn.execute_core(
                    select(relationships.c.source_table_id, relationships.c.target_table_id).where(
                        relationships.c.id == input.id
                    )
                )
                rel_row = _rres.fetchone()
                if rel_row and rel_row.target_table_id is not None:
                    # DO UPDATE sets the same literal values it inserts (accepted / 1.0 /
                    # 'SQL modeling (admin)'), so an EXCLUDED-column upsert is equivalent.
                    await _conn.upsert(
                        relationship_candidates,
                        {
                            "source_table_id": rel_row.source_table_id,
                            "target_table_id": rel_row.target_table_id,
                            "source_column": input.source_column,
                            "target_column": input.target_column or None,
                            "cardinality": input.cardinality,
                            "confidence": 1.0,
                            "reasoning": "SQL modeling (admin)",
                            "suggested_name": input.id,
                            "scope": "admin",
                            "status": "accepted",
                        },
                        index_elements=[
                            "source_table_id",
                            "source_column",
                            "target_table_id",
                            "target_column",
                        ],
                        update_columns=["status", "confidence", "reasoning"],
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
            deleted = await rel_repo.delete(cast("Connection", conn), id)
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
            result = await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
            )
            if (result.rowcount or 0) == 0:
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
            result = await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(cache_ttl=cache_ttl)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(success=False, message=f"Table {table_id} not found")
        return MutationResult(success=True, message=f"Cache TTL updated for table {table_id}")

    @strawberry.mutation
    async def update_source_prefer_materialized(
        self, source_id: str, prefer_materialized: bool
    ) -> MutationResult:  # REQ-826
        """Force (or release) MATERIALIZED federation for a source's tables — the source-level default."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(prefer_materialized=prefer_materialized)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(success=False, message=f"Source {source_id!r} not found")
        return MutationResult(
            success=True, message=f"prefer_materialized set for source {source_id!r}"
        )

    @strawberry.mutation
    async def update_table_prefer_materialized(
        self, table_id: int, prefer_materialized: bool | None = None
    ) -> MutationResult:  # REQ-826
        """Override MATERIALIZED federation for one table; None = inherit the source-level default."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(prefer_materialized=prefer_materialized)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(success=False, message=f"Table {table_id} not found")
        return MutationResult(success=True, message=f"prefer_materialized set for table {table_id}")

    # ── Admin: Naming Convention ──

    @strawberry.mutation
    async def update_gql_naming_convention(
        self, convention: str
    ) -> MutationResult:  # REQ-253, REQ-416
        """Set the global naming convention and rebuild schemas for all roles."""
        from provisa.api.app import state

        from provisa.compiler import naming as _naming

        # REQ-416: reject free-form conventions; only the presets (and their aliases) are valid.
        err = _naming.validation_error_for_convention(convention)
        if err:
            return MutationResult(success=False, message=err)

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
            result = await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(gql_naming_convention=gql_naming_convention)
            )
            if (result.rowcount or 0) == 0:
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
            result = await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(allowed_domains=allowed_domains)
            )
            if (result.rowcount or 0) == 0:
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
            result = await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(gql_naming_convention=gql_naming_convention)
            )
            if (result.rowcount or 0) == 0:
                return MutationResult(success=False, message=f"Table {table_id} not found")
        await _rebuild_schemas()
        return MutationResult(
            success=True, message=f"Naming convention updated for table {table_id}"
        )

    # ── Admin: MV Management ──

    @strawberry.mutation
    async def refresh_mv(self, mv_id: str) -> MutationResult:  # REQ-133, REQ-158
        """Trigger a manual refresh of a materialized view."""
        from provisa.api.app import state

        mv = state.mv_registry.get(mv_id)
        if mv is None:
            return MutationResult(success=False, message=f"MV {mv_id!r} not found")
        try:
            from provisa.mv.refresh import refresh_mv

            assert state.federation_engine is not None
            await refresh_mv(state.federation_engine, mv, state.mv_registry)
            return MutationResult(success=True, message=f"MV {mv_id!r} refreshed")
        except Exception as e:
            logging.getLogger(__name__).exception("refresh_mv %r failed", mv_id)
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
            logging.getLogger(__name__).exception("purge_cache failed")
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
            logging.getLogger(__name__).exception("purge_cache_by_table %s failed", table_id)
            return MutationResult(success=False, message=str(e))

    @strawberry.mutation
    async def invalidate_file_source(self, table_id: int) -> MutationResult:
        """Force re-migration of a file-backed (SQLite) table into PG."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            _res = await _conn.execute_core(
                select(
                    registered_tables.c.table_name,
                    registered_tables.c.schema_name,
                    sources.c.type,
                    sources.c.path,
                    sources.c.id.label("source_id"),
                )
                .select_from(
                    registered_tables.join(sources, sources.c.id == registered_tables.c.source_id)
                )
                .where(registered_tables.c.id == table_id)
            )
            _srow = _res.fetchone()
            row = dict(_srow._mapping) if _srow is not None else None
            if not row:
                return MutationResult(success=False, message=f"Table {table_id} not found")
            if row["type"] != "sqlite":
                return MutationResult(
                    success=False, message=f"Source type {row['type']!r} is not sqlite"
                )
            from provisa.api.app import state as _state

            # An ATTACH engine (DuckDB) reads the sqlite file live — no replica to re-migrate (REQ-947).
            if engine_attaches(getattr(_state, "federation_engine", None), "sqlite"):
                return MutationResult(success=True, message="attached live; no migration needed")
            from provisa.file_source.pg_migrate import migrate_sqlite_table, record_mtime

            try:
                await _conn.execute_core(
                    _delete(file_source_mtimes).where(file_source_mtimes.c.table_id == table_id)
                )
                _pg_conn = cast("Connection", _conn)  # core Connection (proxies asyncpg)
                await migrate_sqlite_table(
                    row["path"], row["table_name"], _pg_conn, row["schema_name"], row["table_name"]
                )
                await record_mtime(table_id, row["path"], _pg_conn)
                return MutationResult(
                    success=True, message=f"Re-migrated {row['source_id']}.{row['table_name']}"
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    "invalidate_file_source: re-migration of table %s failed", table_id
                )
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

    async def refresh_source_statistics(self, source_id: str) -> MutationResult:  # REQ-276
        """Run ANALYZE on all registered tables for a source (Phase AL).

        Triggers the engine to collect fresh table statistics, which improves the
        quality of join-order and broadcast decisions for federated queries.
        """
        from provisa.api.app import state

        if state.federation_engine is None:
            return MutationResult(success=False, message="Query engine not available")

        pool = await _get_pool()
        if pool is None:
            return MutationResult(success=False, message="Database pool not available")

        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(registered_tables.c.schema_name, registered_tables.c.table_name).where(
                    registered_tables.c.source_id == source_id
                )
            )
            rows = _res.fetchall()

        if not rows:
            return MutationResult(
                success=False,
                message=f"No tables registered for source {source_id!r}",
            )

        analyzed: list[str] = []
        errors: list[str] = []
        source_catalog = source_to_catalog(source_id)

        for row in rows:
            full_name = f"{source_catalog}.{row.schema_name}.{row.table_name}"
            try:
                await state.federation_engine.execute_engine(f"ANALYZE {full_name}")
                analyzed.append(full_name)
            except Exception as exc:
                logging.getLogger(__name__).exception("ANALYZE %s failed", full_name)
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
    async def compile_query(self, input: CompileQueryInput) -> list[CompileQueryResult]:  # REQ-161
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
                    engine_sql=r.get("engine_sql"),
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
        return await _ops.deploy_view_to_db(info, table_id)
