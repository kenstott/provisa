# Copyright (c) 2026 Kenneth Stott
# Canary: 6ce0dc34-587c-4adf-bd5e-60e767a084c1
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

from provisa.core.repositories import rls as rls_repo
from provisa.federation.strategy import engine_attaches
from provisa.api.admin._config_io import config_path as _config_path, read_config
from provisa.api.admin.types import (
    CalendarInput,
    ColumnAliasType,
    CompileQueryInput,
    CompileQueryResult,
    DomainInput,
    DqDryRunCheckType,
    DqDryRunType,
    EnforcementType,
    EntityInput,
    FactInput,
    MetricInput,
    MutationResult,
    RelationshipInput,
    RLSRuleInput,
    RoleInput,
    SourceInput,
    TableInput,
    TagAssignmentInput,
    TagInput,
    TagParamValueInput,
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
    _federation_hints_from_input,
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


async def _upsert_relationship_impl(
    info: StrawberryInfo, input: RelationshipInput
) -> MutationResult:  # REQ-019, REQ-020, REQ-366, REQ-434
    """Shared body of the upsertRelationship mutation. Module-level so register_fact can
    create its dimension links directly — strawberry invokes root mutations with self=None,
    so a self.upsert_relationship call never works from inside another resolver."""
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
            code="schema.invalid_cardinality",
            params={"cardinality": input.cardinality},
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
        code="schema.relationship_saved",
        params={"relationship": input.id},
    )


def _assignment_target_problem(model) -> "MutationResult | None":  # REQ-1377
    """Reject an assignment whose typed target fields don't match its object_type."""
    from provisa.core.models import TAG_OBJECT_TYPES

    required = {
        "source": model.source_id,
        "table": model.table_id,
        "column": model.table_id is not None and model.column_name,
        "relationship": model.relationship_id,
        "command": model.command_name,
    }
    if model.object_type not in TAG_OBJECT_TYPES:
        return MutationResult(
            success=False,
            message=f"object_type must be one of {list(TAG_OBJECT_TYPES)}",
            code="schema.tag_bad_object_type",
            params={"objectType": model.object_type},
        )
    if not required[model.object_type]:
        return MutationResult(
            success=False,
            message=f"Missing target identifier for a {model.object_type!r} tag assignment",
            code="schema.tag_bad_target",
            params={"objectType": model.object_type},
        )
    return None


async def _refresh_config_tags() -> None:  # REQ-1373/1377
    """The DB is the source of truth for tags; mirror it into state.config for consumers
    (metadata export builder) that read the in-memory config."""
    from provisa.api.app import state
    from provisa.core.models import Tag as TagModel, TagAssignment as TagAssignmentModel
    from provisa.core.repositories import tag as tag_repo

    if state.config is None:
        return
    pool = await _get_pool()
    async with pool.acquire() as conn:
        tag_rows = await tag_repo.list_all(cast("Connection", conn))
        assignment_rows = await tag_repo.list_assignments(cast("Connection", conn))
    state.config.tags = [
        TagModel(
            id=r["id"],
            description=r["description"],
            applies_to=list(r["applies_to"] or []),
            is_system=bool(r["is_system"]),
            derived=bool(r["derived"]),
            reason_policy=r["reason_policy"],
            expires_policy=r["expires_policy"],
            param_policy=r["param_policy"],  # REQ-1467
        )
        for r in tag_rows
    ]
    state.config.tag_assignments = [
        TagAssignmentModel(
            tag_id=r["tag_id"],
            object_type=r["object_type"],
            source_id=r["source_id"],
            table_id=r["table_id"],
            column_name=r["column_name"],
            relationship_id=r["relationship_id"],
            command_name=r["command_name"],
            table_ref=r["table_ref"],
            reason=r["reason"],
            expires_on=r["expires_on"],
        )
        for r in assignment_rows
    ]


def _validate_load_protection(
    load_protected: bool | None,
    off_peak_window: str | None,
    cache_ttl: int | None,
    change_signal: str | None,
    who: str,
) -> "MutationResult | None":  # REQ-1141
    """Enforce the REQ-1141 ≥1-gate rule for a load-protected target. Returns a failing
    MutationResult when load protection is on but no gate is armed, else None."""
    if not load_protected:
        return None
    armed = (
        bool(off_peak_window)
        or cache_ttl is not None
        or (change_signal in ("probe", "ttl_probe"))
    )
    if not armed:
        return MutationResult(
            success=False,
            message=(
                f"{who}: load protection requires at least one refresh gate — set an off-peak "
                "window, a cache_ttl cadence, or a probing change_signal (probe/ttl_probe) (REQ-1141)"
            ),
            code="schema.load_protection_gate_required",
            params={"who": who},
        )
    return None


def _snapshot_load_protection_conflict(
    load_protected: bool | None,
    off_peak_window: str | None,
    mv_bitemporal_mode: str | None,
) -> bool:  # REQ-1170
    """True when load protection (off-peak window) AND snapshotting are both configured on one
    table — their timing can fight (a snapshot boundary may fall outside the off-peak window), so
    the caller emits a WARNING (not a block)."""
    return bool((load_protected or off_peak_window) and mv_bitemporal_mode)


def _parsed_off_peak(off_peak_window: str | None, tz: str) -> "MutationResult | None":  # REQ-1141
    """Validate an off-peak window spec at write time; returns a failing MutationResult on a
    malformed spec/zone, else None (no silent default window)."""
    if off_peak_window is None:
        return None
    from provisa.federation.scheduled_refresh import parse_off_peak_window

    try:
        parse_off_peak_window(off_peak_window, tz)
    except (ValueError, KeyError) as e:  # ZoneInfoNotFoundError subclasses KeyError
        return MutationResult(success=False, message=f"invalid off-peak window: {e}", code="schema.invalid_off_peak_window", params={"error": str(e)})
    return None


async def _refuse_over_source_limit(source_id: str) -> MutationResult | None:  # REQ-1513
    """Refuse a NEW source the org's plan has no room for, or None when there is room.

    The ceiling is the one the Billing page prints on the plan card, so the number an administrator
    chose the plan for is the number enforced here. Only a source the org does not already hold is
    tested: ``create_source`` is an upsert, and editing the connection details of a source that is
    already registered adds nothing to the count.

    None on a self-hosted deployment — there is no subscription, so there is no ceiling (REQ-1513).
    """
    from provisa.api.app import state
    from provisa.api.org_runtime import current_org
    from provisa.core.commerce import source_limit_for_org
    from provisa.core.repositories import source as source_repo

    org_id = current_org.get() or state.org_id
    limit = await source_limit_for_org(state, org_id)
    if limit is None:
        return None
    max_sources, plan = limit
    pool = await _get_pool()
    async with pool.acquire() as conn:
        if await source_repo.get(conn, source_id) is not None:
            return None
        held = await source_repo.count_billable(conn)
    if held < max_sources:
        return None
    return MutationResult(
        success=False,
        message=(
            f"This organization holds {held} of the {max_sources} data sources its {plan} plan "
            f"admits. Change the plan on the Billing page, or remove a source."
        ),
        code="schema.source_limit_reached",
        params={"source": source_id, "held": str(held), "limit": str(max_sources), "plan": plan},
    )


@strawberry.type
class Mutation:  # REQ-012, REQ-013, REQ-016, REQ-042
    @strawberry.mutation
    async def rebuild_schemas(self) -> MutationResult:
        """Rebuild in-memory schema from DB state. Useful after external DB changes."""
        await _rebuild_schemas()
        return MutationResult(success=True, message="Schemas rebuilt", code="schema.schemas_rebuilt")

    @strawberry.mutation
    async def dry_run_dq_contract(  # REQ-1443 clause 7
        self, source_id: str, contract_text: str
    ) -> DqDryRunType:
        """Run a contract against the live table and report the outcomes, landing none.

        A mutation rather than a query because it costs a real scan — a client that refetched it on
        cache invalidation would re-scan the table — but it writes nothing: the checker's rows go
        into the response instead of into the results table. What it proves is the thing a syntax
        check cannot: which governed table the dataset identifier actually resolved to."""
        from provisa.api.admin._dq_resolvers import dry_run_contract

        pool = await _get_pool()
        async with pool.acquire() as conn:
            result = await dry_run_contract(
                cast("Connection", conn), source_id=source_id, contract_text=contract_text
            )
        if not result["success"]:
            return DqDryRunType(success=False, message=result["message"])
        return DqDryRunType(
            success=True,
            message=result["message"],
            checker_version=result["checker_version"],
            checks=[DqDryRunCheckType(**c) for c in result["checks"]],
        )

    @strawberry.mutation
    async def create_calendar(self, input: "CalendarInput") -> MutationResult:  # REQ-962
        """Create/replace a versioned snapshot-boundary calendar (REQ-962). Validated by constructing
        the in-memory Calendar (fails loud on a bad base_system/tz/anchor) before it is persisted; a
        rebuild reloads the registry so a periodic MV can resolve it."""
        from datetime import date

        from provisa.core.repositories import calendar as calendar_repo
        from provisa.events.calendars import BaseSystem, Calendar

        try:
            Calendar(  # validation only — raises on an unknown base_system / bad tz
                name=input.name,
                version=input.version,
                base_system=BaseSystem(input.base_system),
                tz=input.tz,
                fiscal_anchor=(input.fiscal_anchor_month, input.fiscal_anchor_day),
                retail_anchor=date.fromisoformat(input.retail_anchor) if input.retail_anchor else None,
                week_start=input.week_start,
                holidays=frozenset(date.fromisoformat(d) for d in input.holidays),
                weekend=frozenset(input.weekend),
            )
        except (ValueError, KeyError) as e:
            return MutationResult(success=False, message=f"invalid calendar: {e}", code="schema.invalid_calendar", params={"error": str(e)})
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await calendar_repo.upsert(
                cast("Connection", conn),
                {
                    "name": input.name,
                    "version": input.version,
                    "base_system": input.base_system,
                    "tz": input.tz,
                    "fiscal_anchor_month": input.fiscal_anchor_month,
                    "fiscal_anchor_day": input.fiscal_anchor_day,
                    "retail_anchor": date.fromisoformat(input.retail_anchor)
                    if input.retail_anchor
                    else None,
                    "week_start": input.week_start,
                    "holidays": input.holidays,
                    "weekend": input.weekend,
                },
            )
        return MutationResult(
            success=True,
            message=f"calendar {input.name!r} v{input.version} saved",
            code="schema.calendar_saved",
            params={"calendar": input.name, "version": input.version},
        )

    @strawberry.mutation
    async def delete_calendar(self, name: str) -> MutationResult:  # REQ-962
        """Delete a snapshot-boundary calendar (all versions) — ONLY when no MV references it. A
        calendar in use MUST NOT be removed (its snapshots would lose their boundary source), so this
        fails loud with the usage count rather than orphaning a periodic MV."""
        from provisa.core.repositories import calendar as calendar_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            used_by = await calendar_repo.usage_count(_conn, name)
            if used_by > 0:
                return MutationResult(
                    success=False,
                    message=f"calendar {name!r} is in use by {used_by} materialized view(s) — "
                    "clear their snapshot schedule before deleting",
                    code="schema.calendar_in_use",
                    params={"calendar": name, "count": used_by},
                )
            removed = await calendar_repo.delete(_conn, name)
        if removed == 0:
            return MutationResult(success=False, message=f"calendar {name!r} not found", code="schema.calendar_not_found", params={"calendar": name})
        return MutationResult(success=True, message=f"calendar {name!r} deleted", code="schema.calendar_deleted", params={"calendar": name})

    @strawberry.mutation
    async def create_source(
        self, info: StrawberryInfo, input: SourceInput
    ) -> MutationResult:  # REQ-012, REQ-013
        from provisa.api.admin.capabilities import require_capability

        require_capability(info, "source_registration")
        from provisa.core.models import Source as SourceModel, SourceType as SourceTypeEnum

        _limit_refusal = await _refuse_over_source_limit(input.id)
        if _limit_refusal is not None:
            return _limit_refusal

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
            federation_hints=_federation_hints_from_input(input),
            change_signal=input.change_signal,
            load_protected=input.load_protected,  # REQ-1141
            off_peak_window=input.off_peak_window,  # REQ-1141
            off_peak_tz=input.off_peak_tz,  # REQ-1141
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
                code="schema.source_connection_failed",
                params={"source": input.id, "error": str(_conn_err)},
            )

        await _upsert_source_with_domains(pool, model, input)

        if input.type == "govdata" and input.username:
            _configure_govdata_env(input)

        _domains = [d for d in (input.allowed_domains or []) if d.strip()]
        if _domains:
            state.source_allowed_domains[input.id] = _domains
        state.source_types[input.id] = input.type
        state.source_dialects[input.id] = ""
        if model.federation_hints:
            # Mirrors _populate_source_catalog_names in app_loaders.py: the config path publishes
            # the hints to runtime state, so the dynamic path must too.
            state.source_federation_hints[input.id] = dict(model.federation_hints)

        # Populate the org-scoped catalog name so catalog_for() resolves this source
        # after dynamic creation (mirrors _populate_source_catalog_names in app_loaders.py).
        from provisa.api.app_loaders import fixed_catalog_for_engine
        from provisa.api.org_runtime import current_org
        from provisa.compiler.naming import org_prefixed_catalog, source_to_catalog
        # The physical catalog is derived from the source id and nothing else — create_catalog
        # (provisa/core/catalog.py:116) names it `_to_catalog_name(source.id)`, and native engines
        # attach by source id too. `input.database` is the *remote* database/tenant the connector
        # talks to, never a catalog name: a SharePoint source puts its Azure tenant GUID there, so
        # deriving the catalog from it recorded `"5d2609cc-…"` for a catalog physically created as
        # `e2e_sharepoint`, and every engine query for the source died on CATALOG_NOT_FOUND.
        # Exception: a fixed-catalog warehouse engine (BigQuery/Fabric/Synapse) pins every source to
        # the one warehouse catalog instead — see fixed_catalog_for_engine, mirrored from the
        # config-load path in _populate_source_catalog_names.
        _building_org = current_org.get() or state.org_id
        state.source_catalogs[input.id] = fixed_catalog_for_engine(state) or org_prefixed_catalog(
            _building_org, source_to_catalog(input.id), default_org=state.org_id
        )

        # Provision on the bound engine (the engine makes a catalog; native engines no-op / attach lazily).
        _register_source_on_engine(state, model, input)
        await _analyze_source_on_engine(state, pool, model, input)

        if input.type == "govdata" and input.database and input.username:
            _prime_govdata_cache(input)

        _fire_catalog_indexing(state, pool, input)

        return MutationResult(success=True, message=f"Source {input.id!r} created", code="schema.source_created", params={"source": input.id})

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
                return MutationResult(success=False, message=f"Source {input.id!r} not found", code="schema.source_not_found", params={"source": input.id})
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
                federation_hints=_federation_hints_from_input(input),
                change_signal=input.change_signal,
                load_protected=input.load_protected,  # REQ-1141
                off_peak_window=input.off_peak_window,  # REQ-1141
                off_peak_tz=input.off_peak_tz,  # REQ-1141
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

        # Keep catalog name in sync with the (possibly renamed) source config.
        from provisa.api.app_loaders import fixed_catalog_for_engine
        from provisa.api.org_runtime import current_org
        from provisa.compiler.naming import org_prefixed_catalog, source_to_catalog
        # Source id only — see the same derivation in create_source above for why `input.database`
        # (the remote database/tenant) is not a catalog name, and the fixed-catalog exception.
        _building_org = current_org.get() or state.org_id
        state.source_catalogs[input.id] = fixed_catalog_for_engine(state) or org_prefixed_catalog(
            _building_org, source_to_catalog(input.id), default_org=state.org_id
        )

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

        return MutationResult(success=True, message=f"Source {input.id!r} updated", code="schema.source_updated", params={"source": input.id})

    @strawberry.mutation
    async def rename_source(self, old_id: str, new_id: str) -> MutationResult:
        from provisa.core.repositories import source as source_repo

        if not new_id.strip():
            return MutationResult(success=False, message="New ID must not be empty", code="schema.new_id_empty")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            renamed = await source_repo.rename(cast("Connection", conn), old_id, new_id)
        if renamed:
            return MutationResult(
                success=True,
                message=f"Source renamed {old_id!r} → {new_id!r}",
                code="schema.source_renamed",
                params={"old": old_id, "new": new_id},
            )
        return MutationResult(success=False, message=f"Source {old_id!r} not found", code="schema.source_not_found", params={"source": old_id})

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
            return MutationResult(success=True, message=f"Source {id!r} deleted", code="schema.source_deleted", params={"source": id})
        return MutationResult(success=False, message=f"Source {id!r} not found", code="schema.source_not_found", params={"source": id})

    @strawberry.mutation
    async def create_domain(self, input: DomainInput) -> MutationResult:  # REQ-021
        from provisa.api.metadata_export.refs import RESERVED_KIND_KEYWORDS
        from provisa.core.models import Domain as DomainModel
        from provisa.core.repositories import domain as domain_repo

        # REQ-1385: kind keywords are reserved URI path segments; a domain with one of these
        # names would make its semantic URIs unparseable.
        if input.id in RESERVED_KIND_KEYWORDS:
            return MutationResult(
                success=False,
                message=f"Domain id {input.id!r} is a reserved word",
                code="schema.domain_reserved_word",
                params={"domain": input.id},
            )
        pool = await _get_pool()
        model = DomainModel(
            id=input.id,
            description=input.description,
            steward=input.steward or None,  # REQ-609
            graphql_alias=input.graphql_alias or None,
        )
        async with pool.acquire() as conn:
            await domain_repo.upsert(cast("Connection", conn), model)
        return MutationResult(success=True, message=f"Domain {input.id!r} created", code="schema.domain_created", params={"domain": input.id})

    @strawberry.mutation
    async def delete_domain(self, id: str) -> MutationResult:
        from provisa.core.repositories import domain as domain_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await domain_repo.delete(cast("Connection", conn), id)
        if deleted:
            return MutationResult(success=True, message=f"Domain {id!r} deleted", code="schema.domain_deleted", params={"domain": id})
        return MutationResult(success=False, message=f"Domain {id!r} not found", code="schema.domain_not_found", params={"domain": id})

    @strawberry.mutation
    async def upsert_tag(self, input: TagInput) -> MutationResult:  # REQ-1373, REQ-1375
        from provisa.core.models import (
            DERIVED_TAG_IDS,
            SYSTEM_TAG_IDS,
            TAG_FIELD_POLICIES,
            TAG_OBJECT_TYPES,
            TAG_PARAM_POLICIES,
            TAG_PARAM_SEPARATOR,
            Tag as TagModel,
        )
        from provisa.core.repositories import tag as tag_repo

        # REQ-1467: a registry id is a base id. Accepting "entity:customer" here would define a
        # tag whose id parses as the system `entity` tag carrying a parameter, and every base-id
        # comparison downstream would then resolve the user tag to the intrinsic.
        if TAG_PARAM_SEPARATOR in input.id:
            return MutationResult(
                success=False,
                message=(
                    f"Tag id {input.id!r} may not contain {TAG_PARAM_SEPARATOR!r} — "
                    "the separator introduces a parameter value on an assignment"
                ),
                code="schema.tag_id_has_separator",
                params={"tag": input.id},
            )
        # REQ-1443: a derived tag is code-defined like a system tag, so it is reserved the same way.
        if input.id in SYSTEM_TAG_IDS + DERIVED_TAG_IDS:
            return MutationResult(
                success=False,
                message=f"Tag {input.id!r} is a system tag and cannot be redefined",
                code="schema.tag_system_immutable",
                params={"tag": input.id},
            )
        bad_scopes = [s for s in input.applies_to if s not in TAG_OBJECT_TYPES]
        if bad_scopes or not input.applies_to:
            return MutationResult(
                success=False,
                message=f"applies_to must be a non-empty subset of {list(TAG_OBJECT_TYPES)}",
                code="schema.tag_bad_scope",
                params={"tag": input.id, "scopes": ",".join(bad_scopes)},
            )
        for policy in (input.reason_policy, input.expires_policy):
            if policy not in TAG_FIELD_POLICIES:
                return MutationResult(
                    success=False,
                    message=f"Field policy must be one of {list(TAG_FIELD_POLICIES)}",
                    code="schema.tag_bad_policy",
                    params={"tag": input.id, "policy": policy},
                )
        if input.param_policy not in TAG_PARAM_POLICIES:  # REQ-1467
            return MutationResult(
                success=False,
                message=f"Parameter policy must be one of {list(TAG_PARAM_POLICIES)}",
                code="schema.tag_bad_param_policy",
                params={"tag": input.id, "policy": input.param_policy},
            )
        model = TagModel(
            id=input.id,
            description=input.description,
            applies_to=list(input.applies_to),
            reason_policy=input.reason_policy,
            expires_policy=input.expires_policy,
            param_policy=input.param_policy,
        )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await tag_repo.upsert(cast("Connection", conn), model)
        await _refresh_config_tags()
        return MutationResult(
            success=True,
            message=f"Tag {input.id!r} saved",
            code="schema.tag_saved",
            params={"tag": input.id},
        )

    @strawberry.mutation
    async def delete_tag(self, id: str) -> MutationResult:  # REQ-1373, REQ-1375
        from provisa.core.models import DERIVED_TAG_IDS, SYSTEM_TAG_IDS, base_tag_id
        from provisa.core.repositories import tag as tag_repo

        # REQ-1467: on the base id, so "entity:customer" is refused as the system tag it names
        # rather than looked up as a user tag, found missing, and reported as not found.
        if base_tag_id(id) in SYSTEM_TAG_IDS + DERIVED_TAG_IDS:
            return MutationResult(
                success=False,
                message=f"Tag {id!r} is a system tag and cannot be deleted",
                code="schema.tag_system_immutable",
                params={"tag": id},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await tag_repo.delete(cast("Connection", conn), id)
        if not deleted:
            return MutationResult(
                success=False,
                message=f"Tag {id!r} not found",
                code="schema.tag_not_found",
                params={"tag": id},
            )
        await _refresh_config_tags()
        return MutationResult(
            success=True,
            message=f"Tag {id!r} deleted",
            code="schema.tag_deleted",
            params={"tag": id},
        )

    @strawberry.mutation
    async def assign_tag(self, input: TagAssignmentInput) -> MutationResult:  # REQ-1376/1377
        from provisa.core.models import TagAssignment as TagAssignmentModel
        from provisa.core.repositories import tag as tag_repo

        model = TagAssignmentModel(
            tag_id=input.tag_id,
            object_type=input.object_type,
            source_id=input.source_id,
            table_id=input.table_id,
            column_name=input.column_name,
            relationship_id=input.relationship_id,
            command_name=input.command_name,
            reason=(input.reason or "").strip() or None,
            expires_on=(input.expires_on or "").strip() or None,
        )
        problem = _assignment_target_problem(model)
        if problem is not None:
            return problem
        if model.expires_on is not None:
            import datetime as _dt

            try:
                _dt.date.fromisoformat(model.expires_on)
            except ValueError:
                return MutationResult(
                    success=False,
                    message=f"expires_on must be an ISO date, got {model.expires_on!r}",
                    code="schema.tag_bad_expires_on",
                    params={"expiresOn": model.expires_on},
                )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            tag_row = await tag_repo.get(cast("Connection", conn), input.tag_id)
            if tag_row is None:
                return MutationResult(
                    success=False,
                    message=f"Tag {input.tag_id!r} not found",
                    code="schema.tag_not_found",
                    params={"tag": input.tag_id},
                )
            # REQ-1443: a derived tag reports state the table already carries, so assigning it
            # would either duplicate that state or contradict it — the registration is the only
            # way to change it.
            if tag_row["derived"]:
                return MutationResult(
                    success=False,
                    message=(
                        f"Tag {input.tag_id!r} is derived from the object's own registration "
                        "and cannot be assigned"
                    ),
                    code="schema.tag_derived_immutable",
                    params={"tag": input.tag_id},
                )
            # REQ-1375: the registry's per-tag field policy governs the assignment fields —
            # a required field refuses absence, a hidden field refuses presence.
            for field_name, value, policy in (
                ("reason", model.reason, tag_row["reason_policy"]),
                ("expires_on", model.expires_on, tag_row["expires_policy"]),
            ):
                if policy == "required" and value is None:
                    return MutationResult(
                        success=False,
                        message=f"Tag {input.tag_id!r} requires {field_name}",
                        code="schema.tag_reason_required"
                        if field_name == "reason"
                        else "schema.tag_expires_required",
                        params={"tag": input.tag_id, "field": field_name},
                    )
                if policy == "hidden" and value is not None:
                    return MutationResult(
                        success=False,
                        message=f"Tag {input.tag_id!r} does not take {field_name}",
                        code="schema.tag_field_hidden",
                        params={"tag": input.tag_id, "field": field_name},
                    )
            # REQ-1467: the parameter is part of the assignment, and the permitted values are a
            # closed maintainer-owned list. An unlisted value is refused rather than stored: a
            # misspelt "entity:custmoer" indexes the column's values under a type nothing
            # queries, and the empty result reads as absence rather than as the typo it is.
            param = model.tag_param()
            if tag_row["param_policy"] == "required":
                if param is None:
                    return MutationResult(
                        success=False,
                        message=(
                            f"Tag {input.tag_id!r} must be assigned with a value, "
                            f"as {input.tag_id}:<value>"
                        ),
                        code="schema.tag_param_required",
                        params={"tag": input.tag_id},
                    )
                permitted = {
                    p["value"]
                    for p in await tag_repo.list_param_values(
                        cast("Connection", conn), input.tag_id
                    )
                }
                if param not in permitted:
                    return MutationResult(
                        success=False,
                        message=(
                            f"{param!r} is not a permitted value for tag "
                            f"{model.base_tag_id()!r} — choose one of {sorted(permitted)} "
                            "or add it to the tag's value list"
                        ),
                        code="schema.tag_param_unknown",
                        params={"tag": model.base_tag_id(), "value": param},
                    )
            elif param is not None:
                return MutationResult(
                    success=False,
                    message=f"Tag {model.base_tag_id()!r} does not take a value",
                    code="schema.tag_param_not_allowed",
                    params={"tag": model.base_tag_id(), "value": param},
                )
            if input.object_type not in list(tag_row["applies_to"] or []):
                return MutationResult(
                    success=False,
                    message=(
                        f"Tag {input.tag_id!r} does not apply to {input.object_type!r} objects"
                    ),
                    code="schema.tag_scope_mismatch",
                    params={"tag": input.tag_id, "objectType": input.object_type},
                )
            await tag_repo.assign(cast("Connection", conn), model)
        await _refresh_config_tags()
        return MutationResult(
            success=True,
            message=f"Tag {input.tag_id!r} assigned",
            code="schema.tag_assigned",
            params={"tag": input.tag_id, "objectKey": model.object_key()},
        )

    @strawberry.mutation
    async def unassign_tag(self, input: TagAssignmentInput) -> MutationResult:  # REQ-1377
        from provisa.core.models import TagAssignment as TagAssignmentModel
        from provisa.core.repositories import tag as tag_repo

        model = TagAssignmentModel(
            tag_id=input.tag_id,
            object_type=input.object_type,
            source_id=input.source_id,
            table_id=input.table_id,
            column_name=input.column_name,
            relationship_id=input.relationship_id,
            command_name=input.command_name,
        )
        problem = _assignment_target_problem(model)
        if problem is not None:
            return problem
        pool = await _get_pool()
        async with pool.acquire() as conn:
            removed = await tag_repo.unassign(
                cast("Connection", conn), input.tag_id, model.object_key()
            )
        if not removed:
            return MutationResult(
                success=False,
                message=f"Tag {input.tag_id!r} is not assigned to that object",
                code="schema.tag_assignment_not_found",
                params={"tag": input.tag_id, "objectKey": model.object_key()},
            )
        await _refresh_config_tags()
        return MutationResult(
            success=True,
            message=f"Tag {input.tag_id!r} unassigned",
            code="schema.tag_unassigned",
            params={"tag": input.tag_id, "objectKey": model.object_key()},
        )

    @strawberry.mutation
    async def upsert_tag_param_value(self, input: TagParamValueInput) -> MutationResult:  # REQ-1467
        """Add or re-describe a permitted parameter value for a parameterized tag.

        The value list is data, not definition — it is editable on a system tag, whose definition
        is not. An org that trades in vessels adds ``entity:vessel`` here; nothing in code has to
        know the word.
        """
        from provisa.core.models import TAG_PARAM_SEPARATOR, TagParamValue
        from provisa.core.repositories import tag as tag_repo

        value = input.value.strip()
        if not value or TAG_PARAM_SEPARATOR in value:
            return MutationResult(
                success=False,
                message=(
                    f"Parameter value {input.value!r} must be non-empty and may not contain "
                    f"{TAG_PARAM_SEPARATOR!r}"
                ),
                code="schema.tag_param_value_invalid",
                params={"tag": input.tag_id, "value": input.value},
            )
        pool = await _get_pool()
        async with pool.acquire() as conn:
            tag_row = await tag_repo.get(cast("Connection", conn), input.tag_id)
            if tag_row is None:
                return MutationResult(
                    success=False,
                    message=f"Tag {input.tag_id!r} not found",
                    code="schema.tag_not_found",
                    params={"tag": input.tag_id},
                )
            if tag_row["param_policy"] == "none":
                return MutationResult(
                    success=False,
                    message=f"Tag {input.tag_id!r} does not take values",
                    code="schema.tag_param_not_allowed",
                    params={"tag": input.tag_id, "value": value},
                )
            await tag_repo.upsert_param_value(
                cast("Connection", conn),
                TagParamValue(
                    tag_id=input.tag_id, value=value, description=input.description.strip()
                ),
            )
        return MutationResult(
            success=True,
            message=f"Value {value!r} saved for tag {input.tag_id!r}",
            code="schema.tag_param_value_saved",
            params={"tag": input.tag_id, "value": value},
        )

    @strawberry.mutation
    async def delete_tag_param_value(self, tag_id: str, value: str) -> MutationResult:  # REQ-1467
        """Remove a permitted value, refusing while any assignment still carries it.

        Deleting a value in use would leave those assignments naming a type the list no longer
        admits — legal in the database, unreachable from the picker, and silently unfixable.
        """
        from provisa.core.repositories import tag as tag_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            in_use = await tag_repo.param_value_assignment_count(
                cast("Connection", conn), tag_id, value
            )
            if in_use:
                return MutationResult(
                    success=False,
                    message=(
                        f"Value {value!r} is carried by {in_use} assignment(s) — "
                        "unassign them first"
                    ),
                    code="schema.tag_param_value_in_use",
                    params={"tag": tag_id, "value": value, "count": str(in_use)},
                )
            deleted = await tag_repo.delete_param_value(cast("Connection", conn), tag_id, value)
        if not deleted:
            return MutationResult(
                success=False,
                message=f"Tag {tag_id!r} has no value {value!r}",
                code="schema.tag_param_value_not_found",
                params={"tag": tag_id, "value": value},
            )
        return MutationResult(
            success=True,
            message=f"Value {value!r} removed from tag {tag_id!r}",
            code="schema.tag_param_value_deleted",
            params={"tag": tag_id, "value": value},
        )

    @strawberry.mutation
    async def create_role(
        self, input: RoleInput
    ) -> MutationResult:  # REQ-042, REQ-059, REQ-060, REQ-215
        from provisa.core.models import Role as RoleModel
        from provisa.core.models import RoleRateLimit
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        # REQ-1174: carry the per-role rate + query-complexity limits through to the model/DB.
        rl = input.rate_limit
        rate_limit = (
            RoleRateLimit(
                requests_per_second=rl.requests_per_second,
                max_query_depth=rl.max_query_depth,
                max_query_nodes=rl.max_query_nodes,
                max_query_time_ms=rl.max_query_time_ms,
            )
            if rl is not None
            else None
        )
        model = RoleModel(
            id=input.id,
            capabilities=input.capabilities,
            domain_access=input.domain_access,
            rate_limit=rate_limit,
        )
        async with pool.acquire() as conn:
            await role_repo.upsert(cast("Connection", conn), model)
        return MutationResult(success=True, message=f"Role {input.id!r} created", code="schema.role_created", params={"role": input.id})

    @strawberry.mutation
    async def register_table(
        self, info: StrawberryInfo, input: TableInput
    ) -> MutationResult:  # REQ-013, REQ-016, REQ-252, REQ-366, REQ-413, REQ-432, REQ-433, REQ-434
        return await _ops.register_table(info, input)

    @strawberry.mutation
    async def register_entity(self, info: StrawberryInfo, input: "EntityInput") -> MutationResult:
        """REQ-1164: entity sugar → lower to a (bitemporal, when historized) MV and register it."""
        from provisa.api.admin.modeling_register import entity_table_input

        return await _ops.register_table(info, entity_table_input(input))

    @strawberry.mutation
    async def register_fact(self, info: StrawberryInfo, input: "FactInput") -> MutationResult:
        """REQ-1164: fact sugar → lower to an aggregate MV + dimension relationships and register."""
        from provisa.api.admin.modeling_register import fact_table_input

        from provisa.core.repositories import metric as metric_repo

        ti, rels, fact_metrics = fact_table_input(input)
        res = await _ops.register_table(info, ti)
        if not res.success:
            return res
        # Relationships resolve tables by their VIRTUAL name (alias when set, else
        # table_name — find_by_table_name). Registration may auto-alias under the org
        # naming convention (dim_pet → dimPet), so resolve each side before linking.
        pool = await _get_pool()

        async def _virtual_name(name: str) -> str:
            async with pool.acquire() as conn:
                row = (
                    await conn.execute_core(
                        select(registered_tables.c.alias).where(
                            registered_tables.c.table_name == name
                        )
                    )
                ).fetchone()
            if row is None:
                raise ValueError(f"table {name!r} is not registered")
            return row.alias or name

        for rel in rels:
            rel.source_table_id = await _virtual_name(rel.source_table_id)
            rel.target_table_id = await _virtual_name(rel.target_table_id)
            rr = await _upsert_relationship_impl(info, rel)
            if not rr.success:
                return rr
        # REQ-1320: each fact measure auto-registers as a governed metric (upsert by name).
        async with pool.acquire() as conn:
            for m in fact_metrics:
                await metric_repo.upsert(cast("Connection", conn), m)
        if fact_metrics:
            await _rebuild_schemas()  # republish state.metrics + schema metric blocks
        return MutationResult(
            success=True,
            message=(
                f"Fact {input.name!r} registered with {len(rels)} dimension link(s) "
                f"and {len(fact_metrics)} metric(s)"
            ),
            code="schema.fact_registered",
            params={"fact": input.name, "links": len(rels), "metrics": len(fact_metrics)},
        )

    @strawberry.mutation
    async def upsert_metric(self, info: StrawberryInfo, input: MetricInput) -> MutationResult:  # REQ-1317
        """Create or replace a governed metric definition (REQ-1317). The expression must parse
        under sqlglot and contain at least one aggregate function — hard error otherwise."""
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.models import Metric as MetricModel
        from provisa.core.repositories import metric as metric_repo

        require_capability(info, "table_registration")
        try:
            model = MetricModel(
                name=input.name,
                expression=input.expression,
                datatype=input.datatype,
                description=input.description,
                ai_context=input.ai_context,
                visible_to=list(input.visible_to),
            )
        except ValueError as e:  # pydantic name validation (snake_case)
            return MutationResult(success=False, message=str(e))
        pool = await _get_pool()
        try:
            async with pool.acquire() as conn:
                await metric_repo.upsert(cast("Connection", conn), model)
                # REQ-1318: every registered view whose view_metrics spec references this
                # metric regenerates its stored view_sql against the UPDATED definition.
                # Free-hand view_sql born from inline metric() calls carries no stored
                # provenance and is not regenerated (config-path views regenerate on reload).
                from provisa.api.admin._metric_views import regenerate_metric_views

                regenerated = await regenerate_metric_views(cast("Connection", conn), input.name)
        except ValueError as e:  # REQ-1317/1318: invalid expression / spec no longer compiles
            return MutationResult(success=False, message=str(e))
        # Always rebuild: state.metrics (raw-SQL `metrics.<name>` expansion) and the
        # per-role schema metric blocks republish from the DB registry on rebuild.
        await _rebuild_schemas()
        if regenerated:
            return MutationResult(
                success=True,
                message=(
                    f"Metric {input.name!r} saved; regenerated view(s): "
                    + ", ".join(sorted(regenerated))
                ),
                code="schema.metric_saved_regenerated",
                params={"metric": input.name, "views": ", ".join(sorted(regenerated))},
            )
        return MutationResult(success=True, message=f"Metric {input.name!r} saved", code="schema.metric_saved", params={"metric": input.name})

    @strawberry.mutation
    async def delete_metric(self, info: StrawberryInfo, name: str) -> MutationResult:  # REQ-1317
        """Delete a governed metric definition by name (REQ-1317)."""
        from provisa.api.admin.capabilities import require_capability
        from provisa.core.repositories import metric as metric_repo

        require_capability(info, "table_registration")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await metric_repo.delete(cast("Connection", conn), name)
        if deleted:
            await _rebuild_schemas()  # republish state.metrics + schema metric blocks
            return MutationResult(success=True, message=f"Metric {name!r} deleted", code="schema.metric_deleted", params={"metric": name})
        return MutationResult(success=False, message=f"Metric {name!r} not found", code="schema.metric_not_found", params={"metric": name})

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
        # REQ-957/964: reject a non-deterministic / unsafe preprocess hook at registration.
        from provisa.mv.preprocess import validate_preprocess

        try:
            validate_preprocess(input.mv_preprocess)
        except ValueError as _pp_err:
            return MutationResult(success=False, message=str(_pp_err))
        model = _table_model_from_input(input, columns, presets, input.alias)
        async with pool.acquire() as conn:
            _conn = cast("Connection", conn)
            # REQ-1443: same contract-driven derivation the register path and the YAML loader run.
            from provisa.api.admin._dq_registration import apply_dq_registration

            try:
                await apply_dq_registration(_conn, model)
            except ValueError as _dq_err:
                return MutationResult(success=False, message=str(_dq_err))
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
                    consistency=input.mv_consistency,  # REQ-879
                    preprocess=input.mv_preprocess,  # REQ-957
                    bitemporal_mode=input.mv_bitemporal_mode,  # REQ-1162
                    bitemporal_key=list(input.mv_bitemporal_key),  # REQ-1162
                    persist=input.mv_persist,  # REQ-965
                    primary_key=list(input.mv_primary_key),  # REQ-970
                    incremental=input.mv_incremental,  # REQ-969
                    calendar=input.mv_calendar,  # REQ-962
                    grain=input.mv_grain,  # REQ-962/1168
                    allowed_lateness=input.mv_allowed_lateness,  # REQ-961
                    expected_events=input.mv_expected_events,  # REQ-961
                    business_day_grain=input.mv_business_day_grain,  # REQ-962
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
            code="schema.table_updated",
            params={"table": input.table_name, "id": table_id},
        )

    @strawberry.mutation
    async def delete_table(self, id: int) -> MutationResult:
        from provisa.core.repositories import table as table_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await table_repo.delete(cast("Connection", conn), id)
        if deleted:
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Table {id} deleted", code="schema.table_deleted", params={"table": id})
        return MutationResult(success=False, message=f"Table {id} not found", code="schema.table_not_found", params={"table": id})

    @strawberry.mutation
    async def delete_role(self, id: str) -> MutationResult:
        from provisa.core.repositories import role as role_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await role_repo.delete(cast("Connection", conn), id)
        if deleted:
            return MutationResult(success=True, message=f"Role {id!r} deleted", code="schema.role_deleted", params={"role": id})
        return MutationResult(success=False, message=f"Role {id!r} not found", code="schema.role_not_found", params={"role": id})

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
            code="schema.rls_rule_saved_domain" if input.domain_id else "schema.rls_rule_saved_table",
            params=(
                {"domain": input.domain_id, "role": input.role_id}
                if input.domain_id
                else {"table": input.table_id, "role": input.role_id}
            ),
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
            return MutationResult(success=True, message="RLS rule deleted", code="schema.rls_rule_deleted")
        return MutationResult(success=False, message="RLS rule not found", code="schema.rls_rule_not_found")

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
            return MutationResult(success=False, message="Request not found or already resolved", code="schema.request_not_pending")
        try:
            require_capability(info, req["capability"])
        except PermissionError as e:
            return MutationResult(success=False, message=str(e))

        if req["request_type"] == "relationship":
            # Same strawberry-decorator signature limitation as above.
            result = await self.upsert_relationship(  # pyright: ignore[reportCallIssue]
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
                return MutationResult(success=False, message=f"Webhook {wh_name!r} not found", code="schema.webhook_not_found", params={"webhook": wh_name})
            from provisa.api.app import _rebuild_schemas

            await _rebuild_schemas()
            result = MutationResult(success=True, message=f"Approved webhook {wh_name!r}", code="schema.webhook_approved", params={"webhook": wh_name})
        else:
            return MutationResult(
                success=False,
                message=f"Unknown request type {req['request_type']!r}",
                code="schema.unknown_request_type",
                params={"type": req["request_type"]},
            )
        if not result.success:
            return result

        identity = _identity_from_info(info)
        resolved_by = getattr(identity, "user_id", None) if identity is not None else None
        async with pool.acquire() as conn:
            await cr_repo.mark_executed(cast("Connection", conn), request_id, resolved_by)
        return MutationResult(success=True, message=f"Executed creation request #{request_id}", code="schema.request_executed", params={"id": request_id})

    @strawberry.mutation
    async def reject_creation_request(  # REQ-434, REQ-063
        self, info: StrawberryInfo, request_id: int, reason: str
    ) -> MutationResult:
        """REQ-434/063: a rights-holder rejects a queued request with an actionable reason."""
        from provisa.api.admin.capabilities import _identity_from_info, require_capability
        from provisa.core.repositories import creation_request as cr_repo

        if not reason or not reason.strip():
            return MutationResult(success=False, message="A rejection reason is required", code="schema.rejection_reason_required")
        pool = await _get_pool()
        async with pool.acquire() as conn:
            req = await cr_repo.get(cast("Connection", conn), request_id)
            if req is None or req["status"] != "pending":
                return MutationResult(
                    success=False,
                    message="Request not found or already resolved",
                    code="schema.request_not_pending",
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
        return MutationResult(success=True, message=f"Rejected creation request #{request_id}", code="schema.request_rejected", params={"id": request_id})

    @strawberry.mutation
    async def upsert_relationship(  # REQ-019, REQ-020, REQ-366, REQ-434
        self, info: StrawberryInfo, input: RelationshipInput
    ) -> MutationResult:
        return await _upsert_relationship_impl(info, input)

    @strawberry.mutation
    async def delete_relationship(self, id: str) -> MutationResult:
        from provisa.core.repositories import relationship as rel_repo

        pool = await _get_pool()
        async with pool.acquire() as conn:
            deleted = await rel_repo.delete(cast("Connection", conn), id)
        if deleted:
            await _rebuild_schemas()
            return MutationResult(success=True, message=f"Relationship {id!r} deleted", code="schema.relationship_deleted", params={"relationship": id})
        return MutationResult(success=False, message=f"Relationship {id!r} not found", code="schema.relationship_not_found", params={"relationship": id})

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
                return MutationResult(success=False, message=f"Source {source_id!r} not found", code="schema.source_not_found", params={"source": source_id})
        return MutationResult(
            success=True,
            message=f"Cache settings updated for source {source_id!r}",
            code="schema.source_cache_updated",
            params={"source": source_id},
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
                return MutationResult(success=False, message=f"Table {table_id} not found", code="schema.table_not_found", params={"table": table_id})
        return MutationResult(success=True, message=f"Cache TTL updated for table {table_id}", code="schema.table_cache_updated", params={"table": table_id})

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
                return MutationResult(success=False, message=f"Source {source_id!r} not found", code="schema.source_not_found", params={"source": source_id})
        return MutationResult(
            success=True,
            message=f"prefer_materialized set for source {source_id!r}",
            code="schema.source_prefer_materialized_set",
            params={"source": source_id},
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
                return MutationResult(success=False, message=f"Table {table_id} not found", code="schema.table_not_found", params={"table": table_id})
        return MutationResult(success=True, message=f"prefer_materialized set for table {table_id}", code="schema.table_prefer_materialized_set", params={"table": table_id})

    @strawberry.mutation
    async def update_source_load_protection(
        self,
        source_id: str,
        load_protected: bool,
        off_peak_window: str | None = None,
        off_peak_tz: str = "UTC",
    ) -> MutationResult:  # REQ-1141
        """Mark a source load-protected (scheduled-refresh-only) and set its off-peak window.

        Enforces the REQ-1141 rule that a load-protected source MUST arm at least one refresh gate
        (off-peak window, a cache_ttl cadence, or a probing change_signal); a validation failure is a
        governed error, never a silently-accepted no-gate config."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(sources.c.cache_ttl, sources.c.change_signal).where(
                    sources.c.id == source_id
                )
            )
            row = _res.fetchone()
            if row is None:
                return MutationResult(success=False, message=f"Source {source_id!r} not found", code="schema.source_not_found", params={"source": source_id})
            err = _validate_load_protection(
                load_protected, off_peak_window, row.cache_ttl, row.change_signal, source_id
            )
            if err is not None:
                return err
            _window = _parsed_off_peak(off_peak_window, off_peak_tz)
            if isinstance(_window, MutationResult):
                return _window
            await conn.execute_core(
                update(sources)
                .where(sources.c.id == source_id)
                .values(
                    load_protected=load_protected,
                    off_peak_window=off_peak_window,
                    off_peak_tz=off_peak_tz,
                )
            )
        return MutationResult(success=True, message=f"load protection set for source {source_id!r}", code="schema.source_load_protection_set", params={"source": source_id})

    @strawberry.mutation
    async def update_table_load_protection(
        self,
        table_id: int,
        load_protected: bool | None = None,
        off_peak_window: str | None = None,
        off_peak_tz: str | None = None,
    ) -> MutationResult:  # REQ-1141
        """Override load protection for one table; None load_protected = inherit the source default.

        When the EFFECTIVE load_protected is True, enforces the REQ-1141 ≥1-gate rule over the
        effective (table→source) window/cadence/probe."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            _res = await conn.execute_core(
                select(
                    registered_tables.c.source_id,
                    registered_tables.c.cache_ttl,
                    registered_tables.c.change_signal,
                    registered_tables.c.off_peak_window,
                    registered_tables.c.mv_bitemporal_mode,
                ).where(registered_tables.c.id == table_id)
            )
            row = _res.fetchone()
            if row is None:
                return MutationResult(success=False, message=f"Table {table_id} not found", code="schema.table_not_found", params={"table": table_id})
            # REQ-1141/1162: load protection (WHEN a source may be hit) and snapshotting (WHAT
            # point-in-time the data represents) are different axes, but on ONE table their timing can
            # fight — a snapshot boundary can fall outside the off-peak window, so the snapshot never
            # captures the intended instant. Warn (not block): they compose only when the snapshot
            # deadline can wait for the off-peak refresh (allowed_lateness).
            if _snapshot_load_protection_conflict(
                load_protected, off_peak_window, row.mv_bitemporal_mode
            ):
                logging.getLogger(__name__).warning(
                    "table %s: load protection (off-peak window) AND %r snapshotting are both set — "
                    "verify the snapshot boundary falls inside the off-peak refresh window (or that "
                    "allowed_lateness covers the lag), else snapshots may miss their intended instant "
                    "(REQ-1141/1162)",
                    table_id,
                    row.mv_bitemporal_mode,
                )
            _sres = await conn.execute_core(
                select(
                    sources.c.load_protected,
                    sources.c.cache_ttl,
                    sources.c.change_signal,
                    sources.c.off_peak_window,
                ).where(sources.c.id == row.source_id)
            )
            src = _sres.fetchone()
            effective_lp = src.load_protected if load_protected is None else load_protected
            eff_window = off_peak_window or (src.off_peak_window if src else None)
            eff_ttl = row.cache_ttl if row.cache_ttl is not None else (src.cache_ttl if src else None)
            eff_sig = row.change_signal or (src.change_signal if src else None)
            err = _validate_load_protection(
                effective_lp, eff_window, eff_ttl, eff_sig, f"table {table_id}"
            )
            if err is not None:
                return err
            _window = _parsed_off_peak(off_peak_window, off_peak_tz or "UTC")
            if isinstance(_window, MutationResult):
                return _window
            await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == table_id)
                .values(
                    load_protected=load_protected,
                    off_peak_window=off_peak_window,
                    off_peak_tz=off_peak_tz,
                )
            )
        return MutationResult(success=True, message=f"load protection set for table {table_id}", code="schema.table_load_protection_set", params={"table": table_id})

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
        return MutationResult(success=True, message=f"Naming convention set to {convention!r}", code="schema.naming_convention_set", params={"convention": convention})

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
                return MutationResult(success=False, message=f"Source {source_id!r} not found", code="schema.source_not_found", params={"source": source_id})
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Naming convention updated for source {source_id!r}",
            code="schema.source_naming_updated",
            params={"source": source_id},
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
                return MutationResult(success=False, message=f"Source {source_id!r} not found", code="schema.source_not_found", params={"source": source_id})
        from provisa.api.app import state

        if allowed_domains:
            state.source_allowed_domains[source_id] = list(allowed_domains)
        else:
            state.source_allowed_domains.pop(source_id, None)
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Allowed domains updated for source {source_id!r}",
            code="schema.allowed_domains_updated",
            params={"source": source_id},
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
                return MutationResult(success=False, message=f"Table {table_id} not found", code="schema.table_not_found", params={"table": table_id})
        await _rebuild_schemas()
        return MutationResult(
            success=True,
            message=f"Naming convention updated for table {table_id}",
            code="schema.table_naming_updated",
            params={"table": table_id},
        )

    # ── Admin: Forced Regen ──

    @strawberry.mutation
    async def force_regen(self, table_id: int, reason: str) -> MutationResult:  # REQ-968
        """Recompute one table's landed rows ON DEMAND, bypassing the REQ-958/981 change gate.

        THE SCOPE IS DERIVED, never asked of the operator: a derived view recomputes from its own SQL
        (``node``) while a landed source re-lands and cascades to its dependents (``source``) — the
        operator knows they want this table rebuilt, not which kind of node the event loop thinks it
        is. A table that federates LIVE has no landed rows to regenerate, so it is refused rather than
        given an event nothing will ever claim. The reason is REQ-968's audit why-tag and rides on the
        posted event.
        """
        from provisa.api.app import state
        from provisa.api.admin._refresh_summary import _resolve_engine
        from provisa.events import injector
        from provisa.federation.engine import UnreachableSource
        from provisa.federation.strategy import Strategy, federate

        pool = await _get_pool()
        async with pool.acquire() as conn:
            row = (
                await conn.execute_core(
                    select(
                        registered_tables.c.schema_name,
                        registered_tables.c.table_name,
                        registered_tables.c.source_id,
                    ).where(registered_tables.c.id == table_id)
                )
            ).fetchone()
            if row is None:
                return MutationResult(success=False, message=f"Table {table_id} not found", code="schema.table_not_found", params={"table": table_id})
            schema_name, table_name, source_id = row[0], row[1], row[2]
            node = f"{schema_name}.{table_name}"
            if state.mv_registry.get(f"view-{table_name}") is not None:
                scope = "node"  # a derived view: recompute its SQL without re-landing its inputs
            else:
                scope = "source"
                engine = _resolve_engine()
                if engine is None:
                    return MutationResult(success=False, message="Federation engine is not ready", code="schema.engine_not_ready", params={"table": node})
                # The live config's own Source model — the same object the event loop classifies from,
                # so this answer and the DAG's cannot disagree.
                src = next(
                    (s for s in (state.config.sources if state.config else []) if s.id == source_id),
                    None,
                )
                if src is None:
                    return MutationResult(success=False, message=f"Source {source_id!r} not found", code="schema.source_not_found", params={"source": source_id})
                try:
                    landed = federate(src, engine) is Strategy.MATERIALIZED
                except UnreachableSource:
                    # The source is down. Its replica is a frozen snapshot (REQ-1143), and a forced
                    # re-land is exactly how an operator retries the load once it is back.
                    landed = True
                if not landed:
                    return MutationResult(success=False, message=f"{node} federates live — it has no landed rows to regenerate", code="schema.table_not_landed", params={"table": node})
            try:
                event_id = await injector.force_regen(conn, scope=scope, node=node, reason=reason)
            except ValueError as exc:  # REQ-968 refuses a missing reason / unknown scope, loudly
                return MutationResult(success=False, message=str(exc), code="schema.regen_refused", params={"table": node})
        return MutationResult(
            success=True,
            message=f"Regen queued for {node}",
            code="schema.regen_queued",
            params={"table": node, "scope": scope, "event": event_id},
        )

    # ── Admin: MV Management ──

    @strawberry.mutation
    async def refresh_mv(self, mv_id: str) -> MutationResult:  # REQ-133, REQ-158
        """Trigger a manual refresh of a materialized view."""
        from provisa.api.app import state

        mv = state.mv_registry.get(mv_id)
        if mv is None:
            return MutationResult(success=False, message=f"MV {mv_id!r} not found", code="schema.mv_not_found", params={"mv": mv_id})
        try:
            from provisa.mv.refresh import refresh_mv

            assert state.federation_engine is not None
            # REQ-879: coordinate the refresh across the fleet via the shared control-plane catalog.
            await refresh_mv(state.federation_engine, mv, state.mv_registry, store=state.tenant_db)
            return MutationResult(success=True, message=f"MV {mv_id!r} refreshed", code="schema.mv_refreshed", params={"mv": mv_id})
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
            return MutationResult(success=False, message=f"MV {mv_id!r} not found", code="schema.mv_not_found", params={"mv": mv_id})
        mv.enabled = enabled
        if not enabled:
            mv.status = MVStatus.DISABLED
        elif mv.status == MVStatus.DISABLED:
            mv.status = MVStatus.STALE
        return MutationResult(
            success=True,
            message=f"MV {mv_id!r} {'enabled' if enabled else 'disabled'}",
            code="schema.mv_enabled" if enabled else "schema.mv_disabled",
            params={"mv": mv_id},
        )

    # ── Admin: Cache Management ──

    @strawberry.mutation
    async def purge_cache(self) -> MutationResult:
        """Purge all cached query results."""
        from provisa.api.app import state

        try:
            count = await state.response_cache_store.invalidate_by_pattern("provisa:cache:*")
            return MutationResult(success=True, message=f"Purged {count} cache entries", code="schema.cache_purged", params={"count": count})
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
                success=True,
                message=f"Purged {count} cache entries for table {table_id}",
                code="schema.cache_purged_table",
                params={"count": count, "table": table_id},
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
                return MutationResult(success=False, message=f"Table {table_id} not found", code="schema.table_not_found", params={"table": table_id})
            if row["type"] != "sqlite":
                return MutationResult(
                    success=False,
                    message=f"Source type {row['type']!r} is not sqlite",
                    code="schema.source_type_not_sqlite",
                    params={"type": row["type"]},
                )
            from provisa.api.app import state as _state

            # An ATTACH engine (DuckDB) reads the sqlite file live — no replica to re-migrate (REQ-947).
            if engine_attaches(getattr(_state, "federation_engine", None), "sqlite"):
                return MutationResult(success=True, message="attached live; no migration needed", code="schema.attached_live")
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
                    success=True,
                    message=f"Re-migrated {row['source_id']}.{row['table_name']}",
                    code="schema.remigrated",
                    params={"source": row["source_id"], "table": row["table_name"]},
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
            return MutationResult(success=False, message="Config file not found", code="schema.config_not_found")

        cfg = read_config()
        triggers = cfg.get("scheduled_triggers", [])
        found = False
        for t in triggers:
            if t["id"] == task_id:
                t["enabled"] = enabled
                found = True
                break

        if not found:
            return MutationResult(success=False, message=f"Task {task_id!r} not found", code="schema.task_not_found", params={"task": task_id})

        with open(path, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

        return MutationResult(
            success=True,
            message=f"Task {task_id!r} {'enabled' if enabled else 'disabled'}",
            code="schema.task_enabled" if enabled else "schema.task_disabled",
            params={"task": task_id},
        )

    @strawberry.mutation
    async def create_scheduled_task(  # REQ-1003, REQ-1004
        self,
        id: str,
        name: str,
        cron: str,
        kind: str,
        webhook_name: Optional[str] = None,
        args_json: Optional[str] = None,
        sql: Optional[str] = None,
    ) -> MutationResult:
        """Create a scheduled trigger (webhook or SQL) and register it live (REQ-1003/1004)."""
        return await _ops.create_scheduled_task_op(
            id, name, cron, kind, webhook_name, args_json, sql
        )

    @strawberry.mutation
    async def delete_scheduled_task(self, task_id: str) -> MutationResult:  # REQ-1003
        """Remove a scheduled trigger from config and the live scheduler."""
        return await _ops.delete_scheduled_task_op(task_id)

    @strawberry.mutation
    async def refresh_source_statistics(self, source_id: str) -> MutationResult:  # REQ-276
        """Run ANALYZE on all registered tables for a source (Phase AL).

        Triggers the engine to collect fresh table statistics, which improves the
        quality of join-order and broadcast decisions for federated queries.
        """
        from provisa.api.app import state

        if state.federation_engine is None:
            return MutationResult(success=False, message="Query engine not available", code="schema.query_engine_unavailable")

        pool = await _get_pool()
        if pool is None:
            return MutationResult(success=False, message="Database pool not available", code="schema.db_pool_unavailable")

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
                code="schema.no_tables_for_source",
                params={"source": source_id},
            )

        analyzed: list[str] = []
        errors: list[str] = []
        source_catalog = state.catalog_for(source_id)

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
                code="schema.analyze_errors",
                params={"ok": len(analyzed), "errors": str(errors)},
            )
        return MutationResult(
            success=True,
            message=f"ANALYZE completed for {len(analyzed)} table(s) on source {source_id!r}",
            code="schema.analyze_completed",
            params={"count": len(analyzed), "source": source_id},
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
