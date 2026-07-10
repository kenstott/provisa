# Copyright (c) 2026 Kenneth Stott
# Canary: 2ae8ef6d-2550-4cb3-bd42-e938c6f76e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared leaf helpers for the admin GraphQL schema.

Strawberry types, admin-context resolution, creation-request queueing, and the
source-management operations (pool/engine registration, govdata, view MV sync)
used by both the Query and Mutation resolvers. No dependency on those classes.
"""

# complexity-gate: allow-ble=2 reason="two genuine boundaries, not grandfathering: (1) _validate_govdata_api_key TESTS an external credential — any failure to connect means the key is invalid, which it REPORTS to the user as MutationResult(success=False); narrowing would let an unexpected JVM/JDBC failure crash the mutation instead of reporting an invalid key. (2) _register_source_on_engine is best-effort engine provisioning; register_source logs its own warnings and provisioning is non-fatal (the source stays usable direct-routed), matching the established convention at config_loader.py:248."

import logging
from typing import TYPE_CHECKING, Optional, cast

import strawberry
from sqlalchemy import select, update
from strawberry.types.info import Info as StrawberryInfo

from provisa.core.schema_org import (
    registered_tables,
    sources,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.types import (
    MutationResult,
    SourceInput,
)

from provisa.api.admin.schema_helpers import (
    _get_pool,
)
from provisa.api.admin._table_ops import (
    _build_column_models,  # noqa: F401  (re-export: tests + steps import from schema)
    _ensure_view_column_types,  # noqa: F401  (re-export: tests import from schema)
)


@strawberry.type
class CreationRequestType:  # REQ-434, REQ-063
    id: int
    request_type: str
    capability: str
    requested_by: Optional[str]
    status: str
    rejection_reason: Optional[str]
    payload_json: str


def _rebuild_relationship_input(payload: dict):
    from provisa.api.admin.types import RelationshipInput

    return RelationshipInput(**payload)


def _rebuild_table_input(payload: dict):
    from provisa.api.admin.types import ColumnInput, ColumnPresetInput, TableInput

    data = dict(payload)
    data["columns"] = [ColumnInput(**c) for c in payload.get("columns", [])]
    data["column_presets"] = [ColumnPresetInput(**c) for c in payload.get("column_presets", [])]
    return TableInput(**data)


async def _queue_creation_request(  # REQ-434
    info, request_type: str, capability: str, input
) -> MutationResult:
    """Persist a governed create the caller is not authorized to perform (REQ-434)."""
    import dataclasses

    from provisa.api.admin.capabilities import _identity_from_info
    from provisa.core.repositories import creation_request as cr_repo

    payload = dataclasses.asdict(input)
    identity = _identity_from_info(info)
    requested_by = getattr(identity, "user_id", None) if identity is not None else None
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rid = await cr_repo.create(
            cast("Connection", conn), request_type, capability, payload, requested_by
        )
    return MutationResult(
        success=True,
        message=(
            f"Queued as creation request #{rid} — awaiting a user holding "
            f"{capability!r} to execute or reject it."
        ),
    )


def _resolve_admin_context(info: StrawberryInfo) -> tuple[str, bool]:
    """Return (active_org_id, is_admin) for the current request identity."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state as _state

    request = info.context["request"]
    active_org_id = require_active_org_id(request)
    identity = getattr(request.state, "identity", None)
    caps = _resolved_capabilities(identity, _state) if identity else set()
    is_admin = bool(caps & {"superadmin", "admin"})
    return active_org_id, is_admin


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
            await conn.execute_core(
                update(sources).where(sources.c.id == input.id).values(allowed_domains=_domains)
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


def _register_source_on_engine(state, model, input: SourceInput) -> None:
    """Provision the source on the bound engine (mirrors config_loader path)."""
    from provisa.core.secrets import resolve_secrets

    try:
        state.federation_engine.register_source(
            model, resolve_secrets(input.password) if input.password else ""
        )
    except Exception as _cat_err:
        logging.getLogger(__name__).warning(
            "engine source provisioning for %r failed: %s", input.id, _cat_err
        )


async def _analyze_source_on_engine(state, pool, model, input: SourceInput) -> None:
    """Fire engine ANALYZE on all registered tables for this source (errors swallowed)."""

    class _TblRef:
        def __init__(self, source_id: str, schema_name: str, table_name: str) -> None:
            self.source_id = source_id
            self.schema_name = schema_name
            self.table_name = table_name

    async with pool.acquire() as _conn:
        _res = await _conn.execute_core(
            select(registered_tables.c.schema_name, registered_tables.c.table_name).where(
                registered_tables.c.source_id == input.id
            )
        )
        rows = _res.fetchall()
    table_refs = [_TblRef(input.id, r.schema_name, r.table_name) for r in rows]
    if table_refs:
        state.federation_engine.analyze(model, table_refs)


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
            state.federation_engine,
            state.source_pools,
            state.source_types,
            state,
        )
    )


def _sync_view_mv(
    table_name: str, view_sql: str, refresh_interval: int, change_signal: str | None = None
) -> None:
    """Register or update an MVDefinition for a materialized user-defined view."""
    from provisa.api.app import state
    from provisa.mv.models import MVDefinition, MVStatus
    from provisa.core.change_signal import resolve, to_freshness_mode  # REQ-932
    from provisa.mv.determinism import check_view_determinism  # REQ-964

    # REQ-964 (proof obligation 1): an MV's SQL must be deterministic — recompute-to-current
    # and replay demand it. Reject volatile SQL (now()/random/…) at registration; the engine's
    # dialect parses the check (None → sqlglot default parse, still catches volatile funcs).
    dialect = getattr(getattr(state, "engine", None), "dialect", None)
    ok, reason = check_view_determinism(view_sql, dialect)
    if not ok:
        raise ValueError(f"non-deterministic MV {table_name!r}: {reason}")

    mv_id = f"view-{table_name}"
    existing = state.mv_registry.get(mv_id)
    # REQ-932: derive the refresh gate from change_signal. A user view has no backing source, so
    # resolve falls to the global default. Push signals return None → keep ttl until CDC-apply.
    freshness = to_freshness_mode(resolve(change_signal, None)) or "ttl"
    mv = MVDefinition(
        id=mv_id,
        source_tables=[],
        target_catalog="postgresql",
        target_schema=f"org_{state.org_id}_mv_cache",
        target_table=f"mv_{table_name}",
        refresh_interval=refresh_interval,
        enabled=True,
        sql=view_sql,
        expose_in_sdl=False,
        status=existing.status if existing is not None else MVStatus.STALE,
        freshness_mode=freshness,
    )
    state.mv_registry.register(mv)


def _remove_view_mv(table_name: str) -> None:
    """Remove a materialized view definition when materialize is toggled off."""
    from provisa.api.app import state

    state.mv_registry.unregister(f"view-{table_name}")
