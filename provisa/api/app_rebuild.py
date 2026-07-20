# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Post-rebuild state reconciliation for app startup / schema rebuild.

Background API hydration, live-engine reconcile, user-view registration, and
rebuild finalization. Reaches the app state singleton lazily; best-effort steps
go through tolerate_startup_failure.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, cast

from sqlalchemy import select

from provisa.core.database import Database
from provisa.core.schema_org import registered_tables as _registered_tables_t
from provisa.api.startup_resilience import tolerate_startup_failure

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)


def compile_view_sql_to_physical(sql: str, ctx) -> str:
    """Compile a view's *semantic* SQL (domain.field / schema.table refs) into a
    catalog-qualified *physical* plan. Normalize first (sqlglot parse) so unquoted
    semantic refs resolve, then catalog-qualify — the exact rewrite the query path
    applies to inline views. Idempotent on already-physical SQL."""
    from provisa.compiler.sql_rewrite import (
        normalize_table_refs,
        rewrite_semantic_to_catalog_physical,
    )

    return rewrite_semantic_to_catalog_physical(normalize_table_refs(sql, ctx), ctx)


def compile_registry_mvs_to_physical(mv_registry, ctx) -> None:
    """Rewrite every custom-SQL MV's semantic SQL to a physical plan in place.

    Materialized user-view MVs carry semantic SQL (set in _register_user_views_in_state);
    their refresh executes mv.sql straight at the federation engine, which only knows
    physical catalogs. Without this compile the refresh fails with "schema <domain> does
    not exist". Join-pattern MVs (sql is None) are untouched — they build SQL at refresh.
    """
    for mv in mv_registry._mvs.values():
        if mv.sql:
            mv.sql = compile_view_sql_to_physical(mv.sql, ctx)


async def _bg_hydrate_api_endpoints() -> None:
    """Background-hydrate zero-param API endpoints (no path params → full collection known at startup)."""
    from provisa.api.app import state

    _zero_param_eps = [
        (ep, state.api_sources[ep.source_id])
        for ep in state.api_endpoints.values()
        if "{" not in ep.path and ep.source_id in state.api_sources
    ]
    if not _zero_param_eps:
        return

    assert state.tenant_db is not None

    async def _bg_hydrate(eps=_zero_param_eps, pool: Database = state.tenant_db):
        from provisa.openapi.pg_cache import fill_api_table

        async with pool.acquire() as _conn:
            for _ep, _src in eps:
                # Best-effort: one endpoint's hydration failing must not stop the rest.
                with tolerate_startup_failure(f"BG hydration for {_ep.table_name}"):
                    await fill_api_table(
                        _src.base_url,
                        _ep.path,
                        _ep.default_params,
                        cast("Connection", _conn),
                        "default",
                        _ep.table_name,
                        _ep.ttl,
                        _ep.response_root,
                        _ep.error_path,
                        _ep.pk_column,
                    )

    asyncio.create_task(_bg_hydrate())


async def _reconcile_live_engine(conn: "Connection") -> None:  # REQ-565, REQ-813
    """Reconcile the LiveEngine poll jobs from persisted per-table live config."""
    from provisa.api.app import state
    from provisa.live.reconcile import reconcile_live_engine

    await reconcile_live_engine(conn, state.live_engine)


async def _register_user_views_in_state(conn: "Connection", raw_config: dict | None) -> None:
    """Register __provisa__ views in mv_registry (REQ-199) or view_sql_map. Non-fatal."""
    from provisa.api.app import state

    with tolerate_startup_failure("user views for inline expansion"):
        _view_rows = [
            dict(_r._mapping)
            for _r in (
                await conn.execute_core(
                    select(
                        _registered_tables_t.c.table_name,
                        _registered_tables_t.c.view_sql,
                        _registered_tables_t.c.materialize,
                        _registered_tables_t.c.mv_refresh_interval,
                        _registered_tables_t.c.change_signal,
                        _registered_tables_t.c.mv_preprocess,  # REQ-957
                        _registered_tables_t.c.mv_bitemporal_mode,  # REQ-1162
                        _registered_tables_t.c.mv_bitemporal_key,  # REQ-1162
                    ).where(
                        _registered_tables_t.c.source_id == "__provisa__",
                        _registered_tables_t.c.view_sql.is_not(None),
                    )
                )
            ).fetchall()
        ]
        # REQ-199: MVs without an explicit interval fall back to the configured default TTL.
        _mv_default_ttl = int(
            (raw_config or {}).get("materialized_views", {}).get("default_ttl", 300)
        )
        for _vr in _view_rows:
            # Store the *semantic* view SQL; _compile_view_sqls rewrites it to a physical plan.
            # EVERY user view (materialized or not) goes into view_sql_map so the query path can
            # inline-expand it live. A materialized view is ALSO registered as an MV below — the query
            # path expands the view and, when its MV is fresh, rewrite_if_mv_match redirects to the
            # materialized table. Without the view_sql_map entry a materialized-but-unrefreshed view
            # is unqueryable: its raw source catalog (e.g. __provisa__) reaches the engine and fails
            # with "Catalog __provisa__ does not exist".
            _semantic_sql = _vr["view_sql"].rstrip().rstrip(";")
            # REQ-1162: reconstruct the bitemporal spec from the persisted mode/key (None = ordinary).
            _bt_spec = None
            if _vr.get("mv_bitemporal_mode"):
                from provisa.mv.bitemporal import BitemporalSpec

                _bt_spec = BitemporalSpec(
                    key=tuple(_vr.get("mv_bitemporal_key") or []),
                    mode=_vr["mv_bitemporal_mode"],
                )
            if _bt_spec is not None and _vr.get("materialize"):
                # REQ-1163: a bitemporal MV is served from its materialized append log — the read
                # reconstructs CURRENT state (the live view SQL would carry no history). Point the
                # inline-expansion entry at the reconstruction over the physical mv target.
                from provisa.mv.bitemporal import view_read_sql

                _tgt_cat, _tgt_schema = state.federation_engine.materialize_store_target(
                    state.org_id
                )
                _mv_ref = f'"{_tgt_cat}"."{_tgt_schema}"."mv_{_vr["table_name"]}"'
                state.view_sql_map[_vr["table_name"]] = view_read_sql(_mv_ref, _bt_spec)
            else:
                state.view_sql_map[_vr["table_name"]] = _semantic_sql
            if _vr.get("materialize"):
                from provisa.mv.models import MVDefinition, MVStatus
                from provisa.core.change_signal import resolve, to_freshness_mode  # REQ-932

                _mv_id = f"view-{_vr['table_name']}"
                # REQ-932: derive the refresh gate from change_signal. A __provisa__ view has no
                # backing source, so resolve falls to the global default. Push signals return None
                # (event-driven, no poll gate) → keep the ttl default until CDC-apply landing exists.
                _sig = resolve(_vr.get("change_signal"), None)
                _fresh = to_freshness_mode(_sig) or "ttl"  # REQ-932: push → ttl until Phase 3
                _existing = state.mv_registry.get(_mv_id)
                if _existing is None:
                    # Target the store the ACTIVE engine materializes into (DuckDB attaches its store
                    # as ``mat_store``, not ``postgresql``), matching _sync_view_mv.
                    _tgt_cat, _tgt_schema = state.federation_engine.materialize_store_target(
                        state.org_id
                    )
                    state.mv_registry.register(
                        MVDefinition(
                            id=_mv_id,
                            source_tables=[],
                            target_catalog=_tgt_cat,
                            target_schema=_tgt_schema,
                            target_table=f"mv_{_vr['table_name']}",
                            refresh_interval=int(_vr.get("mv_refresh_interval") or _mv_default_ttl),
                            enabled=True,
                            sql=_semantic_sql,
                            expose_in_sdl=False,
                            status=MVStatus.STALE,
                            freshness_mode=_fresh,
                            preprocess=_vr.get("mv_preprocess"),  # REQ-957
                            bitemporal=_bt_spec,  # REQ-1162: survive restart
                        )
                    )
                else:
                    _existing.sql = _semantic_sql
                    _existing.preprocess = _vr.get("mv_preprocess")  # REQ-957
                    _existing.bitemporal = _bt_spec  # REQ-1162


async def _finalize_rebuild_state(_rebuild_log: logging.Logger) -> None:
    """Reconcile live engine (REQ-565) and compile view SQLs after a schema rebuild."""
    from provisa.api.app import state

    # Re-drive the live poll engine from the now-current DB state so admin edits
    # to per-table live config take effect without a restart (REQ-565).
    if state.live_engine is not None and state.tenant_db is not None:
        with tolerate_startup_failure("live engine reconcile", exc_info=True):
            async with state.tenant_db.acquire() as _lc:
                await _reconcile_live_engine(_lc)

    # Compile inline view SQLs now that a context is available
    if state.contexts:
        ctx = next(iter(state.contexts.values()))
        if state.view_sql_map:
            state.view_sql_map = {
                name: compile_view_sql_to_physical(sql, ctx)
                for name, sql in state.view_sql_map.items()
            }
        compile_registry_mvs_to_physical(state.mv_registry, ctx)
