# Copyright (c) 2026 Kenneth Stott
# Canary: ec26f2f0-1692-470e-b8bc-e0f73538e2cb
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

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core.schema_org import registered_tables as _registered_tables_t
from provisa.api.startup_resilience import tolerate_startup_failure
from provisa.core.models import DERIVED_SOURCE_ID

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


async def _reconcile_live_engine(conn: "Connection") -> None:  # REQ-565, REQ-813
    """Reconcile the LiveEngine poll jobs from persisted per-table live config.

    REQ-1266: the engine polls for ONE org (the deployment's, built at boot) and reconciles only
    from that org's model. Another org's rebuild would hand it that org's live tables -- polled in
    the engine's org, rows delivered to the other org's outputs -- and drop the engine's own jobs,
    so it is refused by name here and the engine is left as it is."""
    from provisa.api.app import state
    from provisa.core.request_context import require_current_org
    from provisa.live.reconcile import reconcile_live_engine

    engine = state.live_engine
    org_id = require_current_org()
    if engine is not None and org_id != engine.org_id:
        log.error(
            "live delivery for org %r is not served: the live-query engine polls for org %r only",
            org_id,
            engine.org_id,
        )
        return
    await reconcile_live_engine(conn, engine)


async def _register_user_views_in_state(conn: "Connection", raw_config: dict | None) -> None:
    """Register __derived__ views in mv_registry (REQ-199) or view_sql_map. Non-fatal."""
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
                        _registered_tables_t.c.mv_persist,  # REQ-965
                        _registered_tables_t.c.mv_primary_key,  # REQ-970
                        _registered_tables_t.c.mv_incremental,  # REQ-969
                        _registered_tables_t.c.mv_calendar,  # REQ-962
                        _registered_tables_t.c.mv_grain,  # REQ-962/1168
                        _registered_tables_t.c.mv_allowed_lateness,  # REQ-961
                        _registered_tables_t.c.mv_expected_events,  # REQ-961
                        _registered_tables_t.c.mv_business_day_grain,  # REQ-962
                    ).where(
                        _registered_tables_t.c.source_id == DERIVED_SOURCE_ID,
                        _registered_tables_t.c.view_sql.is_not(None),
                        # REQ-1921: a draft view is neither built nor expanded.
                        _registered_tables_t.c.draft.is_(False),
                    )
                )
            ).fetchall()
        ]
        # REQ-199: MVs without an explicit interval fall back to the configured default TTL.
        from provisa.core import settings_registry  # REQ-1913: an operator setting

        _mv_default_ttl = settings_registry.value("materialized_views.default_ttl")
        # REQ-1921: a view that is draft goes out of service here, wherever this runs (each
        # region on its reload): it is not expanded, and its build is no longer registered — so
        # its stored table is an orphan the reclamation sweep drops (mv/refresh.py, REQ-234).
        for (_draft_name,) in (
            await conn.execute_core(
                select(_registered_tables_t.c.table_name).where(
                    _registered_tables_t.c.source_id == DERIVED_SOURCE_ID,
                    _registered_tables_t.c.draft.is_(True),
                )
            )
        ).fetchall():
            state.view_sql_map.pop(_draft_name, None)
            if state.mv_registry.get(f"view-{_draft_name}") is not None:
                state.mv_registry.unregister(f"view-{_draft_name}")
        for _vr in _view_rows:
            # Store the *semantic* view SQL; _compile_view_sqls rewrites it to a physical plan.
            # EVERY user view (materialized or not) goes into view_sql_map so the query path can
            # inline-expand it live. A materialized view is ALSO registered as an MV below — the query
            # path expands the view and, when its MV is fresh, rewrite_if_mv_match redirects to the
            # materialized table. Without the view_sql_map entry a materialized-but-unrefreshed view
            # is unqueryable: its raw source catalog (e.g. __derived__) reaches the engine and fails
            # with "Catalog __derived__ does not exist".
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
                # REQ-1163: remember (target, spec) so a request-level as-of can overlay an as-of
                # reconstruction over this view's append log at query time.
                state.bitemporal_view_reads[_vr["table_name"]] = (_mv_ref, _bt_spec)
            else:
                state.view_sql_map[_vr["table_name"]] = _semantic_sql
            if _vr.get("materialize"):
                from provisa.mv.models import MVDefinition, MVStatus
                from provisa.mv.readable_inputs import read_table_names
                from provisa.core.change_signal import resolve, to_freshness_mode  # REQ-932

                _mv_id = f"view-{_vr['table_name']}"
                # REQ-932: derive the refresh gate from change_signal. A __derived__ view has no
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
                    # A saved view coming back into memory, not a save: registered as it is.
                    # If an input has since stopped being readable (a table switched to
                    # row-level replication) its refresh fails with that reason, shown as the
                    # view's error (provisa/mv/readable_inputs.py) — a rebuild that refused it
                    # here would fail every later change to the org, the one that fixes it too.
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
                            semantic_sql=_semantic_sql,  # REQ-1921/1922
                            read_tables=read_table_names(_semantic_sql),
                            expose_in_sdl=False,
                            status=MVStatus.STALE,
                            freshness_mode=_fresh,
                            preprocess=_vr.get("mv_preprocess"),  # REQ-957
                            bitemporal=_bt_spec,  # REQ-1162: survive restart
                            persist=_vr.get("mv_persist") or "replace",  # REQ-965
                            primary_key=list(_vr.get("mv_primary_key") or []),  # REQ-970
                            incremental=bool(_vr.get("mv_incremental")),  # REQ-969
                            calendar=_vr.get("mv_calendar"),  # REQ-962
                            grain=_vr.get("mv_grain"),  # REQ-962/1168
                            allowed_lateness=float(
                                _vr.get("mv_allowed_lateness") or 0.0
                            ),  # REQ-961
                            expected_events=_vr.get("mv_expected_events"),  # REQ-961
                            business_day_grain=bool(_vr.get("mv_business_day_grain")),  # REQ-962
                        )
                    )
                else:
                    # A refresh may run while this rebuild is under way, so the view's SQL is never
                    # left in its semantic form: it is lowered here against the model the previous
                    # build left (this build's model-wide context is made later, by
                    # _build_and_register_schemas), and lowered again against this build's at
                    # _finalize_rebuild_state.
                    if state.view_context is None:
                        raise RuntimeError(
                            f"view {_mv_id} is registered but no model-wide view context exists to "
                            "lower its SQL against"
                        )
                    _existing.sql = compile_view_sql_to_physical(_semantic_sql, state.view_context)
                    _existing.semantic_sql = _semantic_sql  # REQ-1921/1922
                    _existing.read_tables = read_table_names(_semantic_sql)
                    _existing.preprocess = _vr.get("mv_preprocess")  # REQ-957
                    _existing.bitemporal = _bt_spec  # REQ-1162
                    _existing.persist = _vr.get("mv_persist") or "replace"  # REQ-965
                    _existing.primary_key = list(_vr.get("mv_primary_key") or [])  # REQ-970
                    _existing.incremental = bool(_vr.get("mv_incremental"))  # REQ-969
                    _existing.calendar = _vr.get("mv_calendar")  # REQ-962
                    _existing.grain = _vr.get("mv_grain")  # REQ-962/1168
                    _existing.allowed_lateness = float(
                        _vr.get("mv_allowed_lateness") or 0.0
                    )  # REQ-961
                    _existing.expected_events = _vr.get("mv_expected_events")  # REQ-961
                    _existing.business_day_grain = bool(_vr.get("mv_business_day_grain"))  # REQ-962


async def _finalize_rebuild_state(_rebuild_log: logging.Logger) -> None:
    """Reconcile live engine (REQ-565) and compile view SQLs after a schema rebuild."""
    from provisa.api.app import state

    # Re-drive the live poll engine from the now-current DB state so admin edits
    # to per-table live config take effect without a restart (REQ-565).
    if state.live_engine is not None and state.model_db is not None:
        with tolerate_startup_failure("live engine reconcile", exc_info=True):
            async with state.model_db.acquire() as _lc:
                await _reconcile_live_engine(_lc)

    # Lower every view's SQL (inline and materialized) against the model-wide context the build
    # made for exactly this (api/app_loaders.py) — never a role's.
    if state.contexts:
        ctx = state.view_context
        if ctx is None:
            raise RuntimeError(
                "the schema build compiled role contexts but no model-wide view context: view SQL "
                "cannot be lowered"
            )
        if state.view_sql_map:
            # REQ-1163: a bitemporal view's entry is already a PHYSICAL reconstruction over its append
            # log (view_read_sql over the mv target) — do NOT re-qualify it, or the semantic→physical
            # rewrite mangles the fully-qualified store ref (drops the schema). Only the ordinary
            # semantic view SQL needs compiling.
            _bt_views = getattr(state, "bitemporal_view_reads", {})
            state.view_sql_map = {
                name: (sql if name in _bt_views else compile_view_sql_to_physical(sql, ctx))
                for name, sql in state.view_sql_map.items()
            }
        compile_registry_mvs_to_physical(state.mv_registry, ctx)
