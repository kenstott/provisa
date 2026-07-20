# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8c2d70-9b31-4e62-8f05-1c7b0d4f2e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Adapter: build the REQ-1143 refresh-policy summary for a RegisteredTableType admin object.

Reconstructs the minimal Source + Table needed by the pure ``describe_refresh_policy`` (federation)
from the admin type + the persisted source row, and resolves the bare FederationEngine off the app
state's EngineRuntime. The decision tree itself lives once in provisa/federation/policy_summary.py —
this only marshals config into it, so the admin surface never re-derives the policy (REQ-1143).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.admin.types import RefreshPolicySummaryType, RegisteredTableType


def _resolve_engine():
    """Navigate app state to the bare FederationEngine, or None during startup (REQ-1143).

    ``state.federation_engine`` is an EngineRuntime wrapper; ``federate()`` reads ``engine.connectors``,
    which lives on the bare FederationEngine (runtime.engine). A None here is the nullable-by-contract
    "not computable yet" signal, not a masked error."""
    from provisa.api.app import state

    runtime = getattr(state, "federation_engine", None)
    if runtime is None:
        return None
    engine = runtime if getattr(runtime, "connectors", None) is not None else getattr(runtime, "engine", None)
    if engine is None or getattr(engine, "connectors", None) is None:
        return None
    return engine


async def _summarize(
    *,
    source_id: str,
    domain_id: str,
    schema_name: str,
    table_name: str,
    cache_ttl: int | None,
    prefer_materialized: bool | None,
    load_protected: bool | None,
    off_peak_window: str | None,
    off_peak_tz: str | None,
    change_signal: str | None,
) -> RefreshPolicySummaryType | None:
    """Marshal a set of table knobs through the pure ``describe_refresh_policy`` (REQ-1143).

    The single choke point for both the persisted-table field and the draft preview — so the decision
    tree is derived server-side exactly once, never re-derived in the client."""
    from provisa.api.admin.types import RefreshPolicySummaryType
    from provisa.core.models import Table
    from provisa.federation.policy_summary import describe_refresh_policy

    # A __provisa__ virtual view has no external-source freshness — its refresh is governed by the
    # view/MV config, not a source-refresh policy — and its persisted source `type` is the federation
    # engine name (e.g. "trino"), which is not a SourceType. Skip the source-policy summary for it so
    # the tables query never errors on a view row (REQ-1143).
    if source_id == "__provisa__":
        return None

    engine = _resolve_engine()
    if engine is None:
        return None  # engine not connected yet (startup) — field is nullable by contract

    from provisa.api.app import state

    default_ttl = getattr(state, "response_cache_default_ttl", 300)

    source = await _load_source(source_id)
    tbl = Table(
        source_id=source_id,
        domain_id=domain_id,
        table_name=table_name,
        schema_name=schema_name,
        columns=[],  # policy resolution does not read columns
        cache_ttl=cache_ttl,
        prefer_materialized=prefer_materialized,
        load_protected=load_protected,
        off_peak_window=off_peak_window,
        off_peak_tz=off_peak_tz,
        change_signal=change_signal,
    )
    summary = describe_refresh_policy(source, tbl, engine, default_ttl)
    return RefreshPolicySummaryType(
        text=summary.text, serving=summary.serving.value, warning=summary.warning
    )


async def summarize_table_policy(table: RegisteredTableType) -> RefreshPolicySummaryType | None:
    """Compute the effective refresh-policy summary for one persisted admin table (REQ-1143).

    Returns None when the federation engine is not yet available (startup) — a nullable-by-contract
    "not computable yet" signal for the GraphQL field, not a masked error. Any other missing input
    (an unknown source) is a real inconsistency and raises."""
    return await _summarize(
        source_id=table.source_id,
        domain_id=table.domain_id,
        schema_name=table.schema_name,
        table_name=table.table_name,
        cache_ttl=table.cache_ttl,
        prefer_materialized=table.prefer_materialized,
        load_protected=table.load_protected,
        off_peak_window=table.off_peak_window,
        off_peak_tz=table.off_peak_tz,
        change_signal=table.change_signal,
    )


async def preview_table_policy(
    *,
    source_id: str,
    domain_id: str,
    schema_name: str,
    table_name: str,
    cache_ttl: int | None,
    prefer_materialized: bool | None,
    load_protected: bool | None,
    off_peak_window: str | None,
    off_peak_tz: str | None,
    change_signal: str | None,
) -> RefreshPolicySummaryType | None:
    """Preview the refresh-policy summary for DRAFT (unsaved) table knobs from the editor (REQ-1143).

    Same pure derivation as the persisted field — only the knob values come from the in-flight form
    instead of the row — so the top-of-form summary tracks edits without persisting or re-deriving the
    tree client-side."""
    return await _summarize(
        source_id=source_id,
        domain_id=domain_id,
        schema_name=schema_name,
        table_name=table_name,
        cache_ttl=cache_ttl,
        prefer_materialized=prefer_materialized,
        load_protected=load_protected,
        off_peak_window=off_peak_window,
        off_peak_tz=off_peak_tz,
        change_signal=change_signal,
    )


async def _load_source(source_id: str):
    """Reconstruct the minimal Source model from the persisted row (its type + the load-protection /
    freshness config the policy reads). An unknown source_id is a referential inconsistency — raise."""
    from provisa.api.admin._table_ops import _get_pool
    from provisa.core.models import Source, SourceType as SourceTypeEnum
    from provisa.core.repositories import source as source_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await source_repo.get(conn, source_id)
    if row is None:
        raise ValueError(f"table references unknown source {source_id!r} (REQ-1143 summary)")
    return Source(
        id=row["id"],
        type=SourceTypeEnum(row["type"]),
        host=row.get("host") or "",
        port=row.get("port") or 0,
        database=row.get("database") or "",
        username=row.get("username") or "",
        cache_ttl=row.get("cache_ttl"),
        prefer_materialized=bool(row.get("prefer_materialized", False)),
        load_protected=bool(row.get("load_protected", False)),
        off_peak_window=row.get("off_peak_window"),
        off_peak_tz=row.get("off_peak_tz") or "UTC",
        change_signal=row.get("change_signal") or "ttl",
    )
