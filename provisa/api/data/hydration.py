# Copyright (c) 2026 Kenneth Stott
# Canary: 8cecaf23-1646-4926-8b18-37a14a446d6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""API-table hydration for the /data/graphql endpoint (REQ-140, REQ-848, REQ-1915).

Before the engine executes, the call an API-backed table of the request needs (its
collection with this request's arguments) is made and their answers kept as fills in the store's API cache schema
(``api_source.fill_cache``) — never in the control plane. Extracted from endpoint.py; leaf
module.
"""

from __future__ import annotations

import logging
import time as _time


log = logging.getLogger(__name__)


# Source-level hydration expiry: source_id -> monotonic expiry.
# When set, the entire source is skipped (no pool acquire, no PG queries).
_source_hydration_expiry: dict[str, float] = {}


async def _hydrate_collection(
    src,
    endpoint,
    ttl,
    source_id,
    compiled,
    state,
    hydration_rows: dict,
    cache_hit_sources: set,
) -> None:
    """Collection branch: the call for this request's arguments, unless its fill is fresh."""
    from provisa.api_source import fill_cache

    # The endpoint's own arguments, as the API step reads the fill back by them (REQ-318).
    query_params = fill_cache.endpoint_args(endpoint, compiled.api_args)
    if fill_cache.is_mem_fresh(fill_cache.fill_table(state, endpoint, src), query_params):
        cache_hit_sources.add(source_id)
        return
    n = await fill_cache.fill(state, endpoint, src, [query_params], ttl)
    hydration_rows[source_id] = hydration_rows.get(source_id, 0) + n


async def _hydrate_api_tables_before_engine(
    compiled, ctx, state
) -> tuple[set, dict[str, float], dict[str, int], set]:
    """Ensure the fills an API-backed table of this request needs are in the store before the
    engine executes (``api_source.fill_cache``: TTL-aware, keyed by the hash of the arguments).

    For each API source in compiled.sources, each endpoint without a path parameter is called
    with this request's arguments. A parameter a join gives its values is bound by the statement
    (REQ-1986), which calls the endpoint once per value.

    Returns (dataloader_sources, hydration_times_ms, hydration_rows, cache_hit_sources).
    """
    from provisa.api.data.materialization import (
        api_answered_from_replica,
        landing_ttl,
        registration_of,
    )
    from provisa.api_source.models import ParamType

    dataloader_sources: set = set()
    hydration_times: dict[str, float] = {}
    hydration_rows: dict[str, int] = {}
    cache_hit_sources: set = set()
    if not hasattr(state, "api_endpoints") or not state.api_endpoints:
        return dataloader_sources, hydration_times, hydration_rows, cache_hit_sources
    # REQ-1865: a table replicated row by row (row_materialize) has no whole-table API cache table
    # to fill — its rows live in the row-level replica, filled by key (ensure_rows_resident). The
    # same rule _materialize_api_to_engine_cache applies on the raw-SQL/compiled path. Filling it
    # here read a "default" cache table that does not exist and failed every GraphQL query that
    # touched the source.
    _row_level_tables = {
        t.get("table_name")
        for t in (getattr(state, "tables", None) or [])
        if t.get("row_materialize")
    }

    for source_id in compiled.sources:
        _t_src = _time.perf_counter()
        if _source_hydration_expiry.get(source_id, 0) > _time.monotonic():
            hydration_times[source_id] = (_time.perf_counter() - _t_src) * 1000
            cache_hit_sources.add(source_id)
            continue
        src = (state.api_sources or {}).get(source_id)
        if src is None:
            continue
        _min_ttl = None
        for (_ep_source, table_name), endpoint in state.api_endpoints.items():
            if endpoint.source_id != source_id:
                continue
            if table_name in _row_level_tables:
                continue
            # REQ-2031, REQ-2089: a table answered from its replica (a demo's declared rows) is
            # never filled from its endpoint; one declared and not landed is refused by name.
            if api_answered_from_replica(
                state, registration_of(state, source_id, endpoint.table_name)
            ):
                continue
            # REQ-1986: the one resolver. A parameterized endpoint with no TTL of its own is 0:
            # every fill is fetched again for each statement, and its read takes only those.
            ttl = landing_ttl(
                registration_of(state, source_id, endpoint.table_name),
                state,
                endpoint_ttl=endpoint.ttl,
            )
            _min_ttl = ttl if _min_ttl is None else min(_min_ttl, ttl)

            # REQ-1986: a path parameter is bound by the statement (an equality, or a join on the
            # parameter itself, one call per key: ``materialization.bind_statement``), which reads
            # the endpoint under each value. Only an endpoint without one is called here, with
            # this request's arguments.
            if any(c.param_type == ParamType.path for c in endpoint.columns):
                continue
            await _hydrate_collection(
                src,
                endpoint,
                ttl,
                source_id,
                compiled,
                state,
                hydration_rows,
                cache_hit_sources,
            )

        hydration_times[source_id] = (_time.perf_counter() - _t_src) * 1000
        if _min_ttl is not None:
            _source_hydration_expiry[source_id] = _time.monotonic() + _min_ttl

    return dataloader_sources, hydration_times, hydration_rows, cache_hit_sources
