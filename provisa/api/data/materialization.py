# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1a1f2c-c478-4cb0-b137-cdfc82d195c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""API-source materialization for the /data/graphql endpoint (REQ-848, REQ-941).

Fetch + schema-less-store of API / graphql_remote / gRPC source rows into the
engine cache (VALUES-CTE rewrites). Extracted from endpoint.py; leaf module.
"""

# complexity-gate: allow-cc=32 allow-ble=4 reason="materialization fetch/store paths relocated verbatim from endpoint.py; the broad excepts make API-source landing best-effort (fall back to live execution, never fail the query) per REQ-848/REQ-941; per-route split is separately-tracked debt"

from __future__ import annotations

import json
import logging
import secrets
from collections.abc import Iterable
from typing import Any

from provisa.cache.values_cte import InlineRows
from provisa.core.connection_loop import spawn_background


log = logging.getLogger(__name__)


class _StatementHot:
    """The hot tier as one statement sees it (REQ-230, REQ-236): the hot rows of the tables it
    reads — the ids the pipeline resolved for it — by the name its SQL carries for each, and
    rows it fetched held under the reading table's id for the next statement. A name the
    statement's tables do not carry exactly once is not held: the rows are substituted into this
    statement only."""

    def __init__(self, hot_mgr: Any, state: Any, table_ids: Iterable[int]) -> None:
        from provisa.compiler.naming import apply_sql_name

        self._mgr = hot_mgr
        read = set(table_ids)
        names: dict[str, set[int]] = {}
        for row in getattr(state, "tables", None) or []:
            if int(row["id"]) in read:
                for name in {row["table_name"], apply_sql_name(row["table_name"])}:
                    names.setdefault(name, set()).add(int(row["id"]))
        self._ids = {name: next(iter(ids)) for name, ids in names.items() if len(ids) == 1}
        self.entries = hot_mgr.entries_for(read) if hot_mgr is not None else {}

    def table_id(self, tn: str) -> int | None:
        """The id of the table the statement reads under the name ``tn``, when exactly one does."""
        return self._ids.get(tn)

    def holds(self, tn: str) -> bool:
        """Whether rows fetched for ``tn`` are held for the next statement."""
        return self._mgr is not None and tn in self._ids

    def hold(
        self,
        tn: str,
        *,
        catalog: str,
        schema: str,
        pk_column: str,
        rows: list[dict],
        column_names: list[str],
        whole: bool,
    ) -> Any:
        """The rows to substitute for ``tn`` in this statement, held for the next one when
        :meth:`holds` and ``whole`` — the rows are all of the table's rows, fetched with no
        arguments. Rows fetched with arguments (a parameterized fetch, a filter pushed to the
        source) answer this statement only: held as the table's rows they would be served to a
        statement that asked for other ones."""
        if not (whole and self.holds(tn)):
            return InlineRows(rows, column_names)
        from provisa.cache.hot_tables import HotTableEntry

        entry = HotTableEntry(
            table_id=self._ids[tn],
            table_name=tn,
            catalog=catalog,
            schema=schema,
            pk_column=pk_column,
            rows=rows,
            column_names=column_names,
            is_api=True,
        )
        self._mgr.hold(entry)
        return entry


def _lookup_ep(state, table_name: str):
    """Find API endpoint by table name."""
    ep_map: dict = getattr(state, "api_endpoints", {})
    return ep_map.get(table_name)


def _lookup_gql_remote_table(state, table_name: str):
    """Find a graphql_remote table registration by SQL table name (snake_case or camelCase)."""
    from provisa.compiler.naming import apply_sql_name as _asn

    normalised = _asn(table_name)
    gql_srcs = getattr(state, "graphql_remote_sources", {})
    for reg in gql_srcs.values():
        for tbl in reg.get("tables", []):
            if tbl["sql_name"] == table_name or tbl["sql_name"] == normalised:
                return reg, tbl
    return None, None


def _lookup_grpc_remote_table(state, table_name: str):
    """Find a grpc_remote query registration by its landed table name (<ns__>service__method).

    Returns (source_id, reg, query) — the registration dict (see grpc_remote_router.py) carries
    no "source_id" key of its own, only its channel/queries/etc., so the source_id must come from
    the state.grpc_remote_sources mapping key itself.
    """
    grpc_srcs = getattr(state, "grpc_remote_sources", {})
    for source_id, reg in grpc_srcs.items():
        prefix = f"{reg.get('namespace', '')}__" if reg.get("namespace") else ""
        for q in reg.get("queries", []):
            if f"{prefix}{q.service}__{q.method}" == table_name:
                return source_id, reg, q
    return None, None, None


_OPENAPI_QUERIES_ATTR = "_req_1877_openapi_queries"


def _openapi_queries(state, source_id: str, entry: dict) -> list | None:
    """The query operations of one registered OpenAPI source, parsed once per spec entry.

    Kept on ``state`` (per instance, never module-global) and answered only while the entry, its
    spec and its operation overrides are the SAME objects the parse read — the identity rule a
    kept plan follows (``pgwire.governed_plan``). Registration and reload replace the entry
    (``state.openapi_specs[source_id] = {...}``); nothing edits one in place. None when the entry
    has no spec or the spec does not parse — that is not kept, so it is tried again."""
    from provisa.openapi.mapper import parse_spec

    spec = entry.get("spec")
    if not spec:
        return None
    overrides = entry.get("operation_overrides")
    kept_by_source = getattr(state, _OPENAPI_QUERIES_ATTR, None)
    if kept_by_source is None:
        kept_by_source = {}
        setattr(state, _OPENAPI_QUERIES_ATTR, kept_by_source)
    kept = kept_by_source.get(source_id)
    if kept is not None and kept[0] is entry and kept[1] is spec and kept[2] is overrides:
        return kept[3]
    try:
        queries, _mutations = parse_spec(spec, operation_overrides=overrides)
    except Exception:
        return None
    kept_by_source[source_id] = (entry, spec, overrides, queries)
    return queries


def _lookup_openapi_table(state, table_name: str):
    """Find an openapi query registration by its operation_id (the SQL table name).

    REQ-1730: unlike graphql_remote/grpc_remote (state.graphql_remote_sources/
    grpc_remote_sources — a pre-parsed cache), openapi had no equivalent lookup here. The
    operations come from ``state.openapi_specs[source_id]["spec"]`` (already boot-safe — populated
    by _load_openapi_specs, which reads the sources table directly, control-plane-only sources
    included), parsed once per spec entry (:func:`_openapi_queries`): this runs for every table of
    every statement the pipeline routes.
    """
    from provisa.compiler.naming import apply_sql_name as _asn

    normalised = _asn(table_name)
    specs = getattr(state, "openapi_specs", {})
    for source_id, entry in specs.items():
        queries = _openapi_queries(state, source_id, entry)
        if queries is None:
            continue
        for q in queries:
            if q.operation_id == table_name or _asn(q.operation_id) == normalised:
                return source_id, entry, q
    return None, None, None


async def _promote_joined_from_fills(
    state, ep, tn, hot, col_names, meta_cols, cache_loc, hot_threshold
) -> None:
    """Read a joined API table's fills from the store and hold them in the hot tier for the next
    request's Values CTE. Best effort: the hot copy is an optimization of a later request, and
    this request already has its answer."""
    try:
        rows = await _mat_fetch_rows_from_fills(ep, col_names, meta_cols, state)
        if 0 < len(rows) <= hot_threshold:
            hot.hold(
                tn,
                catalog=cache_loc.catalog,
                schema=cache_loc.schema,
                pk_column=col_names[0] if col_names else "id",
                rows=rows,
                column_names=col_names,
                whole=True,
            )
            log.warning(
                "[MAT] promoted %s → hot tier (%d rows) for next-request Values CTE", tn, len(rows)
            )
    except Exception as exc:
        log.warning("[MAT] _promote_joined_from_fills failed for %s: %s", tn, exc)


def _normalize_mat_value(v):
    """Normalize a value for materialization into the engine cache (VARCHAR/scalar types)."""
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    return str(v)


async def _fetch_gql_remote_rows(
    gql_reg, gql_tbl, col_selections, variables, gql_to_sql, max_items, max_rows
) -> tuple[list[dict], bool]:
    """Fetch a graphql_remote field (with its native-filter args) and remap each row's GQL field
    keys to the sql column names the store lands under. A single-record field returns null (→ [None])
    when nothing matches — drop non-dict rows so the caller lands an empty result, not a crash.
    The flag: the read stopped at max_rows with more to read (REQ-1350)."""
    from provisa.graphql_remote.executor import NO_POLICY, execute_remote

    answer = await execute_remote(
        url=gql_reg["url"],
        auth=gql_reg.get("auth"),
        field_name=gql_tbl.get("field_name") or gql_tbl["name"],
        columns=col_selections,
        variables=variables or None,
        required_args=gql_tbl.get("required_args") or None,
        limit=max_items,
        pagination=gql_tbl.get("pagination"),
        rows_path=gql_tbl.get("rows_path"),
        max_rows=max_rows,
        error_policy=gql_reg.get("error_policy") or NO_POLICY,
    )
    rows = [
        {gql_to_sql.get(k, k): v for k, v in row.items()}
        for row in answer.rows
        if isinstance(row, dict)
    ]
    return rows, answer.cut


async def _mat_gql_remote_table(
    tn: str,
    gql_reg: dict,
    gql_tbl: dict,
    state,
    hot: _StatementHot,
    _hot_threshold: int,
    cache_rewrites: dict,
    values_cte_entries: dict,
    extra_selections: dict[str, str] | None = None,
    variables: dict | None = None,
) -> None:
    """Materialize a graphql_remote-backed table into the engine cache or VALUES CTE."""
    from provisa.api_source.engine_cache import (
        org_cache_schema,
        cache_location,
        cache_table_name,
        ensure_cache_schema,
        land_api_cache,
        resolved_cache_catalog,
        schedule_drop,
        table_known_live,
    )
    from provisa.executor.redirect import RedirectConfig
    from dataclasses import dataclass as _dc

    @_dc
    class _GCol:
        name: str
        type: str

    _GQL_TYPE_MAP = {
        "text": "string",
        "integer": "integer",
        "numeric": "number",
        "boolean": "boolean",
        "jsonb": "jsonb",
    }
    col_dicts = list(gql_tbl.get("columns", []))
    if extra_selections:
        _existing_names = {c["name"] for c in col_dicts}
        for _fname, _gql_sel in extra_selections.items():
            if _fname not in _existing_names:
                col_dicts.append({"name": _fname, "type": "jsonb", "gql_selection": _gql_sel})
    _gql_srcs = getattr(state, "graphql_remote_sources", {})
    _governed_gql_types = {
        tbl.get("gql_type_name")
        for reg in _gql_srcs.values()
        for tbl in reg.get("tables", [])
        if tbl.get("gql_type_name")
    }
    if _governed_gql_types:
        _governed_excluded = {
            c["name"]
            for c in col_dicts
            if c.get("gql_object_type")
            and not c.get("gql_is_list", False)
            and c["gql_object_type"] in _governed_gql_types
        }
        if _governed_excluded:
            col_dicts = [c for c in col_dicts if c["name"] not in _governed_excluded]
    from provisa.compiler.naming import (
        apply_sql_name as _apply_sql_name,
        apply_gql_name as _apply_gql_name,
    )

    def _gql_selection(c: dict) -> str:
        # The store lands under the semantic sql name; the remote keys the field by its GraphQL
        # name. When they differ, emit a GraphQL alias ``<sql_name>: <gqlField>`` so the outbound
        # field matches the remote AND the response comes back keyed by the sql name the store
        # expects; when they coincide, the bare field. gql_selection (nested object path) still
        # wins. Mirrors source_loader.py's _selection / cypher_exec.py's _gql_selection.
        if c.get("gql_selection"):
            return c["gql_selection"]
        sql_name = _apply_sql_name(c["name"])
        gql_field = _apply_gql_name(c["name"])
        return gql_field if sql_name == gql_field else f"{sql_name}: {gql_field}"

    def _sel_from_obj_fields(fname: str, sub_fields: list) -> str:
        parts = []
        for sf in sub_fields or []:
            if sf.get("fields"):
                parts.append(_sel_from_obj_fields(sf["name"], sf["fields"]))
            else:
                parts.append(sf["name"])
        return f"{fname} {{ {' '.join(parts)} }}" if parts else fname

    # Synthesize gql_selection from gql_object_fields when not explicitly set
    for _c in col_dicts:
        if not _c.get("gql_selection") and _c.get("gql_object_fields"):
            _c["gql_selection"] = _sel_from_obj_fields(_c["name"], _c["gql_object_fields"])

    # Map raw GQL field name → SQL name (snake_case) so CTE headers match SQL column refs
    _gql_to_sql = {c["name"]: _apply_sql_name(c["name"]) for c in col_dicts}
    col_names = [_gql_to_sql[c["name"]] for c in col_dicts]
    col_selections = [_gql_selection(c) for c in col_dicts]
    col_objs = [
        _GCol(name=_gql_to_sql[c["name"]], type=_GQL_TYPE_MAP.get(c.get("type", "text"), "string"))
        for c in col_dicts
    ]

    _cache_cat = resolved_cache_catalog(state.federation_engine)
    # REQ-1623: the cache belongs to the org and environment that filled it, so retiring the
    # environment removes it and no org or environment serves another's cached rows.
    gql_cache_loc = cache_location(
        gql_reg["source_id"], _cache_cat, org_cache_schema(state, "_gql_cache")
    )
    _cache_hash: dict = {"cols": sorted(col_selections)}
    if variables:
        _cache_hash.update(variables)
    gql_cache_tbl = cache_table_name(gql_reg["source_id"], tn, _cache_hash)

    redirect_config = RedirectConfig.from_env()

    # A schema-less materialization store (SQLite) has no separate cache schema to CREATE, so the
    # engine-cache path (ensure_cache_schema → CREATE SCHEMA) is unavailable. Fetch fresh from the
    # remote and inject the result INLINE as a VALUES CTE — the parameterized fetch is bounded by
    # max_list_items, and an empty result still injects an empty CTE (query returns []).
    from urllib.parse import urlparse as _urlparse

    _store_scheme = _urlparse(state.federation_engine.materialize_store_dsn()).scheme.split("+", 1)[
        0
    ]
    _max_items = state.config.graphql_remote.max_list_items
    from provisa.federation.registry_view import connection_rows

    _max_rows = await connection_rows(state, gql_reg["source_id"], tn)  # REQ-318
    if _store_scheme == "sqlite":
        gql_rows, _cut = await _fetch_gql_remote_rows(
            gql_reg, gql_tbl, col_selections, variables, _gql_to_sql, _max_items, _max_rows
        )
        # Inline THIS query only — never held by the hot tier: a parameterized fetch is keyed by
        # its arg, so holding it as the table's rows would serve one arg's rows for another.
        values_cte_entries[tn] = InlineRows(gql_rows, col_names)
        return

    # Cache hit — only trust in-process table_known_live
    with state.federation_engine.isolated_sync() as _c:
        ensure_cache_schema(_c, gql_cache_loc)
    if table_known_live(gql_cache_loc, gql_cache_tbl):
        cache_rewrites[tn] = (gql_cache_loc, gql_cache_tbl)
        return

    # Cache miss — fetch from remote
    try:
        gql_rows, cut = await _fetch_gql_remote_rows(
            gql_reg, gql_tbl, col_selections, variables, _gql_to_sql, _max_items, _max_rows
        )
    except Exception as fetch_exc:
        raise RuntimeError(f"GQL remote fetch failed for {tn!r}: {fetch_exc}") from fetch_exc
    if cut:
        # REQ-1350: an answer cut at max_rows lands under a name of this statement's own, so no
        # later statement finds it as the table's answer, and it is never held hot.
        gql_cache_tbl = cache_table_name(
            gql_reg["source_id"], tn, {**_cache_hash, "__cut__": secrets.token_hex(8)}
        )

    # Hydrate to the engine cache (best-effort)
    try:
        await land_api_cache(
            state.federation_engine, gql_cache_loc, gql_cache_tbl, gql_rows, col_objs
        )
        schedule_drop(state.federation_engine, gql_cache_loc, gql_cache_tbl, 300, redirect_config)
    except Exception as cache_exc:
        log.warning("[GQL REMOTE] cache write failed for %s: %s", tn, cache_exc)

    # Inline as VALUES CTE if below threshold; else use cache rewrite
    if 0 < len(gql_rows) <= _hot_threshold:
        entry = hot.hold(
            tn,
            catalog=gql_cache_loc.catalog,
            schema=gql_cache_loc.schema,
            pk_column=col_names[0] if col_names else "id",
            rows=gql_rows,
            column_names=col_names,
            # A fetch with variables, or a cut answer (REQ-1350), is not the table's rows.
            whole=not variables and not cut,
        )
        values_cte_entries[tn] = entry
        log.warning("[GQL REMOTE] VALUES CTE inline for %s (%d rows)", tn, len(gql_rows))
    else:
        cache_rewrites[tn] = (gql_cache_loc, gql_cache_tbl)
        log.warning(
            "[GQL REMOTE] %d rows → the engine cache %s.%s.%s",
            len(gql_rows),
            gql_cache_loc.catalog,
            gql_cache_loc.schema,
            gql_cache_tbl,
        )


async def _mat_grpc_remote_table(
    tn: str,
    source_id: str,
    reg: dict,
    grpc_query,
    state,
    hot: _StatementHot,
    _hot_threshold: int,
    cache_rewrites: dict,
    values_cte_entries: dict,
    nf_args: dict | None = None,
) -> None:
    """Materialize a grpc_remote query's result into the engine cache or VALUES CTE.

    Mirrors _mat_gql_remote_table: the raw-SQL surface (/data/sql, pgwire) has no live-fetch
    handler of its own, so without this a grpc_remote table left decide_route at Route.API with
    no source_pools entry, and _execute_plan_in_org's no-native-pool fallback ran the query
    against tenant_db instead of the engine ("no such table"). Reuses the same PG-cache-table +
    VALUES-CTE mechanism endpoint_executors._execute_grpc_remote_source uses for the compiled
    GraphQL path.
    """
    from dataclasses import dataclass as _dc

    from provisa.api_source.engine_cache import (
        org_cache_schema,
        cache_location,
        cache_table_name,
        ensure_cache_schema,
        land_api_cache,
        resolved_cache_catalog,
        schedule_drop,
        table_known_live,
    )
    from provisa.api.data.endpoint_helpers import _grpc_cache_type
    from provisa.cache.store import NoopCacheStore
    from provisa.executor.redirect import RedirectConfig
    from provisa.source_adapters import grpc_remote_adapter

    @_dc
    class _GCol:
        name: str
        type: str

    col_names = [c.name for c in grpc_query.columns] if grpc_query.columns else []
    cache_cols = (
        [_GCol(name=c.name, type=_grpc_cache_type(c.type)) for c in grpc_query.columns]
        if grpc_query.columns
        else [_GCol(name=n, type="string") for n in col_names]
    )

    _cache_cat = resolved_cache_catalog(state.federation_engine)
    cache_loc = cache_location(source_id, _cache_cat, org_cache_schema(state))
    cache_tbl = cache_table_name(source_id, tn, nf_args or {})
    redirect_config = RedirectConfig.from_env()

    with state.federation_engine.isolated_sync() as _c:
        ensure_cache_schema(_c, cache_loc)

    if table_known_live(cache_loc, cache_tbl):
        cache_rewrites[tn] = (cache_loc, cache_tbl)
        return

    rows = await grpc_remote_adapter.fetch(
        source_id=source_id,
        full_method_path=grpc_query.full_method_path,
        input_message_name=grpc_query.input_message,
        output_message_name=grpc_query.output_message,
        pb2=reg["pb2"],
        args=nf_args or {},
        grpc_remote_sources=getattr(state, "grpc_remote_sources", {}),
        response_cache_store=NoopCacheStore(),  # the PG cache table is the cache, not Redis
        ttl=reg.get("cache_ttl", 300),
        server_streaming=grpc_query.server_streaming,
    )
    if not col_names:
        col_names = list(rows[0].keys()) if rows else []
        cache_cols = [_GCol(name=n, type="string") for n in col_names]

    if rows:
        try:
            await land_api_cache(state.federation_engine, cache_loc, cache_tbl, rows, cache_cols)
            schedule_drop(
                state.federation_engine,
                cache_loc,
                cache_tbl,
                reg.get("cache_ttl", 300),
                redirect_config,
            )
        except Exception as cache_exc:
            log.warning("[GRPC REMOTE] cache write failed for %s: %s", tn, cache_exc)

    if 0 < len(rows) <= _hot_threshold:
        entry = hot.hold(
            tn,
            catalog=cache_loc.catalog,
            schema=cache_loc.schema,
            pk_column=col_names[0] if col_names else "id",
            rows=rows,
            column_names=col_names,
            whole=not nf_args,
        )
        values_cte_entries[tn] = entry
        log.warning("[GRPC REMOTE] VALUES CTE inline for %s (%d rows)", tn, len(rows))
    else:
        cache_rewrites[tn] = (cache_loc, cache_tbl)
        log.warning(
            "[GRPC REMOTE] %d rows → the engine cache %s.%s.%s",
            len(rows),
            cache_loc.catalog,
            cache_loc.schema,
            cache_tbl,
        )


async def _mat_openapi_table(
    tn: str,
    source_id: str,
    entry: dict,
    query,
    state,
    hot: _StatementHot,
    _hot_threshold: int,
    cache_rewrites: dict,
    values_cte_entries: dict,
    nf_args: dict | None = None,
) -> None:
    """Materialize an openapi query's result into the engine cache or VALUES CTE.

    Mirrors _mat_grpc_remote_table (see its own comment for why this exists at all): the raw-SQL
    surface (/data/sql, pgwire) has no live-fetch handler of its own, so without this an openapi
    table left decide_route at Route.API with no source_pools entry, and the no-native-pool
    fallback ran the query against tenant_db (the control-plane database, not the materialize
    store) instead — "relation ... does not exist". Invisible until REQ-1730's own reboot-harness
    e2e (a genuine restart with no mutation replay) exercised a query against an openapi table
    with no dynamic (native-filter) columns: Trino never hits this path at all — it has its own
    TrinoOpenapiConnector reading the landed materialize-store table directly as a real engine
    catalog — and graphql_remote/grpc_remote/govdata, the other REQ-1730 MATERIALIZE_ONLY types
    that DO land eagerly, happen to have no query parameters either, so decide_route's blanket
    Route.API classification (by TYPE, not by whether this specific table actually needs
    per-query dynamic fetching) was never exercised for the one type missing this fallback.
    """
    from dataclasses import dataclass as _dc

    from provisa.api_source.engine_cache import (
        org_cache_schema,
        cache_location,
        cache_table_name,
        ensure_cache_schema,
        land_api_cache,
        resolved_cache_catalog,
        schedule_drop,
        table_known_live,
    )
    from provisa.cache.store import NoopCacheStore
    from provisa.core.secrets import resolve_secrets
    from provisa.executor.redirect import RedirectConfig
    from provisa.openapi import executor as openapi_executor
    from provisa.openapi.register import _schema_to_columns

    @_dc
    class _OCol:
        name: str
        type: str

    schema_cols = _schema_to_columns(query.response_schema)
    col_names = [c["name"] for c in schema_cols]
    cache_cols = [_OCol(name=c["name"], type=c["type"]) for c in schema_cols]

    _cache_cat = resolved_cache_catalog(state.federation_engine)
    cache_loc = cache_location(source_id, _cache_cat, org_cache_schema(state))
    cache_tbl = cache_table_name(source_id, tn, nf_args or {})
    redirect_config = RedirectConfig.from_env()

    with state.federation_engine.isolated_sync() as _c:
        ensure_cache_schema(_c, cache_loc)

    if table_known_live(cache_loc, cache_tbl):
        cache_rewrites[tn] = (cache_loc, cache_tbl)
        return

    auth_config = entry.get("auth_config")
    if auth_config:
        auth_config = {
            k: (resolve_secrets(v) if isinstance(v, str) else v) for k, v in auth_config.items()
        }
    rows = await openapi_executor.fetch(
        base_url=entry.get("base_url", ""),
        query=query,
        args=nf_args or {},
        auth_config=auth_config,
        response_cache_store=NoopCacheStore(),  # the PG cache table is the cache, not Redis
        source_id=source_id,
        ttl=entry.get("cache_ttl", 300),
    )
    if not col_names:
        col_names = list(rows[0].keys()) if rows else []
        cache_cols = [_OCol(name=n, type="string") for n in col_names]

    if rows:
        try:
            await land_api_cache(state.federation_engine, cache_loc, cache_tbl, rows, cache_cols)
            schedule_drop(
                state.federation_engine,
                cache_loc,
                cache_tbl,
                entry.get("cache_ttl", 300),
                redirect_config,
            )
        except Exception as cache_exc:
            log.warning("[OPENAPI] cache write failed for %s: %s", tn, cache_exc)

    if 0 < len(rows) <= _hot_threshold:
        hot_entry = hot.hold(
            tn,
            catalog=cache_loc.catalog,
            schema=cache_loc.schema,
            pk_column=col_names[0] if col_names else "id",
            rows=rows,
            column_names=col_names,
            whole=not nf_args,
        )
        values_cte_entries[tn] = hot_entry
        log.warning("[OPENAPI] VALUES CTE inline for %s (%d rows)", tn, len(rows))
    else:
        cache_rewrites[tn] = (cache_loc, cache_tbl)
        log.warning(
            "[OPENAPI] %d rows → the engine cache %s.%s.%s",
            len(rows),
            cache_loc.catalog,
            cache_loc.schema,
            cache_tbl,
        )


async def _mat_fetch_rows_from_fills(ep, col_names: list, _META_COLS: set, state) -> list[dict]:
    """The rows this API table's fills hold in the store (``api_source.fill_cache``) -- ``[]``
    when no fill has made its table yet (a miss the caller fills live). A failed read raises:
    it is never a cue to fetch from REST instead (REQ-1661, amended 2026-09-30)."""
    from provisa.api_source import fill_cache

    table = fill_cache.fill_table(state, ep, getattr(state, "api_sources", {}).get(ep.source_id))
    with state.federation_engine.isolated_sync() as conn:
        raw = fill_cache.read_rows(conn, table)
    col_set = set(col_names)
    return [
        {k: _normalize_mat_value(v) for k, v in r.items() if k not in _META_COLS and k in col_set}
        for r in raw
    ]


async def _mat_fetch_rows_from_rest(
    ep,
    col_names: list,
    engine,
    api_source,
    source_id,
    state,
    _cache_loc,
    cache_tbl,
    cache_rewrites: dict,
    params: dict | None = None,
) -> list | None:
    """Fetch rows for an API endpoint from REST fallback.

    Returns rows list, or None if the table is in cache_rewrites (already handled).
    Raises on unrecoverable REST failure.
    """
    from provisa.api_source.router_integration import handle_api_query

    rest_result = await handle_api_query(
        ep,
        params or {},
        engine,
        source=api_source,
        source_ttl=getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl"),
        global_ttl=getattr(state, "response_cache_default_ttl", None),
        loc=_cache_loc,
    )
    log.warning(
        "[MAT] REST fallback for %s: from_cache=%s rows=%d",
        ep.table_name,
        rest_result.from_cache,
        len(rest_result.rows),
    )
    if rest_result.from_cache:
        cache_rewrites[ep.table_name] = (_cache_loc, cache_tbl)
        return None
    col_set = set(col_names)
    return [
        {k: _normalize_mat_value(v) for k, v in r.items() if k in col_set} for r in rest_result.rows
    ]


def _mat_store_rows(
    tn: str,
    rows: list,
    col_names: list,
    _cache_loc,
    cache_tbl: str,
    _hot_threshold: int,
    hot: _StatementHot,
    response_cols: list,
    engine,
    ttl,
    redirect_config,
    cache_rewrites: dict,
    values_cte_entries: dict,
    all_ep_col_names: list | None = None,
    *,
    whole: bool,
) -> None:
    """ALWAYS persist rows to the materialization store (the durable source of truth), then inline a
    small table as a VALUES CTE for this query — the hot cache is a rebuildable projection of the
    store, so an inlined small table survives a restart (re-promoted from the store, not re-fetched)."""
    from provisa.api_source.engine_cache import (
        analyze_cache_table,
        create_and_insert,
        schedule_drop,
    )
    from provisa.compiler.naming import apply_sql_name as _apply_sql_name

    # Column names must match the compiler's snake_case output.
    _name_map = {c.name: _apply_sql_name(c.name) for c in response_cols}
    _snake_cols = [c.model_copy(update={"name": _apply_sql_name(c.name)}) for c in response_cols]
    _snake_rows = [{_name_map.get(k, k): v for k, v in r.items()} for r in rows]
    with engine.isolated_sync() as _c:
        create_and_insert(_c, _cache_loc, cache_tbl, _snake_rows, _snake_cols)
    # REQ-1688: statistics where the table lives, off the query's critical path.
    spawn_background(analyze_cache_table(engine, _cache_loc, cache_tbl))
    schedule_drop(engine, _cache_loc, cache_tbl, ttl, redirect_config)
    log.warning("[MAT] persisted %d rows → store %s", len(rows), cache_tbl)

    if 0 < len(rows) <= _hot_threshold:
        # Small + not hot → promote to the hot cache and inline for THIS query. Include all endpoint
        # columns (response + params) so generated SQL referencing a param column resolves to NULL.
        hot_col_names = all_ep_col_names if all_ep_col_names else col_names
        entry = hot.hold(
            tn,
            catalog=_cache_loc.catalog,
            schema=_cache_loc.schema,
            pk_column=col_names[0] if col_names else "id",
            # _snake_rows, not rows: the CTE reads row[c] for c in column_names, and those
            # names are snake_case. Raw camelCase keys would silently inline NULL.
            rows=_snake_rows,
            column_names=hot_col_names,
            whole=whole,
        )
        values_cte_entries[tn] = entry
        log.warning("[MAT] + hot VALUES CTE inline for %s (%d rows)", tn, len(rows))
    else:
        cache_rewrites[tn] = (_cache_loc, cache_tbl)


async def _mat_api_ep_table(
    tn: str,
    ep,
    state,
    hot: _StatementHot,
    _hot_threshold: int,
    _META_COLS: set,
    cache_rewrites: dict,
    values_cte_entries: dict,
    nf_args: dict | None = None,
) -> None:
    """Materialize a REST API endpoint-backed table into the engine cache or VALUES CTE."""
    from provisa.api_source.engine_cache import (
        cache_table_name,
        ensure_cache_schema,
        table_exists,
        table_known_live,
    )
    from provisa.executor.redirect import RedirectConfig

    source_id = ep.source_id
    api_source = getattr(state, "api_sources", {}).get(source_id)

    # REQ-1730/REQ-1623: the source's API cache, where its fills are too.
    from provisa.api_source.fill_cache import source_cache_location

    _cache_loc = source_cache_location(state, source_id, api_source)
    cache_tbl = cache_table_name(source_id, tn, {})
    ttl = (
        getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl")
        or getattr(state, "response_cache_default_ttl", None)
        or ep.ttl
    )
    from provisa.compiler.naming import apply_sql_name

    # param_only, not param_type: a query/path param whose name collides with a response field is
    # merged into one column that carries BOTH. Excluding it on param_type would drop its value.
    response_cols = [c for c in ep.columns if not c.param_only]
    col_names = [c.name for c in response_cols]
    all_ep_col_names = [apply_sql_name(c.name) for c in ep.columns]
    redirect_config = RedirectConfig.from_env()
    # An endpoint with a parameter is a function of its arguments: what its fills or a fetch hold
    # is the rows for some arguments, never the table's whole rows, so none of it is held hot.
    whole = not any(c.param_type for c in ep.columns)

    if not response_cols:
        log.warning("[MAT] %s has no response columns — skipping", tn)
        return

    # Priority 2: the engine cache hit
    if table_known_live(_cache_loc, cache_tbl):
        log.warning(
            "[MAT] the engine cache hit for %s → %s.%s.%s",
            tn,
            _cache_loc.catalog,
            _cache_loc.schema,
            cache_tbl,
        )
        cache_rewrites[tn] = (_cache_loc, cache_tbl)
        if whole and hot.holds(tn):
            spawn_background(
                _promote_joined_from_fills(
                    state, ep, tn, hot, col_names, _META_COLS, _cache_loc, _hot_threshold
                )
            )
        return

    with state.federation_engine.isolated_sync() as _c:
        ensure_cache_schema(_c, _cache_loc)
        _cache_hit = table_exists(_c, _cache_loc, cache_tbl, ttl=ttl)
    if _cache_hit:
        log.warning(
            "[MAT] the engine cache hit for %s → %s.%s.%s",
            tn,
            _cache_loc.catalog,
            _cache_loc.schema,
            cache_tbl,
        )
        cache_rewrites[tn] = (_cache_loc, cache_tbl)
        if whole and hot.holds(tn):
            spawn_background(
                _promote_joined_from_fills(
                    state, ep, tn, hot, col_names, _META_COLS, _cache_loc, _hot_threshold
                )
            )
        return

    # Priority 3: cache miss — the fills in the store, then REST
    rows = await _mat_fetch_rows_from_fills(ep, col_names, _META_COLS, state)

    if not rows:
        path_cols = [c for c in ep.columns if c.param_type == "path"]
        rest_params: dict = {}
        if path_cols:
            from provisa.compiler.naming import apply_sql_name as _apply_sql_name

            _nf_canon = {_apply_sql_name(k.lstrip("_")): v for k, v in (nf_args or {}).items()}
            missing = []
            for c in path_cols:
                canon = _apply_sql_name(c.name)
                if canon in _nf_canon:
                    rest_params[c.name] = _nf_canon[canon]
                else:
                    missing.append(c.name)
            if missing:
                # Required path param(s) absent from this query — cannot call the endpoint
                # generically (mirrors the graphql_remote required_args branch above).
                log.warning("[MAT] %s requires path param(s) %s — skipping", tn, missing)
                return
        # REQ-1661 (amended 2026-09-30): a failed live fetch fails the query -- it is never
        # logged and skipped, which left the engine answering from whatever cache it held.
        rows = await _mat_fetch_rows_from_rest(
            ep,
            col_names,
            state.federation_engine,
            api_source,
            source_id,
            state,
            _cache_loc,
            cache_tbl,
            cache_rewrites,
            params=rest_params,
        )
        if rows is None:
            return  # already written to cache_rewrites by _mat_fetch_rows_from_rest

    # A cut answer (the call stopped at max_pages with more to read, warned earlier in this
    # statement) is never cached as complete: its rows go to a table of this statement's own,
    # which no later request looks up, and never to the hot tier.
    cut = _cut_in_statement(ep.table_name)
    if cut:
        cache_tbl = cache_table_name(source_id, tn, {"__cut__": secrets.token_hex(8)})
    _mat_store_rows(
        tn,
        rows,
        col_names,
        _cache_loc,
        cache_tbl,
        _hot_threshold,
        hot,
        response_cols,
        state.federation_engine,
        ttl,
        redirect_config,
        cache_rewrites,
        values_cte_entries,
        all_ep_col_names=all_ep_col_names,
        # A cut answer (REQ-1350) is the rows this statement asked for, never the table's.
        whole=whole and not cut,
    )


def _cut_in_statement(table_name: str) -> bool:
    """Whether this statement already warned that ``table_name``'s answer was cut."""
    from provisa.core.statement_warnings import raised

    return any(w.code == "api.answer_cut" and w.params.get("table") == table_name for w in raised())


def would_materialize_optimize(exec_sql: str, state, *, table_ids: Iterable[int]) -> bool:
    """REQ-1877 routing addendum: cheap, no-I/O predictor of whether
    `_materialize_api_to_engine_cache(exec_sql, state, ...)` would do ANYTHING for this call —
    i.e. whether its LIVE/time-varying branches (hot-table inline, TTL-cached API-endpoint fetch,
    graphql_remote/grpc_remote/openapi remote fetch) are reachable at all.

    Mirrors that function's own control flow table name-by-name, using the exact same lookups
    (`find_api_table_names`, `_lookup_ep`, `_lookup_gql_remote_table`, `_lookup_grpc_remote_table`,
    `_lookup_openapi_table`) — all in-memory dict lookups / CPU-only spec re-parses (see
    `_lookup_openapi_table`'s own docstring), never I/O. Returns True the moment any table COULD
    reach a live branch; a caller must treat True as "cannot prove this call is a no-op" and take
    the full, uncached path.

    IMPORTANT: `find_api_table_names` returns every table name in the query's FROM/JOIN clauses,
    not only API-backed ones — an ordinary multi-table SQL query is NOT "no candidates" just
    because it has tables; it only returns False here once every one of those tables is checked
    and none is hot or registered as an API/graphql_remote/grpc_remote/openapi table (a
    row_materialize table is skipped). A table registered as a (non-hot) API endpoint is ALWAYS
    live here: its TTL cache can go stale between calls.
    """
    from provisa.compiler.nf_extractor import find_api_table_names

    table_names = find_api_table_names(exec_sql)
    if not table_names:
        return False
    hot = _StatementHot(getattr(state, "hot_manager", None), state, table_ids)
    row_materialize_table_names = {
        t.get("table_name")
        for t in (getattr(state, "tables", None) or [])
        if t.get("row_materialize")
    }
    for tn in table_names:
        if tn in row_materialize_table_names:
            continue
        if tn in hot.entries:
            return True
        if _lookup_ep(state, tn) is not None:
            return True
        gql_reg, _gql_tbl = _lookup_gql_remote_table(state, tn)
        if gql_reg is not None:
            return True
        _grpc_source_id, grpc_reg, _grpc_query = _lookup_grpc_remote_table(state, tn)
        if grpc_reg is not None:
            return True
        _oa_source_id, oa_entry, _oa_query = _lookup_openapi_table(state, tn)
        if oa_entry is not None:
            return True
    return False


async def _materialize_api_to_engine_cache(
    exec_sql: str,
    state,
    gql_remote_extra_selections: dict | None = None,
    nf_args: dict | None = None,
    *,
    table_ids: Iterable[int],
) -> tuple[dict, dict, dict[str, str]]:
    """Materialize API-backed tables into the engine cache (VARCHAR columns) before the engine SQL runs.

    Avoids INVALID_CAST_ARGUMENT: the engine's PG connector exposes JSONB as json type;
    cache tables store all columns as VARCHAR/scalar types instead.

    Reads the fills _hydrate_api_tables_before_engine keeps in the store — no HTTP call.
    Returns (cache_rewrites, values_cte_entries, dropped_tables):
      cache_rewrites: {physical_table_name: (CacheLocation, cache_tbl)}
      values_cte_entries: {physical_table_name: rows} — inlined as VALUES CTEs

    ``table_ids`` are the registered tables the statement reads, as the pipeline resolved them:
    the hot rows substituted are theirs, and rows fetched here are held under them.
      dropped_tables: {physical_table_name: reason} whose UNION branches should be dropped -- a
        table the query cannot address because it gives none of the table's required filter /
        path parameters. A table with no UNION to drop from survives the branch-drop, and
        ``nf_extractor.apply_dropped_tables`` raises the reason for it. A remote that fails is
        never dropped: its error fails the query (REQ-1661, amended 2026-09-30).
    """
    from provisa.compiler.nf_extractor import find_api_table_names

    cache_rewrites: dict = {}
    values_cte_entries: dict = {}
    dropped_tables: dict[str, str] = {}
    hot_mgr = getattr(state, "hot_manager", None)
    table_names = find_api_table_names(exec_sql)
    if not table_names:
        return cache_rewrites, values_cte_entries, dropped_tables
    hot = _StatementHot(hot_mgr, state, table_ids)

    _META_COLS = {"_params_hash", "_cached_at"}
    _hot_threshold = hot_mgr.auto_threshold if hot_mgr is not None else 500
    # REQ-1865: a row_materialize=True table's residency is governed EXCLUSIVELY by the
    # row-level cache (ensure_rows_resident, invoked later once routing/pk_bounds resolve) --
    # never by this whole-table API-cache materialize, which has no keyed/filtered fetch concept
    # at all and would otherwise fetch every row via REST on every cache miss regardless of how
    # narrow the query's own predicate is. Confirmed live: a single-key point lookup against a
    # row_materialize-enabled neo4j table (itself registered as an ApiEndpoint, REQ-1668) paid the
    # full-table REST fetch cost here, entirely bypassing the row cache -- this runs earlier in the
    # pipeline (_optimize_and_route, before routing) than ensure_resident/ensure_rows_resident ever
    # get a chance to engage.
    _row_materialize_table_names = {
        t.get("table_name")
        for t in (getattr(state, "tables", None) or [])
        if t.get("row_materialize")
    }

    for tn in table_names:
        if tn in _row_materialize_table_names:
            continue
        # Hot cache: inline rows as VALUES CTE — avoids cross-catalog JOIN entirely
        entry = hot.entries.get(tn)
        if entry is not None:
            values_cte_entries[tn] = entry
            log.warning("[MAT] hot VALUES CTE for %s (%d rows inline)", tn, len(entry.rows))
            continue

        ep = _lookup_ep(state, tn)
        if ep is None:
            gql_reg, gql_tbl = _lookup_gql_remote_table(state, tn)
            if gql_reg is not None:
                assert gql_tbl is not None
                assert isinstance(gql_tbl, dict)
            if gql_reg is not None and gql_tbl is not None:
                req_args = gql_tbl.get("required_args") or []
                # required_args carry the REMOTE arg name (e.g. ``name``, ``breedName``); the
                # extracted nf_args are keyed by the GraphQL schema arg, which Provisa prefixes
                # with ``_`` when it collides with a scalar field and stores in sql convention
                # (``breedName`` → ``_breed_name``). Match through the naming authority: both
                # sides reduce to the same sql name once the disambiguation ``_`` is dropped.
                from provisa.compiler.naming import apply_sql_name as _apply_sql_name

                _nf_canon = {_apply_sql_name(k.lstrip("_")): v for k, v in (nf_args or {}).items()}
                # Every nf_arg the query pushed down for this table must distinguish the
                # materialized cache entry — a required arg resolves to its REMOTE name (so it
                # is also forwarded to the remote fetch); any other filter still keyed here
                # under its canonical name so two queries that differ only by that filter never
                # collide on the same cache_table_name (REQ-848/REQ-941 scoped this for
                # required args only, leaving non-required filters unkeyed).
                resolved = dict(_nf_canon)
                missing = []
                for a in req_args:
                    canon = _apply_sql_name(a["name"].lstrip("_"))
                    if canon in _nf_canon:
                        resolved[a["name"]] = resolved.pop(canon)
                    else:
                        missing.append(a["name"])
                if missing:
                    # Required filter(s) absent — exclude the object (drop its union branch) so a
                    # broad sweep (graph counts, multi-label union) skips it instead of erroring.
                    # A table with no union to drop from (a lone FROM target) survives the branch
                    # drop and is surfaced as this exact reason by nf_extractor.apply_dropped_tables
                    # instead of reaching the engine as an unqualified, confusing catalog error.
                    log.warning("[MAT] %s requires filter(s) %s — dropping branch", tn, missing)
                    dropped_tables[tn] = (
                        f"requires filter(s) {missing} — add a WHERE clause with the "
                        "required parameter(s)"
                    )
                else:
                    # REQ-1661 (amended 2026-09-30): an unreachable remote fails the whole
                    # query -- its UNION branch is never dropped to return the rest as complete.
                    await _mat_gql_remote_table(
                        tn,
                        gql_reg,
                        gql_tbl,
                        state,
                        hot,
                        _hot_threshold,
                        cache_rewrites,
                        values_cte_entries,
                        extra_selections=(gql_remote_extra_selections or {}).get(tn),
                        variables=resolved or None,
                    )
                continue

            grpc_source_id, grpc_reg, grpc_query = _lookup_grpc_remote_table(state, tn)
            if grpc_reg is not None and grpc_query is not None:
                assert grpc_source_id is not None
                await _mat_grpc_remote_table(  # a failed fetch fails the query (REQ-1661)
                    tn,
                    grpc_source_id,
                    grpc_reg,
                    grpc_query,
                    state,
                    hot,
                    _hot_threshold,
                    cache_rewrites,
                    values_cte_entries,
                    nf_args=nf_args,
                )
                continue

            # REQ-1730: openapi (also API_SOURCES) had no fallback here at all — see
            # _mat_openapi_table's own comment for the exact failure this closes.
            oa_source_id, oa_entry, oa_query = _lookup_openapi_table(state, tn)
            if oa_entry is not None and oa_query is not None:
                assert oa_source_id is not None
                await _mat_openapi_table(  # a failed fetch fails the query (REQ-1661)
                    tn,
                    oa_source_id,
                    oa_entry,
                    oa_query,
                    state,
                    hot,
                    _hot_threshold,
                    cache_rewrites,
                    values_cte_entries,
                    nf_args=nf_args,
                )
            continue

        await _mat_api_ep_table(
            tn,
            ep,
            state,
            hot,
            _hot_threshold,
            _META_COLS,
            cache_rewrites,
            values_cte_entries,
            nf_args=nf_args,
        )
        if tn not in cache_rewrites and tn not in values_cte_entries:
            log.warning("[MAT] %s could not be materialized — dropping union branch", tn)
            dropped_tables[tn] = "could not be materialized"

    return cache_rewrites, values_cte_entries, dropped_tables
