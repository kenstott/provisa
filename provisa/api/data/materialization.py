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
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
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


def _statement_endpoints(state, table_ids: Iterable[int]) -> dict:
    """The endpoints of the API tables the statement reads, by the names its SQL carries."""
    from provisa.api_source.endpoints import statement_endpoints

    return statement_endpoints(state, table_ids)


def remote_arguments(gql_tbl: dict) -> list[dict]:
    """A remote GraphQL table's arguments, required and optional (REQ-1986): the fetch sends those
    the read gives a value for."""
    return [*(gql_tbl.get("required_args") or []), *(gql_tbl.get("optional_args") or [])]


def _sql_name(name: str) -> str:
    from provisa.compiler.naming import apply_sql_name

    return apply_sql_name(name)


def is_native_read(table: dict, types: dict[str, str]) -> bool:
    """Whether Provisa reads ``table`` itself, per statement, under the statement's arguments: a
    MongoDB pipeline table (REQ-1980), a Cosmos DB table with a query or parameters (REQ-1982),
    a Qdrant search (REQ-1983), an Elasticsearch native search (REQ-2017), or a code source's
    table, a function run in its worker or on its built host (REQ-1984, REQ-1993, REQ-2100). No
    engine runs any of them."""
    from provisa.codesource.table import is_code_table
    from provisa.cosmosdb.table import has_parameters
    from provisa.elasticsearch.native_table import is_native_search_table
    from provisa.mongodb.pipeline_table import is_pipeline_table
    from provisa.qdrant.table import is_search

    if (
        is_code_table(table, types)
        or is_pipeline_table(table, types)
        or is_search(table, types)
        or is_native_search_table(table, types)
    ):
        return True
    return types.get(table["source_id"]) == "cosmosdb" and (
        bool(table.get("query_template")) or has_parameters(table)
    )


def _kept_for_good(state, table: dict) -> bool:
    """REQ-2031: whether ``table`` never expires and its replica has landed in this engine's store
    (``ReplicaRoutes.kept_for_good``). Such a table is read only from its replica: nothing reads
    it in place or runs it, whatever its kind."""
    key = (table["source_id"], table["schema_name"], table["table_name"])
    return key in state.replica_routes.kept_for_good


def _pipeline_tables(state, table_ids: Iterable[int]) -> dict[str, dict]:
    """The tables the statement reads that Provisa reads itself (``is_native_read``), by their
    sql names. A table kept for good is not one of them: its reads go to its replica, so a frozen
    code table never starts its worker (REQ-2031)."""
    ids = set(table_ids)
    types = getattr(state, "source_types", None) or {}
    return {
        _sql_name(t["table_name"]): t
        for t in (getattr(state, "tables", None) or [])
        if t.get("id") in ids and is_native_read(t, types) and not _kept_for_good(state, t)
    }


def frozen_parameter_filters(state, table_ids: Iterable[int]) -> dict[str, str | None]:
    """REQ-2031: each parameter of a native-read table the statement reads that is kept for good
    (``_nf_<name>``), and the landed column its value filters: the table's column of the same
    name, or None when it has none. Its rows are landed, never fetched under the parameter, so a
    condition on the parameter filters them instead (``nf_extractor.extract_nf_args``)."""
    from provisa.core.parameters import NF_PREFIX, is_parameter

    ids = set(table_ids)
    types = getattr(state, "source_types", None) or {}
    filters: dict[str, str | None] = {}
    for t in getattr(state, "tables", None) or []:
        if t.get("id") not in ids or not is_native_read(t, types) or not _kept_for_good(state, t):
            continue
        landed = {c["column_name"] for c in t["columns"] if not is_parameter(c)}
        for c in t["columns"]:
            if is_parameter(c):
                name = c["column_name"].removeprefix(NF_PREFIX)
                filters[c["column_name"]] = name if name in landed else None
    return filters


#: REQ-1986: the scratch tables landed for the statement being planned -- rows of a table with a
#: TTL of 0 too many to inline -- each (CacheLocation, table name), dropped when it ends
#: (``pgwire._pipeline._execute_plan_in_org``). Open while the statement is routed.
_STATEMENT_SCRATCH: ContextVar[list | None] = ContextVar("statement_scratch", default=None)


@contextmanager
def statement_scratch() -> Iterator[list]:
    """Collect the scratch tables landed while one statement is planned (see
    :data:`_STATEMENT_SCRATCH`). A nested statement's go to the one that holds it."""
    held = _STATEMENT_SCRATCH.get()
    if held is not None:
        yield held
        return
    scratch: list = []
    token = _STATEMENT_SCRATCH.set(scratch)
    try:
        yield scratch
    finally:
        _STATEMENT_SCRATCH.reset(token)


def landing_ttl(table: dict, state) -> int:  # REQ-1986
    """The seconds a table's landed rows are kept: its own TTL, else its source's, else the
    response cache's default -- the first one set. A TTL of 0 is set: the rows are never kept."""
    for ttl in (
        table.get("cache_ttl"),
        getattr(state, "source_cache", {}).get(table["source_id"], {}).get("cache_ttl"),
        state.response_cache_default_ttl,
    ):
        if ttl is not None:
            return int(ttl)
    raise ValueError(f"table {table['table_name']!r}: no TTL is set, nor any default")


async def _mat_pipeline_table(
    tn: str,
    table: dict,
    state,
    cache_rewrites: dict,
    values_cte_entries: dict,
    nf_args: dict | None,
    statement: str,
) -> None:
    """Run a pipeline table with the statement's arguments and land its rows for the engine,
    kept under those arguments for the table's TTL (REQ-1980): two argument sets are two
    answers, never one."""
    source_type = state.source_types[table["source_id"]]
    if source_type in ("node", "python"):
        # REQ-2100, REQ-1756: a code table is called by its source's proxy, which plans the
        # read -- its declared predicates a pre-filter -- and its rows are kept under every
        # argument the call was given.
        from provisa.codesource.proxy import CodeProxy
        from provisa.codesource.table import cache_arguments as code_arguments
        from provisa.federation.connector_contract import Read

        proxy = CodeProxy()
        plan = await proxy.plan(
            state,
            Read(
                table=table,
                name=tn,
                statement=statement,
                source_id=table["source_id"],
                arguments=nf_args,
            ),
        )
        await _land_per_statement(
            tn,
            table,
            state,
            cache_rewrites,
            values_cte_entries,
            code_arguments(table, plan.arguments),
            lambda: proxy.call(state, plan),
        )
        return
    if source_type == "qdrant":
        # A search table is landed (REQ-1983): its connector's call, under its arguments.
        from provisa.federation.connector_contract import Plan, Read
        from provisa.qdrant.contract import QdrantContract
        from provisa.qdrant.table import cache_arguments as search_arguments

        search = Plan(
            read=Read(table=table, name=tn, statement=statement, source_id=table["source_id"]),
            arguments=dict(nf_args or {}),
        )
        await _land_per_statement(
            tn,
            table,
            state,
            cache_rewrites,
            values_cte_entries,
            search_arguments(table, nf_args),
            lambda: QdrantContract().call(state, search),
        )
        return
    if source_type == "cosmosdb":
        from provisa.cosmosdb.table import cache_arguments, read
    elif source_type == "elasticsearch":
        from provisa.elasticsearch.native_table import cache_arguments, read
    else:
        from provisa.mongodb.pipeline_table import cache_arguments, read
    await _land_per_statement(
        tn,
        table,
        state,
        cache_rewrites,
        values_cte_entries,
        cache_arguments(table, nf_args),
        lambda: read(state, table, nf_args),
    )


async def mat_live_part(
    tn: str, part, state, cache_rewrites: dict, values_cte_entries: dict
) -> None:
    """REQ-2000: a live-only table's part of an engine-answered statement
    (``federation.live_only``), read live from its source and landed for the engine under the
    part's text, as a pipeline table's rows are under its arguments."""
    from provisa.federation.live_only import read

    await _land_per_statement(
        tn,
        part.table,
        state,
        cache_rewrites,
        values_cte_entries,
        {"part": part.sql},
        lambda: read(state, part),
    )


async def _land_per_statement(
    tn: str,
    table: dict,
    state,
    cache_rewrites: dict,
    values_cte_entries: dict,
    key: dict,
    answer,
) -> None:
    """Hand the engine the rows ``answer()`` reads for one statement's read of ``table``.

    With a TTL above 0 they are landed, kept under ``key`` for the TTL and reused by the next
    statement that asks the same: the reuse pays for the write. With a TTL of 0 the read is live
    for this statement alone (REQ-1986): rows up to the large-result threshold are inlined into
    it as a VALUES CTE, with no write on any engine -- on a warehouse engine a table write costs
    seconds -- and more are landed in a scratch table dropped when the statement ends, never
    kept or reused."""
    from provisa.api_source.engine_cache import (
        analyze_cache_table,
        cache_location,
        cache_table_name,
        ensure_cache_schema,
        org_cache_schema,
        resolved_cache_catalog,
        schedule_drop,
        table_known_live,
    )
    from provisa.executor.redirect import RedirectConfig

    source_id = table["source_id"]
    engine = state.federation_engine
    ttl = landing_ttl(table, state)
    cache_loc = cache_location(source_id, resolved_cache_catalog(engine), org_cache_schema(state))
    if ttl == 0:
        await _answer_live(tn, state, cache_loc, cache_rewrites, values_cte_entries, answer)
        return
    cache_tbl = cache_table_name(source_id, f"pipeline:{tn}", key)
    if table_known_live(cache_loc, cache_tbl):
        cache_rewrites[tn] = (cache_loc, cache_tbl)
        return
    with engine.isolated_sync() as _c:
        ensure_cache_schema(_c, cache_loc)
    answer = await answer()
    await engine.land_source_table(
        schema=cache_loc.schema, table=cache_tbl, columns=answer.columns, rows=answer.rows
    )
    await analyze_cache_table(engine, cache_loc, cache_tbl)
    schedule_drop(engine, cache_loc, cache_tbl, ttl, RedirectConfig.from_env())
    cache_rewrites[tn] = (cache_loc, cache_tbl)
    log.warning("[MAT] pipeline %s: %d rows → %s", tn, len(answer.rows), cache_tbl)


async def _answer_live(
    tn: str, state, cache_loc, cache_rewrites: dict, values_cte_entries: dict, answer
) -> None:
    """A TTL-0 read (REQ-1986): inlined when its rows are within the large-result threshold
    (``redirect.threshold``), else landed in a scratch table of this statement alone."""
    from provisa.api_source.engine_cache import ensure_cache_schema
    from provisa.executor.redirect import RedirectConfig

    read = await answer()
    if len(read.rows) <= RedirectConfig.from_env().threshold:
        values_cte_entries[tn] = InlineRows(read.rows, [name for name, _ in read.columns])
        log.warning("[MAT] live %s: %d rows inline", tn, len(read.rows))
        return
    scratch = _STATEMENT_SCRATCH.get()
    if scratch is None:
        raise RuntimeError(f"{tn!r}: a scratch table is landed only while a statement is planned")
    engine = state.federation_engine
    scratch_tbl = f"scratch__{secrets.token_hex(12)}"
    with engine.isolated_sync() as _c:
        ensure_cache_schema(_c, cache_loc)
    await engine.land_source_table(
        schema=cache_loc.schema, table=scratch_tbl, columns=read.columns, rows=read.rows
    )
    scratch.append((cache_loc, scratch_tbl))
    cache_rewrites[tn] = (cache_loc, scratch_tbl)
    log.warning("[MAT] live %s: %d rows → scratch %s", tn, len(read.rows), scratch_tbl)


async def drop_scratch(scratch, state) -> None:  # REQ-1986
    """Drop the scratch tables a statement landed, now that it has ended."""
    from provisa.api_source.engine_cache import drop_cache_table
    from provisa.executor.redirect import RedirectConfig

    for cache_loc, table in scratch:
        await drop_cache_table(
            state.federation_engine, cache_loc, table, 0, RedirectConfig.from_env()
        )


def _parameter_tables(state, table_ids: Iterable[int]) -> dict[str, list]:
    """The parameter columns of each registered table the statement reads, by its sql name. A
    native-read table kept for good has none to give: its landed rows are read whole, a condition
    on a parameter filtering them (``frozen_parameter_filters``, REQ-2031)."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.core.parameters import is_parameter

    ids = set(table_ids)
    types = getattr(state, "source_types", None) or {}
    return {
        apply_sql_name(t["table_name"]): [c for c in t["columns"] if is_parameter(c)]
        for t in (getattr(state, "tables", None) or [])
        if t.get("id") in ids and not (is_native_read(t, types) and _kept_for_good(state, t))
    }


def _missing_parameters(
    params_of: dict[str, list],
    tn: str,
    endpoint,
    nf_args: dict | None,
    ctx,
    feed: "_Feed | None" = None,
) -> list[str]:
    """The required parameters of ``tn`` the statement gives no value (REQ-1986). An OpenAPI
    parameter a model join feeds from its parent's keys is given by the join, and so is a
    parameter the relationship ``feed`` feeds from the parent the statement reads (REQ-2099)."""
    from provisa.api_source.fill_cache import joined_from
    from provisa.compiler.naming import apply_sql_name
    from provisa.core.parameters import NF_PREFIX, missing_parameters

    columns = params_of.get(apply_sql_name(tn))
    if not columns:
        return []
    fed = set() if feed is None else {apply_sql_name(p) for p in feed.parameters}
    return missing_parameters(
        columns,
        nf_args,
        lambda column: (
            (endpoint is not None and joined_from(endpoint, column, ctx) is not None)
            or apply_sql_name(column.removeprefix(NF_PREFIX)) in fed
        ),
    )


@dataclass(frozen=True)
class _Feed:
    """REQ-2099: a relationship that feeds a parameterized table's parameters from its source
    table, which the statement also reads: ``parameters`` maps each fed parameter to the source
    column whose value it is given, and ``source`` is the source table as the role sees it."""

    relationship: str
    parameters: dict[str, str]
    source: Any  # the source table's TableMeta in the statement's model
    # The target's column the relationship joins on (REQ-2099): a parameter column when the
    # target's rows do not carry the value they were read for, as a Qdrant search's points do not.
    target_column: str | None = None


def _feed_of(state, ctx, table: dict, table_ids: Iterable[int], given: dict | None) -> _Feed | None:
    """The relationship feeding ``table``'s parameters from a table the statement reads, or None.
    Only a table of the role's model feeds one: the source is read as the role reads it. A
    statement that gives one of the parameters the relationship feeds reads the table under its
    own arguments, and is not fed: a search for a phrase of its own, beside the lines it joins its
    points back to."""
    from provisa.compiler.naming import apply_sql_name

    if ctx is None:
        return None
    stated = {apply_sql_name(k.lstrip("_")) for k, v in (given or {}).items() if v is not None}
    ids = set(table_ids)
    metas = {m.table_id: m for m in ctx.tables.values()}
    for rel in getattr(state, "relationships", None) or []:
        if not rel["parameters"] or rel.get("target_table_id") != table["id"]:
            continue
        if stated & {apply_sql_name(p) for p in rel["parameters"]}:
            continue
        source = metas.get(rel["source_table_id"])
        if source is not None and source.table_id in ids:
            return _Feed(rel["id"], dict(rel["parameters"]), source, rel.get("target_column"))
    return None


def _carried(feed: _Feed, state) -> dict[str, tuple[str, str]]:
    """REQ-2099: the parameter column the relationship joins on, when it joins on one, as
    ``{column: (parameter, IR type)}``: each fed row carries the value its argument set gave that
    parameter, typed as the source column it was read from, so the join matches each row to the
    source row it was read for. Empty when the relationship joins on a column of the rows."""
    from provisa.core.parameters import NF_PREFIX

    column = feed.target_column
    if column is None or not column.startswith(NF_PREFIX):
        return {}
    parameter = column[len(NF_PREFIX) :]
    if parameter not in feed.parameters:
        raise ValueError(
            f"relationship {feed.relationship!r} joins on {column!r}, a parameter it does not feed"
        )
    source_column = feed.parameters[parameter]
    [source] = [t for t in state.tables if t["id"] == feed.source.table_id]
    [data_type] = [c["data_type"] for c in source["columns"] if c["column_name"] == source_column]
    return {column: (parameter, data_type)}


async def _fed_argument_sets(feed: _Feed, role_id: str) -> list[dict]:
    """The distinct argument sets ``feed`` gives: its source columns read through the one
    pipeline as ``role_id`` reads them (REQ-2099), so the role's row rules choose the rows and its
    masks the values the code is given."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.compiler.sql_rewrite import semantic_ref
    from provisa.core.request_context import current_session_vars
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    def _q(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    projection = ", ".join(
        f"{_q(apply_sql_name(column))} AS {_q(parameter)}"
        for parameter, column in feed.parameters.items()
    )
    sql = f"SELECT DISTINCT {projection} FROM {semantic_ref(feed.source)}"
    plan = await _govern_and_route(sql, role_id, session_vars=current_session_vars.get())
    result = await _execute_plan(plan)
    return [dict(zip(result.column_names, row, strict=True)) for row in result.rows]


async def _mat_fed_table(
    tn: str,
    table: dict,
    feed: _Feed,
    role_id: str,
    state,
    cache_rewrites: dict,
    values_cte_entries: dict,
    nf_args: dict | None,
) -> None:
    """A code table or a Qdrant search joined over a relationship that feeds its parameters
    (REQ-2099): called once per distinct argument set the source gives the role, with the
    statement's own arguments for the parameters the relationship does not feed, and landed
    together for the engine's join, kept under those argument sets for the table's TTL. A search's
    rows carry the value of the parameter the relationship joins on (:func:`_carried`)."""
    from provisa.qdrant.table import is_search

    given = dict(nf_args or {})
    sets = [{**given, **fed} for fed in await _fed_argument_sets(feed, role_id)]
    key = {"fed_by": feed.relationship, "sets": json.dumps(sets, sort_keys=True, default=str)}
    if is_search(table, state.source_types):
        from provisa.qdrant.table import read_each as search_each

        carried = _carried(feed, state)

        def answer():
            return search_each(state, table, sets, carried)
    else:
        from provisa.codesource.proxy import CodeProxy

        def answer():
            # REQ-2099: through the code source's proxy, once per set of the parent's values.
            return CodeProxy().call_each(state, table, sets, joined=True)

    await _land_per_statement(tn, table, state, cache_rewrites, values_cte_entries, key, answer)


def _refuse_or_drop(exec_sql: str, tn: str, missing: list[str], dropped_tables: dict) -> None:
    """A read of ``tn`` lacking ``missing``: refused, naming the table and the parameters --
    unless ``tn`` is only a branch of a UNION (a multi-label sweep), whose branch is dropped."""
    from provisa.compiler.nf_extractor import (
        drop_union_branches_for_table,
        find_api_table_names,
    )
    from provisa.core.parameters import ParameterRequired, missing_reason

    reason = missing_reason(missing)
    if tn in find_api_table_names(drop_union_branches_for_table(exec_sql, tn)):
        raise ParameterRequired(tn, reason)
    log.warning("[MAT] %s lacks required parameter(s) %s — dropping its union branch", tn, missing)
    dropped_tables[tn] = reason


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

    # REQ-2061: the table's fixed arguments are sent with every read; a read gives no value for one.
    variables = {**(gql_tbl.get("arguments") or {}), **(variables or {})}
    answer = await execute_remote(
        url=gql_reg["url"],
        auth=gql_reg.get("auth"),
        field_name=gql_tbl.get("field_name") or gql_tbl["name"],
        columns=col_selections,
        variables=variables or None,
        required_args=remote_arguments(gql_tbl) or None,
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
    # The columns the relation holds: a field pointing at one object of a registered type is a
    # relationship, not data (graphql_remote.relation_columns -- the one statement of that rule).
    from provisa.graphql_remote.relation_columns import governed_types, reference_columns

    _governed_excluded = reference_columns(
        {"columns": col_dicts}, governed_types(getattr(state, "graphql_remote_sources", {}))
    )
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
    against tenant_db instead of the engine ("no such table"). The same PG-cache-table +
    VALUES-CTE mechanism serves every surface, compiled GraphQL included, through the pipeline's
    API stage.
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
    # REQ-2003: the registration's TTL, else its source's, else the default -- the first one set.
    ttl = landing_ttl(
        {"table_name": tn, "source_id": source_id, "cache_ttl": reg.get("cache_ttl")}, state
    )

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
        ttl=ttl,
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
                ttl,
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


async def _mat_fetch_rows_from_fills(
    ep, col_names: list, _META_COLS: set, state, args: dict | None = None
) -> list[dict]:
    """The rows this API table's fills hold in the store (``api_source.fill_cache``): every
    argument set's, or only the answer to ``args`` when one is given -- ``[]`` when no fill
    has made its table yet (a miss the caller fills live). A failed read raises: it is never a
    cue to fetch from REST instead (REQ-1661, amended 2026-09-30)."""
    from provisa.api_source import fill_cache

    table = fill_cache.fill_table(state, ep, getattr(state, "api_sources", {}).get(ep.source_id))
    with state.federation_engine.isolated_sync() as conn:
        raw = fill_cache.read_rows(conn, table, args)
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
    ctx=None,
    pushed: dict | None = None,
) -> None:
    """Materialize a REST API endpoint-backed table into the engine cache or VALUES CTE.

    The cache table is kept under the arguments the statement gives the endpoint (REQ-318): an
    endpoint with a parameter is a function of its arguments, so the answer to one argument set
    is never another's. ``ctx`` is the model the statement compiled against, for the joins that
    feed a parameter from a parent's keys. ``pushed``: the arguments the statement's own
    conditions narrow the call by (a SPARQL table's narrowed query, REQ-2138), part of the call
    and of the name its answer is kept under."""
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
    from provisa.api_source.fill_cache import (
        endpoint_args,
        read_by_parent_keys,
        source_cache_location,
    )

    _cache_loc = source_cache_location(state, source_id, api_source)
    args = {**endpoint_args(ep, nf_args), **(pushed or {})}
    # Read by its parent's keys, the fills are one argument set per key and the table is all of
    # them, whatever else the statement gives. Read by argument, the table is that answer's:
    # named as the fetch names it, with the endpoint's default parameters under the arguments.
    by_keys = read_by_parent_keys(ep, args, ctx)
    cache_tbl = cache_table_name(source_id, tn, {} if by_keys else {**ep.default_params, **args})
    # The first one set: a TTL of 0 is set, and keeps nothing (REQ-1986).
    ttl = next(
        t
        for t in (
            getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl"),
            getattr(state, "response_cache_default_ttl", None),
            ep.ttl,
        )
        if t is not None
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
    whole = not any(c.param_type for c in ep.columns) and not pushed

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
    rows = await _mat_fetch_rows_from_fills(
        ep, col_names, _META_COLS, state, None if by_keys else args
    )
    fetched = not rows  # the rows are a live fetch's, not the fills'

    if not rows:
        missing = [
            c.name
            for c in ep.columns
            if c.param_type == "path" and (c.param_name or c.name) not in args
        ]
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
            # No argument when the table is all its parent-keyed fills: its name carries none.
            params={} if by_keys else args,
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
    if fetched and ep.promotions and _cache_loc.backend != "iceberg":
        # REQ-119: JSON fields promoted to generated columns of the (PG-backed) cache table a
        # live fetch was stored in, as the fetch itself did when it wrote the table. The table
        # holds its columns under their SQL names. Iceberg has no generated columns.
        from provisa.api_source.router_integration import _apply_cache_promotions

        promoted = ep.model_copy(
            update={
                "promotions": [
                    p.model_copy(update={"jsonb_column": apply_sql_name(p.jsonb_column)})
                    for p in ep.promotions
                ]
            }
        )
        await _apply_cache_promotions(_cache_loc, cache_tbl, promoted)


def _cut_in_statement(table_name: str) -> bool:
    """Whether this statement already warned that ``table_name``'s answer was cut."""
    from provisa.core.statement_warnings import raised

    return any(w.code == "api.answer_cut" and w.params.get("table") == table_name for w in raised())


def would_materialize_optimize(exec_sql: str, state, *, table_ids: Iterable[int]) -> bool:
    """REQ-1877 routing addendum: cheap, no-I/O predictor of whether
    `_materialize_api_to_engine_cache(exec_sql, state, ...)` would do ANYTHING for this call —
    i.e. whether its LIVE/time-varying branches (hot-table inline, TTL-cached API-endpoint fetch,
    graphql_remote/grpc_remote remote fetch) are reachable at all.

    Mirrors that function's own control flow table name-by-name, using the exact same lookups
    (`find_api_table_names`, `_statement_endpoints`, `_lookup_gql_remote_table`,
    `_lookup_grpc_remote_table`) — all in-memory dict lookups, never I/O. An OpenAPI table is an
    API endpoint (`_statement_endpoints`), like every other. Returns True the moment any table COULD
    reach a live branch; a caller must treat True as "cannot prove this call is a no-op" and take
    the full, uncached path.

    IMPORTANT: `find_api_table_names` returns every table name in the query's FROM/JOIN clauses,
    not only API-backed ones — an ordinary multi-table SQL query is NOT "no candidates" just
    because it has tables; it only returns False here once every one of those tables is checked
    and none is hot or registered as an API/graphql_remote/grpc_remote table (a
    row_materialize table is skipped). A table registered as a (non-hot) API endpoint is ALWAYS
    live here: its TTL cache can go stale between calls.
    """
    from provisa.compiler.nf_extractor import find_api_table_names, read_base

    # A read named for itself (nf_extractor.separate_parameter_reads) is its table's.
    table_names = [read_base(tn) for tn in find_api_table_names(exec_sql)]
    if not table_names:
        return False
    hot = _StatementHot(getattr(state, "hot_manager", None), state, table_ids)
    endpoints = _statement_endpoints(state, table_ids)
    row_materialize_table_names = {
        t.get("table_name")
        for t in (getattr(state, "tables", None) or [])
        if t.get("row_materialize")
    }
    pipelines = _pipeline_tables(state, table_ids)
    for tn in table_names:
        if tn in row_materialize_table_names:
            continue
        if _sql_name(tn) in pipelines:  # REQ-1980: run on every read, kept for its TTL
            return True
        if tn in hot.entries:
            return True
        if tn in endpoints:
            return True
        gql_reg, _gql_tbl = _lookup_gql_remote_table(state, tn)
        if gql_reg is not None:
            return True
        _grpc_source_id, grpc_reg, _grpc_query = _lookup_grpc_remote_table(state, tn)
        if grpc_reg is not None:
            return True
    return False


async def _materialize_api_to_engine_cache(
    exec_sql: str,
    state,
    gql_remote_extra_selections: dict | None = None,
    nf_args: dict | None = None,
    *,
    table_ids: Iterable[int],
    ctx=None,
    read_args: dict | None = None,
    role_id: str | None = None,
) -> tuple[dict, dict, dict[str, str]]:
    """Materialize API-backed tables into the engine cache (VARCHAR columns) before the engine SQL runs.

    Avoids INVALID_CAST_ARGUMENT: the engine's PG connector exposes JSONB as json type;
    cache tables store all columns as VARCHAR/scalar types instead.

    Reads the fills the API stage keeps in the store (a compiled GraphQL read fills them first,
    ``hydration._hydrate_api_tables_before_engine``), and fetches over REST on a miss.
    Returns (cache_rewrites, values_cte_entries, dropped_tables):
      cache_rewrites: {physical_table_name: (CacheLocation, cache_tbl)}
      values_cte_entries: {physical_table_name: rows} — inlined as VALUES CTEs

    ``table_ids`` are the registered tables the statement reads, as the pipeline resolved them:
    the hot rows substituted are theirs, and rows fetched here are held under them.

    ``role_id``: the role the statement is governed for, as which a relationship that feeds a
    code table's parameters reads its source (REQ-2099).

    ``read_args``: each read the statement names for itself (nf_extractor.separate_parameter_reads
    -- one table read under several sets of arguments), as (the table it reads, its arguments).
    Such a read is answered under its own arguments and put in for itself alone.
      dropped_tables: {physical_table_name: reason} whose UNION branches should be dropped -- a
        table the query cannot address because it gives none of the table's required filter /
        path parameters. A table with no UNION to drop from survives the branch-drop, and
        ``nf_extractor.apply_dropped_tables`` raises the reason for it. A remote that fails is
        never dropped: its error fails the query (REQ-1661, amended 2026-09-30).
    """
    from provisa.codesource.table import is_code_table
    from provisa.compiler.nf_extractor import find_api_table_names
    from provisa.qdrant.table import is_search
    from provisa.sparql import pushdown as sparql_pushdown

    cache_rewrites: dict = {}
    values_cte_entries: dict = {}
    dropped_tables: dict[str, str] = {}
    hot_mgr = getattr(state, "hot_manager", None)
    table_names = find_api_table_names(exec_sql)
    if not table_names:
        return cache_rewrites, values_cte_entries, dropped_tables
    hot = _StatementHot(hot_mgr, state, table_ids)
    endpoints = _statement_endpoints(state, table_ids)

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

    params_of = _parameter_tables(state, table_ids)
    pipelines = _pipeline_tables(state, table_ids)
    statement_args = nf_args
    for tn in table_names:
        # A read named for itself is its table's, under its own arguments; its rows are put in
        # under its own name, so the statement's other reads of the table keep theirs.
        table, nf_args = (read_args or {}).get(tn, (tn, statement_args))
        pipeline_table = pipelines.get(_sql_name(table))
        # REQ-2099: a code table or a Qdrant search the statement joins to a table whose
        # relationship feeds its parameters is given them by the join.
        feed = (
            _feed_of(state, ctx, pipeline_table, table_ids, nf_args)
            if pipeline_table is not None
            and (
                is_code_table(pipeline_table, state.source_types)
                or is_search(pipeline_table, state.source_types)
            )
            else None
        )
        # REQ-1986: a table read without a value for a required parameter is refused before
        # anything is sent to its source -- every source kind, one rule.
        missing = _missing_parameters(params_of, table, endpoints.get(table), nf_args, ctx, feed)
        if missing:
            _refuse_or_drop(exec_sql, tn, missing, dropped_tables)
            continue
        if table in _row_materialize_table_names:
            continue
        # REQ-1980: a MongoDB pipeline table is run by the native reader on every engine; no
        # engine connector runs a pipeline.
        if feed is not None:
            assert pipeline_table is not None and role_id is not None
            await _mat_fed_table(
                tn,
                pipeline_table,
                feed,
                role_id,
                state,
                cache_rewrites,
                values_cte_entries,
                nf_args,
            )
            continue
        if pipeline_table is not None:
            # REQ-2100 (amended 2026-10-23): a code table's rows are landed, its arguments a
            # pre-filter, and the engine runs the statement over them.
            await _mat_pipeline_table(
                tn, pipeline_table, state, cache_rewrites, values_cte_entries, nf_args, exec_sql
            )
            continue
        # Hot cache: inline rows as VALUES CTE — avoids cross-catalog JOIN entirely
        entry = hot.entries.get(table)
        if entry is not None:
            values_cte_entries[tn] = entry
            log.warning("[MAT] hot VALUES CTE for %s (%d rows inline)", tn, len(entry.rows))
            continue

        ep = endpoints.get(table)
        if ep is None:
            gql_reg, gql_tbl = _lookup_gql_remote_table(state, table)
            if gql_reg is not None:
                assert gql_tbl is not None
                assert isinstance(gql_tbl, dict)
            if gql_reg is not None and gql_tbl is not None:
                # The arguments carry the REMOTE name (e.g. ``name``, ``breedName``); the
                # extracted nf_args are keyed by the GraphQL schema arg, which Provisa prefixes
                # with ``_`` when it collides with a scalar field and stores in sql convention
                # (``breedName`` → ``_breed_name``). Match through the naming authority: both
                # sides reduce to the same sql name once the disambiguation ``_`` is dropped.
                # A required one left without a value was refused above (REQ-1986).
                from provisa.compiler.naming import apply_sql_name as _apply_sql_name

                _nf_canon = {_apply_sql_name(k.lstrip("_")): v for k, v in (nf_args or {}).items()}
                # Every nf_arg the query pushed down for this table must distinguish the
                # materialized cache entry — an argument resolves to its REMOTE name (so it is
                # also forwarded to the remote fetch); any other filter still keyed here under
                # its canonical name so two queries that differ only by that filter never
                # collide on the same cache_table_name (REQ-848/REQ-941).
                resolved = dict(_nf_canon)
                for a in remote_arguments(gql_tbl):
                    canon = _apply_sql_name(a["name"].lstrip("_"))
                    if canon in _nf_canon:
                        resolved[a["name"]] = resolved.pop(canon)
                # REQ-1661 (amended 2026-09-30): an unreachable remote fails the whole query --
                # its UNION branch is never dropped to return the rest as complete.
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

            grpc_source_id, grpc_reg, grpc_query = _lookup_grpc_remote_table(state, table)
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
            ctx=ctx,
            pushed=sparql_pushdown.pushed_arguments(ep, exec_sql, tn),
        )
        if tn not in cache_rewrites and tn not in values_cte_entries:
            log.warning("[MAT] %s could not be materialized — dropping union branch", tn)
            dropped_tables[tn] = "could not be materialized"

    return cache_rewrites, values_cte_entries, dropped_tables
