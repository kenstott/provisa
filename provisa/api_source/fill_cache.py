# Copyright (c) 2026 Kenneth Stott
# Canary: 436eb632-52b7-49d8-8b0b-2f6a6a57dcad
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Parameter-set fills: the answers an API table gave to particular calls (REQ-318, REQ-1915).

A request that reads an API table through a join or with arguments calls the remote for the
argument sets it needs — one collection call, one call for a batch of parent keys, one call per
parent key — and keeps each answer for the endpoint's TTL. Those answers are a CACHE of calls,
not a replica of a table: they are kept in the store's API cache schema
(``org_<id>[_env_<env>]_api_cache``), in one table per API table, named for it, whose rows carry
the hash of the arguments that produced them and when they were fetched.

Nothing fetched from a remote is written into the control plane. The table is written and read
through the connection the API cache uses on every engine (``EngineRuntime.isolated_sync``), and
the remote is called the one way every path calls it (``caller.call_api``: the source's auth,
the endpoint's paging, retries).
"""

# Requirements: REQ-318, REQ-544, REQ-859, REQ-1661, REQ-1915

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

import sqlglot.expressions as exp

from provisa.api_source.caller import (
    AnswerCut,
    ApiCallError,
    ApiNotFoundError,
    answer_cut_warning,
    answer_rows,
    call_api,
)
from provisa.api_source import engine_cache
from provisa.api_source.engine_cache import (
    CacheLocation,
    _string_literal,
    _table_ref,
    create_and_insert,
    ensure_cache_schema,
    org_cache_schema,
)
from provisa.api_source.models import ApiEndpoint
from provisa.core.statement_warnings import warn

log = logging.getLogger(__name__)

PARAMS_HASH = "_params_hash"
CACHED_AT = "_cached_at"
META_COLUMNS = (PARAMS_HASH, CACHED_AT)

#: Hash groups this process knows are fresh: (schema, table, hash) -> monotonic expiry. A hit
#: costs no statement. Pruned as it is written, under the lock, so a prune never iterates a
#: dict another thread is writing.
_mem_fresh: dict[tuple[str, str, str], float] = {}
_mem_fresh_lock = threading.Lock()

#: The column shape each fill table was last made with in this process: (catalog, schema,
#: table) -> digest. A table made for another shape is dropped and made again.
_shapes: dict[tuple[str, str, str], str] = {}
_shapes_lock = threading.Lock()


@dataclass(frozen=True)
class _Column:
    """A column of the fill table, as ``engine_cache.create_and_insert`` reads one."""

    name: str
    type: str


@dataclass(frozen=True)
class FillTable:
    """Where one API table's fills are kept, and with which columns."""

    loc: CacheLocation
    name: str
    columns: tuple[_Column, ...]  # the endpoint's response columns, then the two of the fill

    @property
    def data_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.name not in META_COLUMNS]

    @property
    def shape(self) -> str:
        described = [[c.name, c.type] for c in self.columns]
        return hashlib.sha256(json.dumps(described).encode()).hexdigest()[:16]


def params_hash(params: dict) -> str:
    """The hash group of one argument set."""
    return hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:16]


def source_cache_location(state: Any, source_id: str, api_source: Any) -> CacheLocation:
    """Where the acting org's API cache for ``source_id`` is, in the bound engine's terms: the
    source's own cache catalog when it names one, else the engine's own cache catalog (a native
    engine's attached store), else the catalog the engine reads the source through (REQ-1730), in
    the org's API cache schema (REQ-1623)."""
    catalog = getattr(api_source, "cache_catalog", None) if api_source else None
    default_schema = org_cache_schema(state)
    schema = getattr(api_source, "cache_schema", default_schema) if api_source else default_schema
    # Through the module, as router_integration resolves it: one binding of the location rule.
    return engine_cache.cache_location(
        source_id,
        catalog,
        schema,
        engine=state.federation_engine,
        source_catalog=getattr(state, "source_catalogs", {}).get(source_id),
    )


def fill_table(state: Any, endpoint: ApiEndpoint, api_source: Any) -> FillTable:
    """The fill table of ``endpoint``'s API table: named for the source and the table, in the
    org's API cache schema."""
    from provisa.federation.replica_address import replica_table_name

    columns = [
        _Column(c.name, c.type.value if hasattr(c.type, "value") else str(c.type))
        for c in endpoint.columns
        if not c.param_only
    ]
    columns += [_Column(PARAMS_HASH, "string"), _Column(CACHED_AT, "number")]
    return FillTable(
        loc=source_cache_location(state, endpoint.source_id, api_source),
        # The joined name, cut to the store's identifier limit with a digest when it is long.
        name=replica_table_name(endpoint.source_id, "fills", endpoint.table_name),
        columns=tuple(columns),
    )


# -- freshness -------------------------------------------------------------------------------------


def _mark_fresh(table: FillTable, phash: str, ttl: int) -> None:
    now = time.monotonic()
    with _mem_fresh_lock:
        _mem_fresh[(table.loc.schema, table.name, phash)] = now + ttl
        for key in [k for k, expiry in _mem_fresh.items() if expiry <= now]:
            del _mem_fresh[key]


def is_mem_fresh(table: FillTable, params: dict) -> bool:  # REQ-544
    """Whether this process knows the fill of ``params`` is within its TTL — no statement."""
    key = (table.loc.schema, table.name, params_hash(params))
    return _mem_fresh.get(key, 0) > time.monotonic()


def _quoted(name: str, dialect: str) -> str:
    """``name`` as a quoted identifier of ``dialect``: a column name is a response key, data."""
    return exp.to_identifier(name, quoted=True).sql(dialect=dialect)


def _ref(conn: Any, table: FillTable) -> str:
    return _table_ref(table.loc, table.name, conn.dialect)


def _exists(conn: Any, table: FillTable) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {_ref(conn, table)} LIMIT 1")
        conn.fetchall()
    except Exception as exc:  # allow-ble: an absent table is the engine's own error class, which differs per engine; the reason is logged, and a table that exists and cannot be read fails at the next statement
        log.debug("[API FILLS] %s.%s is not there: %s", table.loc.schema, table.name, exc)
        return False
    return True


def stale_hashes(conn: Any, table: FillTable, hashes: list[str], ttl: int) -> list[str]:
    """Which of ``hashes`` have no fill within ``ttl`` seconds, in the order given. Decided in
    memory where this process knows; one statement covers the rest. The TTL decision is the
    shared freshness module's (REQ-859)."""
    from provisa.freshness import Ttl, evaluate
    from provisa.freshness.adapters import StateSubject

    now_mono = time.monotonic()
    unknown = [
        h for h in hashes if _mem_fresh.get((table.loc.schema, table.name, h), 0) <= now_mono
    ]
    if not unknown or not _exists(conn, table):
        return unknown
    listed = ", ".join(_string_literal(h, conn.dialect) for h in unknown)
    group, at = _quoted(PARAMS_HASH, conn.dialect), _quoted(CACHED_AT, conn.dialect)
    conn.execute(
        f"SELECT {group}, MAX({at}) FROM {_ref(conn, table)} "
        f"WHERE {group} IN ({listed}) GROUP BY {group}"
    )
    cached = {row[0]: row[1] for row in conn.fetchall()}
    now = time.time()
    stale = []
    for h in unknown:
        at = cached.get(h)
        if (
            at is not None
            and evaluate(StateSubject(refreshed_at=float(at)), Ttl(ttl), now).is_fresh
        ):
            _mark_fresh(table, h, ttl)
        else:
            stale.append(h)
    return stale


# -- the table -------------------------------------------------------------------------------------


def _has_columns(conn: Any, table: FillTable) -> bool:
    names = ", ".join(_quoted(c.name, conn.dialect) for c in table.columns)
    try:
        conn.execute(f"SELECT {names} FROM {_ref(conn, table)} LIMIT 1")
        conn.fetchall()
    except Exception as exc:  # allow-ble: a missing column is the engine's own error class, which differs per engine; the table is then one made for another definition and is replaced, with the reason logged
        log.info(
            "[API FILLS] %s.%s was made for another definition (%s); replacing it",
            table.loc.schema,
            table.name,
            exc,
        )
        return False
    return True


def _ensure(conn: Any, table: FillTable) -> None:
    """The fill table, with this endpoint's columns. One made for another column shape (the
    endpoint's definition changed) is dropped and made again: its rows were answers to the old
    definition."""
    key = (table.loc.catalog, table.loc.schema, table.name)
    if _shapes.get(key) == table.shape:
        return
    with _shapes_lock:
        made_as = _shapes.get(key)
        if made_as == table.shape:
            return
        ensure_cache_schema(conn, table.loc)
        # Another shape: this process made it for one, or a table from before this process
        # started lacks a column this definition has.
        if made_as is not None or (_exists(conn, table) and not _has_columns(conn, table)):
            conn.execute(f"DROP TABLE IF EXISTS {_ref(conn, table)}")
            conn.fetchall()
        create_and_insert(conn, table.loc, table.name, [], list(table.columns))
        _shapes[key] = table.shape


def store(
    conn: Any,
    table: FillTable,
    fills: dict[str, list[dict]],
    ttl: int,
    cut: frozenset[str] = frozenset(),
) -> int:
    """Replace the rows of each hash group in ``fills`` with the rows fetched for it. A group
    fetched with no rows is emptied: the remote's answer for those arguments is now nothing.

    A group in ``cut`` was answered short (the call stopped at the endpoint's max_pages): its
    rows serve the statement that fetched them, but carry no fetch time, so the group is never
    fresh and the next request calls again — a cut answer is never cached as complete."""
    if not fills:
        return 0
    _ensure(conn, table)
    listed = ", ".join(_string_literal(h, conn.dialect) for h in fills)
    conn.execute(
        f"DELETE FROM {_ref(conn, table)} WHERE {_quoted(PARAMS_HASH, conn.dialect)} IN ({listed})"
    )
    conn.fetchall()
    now = time.time()
    rows = [
        {**row, PARAMS_HASH: phash, CACHED_AT: None if phash in cut else now}
        for phash, group in fills.items()
        for row in group
    ]
    if rows:
        create_and_insert(conn, table.loc, table.name, rows, list(table.columns))
    for phash in fills:
        if phash not in cut:
            _mark_fresh(table, phash, ttl)
    log.info(
        "[API FILLS] %d rows of %d argument set(s) → %s.%s",
        len(rows),
        len(fills),
        table.loc.schema,
        table.name,
    )
    return len(rows)


def read_rows(conn: Any, table: FillTable) -> list[dict]:
    """Every row the table holds, as the endpoint's response columns — ``[]`` when no fill has
    made the table yet. A read that fails raises: it is never a cue to call the remote."""
    if not _exists(conn, table):
        return []
    names = table.data_columns
    select = ", ".join(_quoted(n, conn.dialect) for n in names)
    conn.execute(f"SELECT {select} FROM {_ref(conn, table)}")
    return [dict(zip(names, row)) for row in conn.fetchall()]


def distinct_values(conn: Any, table: FillTable, column: str) -> list[Any]:
    """The distinct non-null values of one column of the fills: the parent keys a dependent
    table is fetched for."""
    if column not in table.data_columns:
        raise KeyError(f"{column!r} is not a response column of {table.name}")
    if not _exists(conn, table):
        return []
    name = _quoted(column, conn.dialect)
    conn.execute(f"SELECT DISTINCT {name} FROM {_ref(conn, table)} WHERE {name} IS NOT NULL")
    return [row[0] for row in conn.fetchall()]


# -- the remote ------------------------------------------------------------------------------------


def _reported_error(page: Any, error_path: str | None) -> str | None:
    """What the answer reports at the endpoint's ``error_path``, when it reports anything."""
    if not error_path or not isinstance(page, dict):
        return None
    value: Any = page
    for key in error_path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return str(value) if value else None


async def fetch(
    endpoint: ApiEndpoint, api_source: Any, params: dict
) -> tuple[list[dict], AnswerCut | None]:
    """The rows the remote answers ``params`` with, through the one remote call every path
    uses, and how the answer was cut (None when it is whole). A 404 is an answer with no rows.
    An answer that reports an error at the endpoint's ``error_path``, or a call that fails,
    fails the request (REQ-1661): it is never logged and answered with whatever the cache held."""
    try:
        answer = await call_api(
            endpoint, params, base_url=api_source.base_url, auth=api_source.auth
        )
    except ApiNotFoundError:
        return [], None
    for page in answer.pages:
        reported = _reported_error(page, endpoint.error_path)
        if reported:
            raise ApiCallError(
                f"the API answered {endpoint.table_name!r} with an error at "
                f"{endpoint.error_path!r}: {reported}"
            )
    return answer_rows(endpoint, answer)


async def fill(
    state: Any, endpoint: ApiEndpoint, api_source: Any, param_sets: list[dict], ttl: int
) -> int:
    """Fetch and keep the answer to each of ``param_sets`` that has no fill within ``ttl``.
    Returns the rows written."""
    if not param_sets:
        return 0
    table = fill_table(state, endpoint, api_source)
    by_hash = {params_hash(p): p for p in param_sets}
    engine = state.federation_engine
    with engine.isolated_sync() as conn:
        stale = stale_hashes(conn, table, list(by_hash), ttl)
    if not stale:
        return 0
    fetched: dict[str, list[dict]] = {}
    cut: set[str] = set()
    for phash in stale:
        rows, short = await fetch(endpoint, api_source, by_hash[phash])
        fetched[phash] = rows
        if short is not None:
            cut.add(phash)
            # The statement answered from it says so (REQ-1350).
            warn(answer_cut_warning(endpoint.table_name, short))
    with engine.isolated_sync() as conn:
        return store(conn, table, fetched, ttl, frozenset(cut))
