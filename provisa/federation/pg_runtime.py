# Copyright (c) 2026 Kenneth Stott
# Canary: 54db22a2-65d6-47e1-a189-699b04b38b5b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PgFederationRuntime — the PostgreSQL engine's in-process federation runtime (REQ-904).

A single PostgreSQL connection is the engine. Each registered source is ATTACHed in place through the
(postgres, source_type) connector's SQL/MED DDL — postgres_fdw / sqlite_fdw import a foreign schema,
file_fdw defines a per-table foreign table — and then wrapped in a physical-named view so the compiled
``schema.table`` reference resolves unchanged. Non-attachable sources LAND into the materialization
store; for the pg engine the store is a PostgreSQL, so its cache/landed tables live in a schema the
engine reads directly. Conforms to the NativeEngineBackend runtime protocol: connection, run/run_sync,
attach_source, ensure_materialize_attached.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any

import psycopg2
import psycopg2.pool

from provisa.core import request_deadline
from provisa.federation.land_guard import LandGuard
from provisa.executor.result import QueryResult, ResultStream, StreamingQueryResult
from provisa.federation.engine import build_pg_engine
from provisa.federation.runtime_support import _STREAM_BATCH_ROWS

_log = logging.getLogger(__name__)

# REQ-1895: run_sync/run_arrow/run_arrow_stream read-connection pool bounds — a fresh connection
# per call (TCP + auth handshake, no server-side generic-plan reuse across calls) was measured as
# the dominant per-query cost on the pg engine (perf-bench --engine pg). min=1 keeps one warm
# connection idle-ready; max=10 is a sane fixed ceiling — no existing per-runtime tunable for this
# in the module to inherit from (store_writer.py's create_engine_from_url(pool_size=1) is the
# closest existing convention: a small fixed pool per store connection, not a config knob).
_POOL_MINCONN = 1
_POOL_MAXCONN = 10
# REQ-1882 (amended 2026-09-29): with every request on its own thread, more than _POOL_MAXCONN
# requests in one worker can want an engine connection at once. The maintainer's rule: the extra
# request WAITS for a free connection, it never fails with "pool exhausted". The wait is bounded by
# the pgwire request budget (server.py's .result(timeout=120) contract) so a leaked connection
# surfaces as a clear error instead of an indefinite hang.
_POOL_WAIT_S = 120.0


class _WaitingThreadedPool(psycopg2.pool.ThreadedConnectionPool):
    """``ThreadedConnectionPool`` whose ``getconn`` blocks until a slot frees up.

    psycopg2's own pool raises ``PoolError("connection pool exhausted")`` the moment ``maxconn``
    connections are checked out. A bounded semaphore sized to ``maxconn`` gates checkout so the
    (maxconn+1)th caller waits instead; every ``putconn`` (including ``close=True`` discards)
    releases its slot."""

    def __init__(self, minconn: int, maxconn: int, *args: Any, **kwargs: Any) -> None:
        super().__init__(minconn, maxconn, *args, **kwargs)
        self._slots = threading.BoundedSemaphore(maxconn)

    def getconn(self, key: Any = None) -> Any:
        budget = request_deadline.remaining()
        wait = _POOL_WAIT_S if budget is None else min(_POOL_WAIT_S, budget)
        if not self._slots.acquire(timeout=wait):
            raise psycopg2.pool.PoolError(
                f"no engine connection freed within {wait:.1f}s (all {self.maxconn} checked out)"
            )
        try:
            return super().getconn(key)
        except BaseException:
            self._slots.release()
            raise

    def putconn(self, conn: Any = None, key: Any = None, close: bool = False) -> None:
        try:
            super().putconn(conn, key, close)
        finally:
            self._slots.release()


def _psycopg2_exec_args(sql: str, params: list | None) -> tuple[str, dict[str, Any] | None]:
    """Rewrite Postgres-native ``$1``/``$2`` positional placeholders to psycopg2's own
    ``%(pN)s`` paramstyle, with a matching ``{"p1": ..., "p2": ...}`` dict.

    The compiled pipeline (GraphQL/Cypher/Flight/gRPC — every compiled-surface transport shares
    one physical-SQL construction) emits real Postgres wire-protocol placeholders, since that is
    what pgwire/asyncpg both speak natively. This runtime's connection is psycopg2, whose DBAPI
    paramstyle is ``%s``/``%(name)s`` — it has no notion of ``$N`` at all, so hitting ``cur.
    execute(sql, params)`` with unconverted ``$1``/``$2`` text sends the literal characters
    ``$1``/``$2`` straight through with nothing bound, and Postgres rejects it with "there is no
    parameter $1" — confirmed live (GraphQL federated_join against the pg engine, the first
    compiled query whose params survive un-inlined all the way to this runtime; every other
    compiled query either carries no params or has them literal-substituted upstream). A named
    dict (not positional ``%s``) is used because ``%s`` consumes params strictly in occurrence
    order — a repeated ``$1`` reference would then need its value repeated in the tuple too,
    which the caller's ``params`` list (ordered by first appearance, one entry per placeholder
    NUMBER) does not guarantee; the dict form binds by number regardless of how many times or
    where each ``$N`` appears."""
    if not params:
        return sql, None
    import re

    # With params bound psycopg2 %-formats the whole statement, so a literal % (LIKE 'a%') must be
    # escaped before the placeholders are rewritten.
    converted = re.sub(r"\$(\d+)", lambda m: f"%(p{m.group(1)})s", sql.replace("%", "%%"))
    return converted, {f"p{i + 1}": v for i, v in enumerate(params)}


# pg_type OID -> type name for the built-in types (their OIDs are fixed across every Postgres);
# any other OID is looked up in pg_type once and cached.
_PG_TYPE_NAMES: dict[int, str] = {
    16: "bool",
    17: "bytea",
    18: "char",
    19: "name",
    20: "int8",
    21: "int2",
    23: "int4",
    25: "text",
    26: "oid",
    114: "json",
    700: "float4",
    701: "float8",
    1007: "INTEGER[]",
    1009: "VARCHAR[]",
    1015: "VARCHAR[]",
    1042: "bpchar",
    1043: "varchar",
    1082: "date",
    1083: "time",
    1114: "timestamp",
    1184: "timestamptz",
    1186: "interval",
    1266: "timetz",
    1700: "numeric",
    2950: "uuid",
    3802: "jsonb",
}
_PG_TYPE_NAMES_LOCK = threading.Lock()


def _pg_type_names(con: Any, oids: list[int]) -> list[str]:
    missing = [o for o in set(oids) if o not in _PG_TYPE_NAMES]
    if missing:
        cur = con.cursor()
        try:
            cur.execute("SELECT oid, typname FROM pg_type WHERE oid = ANY(%s)", (missing,))
            found = dict(cur.fetchall())
        finally:
            cur.close()
        unknown = [o for o in missing if o not in found]
        if unknown:
            raise RuntimeError(f"pg_type has no entry for result column type OID(s) {unknown}")
        with _PG_TYPE_NAMES_LOCK:
            _PG_TYPE_NAMES.update(found)
    return [_PG_TYPE_NAMES[o] for o in oids]


class _AdbcConnectionPool:
    """Minimal thread-safe bounded pool for ADBC connections (``run_arrow``/``run_arrow_stream``).

    Scoped per ``PgFederationRuntime`` instance — same reasoning as
    ``registered_tables_cache.py``'s per-instance cache: two unrelated runtime instances must never
    share pooled connections. ``psycopg2.pool`` only pools psycopg2 DBAPI connections; there is no
    equivalent built into ``adbc_driver_postgresql``, so this hand-rolled pool is used instead of
    adding a new pooling dependency (matches the "prefer what's already a dependency" constraint).

    No liveness probe on borrow: psycopg2's pool can cheaply read a connection's
    ``transaction_status`` before reuse (see ``run_sync``'s pool below); ADBC exposes no equivalent
    state check. A dead pooled connection therefore surfaces as a real ``execute()`` failure to the
    caller, who must call ``discard()`` (closes it, lets the pool create a fresh one on the next
    ``getconn()``) rather than this pool silently retrying or swallowing the error."""

    def __init__(self, dsn: str, *, minconn: int, maxconn: int) -> None:
        from adbc_driver_postgresql import dbapi as adbc_pg

        self._connect = lambda: adbc_pg.connect(dsn)
        self._maxconn = maxconn
        self._pool: list[Any] = [self._connect() for _ in range(minconn)]
        self._created = minconn
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

    def getconn(self) -> Any:
        budget = request_deadline.remaining()
        deadline = time.monotonic() + (
            _POOL_WAIT_S if budget is None else min(_POOL_WAIT_S, budget)
        )
        with self._lock:
            while True:
                if self._pool:
                    return self._pool.pop()
                if self._created < self._maxconn:
                    self._created += 1
                    return self._connect()
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._not_empty.wait(remaining):
                    raise RuntimeError(
                        f"no ADBC engine connection freed within {_POOL_WAIT_S:.0f}s "
                        f"(all {self._maxconn} checked out)"
                    )

    def putconn(self, con: Any) -> None:
        with self._lock:
            self._pool.append(con)
            self._not_empty.notify()

    def discard(self, con: Any) -> None:
        con.close()
        with self._lock:
            self._created -= 1
            self._not_empty.notify()

    def closeall(self) -> None:
        with self._lock:
            for con in self._pool:
                con.close()
            self._pool.clear()


class PgFederationRuntime:  # REQ-825, REQ-840, REQ-904
    # Class-level default so _get_adbc_pool's `self._adbc_pool is None` check is well-defined even
    # for an instance built via __new__ without __init__ running (the Arrow-transport unit tests
    # build one this way, on purpose, to avoid opening a real psycopg2 connection for a read path
    # that must never touch it — see tests/unit/test_pg_arrow_transport.py's _runtime() docstring).
    # __init__ still sets its own instance attribute below; this is only the pre-__init__ default.
    _adbc_pool: "_AdbcConnectionPool | None" = None

    def __init__(self, *, engine_dsn: str, materialize_dsn: str | None = None) -> None:
        self._engine = build_pg_engine()
        self._con = psycopg2.connect(engine_dsn)
        self._con.autocommit = True
        self._engine_dsn = engine_dsn
        # The materialization store for a Postgres engine is a Postgres — its own DB unless an external
        # store is configured. Landed/cached rows live in a schema this same connection reads.
        self._materialize_dsn = materialize_dsn
        self._raw_attached: set[str] = set()
        # REQ-1895: run_sync's read path borrows from this pool instead of opening a fresh
        # psycopg2 connection per call — scoped to THIS runtime instance (never a process-global
        # pool; see _AdbcConnectionPool's docstring for why).
        self._read_pool = _WaitingThreadedPool(_POOL_MINCONN, _POOL_MAXCONN, engine_dsn)
        self._raw_statements: dict[int, dict[str, Any]] = {}
        self._raw_statements_lock = threading.Lock()
        # run_arrow/run_arrow_stream's ADBC pool — created lazily on first use since
        # adbc_driver_postgresql is an optional dependency, matching the existing lazy import in
        # run_arrow/run_arrow_stream below.
        self._adbc_pool: _AdbcConnectionPool | None = None
        # One store connection, so one write on it at a time (two lands interleaving on it was a
        # confirmed regression). A lock serializes it, and each write runs on the thread that asked
        # for it — a read-triggered land stays on its request's thread (REQ-1882), where a
        # one-worker pool took it off.
        self._land_guard = LandGuard("Postgres store connection")

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any) -> None:
        """Expose an ATTACH source at its physical ``schema.table`` via the engine's connector DDL."""
        entry = self._engine.resolve(source)  # picks the (postgres, source_type) connector
        details = entry.details
        cur = self._con.cursor()
        if "attach_ddl" in details:  # postgres_fdw / sqlite_fdw — import a foreign schema
            remote = f'"{details["local_schema"]}"."{source.table_name}"'
            # A connector that can import ONE table (postgres_fdw) is attached per table: the
            # foreign tables outlive this process, and a replica build imports one on its own
            # (copy_replica), so importing the whole schema again fails on the first table already
            # there. The others import the source's schema once.
            per_table = "server_ddl_for_copy" in details
            attach_key = f"{source.id}\x00{source.table_name}" if per_table else source.id
            if attach_key not in self._raw_attached:
                if per_table:
                    self._ensure_foreign_table(cur, details, source.table_name)
                else:
                    for ddl in details["attach_ddl"]:
                        cur.execute(ddl)
                self._raw_attached.add(attach_key)
                # REQ-1900: `IMPORT FOREIGN SCHEMA` creates the foreign table with NO statistics —
                # postgres_fdw/clickhouse_fdw/mongodb_wrapper foreign tables are never touched by
                # autovacuum, so pg_statistic stays empty until something explicitly ANALYZEs them.
                # Confirmed live: a real 3-way federated join (up to 1M matching rows per side)
                # planned against the FDW's placeholder default estimate (`rows=1000`/`rows=1`, not
                # real data) chose a hash join sized for that tiny estimate, spilled to disk
                # repeatedly under the ACTUAL row count, and ran 13+ minutes for what should be a
                # few seconds — root-caused by comparing EXPLAIN VERBOSE's default estimate against
                # the query's own literal BETWEEN range. ANALYZE here, once per source at attach
                # time, gives the planner real cardinality/selectivity before any query ever runs
                # against it.
                #
                # This must stay INSIDE the `attach_key not in self._raw_attached` guard above (it
                # previously ran unconditionally on every call): confirmed live, an un-guarded
                # ANALYZE against a large ClickHouse-backed foreign table (order_events) took
                # >120s EVERY SINGLE CALL, not just the first — including calls made long after
                # the table's statistics were already current, turning a one-time cold-attach cost
                # into a permanent per-query tax and masking as an indefinite hang once REQ-1882's
                # shared-loop fix moved it off the main thread (it stopped blocking OTHER
                # concurrent requests, but this query's own call still paid the full re-ANALYZE
                # cost every time).
                cur.execute(f"ANALYZE {remote}")
        elif (
            "server_ddl" in details
        ):  # file_fdw (csv) — per-table foreign table from column metadata
            first_attach = source.id not in self._raw_attached
            if first_attach:
                for ddl in details["server_ddl"]:
                    cur.execute(ddl)
                self._raw_attached.add(source.id)
            cols = ", ".join(f'"{n}" {t}' for n, t in source.columns)
            ft = f'"{details["server"]}__{source.table_name}"'
            cur.execute(
                f"CREATE FOREIGN TABLE IF NOT EXISTS {ft} ({cols}) "
                f"SERVER {details['server']} {details['table_options']}"
            )
            remote = ft
            if first_attach:
                cur.execute(f"ANALYZE {remote}")  # REQ-1900: see the postgres_fdw branch's comment
        else:
            raise KeyError(f"pg connector for {source.type.value!r} has no attach/server DDL")
        # REQ-1730: the ENGINE route's own physical SQL, on this catalog-incapable engine, folds
        # catalog.schema into one schema segment (fold_catalog_into_schema, provisa/compiler/
        # sql_rewrite.py) — "{source_to_catalog(source_id)}_{schema_name}" — because two sources
        # whose registered tables share a bare schema_name (e.g. two elasticsearch-type sources
        # both reporting "default") would otherwise collide once the catalog segment vanished.
        # This view must live under that SAME folded name, not the source's bare native
        # schema_name, or the query asks for a schema this attach never created. Confirmed live:
        # federated_join under PROVISA_ENGINE=pg failed 100% of calls with `relation
        # "bench_postgresql_public.orders" does not exist` — the two sides had never been
        # reconciled to the same convention.
        from provisa.compiler.naming import source_to_catalog

        folded_schema = f"{source_to_catalog(source.id)}_{source.schema_name}"
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{folded_schema}"')
        # The live view and a replica of this table share this one name. A source that was
        # replicated and is now read live again has its replica TABLE here, and CREATE OR REPLACE
        # VIEW refuses a table — the stale replica would go on answering reads. It is the engine's
        # own copy, so it is dropped (as the ordinary table the catalog says it is) to make room.
        from provisa.federation.replica_guard import (
            ReplicaTargetError,
            pg_kind_name,
            pg_relation_kind,
        )

        found = pg_relation_kind(cur, folded_schema, source.table_name)
        if found is not None and found[1] == "r":
            _log.warning(
                'pg: "%s"."%s" is read live again; dropping the replica table that held its name',
                folded_schema,
                source.table_name,
            )
            cur.execute(f'DROP TABLE "{folded_schema}"."{source.table_name}"')
        elif found is not None and found[1] != "v":
            raise ReplicaTargetError(
                f'"{folded_schema}"."{source.table_name}"',
                pg_kind_name(found[1]),
                "create the live view at",
            )
        cur.execute(
            f'CREATE OR REPLACE VIEW "{folded_schema}"."{source.table_name}" '
            f"AS SELECT * FROM {remote}"
        )

    # -- landing (MATERIALIZE_ONLY sources — no live FDW reach) -----------------

    def _existing_columns(self, cur: Any, schema: str, table: str) -> list[str]:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]

    def _existing_pk(self, cur: Any, schema: str, table: str) -> list[str]:
        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            """,
            (f'"{schema}"."{table}"',),
        )
        return [r[0] for r in cur.fetchall()]

    def _create_table_ddl(
        self, schema: str, table: str, columns: list[tuple[str, str]], pk_columns: list[str]
    ) -> str:
        from provisa.core.ir_types import to_physical

        pk = set(pk_columns)
        col_defs = [f'"{name}" {to_physical(sql_type, "postgresql")}' for name, sql_type in columns]
        if pk:
            col_defs.append(f"PRIMARY KEY ({', '.join(f'"{c}"' for c in pk)})")
        return f'CREATE TABLE "{schema}"."{table}" ({", ".join(col_defs)})'

    async def attach_landed_source(
        self, source: Any, columns: list[tuple[str, str]], *, pk_columns: list[str] | None = None
    ) -> str:
        """``_reconcile_landed`` on the caller's own thread with the connection held, as
        ``land_table`` runs. ``NativeEngineBackend.reconcile_landed_tables`` awaits this for every
        native runtime; as a plain ``def`` it returned a ``str`` to that ``await``, which raised
        ``TypeError`` after the first table and ended the reconcile there."""
        return await self._land_guard.run(
            lambda: self._reconcile_landed(source, columns, pk_columns=pk_columns)
        )

    def _reconcile_landed(
        self, source: Any, columns: list[tuple[str, str]], *, pk_columns: list[str] | None = None
    ) -> str:
        """Eager reconcile (boot/registration, REQ-846/REQ-1651): converge the source's landed
        table to ``columns`` + ``pk_columns`` (DDL only, no rows — the refresh's job is
        ``land_table``) — the sync mirror of ``SqlAlchemyFederationRuntime.attach_landed_source``,
        raw psycopg2 SQL instead of SQLAlchemy Core since this runtime never uses SQLAlchemy.
        Returns ``created`` | ``kept`` | ``recreated`` (drift = column set/order or PK mismatch).

        The schema is ``{catalog}_{schema_name}``, not bare ``schema_name`` — MUST match
        ``PgBackend.landing_target``'s own fold exactly (its own docstring has the full reasoning:
        this engine's ``catalog_qualified=False`` means the compiler folds the catalog into the
        schema rather than stripping it, since two MATERIALIZED sources can otherwise share a
        native ``schema_name`` and collide once catalog qualification is gone). This method is
        reached independently of ``landing_target`` (``reconcile_landed_tables`` calls it directly
        off the registered-table row, not through the backend's ``landing_target`` seam), so it
        must recompute the SAME fold here rather than trust a value threaded through."""
        from provisa.compiler.naming import source_to_catalog

        schema = f"{source_to_catalog(source.id)}_{source.schema_name}"
        table = source.table_name
        want_cols = [name for name, _ in columns]
        want_pk = list(pk_columns or ())
        from provisa.federation.replica_guard import pg_relation_kind, require_pg_replica_table

        cur = self._con.cursor()
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        # REQ-826/REQ-1141: what stands at the replica's name decides what may be done to it. A
        # source that was read live left its live VIEW here (attach_source) — over a postgres_fdw
        # foreign table, so every write to it is a write into the source. The source is replicated
        # now: the view goes (dropped AS a view, on the catalog's word that it is one) and the
        # replica table takes the name. Any other non-table relation is refused, never written.
        found = pg_relation_kind(cur, schema, table)
        if found is not None and found[1] == "v":
            _log.warning(
                'pg: "%s"."%s" is replicated now; dropping the live view that held its name',
                schema,
                table,
            )
            cur.execute(f'DROP VIEW "{schema}"."{table}"')
        if not require_pg_replica_table(cur, schema, table, action="reconcile the replica at"):
            cur.execute(self._create_table_ddl(schema, table, columns, want_pk))
            return "created"
        have_cols = self._existing_columns(cur, schema, table)
        have_pk = self._existing_pk(cur, schema, table)
        if have_cols == want_cols and sorted(have_pk) == sorted(want_pk):
            return "kept"
        cur.execute(f'DROP TABLE "{schema}"."{table}"')
        cur.execute(self._create_table_ddl(schema, table, columns, want_pk))
        return "recreated"

    def _ensure_table(
        self,
        cur: Any,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> None:
        """Create the replica table when nothing stands at its name; proceed only when what stands
        there is an ordinary table. A view or a foreign table raises ``ReplicaTargetError`` — every
        write a caller issues after this would otherwise go through it into the source."""
        from provisa.federation.replica_guard import require_pg_replica_table

        if not require_pg_replica_table(cur, schema, table, action="write the replica"):
            cur.execute(self._create_table_ddl(schema, table, columns, pk_columns))

    async def land_table(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
        shape: str | None = None,
    ) -> str:
        """Land ``rows`` into ``schema.table`` of THIS engine's own store (REQ-1730) — create-if-
        absent only (drift is ``attach_landed_source``'s job), then REPLACE (delete+insert) or
        APPEND (insert, or upsert-by-key via Postgres's native ``ON CONFLICT`` when ``pk_columns``
        is given).

        Was a plain (non-``async``) ``def`` awaited unconditionally by
        ``NativeEngineBackend.land_source_table`` (``await runtime.land_table(...)``) — every call
        crashed with ``TypeError: object str can't be used in 'await' expression`` the moment a
        Postgres-store land fired (TTL background refresh or a query's own read-triggered stale
        materialize, REQ-1661). Now ``async``, on a PRIVATE cursor, on the caller's own thread with
        the connection held (``LandGuard``, REQ-1882)."""
        from provisa.core.change_signal import APPEND, CDC, REPLACE, select_landing_shape

        del match_floor
        pk = list(pk_columns or ())
        landing_shape = shape or select_landing_shape(change_signal, watermark_column)
        names = [name for name, _ in columns]

        def _run() -> str:
            cur = self._con.cursor()
            try:
                self._ensure_table(cur, schema, table, columns, pk)
                if landing_shape == REPLACE:
                    cur.execute(f'DELETE FROM "{schema}"."{table}"')
                    self._insert_rows(cur, schema, table, names, rows)
                elif landing_shape == APPEND:
                    if pk:
                        self._upsert_rows(cur, schema, table, names, pk, rows)
                    else:
                        self._insert_rows(cur, schema, table, names, rows)
                elif landing_shape == CDC:
                    if not pk:
                        raise ValueError(
                            f"CDC land into {schema}.{table} requires primary key columns"
                        )
                    self._upsert_rows(cur, schema, table, names, pk, rows)
                else:
                    raise ValueError(f"unhandled landing shape {landing_shape!r}")
                return f"{schema}.{table}"
            finally:
                cur.close()

        return await self._land_guard.run(_run)

    def _insert_rows(
        self, cur: Any, schema: str, table: str, names: list[str], rows: list[dict]
    ) -> None:
        if not rows:
            return
        cols_sql = ", ".join(f'"{n}"' for n in names)
        placeholders = ", ".join(["%s"] * len(names))
        values = [tuple(r.get(n) for n in names) for r in rows]
        cur.executemany(
            f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({placeholders})', values
        )

    def _upsert_rows(
        self,
        cur: Any,
        schema: str,
        table: str,
        names: list[str],
        pk: list[str],
        rows: list[dict],
    ) -> None:
        if not rows:
            return
        cols_sql = ", ".join(f'"{n}"' for n in names)
        placeholders = ", ".join(["%s"] * len(names))
        conflict_cols = ", ".join(f'"{c}"' for c in pk)
        update_cols = [n for n in names if n not in pk]
        set_sql = ", ".join(f'"{n}" = EXCLUDED."{n}"' for n in update_cols)
        do_update = f"DO UPDATE SET {set_sql}" if update_cols else "DO NOTHING"
        values = [tuple(r.get(n) for n in names) for r in rows]
        cur.executemany(
            f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({placeholders}) '
            f"ON CONFLICT ({conflict_cols}) {do_update}",
            values,
        )

    # -- engine-side replica build -----------------------------------------------

    # Advisory-lock class for replica builds; the object id is the replica's name, hashed.
    _REPLICA_LOCK_CLASS = 0x70726F76  # "prov"

    def replica_lock(self, schema: str, table: str) -> Any:
        """A dedicated connection holding the store's advisory lock for the replica
        ``schema.table`` — taken by one build at a time across every worker process that shares
        this store. Blocks (polling) until the lock is free; the caller closes the connection,
        which releases the lock, with :meth:`replica_unlock`."""
        con = psycopg2.connect(self._engine_dsn)
        con.autocommit = True
        try:
            cur = con.cursor()
            while True:
                cur.execute(
                    "SELECT pg_try_advisory_lock(%s, hashtext(%s))",
                    (self._REPLICA_LOCK_CLASS, f"{schema}.{table}"),
                )
                row = cur.fetchone()
                assert row is not None  # a scalar function: always exactly one row
                if row[0]:
                    return con
                time.sleep(0.25)
        except BaseException:
            con.close()
            raise

    @staticmethod
    def _ensure_foreign_table(cur: Any, details: dict, table_name: str) -> None:
        """The source's foreign server, and the foreign table for ``table_name`` in the
        connector's own schema — each created only when it is not there, so a live attach and a
        replica build (in any order, in any process, across restarts) converge on one object."""
        for ddl in details["server_ddl_for_copy"]:
            cur.execute(ddl)
        local = details["local_schema"]
        cur.execute("SELECT to_regclass(%s)", (f'"{local}"."{table_name}"',))
        row = cur.fetchone()
        assert row is not None  # a scalar function: always exactly one row
        if row[0] is None:
            cur.execute(
                f'IMPORT FOREIGN SCHEMA "{details["remote_schema"]}" '
                f'LIMIT TO ("{table_name}") FROM SERVER "{details["server"]}" '
                f'INTO "{local}"'
            )

    def replica_unlock(self, con: Any) -> None:
        con.close()  # a session lock: released with its session

    def copy_replica(
        self,
        con: Any,
        source: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> int:
        """Build the replica ``schema.table`` from the source's own table, inside the engine: one
        ``INSERT … SELECT`` from the source's postgres_fdw foreign table, in one transaction. No
        row passes through this process, so the build costs the worker nothing as the table
        grows; readers see the previous replica until the commit. ``con`` is the connection that
        holds the replica's lock. Returns the rows copied.

        The foreign table lives in the connector's own schema and is imported for this one table
        when it is not there. No live view is created: a replicated table is read from its
        replica."""
        details = self._engine.resolve(source).details
        local = details["local_schema"]
        names = ", ".join(f'"{name}"' for name, _ in columns)
        cur = con.cursor()
        self._ensure_foreign_table(cur, details, source.table_name)
        cur.execute("BEGIN")
        try:
            self._ensure_table(cur, schema, table, columns, pk_columns)
            cur.execute(f'DELETE FROM "{schema}"."{table}"')
            cur.execute(
                f'INSERT INTO "{schema}"."{table}" ({names}) '
                f'SELECT {names} FROM "{local}"."{source.table_name}"'
            )
            copied = cur.rowcount
            cur.execute("COMMIT")
        except BaseException:
            cur.execute("ROLLBACK")
            raise
        return copied

    async def apply_cdc_events(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        events: list,
    ) -> dict[str, int]:
        """Apply CDC change events (insert/update -> upsert by PK, delete -> tombstone) to a table
        already landed in this engine's own store (REQ-1733) — the raw-psycopg2 mirror of
        ``SqlAlchemyFederationRuntime.apply_cdc_events``.

        Runs on the caller's own thread with the connection held, as ``land_table`` does."""
        if not pk_columns:
            raise ValueError(f"CDC land into {schema}.{table} requires primary key columns")
        names = [name for name, _ in columns]

        def _run() -> dict[str, int]:
            cur = self._con.cursor()
            try:
                self._ensure_table(cur, schema, table, columns, pk_columns)
                counts = {"upsert": 0, "delete": 0}
                for ev in events:
                    if ev.operation.lower() == "delete":
                        where = " AND ".join(f'"{c}" = %s' for c in pk_columns)
                        cur.execute(
                            f'DELETE FROM "{schema}"."{table}" WHERE {where}',
                            tuple(ev.row.get(c) for c in pk_columns),
                        )
                        counts["delete"] += 1
                    else:
                        self._upsert_rows(cur, schema, table, names, pk_columns, [ev.row])
                        counts["upsert"] += 1
                return counts
            finally:
                cur.close()

        return await self._land_guard.run(_run)

    # -- materialization store -------------------------------------------------

    def ensure_materialize_attached(self) -> str:
        """The Postgres engine materializes into a Postgres store. When it is this same DB the cache
        lives here directly, so the reference is the current database name — a catalog-physical
        ``db.schema.table`` cache ref then resolves natively. (An external store is future work.)"""
        cur = self._con.cursor()
        cur.execute("SELECT current_database()")
        row = cur.fetchone()
        assert row is not None  # SELECT current_database() always returns exactly one row
        return row[0]

    @property
    def connection(self):
        """The psycopg2 connection — the backend's cache terminal issues CREATE TABLE/INSERT through
        its ``cursor()`` into the materialization store (this Postgres)."""
        return self._con

    # -- execution -------------------------------------------------------------

    def borrow_raw(self) -> Any:
        """One connection from the read pool for pgwire's raw-DataRow passthrough (REQ-1863): the
        passthrough drives the extended-query exchange on the connection's own socket and hands it
        back idle (or discards it when its exchange did not complete)."""
        from provisa.pgwire.pg_passthrough import BorrowedPgConnection

        con = self._read_pool.getconn()
        # What the passthrough prepared on this backend session, kept with it. A psycopg2
        # connection cannot be weakly referenced, so the entry is keyed by the session's backend
        # pid and dropped when the connection is discarded.
        pid = con.info.backend_pid
        with self._raw_statements_lock:
            statements = self._raw_statements.setdefault(pid, {})

        def _release(discard: bool) -> None:
            if discard:
                with self._raw_statements_lock:
                    self._raw_statements.pop(pid, None)
            self._read_pool.putconn(con, close=discard)

        return BorrowedPgConnection(
            fileno=con.fileno(),
            ssl_in_use=bool(con.info.ssl_in_use),
            cancel=con.cancel,
            release=_release,
            statements=statements,
        )

    def run_sync(self, sql: str, params: list | None = None) -> ResultStream:
        """Execute governed physical SQL (a SELECT — transpiled by the backend seam) and STREAM it.

        A psycopg2 default cursor buffers the entire result client-side on ``execute``, so ``fetchmany``
        alone would not bound memory. Genuine streaming needs a SERVER-SIDE (named) cursor, which holds
        an open portal and thus requires a transaction — incompatible with the engine connection's
        ``autocommit``. So the read runs on a connection BORROWED FROM ``self._read_pool`` (autocommit
        off): the named cursor pulls ``itersize`` rows per round-trip from Postgres, peak memory bounded
        by one batch. The cursor closes and the connection returns to the pool when the stream drains
        (``on_close``) — REQ-1895: reusing pooled backend sessions (instead of a brand-new connection,
        and thus a brand-new Postgres backend, per call) lets Postgres's own server-side generic-plan-
        after-5-executions optimization actually engage for a repeated-shape query, and avoids paying a
        fresh TCP+auth handshake on every single query. A pooled connection also isolates the open
        portal from the autocommit write/cache connection and from other concurrent streams, same as
        the prior dedicated-connection isolation. Consumers that call ``.rows`` still get the full list
        — the buffering is then explicit at their call site (REQ-1217)."""
        read_con = self._read_pool.getconn()
        try:
            cur = read_con.cursor(name="provisa_stream")  # named ⇒ server-side portal
            cur.itersize = _STREAM_BATCH_ROWS
            with request_deadline.cancel_on_deadline(read_con.cancel):
                cur.execute(*_psycopg2_exec_args(sql, params))
                # psycopg2 populates a NAMED cursor's ``.description`` only after the first FETCH,
                # so peek one batch to force the portal and expose the columns before streaming.
                first = cur.fetchmany(_STREAM_BATCH_ROWS)
        except Exception:
            # Setup failed before the stream/on_close path exists to return this connection —
            # discard it (don't return a possibly-mid-transaction connection to the pool for reuse).
            self._read_pool.putconn(read_con, close=True)
            raise

        def _close(*_: Any) -> None:
            cur.close()
            read_con.commit()
            self._read_pool.putconn(read_con)

        if not cur.description:  # non-row-returning statement — drain now
            _close()
            return QueryResult(rows=[], column_names=[])
        cols = [d[0] for d in cur.description]
        # The description's type_code is the column's pg_type OID, known even for a zero-row
        # result — the pgwire Describe reports it without running the full statement (REQ-589).
        types = _pg_type_names(read_con, [d[1] for d in cur.description])

        def _batches() -> Any:
            if first:
                yield first
            while True:
                chunk = cur.fetchmany(_STREAM_BATCH_ROWS)
                if not chunk:
                    return
                yield chunk

        return StreamingQueryResult(
            _batches(), column_names=cols, column_types=types, on_close=_close
        )

    def describe_sync(self, sql: str, params: list | None = None) -> ResultStream:
        """The statement's result shape without running it: behind a constant-false filter the
        planner emits a one-time false filter, so no row is produced and no input is scanned, while
        the portal's description still carries every column's name and type OID. Postgres keeps
        duplicate column names through ``SELECT *`` of a subquery. REQ-589."""
        return self.run_sync(f"SELECT * FROM ({sql}) _provisa_describe WHERE false", params)

    # -- Arrow transport (ADBC zero-copy) (REQ-1220) ---------------------------

    def _get_adbc_pool(self) -> _AdbcConnectionPool:
        if self._adbc_pool is None:
            self._adbc_pool = _AdbcConnectionPool(
                self._engine_dsn, minconn=_POOL_MINCONN, maxconn=_POOL_MAXCONN
            )
        return self._adbc_pool

    def run_arrow(self, sql: str, params: list | None = None) -> Any:
        """Execute governed physical SQL and return a ``pyarrow.Table`` via the ADBC PostgreSQL
        driver's native Arrow reader — Postgres rows are decoded straight into Arrow, so NO Python
        rows are materialized for the Flight/airport transport (zero-copy relative to the row path).

        The connection is BORROWED from ``self._adbc_pool`` (REQ-1895 — same fresh-connection-per-
        call cost as ``run_sync`` before pooling, isolated here from the engine's psycopg2
        write/cache connection) and returned when the table is built, or discarded on failure."""
        pool = self._get_adbc_pool()
        con = pool.getconn()
        try:
            cur = con.cursor()
            with request_deadline.cancel_on_deadline(cur.adbc_cancel):
                cur.execute(sql, params or None)
                table = cur.fetch_arrow_table()
        except Exception:
            pool.discard(con)
            raise
        pool.putconn(con)
        return table

    def run_arrow_stream(self, sql: str, params: list | None = None) -> tuple[Any, Any]:
        """Execute governed physical SQL and return ``(schema, batch_generator)`` for lazy
        record-batch streaming. ADBC's ``fetch_record_batch`` yields an Arrow ``RecordBatchReader``
        that pulls batches from the Postgres server on demand, so the full result never materializes
        — peak memory is bounded by one batch. The ADBC connection (from ``self._adbc_pool``,
        REQ-1895) returns to the pool when the generator drains or the consumer stops early, or is
        discarded on setup failure."""
        pool = self._get_adbc_pool()
        con = pool.getconn()
        try:
            cur = con.cursor()
            with request_deadline.cancel_on_deadline(cur.adbc_cancel):
                cur.execute(sql, params or None)
                reader = cur.fetch_record_batch()
            schema = reader.schema
        except Exception:
            pool.discard(con)
            raise

        def _batches() -> Any:
            try:
                for batch in reader:
                    yield batch
            finally:
                cur.close()
                pool.putconn(con)

        return schema, _batches()

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        """Async variant: MATERIALIZES on the executor (unlike ``run_sync``), because a lazy
        server-side ``fetchmany`` pulled across the async boundary would block the event loop.

        Runs on a connection BORROWED FROM ``self._read_pool`` (REQ-1906), not ``self._con``: a
        query dispatched here can run past its caller's ``.result(timeout=...)`` deadline (the
        caller gives up but nothing cancels the executor thread underneath), and ``self._con`` is
        also ``attach_source``'s DDL/ANALYZE connection. Confirmed live on the perf-bench VM — a
        federated_join call that outran its pgwire caller's 120s budget kept running on
        ``self._con`` for several more minutes; a second, unrelated call's ``attach_source`` (a
        genuinely new source, not yet in ``_raw_attached``) blocked the entire time waiting for
        that same connection to free up, surfacing as an indefinite ~120s+ hang with no trace of
        why. Borrowing from the pool isolates one call's overrun from every other call's attach
        or query work, exactly as REQ-1895 already isolates ``run_sync``'s reads from ``self._con``."""
        loop = asyncio.get_event_loop()

        def _run() -> QueryResult:
            con = self._read_pool.getconn()
            try:
                cur = con.cursor()
                with request_deadline.cancel_on_deadline(con.cancel):
                    cur.execute(*_psycopg2_exec_args(sql, params))
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = list(cur.fetchall()) if cur.description else []
                con.commit()
                cur.close()
            except Exception:
                self._read_pool.putconn(con, close=True)
                raise
            else:
                self._read_pool.putconn(con)
            return QueryResult(rows=rows, column_names=cols)

        return await loop.run_in_executor(None, _run)

    def close(self) -> None:
        self._con.close()
        self._read_pool.closeall()
        if self._adbc_pool is not None:
            self._adbc_pool.closeall()
