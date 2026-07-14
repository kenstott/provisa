# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Control-plane database abstraction backed by SQLAlchemy Core.

This is the ``AdminDatabase`` contract that decouples the Provisa control plane
from raw asyncpg. It wraps a SQLAlchemy :class:`AsyncEngine` (whose connection
pool replaces the former ``asyncpg.create_pool``) and exposes an asyncpg-shaped
async API so the ~586 existing call sites keep working with minimal churn:

    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM sources WHERE id = $1", sid)
        await conn.execute("DELETE FROM sources WHERE id = $1", sid)

Two instances back the split control plane (see ``schema_admin`` /
``schema_org``): the **platform control plane** (``admin``) and the **tenant
control plane** (``org``, per-org).

Semantics deliberately mirror asyncpg:

- Positional ``$1``/``$2`` placeholders are translated to SQLAlchemy ``:pN``
  named binds; call sites pass positional args unchanged.
- Statements outside an explicit :meth:`Connection.transaction` are committed
  immediately (asyncpg's default autocommit). Inside ``transaction()`` they are
  grouped and committed/rolled back together; nested blocks use savepoints.
- :meth:`Connection.execute` returns an asyncpg-style status string
  (``"DELETE 1"``, ``"UPDATE 3"``, ``"INSERT 0 1"``) so status parsing at call
  sites (e.g. ``repositories/source.py``) is preserved.
- ``jsonb``/``json`` columns are (de)serialized via the same codec the old pool
  registered, so ``row['mapping']`` is a ``dict`` not a JSON string.

Portability (Tier-2: SQLite >=3.35, MySQL 8) is layered on in later phases via
:class:`Capabilities` gating; on PostgreSQL behavior is identical to the former
asyncpg pool.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncGenerator

from sqlalchemy import Table, event, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine


def _json_encoder(v: Any) -> str:
    return v if isinstance(v, str) else json.dumps(v)


async def _register_json_codecs(conn: Any) -> None:
    """Match the codec the former asyncpg pool installed (provisa/core/db.py)."""
    await conn.set_type_codec(
        "jsonb", encoder=_json_encoder, decoder=json.loads, schema="pg_catalog"
    )
    await conn.set_type_codec(
        "json", encoder=_json_encoder, decoder=json.loads, schema="pg_catalog"
    )


# --------------------------------------------------------------------------- #
# capabilities
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Capabilities:
    """Per-dialect feature flags. PostgreSQL supports everything; Tier-2
    backends gate PG-only features (LISTEN/NOTIFY, advisory locks, arrays,
    append-only RULEs, RETURNING) so callers can branch.

    ``schemas`` is the org-isolation axis. Schema-capable backends (PG/Oracle/
    MySQL) scope an org as a namespace switched on a single connection — see
    ``enter_org_sql``. Non-schema-capable backends (SQLite) carry the org in the
    database *file*, so an org is a distinct engine/connection, not a statement;
    ``enter_org_sql`` returns None and org selection happens when the engine is
    built (file-per-org)."""

    dialect: str
    listen_notify: bool
    advisory_lock: bool
    arrays: bool
    rules: bool
    returning: bool
    schemas: bool

    def enter_org_sql(self, schema: str) -> str | None:
        """The statement that scopes a connection to ``schema`` (org namespace),
        or None for non-schema-capable backends. Semantics differ per dialect:
        PG search_path, MySQL current database, Oracle current schema."""
        if not self.schemas:
            return None
        if self.dialect == "postgresql":
            return f"SET search_path TO {schema}"
        if self.dialect in ("mysql", "mariadb"):
            return f"USE {schema}"
        if self.dialect == "oracle":
            return f"ALTER SESSION SET CURRENT_SCHEMA = {schema}"
        return None

    @classmethod
    def for_dialect(cls, dialect: str) -> "Capabilities":
        d = dialect.split("+", 1)[0]
        if d == "postgresql":
            return cls(
                d,
                listen_notify=True,
                advisory_lock=True,
                arrays=True,
                rules=True,
                returning=True,
                schemas=True,
            )
        if d == "sqlite":
            return cls(
                d,
                listen_notify=False,
                advisory_lock=False,
                arrays=False,
                rules=False,
                returning=True,
                schemas=False,
            )
        if d in ("mysql", "mariadb"):
            return cls(
                d,
                listen_notify=False,
                advisory_lock=True,
                arrays=False,
                rules=False,
                returning=False,
                schemas=True,
            )
        if d == "duckdb":
            # An embedded DuckDB file used as a materialization store (REQ-989): schema-capable,
            # RETURNING-capable; no LISTEN/NOTIFY or advisory locks (single-process file store).
            return cls(
                d,
                listen_notify=False,
                advisory_lock=False,
                arrays=True,
                rules=False,
                returning=True,
                schemas=True,
            )
        if d == "oracle":
            return cls(
                d,
                listen_notify=False,
                advisory_lock=False,
                arrays=False,
                rules=False,
                returning=True,
                schemas=True,
            )
        return cls(
            d,
            listen_notify=False,
            advisory_lock=False,
            arrays=False,
            rules=False,
            returning=False,
            schemas=False,
        )


# --------------------------------------------------------------------------- #
# row adapter
# --------------------------------------------------------------------------- #
class Row:
    """Wraps a SQLAlchemy ``Row`` to mimic ``asyncpg.Record``: ``row['col']``,
    ``row[0]``, ``dict(row)``, ``.get()``, ``.keys()``, and value-iteration."""

    __slots__ = ("_row", "_mapping")

    def __init__(self, row: Any) -> None:
        self._row = row
        self._mapping = row._mapping

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return self._row[key]
        return self._mapping[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._mapping.get(key, default)

    def keys(self):
        return self._mapping.keys()

    def values(self):
        return self._mapping.values()

    def items(self):
        return self._mapping.items()

    def __iter__(self):
        # asyncpg.Record iterates values; dict(row) uses keys()+__getitem__.
        return iter(self._mapping.values())

    def __contains__(self, key: Any) -> bool:
        return key in self._mapping

    def __len__(self) -> int:
        return len(self._mapping)

    def __repr__(self) -> str:
        return f"Row({dict(self._mapping)!r})"


# --------------------------------------------------------------------------- #
# placeholder translation
# --------------------------------------------------------------------------- #
_PLACEHOLDER = re.compile(r"\$(\d+)")
# A PG cast (``::jsonb``, ``::text[]`` …) applied directly to a placeholder.
# SQLAlchemy's text() bind regex has a ``(?!:)`` lookahead, so it refuses to
# bind ``:pN`` when ``::`` follows. We drop the cast on the placeholder: PG
# infers the param type from the target column/context, and the jsonb codec
# (registered on the connection) still (de)serializes correctly. Standalone
# casts like ``col::text`` are left untouched.
_CAST_ON_BIND = re.compile(r"(:p\d+)::\w+(?:\[\])?")


def _translate(sql: str, args: tuple) -> tuple[str, dict[str, Any]]:
    """Convert asyncpg ``$1``-style SQL + positional args to SQLAlchemy
    ``:pN``-style SQL + a param dict."""
    if not args:
        return sql, {}
    params = {f"p{i + 1}": a for i, a in enumerate(args)}
    sql = _PLACEHOLDER.sub(lambda m: f":p{m.group(1)}", sql)
    # Strip ``::type`` casts on binds — SQLAlchemy text() would misread the ``::``
    # as another bind. Array-typed binds that need a cast (e.g. unnest) must use
    # CAST(:p AS type[]) form instead, which survives this and SQLAlchemy parsing.
    sql = _CAST_ON_BIND.sub(lambda m: m.group(1), sql)
    return sql, params


_DOLLAR_QUOTE = re.compile(r"\$\$.*?\$\$", re.DOTALL)
_SQL_STRING = re.compile(r"'(?:[^']|'')*'")
_LINE_COMMENT = re.compile(r"--[^\n]*")


def _is_multi_statement(sql: str) -> bool:
    """True if *sql* contains more than one top-level statement (separated by
    ``;``), ignoring ``$$``-quoted blocks, string literals, and line comments.

    asyncpg's extended/prepared protocol (what SQLAlchemy ``text()`` uses)
    rejects multiple commands with "cannot insert multiple commands into a
    prepared statement"; such scripts must run on the raw driver connection.
    A single ``DO $$ ... $$`` block is NOT multi-statement."""
    s = _DOLLAR_QUOTE.sub("", sql)
    s = _SQL_STRING.sub("", s)
    s = _LINE_COMMENT.sub("", s)
    s = s.strip().rstrip(";").strip()
    return ";" in s


_VERB = re.compile(r"^\s*(\w+)")


def _status(sql: str, rowcount: int) -> str:
    """Synthesize an asyncpg-style command status tag from the verb + rowcount."""
    m = _VERB.match(sql)
    verb = m.group(1).upper() if m else ""
    if verb == "INSERT":
        return f"INSERT 0 {max(rowcount, 0)}"
    if verb in ("UPDATE", "DELETE", "SELECT"):
        return f"{verb} {max(rowcount, 0)}"
    return verb


# --------------------------------------------------------------------------- #
# connection
# --------------------------------------------------------------------------- #
class Connection:
    """asyncpg-shaped wrapper over a SQLAlchemy :class:`AsyncConnection`."""

    def __init__(self, ac: AsyncConnection, caps: Capabilities) -> None:
        self._ac = ac
        self.capabilities = caps
        self._tx_depth = 0

    async def _run(self, sql: str, args: tuple):
        stmt, params = _translate(sql, args)
        return await self._ac.execute(text(stmt), params)

    async def _commit_if_autocommit(self) -> None:
        if self._tx_depth == 0:
            await self._ac.commit()

    async def execute(self, sql: str, *args: Any) -> str:
        # No-arg DDL scripts (multiple statements) can't go through the prepared
        # protocol; route them to the raw driver. Parameterized statements never
        # reach here as multi-statement (they carry args).
        if not args and _is_multi_statement(sql):
            await self.execute_script(sql)
            await self._commit_if_autocommit()
            return ""
        result = await self._run(sql, args)
        rowcount = result.rowcount if result.rowcount is not None else -1
        await self._commit_if_autocommit()
        return _status(sql, rowcount)

    async def executemany(self, sql: str, args_seq: list) -> None:
        stmt, _ = _translate(sql, tuple(args_seq[0]) if args_seq else ())
        param_list = [{f"p{i + 1}": v for i, v in enumerate(row)} for row in args_seq]
        if param_list:
            await self._ac.execute(text(stmt), param_list)
        await self._commit_if_autocommit()

    async def fetch(self, sql: str, *args: Any) -> list[Row]:
        result = await self._run(sql, args)
        rows = [Row(r) for r in result.fetchall()]
        await self._commit_if_autocommit()
        return rows

    async def fetchrow(self, sql: str, *args: Any) -> Row | None:
        result = await self._run(sql, args)
        r = result.fetchone()
        await self._commit_if_autocommit()
        return Row(r) if r is not None else None

    async def fetchval(self, sql: str, *args: Any, column: int = 0) -> Any:
        result = await self._run(sql, args)
        r = result.fetchone()
        await self._commit_if_autocommit()
        return r[column] if r is not None else None

    async def reflect_columns(self, table: str, schema: str | None = None) -> list[dict]:
        """Portable column reflection via the SQLAlchemy Inspector — replaces raw
        ``information_schema`` queries so it runs on any backend. Returns one dict per
        column: ``{column_name, data_type, is_primary_key}``. ``data_type`` is the
        lowercased SQL type name with array/json collapsed to ``text`` (mirroring the
        engine surface, which sees those columns as text).

        ``schema`` is the org-isolation schema; it is honoured only on schema-capable
        backends (``capabilities.schemas``) and ignored on schema-less ones (SQLite),
        so callers pass the org schema unconditionally and the dialect decision stays
        inside the abstraction."""
        eff_schema = schema if self.capabilities.schemas else None
        is_sqlite = self.capabilities.dialect == "sqlite"

        # SQLite does not type VIEW columns, so the Inspector reports NullType (→ "null") for the
        # meta-table views. Rather than default such a column to string, analyze the actual data
        # in-line with SQL (``typeof``) and pick the best match; storage classes map cleanly.
        _SQLITE_STORAGE = {"integer": "integer", "real": "double", "text": "text", "blob": "blob"}

        def _inspect(sync_conn: Any) -> list[dict]:
            from sqlalchemy import inspect as _sa_inspect

            insp = _sa_inspect(sync_conn)
            pk = set(
                insp.get_pk_constraint(table, schema=eff_schema).get("constrained_columns") or []
            )
            ref = f'"{table}"' if eff_schema is None else f'"{eff_schema}"."{table}"'

            def _infer_sqlite(col: str) -> str:
                # A column with no non-null sample (empty table / all-null) yields no row — the
                # storage class is genuinely undetermined, so "text" is the neutral class. This is
                # the design-mandated default (REQ-947 design-time typing), not error-swallowing.
                row = sync_conn.exec_driver_sql(
                    f'SELECT typeof("{col}") FROM {ref} WHERE "{col}" IS NOT NULL LIMIT 1'
                ).fetchone()
                return _SQLITE_STORAGE.get(row[0], "text") if row else "text"

            cols: list[dict] = []
            for c in insp.get_columns(table, schema=eff_schema):
                type_name = str(c["type"]).split("(")[0].strip().lower()
                if "[]" in type_name or type_name in ("array", "json", "jsonb"):
                    type_name = "text"
                elif is_sqlite and type_name in ("null", "nulltype", ""):
                    type_name = _infer_sqlite(c["name"])
                cols.append(
                    {
                        "column_name": c["name"],
                        "data_type": type_name,
                        "is_primary_key": c["name"] in pk,
                    }
                )
            return cols

        return await self._ac.run_sync(_inspect)

    # -- advisory locks (dialect-portable; a no-op where the backend has none) --
    async def advisory_xact_lock(self, key: int) -> None:
        """Take a transaction-scoped advisory lock keyed by ``key`` (auto-released at commit).
        A no-op on backends without advisory locks — single-writer file DBs (SQLite) need none."""
        caps = self.capabilities
        if not caps.advisory_lock:
            return
        if caps.dialect == "postgresql":
            await self.execute(f"SELECT pg_advisory_xact_lock({key})")
        elif caps.dialect in ("mysql", "mariadb"):
            await self.execute(f"SELECT GET_LOCK('{key}', -1)")

    @asynccontextmanager
    async def advisory_lock(self, key: int) -> "AsyncGenerator[Connection]":
        """Hold a session advisory lock keyed by ``key`` for the ``with`` block, released on exit.
        A no-op on backends without advisory locks."""
        caps = self.capabilities
        held = caps.advisory_lock and caps.dialect in ("postgresql", "mysql", "mariadb")
        if held:
            take = (
                f"SELECT pg_advisory_lock({key})"
                if caps.dialect == "postgresql"
                else f"SELECT GET_LOCK('{key}', -1)"
            )
            await self.execute(take)
        try:
            yield self
        finally:
            if held:
                release = (
                    f"SELECT pg_advisory_unlock({key})"
                    if caps.dialect == "postgresql"
                    else f"SELECT RELEASE_LOCK('{key}')"
                )
                await self.execute(release)

    # -- portable Core helpers (dialect-agnostic; used by migrated repositories) --
    async def execute_core(self, stmt: Any) -> Any:
        """Execute a SQLAlchemy Core statement (select/insert/update/delete)
        and return the CursorResult. Autocommits outside a transaction."""
        result = await self._ac.execute(stmt)
        await self._commit_if_autocommit()
        return result

    async def bulk_copy(self, table: Table, rows: list[dict[str, Any]]) -> int:
        """Bulk-ingest ``rows`` into ``table`` via the store's fastest columnar / bulk path (REQ-990).

        The path is chosen from the dialect capability — explicit, never a silent fallback:
        - PostgreSQL: binary ``COPY`` (asyncpg ``copy_records_to_table``) — the columnar bulk-load
          path, one round trip, no per-row statement.
        - Every other relational backend: a single ``executemany`` Core INSERT (one prepared
          statement, N parameter sets) — still a bulk path, never a per-row loop.

        Rows are normalized to the table's column order; a key absent from a row lands as NULL.
        Returns the number of rows ingested. A no-op for an empty batch."""
        if not rows:
            return 0
        colnames = [c.name for c in table.columns]
        if self.capabilities.dialect == "postgresql":
            records = [tuple(r.get(cn) for cn in colnames) for r in rows]
            raw = await self._driver_connection()
            await raw.copy_records_to_table(
                table.name, records=records, columns=colnames, schema_name=table.schema
            )
            await self._commit_if_autocommit()
            return len(records)
        param_list = [{cn: r.get(cn) for cn in colnames} for r in rows]
        await self._ac.execute(table.insert(), param_list)
        await self._commit_if_autocommit()
        return len(param_list)

    async def upsert(
        self,
        table: Table,
        values: dict[str, Any],
        *,
        index_elements: list[str],
        update_columns: list[str] | None = None,
        set_extra: dict[str, Any] | None = None,
    ) -> None:
        """Upsert a row, dialect-AGNOSTICALLY: UPDATE by the conflict keys, and INSERT if no row
        matched. Uses only generic Core (update/insert/select) — no dialect-specific ON CONFLICT /
        MERGE / ON DUPLICATE KEY, so it works on EVERY SQLAlchemy backend, not an enumerated few.

        ``update_columns`` defaults to all inserted columns except the conflict keys; an empty list
        means DO NOTHING (insert-if-absent). ``set_extra`` adds/overrides set assignments with Core
        expressions (e.g. ``{"version": table.c.version + 1}``)."""
        from sqlalchemy import (
            and_,
            insert as _insert,
            literal,
            select as _select,
            update as _update,
        )
        from sqlalchemy.exc import IntegrityError

        cols = (
            update_columns
            if update_columns is not None
            else [c for c in values if c not in index_elements]
        )
        set_map: dict[str, Any] = {c: values[c] for c in cols}
        set_map.update(set_extra or {})
        where = and_(*[table.c[k] == values[k] for k in index_elements])

        if set_map:
            res = await self.execute_core(_update(table).where(where).values(**set_map))
            if (res.rowcount or 0) > 0:
                return
        else:
            exists = await self.execute_core(_select(literal(1)).select_from(table).where(where))
            if exists.fetchone() is not None:
                return  # DO NOTHING — row already present
        try:
            await self.execute_core(_insert(table).values(**values))
        except IntegrityError:
            # Lost an insert race with a concurrent writer — fall back to the update.
            if set_map:
                await self.execute_core(_update(table).where(where).values(**set_map))

    async def upsert_returning(
        self,
        table: Table,
        values: dict[str, Any],
        *,
        index_elements: list[str],
        returning: str,
        update_columns: list[str] | None = None,
        set_extra: dict[str, Any] | None = None,
    ) -> Any:
        """Upsert (see :meth:`upsert`) then return one column of the row (e.g. its id), via a plain
        SELECT on the conflict keys — dialect-agnostic, no RETURNING dependency."""
        from sqlalchemy import and_, select as _select

        await self.upsert(
            table,
            values,
            index_elements=index_elements,
            update_columns=update_columns,
            set_extra=set_extra,
        )
        where = and_(*[table.c[k] == values[k] for k in index_elements])
        res = await self.execute_core(_select(table.c[returning]).where(where))
        row = res.fetchone()
        return row[0] if row is not None else None

    async def insert_returning(self, table: Table, values: dict[str, Any], returning: str) -> Any:
        """INSERT and return one generated column value, portably.

        Uses ``RETURNING`` where supported (PostgreSQL, SQLite >=3.35); falls
        back to ``lastrowid`` on MySQL 8, which lacks RETURNING."""
        from sqlalchemy import insert as _insert

        if self.capabilities.returning:
            stmt = _insert(table).values(**values).returning(table.c[returning])
            result = await self.execute_core(stmt)
            row = result.fetchone()
            return row[0] if row is not None else None
        result = await self.execute_core(_insert(table).values(**values))
        return result.lastrowid

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        """Group statements; commit on success, roll back on exception.

        Mirrors ``asyncpg.Connection.transaction``: the outermost block is a
        real transaction; nested blocks use savepoints.
        """
        if self._tx_depth == 0:
            self._tx_depth += 1
            try:
                yield
                await self._ac.commit()
            except BaseException:
                await self._ac.rollback()
                raise
            finally:
                self._tx_depth -= 1
        else:
            self._tx_depth += 1
            sp = await self._ac.begin_nested()
            try:
                yield
                await sp.commit()
            except BaseException:
                await sp.rollback()
                raise
            finally:
                self._tx_depth -= 1

    async def execute_script(self, sql: str) -> None:
        """Run a multi-statement SQL script (DDL bootstrap).

        PostgreSQL: SQLAlchemy ``text()`` uses the extended protocol, which rejects
        multiple statements and ``DO $$`` blocks in a single call, so route the whole
        script to asyncpg's simple query protocol (``conn.execute``), which allows both.

        Other dialects: split into individual statements and run each through the
        SQLAlchemy connection via ``exec_driver_sql`` (dialect-agnostic — no driver-
        specific method). The non-PG scripts we emit are plain DDL (e.g. meta-view
        ``DROP``/``CREATE``) with no procedural blocks or embedded statement separators;
        tables come from ``metadata.create_all``, not this path."""
        if self.capabilities.dialect == "postgresql":
            conn = await self._driver_connection()
            await conn.execute(sql)
            return
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                await self._ac.exec_driver_sql(stmt)

    async def prepare(self, sql: str) -> Any:
        """Prepare a statement on the raw asyncpg driver connection and return
        the asyncpg ``PreparedStatement`` (used for describe-empty-result paths
        via ``stmt.get_attributes()`` in cypher_router / pgwire). PostgreSQL
        only — these paths are PG-protocol specific."""
        conn = await self._driver_connection()
        return await conn.prepare(sql)

    # -- raw driver access for PG-only LISTEN/NOTIFY (subscriptions, triggers) --
    async def _driver_connection(self) -> Any:
        raw = await self._ac.get_raw_connection()
        return raw.driver_connection

    async def add_listener(self, channel: str, callback: Any) -> None:
        conn = await self._driver_connection()
        await conn.add_listener(channel, callback)

    async def remove_listener(self, channel: str, callback: Any) -> None:
        conn = await self._driver_connection()
        await conn.remove_listener(channel, callback)


# --------------------------------------------------------------------------- #
# database
# --------------------------------------------------------------------------- #
class Database:
    """A control-plane database handle backed by one SQLAlchemy AsyncEngine.

    ``search_path`` scopes every acquired connection to the org namespace on
    schema-capable backends, preserving the isolation the former asyncpg pool
    provided via its ``setup`` callback. The scoping statement is dialect-
    dispatched (``Capabilities.enter_org_sql``): PG search_path, MySQL current
    database, Oracle current schema. Non-schema-capable backends (SQLite) carry
    the org in the file, so this is a no-op there — org = which engine.
    """

    def __init__(self, engine: AsyncEngine, name: str, search_path: str | None = None) -> None:
        self._engine = engine
        self.name = name
        self.search_path = search_path
        self.dialect = engine.dialect.name
        self.capabilities = Capabilities.for_dialect(self.dialect)

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[Connection]:
        async with self._engine.connect() as ac:
            if self.search_path and (sql := self.capabilities.enter_org_sql(self.search_path)):
                await ac.execute(text(sql))
                await ac.commit()
            yield Connection(ac, self.capabilities)

    # Pool-style passthrough (asyncpg pools proxy connection methods). Used by
    # the few call sites that call db.execute(...) / db.fetch(...) directly.
    async def execute(self, sql: str, *args: Any) -> str:
        async with self.acquire() as conn:
            return await conn.execute(sql, *args)

    async def fetch(self, sql: str, *args: Any) -> list[Row]:
        async with self.acquire() as conn:
            return await conn.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args: Any) -> Row | None:
        async with self.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args: Any, column: int = 0) -> Any:
        async with self.acquire() as conn:
            return await conn.fetchval(sql, *args, column=column)

    def get_size(self) -> int:
        """Current pool size (checked-out + idle connections).

        Returns -1 when the engine uses a pool that doesn't track connection
        counts (NullPool/StaticPool — e.g. some SQLite configs), which has no
        meaningful size.
        """
        pool = self._engine.pool
        if isinstance(pool, QueuePool):
            return pool.checkedout() + pool.checkedin()
        return -1

    def get_idle_size(self) -> int:
        """Idle (checked-in) connections, or -1 for a non-sized pool (see get_size)."""
        pool = self._engine.pool
        if isinstance(pool, QueuePool):
            return pool.checkedin()
        return -1

    async def close(self) -> None:
        await self._engine.dispose()


# --------------------------------------------------------------------------- #
# factory
# --------------------------------------------------------------------------- #
def build_url(
    dialect: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> str:
    """Build a SQLAlchemy async URL (mirrors ingest/engine.py::_build_url)."""
    if not dialect:
        dialect = "postgresql+asyncpg"
    if not host:
        host = "localhost"
    if not port:
        port = 5432
    pw = urllib.parse.quote_plus(password or "")
    return f"{dialect}://{username}:{pw}@{host}:{port}/{database}"


def create_engine(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    pool_size: int = 5,
    pool_min: int = 0,
    dialect: str = "postgresql+asyncpg",
) -> AsyncEngine:
    """Create the control-plane AsyncEngine. ``max_overflow`` is derived from
    ``pool_size - pool_min``. On PostgreSQL, registers the jsonb/json codecs the
    former asyncpg pool used."""
    url = build_url(dialect, host, port, database, user, password)
    return create_engine_from_url(
        url, pool_size=pool_size, max_overflow=max(pool_size - pool_min, 0)
    )


# Control-plane store backends selectable by SQLAlchemy URI (REQ-828). The value is the
# async driver each dialect must use; an embedded engine (sqlite/duckdb) gives the desktop
# deployment model zero external infra, Postgres backs production — one abstraction, same schema.
_ADMIN_ASYNC_DRIVER: dict[str, str] = {
    "postgresql": "asyncpg",
    "sqlite": "aiosqlite",
    "duckdb": "aioduckdb",
    "mysql": "aiomysql",
    "mariadb": "aiomysql",
}


def _normalize_admin_url(url: str) -> str:
    """Resolve a control-plane URI to its async driver, failing loud on an unsupported
    or misconfigured backend (REQ-828 — no silent fallback to a default store).

    A bare backend (``duckdb://…``) is pinned to the one supported async driver; an
    explicit async driver is passed through; a known sync driver is rejected with the
    async form to use; an unknown backend is rejected outright."""
    from sqlalchemy import make_url
    from sqlalchemy.exc import ArgumentError

    try:
        parsed = make_url(url)
    except ArgumentError as exc:
        raise ValueError(f"invalid control-plane store URI {url!r}: {exc}") from exc

    backend = parsed.get_backend_name()
    # ``drivername`` is the raw ``backend[+driver]`` token; ``get_driver_name()`` would
    # substitute the dialect's default sync driver, hiding that none was requested.
    driver = parsed.drivername.split("+", 1)[1] if "+" in parsed.drivername else ""
    if backend not in _ADMIN_ASYNC_DRIVER:
        raise ValueError(
            f"unsupported control-plane store backend {backend!r} in URI {url!r}; "
            f"supported: {', '.join(sorted(_ADMIN_ASYNC_DRIVER))}"
        )
    async_driver = _ADMIN_ASYNC_DRIVER[backend]
    if not driver:
        return str(parsed.set(drivername=f"{backend}+{async_driver}"))
    if driver != async_driver:
        raise ValueError(
            f"control-plane store {backend!r} requires the async driver "
            f"{backend}+{async_driver}, got {backend}+{driver} in URI {url!r}"
        )
    return url


def create_engine_from_url(
    url: str,
    *,
    pool_size: int = 5,
    max_overflow: int = 5,
) -> AsyncEngine:
    """Create a control-plane AsyncEngine from a SQLAlchemy URI (REQ-828).

    The platform and tenant control planes are each configured by an independent
    SQLAlchemy URI (``postgresql+asyncpg://…``, ``sqlite+aiosqlite:///…``,
    ``duckdb:///…``, ``mysql+aiomysql://…``), so neither is tied to PostgreSQL. The
    URI selects the backend; an embedded engine (SQLite/DuckDB) runs the store with
    zero external infra on a developer desktop, Postgres in production — same schema,
    same behavior. An unsupported/misconfigured URI fails loud (no default store).
    On PostgreSQL the jsonb/json codecs the former asyncpg pool used are registered
    per connection.
    """
    from sqlalchemy import make_url

    normalized = _normalize_admin_url(url)
    if normalized.startswith("duckdb"):
        # Ensure the async DuckDB driver is registered before the engine is built.
        import provisa.core.duckdb_async  # noqa: F401

    # An in-memory embedded store (sqlite/duckdb ``:memory:``) lives only inside a
    # single connection, so its dialect forces a StaticPool — which rejects sizing
    # kwargs. File and server backends take the sized async pool.
    kwargs: dict[str, Any] = {"pool_pre_ping": True}
    if make_url(normalized).database not in (None, "", ":memory:"):
        kwargs["pool_size"] = pool_size
        kwargs["max_overflow"] = max_overflow
    engine = create_async_engine(normalized, **kwargs)
    if engine.dialect.name == "postgresql":
        event.listen(engine.sync_engine, "connect", _on_pg_connect)

    return engine


def _on_pg_connect(dbapi_conn: Any, connection_record: Any) -> None:
    """SQLAlchemy ``connect`` listener: install the jsonb/json codecs on each
    new asyncpg connection (matches the former asyncpg pool ``init``)."""
    del connection_record
    dbapi_conn.run_async(_register_json_codecs)


# --------------------------------------------------------------------------- #
# org router — multi-tenant on not-schema-capable backends
# --------------------------------------------------------------------------- #
class OrgRouter:
    """Maps ``org_id`` -> :class:`Database`, one engine/file per org.

    The multi-tenant mechanism for **not-schema-capable** backends (SQLite,
    DuckDB), where an org cannot be a namespace switched on a shared connection
    (``Capabilities.schemas`` is False) and instead lives in its own database
    file. Schema-capable backends (PG/Oracle/MySQL) do NOT need this — a single
    shared :class:`Database` scopes orgs via ``Capabilities.enter_org_sql`` — so
    construct a router only when ``Capabilities.for_dialect(...).schemas`` is
    False. Engines are built lazily and cached, so each org keeps one pool."""

    def __init__(self, base_url: str, *, pool_size: int = 5, max_overflow: int = 5) -> None:
        from sqlalchemy import make_url

        self._base_url = make_url(base_url)
        if Capabilities.for_dialect(self._base_url.get_dialect().name).schemas:
            raise ValueError(
                "OrgRouter is for not-schema-capable backends (file-per-org); "
                f"{self._base_url.get_backend_name()} scopes orgs on a shared engine — "
                "use a single Database with Capabilities.enter_org_sql instead."
            )
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._cache: dict[str, Database] = {}

    def _org_url(self, org_id: str) -> str:
        """Per-org file URL: a sibling of the base file named ``org_<id>.db``."""
        from pathlib import PurePosixPath

        base_db = self._base_url.database
        if not base_db:
            raise ValueError(f"base URL has no database path: {self._base_url!r}")
        parent = PurePosixPath(base_db).parent
        org_path = str(parent / f"org_{org_id}{PurePosixPath(base_db).suffix or '.db'}")
        return str(self._base_url.set(database=org_path))

    def database_for(self, org_id: str) -> "Database":
        from provisa.core.db import _validate_org_id

        _validate_org_id(org_id)
        if org_id not in self._cache:
            engine = create_engine_from_url(
                self._org_url(org_id), pool_size=self._pool_size, max_overflow=self._max_overflow
            )
            self._cache[org_id] = Database(engine, name=f"org_{org_id}")
        return self._cache[org_id]

    async def close(self) -> None:
        for db in self._cache.values():
            await db.close()
        self._cache.clear()
