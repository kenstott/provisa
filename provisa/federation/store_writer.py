# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one write face for landing a data source into the materialization store (REQ-848, REQ-932).

Every site that LANDS source rows into the store goes through ``land`` here — never through a
federation-engine connection. The engine only READS the landed replica (attach / external link);
it is never on the write path, so a read-only engine (Databricks/Snowflake external-linking live
Iceberg, or any engine attaching a separate store) is fully supported.

The write face is a single abstraction. Today its one branch is SQLAlchemy Core (``StoreConn`` over
``provisa.core.database.Connection``), which lands into any relational store dialect. Future
platform branches (e.g. a pyiceberg writer for an Iceberg store) slot in behind ``land`` by store
backend, without changing any caller. The landing SHAPE (replace / append / CDC) is chosen from the
effective change_signal (REQ-932) inside this one place.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from sqlalchemy.schema import CreateTable, DropTable

from provisa.core.change_signal import APPEND, select_landing_shape
from provisa.federation.materialize_exec import build_table, land_append, land_replace


def _qualified(schema: str, table: str) -> str:
    return f"{schema}.{table}" if schema else table


if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

# The async SQLAlchemy driver per relational store backend (materialization._RELATIONAL). A store
# DSN naming a bare scheme gets the async driver injected; a DSN that already names a +driver (or a
# future non-relational branch) is used as-is.
_ASYNC_DRIVER = {
    "postgresql": "asyncpg",
    "mysql": "aiomysql",
    "mariadb": "aiomysql",
    "sqlite": "aiosqlite",
}


def async_store_url(dsn: str) -> str:
    """Normalize a relational store DSN to an async SQLAlchemy URL, injecting the async driver when
    the DSN carries none. Raises on a backend with no known async driver rather than guessing."""
    from sqlalchemy import make_url

    url = make_url(dsn)
    if "+" in url.drivername:
        return url.render_as_string(hide_password=False)
    backend = url.get_backend_name()
    driver = _ASYNC_DRIVER.get(backend)
    if driver is None:
        raise ValueError(
            f"no async SQLAlchemy driver known for materialize store backend {backend!r}"
        )
    return url.set(drivername=f"{backend}+{driver}").render_as_string(hide_password=False)


@asynccontextmanager
async def store_connection(dsn: str) -> AsyncGenerator[Any]:
    """A SQLAlchemy ``Connection`` (the ``StoreConn`` write face) to the relational store ``dsn``.

    Built per landing (landing is on-demand, not hot) and disposed after. This is the ONE place a
    store connection for writing is opened; the federation engine never opens one for writes."""
    from provisa.core.database import Database, create_engine_from_url

    engine = create_engine_from_url(async_store_url(dsn), pool_size=1)
    try:
        async with Database(engine, name="materialize").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


async def ensure_table(
    store_dsn: str,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str] | None = None,
) -> str:
    """Eagerly CREATE the (empty) landing table if absent — the DDL half of landing, split from the
    DML so the catalog is complete at startup and the engine attaches an already-existing table.

    Idempotent (``CREATE TABLE IF NOT EXISTS`` + schema-if-absent); never drops or writes rows. Only
    for LANDED sources — a live source is attached live, not pre-created. Returns the qualified name.
    The engine is never the writer — this opens the store's own connection."""
    tbl = build_table(schema, table, columns, tuple(pk_columns or ()))
    async with store_connection(store_dsn) as conn:
        if conn.capabilities.schemas:
            from sqlalchemy.schema import CreateSchema

            await conn.execute_core(CreateSchema(schema, if_not_exists=True))
        await conn.execute_core(CreateTable(tbl, if_not_exists=True))
    return _qualified(schema, table)


async def reconcile_table(
    store_dsn: str,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str] | None = None,
) -> str:
    """Converge the store's landing table to the config schema (REQ-846) — a reconcile, not a blind
    create, so the store SURVIVES a restart:

    - absent            -> create.
    - schema matches     -> KEEP (landed data intact — the restart case).
    - schema drifted      -> RECREATE (drop+create; a table/engine config change is authoritative,
      landed data is re-landed on the next refresh).

    Drift is a change to the column SET/order vs config (design-driven). Upstream *source* drift
    (the live source's schema moving) is a separate land-time concern (best-effort name-map). Returns
    ``created`` | ``kept`` | ``recreated``. The engine is never the writer."""
    want = [name for name, _ in columns]
    tbl = build_table(schema, table, columns, tuple(pk_columns or ()))
    async with store_connection(store_dsn) as conn:
        if conn.capabilities.schemas:
            from sqlalchemy.schema import CreateSchema

            await conn.execute_core(CreateSchema(schema, if_not_exists=True))
        from sqlalchemy.exc import NoSuchTableError

        try:
            existing = await conn.reflect_columns(table, schema or None)
        except NoSuchTableError:
            existing = []  # not yet created → treat as absent
        have = [c["column_name"] for c in existing]
        if not have:
            await conn.execute_core(CreateTable(tbl))
            return "created"
        if have == want:
            return "kept"
        await conn.execute_core(DropTable(tbl, if_exists=True))
        await conn.execute_core(CreateTable(tbl))
        return "recreated"


def check_source_drift(
    columns: list[tuple[str, str]], rows: list[dict], *, match_floor: float = 0.0
) -> float:
    """Source-drift guard (REQ-932): the incoming ``rows`` are mapped to the target ``columns`` by
    NAME — matched columns land, unmatched target columns are NULL, extra incoming keys are dropped
    (best-effort). If the fraction of target columns present in the incoming data is ``<= match_floor``
    the source has drifted beyond recognition; raise rather than land a mangled table (the caller
    turns this into an ``error`` event). ``match_floor=0.0`` fails only at 0% overlap. Returns the
    match ratio."""
    if not rows:
        return 1.0
    target = {name for name, _ in columns}
    if not target:
        return 1.0
    incoming: set[str] = set().union(*(r.keys() for r in rows))
    ratio = len(target & incoming) / len(target)
    if ratio <= match_floor:
        raise ValueError(
            f"source drift: {len(target & incoming)}/{len(target)} target columns present in the "
            f"incoming data ({ratio:.0%} <= floor {match_floor:.0%}) — refusing to land a mangled "
            f"table; expected {sorted(target)}, got {sorted(incoming)}"
        )
    return ratio


async def land(
    store_dsn: str,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    change_signal: str = "ttl",
    watermark_column: str | None = None,
    pk_columns: list[str] | None = None,
    match_floor: float = 0.0,
) -> str:
    """Land ``rows`` into ``schema.table`` of the materialization store, through the write face.

    The shape is chosen from ``change_signal`` (REQ-932): a poll signal with a watermark AMENDS
    (append the watermark-filtered delta); every other batch is a full REPLACE. Hard-delete CDC is
    the separate streaming path (subscriptions.cdc_landing). ``match_floor`` guards against upstream
    source drift — below it the land is refused (see ``check_source_drift``). Returns the qualified
    landed name. The engine is never the writer — this opens the store's own connection."""
    check_source_drift(columns, rows, match_floor=match_floor)
    shape = select_landing_shape(change_signal, watermark_column)
    tbl = build_table(schema, table, columns, tuple(pk_columns or ()))
    async with store_connection(store_dsn) as conn:
        if conn.capabilities.schemas:
            from sqlalchemy.schema import CreateSchema

            await conn.execute_core(CreateSchema(schema, if_not_exists=True))
        if shape == APPEND:
            return await land_append(conn, tbl, rows)
        return await land_replace(conn, tbl, rows)  # REPLACE, or a push signal's snapshot seed
