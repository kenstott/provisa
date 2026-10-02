# Copyright (c) 2026 Kenneth Stott
# Canary: 7e4b2d91-8f3a-4c1e-b5d0-a2f91e83c740
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

"""Materialize API response rows into a cache table for Phase 2 the engine execution.

Default backend: source's own the engine catalog (PostgreSQL connector) so same-source
JOINs are pushed down to a single database.

Any registered the engine catalog can be the cache target — specify via cache_catalog
on the Source config.  The only special case is the Iceberg catalog ("results"):
table CREATE adds PARQUET format+S3 location, and DROP triggers S3 cleanup.

Execution model for OpenAPI/REST sources:
  Phase 1 — REST call: native filter args (path/query params) build the URL.
             On cache miss, rows are materialized into the cache table.
  Phase 2 — the engine SQL: compiled WHERE/ORDER BY/LIMIT applied to cached rows.
             Same-source JOINs are pushed down by the engine when cache catalog
             matches the source catalog (both PostgreSQL).
"""

# Requirements: REQ-280, REQ-309, REQ-318, REQ-327

# complexity-gate: allow-ble=6 reason="API-response cache materialization is best-effort augmentation: any cache/store failure falls back to live execution, never failing the query"

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import sqlglot
import sqlglot.expressions as exp

log = logging.getLogger(__name__)

_ICEBERG_CATALOG = "results"
_ICEBERG_BUCKET = "provisa-results"
_DEFAULT_CACHE_SCHEMA = "api_cache"

# In-process TTL cache for table_exists results.
# Key: (catalog, schema, table_name) → expiry monotonic time.
# Avoids a live the engine probe on every request when the table is known-live.
_TABLE_EXISTS_CACHE: dict[tuple[str, str, str], float] = {}
_TABLE_EXISTS_SAFETY_MARGIN = 30  # expire this many seconds before the engine TTL

# In-process cache for schema existence — evicted only on process restart.
# Schema DROP is not expected in normal operation; safe to cache indefinitely.
_SCHEMA_EXISTS_CACHE: set[tuple[str, str]] = set()

_API_TYPE_TO_IR: dict[str, str] = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
    "jsonb": "VARCHAR",
}


@dataclass(frozen=True)
class CacheLocation:  # REQ-318, REQ-309, REQ-327
    catalog: str
    schema: str
    backend: str  # "iceberg" or "postgresql" (any non-iceberg catalog)


def cache_location(  # REQ-318, REQ-309, REQ-327
    source_id: str,
    cache_catalog: str | None = None,
    cache_schema: str = _DEFAULT_CACHE_SCHEMA,
    *,
    engine: Any = None,
) -> CacheLocation:
    """Build cache location.

    cache_catalog=None → the bound engine's cache catalog (``engine.cache_catalog()``): a broad
    federator / store-engine caches into the source's own (durable) catalog (returns None → source_id
    with hyphens→underscores); an ephemeral engine caches into its attached materialization store.
    Any explicit catalog is used as-is; "results" triggers Iceberg S3 behaviour.
    """
    if cache_catalog is None and engine is not None:
        cache_catalog = engine.cache_catalog()
    catalog = cache_catalog if cache_catalog is not None else source_id.replace("-", "_")
    backend = "iceberg" if catalog == _ICEBERG_CATALOG else "relational"
    return CacheLocation(catalog, cache_schema, backend)


def org_cache_schema(state: Any, suffix: str = "_api_cache") -> str:  # REQ-1623, REQ-595
    """The schema the ACTING org's cache tables are written to, in the environment it is acting
    in: ``org_<id>[_env_<env>]<suffix>``. It is the acting org's and not the deployment's own
    (``state.org_id``), so an org's cache tables sit with the rest of its stores — counted
    against its quota, and dropped with the org or the environment. ``suffix`` is one of the
    org's store suffixes (``core.environments.SCHEMA_SUFFIXES``): rows fetched from an OpenAPI
    or a gRPC remote source are API cache tables and go to ``_api_cache``; a GraphQL remote's
    go to ``_gql_cache``."""
    from provisa.core.environments import active_org_schema
    from provisa.core.request_context import current_org

    return active_org_schema(current_org.get() or state.org_id, suffix)


def resolved_cache_catalog(engine: Any) -> str:  # REQ-318
    """The bound engine's cache catalog: a native/ephemeral engine (DuckDB) → its attached
    materialization store; a broad federator (Trino) → the writable ``provisa_admin`` config catalog.
    Hardcoding provisa_admin binder-errors on a native engine that never attaches it."""
    return engine.cache_catalog() or "provisa_admin"


def _scope() -> str:
    """The acting org, environment and loaded model: the scope every cache is kept under."""
    from provisa.cache import tenancy

    return tenancy.acting_scope()


def cache_table_name(  # REQ-318, REQ-309, REQ-327
    source_id: str, operation_id: str, native_args: dict
) -> str:
    """Stable table name for a given API call signature, in the acting org, environment and
    model.

    The scope is part of the name because the call alone does not say whose rows these are: a
    source id is an org's own, a branch may bind it to another host, and the endpoint's
    definition (path, response root, columns) is model that can change under the same
    operation. An endpoint definition is a model row, so changing it advances the model stamp
    and, with it, this name. Callers pass only the call; nothing else may name a cache table.
    """
    key = json.dumps(
        {
            "scope": _scope(),
            "s": source_id,
            "o": operation_id,
            "a": sorted(native_args.items()),
            "v": 3,
        },
        sort_keys=True,
    )
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    return f"r_{h}"


def _schema_ref(loc: CacheLocation, dialect: str) -> str:
    """``catalog.schema`` in ``dialect``. A name that is not a plain identifier is quoted with
    the dialect's own quoting; a plain one is left as written, so the engine folds its case as
    it always has."""
    return exp.Table(this=exp.to_identifier(loc.schema), db=exp.to_identifier(loc.catalog)).sql(
        dialect=dialect
    )


def _table_ref(
    loc: CacheLocation, table_name: str, dialect: str, *, with_catalog: bool = True
) -> str:
    """``catalog.schema."table"`` in ``dialect``: the table name always quoted, catalog and
    schema as :func:`_schema_ref` writes them. ``with_catalog=False`` gives ``schema."table"``,
    for a statement run on a connection to the store itself, where the engine's catalog name
    for that store means nothing."""
    return exp.Table(
        this=exp.to_identifier(table_name, quoted=True),
        db=exp.to_identifier(loc.schema),
        catalog=exp.to_identifier(loc.catalog) if with_catalog else None,
    ).sql(dialect=dialect)


def _string_literal(value: str, dialect: str) -> str:
    """``value`` as a string literal of ``dialect``, escaped by the dialect's own rules."""
    return exp.Literal.string(value).sql(dialect=dialect)


def _cell(value: Any) -> Any:
    """One response value as the cache stores it: NULL, a boolean and a number as themselves,
    an object or array as its JSON text, anything else as its text."""
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _literal(value: Any, dialect: str) -> str:
    """A cache value (:func:`_cell`) as a literal of ``dialect`` — for a connection whose
    backend declares no bind marker."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return _string_literal(value, dialect)


# Materialize-store backends a SECOND direct connection can safely reach to create a schema,
# alongside the engine's own connection — server processes with no single-writer constraint.
# DuckDB/SQLite are single-writer file stores (store_writer/store_connection.py) the engine holds
# open via its own attached connection; a second connection opening the same file races or breaks
# it, so any backend absent here is a hard, named failure rather than a silent attempt.
_DIRECT_WRITE_SAFE_STORE_BACKENDS = frozenset({"postgresql", "mysql", "mariadb"})


def _create_schema_directly_against_store(loc: CacheLocation) -> None:
    """REQ-1730: the connector for ``loc.catalog`` refuses to create schemas and ``loc.schema``
    does not exist yet — reach the underlying materialize store directly, the same technique
    ``TrinoBackend.refresh_landed_views`` uses to pre-create it at boot, rather than depend on that
    boot/reload hook having already run for this specific org before this cache lookup needed it.

    Only meaningful for a Postgres/MySQL/MariaDB-backed (``relational``) store — see
    ``_DIRECT_WRITE_SAFE_STORE_BACKENDS``. An ``iceberg`` location is a namespace over an
    external, reachable store (S3 + the Iceberg catalog) that the engine exposes via its OWN
    connector's ``CREATE SCHEMA ... WITH (location = ...)``; a single-writer file store (DuckDB/
    SQLite) must be written through the engine's own attached connection. Neither has a second
    connection this fallback can safely open, so a connector refusal there is a hard failure, not
    something this fallback can paper over."""
    if loc.backend != "relational":
        raise RuntimeError(
            f"ensure_cache_schema: connector for {loc.catalog!r} refused to create "
            f"{loc.schema!r}, and it is a {loc.backend!r} location — an external/reachable store "
            "namespace must be created through the engine's own connector, not by writing "
            "directly to a materialize store that does not own it"
        )
    from sqlalchemy import make_url
    from sqlalchemy.schema import CreateSchema

    from provisa.api.app import state
    from provisa.core.database import sync_engine_from_url

    dsn = state.federation_engine.engine.materialize_store()
    backend = make_url(dsn).get_backend_name()
    if backend not in _DIRECT_WRITE_SAFE_STORE_BACKENDS:
        raise RuntimeError(
            f"ensure_cache_schema: {loc.catalog!r}'s materialize store backend {backend!r} does "
            f"not support a second direct connection (only "
            f"{sorted(_DIRECT_WRITE_SAFE_STORE_BACKENDS)} do) — {loc.schema!r} must be created "
            "through the engine's own connection instead"
        )
    engine = sync_engine_from_url(dsn)
    try:
        with engine.begin() as sa_conn:
            sa_conn.execute(CreateSchema(loc.schema, if_not_exists=True))
    except Exception as direct_exc:
        raise RuntimeError(
            f"ensure_cache_schema: connector for {loc.catalog!r} cannot create schemas, and "
            f"direct creation of {loc.schema!r} against the materialize store also failed: "
            f"{direct_exc}"
        ) from direct_exc
    finally:
        engine.dispose()


def ensure_cache_schema(conn, loc: CacheLocation) -> None:  # REQ-318, REQ-309, REQ-327
    """REQ-1730: an engine's own connection should only ever READ the store — Trino's postgresql
    connector enforces this by refusing CREATE SCHEMA outright (NOT_SUPPORTED) for a catalog whose
    JDBC URL pins a ``currentSchema`` (e.g. ``provisa_admin``), regardless of whether the schema
    already exists. A ``CREATE SCHEMA IF NOT EXISTS`` through such a connection therefore ALWAYS
    fails on the very first call after a fresh process boot, even when something else (a direct
    Postgres connection, matching how ``reconcile_landed_tables``/``store_writer`` land tables)
    already created the schema moments earlier. On that refusal, fall back to a READ — listing
    schemas is something every connector supports — and treat an already-existing schema as
    success rather than a hard failure. When the read confirms the schema genuinely is missing
    (the boot/reload hook that normally pre-creates it has not run yet for this org), create it
    directly against the store ourselves instead of erroring out on a timing gap."""
    key = (loc.catalog, loc.schema)
    if key in _SCHEMA_EXISTS_CACHE:
        return
    if loc.backend == "iceberg":
        s3_location = _string_literal(f"s3a://{_ICEBERG_BUCKET}/{loc.schema}/", conn.dialect)
        sql = (
            f"CREATE SCHEMA IF NOT EXISTS {_schema_ref(loc, conn.dialect)} "
            f"WITH (location = {s3_location})"
        )
    else:
        sql = f"CREATE SCHEMA IF NOT EXISTS {_schema_ref(loc, conn.dialect)}"
    try:
        conn.execute(sql)
        conn.fetchall()
        _SCHEMA_EXISTS_CACHE.add(key)
        return
    except Exception as create_exc:
        if "NOT_SUPPORTED" not in str(create_exc):
            raise RuntimeError(
                f"ensure_cache_schema failed for {key}: {create_exc}"
            ) from create_exc

    try:
        schemata = exp.Table(
            this=exp.to_identifier("schemata"),
            db=exp.to_identifier("information_schema"),
            catalog=exp.to_identifier(loc.catalog),
        ).sql(dialect=conn.dialect)
        conn.execute(
            f"SELECT 1 FROM {schemata} "
            f"WHERE schema_name = {_string_literal(loc.schema, conn.dialect)}"
        )
        exists = bool(conn.fetchall())
    except Exception as read_exc:
        raise RuntimeError(
            f"ensure_cache_schema: connector refused to create {key} and could not verify it "
            f"exists either: {read_exc}"
        ) from read_exc
    if not exists:
        _create_schema_directly_against_store(loc)
    _SCHEMA_EXISTS_CACHE.add(key)


def table_known_live(loc: CacheLocation, table_name: str) -> bool:  # REQ-318, REQ-309, REQ-327
    """Return True if the in-process cache confirms this table is live — no the engine probe."""
    key = (loc.catalog, loc.schema, table_name)
    expiry = _TABLE_EXISTS_CACHE.get(key)
    return expiry is not None and time.monotonic() < expiry


def table_exists(  # REQ-318, REQ-309, REQ-327
    conn, loc: CacheLocation, table_name: str, ttl: int | None = None
) -> bool:
    key = (loc.catalog, loc.schema, table_name)
    expiry = _TABLE_EXISTS_CACHE.get(key)
    if expiry is not None and time.monotonic() < expiry:
        return True

    sql = f"SELECT 1 FROM {_table_ref(loc, table_name, conn.dialect)} LIMIT 1"
    try:
        conn.execute(sql)
        conn.fetchall()
        # Cache the positive result; expire before the engine drops the table
        if ttl is not None and ttl > _TABLE_EXISTS_SAFETY_MARGIN:
            _TABLE_EXISTS_CACHE[key] = time.monotonic() + ttl - _TABLE_EXISTS_SAFETY_MARGIN
        elif ttl is not None:
            _TABLE_EXISTS_CACHE[key] = time.monotonic() + max(ttl - 5, 1)
        else:
            # No TTL known — cache for 60s as a safe default
            _TABLE_EXISTS_CACHE[key] = time.monotonic() + 60
        return True
    except Exception as exc:
        _TABLE_EXISTS_CACHE.pop(key, None)
        log.debug(
            "[API CACHE] table_exists=False: %s.%s.%r — %s",
            loc.catalog,
            loc.schema,
            table_name,
            exc,
        )
        return False


_IR_TO_SQLALCHEMY: dict[str, Any] = {
    "VARCHAR": "String",
    "BIGINT": "BigInteger",
    "DOUBLE": "Float",
    "BOOLEAN": "Boolean",
}


def _create_and_insert_directly_against_store(
    loc: CacheLocation, table_name: str, rows: list[dict], columns: list
) -> None:
    """REQ-1730: the same self-heal as ``_create_schema_directly_against_store``, one level deeper
    — Trino's postgresql connector refuses CREATE TABLE (NOT_SUPPORTED) for a catalog whose JDBC
    URL pins a ``currentSchema``, just as it refuses CREATE SCHEMA, and for the same reason. Build
    and land the cache table directly against the materialize store instead of through the
    connector that cannot write it."""
    if loc.backend != "relational":
        raise RuntimeError(
            f"ensure_cache_schema: connector for {loc.catalog!r} refused to create table "
            f"{table_name!r}, and it is a {loc.backend!r} location — an external/reachable store "
            "table must be created through the engine's own connector, not by writing directly "
            "to a materialize store that does not own it"
        )
    from sqlalchemy import Boolean, Column, MetaData, String, Table, insert, make_url
    from sqlalchemy import BigInteger, Float

    from provisa.api.app import state
    from provisa.core.database import sync_engine_from_url

    dsn = state.federation_engine.engine.materialize_store()
    backend = make_url(dsn).get_backend_name()
    if backend not in _DIRECT_WRITE_SAFE_STORE_BACKENDS:
        raise RuntimeError(
            f"ensure_cache_schema: {loc.catalog!r}'s materialize store backend {backend!r} does "
            f"not support a second direct connection (only "
            f"{sorted(_DIRECT_WRITE_SAFE_STORE_BACKENDS)} do) — table {table_name!r} must be "
            "created through the engine's own connection instead"
        )
    sa_types = {"String": String, "BigInteger": BigInteger, "Float": Float, "Boolean": Boolean}

    def _column_type(col):
        raw = col.type.value if hasattr(col.type, "value") else str(col.type)
        ir = _API_TYPE_TO_IR.get(raw, "VARCHAR")
        return sa_types[_IR_TO_SQLALCHEMY[ir]]()

    md = MetaData(schema=loc.schema)
    tbl = Table(table_name, md, *[Column(c.name, _column_type(c)) for c in columns])
    engine = sync_engine_from_url(dsn)
    try:
        with engine.begin() as sa_conn:
            tbl.create(sa_conn, checkfirst=True)
            if rows:
                col_names = [c.name for c in columns]
                sa_conn.execute(insert(tbl), [{k: r.get(k) for k in col_names} for r in rows])
    except Exception as direct_exc:
        raise RuntimeError(
            f"ensure_cache_schema: connector for {loc.catalog!r} cannot create tables, and "
            f"direct creation of {table_name!r} against the materialize store also failed: "
            f"{direct_exc}"
        ) from direct_exc
    finally:
        engine.dispose()
    log.info(
        '[API CACHE] materialized %d rows → %s.%s."%s" (direct)',
        len(rows),
        loc.catalog,
        loc.schema,
        table_name,
    )


def create_and_insert(  # REQ-318, REQ-309, REQ-327, REQ-280
    conn, loc: CacheLocation, table_name: str, rows: list[dict], columns: list
) -> None:
    """Create cache table and INSERT API response rows."""

    def _column_type(col) -> str:
        raw = col.type.value if hasattr(col.type, "value") else str(col.type)
        return _API_TYPE_TO_IR.get(raw, "VARCHAR")

    dialect = conn.dialect
    col_defs = ", ".join(
        f"{exp.to_identifier(c.name, quoted=True).sql(dialect=dialect)} {_column_type(c)}"
        for c in columns
    )
    ref = _table_ref(loc, table_name, dialect)

    if loc.backend == "iceberg":
        s3_location = _string_literal(
            f"s3a://{_ICEBERG_BUCKET}/{loc.schema}/{table_name}/", dialect
        )
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {ref} "
            f"({col_defs}) "
            f"WITH (format = 'PARQUET', location = {s3_location})"
        )
    else:
        create_sql = f"CREATE TABLE IF NOT EXISTS {ref} ({col_defs})"
    try:
        conn.execute(create_sql)
        conn.fetchall()
    except Exception as create_exc:
        if "NOT_SUPPORTED" not in str(create_exc):
            raise
        _create_and_insert_directly_against_store(loc, table_name, rows, columns)
        return

    if not rows:
        return

    col_names = [c.name for c in columns]

    # A response value is data. Where the connection's driver binds values it is bound;
    # where the backend declares no bind marker it is a literal the dialect itself escapes.
    placeholder = conn.placeholder

    def _do_inserts() -> None:
        for i in range(0, max(len(rows), 1), 500):
            batch = rows[i : i + 500]
            if not batch:
                break
            if placeholder is None:
                vals = ", ".join(
                    "(" + ", ".join(_literal(_cell(r.get(c)), dialect) for c in col_names) + ")"
                    for r in batch
                )
                conn.execute(f"INSERT INTO {ref} VALUES {vals}")
            else:
                row_marks = "(" + ", ".join([placeholder] * len(col_names)) + ")"
                conn.execute(
                    f"INSERT INTO {ref} VALUES " + ", ".join([row_marks] * len(batch)),
                    [_cell(r.get(c)) for r in batch for c in col_names],
                )
            conn.fetchall()

    try:
        _do_inserts()
    except Exception as exc:
        if "TYPE_MISMATCH" in str(exc):
            # Stale cache table has wrong schema — drop and recreate
            conn.execute(f"DROP TABLE IF EXISTS {ref}")
            conn.fetchall()
            conn.execute(create_sql.replace("IF NOT EXISTS ", ""))
            conn.fetchall()
            _do_inserts()
        else:
            raise

    # REQ-1688: statistics are collected by ``analyze_cache_table`` where the table lives — the
    # caller awaits it after this insert; the engine's ANALYZE is not the store's.
    log.info(
        '[API CACHE] materialized %d rows → %s.%s."%s"',
        len(rows),
        loc.catalog,
        loc.schema,
        table_name,
    )


def _land_columns(columns: list) -> list[tuple[str, str]]:
    """API endpoint columns → (name, sql_type) pairs for the write face. The api type enum maps to
    an IR SQL type the store_writer type map understands (VARCHAR/BIGINT/DOUBLE/BOOLEAN)."""
    out: list[tuple[str, str]] = []
    for c in columns:
        raw = c.type.value if hasattr(c.type, "value") else str(c.type)
        out.append((c.name, _API_TYPE_TO_IR.get(raw, "VARCHAR")))
    return out


async def analyze_cache_table(engine, loc: CacheLocation, table_name: str) -> None:  # REQ-280
    """Collect planner statistics on the landed cache table where it lives (REQ-1688): the engine
    runtime dispatches to the store's own connection, or to the engine when the engine is the
    store's analyzer. Best-effort by design (REQ-275): a query must not fail because statistics
    could not be collected, so the reason is logged and the query proceeds."""
    try:
        await engine.analyze_landed_table(catalog=loc.catalog, schema=loc.schema, table=table_name)
    except Exception as exc:  # allow-blind-except: REQ-275 mandates best-effort statistics
        log.warning(
            "[API CACHE] statistics for %s.%s.%s not collected: %s",
            loc.catalog,
            loc.schema,
            table_name,
            exc,
        )


async def land_api_cache(  # REQ-318, REQ-848, REQ-932, REQ-989
    engine, loc: CacheLocation, table_name: str, rows: list[dict], columns: list
) -> None:
    """Land API-response rows into the cache table through the ONE write face
    (``EngineRuntime.land_source_table``), then ANALYZE via the engine. The engine NEVER writes the
    store directly — it only reads the landed table back through its attach (``loc.catalog``); the
    write face itself routes through the engine's own connection for a single-writer store (DuckDB,
    REQ-989) or the store_writer DSN path otherwise. An Iceberg-backed cache is written by the
    pyiceberg branch of the same abstraction (not yet implemented); there is no engine-write
    fallback."""
    if loc.backend == "iceberg":
        raise NotImplementedError(
            "Iceberg api-cache landing requires the pyiceberg write-face branch (REQ-848); "
            "the engine must not write the store"
        )
    await engine.land_source_table(
        schema=loc.schema,
        table=table_name,
        columns=_land_columns(columns),
        rows=rows,
    )
    await analyze_cache_table(engine, loc, table_name)
    log.info(
        '[API CACHE] materialized %d rows → %s.%s."%s"',
        len(rows),
        loc.catalog,
        loc.schema,
        table_name,
    )


def rewrite_from_cache(
    sql: str, loc: CacheLocation, table_name: str, alias_name: str | None = None
) -> str:  # REQ-318, REQ-309, REQ-327
    """Replace the root FROM table in SQL with the cache table.

    ``alias_name`` is the name column qualifiers in the surrounding query actually use (e.g.
    the table's registered semantic/display name) when it differs from the ref's current name —
    by this point an earlier semantic-to-physical rewrite has already swapped the FROM ref's own
    name, so falling back to the (now-physical) ``tbl.name`` would not match those qualifiers.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
        for tbl in tree.find_all(exp.Table):
            # Preserve the original table name as an alias when the ref is
            # unaliased, so column qualifiers still resolve after the relation
            # is renamed to the cache table (mirrors rewrite_all_from_cache).
            if not tbl.alias:
                tbl.set("alias", exp.TableAlias(this=exp.to_identifier(alias_name or tbl.name)))
            tbl.set("catalog", exp.to_identifier(loc.catalog))
            tbl.set("db", exp.to_identifier(loc.schema))
            tbl.set("this", exp.to_identifier(table_name, quoted=True))
            break
        return tree.sql(dialect="postgres")
    except Exception as exc:
        log.warning("rewrite_from_cache SQLGlot failed: %s", exc)

    import re

    cache_ref = _table_ref(loc, table_name, "postgres")
    return re.sub(
        r'FROM\s+"[^"]*"\."[^"]*"(?:\."[^"]*")?',
        lambda _m: f"FROM {cache_ref}",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )


def rewrite_all_from_cache(  # REQ-318, REQ-309, REQ-327
    sql: str,
    cache_rewrites: dict[str, tuple["CacheLocation", str]],
) -> str:
    """Replace ALL API-backed table references with their cache table equivalents.

    cache_rewrites maps physical table name (tbl.name) → (CacheLocation, cache_table_name).
    All matching tables in FROM/JOIN clauses are rewritten; unmatched tables are left as-is.
    """
    if not cache_rewrites:
        return sql
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
        for tbl in tree.find_all(exp.Table):
            if tbl.name in cache_rewrites:
                orig_name = tbl.name
                loc, cache_tbl = cache_rewrites[orig_name]
                # Preserve the original table name as an alias when the ref is
                # unaliased, so column qualifiers (e.g. shelter__animalBreeds.name)
                # still resolve after the relation is renamed to the cache table.
                if not tbl.alias:
                    tbl.set("alias", exp.TableAlias(this=exp.to_identifier(orig_name)))
                tbl.set("catalog", exp.to_identifier(loc.catalog))
                tbl.set("db", exp.to_identifier(loc.schema))
                tbl.set("this", exp.to_identifier(cache_tbl, quoted=True))
        return tree.sql(dialect="postgres")
    except Exception as exc:
        log.warning("rewrite_all_from_cache SQLGlot failed: %s", exc)

    import re

    result = sql
    for orig_tbl, (loc, cache_tbl) in cache_rewrites.items():
        cache_ref = _table_ref(loc, cache_tbl, "postgres")
        result = re.sub(
            rf'"[^"]*"\."[^"]*"\."{re.escape(orig_tbl)}"',
            lambda _m, _ref=cache_ref: _ref,
            result,
            flags=re.IGNORECASE,
        )
    return result


def schedule_drop(  # REQ-318, REQ-309, REQ-327
    engine,
    loc: CacheLocation,
    table_name: str,
    ttl: int,
    redirect_config=None,
) -> None:
    """Drop the cache table once ``ttl`` seconds have passed.

    REQ-1882: the delay is held by the background timer thread, not by a sleeping task — the drop
    runs on a background worker at expiry, acquiring a fresh engine connection then."""
    from provisa.core.connection_loop import spawn_after

    spawn_after(
        ttl,
        drop_cache_table(engine, loc, table_name, ttl, redirect_config),
        name=f"cache-drop:{table_name}",
    )


async def drop_cache_table(  # REQ-318, REQ-309, REQ-327
    engine,
    loc: CacheLocation,
    table_name: str,
    ttl: int,
    redirect_config=None,
) -> None:
    """Drop a cache table whose TTL expired (see :func:`schedule_drop`)."""
    _TABLE_EXISTS_CACHE.pop((loc.catalog, loc.schema, table_name), None)
    try:
        with engine.isolated_sync() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {_table_ref(loc, table_name, conn.dialect)}")
            conn.fetchall()
        log.info("[API CACHE] dropped %s after TTL=%ds", table_name, ttl)
    except Exception as exc:
        log.warning("[API CACHE] drop failed for %s: %s", table_name, exc)
    if loc.backend == "iceberg" and redirect_config is not None:
        from provisa.executor.redirect import cleanup_s3_prefix

        s3_prefix = f"s3a://{_ICEBERG_BUCKET}/{loc.schema}/{table_name}/"
        await cleanup_s3_prefix(s3_prefix, redirect_config)
