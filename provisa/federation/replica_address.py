# Copyright (c) 2026 Kenneth Stott
# Canary: 41e5e2bc-ee6a-4617-a292-8065768d9e2f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a replica lives, and how a read reaches it there (REQ-1912).

A replica is the copy of a source table Provisa keeps in an engine's store. Every replica of an
org's environment lives in one schema that holds nothing else, ``org_<org>[_env_<env>]_replicas``,
under one name, ``<source>__<schema>__<table>`` — on every engine. Materialized views have a
schema of their own (``…_mv_cache``). Neither schema is shared with a source's live attach.

A read is served from a replica by ADDRESSING it there. Nothing is created at the table's
registered name to stand in for it: that name belongs to the live attach, and a replica that
shared it was once refreshed through the live view into the source database.

Two faces:

* the WRITER asks :func:`replica_address` for the ``(schema, table)`` it writes;
* the READER's statement, already lowered to the engine's own table names, passes through
  :func:`address_replicas`, which renames each table that is served from its replica to that
  same address. The set of such tables is the published :class:`ReplicaRoutes`.
"""

# Requirements: REQ-1912, REQ-826

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.federation.engine import FederationEngine

#: The store schemas Provisa writes, per org and environment (``core.environments``).
REPLICAS_SUFFIX = "_replicas"
MVS_SUFFIX = "_mv_cache"
#: The schema a store publishes a view of each replica in, for a catalog export that shares and
#: tags one (Snowflake's Horizon export). It holds those views and nothing else; no statement of
#: Provisa's reads through it.
EXPORT_SUFFIX = "_export"
#: REQ-1939: what separates an environment's schema name from one of its synthetic datasets'.
SYNTHETIC_INFIX = "_syn__"

#: PostgreSQL's identifier limit; over it PostgreSQL truncates a name silently, which would point
#: two replicas at one table. The one naming rule keeps every replica name within it.
_MAX_NAME_BYTES = 63
_DIGEST_CHARS = 16


class StoreHasNoSchemas(Exception):
    """The materialization store has a single namespace (REQ-1912)."""

    def __init__(self, dsn_scheme: str) -> None:
        self.dsn_scheme = dsn_scheme
        super().__init__(
            f"the materialization store is a {dsn_scheme!r} database, which has no schemas. "
            "Replicas and materialized views are each written to a schema that holds nothing "
            "else, so the store must be one that has schemas (PostgreSQL, DuckDB, or the "
            "warehouse itself). Set materialize_store_url / $PROVISA_MATERIALIZE_URL to one."
        )


def _scheme(dsn: str) -> str:
    return dsn.partition("://")[0].split("+", 1)[0]


def require_schema_capable_store(dsn: str) -> None:
    """Refuse a store with no schemas. SQLite is the one such store Provisa can write."""
    scheme = _scheme(dsn)
    if scheme == "sqlite":
        raise StoreHasNoSchemas(scheme)


@dataclass(frozen=True)
class ReplicaAddress:
    """Where one replica is written and read: a table of the store's replicas schema."""

    schema: str
    table: str


def replica_table_name(source_id: str, schema_name: str, table_name: str) -> str:
    """The one name of a table's replica, on every engine: source, schema and table joined.

    A joined name longer than PostgreSQL's identifier limit keeps its head and ends in a digest of
    the whole, so two long names never fold into one table."""
    joined = f"{source_id}__{schema_name}__{table_name}"
    if len(joined.encode()) <= _MAX_NAME_BYTES:
        return joined
    digest = hashlib.sha256(joined.encode()).hexdigest()[:_DIGEST_CHARS]
    head = joined.encode()[: _MAX_NAME_BYTES - _DIGEST_CHARS - 2].decode(errors="ignore")
    return f"{head}__{digest}"


def replica_schema(org_id: str, *, region: str | None = None) -> str:
    """The schema that holds the replicas of ``org_id`` in the environment being served — this
    node's region's, or ``region``'s (REQ-1922: where another region's replica is read)."""
    from provisa.core.environments import active_org_schema

    return active_org_schema(org_id, REPLICAS_SUFFIX, region=region)


def synthetic_schema(org_id: str, env: str | None, dataset: str) -> str:
    """The store schema of synthetic dataset ``dataset`` of ``org_id``'s environment ``env``."""
    from provisa.core.environments import org_schema

    name = f"{org_schema(org_id, env)}{SYNTHETIC_INFIX}{dataset}"
    if len(name.encode()) > _MAX_NAME_BYTES:
        raise ValueError(
            f"synthetic dataset {dataset!r}: its store schema {name!r} is longer than "
            f"{_MAX_NAME_BYTES} bytes; choose a shorter name"
        )
    return name


def mv_schema(org_id: str, *, region: str | None = None) -> str:
    """The schema that holds the materialized views of ``org_id`` in the environment served —
    this node's region's, or ``region``'s (REQ-1921: where a view that names it is read)."""
    from provisa.core.environments import active_org_schema

    return active_org_schema(org_id, MVS_SUFFIX, region=region)


def export_schema(org_id: str) -> str:
    """The schema that holds the export views of ``org_id``'s replicas in the environment served."""
    from provisa.core.environments import active_org_schema

    return active_org_schema(org_id, EXPORT_SUFFIX)


def active_org_id(state: Any) -> str:
    """The org a store address is named for: the one bound to this work, as
    ``AppState._active_runtime`` resolves it; refused when none is bound (REQ-1266)."""
    from provisa.core.request_context import require_current_org

    return require_current_org()


def region_read_name(state: Any, region: Any) -> str:
    """The name this org environment's engine reads ``region``'s replicas store by (REQ-1922):
    ``naming.region_read_catalog`` for the org and environment served — and its views store
    (REQ-1921), a store of its own, by that name with ``__views``."""
    from provisa.compiler.naming import region_read_catalog
    from provisa.core.request_context import active_env

    name = region_read_catalog(
        active_org_id(state), region.id, default_org=state.org_id, env=active_env()
    )
    return f"{name}__views" if region.reads == "views" else name


def replica_address(
    *,
    org_id: str,
    source_id: str,
    schema_name: str,
    table_name: str,
    region: str | None = None,
) -> ReplicaAddress:
    """The address of the replica of ``source_id``'s table ``schema_name.table_name``, for
    ``org_id`` in the environment being served — in this node's region, or ``region``'s (REQ-1922:
    another region's replica, read where that region wrote it). One rule on every engine: the
    engine decides only which store the address is in (``FederationEngine.materialize_store``,
    which refuses a store with no schemas). Pure — no store is opened and nothing is read."""
    return ReplicaAddress(
        replica_schema(org_id, region=region),
        replica_table_name(source_id, schema_name, table_name),
    )


def export_view_address(
    *, org_id: str, source_id: str, schema_name: str, table_name: str
) -> ReplicaAddress:
    """The address of the view a catalog export publishes over the replica of ``source_id``'s
    table: the export schema, under the replica's own name. It is not the table's registered
    address (that belongs to the live attach) and it is in neither schema Provisa reads from, so
    no read is ever answered through it (REQ-1912). Pure."""
    return ReplicaAddress(
        export_schema(org_id), replica_table_name(source_id, schema_name, table_name)
    )


# -- the read side ---------------------------------------------------------------------------------

#: A table as the engine addresses it in a lowered statement: ``(catalog | None, schema, table)``.
TableKey = tuple[str | None, str, str]


@dataclass(frozen=True)
class ReplicaRoute:
    """One table that is served from its replica: where its replica is read."""

    source_id: str
    table_name: str
    target: TableKey


class AmbiguousReplica(RuntimeError):
    """Two registered tables share one engine name and both are served from a replica."""

    def __init__(self, key: TableKey, sources: list[str]) -> None:
        self.key = key
        self.sources = sources
        name = ".".join(f'"{p}"' for p in key if p is not None)
        super().__init__(
            f"{name} names tables of more than one source on this engine ({', '.join(sources)}); "
            "each has its own replica, so a read of that name cannot be answered. Register the "
            "tables under different schema or table names."
        )


@dataclass(frozen=True)
class ReplicaRoutes:
    """The tables of one org environment that are served from their replica on one engine, by the
    name a lowered statement gives them. Published with the registry; read on every engine
    statement."""

    engine_name: str = ""
    routes: Mapping[TableKey, ReplicaRoute] = field(default_factory=dict)
    ambiguous: Mapping[TableKey, tuple[str, ...]] = field(default_factory=dict)
    #: (source_id, table_name) -> why its replica could not be reconciled (REQ-826).
    unreconciled: Mapping[tuple[str, str], BaseException] = field(default_factory=dict)
    #: registered table id -> (source_id, operator setting) for every table whose reads the
    #: operator's settings put on its replica (REQ-030, ``replica_routing.floored_tables``). A
    #: statement is floored by the tables it reads, never by its source's other tables.
    floored: Mapping[int, tuple[str, str]] = field(default_factory=dict)
    #: registered table id -> source_id for every other registered table: no operator setting
    #: puts its reads on a replica, so an engine that reads its source in place reads it live.
    #: With ``floored`` it says, for one statement, which of its sources it reads live.
    unfloored: Mapping[int, str] = field(default_factory=dict)
    #: The tables that passed their Hot threshold (REQ-826), by (source_id, schema, table), and
    #: those of them whose replica exists and serves their reads. A promoted table that is not
    #: yet serving is read live while its replica is built. What the admin summary states.
    promoted: frozenset[tuple[str, str, str]] = frozenset()
    serving: frozenset[tuple[str, str, str]] = frozenset()
    #: REQ-1939: registered table id -> the synthetic dataset whose generated copy this
    #: environment reads in its place. Such a table is routed to its copy and floored (never read
    #: live), is never built or landed, and is never joined to a table reading real data.
    synthetic: Mapping[int, str] = field(default_factory=dict)
    #: How many times this runtime's routes have CHANGED since it was built (REQ-826). Not part
    #: of what the routes say (two publications that say the same are equal whatever their
    #: generation): it is part of the routing-cache key, so a cached route never outlives the
    #: routes it was decided under. Advanced by ``model_reload.publish_replica_routes``.
    generation: int = field(default=0, compare=False)

    def __bool__(self) -> bool:
        return bool(self.routes) or bool(self.ambiguous)


def address_replicas(pg_sql: str, routes: ReplicaRoutes) -> str:
    """Rename each table of ``pg_sql`` that is served from its replica to its replica's address.

    ``pg_sql`` is PostgreSQL-dialect SQL whose tables are already in the engine's own addressing
    (catalog-physical, the catalog folded into the schema where the engine has none). A table's
    alias is kept — the lowering pins one on every table — so the statement's column references
    still bind. A table read live is left exactly as written.

    Raises ``ReplicaUnavailable`` for a table whose replica could not be reconciled (it has no
    replica a read may be answered from), and ``AmbiguousReplica`` for a name two sources share.
    """
    if not routes or not _names_a_routed_table(pg_sql, routes):
        return pg_sql
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(pg_sql, read="postgres")
    changed = False
    for tbl in tree.find_all(exp.Table):
        if not tbl.name:
            continue
        key: TableKey = (tbl.catalog or None, tbl.db, tbl.name)
        target = read_address(key, routes)
        if target == key:
            continue
        catalog, schema, table = target
        if not tbl.alias:
            # Column references written against the table's own name keep binding to it.
            tbl.set("alias", exp.TableAlias(this=exp.to_identifier(tbl.name, quoted=True)))
        tbl.set("this", exp.to_identifier(table, quoted=True))
        tbl.set("db", exp.to_identifier(schema, quoted=True))
        tbl.set("catalog", exp.to_identifier(catalog, quoted=True) if catalog else None)
        changed = True
    return tree.sql(dialect="postgres") if changed else pg_sql


def read_address(key: TableKey, routes: ReplicaRoutes) -> TableKey:
    """Where the table an engine statement names ``key`` is read: its replica's address when it
    is served from its replica, ``key`` itself when it is read live.

    The one lookup :func:`address_replicas` applies to each table of a query; a caller whose
    statement is not a query the rewrite parses (``ANALYZE``, a catalog listing of one table)
    asks it directly. Raises exactly what the rewrite raises."""
    if key in routes.ambiguous:
        raise AmbiguousReplica(key, list(routes.ambiguous[key]))
    route = routes.routes.get(key)
    if route is None:
        return key
    cause = routes.unreconciled.get((route.source_id, route.table_name))
    if cause is not None:
        from provisa.federation.replica_guard import ReplicaUnavailable

        raise ReplicaUnavailable(route.source_id, route.table_name, cause) from cause
    return route.target


def _names_a_routed_table(pg_sql: str, routes: ReplicaRoutes) -> bool:
    """Whether the statement's text names any routed table at all — the parse is skipped for the
    statement that reads only live tables. A name inside a literal costs one parse, nothing more."""
    return any(key[2] in pg_sql for key in routes.routes) or any(
        key[2] in pg_sql for key in routes.ambiguous
    )


def engine_table_keys(
    engine: FederationEngine, catalog: str, schema_name: str, table_name: str
) -> tuple[TableKey, ...]:
    """A registered table as a lowered statement names it on ``engine``: its three-part
    catalog-physical name, and — on an engine whose SQL has no catalog (``catalog_qualified``
    False) — also the catalog folded into the schema, exactly as
    ``sql_rewrite.fold_catalog_into_schema`` folds it. A statement reaches the address pass in
    either form there: folded by the query pipeline, three-part from a stored view body."""
    physical: TableKey = (catalog, schema_name, table_name)
    if engine.catalog_qualified:
        return (physical,)
    return (physical, (None, f"{catalog}_{schema_name}", table_name))


# ``org_<id>[_env_<env>]_replicas`` / ``…_mv_cache`` / ``…_export``. The owner part holds no
# double underscore:
# a live attach's folded schema of any org but the boot org is ``org_<id>__<source>_<schema>``
# (``naming.org_prefixed_catalog``), so no source schema — whatever it is called — folds to a
# name these match.
_SURFACE_OWNER = r"org_(?:(?!__).)+?"
# REQ-1939: a synthetic dataset's own schema, ``org_<id>[_env_<env>]_syn__<dataset>``. The owner
# holds no double underscore and a live attach's folded schema has one right after the org id, so
# neither is read as the other.
_SYNTHETIC = re.escape(SYNTHETIC_INFIX) + r"[a-z][a-z0-9_]*"
_REPLICAS_SCHEMA = re.compile(
    _SURFACE_OWNER + "(?:" + re.escape(REPLICAS_SUFFIX) + "|" + _SYNTHETIC + ")"
)
_WRITE_SURFACE = re.compile(
    _SURFACE_OWNER
    + "(?:"
    + "|".join(re.escape(s) for s in (REPLICAS_SUFFIX, MVS_SUFFIX, EXPORT_SUFFIX))
    + "|"
    + _SYNTHETIC
    + ")"
)


def is_replicas_schema(schema: str) -> bool:
    """Whether ``schema`` is named as the replicas schema of an org environment, or as one of its
    synthetic datasets' schemas (REQ-1939) — the only kinds of schema a replica, a whole copy a
    table's reads are served from, is written into."""
    return _REPLICAS_SCHEMA.fullmatch(schema) is not None


def is_write_surface(schema: str) -> bool:
    """Whether ``schema`` is named as a schema Provisa writes replicas, materialized views or
    export views into — one that holds nothing else, so a live attach never creates anything in
    it."""
    return _WRITE_SURFACE.fullmatch(schema) is not None
