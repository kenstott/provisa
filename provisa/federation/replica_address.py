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


def replica_schema(org_id: str) -> str:
    """The schema that holds the replicas of ``org_id`` in the environment being served."""
    from provisa.core.environments import active_org_schema

    return active_org_schema(org_id, REPLICAS_SUFFIX)


def mv_schema(org_id: str) -> str:
    """The schema that holds the materialized views of ``org_id`` in the environment served."""
    from provisa.core.environments import active_org_schema

    return active_org_schema(org_id, MVS_SUFFIX)


def export_schema(org_id: str) -> str:
    """The schema that holds the export views of ``org_id``'s replicas in the environment served."""
    from provisa.core.environments import active_org_schema

    return active_org_schema(org_id, EXPORT_SUFFIX)


def active_org_id(state: Any) -> str:
    """The org a store address is named for: the one bound to this request, else the boot org —
    the runtime ``state`` resolves to when no org is bound (``AppState._active_runtime``)."""
    from provisa.core.request_context import current_org

    return current_org.get() or state.org_id


def replica_address(
    *, org_id: str, source_id: str, schema_name: str, table_name: str
) -> ReplicaAddress:
    """The address of the replica of ``source_id``'s table ``schema_name.table_name``, for
    ``org_id`` in the environment being served. One rule on every engine: the engine decides only
    which store the address is in (``FederationEngine.materialize_store``, which refuses a store
    with no schemas). Pure — no store is opened and nothing is read."""
    return ReplicaAddress(
        replica_schema(org_id), replica_table_name(source_id, schema_name, table_name)
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
_REPLICAS_SCHEMA = re.compile(_SURFACE_OWNER + re.escape(REPLICAS_SUFFIX))
_WRITE_SURFACE = re.compile(
    _SURFACE_OWNER
    + "(?:"
    + "|".join(re.escape(s) for s in (REPLICAS_SUFFIX, MVS_SUFFIX, EXPORT_SUFFIX))
    + ")"
)


def is_replicas_schema(schema: str) -> bool:
    """Whether ``schema`` is named as the replicas schema of an org environment — the only kind
    of schema a replica is written into."""
    return _REPLICAS_SCHEMA.fullmatch(schema) is not None


def is_write_surface(schema: str) -> bool:
    """Whether ``schema`` is named as a schema Provisa writes replicas, materialized views or
    export views into — one that holds nothing else, so a live attach never creates anything in
    it."""
    return _WRITE_SURFACE.fullmatch(schema) is not None
