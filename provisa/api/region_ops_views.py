# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ops views over what a region keeps, on a node of a region (REQ-1922, REQ-1386).

An ops view reads the org's model, its record (the statements it served: ``query_audit_log``,
``sla_breach_log``) or its state (its replica builds). With no platform regions all three are one
database and every ops view is a view there (``startup_seed._seed_ops_domain``). In a region the
record and the state are kept in the region's own stores, so a view of the model's database over
them reads tables no row is written to.

There, each ops view is read where its tables are:

* one that reads one store only -- the record, or the state -- is laid out in that store, as
  written (:func:`lay_out`);
* one that also reads the model (a table's name, a user's) is the engine's: its SQL, each table
  read from the catalog the engine attaches its store under (:func:`engine_view_sql`).

Every one of them is read under :data:`CATALOG` on such a node (:func:`table_catalogs`), whose
views the engine defines over its attaches (``DuckDBFederationRuntime.attach_region_ops``).
"""

# Requirements: REQ-1922, REQ-1386, REQ-884

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp

if TYPE_CHECKING:
    from collections.abc import Iterable

    from provisa.compiler.sql_types import CompilationContext
    from provisa.core.database import Database

#: The catalog a region's node reads these ops views under.
CATALOG = "provisa_region_ops"

MODEL = "model"
RECORD = "record"
STATE = "state"


@cache
def _definitions() -> dict[str, str]:
    """Every ops view's DDL, by the view's name, in the order the model's seed creates them."""
    from provisa.api._meta_views import (
        _OPS_LOG_TABLE_VIEWS,
        _OPS_REPORT_VIEWS,
        _ops_table_usage_ddl,
    )

    ddls = [*_OPS_LOG_TABLE_VIEWS.values(), _ops_table_usage_ddl(), *_OPS_REPORT_VIEWS.values()]
    return {sqlglot.parse_one(ddl, read="postgres").this.name: ddl for ddl in ddls}


def _reads(ddl: str) -> set[str]:
    tree = sqlglot.parse_one(ddl, read="postgres")
    own = tree.this.name
    ctes = {cte.alias for cte in tree.find_all(exp.CTE)}
    return {t.name for t in _relations(tree) if t.name not in ctes and t.name != own}


def _relations(tree: exp.Expr) -> list[exp.Table]:
    """The relations ``tree`` reads: its tables, not a table function (``jsonb_array_elements_text``
    over a column) read in their place."""
    return [t for t in tree.find_all(exp.Table) if isinstance(t.this, exp.Identifier)]


@cache
def _sides() -> dict[str, frozenset[str]]:
    """The stores each ops view reads, through the views it reads."""
    from provisa.core.store_sides import side_of

    definitions = _definitions()
    sides: dict[str, frozenset[str]] = {}

    def of(name: str) -> frozenset[str]:
        if name in sides:
            return sides[name]
        if name in definitions:
            found: set[str] = set()
            for read in _reads(definitions[name]):
                found |= of(read)
            sides[name] = frozenset(found)
            return sides[name]
        try:
            return frozenset({side_of(name)})
        except KeyError:
            return frozenset({MODEL})  # a meta view the model's seed creates there (tags_meta)

    for name in definitions:
        of(name)
    return sides


def store_views(side: str) -> list[str]:
    """The ops views that read only ``side`` (the record, or the state), in creation order."""
    return [name for name, read in _sides().items() if read == {side}]


def engine_views() -> list[str]:
    """The ops views that read the model and a region's store, in creation order."""
    return [name for name, read in _sides().items() if MODEL in read and read & {RECORD, STATE}]


def region_views() -> frozenset[str]:
    """Every ops view a region's node reads under :data:`CATALOG`."""
    return frozenset([*store_views(RECORD), *store_views(STATE), *engine_views()])


#: The source the ops views are registered under.
OPS_SOURCE = "provisa-admin"


def table_catalogs() -> dict[tuple[str, str], str]:
    """The catalog each of these views is read under, by (source, physical name), on a node of a
    region; empty on a node of no region, which reads them from the model's database."""
    from provisa.core import process_region
    from provisa.core.regions import DEFAULT_REGION

    if process_region.region() == DEFAULT_REGION:
        return {}
    return {(OPS_SOURCE, name): CATALOG for name in region_views()}


async def lay_out(db: "Database", side: str) -> None:
    """Create, in the region's ``side`` store ``db`` is a handle on, the ops views that read only
    that store, as the model's seed creates every ops view in a deployment with no regions."""
    from provisa.api.startup_seed import _seed_view
    from provisa.core.database import Database

    layout = Database(db.engine, name="org-layout", search_path=db.search_path)
    definitions = _definitions()
    async with layout.acquire() as conn:
        for name in store_views(side):
            await _seed_view(conn, definitions[name])


def engine_view_sql(name: str, *, catalogs: dict[str, tuple[str, str]]) -> str:
    """``name``'s SELECT for the engine (DuckDB), each table it reads named in full: ``catalogs``
    gives the ``(catalog, schema)`` the engine reads each side under -- the model's, the record
    store's, the state store's -- and :data:`CATALOG` under the model's schema for these views."""
    from provisa.core.store_sides import side_of

    definitions = _definitions()
    tree = sqlglot.parse_one(definitions[name], read="postgres")
    select = tree.expression
    ctes = {cte.alias for cte in select.find_all(exp.CTE)}
    for table in _relations(select):
        if table.name in ctes:
            continue
        if table.name in definitions:
            read = _sides()[table.name]
            side = next(iter(read)) if len(read) == 1 else None
            catalog, schema = (
                catalogs[side] if side in (RECORD, STATE) else (CATALOG, catalogs[MODEL][1])
            )
        else:
            try:
                side = side_of(table.name)
            except KeyError:
                side = MODEL
            catalog, schema = catalogs[side]
        table.set("catalog", exp.to_identifier(catalog, quoted=True))
        table.set("db", exp.to_identifier(schema, quoted=True))
    return select.sql(dialect="duckdb")


def engine_attachment(state: object) -> tuple[str, dict[str, str], dict[str, str]] | None:
    """What the engine attaches and defines to read these views for the bound org, on a node of
    a region: the model's schema they are read under, the region's record and state stores by the
    alias each is attached under (alias -> libpq DSN), and every view's SELECT by name. None on a
    node of no region, whose ops views are the model database's own."""
    from provisa.federation.native_backend import libpq_dsn

    if not table_catalogs():
        return None
    model_db, record_db, state_db = state.model_db, state.record_db, state.tenant_db  # type: ignore[attr-defined]
    assert record_db is not None and state_db is not None  # bound with the region's stores
    record = (f"provisa_record__{record_db.search_path}", record_db.search_path)
    held = (f"provisa_state__{state_db.search_path}", state_db.search_path)
    catalogs = {MODEL: ("provisa_admin", model_db.search_path), RECORD: record, STATE: held}
    views = {
        name: f'SELECT * FROM "{catalog}"."{schema}"."{name}"'
        for side, (catalog, schema) in ((RECORD, record), (STATE, held))
        for name in store_views(side)
    }
    views.update({name: engine_view_sql(name, catalogs=catalogs) for name in engine_views()})
    stores = {
        record[0]: libpq_dsn(record_db.engine.url),
        held[0]: libpq_dsn(state_db.engine.url),
    }
    return model_db.search_path, stores, views


def reads_region_views(ctx: "CompilationContext", table_ids: "Iterable[int]") -> bool:
    """Whether a statement reading ``table_ids`` reads one of these views where this node reads
    them under :data:`CATALOG` (a node of a region): the engine reads it, not the model's database."""
    ids = set(table_ids)
    return any(
        meta.catalog_name == CATALOG and meta.table_id in ids for meta in ctx.tables.values()
    )
