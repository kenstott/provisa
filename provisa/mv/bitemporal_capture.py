# Copyright (c) 2026 Kenneth Stott
# Canary: d4d74e32-93ea-44a2-9cea-9950d09842c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The versions of a bitemporal table updated in place (REQ-2109).

A table mapping ``business_key``, ``valid_time`` and ``recorded_time`` whose business key is its
primary key holds one row per key: a correction overwrites it. Provisa keeps its versions in a
log, an ordinary delta bitemporal materialized view of the table (``SELECT *`` of it) built into
the materialize store of the table's home region and refreshed on that view's own refresh
interval:

* its key is the business key, its valid column the valid time, and its recorded clock the
  table's own recorded time, which every version carries;
* each capture reads the rows recorded after the latest recorded time the log holds (less the
  view's allowed lateness, so a row committed late with an earlier recorded time is still read)
  and appends those whose values changed. It appends no tombstone: a row it did not read is a row
  not changed since;
* the first capture, and a full one (``refresh_mv(full=True)``, the admin refresh's ``full``),
  read the whole table, its tombstones unconfined: a row the source deleted is recorded gone at
  that capture. History before the first capture is each row's version then.

A plain read of the table reads the table. A read under ``as_of`` or ``as_recorded`` reads its
log (:func:`provisa.mv.bitemporal.view_read_sql`) in its place, after governance judged the
table, so the reader's rules apply to what the log answers.
"""

# Requirements: REQ-2109

from __future__ import annotations

import dataclasses
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from provisa.mv.bitemporal import MODE_DELTA, BitemporalSpec
from provisa.mv.bitemporal_inherit import InheritedBuild
from provisa.mv.bitemporal_tables import VersionedTable

if TYPE_CHECKING:
    from provisa.federation.execution_auth import SystemAuth
    from provisa.mv.models import MVDefinition

#: The id of a table's version log: ``versions-<table id>``.
LOG_PREFIX = "versions-"


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def captured_table_rows(tables: list[dict]) -> dict[int, VersionedTable]:
    """Every registered table (not a view) mapping the three values whose business key is its
    primary key, by table id: the tables whose versions are a log Provisa keeps."""
    out: dict[int, VersionedTable] = {}
    for table in tables:
        key = table.get("business_key") or []
        if not key or table.get("view_sql"):
            continue
        primary = {c["column_name"] for c in table.get("columns") or [] if c.get("is_primary_key")}
        if primary != set(key):
            continue  # its rows are its versions (bitemporal_tables)
        out[table["id"]] = VersionedTable(
            table_id=table["id"],
            business_key=tuple(key),
            valid_time=table["valid_time"],
            recorded_time=table["recorded_time"],
        )
    return out


def log_id(table_id: int) -> str:
    return f"{LOG_PREFIX}{table_id}"


def logged_table_id(mv_id: str) -> int | None:
    """The table whose version log ``mv_id`` is; None for any other view."""
    if not mv_id.startswith(LOG_PREFIX):
        return None
    return int(mv_id[len(LOG_PREFIX) :])


def log_spec(table: VersionedTable) -> BitemporalSpec:
    return BitemporalSpec(
        key=table.business_key,
        mode=MODE_DELTA,
        source_column=table.recorded_time,
        valid_column=table.valid_time,
        captures=True,
    )


def log_mv(
    row: dict,
    table: VersionedTable,
    *,
    target_catalog: str,
    target_schema: str,
    catalog_qualified: bool,
    refresh_interval: int,
) -> "MVDefinition":
    """The version log of the registered table ``row``: its whole table, built where the table
    is homed."""
    from provisa.compiler.naming import source_to_catalog
    from provisa.mv.models import MVDefinition, MVStatus
    from provisa.mv.readable_inputs import read_table_names

    ref = ".".join(
        _q(p) for p in (source_to_catalog(row["source_id"]), row["schema_name"], row["table_name"])
    )
    sql = f"SELECT * FROM {ref}"
    return MVDefinition(
        id=log_id(table.table_id),
        source_tables=[row["table_name"]],
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_catalog_qualified=catalog_qualified,
        target_table=f"{row['table_name']}__versions_{table.table_id}",
        refresh_interval=refresh_interval,
        enabled=not row.get("draft", False),  # REQ-1921: a draft table's log is not captured
        sql=sql,
        semantic_sql=sql,
        region=row.get("region"),
        read_tables=read_table_names(sql),
        expose_in_sdl=False,
        status=MVStatus.STALE,
        bitemporal=log_spec(table),
    )


def register_logs(state: Any, tables: list[dict], refresh_interval: int) -> None:
    """Register the version log of every table updated in place among ``tables`` (the rows this
    build loaded, draft ones among them), with the refresh interval and allowed lateness its
    table's row holds for it (``mv_refresh_interval``, ``mv_allowed_lateness``; the interval
    ``refresh_interval`` when the row names none), and drop the log of a table no longer
    captured."""
    captured = captured_table_rows(tables)
    rows = {row["id"]: row for row in tables}
    catalog, schema = state.federation_engine.materialize_store_target(state.active_org_id)
    for mv in list(state.mv_registry.all()):
        tid = logged_table_id(mv.id)
        if tid is not None and tid not in captured:
            state.mv_registry.unregister(mv.id)
    for tid, table in captured.items():
        fresh = log_mv(
            rows[tid],
            table,
            target_catalog=catalog,
            target_schema=schema,
            catalog_qualified=state.federation_engine.engine.catalog_qualified,
            # REQ-199: a view naming no interval is refreshed on the configured default.
            refresh_interval=int(rows[tid].get("mv_refresh_interval") or refresh_interval),
        )
        fresh.allowed_lateness = float(rows[tid].get("mv_allowed_lateness") or 0.0)  # REQ-961
        kept = state.mv_registry.get(fresh.id)
        if kept is None:
            state.mv_registry.register(fresh)
            continue
        kept.enabled = fresh.enabled
        kept.refresh_interval = fresh.refresh_interval
        kept.allowed_lateness = fresh.allowed_lateness
        kept.region = fresh.region
        kept.bitemporal = fresh.bitemporal
        kept.semantic_sql = fresh.semantic_sql
        kept.read_tables = fresh.read_tables


async def capture_build(
    engine: Any,
    mv: "MVDefinition",
    authorization: "SystemAuth",
    existing: list[str] | None,
    *,
    full: bool,
) -> InheritedBuild:
    """This capture of the version log ``mv`` (module doc). ``existing`` is its log's columns
    (None: not built yet); ``full`` reads the whole table."""
    from provisa.compiler.sql_literals import sql_literal
    from provisa.mv.bitemporal import system_columns_ddl
    from provisa.mv.governed_build import view_build_sql
    from provisa.mv.refresh import _store_statement, _target_ref

    spec = mv.bitemporal
    assert spec is not None and spec.captures and spec.source_column is not None
    if mv.semantic_sql is None:
        raise ValueError(f"MV {mv.id}: a version log is built from its definition")
    if existing is None or full:
        return InheritedBuild(await view_build_sql(mv, engine))
    recorded = _q(spec.source_column)
    rows = await _store_statement(
        engine, f"SELECT MAX({recorded}) FROM {_target_ref(mv)}", authorization
    )
    watermark = rows[0][0]
    if watermark is None:
        return InheritedBuild(await view_build_sql(mv, engine))
    # mv_allowed_lateness is a declared numeric setting whose default is 0 (REQ-961).
    since = watermark - timedelta(seconds=mv.allowed_lateness)
    scoped = (
        f"SELECT * FROM ({mv.semantic_sql}) AS _v "
        f"WHERE {recorded} > {sql_literal(since, 'postgres')}"
    )
    select_sql = await view_build_sql(
        dataclasses.replace(mv, sql=scoped, semantic_sql=scoped), engine
    )
    shape = await engine.execute_engine(
        f"SELECT * FROM ({select_sql}) _shape LIMIT 0", authorization=authorization
    )
    managed = {c for c, _ in system_columns_ddl(spec)}
    if list(shape.column_names) != [c for c in existing if c not in managed]:
        # The table's columns changed since its log was built: the log is rebuilt whole.
        return InheritedBuild(await view_build_sql(mv, engine))
    return InheritedBuild(select_sql, tombstones=False)


def reads_a_log(table_ids: Any, state: Any, travel: Any) -> bool:
    """Whether a statement reading the tables ``table_ids`` under ``travel`` (a
    ``time_travel.Travel``, or one time for every reference) reads a version log: it is then
    computed by the engine, which reads the store the log is kept in."""
    from provisa.mv.time_travel import as_travel  # noqa: PLC0415

    if not as_travel(travel).travels():
        return False
    captured = captured_table_rows(getattr(state, "tables", None) or [])
    return any(tid in captured for tid in table_ids)


def read_logs(sql: str, state: Any, ctx: Any, travel: Any) -> str:
    """``sql`` (governed, catalog-physical) with every reference to a table updated in place read
    from its version log at that reference's time (``travel``: per alias, else the request's;
    REQ-1163); a reference read at no time reads the table. Each reference keeps its alias, so
    what governance put around it applies."""
    from provisa.mv.time_travel import as_travel  # noqa: PLC0415

    travel = as_travel(travel)
    if not travel.travels():
        return sql
    tables = getattr(state, "tables", None) or []
    captured = captured_table_rows(tables)
    if not captured:
        return sql
    import sqlglot  # noqa: PLC0415
    from sqlglot import exp  # noqa: PLC0415

    from provisa.mv.bitemporal import AsOf, view_read_sql  # noqa: PLC0415
    from provisa.mv.models import stored_table_ref  # noqa: PLC0415

    physical = {
        (
            meta.catalog_name.lower(),
            meta.schema_name.lower(),
            meta.table_name.lower(),
        ): meta.table_id
        for meta in ctx.tables.values()
        if meta.table_id in captured
    }
    tree = sqlglot.parse_one(sql, read="postgres")
    found = [
        (node, tid)
        for node in tree.find_all(exp.Table)
        if (tid := physical.get((node.catalog.lower(), node.db.lower(), node.name.lower())))
        is not None
    ]
    if not found:
        return sql
    columns = {row["id"]: [c["column_name"] for c in row.get("columns") or []] for row in tables}
    for node, tid in found:
        alias = node.alias or node.name
        at = travel.at(alias)
        if at is None:
            continue  # this reference reads the table as it is
        point = AsOf.from_key(at)
        mv = state.mv_registry.get(log_id(tid))
        if mv is None or mv.bitemporal is None:
            raise RuntimeError(f"table {tid} keeps its versions but has no version log registered")
        log_ref = stored_table_ref(
            mv.target_catalog,
            mv.target_schema,
            mv.target_table,
            catalog_qualified=mv.target_catalog_qualified,
        )
        cols = ", ".join(_q(c) for c in columns[tid])
        read = sqlglot.parse_one(
            f"SELECT {cols} FROM ({view_read_sql(log_ref, mv.bitemporal, point)}) AS _provisa_log",
            read="postgres",
        )
        node.replace(exp.Subquery(this=read, alias=exp.TableAlias(this=exp.to_identifier(alias))))
    return tree.sql(dialect="postgres")
