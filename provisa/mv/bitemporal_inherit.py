# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d93e4-58a2-4f0c-9e27-d3a41c8f7b25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A materialized view that inherits its versions from the one versioned table it groups
(REQ-2109).

A view declaring none of business_key, valid_time and recorded_time, whose definition reads
exactly one table -- a versioned table (:mod:`provisa.mv.bitemporal_tables`: its rows are its
versions) -- and groups it, keeps every version of its own with no settings:

* its business key is its GROUP BY output columns (``events.lineage.infer_pk``);
* its valid time is its output column that is the input's valid time unchanged (column lineage,
  an identity projection); a view with no such column inherits nothing and is an ordinary MV;
* its recorded time is the append stamp of each refresh.

Each refresh is scoped by a watermark: the latest input recorded time an append has read, kept in
the log itself as the managed column ``sys_input_recorded`` on every upsert, so it advances only
when an append commits and survives a restart (watermark = its maximum over the log). A refresh
reads the input's valid dates changed since the watermark (less the view's allowed lateness, so a
row committed late with an earlier recorded time is still read), rebuilds only those dates through
the one pipeline, and appends what changed, its tombstones confined to the current keys of those
dates. Re-reading a date already appended appends nothing: the delta compares values.

What a scoped refresh cannot see -- a full refresh (``refresh_mv(full=True)``, the admin refresh's
``full``) recomputes every date with tombstones unconfined:

* a row moved between valid dates: the new date is rebuilt, the old date keeps its stale
  aggregate;
* a source that hard-deletes rows: a deleted row has no recorded time to be read by;
* a row with no valid time: it belongs to no date a scoped refresh rebuilds.
"""

# Requirements: REQ-2109

from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import sqlglot
import sqlglot.expressions as exp

from provisa.mv.bitemporal import (
    MODE_DELTA,
    WATERMARK_COLUMN,
    BitemporalSpec,
    InheritedInput,
    system_columns_ddl,
)

if TYPE_CHECKING:
    from provisa.federation.execution_auth import SystemAuth
    from provisa.mv.models import MVDefinition

log = logging.getLogger(__name__)


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _key(parts: tuple[str, ...]) -> str:
    return ".".join(p.lower() for p in parts)


def _spellings(row: dict) -> set[str]:
    """Every spelling a view's SQL may name the registered table ``row`` by (as the model index
    resolves it), and its alias qualified by its domain."""
    from provisa.compiler.naming import apply_sql_name, domain_to_sql_name
    from provisa.mv.view_inputs import row_spellings

    spelled = list(row_spellings(row, None))
    if row.get("alias") and row.get("domain_id"):
        spelled.append((domain_to_sql_name(row["domain_id"]), apply_sql_name(row["alias"])))
    return {_key(p) for p in spelled if all(p)}


def _identity_output(view_sql: str, valid_time: str) -> str | None:
    """The output column of ``view_sql`` that is ``valid_time`` unchanged: a bare column whose
    lineage is that column alone. None when there is none."""
    from provisa.lineage.columns import resolve_column_lineage

    projections = list(getattr(sqlglot.parse_one(view_sql, read="postgres"), "selects", []) or [])
    derivations = resolve_column_lineage(view_sql, "postgres")
    for projection, derivation in zip(projections, derivations, strict=True):
        sources = [s.rsplit(".", 1)[-1] for s in derivation.sources]
        if isinstance(projection.unalias(), exp.Column) and sources == [valid_time]:
            return derivation.output
    return None


def inherited_spec(view_sql: str, tables: list[dict]) -> BitemporalSpec | None:
    """The versions a materialized view declaring none inherits (module doc), or None when it
    inherits none: it reads other than exactly one table, that table is not versioned, it has no
    GROUP BY key, or no output column is the table's valid time unchanged. ``tables`` are the
    registered table rows as the build that registers the view loaded them."""
    from provisa.events.lineage import infer_pk
    from provisa.mv.bitemporal_tables import versioned_table_rows
    from provisa.mv.view_inputs import table_refs

    refs = table_refs(view_sql)
    if len(refs) != 1:
        return None
    named = _key(refs[0])
    matches = [row for row in tables if named in _spellings(row)]
    if len(matches) != 1:
        return None
    versioned = versioned_table_rows(matches).get(matches[0]["id"])
    if versioned is None:
        return None
    key = infer_pk(view_sql)
    if not key:
        return None
    valid = _identity_output(view_sql, versioned.valid_time)
    if valid is None:
        return None
    return BitemporalSpec(
        key=tuple(key),
        mode=MODE_DELTA,
        valid_column=valid,
        inherited=InheritedInput(
            ref=".".join(_q(p) for p in refs[0]),
            valid_time=versioned.valid_time,
            recorded_time=versioned.recorded_time,
        ),
    )


@dataclass(frozen=True)
class InheritedBuild:
    """One refresh of an inherited view: the engine SELECT it appends from (None: no input row
    changed, nothing is appended), the watermark its upserts carry, and the valid dates it rebuilt
    (None: every date, tombstones unconfined)."""

    select_sql: str | None
    watermark: Any = None
    dates: tuple[Any, ...] | None = None

    @property
    def scoped(self) -> bool:
        return self.dates is not None

    def watermark_sql(self, dialect: str) -> str:
        from provisa.compiler.sql_literals import sql_literal

        return sql_literal(self.watermark, dialect)

    def scope_sql(self, spec: BitemporalSpec, dialect: str) -> str | None:
        """The condition confining tombstones to the rebuilt dates, on the current state's alias
        ``c`` (:func:`provisa.mv.bitemporal.append_sql`); None for a full build."""
        if self.dates is None:
            return None
        assert spec.valid_column is not None  # an inherited spec always has one
        return f'"c".{_q(spec.valid_column)} IN ({_literals(self.dates, dialect)})'


def _literals(values: tuple[Any, ...], dialect: str) -> str:
    from provisa.compiler.sql_literals import sql_literal

    return ", ".join(sql_literal(v, dialect) for v in values)


def _governed_copy(mv: "MVDefinition", semantic_sql: str) -> "MVDefinition":
    """``mv`` with ``semantic_sql`` as its definition: the statement a build governs and routes
    as ``mv``'s own, as its region's administrator (``governed_build``)."""
    return dataclasses.replace(mv, sql=semantic_sql, semantic_sql=semantic_sql)


async def _input_rows(mv: "MVDefinition", engine: Any, sql: str) -> list[dict]:
    from provisa.mv.governed_build import view_build_rows

    return await view_build_rows(_governed_copy(mv, sql), engine)


async def inherited_build(
    engine: Any,
    mv: "MVDefinition",
    authorization: "SystemAuth",
    existing: list[str] | None,
    *,
    full: bool,
) -> InheritedBuild:
    """This refresh of the inherited view ``mv`` (module doc). ``existing`` is its log's columns
    (None: not built yet); ``full`` ignores the watermark and rebuilds every date."""
    build = await _build(engine, mv, authorization, existing, full=full)
    if build.scoped and build.select_sql is not None:
        assert existing is not None  # a scoped build reads a log that exists
        shape = await engine.execute_engine(
            f"SELECT * FROM ({build.select_sql}) _shape LIMIT 0", authorization=authorization
        )
        managed = {c for c, _ in system_columns_ddl(_spec(mv))}
        if list(shape.column_names) != [c for c in existing if c not in managed]:
            # Its definition changed since its log was built: the log is rebuilt from every date.
            log.info("MV %s: definition changed since its log was built — full build", mv.id)
            build = await _build(engine, mv, authorization, None, full=True)
    return build


def _spec(mv: "MVDefinition") -> BitemporalSpec:
    spec = mv.bitemporal
    assert spec is not None and spec.inherited is not None  # called for an inherited view only
    return spec


async def _build(
    engine: Any,
    mv: "MVDefinition",
    authorization: "SystemAuth",
    existing: list[str] | None,
    *,
    full: bool,
) -> InheritedBuild:
    from provisa.mv.governed_build import view_build_sql
    from provisa.mv.refresh import _store_statement, _target_ref

    spec = _spec(mv)
    inp = spec.inherited
    assert inp is not None and spec.valid_column is not None
    if mv.semantic_sql is None:
        raise ValueError(f"MV {mv.id}: an inherited view is built from its definition")
    valid, recorded = _q(inp.valid_time), _q(inp.recorded_time)
    watermark = None
    if existing is not None and not full:
        rows = await _store_statement(
            engine, f"SELECT MAX({_q(WATERMARK_COLUMN)}) FROM {_target_ref(mv)}", authorization
        )
        watermark = rows[0][0]
    if watermark is None:
        # First build (or full): every date, the watermark the input's latest recorded time.
        latest = await _input_rows(mv, engine, f"SELECT MAX({recorded}) AS m FROM {inp.ref}")
        return InheritedBuild(await view_build_sql(mv, engine), latest[0]["m"], None)
    # mv_allowed_lateness is a declared numeric setting whose default is 0 (REQ-961).
    since = watermark - timedelta(seconds=mv.allowed_lateness)
    changed = await _input_rows(
        mv,
        engine,
        f"SELECT {valid} AS d, MAX({recorded}) AS m FROM {inp.ref} "
        f"WHERE {recorded} > {_literals((since,), 'postgres')} AND {valid} IS NOT NULL "
        f"GROUP BY {valid}",
    )
    if not changed:
        return InheritedBuild(None)
    dates = tuple(sorted(r["d"] for r in changed))
    scoped = (
        f"SELECT * FROM ({mv.semantic_sql}) AS _v "
        f"WHERE {_q(spec.valid_column)} IN ({_literals(dates, 'postgres')})"
    )
    select_sql = await view_build_sql(_governed_copy(mv, scoped), engine)
    return InheritedBuild(select_sql, max(r["m"] for r in changed), dates)
