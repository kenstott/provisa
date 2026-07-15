# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Residency prep executor (REQ-825 stage-4b, REQ-932): run a Plan's PrepSteps by landing.

``build_execution_plan`` (plan.py) is pure — it emits a prep phase of MATERIALIZED sources that
are stale. This module is the IMPURE counterpart that carries those preps out: for each PrepStep,
resolve the landing arguments from config, fetch the source's current rows through an injected
``ResidencyLoader``, and land them via ``runtime.materialize_source`` — which picks the shape
(REPLACE / APPEND) from the effective change_signal (REQ-932).

The row-fetch is an injected seam, not a fixed call: materialize-only sources (openapi, mongodb,
…) have no universal "snapshot the whole table" primitive — each type fetches differently — so the
loader is supplied by the caller. The engine is NEVER the writer: landing goes through the store's
own SQLAlchemy write face inside materialize_source, and the engine only reads the landed replica.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Protocol

from provisa.core.change_signal import resolve_effective

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from provisa.core.models import Source, Table
    from provisa.federation.plan import Plan


@dataclass(frozen=True)
class LandingArgs:  # REQ-932
    """The materialize_source arguments resolved from a (source, table) config."""

    columns: list[tuple[str, str]]
    change_signal: str
    watermark_column: str | None
    pk_columns: list[str]
    probe_type: str = (
        "none"  # REQ-982: resolved input-probe method (drives the injected event shape)
    )


def resolve_landing_args(
    source: Source, table: Table, *, platform: str | None = None
) -> LandingArgs:
    """Resolve the landing arguments for one MATERIALIZED table (REQ-932/846).

    change_signal: table override → the signal implied by live.strategy → source default (``resolve_effective``).
    watermark_column: the table's, else its live config. pk_columns: the user-designated
    primary key columns. columns: (name, IR data_type) — each column's stored native (engine-
    normalized) type is translated to a canonical IR name via ``to_ir(native, platform)`` so the
    landed table's DDL is engine-independent (the store write face maps IR → SQLAlchemy). ``platform``
    is the federation engine's dialect (its stored types are engine-normalized, e.g. Trino
    ``varbinary``/``row(...)`` that the generic aliases don't cover). A column with no resolved type
    is an error (introspection fills it at startup); an unmapped native type is an IR vocabulary gap
    and raises — never a silent default."""
    from provisa.core.ir_types import to_ir

    live = table.live
    sig = resolve_effective(
        table.change_signal,
        source.change_signal,
        live.strategy if live is not None else None,
    )
    watermark = table.watermark_column or (live.watermark_column if live is not None else None)
    # Native-filter columns (REST/GraphQL query/path params) are synthetic query-arg inputs, never
    # part of the landed replica — exclude them from the landing shape (they carry no data_type).
    data_cols = [c for c in table.columns if getattr(c, "native_filter_type", None) is None]
    pk_columns = [c.name for c in data_cols if c.is_primary_key]
    columns: list[tuple[str, str]] = []
    for c in data_cols:
        if c.data_type is None:
            raise ValueError(
                f"cannot land {table.schema_name}.{table.table_name}: column {c.name!r} has no "
                f"resolved data_type (startup introspection must fill it before materialization)"
            )
        columns.append((c.name, to_ir(c.data_type, platform)))
    # REQ-982: resolve the effective probe_type (validated against the source's capability class;
    # ttl forces none; unset under a probing cadence defaults per class).
    from provisa.events.probes import resolve_probe_type

    source_type = source.type.value if hasattr(source.type, "value") else str(source.type)
    probe_type = resolve_probe_type(
        getattr(table, "probe_type", None),
        source_type=source_type,
        change_signal=sig,
        has_watermark=watermark is not None,
    )
    return LandingArgs(columns, sig, watermark, pk_columns, probe_type)


class ResidencyLoader(Protocol):
    """Fetches the current rows of a MATERIALIZED source table. Implementations dispatch on the
    source type (openapi HTTP call, mongodb find, …); there is no universal snapshot primitive, so
    this is injected. Returns the rows as dicts keyed by column name."""

    async def load(self, source: Source, table: Table) -> list[dict]: ...


async def run_prep(
    plan: Plan,
    *,
    sources_by_id: Mapping[str, Source],
    tables_by_source: Mapping[str, Sequence[Table]],
    runtime: Any,
    loader: ResidencyLoader,
) -> list[tuple[str, str]]:
    """Carry out ``plan.prep``: land each MATERIALIZED table into the store (REQ-825/932).

    For every PrepStep, for every table of that source: resolve the landing args, fetch the rows,
    and call ``runtime.materialize_source`` (which lands through the store's SQLAlchemy write face
    and exposes the replica as a physical-named view). Returns the (source_id, table_name) pairs
    landed, in order — the engine reads them; it never writes."""
    landed: list[tuple[str, str]] = []
    for step in plan.prep:
        source = sources_by_id[step.source_id]
        for table in tables_by_source.get(step.source_id, ()):
            args = resolve_landing_args(source, table, platform=runtime.dialect)
            rows = await loader.load(source, table)
            merged = SimpleNamespace(
                id=source.id,
                type=source.type,
                schema_name=table.schema_name,
                table_name=table.table_name,
            )
            await runtime.materialize_source(
                merged,
                args.columns,
                rows,
                change_signal=args.change_signal,
                watermark_column=args.watermark_column,
                pk_columns=args.pk_columns,
            )
            landed.append((step.source_id, table.table_name))
    return landed
