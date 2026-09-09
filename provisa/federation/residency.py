# Copyright (c) 2026 Kenneth Stott
# Canary: 4c10b01f-4173-4f03-8537-f0afba76df50
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Residency landing arguments (REQ-825 stage-4b, REQ-932): what one MATERIALIZED table lands
as -- its IR-typed columns, key, change signal, watermark and probe type -- resolved from config.
The landing itself is ``EngineBackend.materialize_pending`` (the query path, REQ-1661) and the
event loop's source nodes (boot-create and refresh), both through the engine's landing address and
store write face.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from provisa.core.change_signal import resolve_effective

if TYPE_CHECKING:
    from provisa.core.models import Source, Table


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
