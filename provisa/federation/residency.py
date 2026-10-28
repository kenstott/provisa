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
The whole-table build itself is ``replica_builds.build_replica`` (REQ-1915; a read asks for it through ``query_residency.ensure_resident``, REQ-1661) and the
event loop's source nodes (boot-create and refresh), both through the engine's landing address and
store write face.
"""

from __future__ import annotations

from typing import Any

from dataclasses import dataclass
from typing import TYPE_CHECKING

from provisa.core.change_signal import resolve_effective

if TYPE_CHECKING:
    from provisa.core.models import Source, Table


@dataclass(frozen=True)
class LandingArgs:  # REQ-932
    """The replication arguments resolved from a (source, table) config."""

    columns: list[tuple[str, str]]
    change_signal: str
    watermark_column: str | None
    pk_columns: list[str]
    probe_type: str = (
        "none"  # REQ-982: resolved input-probe method (drives the injected event shape)
    )
    # REQ-2062: the table's landing filter, a predicate its replica's rows pass; None = all rows.
    landing_filter: str | None = None
    # REQ-2171: each subtyped column left out of the replica -- (column, subtype, the reader that
    # has no form to land it in) -- named in the build's note and refused by name on a read.
    not_landed: tuple[tuple[str, str, str], ...] = ()


def landing_readers(source: Any, platform: str | None) -> tuple[str, ...]:
    """Who may read ``source``'s rows for a landing (REQ-2171): an adapter source its own adapter;
    a SQL source its own driver (its dialect) or the engine (``platform``), whichever the build
    takes, so a subtyped column lands only in a form both have."""
    dialect = source.dialect
    if dialect is None:
        return (source.type.value if hasattr(source.type, "value") else str(source.type),)
    return tuple(dict.fromkeys(r for r in (dialect, platform) if r))


def not_landed(source: Any, columns: Any, platform: str | None) -> tuple[tuple[str, str, str], ...]:
    """``(column, subtype, reader)`` for each subtyped column of ``columns`` some reader of
    ``source`` has no form to land in (REQ-2171): left out of the replica, never landed in any
    other form."""
    from provisa.core.ir_types import landing_form

    subtyped = [c for c in columns if getattr(c, "subtype", None)]
    if not subtyped:
        return ()
    readers = landing_readers(source, platform)
    out = []
    for c in subtyped:
        subtype = c.subtype
        lacking = next((r for r in readers if landing_form(subtype, r) is None), None)
        if lacking is not None:
            out.append((c.name, subtype, lacking))
    return tuple(out)


#: Engines whose replica of a table with an embedding column holds it as an indexed vector
#: (REQ-424): each reads replicas from a store whose own SQL searches the index.
EMBEDDING_INDEX_PLATFORMS = frozenset({"duckdb", "postgres"})


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
    is an error (introspection fills it at startup). REQ-2171: a type that fits no IR type lands
    as text."""
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
    # REQ-1908: an epoch-stored temporal column is translated only on live reads (the compiler's
    # physical rewrite). Landing would copy the stored numbers into a temporal-typed replica column
    # with no conversion, so the land is refused here — every landing path resolves through this
    # function — naming the column, rather than failing on a type mismatch or landing raw numbers.
    epoch_stored = [c.name for c in data_cols if getattr(c, "epoch_unit", None)]
    if epoch_stored:
        raise ValueError(
            f"cannot land {table.schema_name}.{table.table_name}: column(s) {epoch_stored} are "
            "epoch-stored temporals (epoch_unit), which are supported on live reads only — this "
            "engine would land the table"
        )
    # REQ-2171: a subtyped column no reader of the source can land in its own form is left out.
    left_out = not_landed(source, data_cols, platform)
    left_out_names = {name for name, _subtype, _reader in left_out}
    columns: list[tuple[str, str]] = []
    for c in data_cols:
        if c.name in left_out_names:
            continue
        if c.data_type is None:
            raise ValueError(
                f"cannot land {table.schema_name}.{table.table_name}: column {c.name!r} has no "
                f"resolved data_type (startup introspection must fill it before materialization)"
            )
        # REQ-424: an embedding column lands as a vector where the engine indexes its replica.
        if c.embedding and platform in EMBEDDING_INDEX_PLATFORMS:
            columns.append((c.name, "vector"))
        elif c.embedding:
            # REQ-424: elsewhere an embedding lands as it did before REQ-1965 (text, the lowering
            # casting it to the engine's array): never as JSON, which REQ-1965 is for the rest
            columns.append((c.name, "text"))
        else:
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
    return LandingArgs(
        columns, sig, watermark, pk_columns, probe_type, table.landing_filter, left_out
    )
