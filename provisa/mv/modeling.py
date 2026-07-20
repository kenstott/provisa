# Copyright (c) 2026 Kenneth Stott
# Canary: 6a305cc3-59af-4fa6-b3f6-9d2e29a396b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Declarative `entity` / `fact` sugar that LOWERS to MV primitives (REQ-1164).

Star schemas and Data Vaults are compositions of a tiny primitive set. Rather than a monolithic
"build me a star schema" macro, Provisa offers the two primitives both patterns are built from and
lowers them to the existing materialized-view / bitemporal / relationship machinery:

- ``Entity`` — a keyed, deduplicated, optionally-historized projection of a source. Lowers to a
  materialized view; when history is requested it is a bitemporal MV (REQ-1162): history="scd2" →
  delta mode, history="snapshot" → snapshot mode, keyed on the business key. Serves a star DIMENSION
  (SCD1/SCD2) and a Data Vault HUB + SATELLITE.
- ``Fact`` — a join to entity keys, reduced to a declared grain, with aggregated measures. Lowers to
  a materialized view (grain + dimension FK columns + aggregated measures, GROUP BY grain+FKs) plus
  registered relationships to the entities. Serves a star FACT and a Data Vault LINK.

The lowering is PURE: a spec becomes exactly the registration a user would otherwise hand-write, so
the model GENERATES the star/vault while staying methodology-neutral — grain, conformance, and SCD
choice remain the modeler's. The generated definitions are IR, so the warehouse retargets across
engines without remodeling.
"""

from __future__ import annotations

from dataclasses import dataclass, field

HISTORY_NONE = "none"
HISTORY_SCD2 = "scd2"
HISTORY_SNAPSHOT = "snapshot"
_HISTORY = frozenset({HISTORY_NONE, HISTORY_SCD2, HISTORY_SNAPSHOT})
# history → bitemporal mode (None = ordinary materialization, no history)
_HISTORY_MODE = {HISTORY_SCD2: "delta", HISTORY_SNAPSHOT: "snapshot"}
_AGGS = frozenset({"sum", "avg", "min", "max", "count"})


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _dedup(seq) -> list[str]:
    """Order-preserving de-duplication."""
    return list(dict.fromkeys(seq))


@dataclass(frozen=True)
class Entity:
    """A dimension / hub+satellite: a keyed projection of ``source``, optionally historized."""

    name: str
    source: str  # source relation the projection reads from
    key: tuple[str, ...]
    attributes: tuple[str, ...] = ()
    history: str = HISTORY_NONE

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError(f"entity {self.name!r} must declare a business key")
        if self.history not in _HISTORY:
            raise ValueError(
                f"entity {self.name!r} history {self.history!r} must be one of {sorted(_HISTORY)}"
            )


@dataclass(frozen=True)
class Measure:
    column: str
    agg: str = "sum"

    def __post_init__(self) -> None:
        if self.agg not in _AGGS:
            raise ValueError(f"measure {self.column!r} agg {self.agg!r} must be one of {sorted(_AGGS)}")


@dataclass(frozen=True)
class DimRef:
    """A fact's reference to a dimension entity via a foreign-key column on the fact source."""

    entity: str  # the referenced Entity's name
    via: str  # the FK column on the fact source


@dataclass(frozen=True)
class Fact:
    """A star fact / DV link: join to entity keys, reduce to ``grain``, aggregate ``measures``."""

    name: str
    source: str
    grain: tuple[str, ...]
    measures: tuple[Measure, ...] = ()
    dimensions: tuple[DimRef, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.grain:
            raise ValueError(f"fact {self.name!r} must declare a grain")


def entity_registration(e: Entity) -> dict:
    """Lower an entity to a table-registration dict (the same shape a hand-written view would use)."""
    cols = _dedup([*e.key, *e.attributes])
    view_sql = f"SELECT {', '.join(_q(c) for c in cols)} FROM {_qualify(e.source)}"
    reg: dict = {
        "table_name": e.name,
        "view_sql": view_sql,
        "materialize": True,
        "columns": cols,
    }
    mode = _HISTORY_MODE.get(e.history)
    if mode is not None:
        # Historized entity → a bitemporal MV keyed on the business key (REQ-1162).
        reg["mv_bitemporal_mode"] = mode
        reg["mv_bitemporal_key"] = list(e.key)
    return reg


def fact_registration(f: Fact) -> dict:
    """Lower a fact to a table-registration dict plus relationships to its dimension entities."""
    dim_cols = [d.via for d in f.dimensions]
    group_cols = _dedup([*f.grain, *dim_cols])
    measure_sql = [f"{m.agg.upper()}({_q(m.column)}) AS {_q(m.column)}" for m in f.measures]
    select = ", ".join([*(_q(c) for c in group_cols), *measure_sql])
    view_sql = f"SELECT {select} FROM {_qualify(f.source)}"
    if f.measures:  # aggregate to the grain; a measureless fact is a pure key-set (DV link)
        view_sql += f" GROUP BY {', '.join(_q(c) for c in group_cols)}"
    return {
        "table_name": f.name,
        "view_sql": view_sql,
        "materialize": True,
        "columns": _dedup([*group_cols, *(m.column for m in f.measures)]),
        # Each dimension FK is a registered relationship to the entity — the only legal join path.
        "relationships": [
            {"source_column": d.via, "target_table": d.entity} for d in f.dimensions
        ],
    }


def _qualify(source: str) -> str:
    """Quote a possibly-dotted source relation: ``schema.table`` → ``"schema"."table"``.
    An already-quoted ref is passed through unchanged."""
    if '"' in source:
        return source
    return ".".join(_q(part) for part in source.split("."))
