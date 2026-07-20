# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lower entity/fact sugar (REQ-1164) to the admin registration inputs.

The registerEntity / registerFact mutations are thin: they call these pure builders to turn an
EntityInput / FactInput into the TableInput (+ RelationshipInputs) a hand registration would use —
via provisa.mv.modeling — then reuse the ordinary register_table / upsert_relationship path. Kept
pure and side-effect-free so the lowering is unit-testable without the app.
"""

from __future__ import annotations

from provisa.api.admin.types import (
    ColumnInput,
    DimRefInput,
    EntityInput,
    FactInput,
    RelationshipInput,
    TableInput,
)
from provisa.mv.modeling import (
    DimRef,
    Entity,
    Fact,
    Measure,
    entity_registration,
    fact_registration,
)

_SCHEMA = "views"  # __provisa__ views land under a fixed schema, like hand-registered views


def _columns(names: list[str], visible_to: list[str]) -> list[ColumnInput]:
    return [ColumnInput(name=n, visible_to=list(visible_to)) for n in names]


def entity_table_input(inp: EntityInput) -> TableInput:
    """EntityInput → the TableInput that registers it as a (bitemporal, when historized) MV."""
    reg = entity_registration(
        Entity(
            name=inp.name,
            source=inp.source,
            key=tuple(inp.key),
            attributes=tuple(inp.attributes),
            history=inp.history,
        )
    )
    return TableInput(
        source_id="__provisa__",
        domain_id=inp.domain_id,
        schema_name=_SCHEMA,
        table_name=reg["table_name"],
        columns=_columns(reg["columns"], inp.visible_to),
        view_sql=reg["view_sql"],
        materialize=True,
        mv_bitemporal_mode=reg.get("mv_bitemporal_mode"),
        mv_bitemporal_key=reg.get("mv_bitemporal_key", []),
    )


def fact_table_input(inp: FactInput) -> tuple[TableInput, list[RelationshipInput]]:
    """FactInput → the TableInput (aggregate MV) plus one RelationshipInput per dimension link."""
    reg = fact_registration(
        Fact(
            name=inp.name,
            source=inp.source,
            grain=tuple(inp.grain),
            measures=tuple(Measure(column=m.column, agg=m.agg) for m in inp.measures),
            dimensions=tuple(DimRef(entity=d.entity, via=d.via) for d in inp.dimensions),
        )
    )
    ti = TableInput(
        source_id="__provisa__",
        domain_id=inp.domain_id,
        schema_name=_SCHEMA,
        table_name=reg["table_name"],
        columns=_columns(reg["columns"], inp.visible_to),
        view_sql=reg["view_sql"],
        materialize=True,
    )
    rels = [
        RelationshipInput(
            id=f"{reg['table_name']}__{r['source_column']}",
            source_table_id=reg["table_name"],
            target_table_id=r["target_table"],
            source_column=r["source_column"],
            cardinality="many_to_one",  # fact rows → one dimension row (the FK target)
        )
        for r in reg["relationships"]
    ]
    return ti, rels
