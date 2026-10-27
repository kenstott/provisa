# Copyright (c) 2026 Kenneth Stott
# Canary: 28211b07-15bc-46ba-9e9a-1bc3045314d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The joins the SQL canvas draws are exactly the joins SQL allows (sql_validator.approved_joins):
each pair of columns once, a junction relationship as its two foreign-key joins -- so
pet_companions joins pets on pet_id and on companion_pet_id, whichever of the relationship's
typed spellings declared it -- and no column equal to itself."""

from __future__ import annotations

from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_types import JunctionMeta


def _meta(table_id: int, name: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name="".join(p.capitalize() for p in name.split("_")),
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name=name,
        domain_id="pets",
    )


def _ctx() -> CompilationContext:
    pets, companions, vets = _meta(1, "pets"), _meta(2, "pet_companions"), _meta(3, "vets")
    ctx = CompilationContext()
    ctx.tables = {"pets": pets, "pet_companions": companions, "vets": vets}
    via = JunctionMeta(
        table=companions, source_columns=("pet_id",), target_columns=("companion_pet_id",)
    )
    # config/provisa-install.yaml: three typed junctions over the same columns.
    ctx.joins = {
        ("Pets", name): JoinMeta(
            source_column="id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=pets,
            cardinality="one-to-many",
            via=via,
        )
        for name in ("bondedPair", "littermate", "sharesEnclosure")
    }
    ctx.joins[("Pets", "vet")] = JoinMeta(
        source_column="id",
        target_column="id",
        source_column_type="integer",
        target_column_type="integer",
        target=vets,
        cardinality="many-to-one",
    )
    return ctx


def test_the_canvas_draws_each_approved_join_once_and_no_column_equal_to_itself():
    from provisa.compiler.sql_validator import canvas_joins

    assert canvas_joins(_ctx()) == [
        (1, "id", 2, "companion_pet_id"),
        (1, "id", 2, "pet_id"),
        (1, "id", 3, "id"),
    ]


async def test_the_endpoint_answers_the_acting_role_s_canvas_joins(monkeypatch):
    from types import SimpleNamespace

    from provisa.api.app import state
    from provisa.api.data.endpoint_dev import sql_approved_joins_endpoint

    monkeypatch.setattr(state, "contexts", {"analyst": _ctx()}, raising=False)
    request = SimpleNamespace(state=SimpleNamespace(role="analyst"))
    answer = await sql_approved_joins_endpoint(request, None)
    assert answer["joins"][0] == {
        "fromTableId": 1,
        "fromColumn": "id",
        "toTableId": 2,
        "toColumn": "companion_pet_id",
    }
    assert len(answer["joins"]) == 3
