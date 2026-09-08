# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e8f34-7a2b-4d96-b3e8-0f6d9a27c4b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1652: the key plan a landing reconcile applies -- derived from the registration tables and
relationships, for every landed table (a Data Product is not the unit; the landed model is)."""

from __future__ import annotations

import pytest

from types import SimpleNamespace

from provisa.federation.landed_keys import (
    LandedTable,
    derived_registration,
    key_plan,
    relationship_edges,
    with_tags,
)

pytestmark = pytest.mark.unit

_BY_ID = {
    1: "pet-store-sqlite.pet_store.pets",
    2: "pet-store-sqlite.pet_store.vets",
    3: "pet-store-sqlite.pet_store.pet_visits",
    4: "graphql-demo.graphql.assignments",
}


def _landed(pk_by_table: dict[str, tuple[str, ...]]) -> list[LandedTable]:
    out = []
    for ident, pk in pk_by_table.items():
        source_id, schema, table = ident.split(".")
        out.append(LandedTable(source_id, schema, table, pk))
    return out


def _rel(rel_id, source_id, target_id, source_column, target_column, cardinality, **via):
    return {
        "id": rel_id,
        "source_table_id": source_id,
        "target_table_id": target_id,
        "source_column": source_column,
        "target_column": target_column,
        "cardinality": cardinality,
        "via_table_id": via.get("via_table_id"),
        "via_source_column": via.get("via_source_column"),
        "via_target_column": via.get("via_target_column"),
    }


def test_many_to_one_puts_the_key_on_the_source_and_one_to_many_on_the_target():
    edges, skipped = relationship_edges(
        [
            _rel("visits-pet", 3, 1, "pet_id", "id", "many-to-one"),
            _rel("pet-visits", 1, 3, "id", "pet_id", "one-to-many"),
        ],
        _BY_ID,
    )
    assert skipped == []
    assert [
        (e.name, e.holder, e.holder_columns, e.referenced, e.referenced_columns) for e in edges
    ] == [
        ("provisa_fk_visits_pet", _BY_ID[3], ("pet_id",), _BY_ID[1], ("id",)),
        ("provisa_fk_pet_visits", _BY_ID[3], ("pet_id",), _BY_ID[1], ("id",)),
    ]


def test_junction_relationship_yields_one_edge_per_hop_with_composite_keys_split():
    edges, _ = relationship_edges(
        [
            _rel(
                "pets-treated-by-vets",
                1,
                2,
                "tenant, id",
                "id",
                "one-to-many",
                via_table_id=3,
                via_source_column="pet_tenant, pet_id",
                via_target_column="vet_id",
            )
        ],
        _BY_ID,
    )
    assert [e.name for e in edges] == [
        "provisa_fk_pets_treated_by_vets_source",
        "provisa_fk_pets_treated_by_vets_target",
    ]
    assert edges[0].holder == _BY_ID[3] and edges[0].holder_columns == ("pet_tenant", "pet_id")
    assert edges[0].referenced_columns == ("tenant", "id")
    assert edges[1].referenced == _BY_ID[2] and edges[1].holder_columns == ("vet_id",)


def test_computed_and_unregistered_relationships_are_skipped_with_a_reason():
    _, skipped = relationship_edges(
        [
            _rel("pets-fn", 1, None, "id", None, "many-to-one"),
            _rel("ghost", 99, 1, "x", "id", "many-to-one"),
        ],
        _BY_ID,
    )
    assert skipped == [
        ("pets-fn", "computed relationship has no target table"),
        ("ghost", "source table is not registered"),
    ]


def test_key_plan_keeps_keys_the_store_can_hold_and_withholds_the_rest():
    landed = _landed(
        {
            _BY_ID[1]: ("id",),
            _BY_ID[3]: ("id",),
            _BY_ID[4]: (),  # assignments: landed, no declared key
        }
    )
    rels = [
        _rel("visits-pet", 3, 1, "pet_id", "id", "many-to-one"),  # fine
        # the demo's breed_name link: not pets' key → withheld
        _rel("pets-to-shelter-assignments", 4, 1, "breed_name", "breed_name", "many-to-one"),
        # vets is not landed → withheld
        _rel("pets-vets", 1, 2, "id", "id", "many-to-one"),
    ]
    plan = key_plan(landed, rels, _BY_ID)
    assert [e.name for e in plan.edges] == ["provisa_fk_visits_pet"]
    reasons = dict(plan.withheld)
    assert (
        "not pet-store-sqlite.pet_store.pets's primary key"
        in reasons["provisa_fk_pets_to_shelter_assignments"]
    )
    assert reasons["provisa_fk_pets_vets"].endswith("both ends must be landed tables")
    assert plan.tables[_BY_ID[1]].primary_key == ("id",)
    assert plan.tables[_BY_ID[4]].primary_key == ()


def test_key_plan_covers_every_landed_table_regardless_of_data_product_membership():
    # The plan is about the landed model. Nothing here knows or asks about Data Products.
    plan = key_plan(_landed({_BY_ID[2]: ("id",)}), [], _BY_ID)
    assert set(plan.tables) == {_BY_ID[2]}
    assert plan.edges == [] and plan.withheld == []


def test_with_tags_attaches_table_and_column_assignments_by_id_or_ref():
    landed = _landed({_BY_ID[1]: ("id",), _BY_ID[2]: ()})
    assignments = [
        SimpleNamespace(
            tag_id="gold", object_type="table", table_id=1, column_name=None, reason="Curated"
        ),
        SimpleNamespace(
            tag_id="pii",
            object_type="column",
            table_id=None,
            table_ref=_BY_ID[1],
            column_name="name",
            reason=None,
        ),
        SimpleNamespace(
            tag_id="deprecated",
            object_type="relationship",
            table_id=None,
            relationship_id="x",
            column_name=None,
            reason=None,
        ),
        SimpleNamespace(
            tag_id="gold",
            object_type="table",
            table_id=99,
            column_name=None,
            reason=None,
            table_ref=None,
        ),
    ]
    out = {t.identity: t for t in with_tags(landed, assignments, _BY_ID)}
    assert out[_BY_ID[1]].tags == (("gold", "Curated"),)
    assert out[_BY_ID[1]].column_tags == {"name": (("pii", "pii"),)}
    assert out[_BY_ID[2]].tags == () and out[_BY_ID[2]].column_tags == {}


def test_derived_registration_matches_the_mv_store_table_to_its_registration_row():
    rows = [
        {"source_id": "__derived__", "schema_name": "views", "table_name": "dim-pet"},
        {"source_id": "pet-store-sqlite", "schema_name": "pet_store", "table_name": "mv_dim_pet"},
    ]
    assert derived_registration(rows, "mv_dim_pet") is rows[0]
    assert derived_registration(rows, "dim-pet") is rows[0]
    assert derived_registration(rows, "mv_other") is None
