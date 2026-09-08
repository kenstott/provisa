# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7d1e93-8b2f-4a65-9d3e-1f6a0c58b7e2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1658: the BigQuery landing store -- DDL-only converge with the registration's key, and the
landed model's keys, descriptions and labels on each landed table."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.bigquery_store import (
    bq_type,
    constraint_statements,
    create_ddl,
    described_table,
    label_key,
    label_value,
)
from provisa.federation.landed_keys import ForeignKeyEdge, KeyTarget

pytestmark = pytest.mark.unit

_PETS = ("proj", "pet_store", "pets")
_VISITS = ("proj", "pet_store", "pet_visits")
_EDGE = ForeignKeyEdge("provisa_fk_visits_pet", "visits", ("pet_id",), "pets", ("id",))


def _targets() -> dict[str, KeyTarget]:
    return {
        "pets": KeyTarget(
            replica=_PETS,
            view=None,
            primary_key=("id",),
            description="Pet inventory",
            column_descriptions={"name": "Pet name"},
            tags=(("gold", "Curated data"),),
        ),
        "visits": KeyTarget(replica=_VISITS, view=None, primary_key=("id",)),
    }


def test_bq_type_maps_ir_and_raises_on_unknown():
    assert bq_type("bigint") == "INT64"
    assert bq_type("timestamptz") == "TIMESTAMP"
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        bq_type("geography")


def test_create_ddl_declares_the_key_not_enforced():
    ddl = create_ddl(_PETS, [("id", "bigint"), ("name", "text")], ["id"])
    assert ddl == (
        "CREATE TABLE IF NOT EXISTS `proj.pet_store.pets` (`id` INT64, `name` STRING, "
        "PRIMARY KEY (`id`) NOT ENFORCED)"
    )
    assert create_ddl(_PETS, [("id", "bigint")], None).endswith("(`id` INT64)")


def test_label_key_and_value_fold_to_the_label_alphabet():
    assert label_key("PII") == "provisa_governance_pii"
    assert label_key("cost.center=X") == "provisa_governance_cost_center_x"
    assert label_value("Curated data") == "curated_data"
    assert len(label_value("x" * 100)) == 63


def test_constraint_statements_add_missing_keys_not_enforced():
    assert constraint_statements(_targets(), [_EDGE], {}, {}) == [
        "ALTER TABLE `proj.pet_store.pets` ADD PRIMARY KEY (`id`) NOT ENFORCED",
        "ALTER TABLE `proj.pet_store.pet_visits` ADD PRIMARY KEY (`id`) NOT ENFORCED",
        "ALTER TABLE `proj.pet_store.pet_visits` ADD CONSTRAINT `provisa_fk_visits_pet` "
        "FOREIGN KEY (`pet_id`) REFERENCES `proj.pet_store.pets` (`id`) NOT ENFORCED",
    ]


def test_constraint_statements_are_idempotent_and_withdraw_orphaned_owned_keys():
    pks = {_PETS: ("id",), _VISITS: ("id",)}
    fks = {_VISITS: frozenset({"provisa_fk_visits_pet", "provisa_fk_gone", "theirs"})}
    assert constraint_statements(_targets(), [_EDGE], pks, fks) == [
        "ALTER TABLE `proj.pet_store.pet_visits` DROP CONSTRAINT `provisa_fk_gone`"
    ]


def test_replacing_a_primary_key_re_adds_the_edges_into_it():
    pks = {_PETS: ("name",), _VISITS: ("id",)}
    fks = {_VISITS: frozenset({"provisa_fk_visits_pet"})}
    stmts = constraint_statements(_targets(), [_EDGE], pks, fks)
    assert stmts[0] == "ALTER TABLE `proj.pet_store.pets` DROP PRIMARY KEY"
    assert stmts[-1].startswith(
        "ALTER TABLE `proj.pet_store.pet_visits` ADD CONSTRAINT `provisa_fk_visits_pet`"
    )


def _table(description="", labels=None, columns=(("id", None), ("name", None))):
    schema = [
        SimpleNamespace(
            name=name, field_type="STRING", mode="NULLABLE", description=desc, fields=()
        )
        for name, desc in columns
    ]
    return SimpleNamespace(description=description, labels=dict(labels or {}), schema=schema)


def test_described_table_writes_description_columns_and_labels_when_absent():
    table = _table()
    assert described_table(table, _targets()["pets"], frozenset({"gold"})) == [
        "description",
        "schema",
        "labels",
    ]
    assert table.description == "Pet inventory"
    assert [(f.name, f.description) for f in table.schema] == [("id", None), ("name", "Pet name")]
    assert table.labels == {"provisa_governance_gold": "curated_data"}


def test_described_table_is_a_noop_when_everything_already_stands():
    table = _table(
        description="Pet inventory -- extended by a catalog",
        labels={"provisa_governance_gold": "curated_data", "theirs": "kept"},
        columns=(("id", None), ("name", "Pet name")),
    )
    assert described_table(table, _targets()["pets"], frozenset({"gold"})) == []


def test_described_table_removes_a_known_label_no_longer_assigned_and_keeps_others():
    table = _table(labels={"provisa_governance_silver": "x", "theirs": "kept"})
    target = KeyTarget(replica=_PETS, view=None, primary_key=("id",))
    assert described_table(table, target, frozenset({"gold", "silver"})) == ["labels"]
    assert table.labels == {"provisa_governance_silver": None, "theirs": "kept"}
