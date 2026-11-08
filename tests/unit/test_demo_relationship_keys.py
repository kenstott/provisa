# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3f6c1e-7d2b-4e85-b0c4-5e8a1f2d7c93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A many-to-one relationship declares its target column to be the target table's key
(``repositories/relationship.py`` flags it ``is_primary_key``), and the landing table is built with
that PRIMARY KEY. The demo GraphQL data has to honour every such claim: ``pets-to-shelter-assignments``
declared many-to-one on ``assignments.breed_name`` while four keepers were assigned to "African
Lion", so every event-loop land of the demo failed on ``assignments_pkey`` -- forever, every 5s."""

from pathlib import Path

import pytest
import yaml

from demo.graphql_server import server as demo

pytestmark = pytest.mark.unit

_CONFIGS = ("config/provisa-install.yaml", "config/provisa.yaml")
_DEMO_ROWS: dict[str, list[dict]] = {
    "animal_breeds": demo._BREEDS,
    "employees": demo._EMPLOYEES,
    "assignments": demo._ASSIGNMENTS,
    "schedules": demo._SCHEDULES,
}


def _many_to_one_targets(path: str) -> list[tuple[str, str, str]]:
    cfg = yaml.safe_load(Path(path).read_text())
    return [
        (r["id"], r["target_table_id"], r["target_column"])
        for r in cfg.get("relationships", [])
        if r.get("cardinality") == "many-to-one" and r["target_table_id"] in _DEMO_ROWS
    ]


@pytest.mark.parametrize("path", _CONFIGS)
def test_every_many_to_one_target_column_is_unique_in_the_demo_data(path):
    rels = _many_to_one_targets(path)
    assert rels, f"{path}: no many-to-one relationship targets a GraphQL demo table"
    for rel_id, table, column in rels:
        values = [row[column] for row in _DEMO_ROWS[table]]
        dupes = sorted({v for v in values if values.count(v) > 1})
        assert not dupes, (
            f"{path}: {rel_id} is many-to-one on {table}.{column} but it repeats: {dupes}"
        )


def _config_table_columns(path: str) -> dict[str, set[str]]:
    cfg = yaml.safe_load(Path(path).read_text())
    cols: dict[str, set[str]] = {}
    for t in cfg.get("tables", []):
        cols.setdefault(t["table"], set()).update(c["name"] for c in t.get("columns", []))
    return cols


@pytest.mark.parametrize("path", _CONFIGS)
def test_seeded_graphql_demo_relationships_key_on_landed_columns(path):
    """Every relationship the boot seeds for the graphql-demo source must key on a column the config
    lands for its source/target table. A relationship on a column the config does not land refuses
    the startup column-drop of the stale column (ColumnDropRefused on ``schedules.employee``): the
    schedules->employees FK must key on the landed ``employee_id`` scalar, not the raw ``employee``
    object field the config no longer lands. Tables registered only from live GQL introspection (not
    in the static config, e.g. ``animal_breeds``) are out of scope here."""
    from provisa.api.app_startup import _DEMO_GRAPHQL_RELATIONSHIPS

    cols = _config_table_columns(path)
    checked = 0
    for rel_id, src_tbl, tgt_tbl, src_col, tgt_col, *_rest in _DEMO_GRAPHQL_RELATIONSHIPS:
        if src_tbl in cols:
            checked += 1
            assert src_col in cols[src_tbl], (
                f"{path}: {rel_id} keys source_column {src_tbl}.{src_col}, not a landed column: "
                f"{sorted(cols[src_tbl])}"
            )
        if tgt_tbl in cols:
            assert tgt_col in cols[tgt_tbl], (
                f"{path}: {rel_id} keys target_column {tgt_tbl}.{tgt_col}, not a landed column: "
                f"{sorted(cols[tgt_tbl])}"
            )
    assert checked, f"{path}: no seeded relationship had a statically-configured source table"


@pytest.mark.parametrize("path", _CONFIGS)
def test_no_relationship_makes_breed_name_a_key_of_pets_or_assignments(path):
    """The replica build keys the "one" side of every relationship, and failed with "The key
    (breed_name) could not be built: the source repeats its values" because pets and assignments
    were joined on breed_name, a column that repeats on both. Pets reach assignments through
    animal_breeds; the landed keys computed from the config and the boot-time seeds must name
    breed_name only on animal_breeds."""
    from provisa.api.app_startup import _DEMO_GRAPHQL_RELATIONSHIPS
    from provisa.federation.landed_keys import relationship_edges

    cfg = yaml.safe_load(Path(path).read_text())
    seeded = [
        {
            "id": rel_id,
            "source_table_id": src,
            "target_table_id": tgt,
            "source_column": src_col,
            "target_column": tgt_col,
            "cardinality": cardinality,
        }
        for rel_id, src, tgt, src_col, tgt_col, cardinality, _alias in _DEMO_GRAPHQL_RELATIONSHIPS
    ]
    declared = [r for r in cfg["relationships"] if not r.get("via_table")]
    tables = {r[k] for r in [*declared, *seeded] for k in ("source_table_id", "target_table_id")}
    ids = {name: i for i, name in enumerate(sorted(tables))}
    by_id = {i: name for name, i in ids.items()}
    rels = [
        {
            **r,
            "source_table_id": ids[r["source_table_id"]],
            "target_table_id": ids[r["target_table_id"]],
        }
        for r in [*declared, *seeded]
    ]
    edges, _skipped = relationship_edges(rels, by_id)
    keyed = {
        (edge.referenced, column)
        for edge in edges
        for column in edge.referenced_columns
        if column == "breed_name"
    }
    assert not keyed & {("pets", "breed_name"), ("assignments", "breed_name")}, (
        f"{path}: breed_name is a replica key of {sorted(keyed)}"
    )
