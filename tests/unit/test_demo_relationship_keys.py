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
