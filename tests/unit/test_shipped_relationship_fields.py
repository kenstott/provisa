# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No shipped configuration names two relationships of one type under one field (REQ-194).

The schema build refuses two relationships of one GraphQL type under one field
(RelationshipFieldTaken), and a refused build answers every request of the deployment. A
relationship's field is named for the table it reaches and its cardinality, unless it takes a
GraphQL alias of its own; so two relationships from one table to another with one cardinality
and no alias to tell them apart are a refused build.

What a node of a shipped configuration holds is its declared relationships, the ones its boot
seeds for the graphql-demo source (app_startup._DEMO_GRAPHQL_RELATIONSHIPS), and the ones that
source's schema offers (graphql_remote.mapper) less those a declared one already is
(repositories.relationship). Nothing checked them together, and the demo's own install config
shipped a refused build."""

# Requirements: REQ-194, REQ-313

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_CONFIGS = sorted(
    [
        *(_REPO / "config").glob("provisa*.yaml"),
        *(_REPO / "demo" / "named").glob("*/config.yaml"),
    ]
)


def _key(rel: dict) -> tuple:
    return (
        rel["source_table_id"],
        rel["target_table_id"],
        rel["source_column"],
        rel["target_column"],
    )


def _offered_by_graphql_demo() -> list[dict]:
    """The relationships the graphql-demo source's schema offers, as its mapper offers them."""
    from demo.graphql_server.server import schema
    from provisa.graphql_remote.introspect import INTROSPECTION_QUERY
    from provisa.graphql_remote.mapper import map_schema

    result = schema.execute_sync(INTROSPECTION_QUERY)
    assert not result.errors and result.data is not None, result.errors
    return map_schema(result.data["__schema"], "", "graphql-demo", "")[2]


def _held(path: Path) -> list[dict]:
    from provisa.api.app_startup import _DEMO_GRAPHQL_RELATIONSHIPS

    cfg = yaml.safe_load(path.read_text()) or {}
    declared = [r for r in cfg.get("relationships") or [] if not r.get("via_table")]
    if not any(s.get("id") == "graphql-demo" for s in cfg.get("sources") or []):
        return declared
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
    by_id = {r["id"]: r for r in [*declared, *seeded]}
    kept = {_key(r) for r in by_id.values()}
    for offered in _offered_by_graphql_demo():
        if offered["id"] in by_id or _key(offered) in kept:
            continue  # seeded under its own id, or a declared relationship is it
        by_id[offered["id"]] = offered
    return list(by_id.values())


@pytest.mark.parametrize("path", _CONFIGS, ids=lambda p: str(p.relative_to(_REPO)))
def test_no_two_relationships_of_one_type_are_named_for_one_field(path):
    fields: dict[tuple, list[str]] = defaultdict(list)
    for rel in _held(path):
        field = rel.get("graphql_alias") or (rel["target_table_id"], rel["cardinality"])
        fields[(rel["source_table_id"], field)].append(rel["id"])
    taken = {field: ids for field, ids in fields.items() if len(ids) > 1}
    assert not taken, f"{path.name}: relationships named for one field: {taken}"
