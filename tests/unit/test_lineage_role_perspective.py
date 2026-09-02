# Copyright (c) 2026 Kenneth Stott
# Canary: 3f81c04a-9d27-4e15-8b60-7c2a5e9d1f34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1625/REQ-1626: Complete Lineage read from a role's vantage point.

The role is a LENS, not a redaction: it picks the seeds (what that role can query) and the graph is
everything those seeds derive from, ancestors included in full whether the role reaches them or not.
"""

from __future__ import annotations

from provisa.api.admin.lineage_router import _role_seeds, _with_registry_columns
from provisa.lineage.graph import Edge, LineageGraph, Node
from provisa.lineage.merge import MergedGraph, ancestor_closure, build_federation_graph


def _rows() -> list[dict]:
    return [
        {
            "domain_id": "pet-store",
            "table_name": "pets",
            "column_name": "price",
            "visible_to": ["vet"],
        },
        {
            "domain_id": "pet-store",
            "table_name": "pets",
            "column_name": "name",
            "visible_to": ["vet", "clerk"],
        },
        {
            "domain_id": "pet-store",
            "table_name": "dim_pet",
            "column_name": "name",
            "visible_to": ["clerk"],
        },
        {
            "domain_id": "hr",
            "table_name": "staff",
            "column_name": "salary",
            "visible_to": ["vet"],
        },
    ]


def test_seeds_are_the_columns_the_role_can_query() -> None:
    seeds, relations = _role_seeds(_rows(), {"clerk"}, set())
    assert seeds == {"pet_store.pets.name", "pet_store.dim_pet.name"}
    assert relations == {"pet_store.pets", "pet_store.dim_pet"}


def test_no_role_selected_seeds_every_registered_column() -> None:
    seeds, _ = _role_seeds(_rows(), set(), set())
    assert "hr.staff.salary" in seeds
    assert "pet_store.pets.price" in seeds


def test_domain_filter_restricts_seeds_only() -> None:
    seeds, relations = _role_seeds(_rows(), {"vet"}, {"pet-store"})
    assert seeds == {"pet_store.pets.price", "pet_store.pets.name"}
    assert "hr.staff" not in relations


def test_ancestors_are_kept_in_full_even_when_the_role_cannot_see_them() -> None:
    # dim_pet.name derives from pets.name, which THIS role has no grant on. Provenance is the
    # question, so the ancestor is returned — the role only decided where the walk starts.
    graph = LineageGraph()
    for nid, col, rel in [
        ("pet_store.pets.name", "name", "pet_store.pets"),
        ("pet_store.dim_pet.name", "name", "pet_store.dim_pet"),
        ("pet_store.mart.label", "label", "pet_store.mart"),
    ]:
        graph.add_node(Node(id=nid, column=col, relation=rel, kind="derived", materialized=False))
    graph.add_edge(Edge("pet_store.pets.name", "pet_store.dim_pet.name", "name", []))
    graph.add_edge(Edge("pet_store.dim_pet.name", "pet_store.mart.label", "name", []))

    scoped = ancestor_closure(graph, {"pet_store.dim_pet.name"})

    assert set(scoped.nodes) == {"pet_store.pets.name", "pet_store.dim_pet.name"}
    # Downstream of the seed belongs to another perspective and is cut.
    assert "pet_store.mart.label" not in scoped.nodes
    assert len(scoped.edges) == 1


def test_registered_table_with_no_derivation_still_appears() -> None:
    merged = MergedGraph(graph=LineageGraph())
    rows = [
        {
            "domain_id": "pet-store",
            "table_name": "pets",
            "column_name": "price",
            "visible_to": ["vet"],
        }
    ]

    out = _with_registry_columns(merged, rows, {"pet_store.pets"})

    node = out.graph.nodes["pet_store.pets.price"]
    assert node.relation == "pet_store.pets"
    assert node.kind == "source"


def test_registry_columns_do_not_mutate_the_cached_merged_graph() -> None:
    cached = MergedGraph(graph=LineageGraph())
    rows = [
        {
            "domain_id": "pet-store",
            "table_name": "pets",
            "column_name": "price",
            "visible_to": ["vet"],
        }
    ]

    _with_registry_columns(cached, rows, {"pet_store.pets"})

    assert cached.graph.nodes == {}


def test_base_table_reference_stitches_to_its_registered_relation() -> None:
    # The view writes pet_store.pets; sqlglot drops the schema, so without the registry's relation
    # list the base table would appear twice — once as a bare source, once as a registry entry.
    fed = build_federation_graph(
        [("pet_store.dim_pet", "SELECT name FROM pet_store.pets")],
        extra_relations=["pet_store.pets"],
    )

    assert "pet_store.pets.name" in fed.graph.nodes
    assert "pets.name" not in fed.graph.nodes


def test_an_ambiguous_bare_name_is_left_unstitched() -> None:
    # Two domains own a table called `pets`; guessing one would attach the lineage to the wrong model.
    fed = build_federation_graph(
        [("pet_store.dim_pet", "SELECT name FROM pets")],
        extra_relations=["pet_store.pets", "shelter.pets"],
    )

    assert "pets.name" in fed.graph.nodes


def test_federation_requires_the_governance_right() -> None:
    """REQ-1628: the lens names ANY role, which discloses that role's ``visible_to``, so the endpoint
    carries the same gate the governance columns carry."""
    import asyncio
    from types import SimpleNamespace

    import pytest

    from provisa.api import app as app_module
    from provisa.api.admin.lineage_router import federation_graph
    from provisa.api.errors import ApiError

    identity = SimpleNamespace(user_id="analyst-1", roles=["analyst"])
    request = SimpleNamespace(state=SimpleNamespace(identity=identity))
    original = app_module.state.roles
    try:
        app_module.state.roles = {"analyst": {"capabilities": ["query_development"]}}
        with pytest.raises(ApiError) as err:
            asyncio.run(federation_graph(request))
        assert err.value.status_code == 403
    finally:
        app_module.state.roles = original
