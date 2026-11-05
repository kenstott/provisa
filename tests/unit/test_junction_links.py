# Copyright (c) 2026 Kenneth Stott
# Canary: 7c4e1a92-5b08-4d3f-a6e9-0f2d8b1c3a57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1586: a junction's rows reach either end.

pets -> pets through pet_companions gave the two pets their companions, and gave the junction table
nothing: a pet_companions row had no ``pet`` and no ``companion`` to follow. Each junction implies
two many-to-one links, from the junction to its source and to its target, given once however many
typed junctions run over the same columns, and served as relationships by the schema build and the
relationships API. In Cypher the junction stays an edge, as REQ-1586 settled it.
"""

# Requirements: REQ-1586

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.junction_links import (
    IMPLIED_PREFIX,
    implied_junction_links,
    table_keys,
    with_junction_links,
)

PETS, COMPANIONS, VETS, FINALS = 1, 2, 3, 4
#: pets.id is the pets table's key (as the pet store's own primary key marks it at introspection).
_KEYS = {(PETS, "id")}


def _junction(rel_id: str, type_value: str) -> dict:
    return {
        "id": rel_id,
        "source_table_id": PETS,
        "target_table_id": PETS,
        "source_column": "id",
        "target_column": "id",
        "cardinality": "one-to-many",
        "source_table_name": "pets",
        "target_table_name": "pets",
        "via_table_id": COMPANIONS,
        "via_table_name": "pet_companions",
        "via_domain_id": "ps",
        "via_source_column": "pet_id",
        "via_target_column": "companion_pet_id",
        "via_type_column": "relation_type",
        "via_type_value": type_value,
        "via_label_source": "column",
    }


_TYPED = [
    _junction("pets-bonded-pair", "bonded pair"),
    _junction("pets-littermate", "littermate"),
    _junction("pets-shares-enclosure", "shares enclosure"),
]
_PLAIN = {
    "id": "pets-to-vets",
    "source_table_id": PETS,
    "target_table_id": VETS,
    "source_column": "vet_id",
    "target_column": "id",
    "cardinality": "many-to-one",
    "via_table_id": None,
}


def test_typed_junctions_over_the_same_columns_imply_each_link_once() -> None:
    links = implied_junction_links([*_TYPED, _PLAIN], _KEYS, "apollo_graphql")
    shape = [
        (
            link["source_table_id"],
            link["source_column"],
            link["target_table_id"],
            link["target_column"],
            link["cardinality"],
            link["graphql_alias"],
            link["cypher_type"],
            link["disable_cypher"],
        )
        for link in links
    ]
    assert shape == [
        (COMPANIONS, "companion_pet_id", PETS, "id", "many-to-one", "companionPet", None, True),
        (COMPANIONS, "pet_id", PETS, "id", "many-to-one", "pet", None, True),
    ]  # fmt: skip
    for link in links:
        assert link["id"].startswith(IMPLIED_PREFIX)
        assert link["derived_from"] == sorted(r["id"] for r in _TYPED)
        assert (link["source_table_name"], link["source_domain_id"]) == ("pet_companions", "ps")
        assert link["via_table_id"] is None


def test_an_end_matched_on_a_column_that_is_not_its_key_implies_nothing() -> None:
    """A junction from countries to the finals each won matches finals by a name the finals repeat
    (finals.winners): a junction row reaches many finals there, not one, so no link is implied."""
    won = {
        **_junction("country-won-final", ""),
        "target_table_id": FINALS,
        "target_column": "winners",
        "via_label_source": "fixed",
    }
    assert [link["target_table_id"] for link in implied_junction_links([won], _KEYS, "snake")] == [
        PETS
    ]
    assert implied_junction_links([won], set(), "snake") == []


def test_a_relationship_with_no_junction_implies_nothing() -> None:
    assert implied_junction_links([_PLAIN], _KEYS, "apollo_graphql") == []


def test_a_link_a_steward_declared_is_not_implied_beside_it() -> None:
    declared = {
        "id": "companions-to-pet",
        "source_table_id": COMPANIONS,
        "target_table_id": PETS,
        "source_column": "pet_id",
        "target_column": "id",
        "cardinality": "many-to-one",
        "via_table_id": None,
    }
    links = implied_junction_links([*_TYPED, declared], _KEYS, "apollo_graphql")
    assert [link["source_column"] for link in links] == ["companion_pet_id"]


def test_the_field_is_spelled_in_the_deployments_convention() -> None:
    links = implied_junction_links(_TYPED[:1], _KEYS, "snake")
    assert sorted(link["graphql_alias"] for link in links) == ["companion_pet", "pet"]


def test_served_relationships_are_the_declared_ones_and_the_implied() -> None:
    served = with_junction_links([*_TYPED, _PLAIN], _KEYS, "apollo_graphql")
    assert served[:4] == [*_TYPED, _PLAIN]
    assert len(served) == 6


# --- the schema build and the label map ------------------------------------------------------------


def _column(name: str, *, key: bool = False) -> dict:
    return {"column_name": name, "visible_to": ["*"], "is_primary_key": key}


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "ps",
        "schema_name": "public",
        "table_name": name,
        "governance": "pre-approved",
        "columns": [_column(c, key=c == "id") for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


@pytest.fixture
def built(monkeypatch):
    """The real role build over pets and pet_companions, with the three typed junctions."""
    import provisa.api.app as appmod
    from provisa.api import app_loaders
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    monkeypatch.setattr(app_loaders, "warm_role_surfaces", lambda _state: None, raising=False)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)
    tables = [
        _table(PETS, "pets", ["id", "name"]),
        _table(COMPANIONS, "pet_companions", ["pet_id", "companion_pet_id", "relation_type"]),
    ]
    integer = lambda n: ColumnMetadata(column_name=n, data_type="integer", is_nullable=False)  # noqa: E731
    text = lambda n: ColumnMetadata(column_name=n, data_type="varchar", is_nullable=True)  # noqa: E731
    relationships = with_junction_links(
        [{**r, "alias": None, "graphql_alias": None} for r in _TYPED],
        table_keys(tables),
        "apollo_graphql",
    )
    app_loaders._build_and_register_schemas(
        roles=[
            {
                "id": "analyst",
                "description": "t",
                "capabilities": ["usage"],
                "domain_access": ["ps"],
            }
        ],
        tables=tables,
        relationships=relationships,
        col_types_converted={
            PETS: [integer("id"), text("name")],
            COMPANIONS: [integer("pet_id"), integer("companion_pet_id"), text("relation_type")],
        },
        naming_rules=[],
        domains=[{"id": "ps"}],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
    )
    return SimpleNamespace(state=state, tables=tables, relationships=relationships)


def test_the_junctions_type_has_a_field_to_either_end(built) -> None:
    from graphql import get_named_type

    from provisa.api.role_surface import role_surface

    schema = role_surface(built.state, "analyst").schema
    junction = next(
        t
        for name, t in schema.type_map.items()
        if not name.startswith("__") and "relationType" in (getattr(t, "fields", None) or {})
    )
    pet_type = get_named_type(junction.fields["pet"].type)
    companion_type = get_named_type(junction.fields["companionPet"].type)
    assert pet_type is companion_type
    assert "name" in pet_type.fields


def test_in_cypher_the_junction_stays_an_edge_and_its_links_bring_back_no_node(built) -> None:
    """REQ-1586: a declared junction is an edge in the graph schema, not a node: its label and the
    foreign-key edges into and out of it are dropped (label_map._drop_junction_nodes). The links it
    implies are such edges, so they go with it, and the typed junction edges are what Cypher
    traverses."""
    from provisa.api.role_surface import role_surface
    from provisa.gql.label_map import CypherLabelMap

    ctx = role_surface(built.state, "analyst").context
    label_map = CypherLabelMap.from_schema(
        ctx,
        domain_access=["ps"],
        all_tables=built.tables,
        all_relationships=built.relationships,
    )
    types = {rm.rel_type for rm in label_map.relationships.values()}
    assert {"BONDED_PAIR", "LITTERMATE", "SHARES_ENCLOSURE"} <= types
    assert not types & {"BELONGS_TO_PET", "BELONGS_TO_COMPANION_PET"}
    assert not any(nm.table_id == COMPANIONS for nm in label_map.nodes.values())


# --- the relationships API ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_api_lists_the_implied_links_marked_derived() -> None:
    from provisa.api.admin._row_mappers import _rel_from_row
    from provisa.api.admin.schema_query import _with_implied

    fetched = [
        SimpleNamespace(
            _mapping={
                **r,
                "materialize": False,
                "refresh_interval": 300,
                "target_function_name": None,
                "function_arg": None,
                "alias": None,
                "graphql_alias": None,
                "graphql_field": "bondedPair",
                "cypher_type": "BONDED_PAIR",
                "disable_cypher": False,
                "same_entity": False,
                "parameters": {},
                "source_domain_id": "ps",
            }
        )
        for r in _TYPED
    ]

    class _Conn:
        async def execute_core(self, _stmt):
            keyed = [SimpleNamespace(table_id=PETS, column_name="id")]
            return SimpleNamespace(fetchall=lambda: keyed)

    listed = [_rel_from_row(r) for r in await _with_implied(_Conn(), fetched)]
    implied = [r for r in listed if r.derived_from is not None]
    assert [(r.source_table_name, r.source_column, r.graphql_alias) for r in implied] == [
        ("pet_companions", "companion_pet_id", "companionPet"),
        ("pet_companions", "pet_id", "pet"),
    ]
    assert all(r.derived_from == sorted(x["id"] for x in _TYPED) for r in implied)
    assert all(r.derived_from is None for r in listed if not r.id.startswith(IMPLIED_PREFIX))
