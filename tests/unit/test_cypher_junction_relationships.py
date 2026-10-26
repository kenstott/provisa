# Copyright (c) 2026 Kenneth Stott
# Canary: 5f2b6c81-9d4a-4e37-bb10-2c8e7a4d6f93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1586: junction tables declared as first-class Cypher relationships."""

import pytest

from provisa.core.models import Relationship
from provisa.gql.label_map import (
    CypherLabelMap,
    JunctionMapping,
    NodeMapping,
    RelationshipMapping,
    _upper_snake,
    junction_rel_type,
)
from provisa.cypher.parser import parse_cypher
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CypherTranslateError


def _term_node() -> NodeMapping:
    return NodeMapping(
        label="GlossaryTerm",
        type_name="GlossaryTerm",
        domain_label=None,
        table_label="GlossaryTerm",
        table_id=1,
        source_id="provisa-admin",
        id_column="id",
        pk_columns=[],
        catalog_name="provisa_admin",
        schema_name="public",
        table_name="glossary_terms",
        # The key is a property, as every published column is (label_map.from_schema).
        properties={"id": "id", "name": "name"},
    )


def _junction(rel_type: str = "KIND_OF") -> JunctionMapping:
    return JunctionMapping(
        catalog_name="provisa_admin",
        schema_name="public",
        table_name="glossary_term_edges",
        type_name="GlossaryTermEdge",
        source_columns=("from_term_id",),
        target_columns=("to_term_id",),
        type_column="rel_type",
        type_value=rel_type,
        attributes={"note": "note"},
        label_source="column",
    )


def _junction_edge(rel_type: str = "KIND_OF") -> RelationshipMapping:
    """The junction-backed edge itself: its endpoints are the two real nodes, not the junction."""
    return RelationshipMapping(
        rel_type=rel_type,
        source_label="GlossaryTerm",
        target_label="GlossaryTerm",
        join_source_column="id",
        join_target_column="id",
        field_name="edges",
        via=_junction(rel_type),
    )


def _fk_edge(rel_type: str, source: str, target: str) -> RelationshipMapping:
    """An ordinary FK/PK edge — no via, so nothing marks it as a junction edge."""
    return RelationshipMapping(
        rel_type=rel_type,
        source_label=source,
        target_label=target,
        join_source_column="id",
        join_target_column="term_id",
        field_name=rel_type.lower(),
    )


def _label_map(rel_type: str = "KIND_OF") -> CypherLabelMap:
    via = _junction(rel_type)
    rm = RelationshipMapping(
        rel_type=rel_type,
        source_label="GlossaryTerm",
        target_label="GlossaryTerm",
        join_source_column="id",
        join_target_column="id",
        field_name="edges",
        via=via,
    )
    return CypherLabelMap(
        nodes={"GlossaryTerm": _term_node()},
        relationships={rel_type: rm},
        aliases={rel_type: [rm]},
    )


# ---------------------------------------------------------------------------
# Naming: the three nominations, all upper-snake
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("kind_of", "KIND_OF"),
        ("KIND_OF", "KIND_OF"),
        ("relatedTo", "RELATED_TO"),
        ("part of", "PART_OF"),
        ("glossary-term-edges", "GLOSSARY_TERM_EDGES"),
    ],
)
def test_nominated_label_is_upper_snake(raw: str, expected: str) -> None:
    assert _upper_snake(raw) == expected


def test_label_from_discriminator_value() -> None:
    assert junction_rel_type(_junction("kind_of")) == "KIND_OF"


def test_label_from_junction_table_name() -> None:
    via = JunctionMapping(
        catalog_name="c",
        schema_name="s",
        table_name="glossary_term_edges",
        type_name="GlossaryTermEdge",
        source_columns=("a",),
        target_columns=("b",),
        label_source="table",
    )
    assert junction_rel_type(via) == "GLOSSARY_TERM_EDGES"


def test_label_from_fixed_alias() -> None:
    via = JunctionMapping(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        type_name="T",
        source_columns=("a",),
        target_columns=("b",),
        label_source="fixed",
        label_fixed="isA",
    )
    assert junction_rel_type(via) == "IS_A"


def test_missing_nomination_is_a_declaration_error() -> None:
    via = JunctionMapping(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        type_name="T",
        source_columns=("a",),
        target_columns=("b",),
    )
    with pytest.raises(ValueError, match="no label source"):
        junction_rel_type(via)


# ---------------------------------------------------------------------------
# Differentiation: junction-backed vs FK/PK-backed
# ---------------------------------------------------------------------------


def test_relationship_kind_is_derived_from_via_table() -> None:
    direct = Relationship(
        id="r1",
        source_table_id="1",
        target_table_id="2",
        source_column="a",
        target_column="b",
        cardinality="many-to-one",
    )
    assert direct.kind == "direct"
    junction = direct.model_copy(
        update={
            "via_table": "glossary_term_edges",
            "via_source_column": "from_term_id",
            "via_target_column": "to_term_id",
            "via_label_source": "table",
        }
    )
    assert junction.kind == "junction"


def _sql(query: str, lm: CypherLabelMap | None = None) -> str:
    ast, _, _ = cypher_to_sql(parse_cypher(query), lm or _label_map(), {})
    return ast.sql(dialect="trino")


# ---------------------------------------------------------------------------
# Emission: two joins through the junction, discriminated
# ---------------------------------------------------------------------------


def test_pattern_joins_through_the_junction_table() -> None:
    sql = _sql("MATCH (t:GlossaryTerm)-[r:KIND_OF]->(b:GlossaryTerm) RETURN b.name", _label_map())
    assert "glossary_term_edges" in sql
    assert "r__via" in sql
    # source hop, discriminator, target hop
    assert 't."id" = r__via."from_term_id"' in sql
    assert "'KIND_OF'" in sql
    assert 'r__via."to_term_id" = b."id"' in sql


def test_edge_attribute_is_readable_and_filterable() -> None:
    sql = _sql(
        "MATCH (t:GlossaryTerm)-[r:KIND_OF]->(b:GlossaryTerm) WHERE r.note = 'x' RETURN r.note",
    )
    assert sql.count('r__via."note"') >= 2
    assert "'x'" in sql


def test_variable_length_counts_whole_hops() -> None:
    sql = _sql(
        "MATCH (t:GlossaryTerm)-[r:KIND_OF*1..3]->(b:GlossaryTerm) RETURN b.name", _label_map()
    )
    # One junction join per hop, in both the seed and the recursive branch.
    assert sql.count("glossary_term_edges") >= 2
    assert sql.count("'KIND_OF'") >= 2


def test_a_junction_edge_names_the_table_its_attributes_came_from():
    """REQ-1586: the edge object carries a junctionTable meta field beside its properties.

    An inspector showing an edge's attributes has to say where they were read from, and it cannot
    re-derive that from the result alone. The field sits beside properties, never inside, so a
    junction column happening to be called junctionTable cannot displace it.
    """
    lm = _label_map()
    ast = parse_cypher("MATCH ()-[r:KIND_OF]->() RETURN r")
    sql, _, _ = cypher_to_sql(ast, lm, {})
    text = sql.sql()
    assert "'junctionTable'" in text
    assert "'glossary_term_edges'" in text


def test_show_all_edges_carries_the_junction_meta_field_too():
    """REQ-1586: the all-relationships union is the show-all-edges query, and its edges are the
    same edges — a junction edge reached that way names its junction exactly as a typed match does.
    """
    lm = _label_map()
    ast = parse_cypher("MATCH p=()-->() RETURN p LIMIT 25")
    sql, _, _ = cypher_to_sql(ast, lm, {})
    text = sql.sql()
    assert "'junctionTable'" in text
    assert "'glossary_term_edges'" in text


# --- a relationship is a relationship (REQ-1586, amended) ------------------------------------------

_PETS, _COMPANIONS = 1, 2


def _pets_table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "ps",
        "schema_name": "public",
        "table_name": name,
        "governance": "pre-approved",
        "columns": [{"column_name": c, "visible_to": ["*"]} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _pets_relationship(rel_id: str, **fields) -> dict:
    return {
        "id": rel_id,
        "alias": None,
        "graphql_alias": None,
        "via_table_id": None,
        "via_source_column": None,
        "via_target_column": None,
        "via_type_column": None,
        "via_type_value": None,
        "via_label_source": None,
        **fields,
    }


@pytest.fixture
def pets_label_map(monkeypatch):
    """The real role build over pets and pet_companions: the typed junction pets -> pets through
    pet_companions, and the junction table's own two relationships, registered like any other."""
    import provisa.api.app as appmod
    from provisa.api import app_loaders
    from provisa.api.role_surface import role_surface
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "source_catalogs", {"pg": "pg"}, raising=False)
    tables = [
        _pets_table(_PETS, "pets", ["id", "name"]),
        _pets_table(
            _COMPANIONS, "pet_companions", ["id", "pet_id", "companion_pet_id", "relation_type"]
        ),
    ]
    relationships = [
        _pets_relationship(
            "pets-bonded-pair",
            cardinality="one-to-many",
            source_table_id=_PETS,
            source_column="id",
            target_table_id=_PETS,
            target_column="id",
            via_table_id=_COMPANIONS,
            via_table_name="pet_companions",
            via_source_column="pet_id",
            via_target_column="companion_pet_id",
            via_type_column="relation_type",
            via_type_value="bonded pair",
            via_label_source="column",
        ),
        _pets_relationship(
            "pet-companions-pet",
            graphql_alias="pet",
            cardinality="many-to-one",
            source_table_id=_COMPANIONS,
            source_column="pet_id",
            target_table_id=_PETS,
            target_column="id",
        ),
        _pets_relationship(
            "pet-companions-companion",
            graphql_alias="companion",
            cardinality="many-to-one",
            source_table_id=_COMPANIONS,
            source_column="companion_pet_id",
            target_table_id=_PETS,
            target_column="id",
        ),
    ]
    integer = lambda n: ColumnMetadata(column_name=n, data_type="integer", is_nullable=False)  # noqa: E731
    text = lambda n: ColumnMetadata(column_name=n, data_type="varchar", is_nullable=True)  # noqa: E731
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
            _PETS: [integer("id"), text("name")],
            _COMPANIONS: [
                integer("id"),
                integer("pet_id"),
                integer("companion_pet_id"),
                text("relation_type"),
            ],
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
    surface = role_surface(state, "analyst")
    label_map = CypherLabelMap.from_schema(
        surface.context, domain_access=["ps"], all_tables=tables, all_relationships=relationships
    )
    return surface, label_map


def test_a_junction_table_is_a_node_label_like_any_other_table(pets_label_map) -> None:
    """REQ-1586 (amended, A RELATIONSHIP IS A RELATIONSHIP): declaring a junction over a table
    leaves the table a table -- a node label a pattern can match, under its domain."""
    _surface, label_map = pets_label_map
    junction_nodes = [nm for nm in label_map.nodes.values() if nm.table_id == _COMPANIONS]
    assert len(junction_nodes) == 1
    assert junction_nodes[0].type_name in label_map.domains[junction_nodes[0].domain_label]


def test_the_junction_tables_relationships_are_ordinary_edges(pets_label_map) -> None:
    """Its relationships to the pet and the companion are edges like any foreign key's, beside
    the declared junction edge, which is still served directly between the two pets."""
    _surface, label_map = pets_label_map
    junction = next(nm.type_name for nm in label_map.nodes.values() if nm.table_id == _COMPANIONS)
    pet = next(nm.type_name for nm in label_map.nodes.values() if nm.table_id == _PETS)
    out_of_junction = {
        rm.field_name
        for rm in label_map.relationships.values()
        if rm.via is None and rm.source_label == junction and rm.target_label == pet
    }
    assert out_of_junction == {"pet", "companion"}
    direct = [rm for rm in label_map.relationships.values() if rm.via is not None]
    assert [(rm.rel_type, rm.source_label, rm.target_label) for rm in direct] == [
        ("BONDED_PAIR", pet, pet)
    ]
    assert all(
        r.source_label in label_map.nodes and r.target_label in label_map.nodes
        for r in label_map.relationships.values()
    )


def test_the_junction_tables_graphql_type_reaches_the_pet_and_the_companion(
    pets_label_map,
) -> None:
    from graphql import get_named_type

    surface, _label_map = pets_label_map
    junction = next(
        t
        for name, t in surface.schema.type_map.items()
        if not name.startswith("__") and "relationType" in (getattr(t, "fields", None) or {})
    )
    pet_type = get_named_type(junction.fields["pet"].type)
    assert get_named_type(junction.fields["companion"].type) is pet_type
    assert "name" in pet_type.fields


def _composite_label_map() -> CypherLabelMap:
    via = JunctionMapping(
        catalog_name="provisa_admin",
        schema_name="public",
        table_name="glossary_term_edges",
        type_name="GlossaryTermEdge",
        source_columns=("from_domain", "from_term_id"),
        target_columns=("to_domain", "to_term_id"),
        label_source="table",
    )
    rm = RelationshipMapping(
        rel_type="GLOSSARY_TERM_EDGES",
        source_label="GlossaryTerm",
        target_label="GlossaryTerm",
        join_source_column="domain,id",
        join_target_column="domain,id",
        field_name="edges",
        via=via,
    )
    return CypherLabelMap(
        nodes={"GlossaryTerm": _term_node()},
        relationships={"GLOSSARY_TERM_EDGES": rm},
        aliases={"GLOSSARY_TERM_EDGES": [rm]},
    )


def test_a_composite_junction_end_pairs_every_column() -> None:
    sql = _sql(
        "MATCH (t:GlossaryTerm)-[r:GLOSSARY_TERM_EDGES]->(b:GlossaryTerm) RETURN b.name",
        _composite_label_map(),
    )
    assert 't."domain" = r__via."from_domain"' in sql
    assert 't."id" = r__via."from_term_id"' in sql
    assert 'r__via."to_domain" = b."domain"' in sql
    assert 'r__via."to_term_id" = b."id"' in sql


def test_a_key_list_that_does_not_pair_is_a_declaration_error() -> None:
    lm = _composite_label_map()
    lm.relationships["GLOSSARY_TERM_EDGES"].join_source_column = "id"
    with pytest.raises(ValueError, match="pairs 2 source"):
        _sql(
            "MATCH (t:GlossaryTerm)-[r:GLOSSARY_TERM_EDGES]->(b:GlossaryTerm) RETURN b.name",
            lm,
        )


def test_a_composite_junction_pairs_every_column_in_a_variable_length_path() -> None:
    sql = _sql(
        "MATCH (t:GlossaryTerm)-[r:GLOSSARY_TERM_EDGES*1..3]->(b:GlossaryTerm) RETURN b.name",
        _composite_label_map(),
    )
    assert '_seed."domain" = _nxt__via."from_domain"' in sql
    assert '_seed."id" = _nxt__via."from_term_id"' in sql
    assert '_cur."domain" = _nxt__via."from_domain"' in sql
    assert '_nxt__via."to_term_id" = _nxt."id"' in sql


def test_the_recursive_path_builder_refuses_a_composite_junction() -> None:
    """path_to_recursive_sql carries one scalar end id per step, so composite has no pairing."""
    from provisa.gql.ir import PathFunction
    from provisa.gql.path_translator import PathTranslateError, path_to_recursive_sql

    lm = _composite_label_map()
    ast = parse_cypher(
        "MATCH p = shortestPath((t:GlossaryTerm)-[:GLOSSARY_TERM_EDGES*1..3]->(b:GlossaryTerm)) "
        "RETURN p"
    )
    clause = next(c for c in ast.match_clauses if isinstance(c.pattern, PathFunction))
    with pytest.raises(PathTranslateError, match="composite key"):
        path_to_recursive_sql(clause.pattern, lm, "t", "b", "p", 3)


def test_a_path_step_edge_carries_the_junction_attributes() -> None:
    """REQ-1586: an edge inside a returned path is the same edge as a returned relationship.

    A path is what the graph surface draws, so a junction edge reached through ``RETURN p`` has to
    arrive with the junction's columns as its properties and the junction named — otherwise the
    inspector shows an empty edge for exactly the edges that have attributes.
    """
    lm = _label_map()
    ast = parse_cypher("MATCH p=(t:GlossaryTerm)-[r:KIND_OF]->(b:GlossaryTerm) RETURN p")
    sql, _, _ = cypher_to_sql(ast, lm, {})
    text = sql.sql()
    assert "'properties', JSON_OBJECT('note', r__via.\"note\")" in text
    assert "'junctionTable', 'glossary_term_edges'" in text


@pytest.mark.parametrize("spelling", ["KIND_OF|RELATED_TO", "KIND_OF|:RELATED_TO"])
def test_type_alternation_parses_both_spellings(spelling: str) -> None:
    """REQ-1586: `:A|B` is the spelling Cypher 5 documents; `:A|:B` is the older one.

    A surface that expands a dropped pill has to know which types it may name, and it can only find
    that out if the pattern it writes parses at all. Both spellings are the same pattern.
    """
    ast = parse_cypher(f"MATCH (t:GlossaryTerm)-[r:{spelling}]->(b:GlossaryTerm) RETURN r")
    assert ast.match_clauses[0].pattern.rels[0].types == ["KIND_OF", "RELATED_TO"]


def test_a_single_hop_alternation_naming_an_undeclared_type_is_refused() -> None:
    """REQ-1586: a single-hop alternation is one junction's rows of each type it names, so a type
    the graph does not declare (RELATED_TO here) is refused, never dropped.

    Silently taking the declared types would draw KIND_OF edges and report them as though they were
    the whole set. Say so instead; the caller writes one pattern per type.
    """
    lm = _label_map()
    ast = parse_cypher("MATCH (t:GlossaryTerm)-[r:KIND_OF|RELATED_TO]->(b:GlossaryTerm) RETURN r")
    with pytest.raises(CypherTranslateError, match="one pattern per type"):
        cypher_to_sql(ast, lm, {})


def test_graph_schema_relationship_payload_distinguishes_the_two_edge_shapes():
    """REQ-1586: /data/graph-schema names the junction table and its relationship properties.

    A client reading the graph schema has to render the two edge shapes differently — a junction
    edge offers filterable attributes and an FK edge offers none — and the endpoint payload is the
    only place it can learn which is which, since the junction table is not in node_labels.
    """
    junction, fk = _junction_edge(), _fk_edge("OWNER", "GlossaryTerm", "GlossaryTerm")

    def payload(r):
        return {
            "type": r.rel_type,
            "junction_table_name": r.via.table_name if r.via else None,
            "properties": sorted(r.properties.keys()),
        }

    assert payload(junction) == {
        "type": "KIND_OF",
        "junction_table_name": "glossary_term_edges",
        "properties": ["note"],
    }
    assert payload(fk) == {"type": "OWNER", "junction_table_name": None, "properties": []}
