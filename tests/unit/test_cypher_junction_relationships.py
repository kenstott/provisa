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
from provisa.cypher.label_map import (
    CypherLabelMap,
    JunctionMapping,
    NodeMapping,
    RelationshipMapping,
    _upper_snake,
    junction_rel_type,
)
from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import cypher_to_sql
from provisa.cypher.translator_types import CypherTranslateError


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
        properties={"name": "name"},
    )


def _junction(rel_type: str = "KIND_OF") -> JunctionMapping:
    return JunctionMapping(
        catalog_name="provisa_admin",
        schema_name="public",
        table_name="glossary_term_edges",
        source_columns=("from_term_id",),
        target_columns=("to_term_id",),
        type_column="rel_type",
        type_value=rel_type,
        attributes={"note": "note"},
        label_source="column",
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


def test_a_declared_junction_is_not_a_node_label_in_the_graph_schema():
    """REQ-1586: once declared, the junction table leaves the node side of the Cypher schema.

    It is an edge, not an entity. Leaving it as a label would put it back on every surface the
    schema drives — a matchable label, a draggable pill in the graph sidebar, an entry under its
    domain — which is the reified-node modelling the declaration exists to replace. It stays a
    registered table and is still queryable in SQL and GraphQL; only the graph schema drops it.
    """
    from types import SimpleNamespace

    from provisa.cypher.label_map import _drop_junction_nodes

    edge_node = NodeMapping(
        label="Meta:GlossaryTermEdge",
        type_name="GlossaryTermEdge",
        domain_label="Meta",
        table_label="GlossaryTermEdge",
        table_id=2,
        source_id="provisa-admin",
        id_column="id",
        pk_columns=[],
        catalog_name="provisa_admin",
        schema_name="public",
        table_name="glossary_term_edges",
        properties={},
    )
    term = _term_node()
    nodes = {"GlossaryTerm": term, "GlossaryTermEdge": edge_node}
    domains = {"Meta": ["GlossaryTerm", "GlossaryTermEdge"]}
    nodes_by_table = {"GlossaryTerm": ["GlossaryTerm"], "GlossaryTermEdge": ["GlossaryTermEdge"]}
    ctx = SimpleNamespace(
        joins={
            "kindOf": SimpleNamespace(
                via=SimpleNamespace(table=SimpleNamespace(type_name="GlossaryTermEdge"))
            ),
            "owner": SimpleNamespace(via=None),
        }
    )

    _drop_junction_nodes(ctx, nodes, domains, nodes_by_table)

    assert "GlossaryTermEdge" not in nodes
    assert "GlossaryTerm" in nodes
    assert domains["Meta"] == ["GlossaryTerm"]
    assert "GlossaryTermEdge" not in nodes_by_table


def test_dropping_a_junction_node_leaves_no_empty_domain_or_table_bucket():
    """REQ-1586: a domain or table bucket that held only the junction goes with it, so no surface
    renders an empty group where the junction used to be."""
    from types import SimpleNamespace

    from provisa.cypher.label_map import _drop_junction_nodes

    edge_node = NodeMapping(
        label="Meta:GlossaryTermEdge",
        type_name="GlossaryTermEdge",
        domain_label="Meta",
        table_label="GlossaryTermEdge",
        table_id=2,
        source_id="provisa-admin",
        id_column="id",
        pk_columns=[],
        catalog_name="provisa_admin",
        schema_name="public",
        table_name="glossary_term_edges",
        properties={},
    )
    nodes = {"GlossaryTermEdge": edge_node}
    domains = {"Meta": ["GlossaryTermEdge"]}
    nodes_by_table = {"GlossaryTermEdge": ["GlossaryTermEdge"]}
    ctx = SimpleNamespace(
        joins={
            "kindOf": SimpleNamespace(
                via=SimpleNamespace(table=SimpleNamespace(type_name="GlossaryTermEdge"))
            )
        }
    )

    _drop_junction_nodes(ctx, nodes, domains, nodes_by_table)

    assert nodes == {}
    assert domains == {}
    assert nodes_by_table == {}


# ---------------------------------------------------------------------------
# Composite ends: each end lists its columns in order, paired positionally
# ---------------------------------------------------------------------------


def _composite_label_map() -> CypherLabelMap:
    via = JunctionMapping(
        catalog_name="provisa_admin",
        schema_name="public",
        table_name="glossary_term_edges",
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
    from provisa.cypher.parser import PathFunction
    from provisa.cypher.path_translator import PathTranslateError, path_to_recursive_sql

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


def test_a_single_hop_refuses_type_alternation() -> None:
    """REQ-1586: one hop resolves one relationship mapping, so an alternation has no answer.

    Silently taking the first type would draw KIND_OF edges and report them as though they were the
    whole set — the exact failure a junction table makes easy, since one junction backs several
    types between the same two tables. Say so instead; the caller writes one pattern per type.
    """
    lm = _label_map()
    ast = parse_cypher("MATCH (t:GlossaryTerm)-[r:KIND_OF|RELATED_TO]->(b:GlossaryTerm) RETURN r")
    with pytest.raises(CypherTranslateError, match="one pattern per type"):
        cypher_to_sql(ast, lm, {})
