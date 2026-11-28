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


def test_a_declared_junction_is_not_a_node_label_in_the_graph_schema():
    """REQ-1586: once declared, the junction table leaves the node side of the Cypher schema.

    It is an edge, not an entity. Leaving it as a label would put it back on every surface the
    schema drives — a matchable label, a draggable pill in the graph sidebar, an entry under its
    domain — which is the reified-node modelling the declaration exists to replace. It stays a
    registered table and is still queryable in SQL and GraphQL; only the graph schema drops it.
    """

    from provisa.gql.label_map import _drop_junction_nodes

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
    kind_of, owner = _junction_edge(), _fk_edge("OWNER", "GlossaryTerm", "GlossaryTerm")
    relationships = {"KIND_OF": kind_of, "OWNER": owner}
    aliases = {"KIND_OF": [kind_of], "OWNER": [owner]}

    _drop_junction_nodes(nodes, relationships, domains, nodes_by_table, aliases)

    assert "GlossaryTermEdge" not in nodes
    assert "GlossaryTerm" in nodes
    assert domains["Meta"] == ["GlossaryTerm"]
    assert "GlossaryTermEdge" not in nodes_by_table
    # An edge between two surviving nodes is untouched by the drop.
    assert set(relationships) == {"KIND_OF", "OWNER"}


def test_dropping_a_junction_node_leaves_no_empty_domain_or_table_bucket():
    """REQ-1586: a domain or table bucket that held only the junction goes with it, so no surface
    renders an empty group where the junction used to be."""

    from provisa.gql.label_map import _drop_junction_nodes

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
    kind_of = _junction_edge()
    relationships = {"KIND_OF": kind_of}
    aliases = {"KIND_OF": [kind_of]}

    _drop_junction_nodes(nodes, relationships, domains, nodes_by_table, aliases)

    assert nodes == {}
    assert domains == {}
    assert nodes_by_table == {}


def test_the_junctions_own_fk_edges_go_with_the_node_they_terminate_on():
    """REQ-1586: the FK edges into and out of a junction table are exactly what the junction edge
    replaces. Left behind, they name an endpoint label that no longer exists — and every consumer
    that resolves an endpoint (the graph-schema endpoint list, a label count) reads that key
    directly, so the stale edge is a KeyError, not a cosmetic duplicate."""
    from provisa.gql.label_map import _drop_junction_nodes

    kind_of = _junction_edge()
    into = _fk_edge("GLOSSARY_TERM_EDGES", "GlossaryTerm", "GlossaryTermEdge")
    out_of = _fk_edge("GLOSSARY_TERM", "GlossaryTermEdge", "GlossaryTerm")
    nodes = {"GlossaryTerm": _term_node()}
    relationships = {"KIND_OF": kind_of, "IN": into, "OUT": out_of}
    aliases = {
        "KIND_OF": [kind_of],
        "GLOSSARY_TERM_EDGES": [into],
        "GLOSSARY_TERM": [out_of],
    }

    _drop_junction_nodes(nodes, relationships, {}, {}, aliases)

    assert set(relationships) == {"KIND_OF"}
    assert set(aliases) == {"KIND_OF"}
    # Every surviving endpoint resolves — which is the property the graph schema depends on.
    assert all(r.source_label in nodes and r.target_label in nodes for r in relationships.values())


def _edge_node() -> NodeMapping:
    return NodeMapping(
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


def _registered_edge(rel_type: str, source: str, target: str) -> RelationshipMapping:
    """An FK/PK edge a registered relationship row declares (as each hop of a Join through)."""
    rm = _fk_edge(rel_type, source, target)
    rm.registered = True
    return rm


def test_a_junction_its_own_registered_relationships_join_keeps_its_node_and_edges():
    """REQ-1586 (amended 2026-11-18): a junction that registered relationships of its own join
    keeps its node and those edges, so after a Join through the two hops are offered in Cypher
    beside the joined relationship, as in SQL and GraphQL."""
    from provisa.gql.label_map import _drop_junction_nodes

    kind_of = _junction_edge()
    into = _registered_edge("GLOSSARY_TERM_EDGES", "GlossaryTerm", "GlossaryTermEdge")
    out_of = _registered_edge("GLOSSARY_TERM", "GlossaryTermEdge", "GlossaryTerm")
    nodes = {"GlossaryTerm": _term_node(), "GlossaryTermEdge": _edge_node()}
    domains = {"Meta": ["GlossaryTerm", "GlossaryTermEdge"]}
    nodes_by_table = {"GlossaryTerm": ["GlossaryTerm"], "GlossaryTermEdge": ["GlossaryTermEdge"]}
    relationships = {"KIND_OF": kind_of, "IN": into, "OUT": out_of}
    aliases = {"KIND_OF": [kind_of], "GLOSSARY_TERM_EDGES": [into], "GLOSSARY_TERM": [out_of]}

    _drop_junction_nodes(nodes, relationships, domains, nodes_by_table, aliases)

    assert set(nodes) == {"GlossaryTerm", "GlossaryTermEdge"}
    assert domains == {"Meta": ["GlossaryTerm", "GlossaryTermEdge"]}
    assert nodes_by_table["GlossaryTermEdge"] == ["GlossaryTermEdge"]
    assert set(relationships) == {"KIND_OF", "IN", "OUT"}
    assert set(aliases) == {"KIND_OF", "GLOSSARY_TERM_EDGES", "GLOSSARY_TERM"}


def test_a_junction_only_synthetic_edges_reach_stays_dropped():
    """REQ-1586: an edge no registered relationship declares (HAS_TABLE, which every table gets)
    is not a relationship of the junction's own, so the junction's node still leaves the graph and
    the edge goes with it."""
    from provisa.gql.label_map import _drop_junction_nodes

    kind_of = _junction_edge()
    has_table = _fk_edge("HAS_TABLE", "GlossaryTermEdge", "Meta_RegisteredTables")
    nodes = {"GlossaryTerm": _term_node(), "GlossaryTermEdge": _edge_node()}
    relationships = {"KIND_OF": kind_of, "HAS": has_table}
    aliases = {"KIND_OF": [kind_of], "HAS_TABLE": [has_table]}

    _drop_junction_nodes(nodes, relationships, {}, {}, aliases)

    assert set(nodes) == {"GlossaryTerm"}
    assert set(relationships) == {"KIND_OF"}
    assert set(aliases) == {"KIND_OF"}


def test_a_registered_join_marks_its_relationship_mapping_registered():
    """REQ-1586: the registered loop's JoinMeta says it came from a relationship row, and the
    label map carries that onto the edge; a synthetic join (HAS_TABLE) does not."""
    from provisa.compiler.sql_types import CompilationContext, JoinMeta, TableMeta
    from provisa.gql.label_map import _build_relationship_mappings

    def _tm(table_id: int, type_name: str) -> TableMeta:
        return TableMeta(
            table_id=table_id,
            field_name=type_name.lower(),
            type_name=type_name,
            source_id="s",
            catalog_name="c",
            schema_name="public",
            table_name=type_name.lower(),
        )

    joins = {
        ("Inquiries", "pets"): JoinMeta(
            source_column="pet_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=_tm(2, "Pets"),
            cardinality="many-to-one",
            registered=True,
        ),
        ("Inquiries", "_meta"): JoinMeta(
            source_column="__table_id__",
            target_column="table_query_name",
            source_column_type="text",
            target_column_type="text",
            target=_tm(3, "Meta_RegisteredTables"),
            cardinality="many-to-one",
            cypher_alias="HAS_TABLE",
        ),
    }
    ctx = CompilationContext(joins=joins)
    relationships: dict[str, RelationshipMapping] = {}

    _build_relationship_mappings(ctx, relationships)

    by_type = {rm.rel_type: rm for rm in relationships.values()}
    assert by_type["HAS_TABLE"].registered is False
    assert [rm.registered for t, rm in by_type.items() if t != "HAS_TABLE"] == [True]


# ---------------------------------------------------------------------------
# Composite ends: each end lists its columns in order, paired positionally
# ---------------------------------------------------------------------------


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
            "junction_table_name": r.via.registered_name if r.via else None,
            "properties": sorted(r.properties.keys()),
        }

    assert payload(junction) == {
        "type": "KIND_OF",
        "junction_table_name": "glossary_term_edges",
        "properties": ["note"],
    }
    assert payload(fk) == {"type": "OWNER", "junction_table_name": None, "properties": []}


def test_a_junction_read_through_another_relation_is_named_by_its_registered_name():
    """REQ-1586: /data/graph-schema names a junction by the table a client knows, never the relation
    the model reads it through: the ontology's term links are rows of _term_edges, read through the
    _term_edges_ontology view, and a client finds them by _term_edges (REQ-2174: the Ontology View
    writes its own Cypher over exactly those link types)."""
    from provisa.compiler.sql_types import JunctionMeta, TableMeta
    from provisa.gql.label_map import _junction_mapping

    via = JunctionMeta(
        table=TableMeta(
            table_id=7,
            field_name="on_term_edges",
            type_name="OnTermEdges",
            source_id="provisa-admin",
            catalog_name="provisa_admin",
            schema_name="public",
            table_name="_term_edges_ontology",
            domain_id="ontology",
            original_table_name="_term_edges",
        ),
        source_columns=("from_term_id",),
        target_columns=("to_term_id",),
        type_column="rel_type",
        type_value="KIND_OF",
        label_source="column",
    )
    mapped = _junction_mapping(via, {})
    assert mapped is not None
    # SQL reads the relation; the client is told the registered name.
    assert mapped.table_name == "_term_edges_ontology"
    assert mapped.registered_name == "_term_edges"
    # A junction read under its own name is named by it.
    assert _junction().registered_name == "glossary_term_edges"
