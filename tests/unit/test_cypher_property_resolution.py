# Copyright (c) 2026 Kenneth Stott
# Canary: 0a8b0f69-e9b6-4d72-b219-ff8dfbbd6fd5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A property of a node or relationship is one its label publishes for the role (REQ-2136): any
other name -- one the label does not have, a column it does not publish (a governed object
column) or one the role may not see -- is null, as a graph's missing property is, in WHERE,
RETURN, ORDER BY and any expression; no column is ever read by a name the label does not give
it. Every node has one built-in property, ``_id``: its key. Each case runs on DuckDB against the
rows it must answer."""

# Requirements: REQ-2136, REQ-345

import json

import duckdb
import pytest

from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_cypher_junction_relationships import _term_node
from tests.unit.test_sql_gen import _build_schema_and_ctx
from tests.unit.test_where_relationships import RELATIONSHIPS


@pytest.fixture
def terms():
    lm = CypherLabelMap(nodes={"GlossaryTerm": _term_node()}, relationships={})
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    con.execute(
        "CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR, _id__ INTEGER)"
    )
    con.execute("INSERT INTO provisa_admin.public.glossary_terms VALUES (1, 'ab', 1), (2, 'cd', 1)")
    return lm, con


def _rows(terms, cypher: str) -> list[tuple]:
    lm, con = terms
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")
    return con.execute(transpile_to_duckdb(sql)).fetchall()


def test_a_condition_on_a_property_the_label_has_not_matches_nothing(terms):
    # _id__ is a column of the table the label does not publish: never read by its name.
    assert _rows(terms, "MATCH (n:GlossaryTerm) WHERE n._id__ = 1 RETURN n.name") == []
    assert _rows(terms, "MATCH (n:GlossaryTerm) WHERE n.nope = 'ab' RETURN n.name") == []


def test_a_property_the_label_has_not_is_null_where_it_is_returned_and_ordered(terms):
    assert sorted(
        _rows(terms, "MATCH (n:GlossaryTerm) RETURN n.name, n.nope ORDER BY n.nope, n.name")
    ) == [("ab", None), ("cd", None)]
    assert _rows(
        terms, "MATCH (n:GlossaryTerm) WHERE n.nope IS NULL RETURN n.name ORDER BY n.name"
    ) == [("ab",), ("cd",)]
    assert _rows(
        terms, "MATCH (n:GlossaryTerm) RETURN coalesce(n.nope, n.name) AS v ORDER BY v"
    ) == [("ab",), ("cd",)]


def test_the_built_in_id_is_the_key(terms):
    assert _rows(terms, "MATCH (n:GlossaryTerm) WHERE n._id = 2 RETURN n._id, n.name") == [
        (2, "cd")
    ]
    assert _rows(terms, "MATCH (n:GlossaryTerm) RETURN n.name ORDER BY n._id DESC") == [
        ("cd",),
        ("ab",),
    ]


def test_a_returned_node_carries_its_id_among_its_properties(terms):
    (row,) = _rows(terms, "MATCH (n:GlossaryTerm) WHERE n.name = 'ab' RETURN n")
    assert json.loads(row[0])["properties"]["_id"] == 1


def _compound() -> tuple[CypherLabelMap, duckdb.DuckDBPyConnection]:
    """A label keyed by (id, name), two of its rows sharing the first key column."""
    term = _term_node()
    term.pk_columns = ["id", "name"]
    term.key_columns = ("id", "name")
    lm = CypherLabelMap(nodes={"GlossaryTerm": term}, relationships={})
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    con.execute("CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR)")
    con.execute(
        "INSERT INTO provisa_admin.public.glossary_terms VALUES (1, 'ab'), (1, 'a|b'), (2, 'cd')"
    )
    return lm, con


def test_a_compound_key_s_id_is_its_node_identity_of_every_key_column():
    assert _rows(
        _compound(), "MATCH (n:GlossaryTerm) WHERE n._id = 'GlossaryTerm|2|cd' RETURN n.name"
    ) == [("cd",)]


def test_two_rows_sharing_a_compound_key_s_first_column_are_two_nodes():
    rows = _rows(_compound(), "MATCH (n:GlossaryTerm) WHERE n.id = 1 RETURN n, n._id")
    nodes = [json.loads(n) for n, _ in rows]
    # A | of a key's own value is escaped: it never reads as the separator.
    assert sorted(n["id"] for n in nodes) == ["GlossaryTerm|1|a\\|b", "GlossaryTerm|1|ab"]
    assert sorted(i for _, i in rows) == ["GlossaryTerm|1|a\\|b", "GlossaryTerm|1|ab"]
    assert sorted(n["properties"]["_id"] for n in nodes) == sorted(i for _, i in rows)


def _domains() -> tuple[CypherLabelMap, duckdb.DuckDBPyConnection]:
    """The meta model's own label map (its Meta:Domains node), over a seeded domains table."""
    import importlib.util
    import sys
    from pathlib import Path

    repo = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "cypher_golden", repo / "scripts" / "cypher_golden.py"
    )
    assert spec is not None and spec.loader is not None
    cypher_golden = importlib.util.module_from_spec(spec)
    sys.modules["cypher_golden"] = cypher_golden
    spec.loader.exec_module(cypher_golden)
    lm = cypher_golden.model_label_map(
        repo / "tests" / "fixtures" / "graphql_golden" / "meta_model.yaml"
    )
    nm = lm.nodes["M__Domains"]
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {nm.catalog_name}")
    con.execute(f"CREATE SCHEMA {nm.catalog_name}.{nm.schema_name}")
    cols = ", ".join(f'"{c}" VARCHAR' for c in nm.properties.values())
    con.execute(f"CREATE TABLE {nm.catalog_name}.{nm.schema_name}.{nm.table_name} ({cols})")
    con.execute(
        f"INSERT INTO {nm.catalog_name}.{nm.schema_name}.{nm.table_name} (id, description) "
        "VALUES ('pet-store', 'pets'), ('sales', 's')"
    )
    return lm, con


def test_a_domain_is_found_by_its_id():
    domains = _domains()
    assert _rows(
        domains, "MATCH (n:Meta:Domains) WHERE n._id = 'pet-store' RETURN n._id LIMIT 25"
    ) == [("pet-store",)]
    # The maintainer's read: the made-up name is null, so nothing matches, and nothing fails.
    assert (
        _rows(
            domains,
            "MATCH (n:Meta:Domains) WHERE n.id = 'pet-store' AND n._id__ = 1 RETURN n LIMIT 25",
        )
        == []
    )


@pytest.fixture
def orders():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA sales")
    con.execute(
        "CREATE TABLE sales.orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), "
        "region VARCHAR, status VARCHAR, created_at TIMESTAMP)"
    )
    con.execute(
        "INSERT INTO sales.orders VALUES (10, 1, 5, 'us', 'new', NULL), "
        "(11, 2, 7, 'eu', 'new', NULL)"
    )
    return con


def _role_rows(con, cypher: str, role_id: str, governed: set | None = None) -> list[tuple]:
    _, ctx = _build_schema_and_ctx(relationships=RELATIONSHIPS, role_id=role_id)
    if governed is not None:
        ctx.gql_governed_object_cols = governed
    lm = CypherLabelMap.from_schema(ctx, domain_access=["*"])
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql_ast, graph_vars, lm).sql(dialect="postgres")
    return sorted(con.execute(transpile_to_duckdb(make_semantic_sql(sql, ctx))).fetchall())


def test_a_column_the_role_may_not_see_is_null_as_an_unknown_property_is(orders):
    # amount and region are visible to admin alone (test_sql_gen's model).
    read = "MATCH (o:Orders) RETURN o.id, o.amount, o.region"
    assert _role_rows(orders, read, "admin") == [(10, 5, "us"), (11, 7, "eu")]
    assert _role_rows(orders, read, "analyst") == [(10, None, None), (11, None, None)]
    where = "MATCH (o:Orders) WHERE o.amount > 6 RETURN o.id"
    assert _role_rows(orders, where, "admin") == [(11,)]
    assert _role_rows(orders, where, "analyst") == []


def test_a_governed_object_column_is_null_not_its_value(orders):
    read = "MATCH (o:Orders) RETURN o.id, o.status"
    assert _role_rows(orders, read, "admin") == [(10, "new"), (11, "new")]
    assert _role_rows(orders, read, "admin", governed={(1, "status")}) == [(10, None), (11, None)]


def _write(cypher: str, role_id: str = "admin") -> str:
    from provisa.cypher.write_translator import WriteTranslator, parse_cypher_write

    _, ctx = _build_schema_and_ctx(relationships=RELATIONSHIPS, role_id=role_id)
    lm = CypherLabelMap.from_schema(ctx, domain_access=["*"])
    return WriteTranslator(lm).translate(parse_cypher_write(cypher))


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (o:Orders) WHERE o.id = 10 SET o.nope = 'x'",
        "CREATE (o:Orders {id: 12, nope: 'x'})",
    ],
)
def test_a_write_of_a_property_the_label_has_not_is_refused_by_name(cypher):
    from provisa.cypher.write_translator import CypherWriteParseError

    with pytest.raises(CypherWriteParseError, match="Orders has no property nope"):
        _write(cypher)


def test_a_write_of_a_column_the_role_may_not_see_is_refused_as_an_unknown_one_is():
    from provisa.cypher.write_translator import CypherWriteParseError

    assert '"amount" = 9' in _write("MATCH (o:Orders) WHERE o.id = 10 SET o.amount = 9")
    with pytest.raises(CypherWriteParseError, match="Orders has no property amount"):
        _write("MATCH (o:Orders) WHERE o.id = 10 SET o.amount = 9", role_id="analyst")


def test_a_write_matches_an_unknown_property_as_a_read_does_and_its_key_by_id():
    # Read in a write's match, the unknown name is null: the write touches no row.
    assert "WHERE NULL = 'x'" in _write("MATCH (o:Orders) WHERE o.nope = 'x' DELETE o")
    assert 'WHERE "id" = 10' in _write("MATCH (o:Orders) WHERE o._id = 10 DELETE o")


def _compound_chain() -> tuple[CypherLabelMap, duckdb.DuckDBPyConnection]:
    """The compound-keyed terms, (1, 'ab') NEXT (2, 'cd'); (1, 'a|b') shares (1, 'ab')'s first
    key column and has no NEXT."""
    from provisa.gql.label_map import RelationshipMapping

    term = _term_node()
    term.pk_columns = ["id", "name"]
    term.key_columns = ("id", "name")
    term.properties = {**term.properties, "nextId": "next_id"}
    nxt = RelationshipMapping(
        rel_type="NEXT",
        source_label="GlossaryTerm",
        target_label="GlossaryTerm",
        join_source_column="next_id",
        join_target_column="id",
        field_name="next",
    )
    lm = CypherLabelMap(
        nodes={"GlossaryTerm": term}, relationships={"NEXT": nxt}, aliases={"NEXT": [nxt]}
    )
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    con.execute(
        "CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR, next_id INTEGER)"
    )
    con.execute(
        "INSERT INTO provisa_admin.public.glossary_terms VALUES "
        "(1, 'ab', 2), (1, 'a|b', NULL), (2, 'cd', NULL)"
    )
    return lm, con


def test_a_node_a_graph_function_returns_has_the_identity_return_n_gives_it():
    chain = _compound_chain()
    (row,) = _rows(
        chain,
        "MATCH (a:GlossaryTerm)-[r:NEXT]->(b:GlossaryTerm) RETURN a, startNode(r), endNode(r), b",
    )
    a, start, end, b = (json.loads(v) for v in row)
    assert a["id"] == start["id"] == "GlossaryTerm|1|ab"
    assert b["id"] == end["id"] == "GlossaryTerm|2|cd"


def test_a_variable_length_path_tells_rows_sharing_a_key_column_apart():
    chain = _compound_chain()
    walk = "MATCH (a:GlossaryTerm)-[:NEXT*1..2]->(b:GlossaryTerm) WHERE a.name = '{}' RETURN b.name"
    assert _rows(chain, walk.format("ab")) == [("cd",)]
    # (1, 'a|b') has no NEXT: the walk from (1, 'ab') is not its.
    assert _rows(chain, walk.format("a|b")) == []


def test_a_path_expression_tells_rows_sharing_a_key_column_apart():
    import dataclasses

    from provisa.gql.ir import EdgeStep, MatchStep, PathMode, RepeatPath

    lm, con = _compound_chain()

    def reached(name: str) -> list[tuple]:
        ast = parse_cypher(
            f"MATCH (a:GlossaryTerm)-[:NEXT]->(b:GlossaryTerm) WHERE a.name = '{name}' "
            "RETURN b.name AS b"
        )
        step = ast.pipeline[0]
        assert isinstance(step, MatchStep)
        pattern = step.matches[0].pattern
        pattern.rels[0] = dataclasses.replace(  # pyright: ignore[reportAttributeAccessIssue]
            pattern.rels[0],  # pyright: ignore[reportAttributeAccessIssue]
            types=[],
            path=RepeatPath(EdgeStep("NEXT"), 1, 2),
            mode=PathMode.WALK,
        )
        sql = cypher_to_sql(ast, lm, {})[0].sql(dialect="duckdb")
        return con.execute(sql).fetchall()

    assert reached("ab") == [("cd",)]
    assert reached("a|b") == []
