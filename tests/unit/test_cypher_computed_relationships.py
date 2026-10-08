# Copyright (c) 2026 Kenneth Stott
# Canary: d35fbc6a-0bbe-4a8a-aec4-571b73ed4324
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship registered with a computed edge is traversed on its own condition, in both
directions (#148): the two directions of one relationship lower to the same condition, return the
same pairs of rows, and pass the relationship guard (REQ-603) for a role held to it."""

# Requirements: REQ-603

from __future__ import annotations

import duckdb
import pytest

from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import (
    approved_joins,
    computed_joins,
    tables_outside_relationships,
)
from provisa.compiler.stage2 import GovernanceContext
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import cypher_to_sql

_COLUMNS = {
    "orders": (1, ["id", "customer_code", "doc"]),
    "customers": (2, ["id", "code", "name"]),
    "regions": (3, ["id", "name"]),
}
# Each relationship: (type, source label, target label, source column, target column, computed).
_RELATIONSHIPS = {
    "source_expr": (
        "PLACED_BY",
        "Orders",
        "Customers",
        "customer_code",
        "code",
        {"source_expr": 'LOWER({alias}."customer_code")'},
    ),
    "target_expr": (
        "PLACED_BY",
        "Orders",
        "Customers",
        "customer_code",
        "code",
        {"target_expr": 'UPPER({alias}."code")'},
    ),
    "json_key": (
        "PLACED_BY",
        "Orders",
        "Customers",
        "doc",
        "code",
        {"source_expr": "JSON_EXTRACT_STRING({alias}.\"doc\", '$.code')"},
    ),
    "constant": ("HOME", "Customers", "Regions", "id", "id", {"source_constant": 1}),
}


def _meta(name: str) -> TableMeta:
    return TableMeta(
        table_id=_COLUMNS[name][0],
        field_name=name,
        type_name=name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name=name,
        domain_id="sales",
        source_type="postgresql",
    )


def _model(kind: str):
    rel_type, source, target, source_column, target_column, computed = _RELATIONSHIPS[kind]
    metas = {name: _meta(name) for name in _COLUMNS}
    ctx = CompilationContext()
    ctx.tables = dict(metas)
    ctx.joins = {
        (source, "edge"): JoinMeta(
            source_column=source_column,
            target_column=target_column,
            source_column_type="varchar",
            target_column_type="varchar",
            target=metas[target.lower()],
            cardinality="many-to-one",
            **computed,
        )
    }
    gov = GovernanceContext()
    gov.table_map = {f"public.{n}": tid for n, (tid, _) in _COLUMNS.items()}
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for _, (tid, cols) in _COLUMNS.items()}
    nodes = {
        name.capitalize(): NodeMapping(
            label=name.capitalize(),
            type_name=name.capitalize(),
            domain_label=None,
            table_label=name.capitalize(),
            table_id=tid,
            source_id="pg",
            id_column="id",
            pk_columns=[],
            catalog_name="pg",
            schema_name="public",
            table_name=name,
            properties={c: c for c in cols},
        )
        for name, (tid, cols) in _COLUMNS.items()
    }
    mapping = RelationshipMapping(
        rel_type=rel_type,
        source_label=source,
        target_label=target,
        join_source_column=source_column,
        join_target_column=target_column,
        field_name="edge",
        **computed,
    )
    label_map = CypherLabelMap(
        nodes=nodes, relationships={rel_type: mapping}, aliases={rel_type: [mapping]}
    )
    return ctx, gov, label_map, (rel_type, source, target)


def _refusals(sql_tree, ctx, gov) -> list[str]:
    computed, constants = computed_joins(ctx)
    return [
        v.message
        for v in tables_outside_relationships(
            sql_tree,
            gov,
            approved_joins(ctx),
            {m.table_id: m for m in ctx.tables.values()},
            computed=computed,
            constants=constants,
        )
    ]


@pytest.fixture
def engine():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute("CREATE TABLE pg.public.orders (id INTEGER, customer_code VARCHAR, doc VARCHAR)")
    con.execute("CREATE TABLE pg.public.customers (id INTEGER, code VARCHAR, name VARCHAR)")
    con.execute("CREATE TABLE pg.public.regions (id INTEGER, name VARCHAR)")
    con.execute(
        "INSERT INTO pg.public.orders VALUES "
        "(1, 'A1', '{\"code\": \"a1\"}'), (2, 'a1', '{\"code\": \"B2\"}'), "
        "(3, 'B2', '{\"code\": \"zz\"}'), (4, 'zz', '{\"code\": \"A1\"}')"
    )
    con.execute("INSERT INTO pg.public.customers VALUES (10, 'a1', 'ann'), (20, 'B2', 'bo')")
    con.execute("INSERT INTO pg.public.regions VALUES (1, 'east'), (2, 'west')")
    return con


def _pairs(engine, sql_tree) -> list[tuple]:
    return sorted(engine.execute(sql_tree.sql(dialect="duckdb")).fetchall())


@pytest.mark.parametrize("kind", list(_RELATIONSHIPS))
def test_both_directions_lower_to_the_registered_condition_and_match_the_same_rows(kind, engine):
    ctx, gov, label_map, (rel, source, target) = _model(kind)
    s, t = source[0].lower(), target[0].lower() + "2"
    forward = f"MATCH ({s}:{source})-[:{rel}]->({t}:{target}) RETURN {s}.id AS a, {t}.id AS b"
    backward = f"MATCH ({t}:{target})<-[:{rel}]-({s}:{source}) RETURN {s}.id AS a, {t}.id AS b"
    lowered = {}
    for name, cypher in (("forward", forward), ("backward", backward)):
        sql_tree, _, _ = cypher_to_sql(parse_cypher(cypher), label_map, {})
        assert _refusals(sql_tree, ctx, gov) == [], (name, sql_tree.sql(dialect="postgres"))
        lowered[name] = _pairs(engine, sql_tree)
    assert lowered["forward"] == lowered["backward"], lowered
    assert lowered["forward"], "the fixture's rows are related by this relationship"


def test_the_expression_decides_the_pairs_not_the_bare_columns(engine):
    """LOWER(orders.customer_code) = customers.code relates orders 1 and 2 to customer 10 only:
    the bare columns would relate order 2 alone, and order 3 ('B2') to customer 20."""
    ctx, gov, label_map, _ = _model("source_expr")
    backward = "MATCH (c:Customers)<-[:PLACED_BY]-(o:Orders) RETURN o.id AS a, c.id AS b"
    sql_tree, _, _ = cypher_to_sql(parse_cypher(backward), label_map, {})
    assert _pairs(engine, sql_tree) == [(1, 10), (2, 10)]


_OTHER_SHAPES = {
    "untyped_forward": "MATCH (o:Orders)-[r]->(c:Customers) RETURN o.id AS a, c.id AS b",
    "untyped_backward": "MATCH (c:Customers)<-[r]-(o:Orders) RETURN o.id AS a, c.id AS b",
    "undirected": "MATCH (o:Orders)-[:PLACED_BY]-(c:Customers) RETURN o.id AS a, c.id AS b",
    "variable_length": "MATCH (o:Orders)-[:PLACED_BY*1..2]->(c:Customers) RETURN o.id AS a, c.id AS b",
    "unlabeled_target": "MATCH (o:Orders)-[:PLACED_BY]->(c) RETURN o.id AS a, c.id AS b",
    "unlabeled_source": "MATCH (o)-[:PLACED_BY]->(c:Customers) RETURN o.id AS a, c.id AS b",
}


@pytest.mark.parametrize("kind", ["source_expr", "target_expr", "json_key"])
@pytest.mark.parametrize("shape", list(_OTHER_SHAPES))
def test_every_traversal_of_a_computed_relationship_is_its_registered_condition(
    kind, shape, engine
):
    """Untyped, undirected, variable-length and unlabeled-end traversals of the relationship
    lower to its own condition too: the guard passes them and they return the pairs the plain
    forward pattern returns."""
    ctx, gov, label_map, _ = _model(kind)
    plain, _, _ = cypher_to_sql(
        parse_cypher("MATCH (o:Orders)-[:PLACED_BY]->(c:Customers) RETURN o.id AS a, c.id AS b"),
        label_map,
        {},
    )
    sql_tree, _, _ = cypher_to_sql(parse_cypher(_OTHER_SHAPES[shape]), label_map, {})
    lowered = sql_tree.sql(dialect="postgres")
    assert _refusals(sql_tree, ctx, gov) == [], lowered
    assert sorted(set(_pairs(engine, sql_tree))) == sorted(set(_pairs(engine, plain))), lowered


_LEFT_POINTING = {
    "variable_length": (
        "MATCH (o:Orders)-[:PLACED_BY*1..2]->(c:Customers) RETURN o.id AS a, c.id AS b",
        "MATCH (c:Customers)<-[:PLACED_BY*1..2]-(o:Orders) RETURN o.id AS a, c.id AS b",
    ),
    "shortest_path": (
        "MATCH p = shortestPath((o:Orders)-[:PLACED_BY*1..2]->(c:Customers)) "
        "RETURN o.id AS a, c.id AS b",
        "MATCH p = shortestPath((c:Customers)<-[:PLACED_BY*1..2]-(o:Orders)) "
        "RETURN o.id AS a, c.id AS b",
    ),
}


@pytest.mark.parametrize("kind", ["plain", "source_expr", "target_expr", "json_key"])
@pytest.mark.parametrize("shape", list(_LEFT_POINTING))
def test_a_left_pointing_variable_length_pattern_matches_what_the_right_pointing_one_does(
    kind, shape, engine, monkeypatch
):
    """(c)<-[:R*1..2]-(o) is (o)-[:R*1..2]->(c) (#151): the same registered condition, the same
    pairs, for a plain relationship and a computed one."""
    monkeypatch.setitem(
        _RELATIONSHIPS, "plain", ("PLACED_BY", "Orders", "Customers", "customer_code", "code", {})
    )
    ctx, gov, label_map, _ = _model(kind)
    forward, backward = _LEFT_POINTING[shape]
    lowered = {}
    for name, cypher in (("forward", forward), ("backward", backward)):
        sql_tree, _, _ = cypher_to_sql(parse_cypher(cypher), label_map, {})
        assert _refusals(sql_tree, ctx, gov) == [], (name, sql_tree.sql(dialect="postgres"))
        lowered[name] = _pairs(engine, sql_tree)
    assert lowered["forward"] == lowered["backward"], lowered
    assert lowered["forward"], "the fixture's rows are related by this relationship"
