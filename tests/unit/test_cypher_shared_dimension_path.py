# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e5a93-2f4d-4b8e-9a06-3d5b8e1f0c27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher path through a shared dimension (REQ-413): the dimension is a view of each value its
sides' columns hold (provisa.discovery.dimension.view_sql), and each side joins it many-to-one
under one relationship type named for it. An OPTIONAL path from one side through the dimension to
another compiles to a join through the dimension's column, with the rest of the path held by a
correlated EXISTS (REQ-2135); governance reads that statement with the view expanded (REQ-603),
and the EXISTS is related to the view along the side's relationship to it, never a cross join of
the view's branches."""

# Requirements: REQ-413, REQ-603, REQ-2135

import dataclasses

import duckdb
import pytest
import sqlglot

from provisa.compiler import join_guard
from provisa.compiler.view_expand import expand_view_refs
from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_cypher_min_hops import _node

T = "IS_NEIGHBORHOOD_DIMENSION"
DIM = "Neighborhood"
#: label -> table; table ids are positions, the dimension's last.
TABLES = {"Budget": "budget", "Complaints": "complaints", "Permits": "permits", DIM: "neighborhood"}
IDS = {table: i + 1 for i, table in enumerate(TABLES.values())}


def _view_sql(sides: list[str]) -> str:
    """The dimension's SQL as discovery writes it: each value the sides hold, once."""
    return " UNION ".join(
        f'SELECT "neighborhood" AS "neighborhood" FROM pg.public.{TABLES[s]} '
        f'WHERE "neighborhood" IS NOT NULL'
        for s in sides
    )


def _shelter(label: str) -> NodeMapping:
    props = (
        {"neighborhood": "neighborhood"}
        if label == DIM
        else {"id": "id", "neighborhood": "neighborhood"}
    )
    node = _node(label, TABLES[label])
    if label == DIM:
        # A shared dimension is a view: a table of the derived source, read under its catalog
        # (discovery.dimension); under another catalog its name is that catalog's table.
        node = dataclasses.replace(node, source_id="__derived__", catalog_name="__derived__")
    return dataclasses.replace(
        node,
        type_name=f"Shelter__{label}",
        label=f"Shelter:{label}",
        domain_label="Shelter",
        domain_id="shelter",
        table_label=label,
        id_column="neighborhood" if label == DIM else "id",
        properties=props,
    )


def _model(sides: list[str]) -> CypherLabelMap:
    """Each side many-to-one to the dimension, every one of the same type (named for it)."""
    nodes = {f"Shelter__{label}": _shelter(label) for label in [*sides, DIM]}
    rels = {
        f"{T}::Shelter__{s}→Shelter__{DIM}": RelationshipMapping(
            rel_type=T,
            source_label=f"Shelter__{s}",
            target_label=f"Shelter__{DIM}",
            join_source_column="neighborhood",
            join_target_column="neighborhood",
            field_name="neighborhood_dimension",
        )
        for s in sides
    }
    return CypherLabelMap(
        nodes=nodes,
        relationships=rels,
        aliases={T: list(rels.values())},
        domains={"Shelter": list(nodes)},
        nodes_by_table={nm.table_label: [k] for k, nm in nodes.items()},
    )


def _registered(sides: list[str]) -> set[tuple[int, int, str, str]]:
    dim = IDS[TABLES[DIM]]
    out: set[tuple[int, int, str, str]] = set()
    for s in sides:
        side = IDS[TABLES[s]]
        out |= {
            (side, dim, "neighborhood", "neighborhood"),
            (dim, side, "neighborhood", "neighborhood"),
        }
    return out


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """Budget lines in North and South; complaints in North (two) and East; a permit in North."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.public")
    for table, rows in {
        "budget": "(1, 'North'), (2, 'South')",
        "complaints": "(10, 'North'), (11, 'North'), (12, 'East')",
        "permits": "(20, 'North')",
    }.items():
        c.execute(f"CREATE TABLE pg.public.{table} (id INTEGER, neighborhood VARCHAR)")
        c.execute(f"INSERT INTO pg.public.{table} VALUES {rows}")
    return c


def _compiled(query: str, sides: list[str]) -> str:
    lm = _model(sides)
    sql, _, graph_vars = cypher_to_sql(parse_cypher(query), lm, {})
    return apply_graph_rewrites(sql, graph_vars, lm).sql(dialect="postgres")


def _refused(query: str, sides: list[str]) -> list[tuple[int, int, str]]:
    """What governance refuses in the statement, the dimension expanded as it is for the check."""
    sql = expand_view_refs(
        _compiled(query, sides), {f"public.{TABLES[DIM]}": _view_sql(sides)}, mark=True
    )
    found = join_guard.unrelated_tables(
        sqlglot.parse_one(sql, read="postgres"),
        resolve_table_id=lambda t: IDS.get(t.name),
        columns_of=lambda _table_id: {"id", "neighborhood"},
        registered=_registered(sides),
        exempt_table=lambda _table_id: False,
        same_remote_source=lambda a, b: False,
    )
    return [(f.left_table, f.right_table, f.reason) for f in found]


def _rows(con, query: str, sides: list[str]) -> list[tuple]:
    sql = expand_view_refs(_compiled(query, sides), {f"public.{TABLES[DIM]}": _view_sql(sides)})
    cur = con.execute(transpile_to_duckdb(sql))
    names = [d[0] for d in cur.description]
    rows = [dict(zip(names, r)) for r in cur.fetchall()]
    import json

    def prop(v, key):
        return None if v is None else json.loads(v)["properties"][key]

    return sorted(
        (prop(r["n"], "id"), prop(r["m"], "neighborhood"), prop(r["c"], "id") or 0) for r in rows
    )


def _path(first: str, last: str, directed: bool = False) -> str:
    """The drop's shape (provisa-ui graph-drop.ts): ``first`` matched, the path through the
    dimension to ``last`` OPTIONAL; ``directed``: each hop along its relationship, into the
    dimension and back out of it."""
    into, out = ("->", "<-") if directed else ("-", "-")
    return (
        f"MATCH (n:Shelter:{first}) "
        f"OPTIONAL MATCH (n)-[r1:{T}]{into}(m:Shelter:{DIM}){out}[r2:{T}]-(c:Shelter:{last}) "
        "RETURN n, r1, m, r2, c"
    )


#: North's budget line reaches both North complaints; South's reaches none (its row kept).
EXPECTED = [(1, "North", 10), (1, "North", 11), (2, None, 0)]


@pytest.mark.parametrize("sides", [["Budget", "Complaints"], ["Budget", "Complaints", "Permits"]])
def test_an_undirected_path_through_a_shared_dimension_is_governed(sides):
    assert _refused(_path("Budget", "Complaints"), sides) == []


@pytest.mark.parametrize("sides", [["Budget", "Complaints"], ["Budget", "Complaints", "Permits"]])
def test_an_undirected_path_through_a_shared_dimension_reads_its_rows(con, sides):
    assert _rows(con, _path("Budget", "Complaints"), sides) == EXPECTED


def test_a_directed_path_into_and_out_of_the_dimension(con):
    """Each side's relationship runs to the dimension: in along the first hop, back along the
    second."""
    sides = ["Budget", "Complaints", "Permits"]
    query = _path("Budget", "Complaints", directed=True)
    assert "->(m:" in query and "<-[r2" in query
    assert _refused(query, sides) == []
    assert _rows(con, query, sides) == EXPECTED


def test_a_path_reaches_only_the_side_it_names(con):
    """The permit side shares the type: a path to Permits reads permits, never complaints."""
    sides = ["Budget", "Complaints", "Permits"]
    assert _refused(_path("Budget", "Permits"), sides) == []
    assert _rows(con, _path("Budget", "Permits"), sides) == [(1, "North", 20), (2, None, 0)]


def test_a_side_with_no_relationship_to_the_dimension_is_still_refused():
    """The view's relationship relates the subquery to it only where one is registered:
    complaints' relationship to the dimension withdrawn, the path to complaints is refused."""
    sides = ["Budget", "Complaints", "Permits"]
    sql = expand_view_refs(
        _compiled(_path("Budget", "Complaints"), sides),
        {f"public.{TABLES[DIM]}": _view_sql(sides)},
        mark=True,
    )
    without = {r for r in _registered(sides) if IDS["complaints"] not in r[:2]}
    found = join_guard.unrelated_tables(
        sqlglot.parse_one(sql, read="postgres"),
        resolve_table_id=lambda t: IDS.get(t.name),
        columns_of=lambda _table_id: {"id", "neighborhood"},
        registered=without,
        exempt_table=lambda _table_id: False,
        same_remote_source=lambda a, b: False,
    )
    assert found
