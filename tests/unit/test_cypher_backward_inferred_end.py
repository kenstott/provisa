# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An unlabelled node's type is the end of the relationship it stands at (#291, REQ-345): read
backward, (m)<-[:SOLD_AT]-(a:Pet), m is SOLD_AT's target -- the shop -- not its source. It had
been given the source's type, and its hop joined on FALSE: no rows. Each case runs on DuckDB."""

# Requirements: REQ-345

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb


def _node(label: str, table: str, table_id: int, props: list[str]) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=table_id,
        source_id="pg",
        id_column="id",
        pk_columns=["id"],
        catalog_name="db",
        schema_name="s",
        table_name=table,
        properties={p: p for p in props},
        physical_properties={p: p for p in props},
    )


_LM = CypherLabelMap(
    nodes={
        "Pet": _node("Pet", "pets", 1, ["id", "name", "shop_id"]),
        "Shop": _node("Shop", "shops", 2, ["id", "city"]),
    },
    relationships={
        "SOLD_AT": RelationshipMapping(
            rel_type="SOLD_AT",
            source_label="Pet",
            target_label="Shop",
            join_source_column="shop_id",
            join_target_column="id",
            field_name="shop",
        )
    },
    nodes_by_table={"Pet": ["Pet"], "Shop": ["Shop"]},
)


@pytest.fixture
def rows():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS db")
    con.execute("CREATE SCHEMA db.s")
    con.execute("CREATE TABLE db.s.pets (id INTEGER, name VARCHAR, shop_id INTEGER)")
    con.execute("INSERT INTO db.s.pets VALUES (1, 'rex', 7), (2, 'tom', NULL)")
    con.execute("CREATE TABLE db.s.shops (id INTEGER, city VARCHAR)")
    con.execute("INSERT INTO db.s.shops VALUES (7, 'oslo'), (8, 'rome')")

    def _rows(cypher: str) -> list[tuple]:
        sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), _LM, {})
        sql = apply_graph_rewrites(sql_ast, graph_vars, _LM).sql(dialect="postgres")
        return sorted(con.execute(transpile_to_duckdb(sql)).fetchall(), key=str)

    return _rows


@pytest.mark.parametrize(
    "cypher, expected",
    [
        ("MATCH (m)<-[:SOLD_AT]-(a:Pet) RETURN m.city AS c", [("oslo",)]),
        ("MATCH (m)<-[:SOLD_AT]-(a) RETURN m.city AS c, a.name AS n", [("oslo", "rex")]),
        # Read forward, as before.
        ("MATCH (a)-[:SOLD_AT]->(m) RETURN m.city AS c, a.name AS n", [("oslo", "rex")]),
        # The unlabelled end of an OPTIONAL hop read backward.
        (
            "MATCH (s:Shop) OPTIONAL MATCH (s)<-[:SOLD_AT]-(p) RETURN s.city AS c, p.name AS n",
            [("oslo", "rex"), ("rome", None)],
        ),
    ],
)
def test_an_unlabelled_node_is_the_end_it_stands_at(rows, cypher, expected):
    assert rows(cypher) == expected


def test_the_node_returned_is_of_that_end_s_type(rows):
    ((node,),) = rows("MATCH (m)<-[:SOLD_AT]-(a:Pet) RETURN m")
    assert '"label":"Shop"' in node and '"city":"oslo"' in node
