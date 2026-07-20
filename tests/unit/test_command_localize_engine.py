# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7f0a92-6b18-4d54-9e02-1a5d8f2c4b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An inline-composed command executes through a REAL engine (REQ-1159).

Drives the actual localization pass (provisa.executor.command_localize), then executes the rewritten
statement on a real in-process DuckDB connection holding a real base table — proving a command joined
inline with a source table produces the correct joined+enriched result set, that the inline relation's
declared IR types survive to the engine, and that the lineage graph over the same statement is
continuous from source columns through the command boundary. In-process DuckDB is the real engine, so
this needs no external service (unit tier, like test_command_localize.py's DuckDB assertions)."""

from __future__ import annotations

import asyncio

import duckdb
import sqlglot

from provisa.executor.command_localize import localize_commands
from provisa.lineage.graph import build_column_graph

_ENRICH = {
    "enrich": {
        "name": "enrich",
        "arguments": [
            {
                "name": "input",
                "arg_kind": "result_set",
                "columns": [{"name": "id", "type": "integer"}, {"name": "region", "type": "text"}],
            }
        ],
        "output_columns": [
            {"name": "id", "type": "integer"},
            {"name": "score", "type": "double"},
            {"name": "geo", "type": "text"},
        ],
    }
}


def _localize(sql: str, runner) -> str:
    tree = sqlglot.parse_one(sql, read="postgres")
    asyncio.run(localize_commands(tree, _ENRICH, runner, dialect="duckdb"))
    return tree.sql(dialect="duckdb")


def test_composed_command_joins_a_real_table_in_duckdb():
    con = duckdb.connect()
    con.execute("CREATE TABLE orders (id INTEGER, region TEXT, note TEXT)")
    con.execute("INSERT INTO orders VALUES (1,'east','a'),(2,'west','b'),(3,'east','c')")

    async def run(name, args):
        assert name == "enrich"
        # values exact in binary floating point, so the FLOAT-family round-trip compares exactly
        return [
            {"id": 1, "score": 0.5, "geo": "40.7,-74.0"},
            {"id": 2, "score": 0.25, "geo": "37.7,-122.4"},
            {"id": 3, "score": 0.75, "geo": "40.7,-74.0"},
        ]

    sql = (
        "SELECT o.id, o.note, e.score, e.geo "
        "FROM orders o JOIN enrich('main.public.orders') e ON o.id = e.id "
        "ORDER BY o.id"
    )
    rows = con.execute(_localize(sql, run)).fetchall()
    assert rows == [
        (1, "a", 0.5, "40.7,-74.0"),
        (2, "b", 0.25, "37.7,-122.4"),
        (3, "c", 0.75, "40.7,-74.0"),
    ]


def test_declared_ir_types_survive_to_the_engine():
    con = duckdb.connect()
    con.execute("CREATE TABLE orders (id INTEGER)")
    con.execute("INSERT INTO orders VALUES (1)")

    async def run(name, args):
        return [{"id": 1, "score": 3.5, "geo": "x"}]

    rewritten = _localize("SELECT e.score FROM orders o JOIN enrich('r') e ON o.id = e.id", run)
    con.execute(f"CREATE TABLE _t AS {rewritten}")
    col_type = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name='_t' AND column_name='score'"
    ).fetchone()
    # 'score' is declared IR floating-point → a FLOAT-family engine type (pinned by the CAST),
    # never text/int inferred from the literal
    assert col_type is not None
    assert any(t in col_type[0].upper() for t in ("FLOAT", "DOUBLE", "REAL"))


def test_lineage_is_continuous_through_the_command():
    sql = (
        "SELECT o.note, e.score, upper(e.geo) AS geo_u "
        "FROM orders o JOIN enrich('main.public.orders') e ON o.id = e.id"
    )
    g = build_column_graph(sql, commands=_ENRICH)
    edges = {(e.source, e.target) for e in g.edges}
    assert ("main.public.orders.region", "e.score") in edges
    assert ("main.public.orders.id", "e.geo") in edges
    assert ("e.geo", "geo_u") in edges
    assert g.nodes["main.public.orders.region"].kind == "source"
