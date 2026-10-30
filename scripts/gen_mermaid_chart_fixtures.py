# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e5b1c-4a96-4f0e-8b3d-91c6e2a5f704
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Write the server-built Mermaid charts the UI checks with Mermaid's own parser.

The charts come from the real builders (Analyze plan tree, SQL stats DAG, GraphQL stats DAG)
fed with plan text shaped like what engines return: quoted identifiers, string literals,
brackets, braces, pipes, semicolons, ``#``, ``<>`` and long SQL. The vitest
``mermaidChartsParse.test.ts`` runs each one through ``mermaid.parse``; the unit test
``tests/unit/test_mermaid_label.py`` fails when this file is out of date with the builders.

Usage: .venv/bin/python3 scripts/gen_mermaid_chart_fixtures.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from provisa.api.data.endpoint_helpers import _build_mermaid  # noqa: E402
from provisa.executor.explain import (  # noqa: E402
    ExplainNode,
    _from_text_indent,
    build_explain_mermaid,
    parse_explain,
)
from provisa.executor.plan_stats import build_plan_mermaid  # noqa: E402

FIXTURE = _REPO / "provisa-ui" / "src" / "__tests__" / "fixtures" / "mermaidCharts.json"

_EDGE_COLS = ("from_term_id", "to_term_id", "relation", "weight", "source_id", "created_at")
# A long projection of quoted columns over "ontology"."term_edges".
TERM_EDGES_SQL = (
    "SELECT "
    + ", ".join(f'"t12"."{c}" AS "{c}_{i}"' for i in range(3) for c in _EDGE_COLS)
    + ' FROM "ontology"."term_edges" AS "t12"'
    + ' WHERE "t12"."relation" = \'is-a\' AND "t12"."weight" > 0.5'
)


def _term_edges_plan() -> str:
    projections = ", ".join(f'"t12"."{c}"' for c in _EDGE_COLS)
    return build_explain_mermaid(
        [
            ExplainNode(
                op="PROJECTION",
                detail={"Projections": projections},
                rows=200,
                children=[
                    ExplainNode(
                        op="FILTER",
                        detail={"Expressions": "(relation = 'is-a') AND (weight > 0.5)"},
                        rows=200,
                        children=[ExplainNode(op="SEQ_SCAN term_edges", rows=1000)],
                    )
                ],
            )
        ],
        route="ENGINE",
        route_reason=f"a function its dialect cannot write (Unsupported: {TERM_EDGES_SQL})",
        optimizations=('hot-table inline: "ontology"."terms"',),
    )


# The maintainer's failing statement (2026-10-29), the Ontology page's term search for "pet" as
# compiled SQL: a provisa-params comment with '%pet%' values, 60 aliases, very long column aliases,
# LEFT JOIN LATERAL subqueries each with an EXISTS over "ontology"."terms", nested
# COALESCE(JSON_GROUP_ARRAY(JSON_OBJECT(...)), CAST('[]' AS JSON)), IN lists, LIKE LOWER(...) and a
# LIMIT 100 FROM subquery. Its DuckDB plan has over 500 edges, past Mermaid's limit.
_LATERALS = 30


def _lateral(n: int) -> str:
    e, w, inner = f"t{2 * n}", f"w{n}", f"t{2 * n + 1}"
    return (
        f'LEFT JOIN LATERAL (SELECT COALESCE(JSON_GROUP_ARRAY(JSON_OBJECT(\'id\', "{e}"."id", '
        f"'rel_type', \"{e}\".\"rel_type\", 'to', (SELECT COALESCE(JSON_GROUP_ARRAY("
        f'JSON_OBJECT(\'id\', "{inner}"."id")), CAST(\'[]\' AS JSON)) FROM "ontology"."terms" AS '
        f'"{inner}" WHERE "{inner}"."id" = "{e}"."to_term_id"))), CAST(\'[]\' AS JSON)) AS "v" '
        f'FROM "ontology"."term_edges" AS "{e}" WHERE "{e}"."from_term_id" = "t0"."id" AND '
        f"\"{e}\".\"rel_type\" IN ('is_a', 'part_of', 'related') AND EXISTS (SELECT 1 FROM "
        f'"ontology"."terms" AS "{w}" WHERE "{w}"."id" = "{e}"."to_term_id" AND "{w}"."status" IN '
        f"('approved', 'draft'))) AS \"l{n}\" ON TRUE"
    )


ONTOLOGY_SEARCH_SQL = (
    "-- provisa-params: $1='approved', $2='draft', $43='is_a', $44='part_of', $45='related', "
    "$46='%pet%', $47='%pet%'\n"
    'SELECT JSON_OBJECT(\'id\', "t0"."id") AS "term", "t0"."name", '
    + ", ".join(
        f'"l{n}"."v" AS "edges_in__source_term__edges_in__source_term__edges_out__rel_type_{n}"'
        for n in range(1, _LATERALS)
    )
    + ' FROM (SELECT * FROM "ontology"."terms" AS "t0x" WHERE LOWER("t0x"."name") LIKE '
    'LOWER(\'%pet%\') OR LOWER("t0x"."definition") LIKE LOWER(\'%pet%\') LIMIT 100) AS "t0" '
    + " ".join(_lateral(n) for n in range(1, _LATERALS))
)


def _ontology_search_plan() -> str:
    import duckdb

    con = duckdb.connect()
    con.execute("CREATE SCHEMA ontology")
    con.execute(
        "CREATE TABLE ontology.terms (id INTEGER, name VARCHAR, definition VARCHAR, status VARCHAR)"
    )
    con.execute(
        "CREATE TABLE ontology.term_edges "
        "(id INTEGER, from_term_id INTEGER, to_term_id INTEGER, rel_type VARCHAR)"
    )
    cur = con.execute("EXPLAIN (FORMAT json) " + ONTOLOGY_SEARCH_SQL)
    rows = cur.fetchall()
    names = [d[0] for d in cur.description]
    return build_explain_mermaid(
        parse_explain(rows, names, "named_json"),
        route="ENGINE",
        route_reason="landed: dialect_supports: source 'provisa-admin' cannot run a join",
    )


_TRINO_ANALYZE = """Trino version: 470
Queued: 1ms, Analysis: 20ms, Planning: 30ms, Execution: 50ms
Fragment 1 [SOURCE]
    CPU: 2.35ms, Scheduled: 3ms, Input: 3 rows (163B); per task: avg.: 3.00 std.dev.: 0.00
    Output layout: [from_term_id, to_term_id, relation]
    Output[columnNames = [from_term_id, to_term_id, relation]]
    │   Layout: [from_term_id:varchar, to_term_id:varchar]
    │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
    └─ ScanFilterProject[table = ontology:term_edges, filterPredicate = ("relation" = 'a|b')]
           Layout: [from_term_id:varchar]
           Estimates: {rows: ? (?), cpu: ?}/{rows: ? (?), cpu: ?}
           note := 'x #1; <b>y</b> `z` \\n end'
"""


def _join_plan() -> str:
    return build_explain_mermaid(
        [
            ExplainNode(
                op="Hash Join",
                detail={"Hash Cond": '("o"."cur" = "c"."code")'},
                cost=44.5,
                children=[
                    ExplainNode(op='Seq Scan "orders" o', rows=120),
                    ExplainNode(op="Hash", children=[ExplainNode(op="Seq Scan currencies c")]),
                ],
            )
        ],
        route="DIRECT",
        route_reason="single source with a direct access path (postgresql)",
    )


def _cte_plan() -> str:
    return build_explain_mermaid(
        [
            ExplainNode(
                op="CTE Scan on \"recent\" [WITH recent AS (SELECT * FROM orders WHERE note = 'a;b')]",
                children=[ExplainNode(op="CTE recent {materialized}")],
            )
        ],
        route="ENGINE",
        route_reason="reads 2 sources",
        optimizations=("materialized view: mv_recent (exact)", "branch dropped: live|quotes"),
    )


def _literal_plan() -> str:
    return build_explain_mermaid(
        [ExplainNode(op="Filter: (name = 'O\"Brien''s \"pub\"')", rows=1)],
        route="DIRECT",
        route_reason='steward override: "direct"',
    )


def _stats_dag() -> str:
    return build_plan_mermaid(
        sources=frozenset({"end", "pg main", "a.b-c"}),
        source_types={"end": "postgresql", "pg main": "sqlite", "a.b-c": "duckdb"},
        route="ENGINE",
        route_reason='operator floor: "eventual" on pg main (read from the replica)',
        optimizations=("api cache: [petstore] {pets}",),
        direct_source_id=None,
        elapsed_ms=12.0,
        rows=7,
    )


def _graphql_dag() -> str:
    return _build_mermaid(
        {"pg-main", "petstore"},
        {"pg-main": "postgresql", "petstore": "openapi"},
        {"petstore": 12.0},
        20.0,
        5,
        "orders",
        join_fields=[("end", "petstore", False), ("pg-main", "petstore", True)],
        root_source_id="pg-main",
    )


def cases() -> dict[str, str]:
    """Every chart the UI fixture holds, by name."""
    return {
        "analyze: ontology term search (maintainer)": _ontology_search_plan(),
        "analyze: term_edges projection": _term_edges_plan(),
        "analyze: trino EXPLAIN ANALYZE text": build_explain_mermaid(
            _from_text_indent(_TRINO_ANALYZE), route="ENGINE", route_reason="reads 2 sources"
        ),
        "analyze: join": _join_plan(),
        "analyze: CTE": _cte_plan(),
        "analyze: string literal with quotes": _literal_plan(),
        "analyze: empty and backtick-led operators": build_explain_mermaid(
            [ExplainNode(op="", children=[ExplainNode(op="`term_edges` scan")])],
            route="ENGINE",
            route_reason="",
            optimizations=(": ", "`md`: x"),
        ),
        "stats: SQL plan DAG": _stats_dag(),
        "stats: GraphQL DAG": _graphql_dag(),
    }


def render() -> str:
    return json.dumps(cases(), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(render(), encoding="utf-8")
    print(FIXTURE)
