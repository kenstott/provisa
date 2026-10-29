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
)
from provisa.executor.plan_stats import build_plan_mermaid  # noqa: E402

FIXTURE = _REPO / "provisa-ui" / "src" / "__tests__" / "fixtures" / "mermaidCharts.json"

_EDGE_COLS = ("from_term_id", "to_term_id", "relation", "weight", "source_id", "created_at")
# The maintainer's statement: a long projection of quoted columns over "ontology"."term_edges".
TERM_EDGES_SQL = (
    "SELECT "
    + ", ".join(f'"t12"."{c}" AS "{c}_{i}"' for i in range(3) for c in _EDGE_COLS)
    + ' FROM "ontology"."term_edges" AS "t12"'
    + ' WHERE "t12"."relation" = \'is-a\' AND "t12"."weight" > 0.5'
)


def _maintainer_plan() -> str:
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
        "analyze: term_edges projection (maintainer)": _maintainer_plan(),
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
