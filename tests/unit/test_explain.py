# Copyright (c) 2026 Kenneth Stott
# Canary: 8e64c40f-127d-4641-8691-c952f32780f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""EXPLAIN of a governed plan and the Provisa annotations on it (REQ-1519).

The engine's plan describes the statement Provisa handed it, which is not the statement the user
wrote. These tests pin both halves: the dialect-specific EXPLAIN and its normalized tree, and the
annotations that account for the difference between what was written and what ran.
"""

import json
from pathlib import Path

import pytest

from provisa.executor.explain import (
    ExplainNode,
    ExplainUnsupported,
    build_explain_mermaid,
    parse_explain,
    syntax_for,
    wrap_explain,
)


class TestWrapExplain:
    def test_postgres_asks_for_json_so_the_tree_survives_transport(self):
        assert wrap_explain("SELECT 1", "postgres", analyze=False).startswith(
            "EXPLAIN (FORMAT JSON) "
        )

    def test_analyze_uses_the_form_that_actually_runs_the_statement(self):
        assert wrap_explain("SELECT 1", "postgres", analyze=True).startswith(
            "EXPLAIN (ANALYZE, FORMAT JSON) "
        )
        assert wrap_explain("SELECT 1", "duckdb", analyze=True).startswith(
            "EXPLAIN (ANALYZE, FORMAT json) "
        )

    def test_sqlite_cannot_time_a_run_and_says_so(self):
        assert wrap_explain("SELECT 1", "sqlite", analyze=False) == "EXPLAIN QUERY PLAN SELECT 1"
        with pytest.raises(ExplainUnsupported):
            wrap_explain("SELECT 1", "sqlite", analyze=True)

    def test_an_unreadable_dialect_is_refused_rather_than_guessed_at(self):
        with pytest.raises(ExplainUnsupported):
            wrap_explain("SELECT 1", "bigquery", analyze=False)

    def test_trino_answers_the_timed_form_as_text_not_json(self):
        syn = syntax_for("trino")
        assert syn.format_for(analyze=False) == "named_json"
        assert syn.format_for(analyze=True) == "text_indent"


class TestParseExplain:
    def test_postgres_json_nests_by_plans_and_carries_the_relation(self):
        payload = json.dumps(
            [
                {
                    "Plan": {
                        "Node Type": "Aggregate",
                        "Total Cost": 12.5,
                        "Plan Rows": 1,
                        "Plans": [
                            {
                                "Node Type": "Seq Scan",
                                "Relation Name": "orders",
                                "Total Cost": 10.0,
                                "Plan Rows": 100,
                                "Filter": "(status = 'open')",
                            }
                        ],
                    }
                }
            ]
        )
        (root,) = parse_explain([(payload,)], ["QUERY PLAN"], "postgres_json")
        assert root.op == "Aggregate"
        assert root.cost == 12.5
        (scan,) = root.children
        assert scan.op == "Seq Scan orders"
        assert scan.rows == 100
        assert scan.detail["Filter"] == "(status = 'open')"

    def test_duckdb_json_arrives_in_the_second_column(self):
        payload = json.dumps(
            [
                {
                    "name": "UNGROUPED_AGGREGATE",
                    "extra_info": {"Aggregates": "count_star()"},
                    "children": [
                        {"name": "SEQ_SCAN", "extra_info": {"Table": "t"}, "children": []}
                    ],
                }
            ]
        )
        (root,) = parse_explain(
            [("explain_value", payload)], ["explain_key", "explain_value"], "named_json"
        )
        assert root.op == "UNGROUPED_AGGREGATE"
        assert root.children[0].op == "SEQ_SCAN"
        assert root.children[0].detail["Table"] == "t"

    def test_a_profile_wrapper_contributes_no_operator_of_its_own(self):
        payload = json.dumps(
            {
                "latency": 0.01,
                "children": [
                    {"operator_name": "SEQ_SCAN", "operator_cardinality": 2, "children": []}
                ],
            }
        )
        (root,) = parse_explain([("k", payload)], ["explain_key", "explain_value"], "named_json")
        assert root.op == "SEQ_SCAN"
        assert root.rows == 2

    def test_duckdb_timings_are_reported_in_milliseconds(self):
        payload = json.dumps(
            {"operator_name": "SEQ_SCAN", "operator_timing": 0.0025, "children": []}
        )
        (root,) = parse_explain([("k", payload)], ["explain_key", "explain_value"], "named_json")
        assert root.actual_ms == pytest.approx(2.5)

    def test_sqlite_rebuilds_the_tree_from_parent_ids(self):
        rows = [(2, 0, 0, "SCAN orders"), (3, 2, 0, "USE TEMP B-TREE FOR ORDER BY")]
        (root,) = parse_explain(rows, ["id", "parent", "notused", "detail"], "sqlite_qp")
        assert root.op == "SCAN orders"
        assert root.children[0].op == "USE TEMP B-TREE FOR ORDER BY"

    def test_indented_text_nests_by_column(self):
        text = "Output[count]\n    Aggregate\n        TableScan[orders]\n"
        (root,) = parse_explain([(text,)], ["Query Plan"], "text_indent")
        assert root.op == "Output[count]"
        assert root.children[0].op == "Aggregate"
        assert root.children[0].children[0].op == "TableScan[orders]"

    def test_an_empty_result_yields_no_nodes(self):
        assert parse_explain([], ["QUERY PLAN"], "postgres_json") == []


class TestBuildExplainMermaid:
    def _tree(self):
        return [
            ExplainNode(
                op="Aggregate",
                cost=12.5,
                children=[ExplainNode(op="Seq Scan orders", rows=100)],
            )
        ]

    def test_the_engine_plan_is_the_spine_hanging_off_the_route(self):
        chart = build_explain_mermaid(self._tree(), route="DIRECT", route_reason="single source")
        assert chart.startswith("flowchart TD")
        assert 'route{{"direct route\\nsingle source"}}' in chart
        assert "route --> p1" in chart
        assert "p1 --> p2" in chart
        assert "Aggregate" in chart and "Seq Scan orders" in chart

    def test_a_leaf_carries_its_row_estimate_and_a_parent_its_cost(self):
        chart = build_explain_mermaid(self._tree(), route="ENGINE", route_reason=None)
        assert "cost 12.5" in chart
        assert "100 rows" in chart

    def test_measured_time_replaces_the_estimate_once_the_statement_ran(self):
        chart = build_explain_mermaid(
            [ExplainNode(op="Seq Scan", cost=10.0, actual_ms=3.25)],
            route="DIRECT",
            route_reason=None,
        )
        assert "3.2ms" in chart
        assert "cost 10" not in chart

    def test_the_optimizations_that_removed_a_scan_are_named_beside_the_plan(self):
        chart = build_explain_mermaid(
            self._tree(),
            route="ENGINE",
            route_reason="multiple sources",
            optimizations=("hot-table inline: currencies", "branch dropped: live_quotes"),
        )
        assert "hot-table inline\\ncurrencies" in chart
        assert "branch dropped\\nlive_quotes" in chart
        assert "o0 --> route" in chart
        assert "classDef provisaOpt" in chart

    def test_no_optimization_styling_when_nothing_was_rewritten(self):
        chart = build_explain_mermaid(self._tree(), route="DIRECT", route_reason=None)
        assert "classDef provisaOpt" not in chart

    def test_quotes_in_a_filter_cannot_break_the_node_label(self):
        chart = build_explain_mermaid(
            [ExplainNode(op='Seq Scan "orders"', detail={"Filter": "x = 'a'"})],
            route="DIRECT",
            route_reason=None,
        )
        assert '"' not in chart.split("flowchart TD")[1].replace('["', "").replace(
            '"]', ""
        ).replace('("', "").replace('")', "").replace('{{"', "").replace('"}}', "")


# REQ-1522: the estimates every engine already computes, read from the key each one uses.
# The fixtures are verbatim EXPLAIN output captured from Trino 481, MySQL 8 and DuckDB 1.5,
# so a key rename in an engine breaks these tests rather than silently emptying the columns.
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "explain"


def _fixture(name: str) -> str:
    return (FIXTURES / f"{name}.json").read_text()


def _walk(nodes: list[ExplainNode]) -> list[ExplainNode]:
    return [n for node in nodes for n in [node, *_walk(node.children)]]


class TestEngineEstimates:
    def test_trino_fragments_are_roots_and_carry_the_optimizer_estimates(self):
        nodes = parse_explain([(_fixture("trino_481_join_group"),)], ["Query Plan"], "named_json")
        # Trino returns one entry per fragment; each is a plan root of its own.
        assert [n.op for n in nodes] == ["Output", "TableScan"]
        by_op = {n.op: n for n in _walk(nodes)}
        assert by_op["InnerJoin"].rows == 15000.0
        assert by_op["InnerJoin"].cost == 418500.0
        assert by_op["TableScan"].rows == 1500.0
        assert by_op["TableScan"].detail["table"] == "tpch:tiny:customer"

    def test_a_trino_node_the_optimizer_could_not_estimate_reports_no_estimate(self):
        nodes = parse_explain([(_fixture("trino_481_join_group"),)], ["Query Plan"], "named_json")
        remote = next(n for n in _walk(nodes) if n.op == "RemoteSource")
        assert remote.rows is None
        assert remote.cost is None

    def test_a_nan_estimate_never_reaches_the_response(self):
        payload = json.dumps(
            {"0": {"name": "ScanProject", "estimates": [{"outputRowCount": float("nan")}]}}
        )
        (root,) = parse_explain([(payload,)], ["Query Plan"], "named_json")
        assert root.rows is None
        json.dumps(root.to_dict(), allow_nan=False)

    def test_duckdb_plan_reads_the_estimated_cardinality_out_of_extra_info(self):
        nodes = parse_explain(
            [("physical_plan", _fixture("duckdb_1_5_scan_agg"))],
            ["explain_key", "explain_value"],
            "named_json",
        )
        scan = next(n for n in _walk(nodes) if n.op == "SEQ_SCAN")
        assert scan.rows == 20.0
        assert scan.detail["Filters"] == "i>5"

    def test_duckdb_profile_still_prefers_the_rows_the_operator_actually_produced(self):
        payload = json.dumps(
            {
                "children": [
                    {
                        "operator_name": "SEQ_SCAN",
                        "operator_cardinality": 95,
                        "operator_timing": 0.002,
                        "extra_info": {"Estimated Cardinality": "20"},
                        "children": [],
                    }
                ]
            }
        )
        (scan,) = parse_explain([(payload,)], ["QUERY PLAN"], "named_json")
        assert scan.rows == 95.0
        assert scan.actual_ms == 2.0

    def test_mysql_json_reads_rows_off_the_table_and_cost_off_each_block(self):
        (block,) = parse_explain([(_fixture("mysql_8_join"),)], ["EXPLAIN"], "mysql_json")
        assert block.op == "Query block"
        assert block.cost == 0.9
        assert [(n.op, n.rows, n.cost) for n in block.children] == [
            ("ALL t", 3.0, 0.55),
            ("eq_ref c", 1.0, 0.9),
        ]

    def test_mysql_wrapping_operations_become_their_own_nodes(self):
        payload = json.dumps(
            {
                "query_block": {
                    "select_id": 1,
                    "cost_info": {"query_cost": "0.55"},
                    "ordering_operation": {
                        "using_filesort": True,
                        "grouping_operation": {
                            "using_temporary_table": True,
                            "table": {
                                "table_name": "t",
                                "access_type": "ALL",
                                "rows_examined_per_scan": 3,
                                "cost_info": {"prefix_cost": "0.55"},
                                "used_columns": ["id", "amt"],
                            },
                        },
                    },
                }
            }
        )
        (block,) = parse_explain([(payload,)], ["EXPLAIN"], "mysql_json")
        (ordering,) = block.children
        (grouping,) = ordering.children
        (table,) = grouping.children
        assert [ordering.op, grouping.op, table.op] == ["Ordering", "Grouping", "ALL t"]
        assert table.rows == 3.0
        assert ordering.detail["using_filesort"] == "True"

    def test_mysql_asks_for_the_json_form_it_can_read(self):
        assert syntax_for("mysql").fmt == "mysql_json"
        assert wrap_explain("SELECT 1", "mysql", analyze=False).startswith("EXPLAIN FORMAT=JSON ")
