# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""EXPLAIN of a governed plan and the Provisa annotations on it (REQ-1519).

The engine's plan describes the statement Provisa handed it, which is not the statement the user
wrote. These tests pin both halves: the dialect-specific EXPLAIN and its normalized tree, and the
annotations that account for the difference between what was written and what ran.
"""

import json

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
