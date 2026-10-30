# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7f9e04-b6d1-4a38-8e52-d1a07b3c6f95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Mermaid label escaping for the server-built query charts.

Each chart label is a quoted Mermaid string whose text cannot end the string, open a shape, start
an edge label, end a statement, start an entity code, break a line or carry HTML.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

from provisa.api.data.endpoint_helpers import _build_mermaid
from provisa.executor.explain import MAX_DRAWN_OPERATORS, ExplainNode, build_explain_mermaid
from provisa.executor.mermaid_label import MAX_LABEL, label, node_id
from provisa.executor.plan_stats import build_plan_mermaid

_REPO = Path(__file__).resolve().parents[2]

# The characters with grammar or entity meaning inside a quoted Mermaid label.
_UNSAFE = set('"#<>[]{}|;`\\&\n')


def _quoted_bodies(chart: str) -> list[str]:
    return re.findall(r'"([^"]*)"', chart)


class TestLabel:
    @pytest.mark.parametrize(
        ("raw", "escaped"),
        [
            ('"t12"."from_term_id"', "#34;t12#34;.#34;from_term_id#34;"),
            ("a|b", "a#124;b"),
            ("a;b", "a#59;b"),
            ("[x]{y}", "#91;x#93;#123;y#125;"),
            ("<b>", "#60;b#62;"),
            ("#1;", "#35;1#59;"),
            ("a\\nb", "a#92;nb"),
            ("`md`", "#96;md#96;"),
            ("a & b", "a #38; b"),
            ("(postgresql)", "(postgresql)"),
        ],
    )
    def test_each_unsafe_character_becomes_its_entity_code(self, raw, escaped):
        assert label(raw) == escaped

    def test_newlines_and_runs_of_space_become_one_space(self):
        assert label("Seq Scan\n   on\torders") == "Seq Scan on orders"

    def test_long_text_is_cut_with_an_ellipsis_before_escaping(self):
        raw = "SELECT " + ", ".join(f'"t12"."c{i}"' for i in range(40))
        out = label(raw)
        assert out.endswith("…")
        # Cut on the raw text, so no entity code is split: every '#' opens a whole code.
        assert re.fullmatch(r"([^#]|#\d+;)*", out)
        assert len(re.sub(r"#\d+;", "x", out)) <= MAX_LABEL

    @pytest.mark.parametrize("raw", ["", "   ", "\n"])
    def test_empty_text_is_a_non_breaking_space(self, raw):
        assert label(raw) == "#160;"

    def test_short_text_is_kept_whole(self):
        assert label("PROJECTION") == "PROJECTION"


class TestNodeId:
    @pytest.mark.parametrize("raw", ["end", "graph", "pg main", "a.b-c", 'x"y'])
    def test_ids_are_prefixed_word_characters(self, raw):
        nid = node_id(raw)
        assert re.fullmatch(r"n_\w+", nid)


class TestChartsEscapeEveryLabel:
    def test_analyze_plan_for_the_term_edges_projection(self):
        cols = ", ".join(f'"t12"."c{i}"' for i in range(30))
        sql = f'SELECT {cols} FROM "ontology"."term_edges" AS "t12" WHERE "t12"."r" = \'a;b\''
        chart = build_explain_mermaid(
            [ExplainNode(op="PROJECTION", children=[ExplainNode(op=sql)])],
            route="ENGINE",
            route_reason=f"a function its dialect cannot write ({sql})",
            optimizations=('hot-table inline: "ontology"."terms"',),
        )
        for body in _quoted_bodies(chart):
            # Entity codes and the builder's own `\n` line breaks are the only escapes left.
            plain = re.sub(r"#\d+;", "", body).replace("\\n", "")
            assert not set(plain) & _UNSAFE, body
        assert "#34;ontology#34;.#34;terms#34;" in chart

    def test_sql_stats_dag(self):
        chart = build_plan_mermaid(
            sources=frozenset({"end", "pg main"}),
            source_types={"end": "postgresql"},
            route="ENGINE",
            route_reason='operator floor: "eventual" | x',
            optimizations=("api cache: [petstore] {pets}",),
            direct_source_id=None,
            elapsed_ms=1.0,
            rows=1,
        )
        assert "    n_end[" in chart and "    n_pg_main[" in chart
        assert 'route -->|"operator floor: #34;eventual#34; #124; x"| result' in chart
        assert "#91;petstore#93; #123;pets#125;" in chart

    def test_graphql_stats_dag(self):
        chart = _build_mermaid(
            {"pg-main", "petstore"},
            {"pg-main": "postgresql", "petstore": "openapi"},
            {},
            5.0,
            3,
            "orders|x",
            join_fields=[("end", "petstore", False)],
            root_source_id="pg-main",
        )
        assert 'n_pg_main["orders#124;x\\n(pg-main)"]' in chart
        assert 'n_join_end["end\\n(petstore)"]' in chart


class TestLargePlans:
    """Mermaid refuses a flowchart past 500 edges; a large statement's plan is past that."""

    @staticmethod
    def _chain(n: int) -> ExplainNode:
        root = node = ExplainNode(op="PROJECTION")
        for _ in range(n - 1):
            child = ExplainNode(op="LEFT_DELIM_JOIN")
            node.children.append(child)
            node = child
        return root

    def test_a_plan_past_the_cap_draws_the_cap_and_counts_the_rest(self):
        chart = build_explain_mermaid([self._chain(1500)], route="ENGINE", route_reason=None)
        assert chart.count(" --> ") + chart.count(" -.-> ") < 500
        assert f"p{MAX_DRAWN_OPERATORS}[" in chart or f"p{MAX_DRAWN_OPERATORS}(" in chart
        assert f"p{MAX_DRAWN_OPERATORS + 1}" not in chart
        assert (
            f'more("+{1500 - MAX_DRAWN_OPERATORS} more operators, in the operator list")' in chart
        )
        assert "route -.-> more" in chart

    def test_a_plan_within_the_cap_is_drawn_whole(self):
        chart = build_explain_mermaid(
            [self._chain(MAX_DRAWN_OPERATORS)], route="ENGINE", route_reason=None
        )
        assert "more(" not in chart

    def test_the_maintainers_term_search_plan_is_past_mermaids_edge_limit(self):
        """The regression fixture exercises the cap: its whole plan has over 500 edges."""
        spec = importlib.util.spec_from_file_location(
            "gen_mermaid_chart_fixtures", _REPO / "scripts" / "gen_mermaid_chart_fixtures.py"
        )
        assert spec is not None and spec.loader is not None
        gen = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gen)
        chart = gen.cases()["analyze: ontology term search (maintainer)"]
        drawn = chart.count(" --> ") + chart.count(" -.-> ")
        more = re.search(r"\+(\d+) more operators", chart)
        assert more is not None
        total = int(more.group(1)) + MAX_DRAWN_OPERATORS
        assert total > 500
        assert drawn < 500


def test_the_ui_fixture_matches_the_builders():
    """provisa-ui runs these charts through Mermaid's parser; they must be the builders' output."""
    spec = importlib.util.spec_from_file_location(
        "gen_mermaid_chart_fixtures", _REPO / "scripts" / "gen_mermaid_chart_fixtures.py"
    )
    assert spec is not None and spec.loader is not None
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    assert gen.FIXTURE.read_text(encoding="utf-8") == gen.render(), (
        "regenerate with: .venv/bin/python3 scripts/gen_mermaid_chart_fixtures.py"
    )
