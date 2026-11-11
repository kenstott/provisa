# Copyright (c) 2026 Kenneth Stott
# Canary: 535cab70-7e07-49d3-aabc-80656f90b113
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The polyfill grid (REQ-2236): one table keyed by feature and engine, validated when it is built,
read by the translator, and the documentation generated from it (A PAGE PER ENGINE, EVERY POLYFILL
LISTED; THE ESCAPE HATCH IS A FUNCTION NAME)."""

# Requirements: REQ-2236

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.polyfill import GRID, docs
from provisa.transpiler.polyfill.grid import (
    ENGINES,
    Feature,
    Grid,
    Limitation,
    Rule,
    Side,
    Status,
    engine_of,
)
from provisa.transpiler.polyfill.notation import Refusal, Rewrite, RuleError

_ROOT = Path(__file__).resolve().parents[3]


def lowered_to_one(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """Writes a call of one_of_a_kind() as the number 1."""
    if isinstance(node, exp.Anonymous) and node.name.lower() == "one_of_a_kind":
        return exp.Literal.number(1)
    return None


_REFUSED = Refusal("0A000", "shout has no form on clickhouse")
_SHOUT = Feature(
    "shout",
    "toy",
    "A text in capitals.",
    read=Side(
        rules=(
            Rule("shout($a::text)", {"duckdb": "upper($a)", "clickhouse": _REFUSED}),
            Rule("shout($a::text, $n::integer)", {"duckdb": "repeat(upper($a), $n)"}),
        ),
        limitations={"clickhouse": (Limitation("Refused.", _REFUSED),)},
    ),
)
_ONE = Feature(
    "one_of_a_kind",
    "toy",
    "The number one.",
    read=Side(rules=(Rule(None, {"trino": Rewrite(lowered_to_one)}),)),
)
_NATIVE = Feature(
    "abs",
    "toy",
    "An absolute value.",
    read=Side(rules=(Rule("abs($a::integer)", {}),)),
)
_WRITTEN = Feature(
    "a_money_value",
    "toy",
    "A value going in.",
    write=Side(
        rules=(Rule("CAST($v AS money)", {"trino": "CAST($v AS DECIMAL(20, 2))"}),),
        limitations={"trino": (Limitation("Two decimal places."),)},
    ),
)
_TOY = Grid((_SHOUT, _ONE, _NATIVE, _WRITTEN))


def _node(sql: str) -> exp.Expr:
    return sqlglot.parse_one(sql, read="postgres")


@pytest.mark.parametrize(
    ("dialect", "engine"),
    [
        ("duckdb", "duckdb"),
        ("trino", "trino"),
        ("presto", "trino"),
        ("athena", "trino"),
        ("clickhouse", "clickhouse"),
        ("snowflake", "sources"),
        ("tsql", "sources"),
        ("postgres", None),
    ],
)
def test_each_dialect_is_one_engine_of_the_grid_or_none(dialect: str, engine: str | None):
    assert engine_of(dialect) == engine


def test_a_rule_matched_gives_the_engines_form_and_bindings():
    applied = _TOY.read(_node("shout('x'::text)"), "duckdb")
    assert applied is not None
    assert applied.feature is _SHOUT and applied.form == "upper($a)"
    assert applied.bindings["a"].sql("postgres") == "CAST('x' AS TEXT)"


def test_the_rules_of_a_function_are_tried_in_order_by_their_patterns():
    applied = _TOY.read(_node("shout(x, 3)"), "duckdb")
    assert applied is not None and applied.form == "repeat(upper($a), $n)"


def test_an_engine_a_rule_gives_no_form_does_the_feature_natively():
    applied = _TOY.read(_node("shout(x, 3)"), "clickhouse")
    assert applied is not None and applied.form is None


def test_a_refusal_is_the_rows_form():
    applied = _TOY.read(_node("shout(x)"), "clickhouse")
    assert applied is not None and applied.form == _REFUSED


def test_a_dialect_that_is_no_engine_matches_with_no_form():
    applied = _TOY.read(_node("shout(x)"), "postgres")
    assert applied is not None and applied.form is None


@pytest.mark.parametrize("sql", ["shout(1)", "shout(x, 'a'::text)", "whisper(x)", "shout()"])
def test_a_node_no_rule_matches_is_none(sql: str):
    assert _TOY.read(_node(sql), "duckdb") is None


def test_a_function_name_rule_rewrites_on_the_tree():
    applied = _TOY.read(_node("one_of_a_kind()"), "trino")
    assert applied is not None
    assert applied.feature is _ONE and isinstance(applied.form, Rewrite)
    assert applied.rewritten == exp.Literal.number(1)
    assert _TOY.read(_node("one_of_a_kind()"), "duckdb") is None  # no rule of its on DuckDB
    assert _TOY.read(_node("other()"), "trino") is None


def test_the_grid_names_the_functions_its_patterns_call():
    assert _TOY.names == {"shout", "abs"}
    assert "array_ndims" in GRID.names and "trim_array" in GRID.names


@pytest.mark.parametrize(
    ("feature", "engine", "status"),
    [
        (_SHOUT, "duckdb", Status.POLYFILLED),
        (_SHOUT, "clickhouse", Status.REFUSED),
        (_SHOUT, "trino", Status.NATIVE),
        (_ONE, "trino", Status.POLYFILLED),
        (_NATIVE, "duckdb", Status.NATIVE),
        (_WRITTEN, "trino", Status.DIFFERS),
    ],
)
def test_each_feature_has_a_status_on_each_engine(feature, engine, status):
    assert _TOY.status(feature, engine) is status


@pytest.mark.parametrize(
    ("features", "message"),
    [
        ((_SHOUT, _SHOUT), "named twice"),
        ((Feature("f", "t", "", Side((Rule("f($a)", {"oracle": "g($a)"}),))),), "no engine"),
        ((Feature("f", "t", "", Side((Rule(None, {"duckdb": "g(1)"}),))),), "function name"),
        ((Feature("f", "t", "", Side((Rule("f($a)", {"duckdb": "g($b)"}),))),), "does not have"),
        ((Feature("f", "t", "", Side((Rule("f($a)", {"trino": "g(($a"}),))),), "does not parse"),
        ((Feature("f", "t", "", Side(limitations={"x": ()})),), "no engine"),
        ((Feature("f", "t", "", Side((Rule("f($a)", {"sources": "g($a)"}),))),), "no one dialect"),
    ],
)
def test_a_grid_that_does_not_hold_is_refused_when_built(features, message: str):
    with pytest.raises(RuleError, match=message):
        Grid(features)


def test_an_engines_page_lists_what_is_not_native_and_counts_the_rest():
    page = docs.engine_page(_TOY, "trino")
    assert page.startswith("# Polyfills on Trino")
    assert "1 of the grid&#x27;s" not in page  # nothing escaped outside a cell
    assert "2 of the grid's 4 features are left out as native" in page
    assert "Rules on this page: 2, of which 1 are only the name of a rewrite function" in page
    assert "<strong>one_of_a_kind</strong>" in page and "<strong>a_money_value</strong>" in page
    assert "<strong>shout</strong>" not in page and "<strong>abs</strong>" not in page
    assert f"function {Rewrite(lowered_to_one).name}: Writes a call of one_of_a_kind()" in page
    assert "CAST($v AS money)\n=&gt;\nCAST($v AS DECIMAL(20, 2))" in page
    assert "<li>Two decimal places.</li>" in page


def test_a_refusal_is_shown_with_its_sqlstate_and_message():
    page = docs.engine_page(_TOY, "clickhouse")
    assert "refused (its error is under the limitations)" in page
    assert (
        "<li>Refused: shout($a::text). <code>0A000</code> shout has no form on clickhouse</li>"
    ) in page
    assert "<li>Refused. <code>0A000</code> shout has no form on clickhouse</li>" in page


def test_the_summary_shows_each_feature_on_each_engine():
    page = docs.summary_page(_TOY)
    assert "| `shout` | toy | polyfilled | native | refused | native |" in page
    assert "| `a_money_value` | toy | native | differs | native | native |" in page


def test_every_page_states_its_rule_and_the_pages_are_one_per_engine():
    written = docs.pages(GRID)
    assert set(written) == {
        f"{docs.DIRECTORY}/index.md",
        *(f"{docs.DIRECTORY}/{engine}.md" for engine in ENGINES),
    }
    for path, text in written.items():
        assert "Generated by scripts/gen_polyfill_docs.py" in text, path


@pytest.mark.parametrize("path", sorted(docs.pages(GRID)))
def test_the_committed_pages_are_the_grids(path: str):
    """The pages cannot drift from the grid: scripts/gen_polyfill_docs.py writes them."""
    assert (_ROOT / path).read_text() == docs.pages(GRID)[path]
