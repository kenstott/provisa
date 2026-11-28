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

import html
import sys
from pathlib import Path

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.polyfill import GRID, docs
from provisa.transpiler.dialect_capabilities import CAPABILITIES
from provisa.transpiler.polyfill.grid import (
    DIALECTS,
    Feature,
    Grid,
    Limitation,
    Rule,
    Side,
    Status,
    column_of,
    written_in,
)
from provisa.transpiler.polyfill.notation import Refusal, Rewrite, RuleError
from provisa.transpiler.polyfill import provenance
from provisa.transpiler.polyfill.provenance import Leg, Provenance, Run

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "scripts" / "conformance"))

import polyfill_provenance  # noqa: E402

#: The committed configuration's provenance, as scripts/gen_polyfill_docs.py reads it.
_PROVENANCE = polyfill_provenance.read(GRID)


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
_DUCK = Leg("duckdb", "in-process (duckdb)")
_CHDB = Leg("clickhouse", "in-process (chdb)")
_CH_SERVER = Leg("clickhouse-server", "clickhouse/clickhouse-server:24.3")
#: The toy grid's rows are on DuckDB (verified), ClickHouse (one leg of two judged) and Trino (no
#: leg).
_TOY_PROVENANCE = {
    "duckdb": Provenance((_DUCK,), (_DUCK,), Run("101", "abc1234")),
    "clickhouse": Provenance((_CHDB, _CH_SERVER), (_CHDB,), Run("101", "abc1234")),
    "trino": Provenance((), (), None, "no leg runs it"),
}


def _node(sql: str) -> exp.Expr:
    return sqlglot.parse_one(sql, read="postgres")


def test_the_columns_are_every_connector_dialect_provisa_sends_sql_to():
    """REQ-2236, EVERY CONNECTOR PROVISA SENDS SQL TO; SQL CONNECTORS ONLY: every dialect the direct
    route knows but Qdrant's, which is lowered to its API and runs no SQL, and the dialects of the
    federation engines Provisa writes its own SQL for (provisa/db2, provisa/saphana), which the
    direct route does not list."""
    assert set(DIALECTS) == set(CAPABILITIES) - {"qdrant"} | {"db2", "saphana"}


@pytest.mark.parametrize(
    ("dialect", "column"),
    [
        ("duckdb", "duckdb"),
        ("trino", "trino"),
        ("presto", "presto"),
        ("athena", "athena"),
        ("clickhouse", "clickhouse"),
        ("snowflake", "snowflake"),
        ("tsql", "tsql"),
        ("postgres", "postgres"),
        ("qdrant", None),
        ("spark", None),
    ],
)
def test_each_dialect_is_its_own_column_or_none(dialect: str, column: str | None):
    assert column_of(dialect) == column


@pytest.mark.parametrize(
    ("dialect", "written"),
    [("presto", "trino"), ("athena", "trino"), ("trino", "trino"), ("duckdb", "duckdb")],
)
def test_presto_and_athena_forms_are_written_in_trinos_sql(dialect: str, written: str):
    assert written_in(dialect) == written


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


def test_a_dialect_that_is_no_column_matches_with_no_form():
    applied = _TOY.read(_node("shout(x)"), "spark")
    assert applied is not None and applied.form is None


def test_postgresql_is_a_column_where_every_feature_is_native():
    applied = _TOY.read(_node("shout(x)"), "postgres")
    assert applied is not None and applied.form is None
    assert all(_TOY.status(f, "postgres") is Status.NATIVE for f in _TOY.features)


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
        ((Feature("f", "t", "", Side((Rule("f($a)", {"qdrant": "g($a)"}),))),), "no dialect"),
        ((Feature("f", "t", "", Side((Rule(None, {"duckdb": "g(1)"}),))),), "function name"),
        ((Feature("f", "t", "", Side((Rule("f($a)", {"duckdb": "g($b)"}),))),), "does not have"),
        ((Feature("f", "t", "", Side((Rule("f($a)", {"trino": "g(($a"}),))),), "does not parse"),
        ((Feature("f", "t", "", Side(limitations={"x": ()})),), "no dialect"),
        ((Feature("f", "t", "", Side((Rule("f($a)", {"sources": "g($a)"}),))),), "no dialect"),
        # Presto's forms are read as Trino's SQL: DuckDB's integer division is none.
        ((Feature("f", "t", "", Side((Rule("f($a)", {"presto": "$a // 2"}),))),), "does not parse"),
    ],
)
def test_a_grid_that_does_not_hold_is_refused_when_built(features, message: str):
    with pytest.raises(RuleError, match=message):
        Grid(features)


def test_a_dialects_page_lists_what_is_not_native_and_counts_the_rest():
    page = docs.dialect_page(_TOY, "trino", _TOY_PROVENANCE)
    assert page.startswith("# Polyfills on Trino")
    assert "1 of the grid&#x27;s" not in page  # nothing escaped outside a cell
    assert "2 of the grid's 4 features are native on Trino and left out" in page
    assert "Rules on this page: 2, of which 1 are only the name of a rewrite function" in page
    assert "<strong>one_of_a_kind</strong>" in page and "<strong>a_money_value</strong>" in page
    assert "<strong>shout</strong>" not in page and "<strong>abs</strong>" not in page
    assert f"function {Rewrite(lowered_to_one).name}: Writes a call of one_of_a_kind()" in page
    assert "CAST($v AS money)\n=&gt;\nCAST($v AS DECIMAL(20, 2))" in page
    assert "<li>Two decimal places.</li>" in page


def test_a_refusal_is_shown_with_its_sqlstate_and_message():
    page = docs.dialect_page(_TOY, "clickhouse", _TOY_PROVENANCE)
    assert "refused (its error is under the limitations)" in page
    assert (
        "<li>Refused: shout($a::text). <code>0A000</code> shout has no form on clickhouse</li>"
    ) in page
    assert "<li>Refused. <code>0A000</code> shout has no form on clickhouse</li>" in page


def test_the_index_is_one_table_per_dialect_of_what_is_not_native_there():
    """The index is pivoted and split: per dialect, a table of each feature not native there, the
    same table as the dialect's page, with the native features counted, not listed."""
    page = docs.summary_page(_TOY, _TOY_PROVENANCE)
    assert "| Feature | Family |" not in page  # no feature-by-dialect matrix
    for dialect, connector in DIALECTS.items():
        assert f"## [{connector.title}]({dialect}.md)" in page
        assert docs.dialect_table(_TOY, dialect, _TOY_PROVENANCE) in page
        assert docs.dialect_table(_TOY, dialect, _TOY_PROVENANCE) in docs.dialect_page(
            _TOY, dialect, _TOY_PROVENANCE
        )
    trino = page.split("## [Trino](trino.md)", 1)[1].split("\n## ", 1)[0]
    assert "2 of the grid's 4 features are native on Trino and left out" in trino
    assert "<strong>one_of_a_kind</strong>" in trino and "<strong>shout</strong>" not in trino
    postgres = page.split("## [PostgreSQL](postgres.md)", 1)[1].split("\n## ", 1)[0]
    assert "4 of the grid's 4 features are native on PostgreSQL and left out" in postgres
    assert "<tbody>\n</tbody>" in postgres


def test_a_page_says_when_its_forms_are_another_dialects_sql():
    assert "Its forms are written in Trino's SQL" in docs.dialect_page(
        _TOY, "athena", _TOY_PROVENANCE
    )
    assert "Its forms are written in" not in docs.dialect_page(_TOY, "trino", _TOY_PROVENANCE)


def test_every_page_states_its_rule_and_the_pages_are_one_per_dialect():
    written = docs.pages(GRID, _PROVENANCE)
    assert set(written) == {
        f"{docs.DIRECTORY}/index.md",
        *(f"{docs.DIRECTORY}/{dialect}.md" for dialect in DIALECTS),
    }
    for path, text in written.items():
        assert "Generated by scripts/gen_polyfill_docs.py" in text, path


@pytest.mark.parametrize("path", sorted(docs.pages(GRID, _PROVENANCE)))
def test_the_committed_pages_are_the_grids(path: str):
    """The pages cannot drift from the grid: scripts/gen_polyfill_docs.py writes them."""
    assert (_ROOT / path).read_text() == docs.pages(GRID, _PROVENANCE)[path]


def test_the_site_lists_every_page():
    """mkdocs.yml's nav names each generated page, and no page the grid no longer writes."""
    nav = (_ROOT / "mkdocs.yml").read_text()
    listed = {
        line.split(": ", 1)[1] for line in nav.splitlines() if "conformance/polyfill/" in line
    }
    assert listed == {path.removeprefix("docs/") for path in docs.pages(GRID, _PROVENANCE)}


def test_each_row_prints_what_it_was_made_and_verified_on_and_its_last_run():
    """REQ-2236 (amended 2026-11-27): each row's provenance is on its page and the index."""
    duckdb = docs.dialect_page(_TOY, "duckdb", _TOY_PROVENANCE)
    row = duckdb.split("<strong>shout</strong>", 1)[1].split("</td>", 1)[0]
    assert "Made on: duckdb. Verified on: duckdb, last by run 101 at abc1234." in row
    clickhouse = docs.dialect_page(_TOY, "clickhouse", _TOY_PROVENANCE)
    row = clickhouse.split("<strong>shout</strong>", 1)[1].split("</td>", 1)[0]
    assert (
        "Made on: clickhouse, clickhouse-server. Verified on: clickhouse, last by run 101"
    ) in row
    trino = docs.dialect_page(_TOY, "trino", _TOY_PROVENANCE)
    for name in ("one_of_a_kind", "a_money_value"):
        row = trino.split(f"<strong>{name}</strong>", 1)[1].split("</td>", 1)[0]
        assert "Made on: no leg. Verified on: none (no leg runs it)." in row
    index = docs.summary_page(_TOY, _TOY_PROVENANCE)
    assert index.count("Verified on:") == 4  # shout on duckdb and clickhouse, two rows on trino


def test_a_page_names_each_leg_with_its_image_and_the_run():
    page = docs.dialect_page(_TOY, "clickhouse", _TOY_PROVENANCE)
    assert "`clickhouse` on in-process (chdb)" in page
    assert (
        "`clickhouse-server` on clickhouse/clickhouse-server:24.3 (not judged by the last run)"
        in (page)
    )
    assert "last verified by run 101 at abc1234, on clickhouse." in page


def test_a_row_with_no_provenance_is_refused():
    """A dialect with a row and no provenance is an error, never printed as unknown."""
    missing = {k: v for k, v in _TOY_PROVENANCE.items() if k != "clickhouse"}
    with pytest.raises(RuleError, match=r"no provenance on \['clickhouse'\]"):
        docs.pages(_TOY, missing)
    with pytest.raises(RuleError, match="no provenance on clickhouse"):
        docs.dialect_table(_TOY, "clickhouse", missing)


@pytest.mark.parametrize(
    "made, verified, run, unverified, message",
    [
        ((_DUCK,), (_DUCK,), None, None, "names its verifying run"),
        ((_DUCK,), (), Run("1", "a"), None, "names the legs it judged"),
        ((_DUCK,), (_DUCK,), Run("1", "a"), "why", "names its verifying run"),
        ((), (_DUCK,), Run("1", "a"), None, "only on a leg it is made on"),
        ((_DUCK,), (_DUCK,), None, "why", "verified on a leg names the run"),
    ],
)
def test_a_provenance_that_does_not_hold_is_refused(made, verified, run, unverified, message):
    with pytest.raises(RuleError, match=message):
        Provenance(made, verified, run, unverified)


def test_every_dialect_with_a_row_has_its_provenance_from_the_test_configuration():
    """Read from function_legs.yaml and expected/functions.json, never written by hand: a leg of a
    dialect is verified exactly when the recorded run judged it, by that run."""
    legs = polyfill_provenance.function_legs.load()["legs"]
    measured = polyfill_provenance.json.loads(polyfill_provenance.MEASURED.read_text())
    judged = set(measured["engines"])
    for dialect in DIALECTS:
        if not GRID.on(dialect):
            assert dialect not in _PROVENANCE
            continue
        of = _PROVENANCE[dialect]
        engines = {
            leg["engine"]
            for leg in legs
            if polyfill_provenance.polyfill_check.ENGINE_DIALECTS[leg["engine"]] == dialect
        }
        assert {leg.engine for leg in of.made_on} == engines, dialect
        assert {leg.engine for leg in of.verified_on} == engines & judged, dialect
        if of.verified_on:
            assert of.run == Run(measured["measured"]["run"], measured["measured"]["sha"])


@pytest.mark.parametrize("dialect", [d for d in DIALECTS if GRID.on(d)])
def test_every_row_on_a_committed_page_prints_its_provenance(dialect: str):
    """A page that omits a row's provenance fails: each row's feature cell carries it."""
    page = (_ROOT / docs.DIRECTORY / f"{dialect}.md").read_text()
    rows = page.split("<tr>")[2:]
    assert len(rows) == len(GRID.on(dialect))
    expected = html.escape(provenance.line(_PROVENANCE[dialect]))
    for row in rows:
        feature = row.split("</td>", 1)[0]
        assert f"<em>{expected}</em>" in feature, row[:200]
