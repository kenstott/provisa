# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1c6e47-3f92-4b05-9d7e-2c4b8f1a6e90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The grid's hints (REQ-2236, A HINT IS A REWRITE FOR AN ENGINE AND SOURCE PAIR; REQ-2352): rows of
the grid keyed by engine and source, each with its reproducing test, validated when the grid is
built and listed under each source on its dialect's generated page with its provenance."""

# Requirements: REQ-2236, REQ-2352

from __future__ import annotations

import dataclasses
import html
import sys
from pathlib import Path

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler import opengauss_bytea_like
from provisa.transpiler.polyfill import GRID, docs, hints
from provisa.transpiler.polyfill.grid import DIALECTS, Feature, Grid, Hint, Rule, Side
from provisa.transpiler.polyfill.notation import RuleError
from provisa.transpiler.polyfill.provenance import HintProvenance, Provenance, hint_line
from provisa.transpiler.transpile import transpile

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "scripts" / "conformance"))

import polyfill_provenance  # noqa: E402

_HINT_PROVENANCE = polyfill_provenance.read_hints(GRID)

_UPPER = Feature(
    "upper(text)", "toy", "A text in capitals.", read=Side(rules=(Rule("upper($a::text)", {}),))
)
_LOWER = Feature(
    "lower(text)",
    "toy",
    "A text in small letters.",
    read=Side(rules=(Rule("lower($a::text)", {"trino": "lower($a)"}),)),
)
#: An engine hint: Trino on its own tables.
_OWN = Hint(
    name="upper of a long text",
    dialect="trino",
    engine="trino",
    source=None,
    difference="Trino's upper cuts a long text.",
    rule=Rule("upper($a::text)", {"trino": "upper(CAST($a AS varchar))"}),
    features=("upper(text)",),
    test="tests/integration/test_toy_upper.py",
    service="trino",
)
#: A source hint: Trino reading an attached MySQL, of one version.
_MYSQL = Hint(
    name="lower pushed into MySQL",
    dialect="trino",
    engine="trino",
    source="mysql",
    setting="MySQL 8.0",
    difference="MySQL's lower ignores a binary collation.",
    rule=Rule("lower($a::text)", {"trino": "lower(CAST($a AS varchar))"}),
    features=("lower(text)",),
    test="tests/integration/test_toy_lower_mysql.py",
    service="mysql",
)
_TOY = Grid((_UPPER, _LOWER), (_MYSQL, _OWN))
_TOY_HINT_PROVENANCE = {
    "trino": HintProvenance("trino", "trinodb/trino:470"),
    "mysql": HintProvenance("mysql", "mysql:8.0"),
}
_TOY_PROVENANCE = {"trino": Provenance((), (), None, "no leg runs it")}


def test_hints_are_rows_of_the_grid_listed_by_engine_then_source():
    assert _TOY.hints == (_MYSQL, _OWN)
    assert _TOY.hints_on("trino") == (_OWN, _MYSQL)  # its own tables first
    assert _TOY.hints_on("duckdb") == ()


@pytest.mark.parametrize(
    "change, message",
    [
        ({"dialect": "nosuch"}, "no dialect 'nosuch'"),
        ({"engine": "nosuch"}, "no engine kind 'nosuch'"),
        ({"engine": "duckdb"}, "engine 'duckdb' does not write 'trino'"),
        ({"source": "openapi"}, "no SQL or lake source type 'openapi'"),
        ({"rule": Rule("upper($a::text)", {"duckdb": "upper($a)"})}, "its one form is of 'trino'"),
        ({"features": ()}, "names the rows it refines"),
        ({"features": ("nosuch(text)",)}, r"no rows \['nosuch\(text\)'\]"),
        ({"test": ""}, "names its reproducing test and its service"),
        ({"service": ""}, "names its reproducing test and its service"),
        ({"rule": Rule("upper($a::text)", {"trino": "upper($b)"})}, "b"),
    ],
)
def test_a_hint_that_does_not_hold_is_refused_when_the_grid_is_built(change, message):
    with pytest.raises(RuleError, match=message):
        Grid((_UPPER, _LOWER), (dataclasses.replace(_OWN, **change),))


def test_a_hint_named_twice_on_one_engine_and_source_is_refused():
    with pytest.raises(RuleError, match="named twice"):
        Grid((_UPPER, _LOWER), (_OWN, _OWN))
    Grid((_UPPER, _LOWER), (_OWN, dataclasses.replace(_OWN, source="mysql")))  # another pair


def test_a_page_lists_its_hints_under_each_engine_and_source():
    page = docs.dialect_page(_TOY, "trino", _TOY_PROVENANCE, _TOY_HINT_PROVENANCE)
    section = page.split("\n## Hints\n", 1)[1]
    own, mysql = section.split("### `trino` on its own tables\n", 1)[1].split(
        "### `trino` on source `mysql` (MySQL 8.0)\n", 1
    )
    assert "#### upper of a long text" in own and "lower pushed into" not in own
    assert "Known difference: Trino&#x27;s upper cuts a long text." in own
    assert "Rows it refines: `upper(text)`." in own
    assert "upper($a::text)\n=&gt;\nupper(CAST($a AS varchar))" in own
    assert "Reproducing test: `tests/integration/test_toy_upper.py`." in own
    assert (
        "<em>Made and verified on: trino, its own tables, in docker-compose.test.yml service trino"
        " (trinodb/trino:470), by its reproducing test tests/integration/test_toy_upper.py.</em>"
    ) in own
    assert "#### lower pushed into MySQL" in mysql
    assert "Made and verified on: trino, source mysql (MySQL 8.0), in docker-compose.test.yml" in (
        mysql
    )


def test_a_page_with_no_hint_says_so_and_the_index_counts_each_dialects():
    page = docs.dialect_page(_TOY, "duckdb", _TOY_PROVENANCE, _TOY_HINT_PROVENANCE)
    assert page.endswith("No hint is known on DuckDB.\n")
    index = docs.summary_page(_TOY, _TOY_PROVENANCE)
    trino = index.split("## [Trino](trino.md)", 1)[1].split("\n## ", 1)[0]
    assert "Hints: 2, listed under each source on [its page](trino.md#hints)." in trino
    duckdb = index.split("## [DuckDB](duckdb.md)", 1)[1].split("\n## ", 1)[0]
    assert "No hint is known." in duckdb


def test_a_hint_with_no_provenance_is_refused():
    with pytest.raises(RuleError, match=r"no provenance of service \['mysql'\]"):
        docs.pages(_TOY, _TOY_PROVENANCE, {"trino": _TOY_HINT_PROVENANCE["trino"]})
    with pytest.raises(RuleError, match="no provenance of service 'mysql'"):
        hint_line(_MYSQL, {})
    with pytest.raises(RuleError, match="names its service and the service's image"):
        HintProvenance("mysql", "")


def test_a_hints_provenance_is_its_services_image_in_the_compose_configuration():
    compose = {"services": {"trino": {"image": "trinodb/trino:470"}, "mysql": {"image": "m:8"}}}
    assert polyfill_provenance.hint_provenance(_TOY, compose) == {
        "trino": HintProvenance("trino", "trinodb/trino:470"),
        "mysql": HintProvenance("mysql", "m:8"),
    }
    with pytest.raises(ValueError, match="has no 'mysql'"):
        polyfill_provenance.hint_provenance(_TOY, {"services": {"trino": {"image": "t"}}})
    with pytest.raises(ValueError, match="names no image"):
        polyfill_provenance.hint_provenance(
            _TOY, {"services": {"trino": {"image": "t"}, "mysql": {}}}
        )


# --- the committed grid ---------------------------------------------------------------------------


def test_the_grid_holds_the_hints():
    assert GRID.hints == hints.HINTS
    assert hints.OPENGAUSS_BYTEA_LIKE in GRID.hints_on("postgres")


@pytest.mark.parametrize("hint", hints.HINTS, ids=lambda h: h.name)
def test_each_hints_reproducing_test_is_on_disk_and_states_the_requirement(hint: Hint):
    test = _ROOT / hint.test
    assert test.is_file(), hint.test
    assert "REQ-2236" in test.read_text(), hint.test


@pytest.mark.parametrize("hint", hints.HINTS, ids=lambda h: h.name)
def test_each_hint_is_listed_on_its_dialects_committed_page_with_its_provenance(hint: Hint):
    page = (_ROOT / docs.DIRECTORY / f"{hint.dialect}.md").read_text()
    assert f"#### {html.escape(hint.name)}" in page
    assert f"<em>{html.escape(hint_line(hint, _HINT_PROVENANCE))}</em>" in page
    index = (_ROOT / docs.DIRECTORY / "index.md").read_text()
    title = DIALECTS[hint.dialect].title
    section = index.split(f"## [{title}]({hint.dialect}.md)", 1)[1].split("\n## ", 1)[0]
    assert f"Hints: {len(GRID.hints_on(hint.dialect))}," in section


_BYTEA_LIKE = [
    r"""SELECT pg_catalog."bytealike"('\xdeadbeef'::bytea, '\xde25'::bytea) AS v""",
    r"""SELECT '\xdeadbeef'::bytea !~~ '\xdeadbeef'::bytea AS v""",
    r"""SELECT 1 WHERE '\xdeadbeef'::bytea LIKE '\xde25'::bytea""",
]


@pytest.mark.parametrize("statement", _BYTEA_LIKE)
def test_the_opengauss_hints_form_is_what_its_engine_runs(statement: str):
    """The translator runs the hint's form on openGauss, and only there."""
    form = hints.OPENGAUSS_BYTEA_LIKE.form
    by_hint = sqlglot.parse_one(statement, read="postgres").transform(
        lambda node: form.function(node, "postgres") or node
    )
    expected = by_hint.sql(dialect="postgres")
    assert "rawlike(hextoraw(" in expected.lower() or "rawnlike(hextoraw(" in expected.lower()
    on_opengauss = transpile(statement, "postgres", generated=False, engine_kind="opengauss")
    assert on_opengauss == expected
    off = transpile(statement, "postgres", generated=False, engine_kind="pg")
    assert "raw" not in off.lower()


def test_the_opengauss_hints_form_leaves_any_other_node():
    form = hints.OPENGAUSS_BYTEA_LIKE.form
    assert form.function(exp.column("a").like(exp.Literal.string("x%")), "postgres") is None
    assert opengauss_bytea_like.rewritten(exp.Literal.number(1), "postgres") is None
