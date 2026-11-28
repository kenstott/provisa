# Copyright (c) 2026 Kenneth Stott
# Canary: 0f0d643f-5722-46a7-b0d4-991af7aafa80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The rows state what the translator and the engines were measured to do (REQ-2236, THE REFUSAL
LIVES IN THE GRID; polyfill triage G3, G9, G10 of run 45042021798): provisa.transpiler.polyfill
.measured. Each refusal it states is the translator's, read with the rows as their families write
them: a row that keeps the old table's form, or is native, refuses where the old table or an
engine's refusal does; and a literal cast to text is still the literal DuckDB's forms read."""

# Requirements: REQ-2236

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest
import sqlglot

from provisa.transpiler import function_macros, polyfill, transpile
from provisa.transpiler.function_macros import FunctionRefused
import provisa.transpiler.polyfill.measured as measured
from provisa.transpiler.polyfill import GRID, Limitation, Refusal, trino
from provisa.transpiler.polyfill.notation import Refused


def _translated(statement: str, dialect: str) -> str:
    """``statement`` as pgwire translates it for an engine of ``dialect``."""
    if dialect == "duckdb":
        return transpile.transpile_to_duckdb(statement, generated=False)
    return transpile.transpile(statement, dialect, generated=False)


def _refusal(statement: str, dialect: str) -> tuple[str, str] | None:
    """The SQLSTATE and message a client gets for ``statement`` on ``dialect``; None where the
    translator writes it."""
    try:
        _translated(statement, dialect)
    except FunctionRefused as refused:
        return refused.sqlstate, str(refused.args[0])
    except NotImplementedError as refused:  # pgwire's 0A000 (server._SQLSTATE_BY_CLASS)
        return "0A000", str(refused)
    return None


@pytest.fixture
def families_grid(monkeypatch: pytest.MonkeyPatch) -> Iterator[polyfill.Grid]:
    """The translator reading the rows as their families write them, nothing of measured stated:
    what it refuses then is what measured must state."""
    grid = polyfill.Grid(polyfill._features())
    monkeypatch.setattr(polyfill, "GRID", grid)
    function_macros._grid_mentioned.cache_clear()
    transpile._duckdb_form.cache_clear()
    yield grid
    function_macros._grid_mentioned.cache_clear()
    transpile._duckdb_form.cache_clear()


def _read(statement: str, dialect: str, grid: polyfill.Grid) -> polyfill.Applied | None:
    node = sqlglot.parse_one(
        transpile._catalog_functions_unqualified(statement), read="postgres"
    ).expressions[0]
    return grid.read(node.unalias(), dialect)


_REFUSED = [
    (feature, pattern, message, dialect, call)
    for (feature, pattern), entries in measured.REFUSED.items()
    for message, dialects, call in entries
    for dialect in dialects.split()
]
_LIMITED = [
    (feature, pattern, sqlstate, message, dialect, call)
    for (feature, pattern), entries in measured.LIMITED.items()
    for sqlstate, message, dialects, _text, call in entries
    for dialect in dialects.split()
]


@pytest.mark.usefixtures("families_grid")
@pytest.mark.parametrize(("feature", "pattern", "message", "dialect", "call"), _REFUSED)
def test_a_refused_rule_is_the_translators_refusal(
    feature: str, pattern: str, message: str, dialect: str, call: str
) -> None:
    """Each rule measured refuses on a dialect is refused there by name with the rules as their
    families write them: the old table's (or an engine refusal's) message."""
    assert _refusal(call, dialect) == ("0A000", message.format(dialect=dialect)), (
        feature,
        pattern,
    )


@pytest.mark.usefixtures("families_grid")
@pytest.mark.parametrize(("feature", "pattern", "sqlstate", "message", "dialect", "call"), _LIMITED)
def test_a_limited_rule_is_the_translators_refusal(
    feature: str, pattern: str, sqlstate: str, message: str, dialect: str, call: str
) -> None:
    """Each refusal measured lists as a row's limitation is the translator's for its call."""
    assert _refusal(call, dialect) == (sqlstate, message.format(dialect=dialect)), (
        feature,
        pattern,
    )


@pytest.mark.parametrize(("feature", "pattern", "message", "dialect", "call"), _REFUSED)
def test_the_grid_states_a_refused_rule_as_its_form(
    feature: str, pattern: str, message: str, dialect: str, call: str
) -> None:
    """The grid's form of a refused rule there is the refusal, which the translator raises."""
    applied = _read(call, dialect, GRID)
    assert applied is not None and (applied.feature.name, applied.rule.pattern or "") == (
        feature,
        pattern,
    )
    assert applied.form == Refusal("0A000", message.format(dialect=dialect))
    assert _refusal(call, dialect) == ("0A000", message.format(dialect=dialect))


def test_a_kept_row_the_old_table_has_no_form_of_is_refused_by_the_grid() -> None:
    """area(box) keeps the old table's form, which T-SQL has none of: its row says so (G3)."""
    applied = GRID.read(sqlglot.parse_one("area(CAST(NULL AS box))", read="postgres"), "tsql")
    assert applied is not None
    assert applied.form == Refusal("0A000", "area() has no form on tsql")
    # DuckDB, Trino and ClickHouse have geo_ops forms of it; Athena keeps the old table's.
    athena_form = GRID.read(sqlglot.parse_one("area(CAST(NULL AS box))", read="postgres"), "athena")
    assert athena_form is not None and athena_form.form is trino.KEPT


def test_a_refusal_for_some_values_is_a_limitation() -> None:
    """avg of an exact value the statement types is refused on MySQL, avg of a column is not: the
    row keeps the old table's form and states the refusal (G3)."""
    row = next(f for f in GRID.features if f.name == "avg")
    refusals = [lim.refusal for lim in row.read.limitations["mysql"]]
    assert Refusal("0A000", "avg() has no form on mysql") in refusals
    assert _refusal("SELECT avg(3::integer) AS v", "mysql") == (
        "0A000",
        "avg() has no form on mysql",
    )
    assert _refusal("SELECT avg(x) AS v FROM t", "mysql") is None


def test_a_rewrite_refusal_the_row_did_not_state_is_a_limitation() -> None:
    """bool of a jsonb value is refused on Trino by the row's own rewrite: the row states it."""
    row = next(f for f in GRID.features if f.name == "bool")
    assert Refusal("0A000", "bool() has no form on trino") in [
        lim.refusal for lim in row.read.limitations["trino"]
    ]


def test_an_engines_edge_raise_is_a_stated_bound() -> None:
    """abs(numeric) on DuckDB raises 22P02 at a numeric of more than 38 digits and at NaN, which
    its DECIMAL does not hold (G9): a bound the row states, with what it raises."""
    row = next(f for f in GRID.features if f.name == "abs")
    texts = [lim.text for lim in row.read.limitations["duckdb"] if lim.refusal is None]
    assert any(
        text.startswith("Raises 22P02 at an edge value") and "numeric 'NaN'" in text
        for text in texts
    )


def test_every_measured_row_and_dialect_is_the_grids() -> None:
    """measured names only rows the grid has, on dialects the grid has."""
    names = {f.name for f in GRID.features}
    assert set(measured.BOUNDED) <= names
    for entries in measured.BOUNDED.values():
        for dialects, _text in entries:
            assert set(dialects.split()) <= set(polyfill.DIALECTS)


def test_a_refused_rule_on_a_written_form_is_a_rule_error() -> None:
    """A rule that writes its own form there is never refused outright by measured."""
    feature = polyfill.Feature(
        "x", "f", "s", read=polyfill.Side(rules=(polyfill.Rule("x($a)", {"tsql": "x($a)"}),))
    )
    with pytest.raises(polyfill.RuleError, match="writes its own form"):
        measured._refused_on(feature, feature.read.rules[0], ["tsql"])


@pytest.mark.parametrize(
    ("statement", "answer"),
    [
        ("""SELECT pg_catalog."to_char"(42::integer, 'FM999.00'::text) AS v""", "42.00"),
        ("""SELECT pg_catalog."to_char"(5000000000::bigint, 'FM999.00'::text) AS v""", "###.##"),
        (
            """SELECT pg_catalog."to_char"('2024-06-15 12:30:45'::timestamp, 'YYYY-MM-DD'::text)"""
            " AS v",
            "2024-06-15",
        ),
        ("""SELECT pg_catalog."encode"('\\xdeadbeef'::bytea, 'hex'::text) AS v""", "deadbeef"),
        ("""SELECT pg_catalog."substring"('abc'::text, 1::text) AS v""", None),
        ("""SELECT pg_catalog."substring"('abc'::text, 'b'::text) AS v""", "b"),
    ],
)
def test_a_literal_cast_to_text_is_a_literal_on_duckdb(statement: str, answer: str | None) -> None:
    """A format or pattern written 'x'::text names its text as plainly as 'x' (G10): PostgreSQL's
    answer, where DuckDB's forms refused it as not a literal."""
    with duckdb.connect() as con:
        assert con.execute(
            transpile.transpile_to_duckdb(statement, generated=False)
        ).fetchall() == [(answer,)]


def test_to_char_of_an_interval_is_refused_on_duckdb() -> None:
    """DuckDB's to_char forms read a timestamp's fields, not an interval's: refused by name, not
    answered as a timestamp (0001-00-01 where PostgreSQL writes 0000-00-01)."""
    statement = "SELECT to_char('1 day 02:03:04'::interval, 'YYYY-MM-DD') AS v"
    with pytest.raises(NotImplementedError, match=r"to_char\(\) of an interval has no form"):
        transpile.transpile_to_duckdb(statement, generated=False)


def test_a_stated_limitation_carries_its_refusal() -> None:
    """A limitation measured adds carries the refusal a client receives, so the docs print it."""
    for (feature, _pattern), entries in measured.LIMITED.items():
        row = next(f for f in GRID.features if f.name == feature)
        for sqlstate, message, dialects, text, _call in entries:
            for dialect in dialects.split():
                expected = Limitation(text, Refusal(sqlstate, message.format(dialect=dialect)))
                assert expected in row.read.limitations[dialect]


def test_no_rewrite_refusal_is_raised_reading_a_refused_rule() -> None:
    """Reading a refused rule raises nothing: its refusal is its form."""
    for (feature, pattern), entries in list(measured.REFUSED.items())[:20]:
        for _message, dialects, call in entries:
            try:
                applied = _read(call, dialects.split()[0], GRID)
            except Refused as refused:  # pragma: no cover - the assertion names it
                pytest.fail(f"{feature} {pattern}: {refused}")
            assert applied is not None
