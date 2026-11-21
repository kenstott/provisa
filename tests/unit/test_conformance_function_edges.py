# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2f47a1-5e93-4b06-9d1a-e46b7c3f2a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The functions measure's edge values (REQ-2236, amended 2026-11-15, AN EDGE CASE RAISES; IT DOES
NOT REFUSE THE FORM): every overload given samples is also called with its argument types' edge
values, at most ``EDGE_CALLS_MAX`` of them; an edge answered otherwise than PostgreSQL is a wrong
answer, an edge raised where PostgreSQL answers is served within its bound when the grid row states
one, and a bounded row's stated error is held to be raised."""

# Requirements: REQ-2236

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from provisa.transpiler.polyfill import Feature, Grid, Limitation, Rule, Side

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))

import function_edges as edges  # noqa: E402
import function_report as report  # noqa: E402
import function_support as support  # noqa: E402
import polyfill_check as check  # noqa: E402
from pgwire_function_probe import Function  # noqa: E402

ABS = Function("abs", "integer", argtypes=("integer",), volatility="i", rettype="integer")
CONCAT = Function("textcat", "text, text", argtypes=("text", "text"), volatility="i")
SUM = Function("sum", "integer", kind="a", argtypes=("integer",), volatility="i")
TRUNC = Function(
    "date_trunc",
    "text, timestamp with time zone",
    argtypes=("text", "timestamp with time zone"),
    volatility="s",
)

#: A bound as a grid row's limitation states one.
STATED = ("Exact from 1900 to 2299; a value outside raises 0A000 (f() has no exact form).",)


def _ok(*values: Any, oid: int = 23) -> dict[str, Any]:
    return {"ok": True, "fields": [oid], "rows": [(v,) for v in values]}


def _raises(sqlstate: str, message: str = "boom") -> dict[str, Any]:
    return {"ok": False, "error": f"ERROR:  {message}", "sqlstate": sqlstate}


class Server:
    """A server answering by statement, and every other statement with ``otherwise``."""

    def __init__(self, answers: dict[str, dict[str, Any]], otherwise: dict[str, Any]) -> None:
        self.answers = answers
        self.otherwise = otherwise

    def answer(self, sql: str) -> dict[str, Any]:
        return self.answers.get(sql, self.otherwise)


def _route(_sql: str) -> str:
    return "PASS_THROUGH"


def _no_bound(_statement: str, _engine: str) -> tuple[str, ...]:
    return ()


def _bound(_statement: str, _engine: str) -> tuple[str, ...]:
    return STATED


def _measured(
    want: dict[str, dict[str, Any]],
    got: dict[str, dict[str, Any]],
    *,
    fn: Function = ABS,
    bounds: edges.Bounds = _no_bound,
) -> dict[str, Any]:
    """``fn`` where its samples and NULL answer 3 on both servers and its edges as given."""
    plain = {c.sql: _ok("3") for c in support.calls(fn, "Numeric")}
    return support.measure_overload(
        fn,
        "Numeric",
        Server({**plain, **want}, _ok("3")),
        Server({**plain, **got}, _ok("3")),
        _route,
        engine="clickhouse",
        bounds=bounds,
    )


def _bounded(statement: str) -> str:
    return f"{statement} LIMIT {edges.EDGE_ROWS_MAX + 1}"


def _edge(fn: Function, kind: str) -> str:
    return next(c.sql for c in support.edge_calls(fn, "Numeric") if c.edge.endswith(kind))


# --- the edge table and the calls it makes ------------------------------------------------------


def test_every_edge_type_is_a_sampled_type_and_every_edge_kind_is_named():
    sampled = set(support.SAMPLES) | set(support.VALUE_SAMPLES)
    assert set(edges.EDGES) <= sampled
    kinds = {
        edges.EMPTY,
        edges.LEAST,
        edges.GREATEST,
        edges.ZERO,
        edges.NEGATIVE,
        edges.MULTIBYTE,
        edges.COMBINING,
        edges.SPECIAL,
        edges.PAST,
    }
    assert {e.kind for row in edges.EDGES.values() for e in row} <= kinds
    assert edges.NULL_EDGE.kind == edges.NULL


def test_the_table_holds_each_kind_of_edge_the_measure_promises():
    integer = {e.kind: e.literal for e in edges.EDGES["integer"]}
    assert integer[edges.LEAST] == "'-2147483648'"
    assert integer[edges.GREATEST] == "'2147483647'"
    assert integer[edges.PAST] == "'2147483648'"
    text = {e.kind: e.literal for e in edges.EDGES["text"]}
    assert text[edges.EMPTY] == "''"
    assert text[edges.COMBINING] == "'été'"
    assert any(len(ch.encode()) == 4 for ch in text[edges.MULTIBYTE])
    stamps = {e.kind: e.literal for e in edges.EDGES["timestamp without time zone"]}
    assert stamps[edges.LEAST] == "'4713-01-01 00:00:00 BC'"
    assert stamps[edges.GREATEST] == "'294276-12-31 23:59:59.999999'"
    intervals = {e.kind for e in edges.EDGES["interval"]}
    assert {edges.ZERO, edges.NEGATIVE} <= intervals
    assert "'{}'" in {e.literal for e in edges.EDGES["jsonb"]}


def test_a_one_argument_function_gets_its_types_edges_and_no_second_null():
    assert [(c.sql, c.edge) for c in support.edge_calls(ABS, "Numeric")] == [
        (_bounded("SELECT pg_catalog.\"abs\"('-2147483648'::integer) AS v"), "integer least"),
        (_bounded("SELECT pg_catalog.\"abs\"('2147483647'::integer) AS v"), "integer greatest"),
        (_bounded("SELECT pg_catalog.\"abs\"('0'::integer) AS v"), "integer zero"),
        (
            _bounded("SELECT pg_catalog.\"abs\"('2147483648'::integer) AS v"),
            "integer past the type",
        ),
    ]
    assert all(c.answers and c.values for c in support.edge_calls(ABS, "Numeric"))


def test_edges_are_shared_round_robin_among_the_arguments_up_to_the_bound():
    planned = support.edge_calls(CONCAT, "String")
    assert len(planned) == edges.EDGE_CALLS_MAX
    assert [c.edge for c in planned[:4]] == ["text null", "text null", "text empty", "text empty"]
    assert planned[0].sql == _bounded("SELECT pg_catalog.\"textcat\"(NULL::text, 'abc'::text) AS v")
    assert planned[1].sql == _bounded("SELECT pg_catalog.\"textcat\"('abc'::text, NULL::text) AS v")


@pytest.mark.parametrize(
    "fn",
    [
        ABS,
        CONCAT,
        SUM,
        TRUNC,
        Function(
            "f",
            "integer, bigint, numeric, text",
            argtypes=("integer", "bigint", "numeric", "text"),
            volatility="i",
        ),
    ],
)
def test_no_overload_gets_more_edge_calls_than_the_bound(fn):
    assert len(support.edge_calls(fn, "Numeric")) <= edges.EDGE_CALLS_MAX


def test_an_argument_given_its_own_literals_keeps_them():
    planned = support.edge_calls(TRUNC, "Date")
    assert planned and all(
        c.sql.startswith("SELECT pg_catalog.\"date_trunc\"('day'::text, ") for c in planned
    )
    assert {c.edge.split(" ")[0] for c in planned} == {"timestamp"}


def test_an_aggregate_reads_its_edge_value_in_two_rows():
    planned = support.edge_calls(SUM, "Aggregate")
    assert planned[0].edge == "integer null"
    greatest = next(c.sql for c in planned if c.edge == "integer greatest")
    assert greatest == _bounded(
        "SELECT pg_catalog.\"sum\"(c1) AS v FROM (VALUES ('2147483647'::integer), "
        "('2147483647'::integer)) AS t(c1)"
    )


def test_an_overload_given_no_samples_gets_no_edges():
    volatile = Function("random", "", volatility="v")
    assert support.edge_calls(volatile, "Numeric") == []
    no_args = Function("pi", "", volatility="i")
    assert support.edge_calls(no_args, "Numeric") == []


# --- what an edge call makes of an overload -----------------------------------------------------


def test_edges_answered_as_postgresql_does_keep_a_pass_right():
    entry = _measured({}, {})
    assert entry["outcome"] == "pass"
    assert entry["edges"] == {
        "calls": 4,
        "uncompared": 0,
        "agreed": 4,
        "postgres_raised": 0,
        "wrong": 0,
        "raised": 0,
        "errors": 0,
    }
    assert "bound_raised" not in entry


def test_an_edge_answered_otherwise_is_a_wrong_answer_at_an_edge():
    least = _edge(ABS, "least")
    entry = _measured({least: _raises("22003")}, {least: _ok("-2147483648")})
    assert entry["outcome"] == "wrong_answer" and entry["wrong_at_edge"] is True
    assert entry["detail"]["statement"] == least
    assert entry["detail"]["edge"] == "integer least"
    assert "PostgreSQL raised 22003" in entry["detail"]["difference"]


def test_an_edge_value_that_differs_is_wrong_even_where_a_bound_is_stated():
    zero = _edge(ABS, "zero")
    entry = _measured({zero: _ok("0")}, {zero: _ok("1")}, bounds=_bound)
    assert entry["outcome"] == "wrong_answer" and entry["wrong_at_edge"] is True


def test_raising_the_same_class_postgresql_raises_agrees():
    past = _edge(ABS, "past the type")
    entry = _measured({past: _raises("22003")}, {past: _raises("22P02")})
    assert entry["outcome"] == "pass" and entry["edges"]["postgres_raised"] == 1


def test_raising_where_postgresql_answers_is_within_a_stated_bound():
    greatest = _edge(ABS, "greatest")
    entry = _measured({greatest: _ok("2147483647")}, {greatest: _raises("0A000")}, bounds=_bound)
    assert entry["outcome"] == support.WITHIN_BOUND
    assert entry["detail"]["edge"] == "integer greatest"
    assert entry["bound_stated"] == list(STATED)
    assert entry["bound_sqlstates"] == ["0A000"] and entry["bound_raised"] is True
    assert report.result_of(entry["outcome"]) == report.WITHIN_BOUND
    assert report.verdict(entry) == report.PASSED


def test_raising_another_class_than_postgresql_is_within_a_stated_bound():
    past = _edge(ABS, "past the type")
    entry = _measured({past: _raises("22003")}, {past: _raises("0A000")}, bounds=_bound)
    assert entry["outcome"] == support.WITHIN_BOUND
    assert entry["detail"]["postgres"] == "22003 ERROR:  boom"


def test_raising_at_an_edge_the_grid_states_no_bound_for_is_an_error():
    greatest = _edge(ABS, "greatest")
    entry = _measured({greatest: _ok("2147483647")}, {greatest: _raises("0A000")})
    assert entry["outcome"] == "error" and entry["error_at_edge"] is True
    assert entry["detail"]["unstated"] == (
        "it raises at an edge value; its grid row on clickhouse states no bound"
    )


def test_a_lost_connection_at_an_edge_is_an_error_and_no_raise():
    greatest = _edge(ABS, "greatest")
    lost = {"ok": False, "error": "connection: closed", "lost": True}
    entry = _measured({greatest: _ok("2147483647")}, {greatest: lost}, bounds=_bound)
    assert entry["outcome"] == "error" and entry["error_at_edge"] is True


def test_a_bounded_row_that_raised_nothing_at_its_edges_is_said():
    entry = _measured({}, {}, bounds=_bound)
    assert entry["outcome"] == "pass"
    assert entry["bound_sqlstates"] == ["0A000"] and entry["bound_raised"] is False


def test_an_edge_postgresql_answers_with_more_rows_than_the_bound_compares_nothing():
    least = _edge(ABS, "least")
    many = _ok(*range(edges.EDGE_ROWS_MAX + 1))
    entry = _measured({least: many}, {least: _ok("1")})
    assert entry["outcome"] == "pass"
    assert entry["edges"]["calls"] == 3 and entry["edges"]["uncompared"] == 1
    assert least.endswith(f"LIMIT {edges.EDGE_ROWS_MAX + 1}")


def test_an_overload_failing_its_samples_gets_no_edge_calls():
    sample = support.value_calls(ABS, "Numeric")[0].sql
    entry = _measured({sample: _ok("3")}, {sample: _ok("4")})
    assert entry["outcome"] == "wrong_answer" and "edges" not in entry


def test_the_stated_raises_are_read_from_the_limitation_text():
    assert edges.stated_raises(("a literal outside raises 0A000 (x)",)) == ["0A000"]
    assert edges.stated_raises(("past its range raises PostgreSQL's 22003",)) == ["22003"]
    assert edges.stated_raises(("Of an interval, refused by name.",)) == []


def test_the_grid_states_a_bound_of_a_call_as_the_translator_reads_it():
    call = (
        "SELECT pg_catalog.\"date_trunc\"('day'::text, "
        "'2024-06-15 12:30:45+00'::timestamp with time zone) AS v"
    )
    stated = edges.grid_bounds(call, "clickhouse")
    assert "0A000" in edges.stated_raises(stated)
    assert edges.grid_bounds('SELECT pg_catalog."abs"(3::integer) AS v', "pg") == ()
    # An engine whose dialect is no column of the grid: none (run 44918205557's KeyError).
    assert edges.grid_bounds('SELECT pg_catalog."abs"(3::integer) AS v', "mariadb") == ()


# --- counts, the report and the polyfill check ---------------------------------------------------


def test_served_is_right_and_within_bound_and_the_edge_findings_are_counted_apart():
    outcomes = [
        {"outcome": "pass"},
        {"outcome": "pass", "rules": [report.ROW_ORDER]},
        {"outcome": support.WITHIN_BOUND},
        {"outcome": "wrong_answer", "wrong_at_edge": True},
        {"outcome": "error", "error_at_edge": True},
        {"outcome": "refused"},
    ]
    c = report.result_counts(outcomes)
    assert (c["passed"], c["right"], c["within_bound"]) == (3, 2, 1)
    assert (c["differs_wrong_answer"], c["differs_wrong_at_edge"]) == (1, 1)
    assert (c["differs_engine_error"], c["differs_error_at_edge"]) == (1, 1)
    assert c["refused_by_name"] == 1 and c["failed"] == 3
    assert c["passed"] + c["failed"] == c["total"]


def _merged(entry: dict[str, Any]) -> dict[str, Any]:
    base = {"function": "abs", "overload": "integer", "family": "Numeric", "kind": "f"}
    leg = {
        "engine": "clickhouse",
        "postgres_version": "17",
        "server_gone": None,
        "overloads": [{**base, "volatility": "i", "answers_checked": True, **entry}],
    }
    return support.merged({"clickhouse": leg})


def test_the_pages_show_the_within_bound_count_and_the_edge_method_in_plain_words():
    raised = {"statement": "SELECT 1", "edge": "integer greatest", "error": "ERROR:  x"}
    doc = _merged({"outcome": support.WITHIN_BOUND, "detail": raised, "edges": {"calls": 4}})
    kept = doc["functions"][0]["outcomes"]["clickhouse"]
    assert kept["result"] == report.WITHIN_BOUND and kept["edges"] == {"calls": 4}
    assert doc["by_engine"]["clickhouse"]["within_bound"] == 1
    page = report.pgwire_block(doc)
    assert "| Engine | Served | Of which within bound |" in page
    assert "| ClickHouse | 1 | 1 |" in page
    assert "### Edge values" in page and "### Served within a bound" in page
    assert f"at most {edges.EDGE_CALLS_MAX} edge calls" in page
    assert "| `abs(integer)` | integer greatest | ERROR: x |" in page
    assert "REQ-" not in page


def test_a_wrong_answer_at_an_edge_says_which_edge():
    wrong = {"statement": "SELECT 1", "edge": "text empty", "difference": "'a', PostgreSQL ''"}
    doc = _merged({"outcome": "wrong_answer", "detail": wrong, "wrong_at_edge": True})
    page = report.pgwire_block(doc)
    assert "at an edge (text empty): 'a', PostgreSQL ''" in page
    assert report.cell(doc["functions"][0]["outcomes"]["clickhouse"]) == "differs at an edge"


def test_the_polyfill_check_holds_within_bound_and_fails_a_bound_never_raised():
    feature = Feature(
        "shout",
        "family",
        "summary",
        read=Side(
            rules=(Rule("shout($a::text)", {"clickhouse": "upper($a)"}),),
            limitations={"clickhouse": (Limitation(STATED[0]),)},
        ),
    )
    grid = Grid((feature,))

    def verdict(outcome: dict[str, Any]) -> str:
        run = {
            "engines": ["clickhouse"],
            "functions": [
                {"function": "shout", "overload": "text", "outcomes": {"clickhouse": outcome}}
            ],
        }
        return check.checked(run, grid)[0].verdict

    assert verdict({"outcome": support.WITHIN_BOUND, "bound_raised": True}) == check.HOLDS
    never = {"outcome": "pass", "bound_raised": False, "bound_sqlstates": ["0A000"]}
    assert verdict(never) == check.FAILS


def _refusing(node: Any, dialect: str) -> Any:
    """Refuses every call it is given, as a rewrite function refusing a value does."""
    from provisa.transpiler.polyfill.notation import Refusal, Refused

    raise Refused(Refusal("0A000", f"growl() has no form on {dialect}"))


def test_a_rewrite_function_refusing_the_call_is_judged_by_its_refusal_not_a_crash():
    """A rewrite function raising its refusal while the check reads the grid (age(xid) on
    ClickHouse does) is the row's statement of that call: it holds where the run raised it."""
    from provisa.transpiler.polyfill.notation import Rewrite

    feature = Feature(
        "growl",
        "family",
        "summary",
        read=Side(rules=(Rule(None, {"clickhouse": Rewrite(_refusing, mentions="growl")}),)),
    )
    grid = Grid((feature,))

    def verdict(outcome: dict[str, Any]) -> tuple[str, str]:
        run = {
            "engines": ["clickhouse"],
            "functions": [
                {"function": "growl", "overload": "text", "outcomes": {"clickhouse": outcome}}
            ],
        }
        [finding] = check.checked(run, grid)
        return finding.verdict, finding.feature

    refused = {
        "outcome": "refused",
        "detail": {
            "statement": "SELECT growl('a')",
            "error": "ERROR:  growl() has no form on clickhouse",
            "sqlstate": "0A000",
        },
    }
    assert verdict(refused) == (check.HOLDS, check.REWRITE_REFUSAL)
    assert verdict({"outcome": "pass"})[0] == check.FAILS
    assert verdict({"outcome": "not_checked"})[0] == check.UNVERIFIED
