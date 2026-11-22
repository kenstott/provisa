# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d3b84-2a9c-4e57-b0d8-1c7e5a92f364
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The polyfill grid checked against a Conformance (functions) run (REQ-2236): a feature the grid
marks native or polyfilled that answers otherwise than PostgreSQL fails, and so does a refusal or
difference the grid does not state. Over fixture results in function_support.json's form."""

# Requirements: REQ-2236

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from provisa.transpiler.polyfill import GRID, Feature, Grid, Limitation, Rule, Side, column_of

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))

import polyfill_check as check  # noqa: E402

_ERROR = "ERROR:  "


def _refused(message: str, sqlstate: str = "0A000") -> dict[str, Any]:
    return {
        "outcome": "refused",
        "detail": {"statement": "SELECT f()", "error": _ERROR + message, "sqlstate": sqlstate},
    }


def _error(message: str) -> dict[str, Any]:
    return {"outcome": "error", "detail": {"statement": "SELECT f()", "error": _ERROR + message}}


_PASS = {"outcome": "pass", "result": "exact"}
_WRONG = {"outcome": "wrong_answer", "detail": {"statement": "SELECT f()", "difference": "3, 2"}}


def _run(function: str, overload: str, **outcomes: dict[str, Any]) -> dict[str, Any]:
    return {
        "engines": sorted(outcomes),
        "functions": [{"function": function, "overload": overload, "outcomes": outcomes}],
    }


def _verdicts(run: dict[str, Any], grid: Grid = GRID) -> dict[str, str]:
    return {f.engine: f.verdict for f in check.checked(run, grid)}


def test_a_polyfilled_row_that_answers_as_postgresql_holds():
    run = _run("array_ndims", "anyarray", duckdb=_PASS, pg=_PASS)
    assert _verdicts(run) == {"duckdb": check.HOLDS, "pg": check.HOLDS}


def test_a_polyfilled_row_that_fails_is_a_failure():
    run = _run("array_ndims", "anyarray", trino=_error("FederationError(type=USER_ERROR)"))
    [finding] = check.checked(run)
    assert finding.verdict == check.FAILS and finding.grid == "polyfilled"
    assert "FederationError" in finding.why and finding.statement == "SELECT f()"


def test_a_native_row_that_answers_otherwise_is_a_failure():
    """array_length is Snowflake's own; on PostgreSQL, the reference, every row is native."""
    [finding] = check.checked(_run("array_length", "anyarray, integer", pg=_WRONG))
    assert (finding.grid, finding.verdict) == ("native", check.FAILS)


def test_a_refusal_raised_as_its_row_says_holds():
    run = _run(
        "generate_subscripts",
        "anyarray, integer, boolean",
        trino=_refused("generate_subscripts() has no form on trino"),
        clickhouse=_refused("generate_subscripts() has no form on clickhouse"),
    )
    assert _verdicts(run) == {"trino": check.HOLDS, "clickhouse": check.HOLDS}


@pytest.mark.parametrize(
    "outcome",
    [
        _refused("generate_subscripts() has no form on a direct source"),
        _refused("generate_subscripts() has no form on trino", sqlstate="42883"),
        _PASS,
        _error("boom"),
    ],
    ids=["another message", "another sqlstate", "answered", "an error"],
)
def test_a_refusal_row_the_run_did_not_raise_as_it_says_is_a_failure(outcome):
    run = _run("generate_subscripts", "anyarray, integer, boolean", trino=outcome)
    [finding] = check.checked(run)
    assert (finding.grid, finding.verdict) == ("refused", check.FAILS)


def test_a_refusal_the_run_kept_no_sqlstate_of_is_held_to_its_message():
    outcome = _refused("generate_subscripts() has no form on trino", sqlstate="")
    run = _run("generate_subscripts", "anyarray, integer, boolean", trino=outcome)
    assert _verdicts(run) == {"trino": check.HOLDS}


def test_a_refusal_a_limitation_states_holds():
    run = _run(
        "trim_array",
        "anyarray, integer",
        clickhouse=_refused("trim_array does not read this value"),
    )
    assert _verdicts(run) == {"clickhouse": check.HOLDS}


def test_a_refusal_the_grid_does_not_state_is_a_failure():
    run = _run(
        "array_shuffle",
        "anyarray",
        clickhouse=_refused("array_shuffle() has no form on clickhouse"),
    )
    [finding] = check.checked(run)
    assert finding.verdict == check.FAILS and "does not state" in finding.why


_DIFFERS = Feature(
    "shout",
    "toy",
    "A text in capitals.",
    read=Side(
        rules=(Rule("shout($a::text)", {"duckdb": "upper($a)", "trino": "upper($a)"}),),
        limitations={"duckdb": (Limitation("Only ASCII letters."),)},
    ),
)


def test_a_difference_a_limitation_states_holds_and_one_it_does_not_fails():
    run = _run("shout", "text", duckdb=_WRONG, trino=_WRONG)
    assert _verdicts(run, Grid((_DIFFERS,))) == {"duckdb": check.HOLDS, "trino": check.FAILS}


@pytest.mark.parametrize("outcome", ["not_checked", "by_design", "not_callable", "not_reached"])
def test_an_outcome_that_compared_nothing_is_unverified(outcome: str):
    run = _run("array_ndims", "anyarray", duckdb={"outcome": outcome})
    assert _verdicts(run) == {"duckdb": check.UNVERIFIED}


def test_a_function_the_grid_has_no_row_of_is_not_read():
    """Read against a grid of the toy row alone: the production grid gains rows (abs has one)."""
    assert check.checked(_run("abs", "integer", duckdb=_WRONG), Grid((_DIFFERS,))) == []


def test_an_overload_no_rule_matches_is_not_read():
    """regexp_replace of no flags is no row of the grid (its flags' row takes four arguments)."""
    assert check.checked(_run("regexp_replace", "text, text, text", trino=_WRONG)) == []


@pytest.mark.parametrize(
    ("function", "overload", "feature"),
    [
        ("array_fill", "anyelement, integer[]", "array_fill"),
        ("array_fill", "anyelement, integer[], integer[]", "array_fill"),
        ("array_length", "anyarray, integer", "array_length"),
        ("array_positions", "anycompatiblearray, anycompatible", "array_positions"),
    ],
)
def test_an_overload_is_read_as_the_translator_reads_a_call_of_it(function, overload, feature):
    applied = GRID.read(check.call_of(function, overload), "duckdb")
    assert applied is not None and applied.feature.name == feature


def test_an_engine_the_check_has_no_dialect_of_is_refused():
    with pytest.raises(SystemExit, match="no dialect of"):
        check.checked(_run("array_ndims", "anyarray", spark=_PASS))


def test_every_leg_has_the_dialect_provisa_transpiles_its_sql_to():
    """Every engine a run can measure (function_legs.yaml) has a dialect, and an engine driven by
    its URL has the one its backend reads off that URL (EngineBackend.dialect, which is
    source_registry.engine_dialect of the scheme). A scheme with no sqlglot dialect is an engine the
    backend refuses by name: the check holds it to no grid column. Run 44918205557 lost thirteen
    legs to a KeyError of an engine the map lacked."""
    from provisa.core.source_registry import engine_dialect

    doc = yaml.safe_load((REPO / "scripts" / "conformance" / "function_legs.yaml").read_text())
    assert {leg["engine"] for leg in doc["legs"]} <= set(check.ENGINE_DIALECTS)
    for leg in doc["legs"]:
        url = leg.get("engine-url")
        if url and leg["engine"] != "pg":
            scheme = url.split("://", 1)[0].split("+", 1)[0]
            dialect = engine_dialect(scheme)
            if dialect is None:
                assert column_of(check.ENGINE_DIALECTS[leg["engine"]]) is None, leg["engine"]
            else:
                assert check.ENGINE_DIALECTS[leg["engine"]] == dialect, leg["engine"]


def test_an_engine_whose_dialect_is_no_grid_column_is_held_to_nothing():
    """The grid states nothing of a dialect that is no column of it (monetdb): no finding, no
    crash."""
    assert column_of(check.ENGINE_DIALECTS["monetdb"]) is None
    assert check.checked(_run("array_ndims", "anyarray", monetdb=_WRONG)) == []


def test_the_report_counts_by_engine_and_lists_each_failure():
    run = {
        "engines": ["duckdb", "trino"],
        "functions": [
            {
                "function": "array_ndims",
                "overload": "anyarray",
                "outcomes": {"duckdb": _PASS, "trino": _error("x | y")},
            },
        ],
    }
    doc = check.report(check.checked(run))
    assert doc["counts"] == {"duckdb": {"holds": 1}, "trino": {"fails": 1}}
    assert doc["failures"] == 1
    assert "array_ndims" not in doc["features_not_in_the_run"]
    assert "trim_array" in doc["features_not_in_the_run"]
    text = check.markdown(doc)
    assert "| duckdb | 1 | 0 | 0 |" in text and "| trino | 0 | 1 | 0 |" in text
    assert "`array_ndims(anyarray)` | trino | polyfilled | error |" in text
    assert "x \\| y" in text  # a cell's own bar is escaped


@pytest.mark.parametrize(("outcome", "status"), [(_PASS, 0), (_error("boom"), 1)])
def test_main_writes_both_files_and_exits_1_on_a_failure(tmp_path: Path, outcome, status: int):
    infile = tmp_path / "function_support.json"
    infile.write_text(json.dumps(_run("array_ndims", "anyarray", duckdb=outcome)))
    out_json, out_md = tmp_path / "check.json", tmp_path / "check.md"
    argv = [
        "polyfill_check.py",
        "--in",
        str(infile),
        "--out-json",
        str(out_json),
        "--out-md",
        str(out_md),
    ]
    assert check.main(argv) == status
    assert json.loads(out_json.read_text())["failures"] == status
    assert out_md.read_text().startswith("## Polyfill grid against the run")


def test_the_functions_workflow_reports_the_check_and_does_not_gate_on_it():
    workflow = yaml.safe_load((REPO / ".github/workflows/conformance-functions.yml").read_text())
    steps = workflow["jobs"]["merge"]["steps"]
    [step] = [s for s in steps if "polyfill_check.py" in s.get("run", "")]
    assert step["continue-on-error"] is True
    assert "GITHUB_STEP_SUMMARY" in step["run"]
    uploaded = next(s for s in steps if s.get("uses", "").startswith("actions/upload-artifact"))
    assert "polyfill-check" in uploaded["with"]["path"]


#: Run 45028182859's G12 overloads (polyfill triage 2026-11-22): each passed on its engine, and the
#: check's probe of it, one NULL of each listed type with the argument names and VARIADIC read as
#: part of the type, met a rewrite function's refusal the run's call never meets.
_RUN_PASSED = [
    ("json_build_object", 'VARIADIC "any"', "trino"),
    ("json_build_object", 'VARIADIC "any"', "clickhouse"),
    ("jsonb_build_object", 'VARIADIC "any"', "trino"),
    ("json_build_array", 'VARIADIC "any"', "clickhouse"),
    ("json_agg", "anyelement", "clickhouse"),
    ("to_jsonb", "anyelement", "clickhouse-server"),
    ("to_json", "anyelement", "clickhouse"),
    ("json_object_agg", 'key "any", value "any"', "clickhouse"),
    ("jsonb_extract_path", "from_json jsonb, VARIADIC path_elems text[]", "clickhouse"),
    ("jsonb_delete", "from_json jsonb, VARIADIC path_elems text[]", "clickhouse-server"),
]


@pytest.mark.parametrize(("function", "overload", "engine"), _RUN_PASSED)
def test_an_overload_the_run_passed_is_not_refused_by_the_probe(function, overload, engine):
    """Where the grid reads the call (on trino a text key keeps json_build_object as written, so
    there is nothing to read), it holds."""
    for finding in check.checked(_run(function, overload, **{engine: _PASS})):
        assert finding.feature != check.REWRITE_REFUSAL, finding.why
        assert finding.verdict == check.HOLDS, finding.why


@pytest.mark.parametrize(
    ("function", "overload", "call"),
    [
        (
            "json_build_object",
            'VARIADIC "any"',
            "JSON_BUILD_OBJECT(CAST('k' AS TEXT), CAST(NULL AS INT))",
        ),
        (
            "jsonb_extract_path",
            "from_json jsonb, VARIADIC path_elems text[]",
            "JSONB_EXTRACT_PATH(CAST(NULL AS JSONB), VARIADIC CAST(NULL AS TEXT[]))",
        ),
        (
            "json_object_agg",
            'key "any", value "any"',
            "JSON_OBJECT_AGG(CAST('k' AS TEXT), CAST(NULL AS INT))",
        ),
        ("json_agg", "anyelement", "JSON_AGG(CAST(NULL AS INT))"),
        ("age", "timestamp with time zone", "AGE(CAST(NULL AS TIMESTAMPTZ))"),
        ("now", "", "NOW()"),
    ],
)
def test_the_probe_is_the_runs_null_call(function, overload, call):
    """pgwire_function_probe's NULL call: its given arguments, else a NULL of each argument type
    (a pseudo-type its concrete type), the last passed VARIADIC where it is an array."""
    assert check.call_of(function, overload).sql("postgres") == call


# --- a stated limit credited per feature, and excusing only its own errors (amended 2026-11-22) --

_BOUND_TEXT = "A value past 1000 raises 0A000 (shout() of a value past 1000 has no form)."
_BOUNDED = Feature(
    "shout",
    "toy",
    "A text in capitals.",
    read=Side(
        rules=(
            Rule("shout($a::text)", {"clickhouse": "upper($a)"}),
            Rule("shout($a::integer)", {"clickhouse": "upper(toString($a))"}),
        ),
        limitations={"clickhouse": (Limitation(_BOUND_TEXT),)},
    ),
)
_EDGES = {"calls": 4, "agreed": 3, "raised": 1}


def _raised(message: str, sqlstate: str = "0A000") -> dict[str, Any]:
    return {
        "statement": "SELECT shout(1001)",
        "edge": "integer greatest",
        "error": _ERROR + message,
        "sqlstate": sqlstate,
    }


def _within(*raised: dict[str, Any]) -> dict[str, Any]:
    return {
        "outcome": "within_bound",
        "detail": raised[0],
        "edges": _EDGES,
        "edge_raised": list(raised),
        "bound_sqlstates": ["0A000"],
        "bound_raised": True,
    }


#: An overload whose edge calls all agreed, as the measure wrote it per overload.
_AGREED = {
    "outcome": "pass",
    "edges": {"calls": 4, "agreed": 4},
    "bound_sqlstates": ["0A000"],
    "bound_raised": False,
}


def _two(text: dict[str, Any], integer: dict[str, Any], grid: Grid) -> dict[str, str]:
    run = {
        "engines": ["clickhouse"],
        "functions": [
            {"function": "shout", "overload": "text", "outcomes": {"clickhouse": text}},
            {"function": "shout", "overload": "integer", "outcomes": {"clickhouse": integer}},
        ],
    }
    return {f.overload: f.verdict for f in check.checked(run, grid)}


def test_a_limit_one_overload_raised_is_held_for_every_overload_of_the_feature():
    held = _within(_raised("shout() of a value past 1000 has no form"))
    assert _two(_AGREED, held, Grid((_BOUNDED,))) == {"text": check.HOLDS, "integer": check.HOLDS}


def test_a_limit_no_overload_of_the_feature_raised_fails_each_overload_with_edges():
    verdicts = _two(_AGREED, dict(_AGREED), Grid((_BOUNDED,)))
    assert verdicts == {"text": check.FAILS, "integer": check.FAILS}
    [finding, _] = check.checked(
        {
            "engines": ["clickhouse"],
            "functions": [
                {"function": "shout", "overload": "text", "outcomes": {"clickhouse": _AGREED}},
                {"function": "shout", "overload": "integer", "outcomes": {"clickhouse": _AGREED}},
            ],
        },
        Grid((_BOUNDED,)),
    )
    assert "no edge call of any overload of shout raised it" in finding.why


def test_an_edge_raise_of_another_sqlstate_than_the_limit_states_fails():
    other = _within(_raised("shout() of a value past 1000 has no form", "22003"))
    assert _two(_AGREED, other, Grid((_BOUNDED,)))["integer"] == check.FAILS


def test_an_edge_raise_of_another_message_than_the_limit_states_fails():
    other = _within(_raised("shout() of a value past 1000 has no form"), _raised("boom: Code 386"))
    [_, finding] = check.checked(
        {
            "engines": ["clickhouse"],
            "functions": [
                {"function": "shout", "overload": "text", "outcomes": {"clickhouse": _AGREED}},
                {"function": "shout", "overload": "integer", "outcomes": {"clickhouse": other}},
            ],
        },
        Grid((_BOUNDED,)),
    )
    assert finding.verdict == check.FAILS
    assert "no limitation of its row states: 0A000 'boom: Code 386'" in finding.why


def test_a_limit_stating_its_sqlstate_alone_excuses_any_message_of_it():
    alone = Feature(
        "shout",
        "toy",
        "A text in capitals.",
        read=Side(
            rules=_BOUNDED.read.rules,
            limitations={"clickhouse": (Limitation("A value past 1000 raises 0A000."),)},
        ),
    )
    raised = _within(_raised("anything at all"))
    assert _two(raised, raised, Grid((alone,))) == {"text": check.HOLDS, "integer": check.HOLDS}


def test_an_edge_raise_another_rows_limitation_states_with_its_message_holds():
    """A literal past the engine's type raises the error its own row states (numeric_in of more
    than 38 digits), whatever call it is an argument of."""
    literal = Feature(
        "a numeric literal",
        "toy",
        "A numeric literal.",
        read=Side(
            limitations={
                "clickhouse": (
                    Limitation(
                        "Of more than 38 digits raises 0A000 (numeric_in() of more than 38 digits)."
                    ),
                )
            }
        ),
    )
    raised = _within(_raised("numeric_in() of more than 38 digits"))
    verdicts = _two(raised, raised, Grid((_BOUNDED, literal)))
    # The literal's raise is stated; shout's own limit is still raised by no overload.
    assert verdicts == {"text": check.FAILS, "integer": check.FAILS}
    both = _within(
        _raised("numeric_in() of more than 38 digits"),
        _raised("shout() of a value past 1000 has no form"),
    )
    assert _two(both, raised, Grid((_BOUNDED, literal))) == {
        "text": check.HOLDS,
        "integer": check.HOLDS,
    }


def test_a_wrong_answer_is_not_excused_by_a_limitation_that_states_an_error():
    assert _two(_WRONG, _WRONG, Grid((_BOUNDED,))) == {"text": check.FAILS, "integer": check.FAILS}


# --- statements of record (amended 2026-11-22) ---------------------------------------------------


def test_every_statement_of_record_names_a_bounded_limitation_of_the_grid():
    import function_edges as edges

    for record in edges.STATEMENTS_OF_RECORD:
        [feature] = [f for f in GRID.features if f.name == record.feature]
        limitations = feature.read.limitations.get(record.dialect, ())
        assert any(record.error in edges.bound_errors(lim) for lim in limitations), record
        assert any(edges.of_record(record.feature, record.dialect, lim) for lim in limitations)


def test_the_statements_of_record_are_the_limits_no_edge_call_reaches():
    import function_edges as edges

    named = {(r.feature, r.dialect) for r in edges.STATEMENTS_OF_RECORD}
    aggregates = {"avg", "variance", "var_samp", "var_pop", "stddev", "stddev_samp", "stddev_pop"}
    assert named == {(a, "clickhouse") for a in aggregates} | {
        ("bpchar", "clickhouse"),
        ("string_to_array", "clickhouse"),
    }


@pytest.mark.parametrize(
    ("function", "overload"),
    [("avg", "bigint"), ("stddev_pop", "integer"), ("string_to_array", "text, text")],
)
def test_a_statement_of_record_is_not_held_to_being_raised(function: str, overload: str):
    run = _run(function, overload, clickhouse=dict(_AGREED))
    assert _verdicts(run) == {"clickhouse": check.HOLDS}


def test_a_statement_of_record_is_marked_by_feature_not_by_its_wording():
    import function_edges as edges

    [avg] = [f for f in GRID.features if f.name == "avg"]
    [limitation] = avg.read.limitations["clickhouse"]
    assert edges.of_record("avg", "clickhouse", limitation)
    assert not edges.of_record("sum", "clickhouse", limitation)
    assert not edges.of_record("avg", "trino", limitation)
