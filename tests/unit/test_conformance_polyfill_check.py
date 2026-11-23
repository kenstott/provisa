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


# --- documented limits (REQ-2167, amended 2026-11-23) -----------------------------------------

_EDGY = Feature(
    "edgy",
    "toy",
    "An integer, doubled.",
    read=Side(
        rules=(Rule("edgy($a::integer)", {"duckdb": "2 * $a", "trino": "2 * $a"}),),
        limitations={
            "duckdb": (
                Limitation(
                    "Answers otherwise than PostgreSQL at an edge value: integer '-2147483648'."
                ),
                Limitation("Raises 22003 at an edge value it does not hold as PostgreSQL does:"),
            ),
            "trino": (Limitation("Raises 22003 at an edge value it does not hold as PostgreSQL"),),
        },
    ),
)


def _wrong_at(*literals: str) -> dict[str, Any]:
    wrong = [
        {
            "statement": f"SELECT edgy(CAST({literal} AS integer))",
            "edge": f"integer {kind}",
            "difference": "1, 2",
        }
        for kind, literal in literals
    ]
    return {
        "outcome": "wrong_answer",
        "detail": wrong[0],
        "wrong_at_edge": True,
        "edge_wrong": wrong,
    }


_LEAST = ("least", "'-2147483648'")
_GREATEST = ("greatest", "'2147483647'")


def test_a_refusal_a_limitation_states_is_a_documented_limit():
    run = _run(
        "trim_array",
        "anyarray, integer",
        clickhouse=_refused("trim_array does not read this value"),
    )
    [(key, why)] = check.documented_limits(run).items()
    assert key == ("trim_array", "anyarray, integer", "clickhouse")
    assert "limitation" in why


def test_a_refusal_as_its_refusal_row_says_is_a_documented_limit():
    run = _run(
        "generate_subscripts",
        "anyarray, integer, boolean",
        trino=_refused("generate_subscripts() has no form on trino"),
    )
    assert set(check.documented_limits(run)) == {
        ("generate_subscripts", "anyarray, integer, boolean", "trino")
    }


@pytest.mark.parametrize(
    "outcome",
    [
        _refused("array_shuffle() has no form on clickhouse"),
        _refused("trim_array does not read this value", sqlstate="22003"),
    ],
    ids=["unstated message", "stated message, another sqlstate"],
)
def test_a_refusal_no_limitation_states_stays_failed(outcome):
    function, overload = (
        ("array_shuffle", "anyarray")
        if "shuffle" in outcome["detail"]["error"]
        else ("trim_array", "anyarray, integer")
    )
    assert check.documented_limits(_run(function, overload, clickhouse=outcome)) == {}


def test_a_wrong_answer_at_a_stated_edge_value_is_a_documented_limit():
    run = _run("edgy", "integer", duckdb=_wrong_at(_LEAST))
    assert set(check.documented_limits(run, Grid((_EDGY,)))) == {("edgy", "integer", "duckdb")}


def test_a_limitation_excuses_only_its_own_values():
    """A limitation naming '-2147483648' does not excuse a wrong answer at '2147483647', nor one
    wrong call of several at an unstated value, nor a wrong answer on a base call."""
    grid = Grid((_EDGY,))
    other = _run("edgy", "integer", duckdb=_wrong_at(_GREATEST))
    both = _run("edgy", "integer", duckdb=_wrong_at(_LEAST, _GREATEST))
    base = _run("edgy", "integer", duckdb=_WRONG)
    assert check.documented_limits(other, grid) == {}
    assert check.documented_limits(both, grid) == {}
    assert check.documented_limits(base, grid) == {}
    assert _verdicts(other, grid) == {"duckdb": check.FAILS}


def test_a_limitation_stating_an_error_excuses_no_wrong_answer():
    run = _run("edgy", "integer", trino=_wrong_at(_LEAST))
    assert check.documented_limits(run, Grid((_EDGY,))) == {}
    assert _verdicts(run, Grid((_EDGY,))) == {"trino": check.FAILS}


def test_a_difference_of_no_particular_value_is_a_documented_limit():
    run = _run("shout", "text", duckdb=_WRONG, trino=_WRONG)
    assert set(check.documented_limits(run, Grid((_DIFFERS,)))) == {("shout", "text", "duckdb")}


def test_a_pass_and_an_error_are_no_documented_limit():
    run = _run("array_ndims", "anyarray", duckdb=_PASS, trino=_error("boom"))
    assert check.documented_limits(run) == {}
