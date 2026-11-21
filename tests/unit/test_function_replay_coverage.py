# Copyright (c) 2026 Kenneth Stott
# Canary: e5a19c37-2d84-4f6b-b0e1-8c3d7a2f95b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function replays on Trino and ClickHouse leave out what conformance-functions measures
(REQ-2167, REQ-2171; REQ-1989 cadence, maintainer 2026-11-20): the classifier's rule
(scripts/conformance/replay_coverage.py) and the data file the replays read through
tests/integration/replay_coverage.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from tests.integration import replay_coverage

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def classifier():
    path = REPO / "scripts" / "conformance" / "replay_coverage.py"
    spec = importlib.util.spec_from_file_location("replay_coverage_script", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["replay_coverage_script"] = module
    spec.loader.exec_module(module)
    return module


def _support(*entries: tuple[str, str, bool, dict[str, str]]) -> dict:
    return {
        "functions": [
            {
                "function": function,
                "overload": overload,
                "answers_checked": checked,
                "type_and_form_checked": False,
                "side_effect_checked": False,
                "outcomes": {engine: {"outcome": said} for engine, said in outcomes.items()},
            }
            for function, overload, checked, outcomes in entries
        ]
    }


def _covered(classifier, sql: str, engine: str, support: dict) -> bool:
    checked, called = classifier.measured(support)
    return classifier.covered(sql, engine, checked, called)


def test_a_constant_call_of_a_compared_overload_is_covered_on_that_engine_only(classifier):
    support = _support(("md5", "text", True, {"trino": "pass", "clickhouse": "not_callable"}))
    sql = "SELECT pg_catalog.\"md5\"('abc'::text) AS v"
    assert _covered(classifier, sql, "trino", support)
    assert not _covered(classifier, sql, "clickhouse", support)


@pytest.mark.parametrize("said", ["pass", "refused", "error", "wrong_answer", "within_bound"])
def test_every_measured_outcome_covers(classifier, said):
    support = _support(("md5", "text", True, {"trino": said}))
    assert _covered(classifier, "SELECT pg_catalog.\"md5\"('abc'::text) AS v", "trino", support)


def test_an_overload_whose_values_are_not_compared_covers_only_its_null_call(classifier):
    support = _support(("md5", "text", False, {"trino": "pass"}))
    assert _covered(classifier, 'SELECT pg_catalog."md5"(NULL::text) AS v', "trino", support)
    assert not _covered(classifier, "SELECT pg_catalog.\"md5\"('abc'::text) AS v", "trino", support)


def test_an_untyped_argument_needs_every_overload_it_could_be_measured(classifier):
    text_only = _support(("md5", "text", True, {"trino": "pass"}))
    both = _support(
        ("md5", "text", True, {"trino": "pass"}), ("md5", "bytea", True, {"trino": "pass"})
    )
    assert not _covered(classifier, "SELECT md5('abc')", "trino", text_only)
    assert _covered(classifier, "SELECT md5('abc')", "trino", both)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT md5(t.v) FROM t",  # over a column
        "SELECT md5('a'::text), md5('b'::text)",  # two calls
        "SELECT md5('a'::text) WHERE true",
        "SELECT pg_catalog.\"md5\"('a'::source.mood) AS v",  # a source's own type
        "SELECT not_a_function('a'::text)",
        "SELECT (",  # no statement the classifier reads
    ],
)
def test_what_is_not_one_constant_call_stays_in_its_replay(classifier, sql):
    support = _support(("md5", "text", True, {"trino": "pass"}))
    assert not _covered(classifier, sql, "trino", support)


def test_the_data_file_names_only_statements_of_its_family(classifier):
    corpus = classifier.corpus()
    recorded = json.loads((REPO / "tests/integration/function_replay_covered.json").read_text())
    assert set(recorded) == set(corpus)
    for family, statements in recorded.items():
        for engine in replay_coverage.ENGINES:
            stale = {s for s, e in statements.items() if engine in e} - set(corpus[family][engine])
            assert stale == set(), f"{family} on {engine}: {sorted(stale)[:3]}"


def test_the_data_file_is_as_the_classifier_writes_it(classifier):
    path = REPO / "tests/integration/function_replay_covered.json"
    recorded = json.loads(path.read_text())
    assert path.read_text() == classifier.written(recorded)


def test_remaining_keeps_order_and_drops_only_what_is_covered():
    family = "text_search"
    measured = replay_coverage.covered(family, "trino")
    one = next(iter(measured))
    statements = ["SELECT 'not recorded'", one, "SELECT 'also not'"]
    assert replay_coverage.remaining(family, "trino", statements) == [
        "SELECT 'not recorded'",
        "SELECT 'also not'",
    ]


def test_an_engine_without_coverage_is_refused_by_name():
    with pytest.raises(ValueError, match="duckdb"):
        replay_coverage.covered("text_search", "duckdb")
