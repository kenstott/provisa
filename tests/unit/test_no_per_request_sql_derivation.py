# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9f6c20-7e4a-4d15-9a82-1c5d0e7f4b68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A repeated request does not re-derive its SQL (REQ-1877).

Measured per uncached request before this: GraphQL's DIRECT read parsed and regenerated its
statement three times (semantic → physical naming, catalog strip, dialect transpile) although its
governed plan was already kept; pgwire parsed twice (catalog-intercept classification and the
registered-function probe). All five are functions of the statement text and of objects a kept
plan is already anchored to, so they are derived once and kept by the same mechanism.
"""

# Requirements: REQ-1877

from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot.generator
import sqlglot.parser

from provisa.pgwire import governed_plan


@pytest.fixture
def sqlglot_calls(monkeypatch) -> dict[str, int]:
    calls = {"parse": 0, "generate": 0}
    real_parse, real_generate = sqlglot.parser.Parser.parse, sqlglot.generator.Generator.generate

    def parse(self, *a, **k):
        calls["parse"] += 1
        return real_parse(self, *a, **k)

    def generate(self, *a, **k):
        calls["generate"] += 1
        return real_generate(self, *a, **k)

    monkeypatch.setattr(sqlglot.parser.Parser, "parse", parse)
    monkeypatch.setattr(sqlglot.generator.Generator, "generate", generate)
    return calls


@pytest.fixture(autouse=True)
def _no_rebuild(monkeypatch):
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)


def test_the_graphql_executor_derives_no_sql_of_its_own():
    """GraphQL's DIRECT read is the compiled pipeline's, which keeps its derived source SQL with
    the governed plan (tests/unit/test_governed_plan_stages.py,
    test_a_repeated_direct_statement_on_the_compiled_stage_parses_once)."""
    import inspect

    from provisa.api.data import endpoint

    source = inspect.getsource(endpoint)
    assert "rewrite_semantic_to_physical" not in source
    assert "transpile(" not in source


def test_classifying_a_statement_for_the_pgwire_catalog_parses_its_text_once(sqlglot_calls):
    from provisa.pgwire.catalog import classify

    sql = "SELECT order_id FROM perf_bench.orders WHERE order_id = 91234567 LIMIT 1"
    assert classify(sql) == "PASS_THROUGH"
    after_first = dict(sqlglot_calls)
    for _ in range(5):
        assert classify(sql) == "PASS_THROUGH"
    assert sqlglot_calls == after_first
    assert classify("SELECT oid, nspname FROM pg_namespace") == "INTERCEPT"


def test_a_statement_naming_no_registered_command_is_not_parsed_to_look_for_one(sqlglot_calls):
    from provisa.pgwire.function_call import detect_sql_function_call

    state = SimpleNamespace(
        roles={"r": {"id": "r", "capabilities": [], "domain_access": ["*"]}},
        tracked_functions={"send_invoice": {"name": "send_invoice", "domain_id": "sales"}},
        tracked_webhooks={},
    )
    assert (
        detect_sql_function_call("SELECT order_id FROM perf_bench.orders LIMIT 1", state, "r")
        is None
    )
    assert sqlglot_calls["parse"] == 0
    # A statement that does name one is still recognised (and parsed to be sure).
    assert detect_sql_function_call("SELECT send_invoice(7)", state, "r") == ("send_invoice", [7])
    assert sqlglot_calls["parse"] == 1
    # ...and one that only mentions the name inside a composed statement is still left alone.
    assert (
        detect_sql_function_call("SELECT a FROM t JOIN send_invoice(7) s ON true", state, "r")
        is None
    )
