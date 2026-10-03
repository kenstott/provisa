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

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.pgwire import governed_plan
from provisa.transpiler.router import Route, RouteDecision


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


def _ctx() -> CompilationContext:
    orders = TableMeta(
        table_id=7,
        field_name="orders",
        type_name="Orders",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
    )
    return CompilationContext(tables={"orders": orders})


def _state(ctx) -> SimpleNamespace:
    return SimpleNamespace(
        compiled_query_cache=CompiledQueryCache(),
        schema_boot_id="boot",
        schema_version=7,
        contexts={"analyst": ctx},
        rls_contexts={},
        roles={"analyst": {"id": "analyst"}},
        masking_rules={},
        tables=[],
    )


_SQL = 'SELECT "order_id" FROM "public"."orders" AS "orders" LIMIT $1'
_DIRECT = RouteDecision(route=Route.DIRECT, source_id="sales-pg", dialect="postgres", reason="t")


def _direct_sql(state, ctx, sql=_SQL, decision=_DIRECT, probe_limit=None, role="analyst"):
    from provisa.api.data.endpoint import _direct_exec_sql

    return _direct_exec_sql(state, role, sql, ctx, decision, probe_limit)


def test_a_repeated_graphql_direct_read_derives_its_source_sql_once(sqlglot_calls):
    ctx = _ctx()
    state = _state(ctx)
    first = _direct_sql(state, ctx)
    derived = dict(sqlglot_calls)
    assert derived["parse"] >= 1 and derived["generate"] >= 1
    for _ in range(5):
        assert _direct_sql(state, ctx) == first
    assert sqlglot_calls == derived  # 0 parses, 0 generations on the repeats


@pytest.mark.parametrize(
    "change",
    ["sql", "dialect", "source", "probe_limit", "role", "generation", "context", "role_set"],
)
def test_anything_that_changes_the_sql_derives_it_again(sqlglot_calls, change, monkeypatch):
    ctx = _ctx()
    state = _state(ctx)
    state.contexts["steward"] = ctx
    state.roles["steward"] = {"id": "steward"}
    _direct_sql(state, ctx)
    before = dict(sqlglot_calls)
    kwargs: dict = {}
    if change == "sql":
        kwargs["sql"] = _SQL.replace('"order_id"', '"order_id", "order_id" AS again')
    elif change == "dialect":
        kwargs["decision"] = RouteDecision(Route.DIRECT, "sales-pg", "mysql", "t")
    elif change == "source":
        kwargs["decision"] = RouteDecision(Route.DIRECT, "other-pg", "postgres", "t")
    elif change == "probe_limit":
        kwargs["probe_limit"] = 11
    elif change == "role":
        kwargs["role"] = "steward"
    elif change == "generation":
        state.schema_version = 8
    elif change == "context":
        ctx = _ctx()  # a rebuild publishes a new compilation context for the role
        state.contexts["analyst"] = ctx
    elif change == "role_set":
        monkeypatch.setattr(governed_plan, "acting_role_set", lambda: ("analyst", "steward"))
    _direct_sql(state, ctx, **kwargs)
    assert sqlglot_calls["parse"] > before["parse"]


def test_the_graphql_executor_uses_the_kept_form():
    import inspect

    from provisa.api.data import endpoint

    source = inspect.getsource(endpoint._execute_one_field)
    assert "_direct_exec_sql(state, role_id, compiled.sql, ctx, decision, probe_limit)" in source
    assert "rewrite_semantic_to_physical(compiled.sql, ctx)" not in source


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
        roles={"r": {"id": "r", "domain_access": ["*"]}},
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
