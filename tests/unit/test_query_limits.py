# Copyright (c) 2026 Kenneth Stott
# Canary: 6ece1883-d01e-4195-b01a-a577234c1e34
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The query complexity guard (REQ-1174): one score for a statement, measured on the semantic
statement so it holds on every surface, against the role's limit under the org's ceiling."""

from types import SimpleNamespace

import pytest
import sqlglot

from provisa.compiler import complexity
from provisa.compiler.complexity import (
    REMOTE_RELATION_POINTS,
    ComplexityLimitExceeded,
    guard_complexity,
    limit_for,
    measure,
    role_max_query_complexity,
)
from provisa.compiler.limits import role_max_query_time_ms
from provisa.compiler.stage2 import GovernanceContext
from provisa.core.models import RoleRateLimit

ORDERS, CUSTOMERS, ISSUES = 1, 2, 3


def _gov() -> GovernanceContext:
    """orders (4 columns, the role sees 3), customers (2 columns), issues (remote, 5 columns)."""
    return GovernanceContext(
        table_map={
            "orders": ORDERS,
            "sales.orders": ORDERS,
            "customers": CUSTOMERS,
            "gh__repository_issues": ISSUES,
        },
        all_columns={
            ORDERS: [("id", "int"), ("total", "numeric"), ("region", "text"), ("ssn", "text")],
            CUSTOMERS: [("id", "int"), ("name", "text")],
            ISSUES: [(c, "text") for c in ("id", "title", "state", "body", "url")],
        },
        visible_columns={
            ORDERS: frozenset({"id", "total", "region"}),
            CUSTOMERS: None,
            ISSUES: None,
        },
    )


def _ctx() -> SimpleNamespace:
    metas = {
        "orders": SimpleNamespace(table_id=ORDERS, source_type="postgresql"),
        "customers": SimpleNamespace(table_id=CUSTOMERS, source_type="postgresql"),
        "issues": SimpleNamespace(table_id=ISSUES, source_type="graphql_remote"),
    }
    return SimpleNamespace(tables=metas)


def _measure(sql: str, remote: frozenset[int] = frozenset({ISSUES})):
    return measure(sqlglot.parse_one(sql, read="postgres"), _gov(), remote)


@pytest.fixture
def org_limit(monkeypatch):
    """Set the org's ceiling (``limits.max_query_complexity``); unset by default."""
    held = {"value": None}
    monkeypatch.setattr("provisa.core.limits.max_query_complexity", lambda: held["value"])
    return held


# --- the measure ---


def test_a_single_table_read_is_its_relation_and_its_columns():
    c = _measure("SELECT id, total FROM orders")
    assert (c.relations, c.remote_relations, c.joins, c.columns, c.blocks) == (1, 0, 0, 2, 0)
    assert c.score == 3


def test_a_join_adds_the_relation_and_the_join():
    c = _measure("SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.id")
    assert (c.relations, c.joins, c.columns) == (2, 1, 2)
    assert c.score == 5


def test_a_star_counts_as_the_columns_the_role_may_see():
    assert _measure("SELECT * FROM orders").columns == 3  # 4 columns, 3 visible to the role
    assert _measure("SELECT * FROM customers").columns == 2
    assert _measure("SELECT * FROM orders o JOIN customers c ON c.id = o.id").columns == 5
    assert _measure("SELECT c.* FROM orders o JOIN customers c ON c.id = o.id").columns == 2


def test_a_nested_query_block_is_counted_and_its_contents_are_too():
    c = _measure("SELECT id FROM orders WHERE id IN (SELECT id FROM customers)")
    assert (c.relations, c.columns, c.blocks) == (2, 2, 1)
    union = _measure("SELECT id FROM orders UNION ALL SELECT id FROM customers")
    assert union.blocks == 1 and union.relations == 2


def test_a_reference_to_a_common_table_expression_is_not_a_relation_read_again():
    c = _measure(
        "WITH big AS (SELECT id, total FROM orders) "
        "SELECT a.id FROM big a JOIN big b ON a.id = b.id"
    )
    assert c.relations == 1  # orders, once; the two references to big read nothing new
    assert (c.joins, c.blocks, c.columns) == (1, 1, 3)


def test_a_relation_fetched_from_a_remote_api_counts_for_more():
    local = _measure("SELECT id FROM orders")
    remote = _measure("SELECT id FROM gh__repository_issues")
    assert remote.remote_relations == 1
    assert remote.score - local.score == REMOTE_RELATION_POINTS - 1


def test_a_schema_qualified_name_is_the_same_relation():
    assert _measure("SELECT id FROM sales.orders").score == _measure("SELECT id FROM orders").score


# --- the limit that applies ---


def test_a_role_with_no_limit_under_an_org_with_none_is_not_limited(org_limit):
    assert limit_for({"rate_limit": None}) is None
    assert limit_for({}) is None


def test_the_roles_limit_applies_when_the_org_sets_none(org_limit):
    assert limit_for({"rate_limit": {"max_query_complexity": 40}}) == (40, "role")


def test_the_org_ceiling_applies_to_a_role_that_sets_none(org_limit):
    org_limit["value"] = 100
    assert limit_for({"rate_limit": {"requests_per_second": 5}}) == (100, "org")


def test_a_role_may_tighten_the_org_ceiling_and_not_loosen_it(org_limit):
    org_limit["value"] = 100
    assert limit_for({"rate_limit": {"max_query_complexity": 40}}) == (40, "role")
    assert limit_for({"rate_limit": {"max_query_complexity": 500}}) == (100, "org")


def test_the_limit_is_read_from_the_model_as_from_the_loaded_dict():
    role = SimpleNamespace(rate_limit=RoleRateLimit(max_query_complexity=7, max_query_time_ms=900))
    assert role_max_query_complexity(role) == 7
    assert role_max_query_time_ms(role) == 900
    assert role_max_query_time_ms({"rate_limit": {"max_query_time_ms": 3000}}) == 3000
    assert role_max_query_time_ms({"rate_limit": {"requests_per_second": 10}}) is None
    assert role_max_query_time_ms({}) is None


# --- the guard ---


def _guard(sql: str, role: dict) -> None:
    guard_complexity(sqlglot.parse_one(sql, read="postgres"), _gov(), _ctx(), role)


def test_a_statement_within_the_limit_passes(org_limit):
    _guard("SELECT id, total FROM orders", {"rate_limit": {"max_query_complexity": 3}})


def test_a_statement_over_the_limit_is_refused_with_what_it_asked_for(org_limit):
    with pytest.raises(ComplexityLimitExceeded) as refused:
        _guard(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.id",
            {"rate_limit": {"max_query_complexity": 4}},
        )
    assert refused.value.limit == 4 and refused.value.limit_of == "role"
    assert refused.value.complexity.score == 5
    message = str(refused.value)
    assert "query complexity 5 exceeds the role limit of 4" in message
    assert "2 relations (0 remote), 1 joins, 2 columns, 0 nested queries" in message
    assert isinstance(refused.value, PermissionError)  # a refusal, on every surface


def test_the_guard_knows_which_tables_are_remote_from_the_roles_context(org_limit):
    role = {"rate_limit": {"max_query_complexity": 5}}
    _guard("SELECT id FROM orders", role)
    with pytest.raises(ComplexityLimitExceeded, match="1 relations \\(1 remote\\)"):
        _guard("SELECT id FROM gh__repository_issues", role)


def test_the_org_ceiling_refuses_a_role_that_set_no_limit(org_limit):
    org_limit["value"] = 2
    with pytest.raises(ComplexityLimitExceeded, match="exceeds the org limit of 2"):
        _guard("SELECT id, total FROM orders", {})


def test_a_role_under_no_limit_is_not_measured(org_limit, monkeypatch):
    def _never(*args, **kwargs):
        raise AssertionError("an unlimited role's statement was measured")

    monkeypatch.setattr(complexity, "measure", _never)
    _guard("SELECT * FROM orders o JOIN customers c ON c.id = o.id", {})


# --- at the pipeline's governing stages ---


async def test_a_refusal_at_the_pipeline_is_recorded_as_a_denial(org_limit, monkeypatch):
    from provisa.pgwire import _pipeline

    denied: list[tuple] = []

    async def _write_denial(sql, role_id, tree, gov_ctx, state):
        denied.append((sql, role_id))

    monkeypatch.setattr("provisa.audit.pipeline.write_denial", _write_denial)
    sql = "SELECT id, total FROM orders"
    state = SimpleNamespace(roles={"analyst": {"rate_limit": {"max_query_complexity": 2}}})
    with pytest.raises(ComplexityLimitExceeded):
        await _pipeline._guard_complexity(
            sql, "analyst", sqlglot.parse_one(sql, read="postgres"), _gov(), _ctx(), state
        )
    assert denied == [(sql, "analyst")]

    state.roles["analyst"]["rate_limit"]["max_query_complexity"] = 3
    await _pipeline._guard_complexity(
        sql, "analyst", sqlglot.parse_one(sql, read="postgres"), _gov(), _ctx(), state
    )
    assert len(denied) == 1


def test_both_governing_stages_of_the_pipeline_call_the_guard():
    import inspect

    from provisa.pgwire import _pipeline

    for stage in (_pipeline.govern_statement, _pipeline._govern_compiled):
        source = inspect.getsource(stage)
        assert "_guard_complexity(" in source, stage.__name__
        # Before the statement is governed.
        governs = source.index("_off_loop(apply_governance")
        assert source.index("_guard_complexity(") < governs, stage.__name__


def test_the_graphql_endpoint_makes_the_same_check_before_it_governs():
    import inspect

    from provisa.api.data import endpoint

    source = inspect.getsource(endpoint._prepare_compiled)
    assert source.index("guard_complexity(") < source.index("compiled.sql = apply_governance(")
    assert "status_code=413" in source
