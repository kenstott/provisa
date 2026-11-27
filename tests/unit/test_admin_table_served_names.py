# Copyright (c) 2026 Kenneth Stott
# Canary: a1c7e5d3-62f9-4b08-9e4a-3d8b5f20c71e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin serves a registered table's SQL names from the names table (REQ-2361, REQ-194: the UI
reads saved objects' names from it and computes none), and its name suggestions from the naming
authority."""

# Requirements: REQ-2361, REQ-194

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.api.admin.schema_query import Query
from provisa.api.admin.types import RegisteredTableType
from provisa.compiler import naming


@pytest.fixture(autouse=True)
def _convention():
    naming.configure(gql="apollo_graphql", sql="snake")
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


def _table(table_id: int) -> RegisteredTableType:
    return RegisteredTableType(
        id=table_id,
        source_id="pg",
        domain_id="sales-eu",
        schema_name="public",
        table_name="Orders",
        write_ops=[],
        alias=None,
        description=None,
        cache_ttl=None,
        replicate=None,
        region=None,
        withdrawn=False,
        source_withdrawn=False,
        row_materialize=False,
        load_protected=None,
        off_peak_window=None,
        off_peak_tz=None,
        gql_naming_convention=None,
        watermark_column=None,
        columns=[],
        registered_at="2026-11-27T00:00:00Z",
    )


class _Names:
    """A names table naming table 7 only, with a SQL name no convention gives Orders."""

    def sql_address(self, table_id: int) -> tuple[str, str]:
        if table_id != 7:
            raise KeyError(table_id)
        return ("<schema>", "<served>")


def test_sql_names_are_the_names_tables():
    with patch("provisa.api.app.state", SimpleNamespace(names_table=_Names())):
        t = _table(7)
        assert (t.sql_schema(), t.sql_name()) == ("<schema>", "<served>")


def test_a_table_the_build_does_not_name_has_no_sql_name():
    with patch("provisa.api.app.state", SimpleNamespace(names_table=_Names())):
        t = _table(8)
        assert (t.sql_schema(), t.sql_name()) == (None, None)
    with patch("provisa.api.app.state", SimpleNamespace(names_table=None)):
        assert _table(7).sql_name() is None


@pytest.fixture
def granted():
    """The caller holds every capability the suggestion resolvers ask for; the gate itself is
    checked by test_admin_routes_gated.py."""
    with patch("provisa.api.admin.schema_query.require_capability"):
        yield object()


def test_reverse_alias_suggestion_is_the_authoritys_under_the_gql_convention(granted):
    q = Query()
    s = q.suggest_reverse_rel_alias(granted, graphql_alias=None, cypher_alias="BY_CUSTOMER")
    assert (s.graphql_alias, s.cypher_alias) == ("byCustomerBy", "BY_CUSTOMER_BY")
    naming.configure(gql="hasura_graphql", sql="snake")
    s = q.suggest_reverse_rel_alias(granted, graphql_alias=None, cypher_alias="BY_CUSTOMER")
    assert (s.graphql_alias, s.cypher_alias) == ("by_customer_by", "BY_CUSTOMER_BY")
    s = q.suggest_reverse_rel_alias(granted, graphql_alias="ordersOf", cypher_alias=None)
    assert (s.graphql_alias, s.cypher_alias) == ("orders", "")


def test_suggestions_route_through_the_authority(monkeypatch, granted):
    q = Query()
    monkeypatch.setattr(naming, "suggest_column_alias", lambda c: None if c == "id" else f"<{c}>")
    monkeypatch.setattr(naming, "suggest_metric_name", lambda f, c, a: f"<{f}.{c}.{a}>")
    monkeypatch.setattr(naming, "suggest_junction_name", lambda left, right: f"<{left}+{right}>")
    monkeypatch.setattr(
        naming, "suggest_reverse_rel_alias", lambda g, c, conv: (f"<{g}:{conv}>", f"<{c}>")
    )
    assert q.suggest_column_aliases(granted, columns=["orderId", "id"]) == ["<orderId>", None]
    assert (
        q.suggest_metric_name(granted, fact="orders", column="amt", agg="sum") == "<orders.amt.sum>"
    )
    assert (
        q.suggest_junction_name(granted, left_table="pets", right_table="owners") == "<pets+owners>"
    )
    s = q.suggest_reverse_rel_alias(granted, graphql_alias="a", cypher_alias="B")
    assert (s.graphql_alias, s.cypher_alias) == ("<a:apollo_graphql>", "<B>")
