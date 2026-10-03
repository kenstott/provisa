# Copyright (c) 2026 Kenneth Stott
# Canary: 384e9b76-5d26-49b9-8fd6-2b5eb0246f6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two sources that both hold ``public.orders`` are two nodes of the event graph, and a view's
edges come from its inputs resolved against the model (REQ-939, REQ-1674).

A node was named ``schema.table``; two sources with the same schema and table were one node — one
freshness record, one land lock, one poll job, one fan-out — and a view's edges were the
spellings its SQL used, matched against node names. These pin the node identity, the edges and
the refusal of a view whose input does not resolve to exactly one table."""

# Requirements: REQ-939, REQ-1674, REQ-961

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.events import supervisor
from provisa.events.boot import specs_from_config
from provisa.events.nodes import expected_event_nodes, lineage_graph, source_node
from provisa.federation.engine import build_duckdb_engine
from provisa.mv.readable_inputs import ViewInputNotReadable, require_readable_inputs
from tests.helpers import DsnEngine

pytestmark = pytest.mark.unit


def _row(table_id: int, source_id: str, table: str = "orders", **more) -> dict:
    row = {
        "id": table_id,
        "source_id": source_id,
        "domain_id": f"{source_id}_dom",
        "schema_name": "public",
        "table_name": table,
        "alias": None,
        "view_sql": None,
    }
    row.update(more)
    return row


def _view(view_id: str, sql: str | None, *, target: str, source_tables=(), **more):
    return SimpleNamespace(
        id=view_id,
        sql=sql,
        source_tables=list(source_tables),
        target_catalog="mat_store",
        target_schema="org_x_mv_cache",
        target_table=target,
        enabled=True,
        **more,
    )


class _Registry:
    def __init__(self, views) -> None:
        self._views = {v.id: v for v in views}

    def get(self, view_id):
        return self._views.get(view_id)

    def get_enabled(self):
        return list(self._views.values())


def _model(views=(), rows=None):
    """Two postgres sources, both holding ``public.orders``, with their engine catalogs."""
    return SimpleNamespace(
        tables=rows
        if rows is not None
        else [_row(1, "sales"), _row(2, "crm"), _row(3, "crm", "accounts")],
        source_catalogs={"sales": "sales_cat", "crm": "crm_cat"},
        contexts={},
        mv_registry=_Registry(views),
        api_endpoints={},
        graphql_remote_sources={},
        grpc_remote_sources={},
    )


# -- node identity ---------------------------------------------------------------------------------


def _src(sid: str):
    return SimpleNamespace(id=sid, type=SimpleNamespace(value="openapi"), change_signal="ttl")


def _tbl(sid: str):
    col = SimpleNamespace(
        name="id", data_type="bigint", is_primary_key=True, native_filter_type=None
    )
    return SimpleNamespace(
        source_id=sid,
        schema_name="public",
        table_name="orders",
        change_signal=None,
        watermark_column=None,
        live=None,
        columns=[col],
        cache_ttl=300,
    )


async def _fetch(_pending):
    return []


def test_two_sources_holding_the_same_schema_table_are_two_nodes():
    """Each node has its own freshness, land lock, poll job and fan-out."""
    specs = specs_from_config(
        sources=[_src("sales_api"), _src("crm_api")],
        tables=[_tbl("sales_api"), _tbl("crm_api")],
        mvs=[],
        engine=build_duckdb_engine(),
        engine_runtime=DsnEngine("sqlite://"),
        source_fetch=lambda s, t: _fetch,
        mv_columns=lambda m: None,
        mv_run_query=lambda m: None,
    )
    assert sorted(s.node for s in specs) == [
        "crm_api/public.orders",
        "sales_api/public.orders",
    ]
    assert source_node("sales_api", "public", "orders") != source_node(
        "crm_api", "public", "orders"
    )


# -- edges -----------------------------------------------------------------------------------------


def test_a_view_over_one_of_them_listens_to_that_source_only():
    """The view's SQL names its input catalog-physically (as the schema build lowers it); an event
    on the other source's ``public.orders`` does not fan out to it."""
    daily = _view("daily", "SELECT count(*) FROM sales_cat.public.orders", target="daily")
    graph = lineage_graph([daily], _model([daily]))
    assert graph == {"org_x_mv_cache.daily": {"sales/public.orders"}}
    dependents_of = supervisor.dependents_of(graph)
    assert dependents_of("sales/public.orders") == ["org_x_mv_cache.daily"]
    assert dependents_of("crm/public.orders") == []


def test_a_semantic_reference_resolves_through_its_domain():
    view = _view("v", "SELECT * FROM crm_dom.orders", target="v")
    assert lineage_graph([view], _model([view])) == {"org_x_mv_cache.v": {"crm/public.orders"}}


def test_a_view_over_another_view_listens_to_that_views_node():
    base = _view("base", "SELECT * FROM sales_cat.public.orders", target="base")
    top = _view("top", "SELECT * FROM org_x_mv_cache.base", target="top")
    graph = lineage_graph([base, top], _model([base, top]))
    assert graph["org_x_mv_cache.top"] == {"org_x_mv_cache.base"}
    assert supervisor.dependents_of(graph)("org_x_mv_cache.base") == ["org_x_mv_cache.top"]


def test_a_view_held_only_as_sql_is_read_through():
    """A view registered as a table and not materialized: the reading view listens to the
    inputs it reads."""
    rows = [
        _row(1, "sales"),
        _row(2, "crm"),
        _row(9, "__derived__", "open_orders", view_sql="SELECT * FROM sales_cat.public.orders"),
    ]
    top = _view("top", "SELECT * FROM open_orders", target="top")
    assert lineage_graph([top], _model([top], rows)) == {
        "org_x_mv_cache.top": {"sales/public.orders"}
    }


def test_a_join_pattern_view_resolves_the_tables_it_joins():
    view = _view("jp", None, target="jp", source_tables=["accounts"])
    assert lineage_graph([view], _model([view])) == {"org_x_mv_cache.jp": {"crm/public.accounts"}}


def test_a_periodic_views_expected_events_are_resolved_like_its_edges():
    view = _view(
        "p",
        "SELECT * FROM sales_cat.public.orders JOIN crm_cat.public.accounts USING (id)",
        target="p",
    )
    model = _model([view])
    graph = lineage_graph([view], model)
    assert expected_event_nodes(view, model, graph) == [
        "crm/public.accounts",
        "sales/public.orders",
    ]
    view.expected_events = ["sales_cat.public.orders"]
    assert expected_event_nodes(view, model, graph) == ["sales/public.orders"]


def test_at_wiring_an_input_that_does_not_resolve_is_a_defect_and_raises():
    view = _view("daily", "SELECT * FROM public.orders", target="daily")
    with pytest.raises(ValueError, match=r"'daily' reads 'public.orders'.*more than one"):
        lineage_graph([view], _model([view]))


# -- refused when the view is declared -------------------------------------------------------------


async def _refusal(view, model) -> str:
    with pytest.raises(ViewInputNotReadable) as refused:
        await require_readable_inputs(view, model)
    return str(refused.value)


async def test_a_view_naming_a_table_two_sources_hold_is_refused_when_declared(monkeypatch):
    async def _none(_state):
        return {}

    monkeypatch.setattr("provisa.federation.query_residency.row_materialized_tables_by_name", _none)
    monkeypatch.setattr("provisa.mv.readable_inputs._request_cache_schemas", _none)
    view = _view("daily", "SELECT * FROM public.orders", target="daily")
    said = await _refusal(view, _model([view]))
    assert "'public.orders' is a reference more than one table or view answers to" in said
    assert "table 1" in said and "table 2" in said


async def test_a_view_naming_no_table_is_refused_when_declared(monkeypatch):
    async def _none(_state):
        return {}

    monkeypatch.setattr("provisa.federation.query_residency.row_materialized_tables_by_name", _none)
    monkeypatch.setattr("provisa.mv.readable_inputs._request_cache_schemas", _none)
    view = _view("daily", "SELECT * FROM public.invoices", target="daily")
    said = await _refusal(view, _model([view]))
    assert "'public.invoices' is a reference that names no table or view" in said


async def test_a_view_whose_inputs_resolve_is_accepted(monkeypatch):
    async def _none(_state):
        return {}

    monkeypatch.setattr("provisa.federation.query_residency.row_materialized_tables_by_name", _none)
    monkeypatch.setattr("provisa.mv.readable_inputs._request_cache_schemas", _none)
    view = _view("daily", "SELECT * FROM sales_cat.public.orders", target="daily")
    await require_readable_inputs(view, _model([view]))


# -- kept true when a table joins the model --------------------------------------------------------


def _new_table(source_id: str, table: str, schema: str = "public"):
    return SimpleNamespace(
        source_id=source_id,
        schema_name=schema,
        table_name=table,
        domain_id=f"{source_id}_dom",
        alias=None,
    )


def test_registering_a_table_that_would_make_a_views_input_ambiguous_is_refused():
    """A join-pattern view names ``accounts`` bare, which today is crm's alone. Registering
    sales' ``accounts`` would give that name a second meaning, and the view would meet an
    ambiguous input at its next wiring: the registration is refused, naming the view."""
    from provisa.mv.readable_inputs import (
        TableMakesViewAmbiguous,
        require_no_view_made_ambiguous,
    )

    view = _view("jp", None, target="jp", source_tables=["accounts"])
    model = _model([view])
    with pytest.raises(TableMakesViewAmbiguous, match=r"view 'jp' reads 'accounts'"):
        require_no_view_made_ambiguous(model, _new_table("sales", "accounts"))
    # crm's own accounts registered again, and a table no view names, are not new meanings.
    require_no_view_made_ambiguous(model, _new_table("crm", "accounts"))
    require_no_view_made_ambiguous(model, _new_table("sales", "invoices"))


def test_a_catalog_qualified_view_input_keeps_one_meaning():
    """A view over ``sales_cat.public.orders`` is not affected by another source registering a
    table of the same schema and name."""
    from provisa.mv.readable_inputs import require_no_view_made_ambiguous

    view = _view("daily", "SELECT * FROM sales_cat.public.orders", target="daily")
    model = _model([view])
    model.source_catalogs["billing"] = "billing_cat"
    require_no_view_made_ambiguous(model, _new_table("billing", "orders"))
