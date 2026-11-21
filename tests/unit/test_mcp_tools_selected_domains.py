# Copyright (c) 2026 Kenneth Stott
# Canary: 81a7ad44-189c-40f0-aceb-b45e50a22271
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every MCP chat tool stays within the domains selected in the domain filter (REQ-2253).

The catalog, query, metric and explore tools were narrowed first (test_mcp_chat_selected_domains).
These are the rest: the glossary tools, the data product and metric tools, the reference library
and term searches, the native source tools, relationship discovery and the profiler, fakes,
synthetic and environment tools. Under a selection nothing outside it is listed, searched,
described or changed; with no selection each tool is as it was.

The model: two domains, ``sales`` (table ``orders``) and ``crm`` (table ``customers``), one term
and one data product in each.
"""

# Requirements: REQ-2253

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import insert, select

from provisa.api.mcp import admin_api, model_tool_specs, tools
from provisa.core.models import Column, DataProduct, Metric, Table
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import metric as metric_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources
from provisa.security.selected_domains import within
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio

SALES = frozenset({"sales"})


def _state(control_plane) -> RoleState:
    return RoleState(
        contexts={"analyst": object()},
        roles={"analyst": {"id": "analyst", "domain_access": ["*"]}},
        model_db=control_plane.org(),
        config=SimpleNamespace(mail=SimpleNamespace(provider="")),
    )


def _request():
    """A request of a deployment with no auth provider: no right gates it, so only the
    selection narrows what each tool reaches."""
    return SimpleNamespace(state=SimpleNamespace(active_org_id="default", identity=None))


def _table(domain: str, name: str) -> Table:
    return Table(
        source_id="pg",
        domain_id=domain,
        schema_name="public",
        table_name=name,
        columns=[Column(name="id", data_type="integer", visible_to=[])],
    )


@asynccontextmanager
async def _model(control_plane):
    """The two-domain model; yields the ids it made."""
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        for d in ("sales", "crm"):
            c.execute(insert(domains).values(id=d, description=""))
        c.execute(insert(sources).values(id="pg", type="postgresql"))
        c.execute(insert(sources).values(id="crm_db", type="postgresql", allowed_domains=["crm"]))
    async with control_plane.org().acquire() as conn:
        ids = {
            "orders": await table_repo.upsert(conn, _table("sales", "orders")),
            "customers": await table_repo.upsert(conn, _table("crm", "customers")),
            "sales_term": await glossary_repo.create_abstract_term(
                conn, "booking", domains={"sales"}
            ),
            "crm_term": await glossary_repo.create_abstract_term(conn, "lead", domains={"crm"}),
            "org_term": await glossary_repo.create_abstract_term(conn, "party", domains={"*"}),
        }
        for pid, domain in (("sales_dp", "sales"), ("crm_dp", "crm")):
            await data_product_repo.upsert(conn, DataProduct(id=pid, domain_id=domain, name=pid))
        await metric_repo.upsert(
            conn,
            Metric(name="customer_count", expression="COUNT(customers.id)", visible_to=["analyst"]),
        )
        yield ids


@pytest.fixture
def _ungated():
    """The glossary's own right gates admit the caller; domain authority is what is tested."""
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_read"),
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()),
        patch("provisa.api.admin.reference_library_router.embed_terms", new=AsyncMock()),
        patch("provisa.api.admin.schema_helpers._rebuild_schemas", new=AsyncMock()),
    ):
        yield


# -- the glossary ---------------------------------------------------------------------------------


async def _term_names(state) -> set[str]:
    return {t["name"] for t in await tools.list_glossary_terms(state, "analyst", _request())}


@pytest.mark.usefixtures("_ungated")
async def test_a_selection_lists_only_its_terms_and_the_orgs(control_plane):
    async with _model(control_plane):
        state = _state(control_plane)
        with within(SALES):
            names = await _term_names(state)
        assert {"booking", "party"} <= names
        assert "lead" not in names


@pytest.mark.usefixtures("_ungated")
async def test_no_selection_lists_every_term(control_plane):
    async with _model(control_plane):
        assert {"booking", "lead", "party"} <= await _term_names(_state(control_plane))


@pytest.mark.usefixtures("_ungated")
async def test_a_term_outside_the_selection_is_not_changed(control_plane):
    async with _model(control_plane) as ids:
        state = _state(control_plane)
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.update_glossary_term(
                state, "analyst", _request(), ids["crm_term"], definition="x"
            )
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.delete_glossary_term(state, "analyst", _request(), ids["crm_term"])
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.add_glossary_term_edge(
                state, "analyst", _request(), ids["sales_term"], ids["crm_term"], "broader"
            )
        async with control_plane.org().acquire() as conn:
            terms = {t["name"]: t for t in await glossary_repo.list_terms(conn)}
        assert "lead" in terms and not terms["lead"].get("definition")


@pytest.mark.usefixtures("_ungated")
async def test_an_org_wide_term_is_outside_any_selection(control_plane):
    async with _model(control_plane) as ids:
        with within(SALES), pytest.raises(PermissionError, match="'\\*'"):
            await tools.update_glossary_term(
                _state(control_plane), "analyst", _request(), ids["org_term"], definition="x"
            )


@pytest.mark.usefixtures("_ungated")
async def test_a_term_inside_the_selection_is_changed(control_plane):
    async with _model(control_plane) as ids:
        with within(SALES):
            await tools.update_glossary_term(
                _state(control_plane), "analyst", _request(), ids["sales_term"], definition="d"
            )
        async with control_plane.org().acquire() as conn:
            terms = {t["name"]: t for t in await glossary_repo.list_terms(conn)}
        assert terms["booking"]["definition"] == "d"


@pytest.mark.usefixtures("_ungated")
async def test_a_term_is_not_created_in_a_domain_outside_the_selection(control_plane):
    async with _model(control_plane):
        with (
            patch("provisa.api.admin.glossary_router._declared_domains", return_value={"crm"}),
            within(SALES),
            pytest.raises(PermissionError, match="'crm'"),
        ):
            await tools.create_glossary_term(
                _state(control_plane), "analyst", _request(), "prospect", domains=["crm"]
            )
        async with control_plane.org().acquire() as conn:
            assert "prospect" not in {t["name"] for t in await glossary_repo.list_terms(conn)}


@pytest.mark.usefixtures("_ungated")
async def test_a_term_is_not_grounded_to_a_table_outside_the_selection(control_plane):
    async with _model(control_plane) as ids:
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.ground_glossary_term_to_table(
                _state(control_plane), "analyst", _request(), ids["sales_term"], ids["customers"]
            )


# -- data products and metrics --------------------------------------------------------------------


async def test_a_selection_lists_only_its_data_products(control_plane):
    async with _model(control_plane):
        state = _state(control_plane)
        with within(SALES):
            listed = await tools.list_data_products(state, "analyst", _request())
        assert [p["id"] for p in listed] == ["sales_dp"]
        every = await tools.list_data_products(state, "analyst", _request())
        assert {p["id"] for p in every} == {"sales_dp", "crm_dp"}


async def test_a_selection_lists_only_its_data_products_findings(control_plane):
    found = {"crm_dp": [{"kind": "no_owner"}], "sales_dp": [{"kind": "no_owner"}]}
    async with _model(control_plane):
        state = _state(control_plane)
        with (
            patch("provisa.core.product_findings.context", new=AsyncMock()),
            patch("provisa.core.product_findings.findings", new=AsyncMock(return_value=found)),
        ):
            with within(SALES):
                listed = await tools.list_data_product_findings(state, "analyst", _request())
            every = await tools.list_data_product_findings(state, "analyst", _request())
        assert [f["product_id"] for f in listed] == ["sales_dp"]
        assert [f["product_id"] for f in every] == ["crm_dp", "sales_dp"]


@pytest.mark.usefixtures("_ungated")
async def test_a_data_product_outside_the_selection_is_not_changed(control_plane):
    async with _model(control_plane) as ids:
        state = _state(control_plane)
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.delete_data_product(state, "analyst", _request(), "crm_dp")
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.create_data_product(state, "analyst", _request(), "new", "crm", "New")
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.change_data_product_status(
                state, "analyst", _request(), "crm_dp", "publish", {}
            )
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.set_data_product_members(
                state, "analyst", _request(), "sales_dp", [ids["customers"]], []
            )
        async with control_plane.org().acquire() as conn:
            assert await data_product_repo.get(conn, "crm_dp") is not None
            assert await data_product_repo.get(conn, "new") is None
            assert await data_product_repo.member_table_ids(conn, "sales_dp") == []


@pytest.mark.usefixtures("_ungated")
async def test_a_metric_reading_outside_the_selection_is_not_changed(control_plane):
    async with _model(control_plane):
        state = _state(control_plane)
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.delete_metric(state, "analyst", _request(), "customer_count")
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await tools.upsert_metric(
                state, "analyst", _request(), "lead_count", "COUNT(customers.id)"
            )
        async with control_plane.org().acquire() as conn:
            assert await metric_repo.get(conn, "customer_count") is not None
            assert await metric_repo.get(conn, "lead_count") is None


# -- searches -------------------------------------------------------------------------------------


async def test_a_selection_searches_only_its_vocabulary(control_plane):
    async with _model(control_plane):
        state = _state(control_plane)
        with patch.object(glossary_repo, "search_terms", new=AsyncMock(return_value=[])) as found:
            with within(SALES):
                await tools.search_terms(state, "analyst", "lead")
            assert found.await_args is not None
            assert found.await_args.kwargs["domains"] == SALES
            await tools.search_terms(state, "analyst", "lead")
            assert found.await_args.kwargs["domains"] is None


async def test_a_selection_draws_only_on_its_reference_documents(control_plane):
    state = _state(control_plane)
    with (
        patch("provisa.reference_library.retrieve.org_embedder", new=AsyncMock()),
        patch(
            "provisa.reference_library.retrieve.retrieve_with_neighbours",
            new=AsyncMock(return_value=[]),
        ) as found,
    ):
        with within(SALES):
            await tools.search_reference_library(state, "analyst", "what is a lead")
        assert found.await_args is not None
        assert found.await_args.args[2] == SALES
        await tools.search_reference_library(state, "analyst", "what is a lead")
        assert found.await_args.args[2] is None


# -- native source tools --------------------------------------------------------------------------


async def test_a_source_open_to_no_selected_domain_is_not_introspected(control_plane):
    async with _model(control_plane):
        state = _state(control_plane)
        with (
            patch(
                "provisa.api.admin.schema_query.Query.available_tables",
                new=AsyncMock(return_value=[]),
            ) as listed,
            patch(
                "provisa.api.admin.schema_query.resolve_available_columns_metadata",
                new=AsyncMock(return_value=[]),
            ) as described,
        ):
            with within(SALES), pytest.raises(PermissionError, match="'crm_db'"):
                await tools.list_native_tables(state, "analyst", "crm_db")
            with within(SALES), pytest.raises(PermissionError, match="'crm_db'"):
                await tools.describe_native_table(state, "analyst", "crm_db", "public", "leads")
            listed.assert_not_awaited()
            described.assert_not_awaited()
            # A source naming no domains is open to every one; no selection reads any source.
            with within(SALES):
                await tools.list_native_tables(state, "analyst", "pg")
            await tools.list_native_tables(state, "analyst", "crm_db")
        assert listed.await_count == 2


# -- relationship discovery -----------------------------------------------------------------------


async def test_discovery_does_not_run_on_a_table_outside_the_selection():
    state = SimpleNamespace(
        tables=[
            {"id": 2, "domain_id": "crm", "schema_name": "public", "table_name": "customers"},
        ],
        model_db=object(),
    )
    with (
        patch(
            "provisa.api.admin.discovery.trigger_discovery",
            new=AsyncMock(side_effect=AssertionError),
        ),
        within(SALES),
        pytest.raises(PermissionError, match="'crm'"),
    ):
        await admin_api.discover_relationships(state, "crm", "customers", request=_request())


# -- profiler, fakes, synthetic data, environments, config export ---------------------------------


async def _dispatch(control_plane, name: str, args: dict) -> AsyncMock:
    """Dispatch model tool ``name``; the tool itself is stood in, so only the dispatch's own
    selection check decides whether it runs."""
    called = AsyncMock(return_value={"ok": True})
    with patch(f"provisa.api.mcp.model_tools.{name}", new=called):
        await model_tool_specs.dispatch(_state(control_plane), "analyst", name, args, _request())
    return called


async def test_a_table_tool_does_not_reach_a_table_outside_the_selection(control_plane):
    async with _model(control_plane) as ids:
        for name, args in (
            ("get_table_profile", {"table_id": ids["customers"]}),
            (
                "create_expectation_check",
                {"table_id": ids["orders"], "expectations_table_id": ids["customers"]},
            ),
            ("find_table_id", {"domain": "crm", "table": "customers"}),
        ):
            with within(SALES), pytest.raises(PermissionError, match="'crm'"):
                await _dispatch(control_plane, name, args)
        with within(SALES):
            ran = await _dispatch(control_plane, "get_table_profile", {"table_id": ids["orders"]})
        ran.assert_awaited_once()


async def test_a_tool_spanning_every_domain_is_refused_under_a_selection(control_plane):
    async with _model(control_plane):
        for name, args in (
            ("export_model_config", {}),
            ("list_synthetic_datasets", {}),
            ("get_environment_detail", {"env": "dev"}),
        ):
            with within(SALES), pytest.raises(PermissionError, match="every domain"):
                await _dispatch(control_plane, name, args)
            # No selection: the tool runs as it did.
            (await _dispatch(control_plane, name, args)).assert_awaited_once()


async def test_a_profiler_with_a_member_outside_the_selection_is_not_run(control_plane):
    async with _model(control_plane):
        with control_plane.engine().begin() as c:
            c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
            from provisa.core.schema_org import registered_tables as rt

            c.execute(rt.update().values(profiler_source_id="pg"))
            assert c.execute(select(rt.c.id)).fetchall()
        with within(SALES), pytest.raises(PermissionError, match="'crm'"):
            await _dispatch(control_plane, "run_profiler", {"source_id": "pg"})
