# Copyright (c) 2026 Kenneth Stott
# Canary: 10f49821-814c-4b25-8f7b-8fce10e1497f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""There is no draft state (REQ-1921, amended 2026-11-16): a registered table is live, and one out
of production is withdrawn -- the waypoint of a move between regions. The data plane's registry
leaves a withdrawn table out, so no schema offers it, no context names it and nothing copies it;
a statement naming it is refused as a relation that is not registered, on SQL and GraphQL alike,
and a view over it is refused naming it. A configuration carries it as withdrawn and refuses
``draft`` by name."""

# Requirements: REQ-1921

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler.schema_gen import SchemaInput

_ROLE = {"id": "analyst", "capabilities": ["usage"], "domain_access": ["sales"]}


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A deployment of one organisation, as the API layer says at import (provisa.api.app):
    run alone, nothing here imports it, and the registry's secret reads ask."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


def _context():
    """The role's context as the build makes it: from the tables in service alone."""
    from provisa.compiler.context import build_context

    si = SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "sales"}],
    )
    return build_context(si)


def test_a_schema_input_and_a_context_hold_no_draft():
    assert "draft_tables" not in SchemaInput.__dataclass_fields__
    ctx = _context()
    assert not hasattr(ctx, "draft_names") and not hasattr(ctx, "draft_ids")
    assert ctx.tables == {}


def test_a_sql_statement_naming_a_table_in_no_context_is_refused_as_not_registered():
    from provisa.compiler.sql_validator import _check_registered_relations

    import sqlglot

    gov = SimpleNamespace(table_map={}, physical_map={})
    for sql in ("SELECT id FROM orders", "SELECT id FROM public.orders", "SELECT id FROM nowhere"):
        violations = _check_registered_relations(sqlglot.parse_one(sql), gov, _context())
        assert [v.code for v in violations] == ["V006"], sql


def _govdata_row(*, alias=None) -> dict:
    """A table registered through the admin from a GovData source into a domain that is not its
    source schema: physical ``sec.financial_facts``, placed in the domain ``pet-shelter``."""
    return {
        "id": 41,
        "source_id": "askamerica",
        "domain_id": "pet-shelter",
        "schema_name": "sec",
        "table_name": "financial_facts",
        "alias": alias,
        "columns": [{"column_name": "cik", "visible_to": ["org_admin"]}],
    }


@pytest.mark.parametrize(
    ("alias", "said"),
    [
        (None, '"pet_shelter"."financial_facts"'),
        ("facts", '"pet_shelter"."facts"'),
        ("facts", "pet_shelter.financial_facts"),
        (None, "sec.financial_facts"),
        (None, "financial_facts"),
    ],
)
def test_a_view_over_a_withdrawn_table_is_refused_naming_it_by_every_name(alias, said):
    """A withdrawn table is named by every name the same table answers to in production -- its
    domain's SQL name with its semantic name or its table name -- so a view over it is refused
    naming it, whichever name the view's SQL gives it."""
    from provisa.compiler.definitions import TableIsWithdrawn
    from provisa.mv.view_read import require_inputs_in_service

    state = SimpleNamespace(
        tables=[], role_build_inputs={"withdrawn_tables": [_govdata_row(alias=alias)]}
    )
    with pytest.raises(TableIsWithdrawn) as refused:
        require_inputs_in_service(f"SELECT * FROM {said}", state)
    assert refused.value.table == "financial_facts"
    assert "'financial_facts' is withdrawn" in str(refused.value)


def test_a_withdrawn_and_a_served_table_answer_to_the_same_qualified_names():
    """One naming rule: the qualified names governance resolves for a served table are names a
    view over the same registration, withdrawn, is refused under."""
    from provisa.compiler.context import build_context
    from provisa.compiler.definitions import TableIsWithdrawn
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.stage2 import build_governance_context
    from provisa.mv.view_read import require_inputs_in_service

    role = {"id": "org_admin", "capabilities": ["usage"], "domain_access": ["*"]}
    row = _govdata_row(alias="facts")
    served_row = {**row, "write_ops": [], "write_returns_rows": False, "write_refused_forms": []}
    served = build_context(
        SchemaInput(
            tables=[served_row],
            relationships=[],
            column_types={41: [ColumnMetadata("cik", "varchar", True)]},
            naming_rules=[],
            role=role,
            domains=[{"id": "pet-shelter"}],
        )
    )
    gov = build_governance_context(
        "org_admin", RLSContext.empty(), {}, served, [served_row], role=role
    )
    qualified = {name for name in gov.table_map if "." in name}
    assert qualified == {
        "pet_shelter.facts",
        "pet_shelter.financial_facts",
        "sec.financial_facts",
        "askamerica.sec.financial_facts",
    }
    withdrawn = SimpleNamespace(tables=[], role_build_inputs={"withdrawn_tables": [row]})
    for name in sorted(qualified - {"askamerica.sec.financial_facts"}):
        with pytest.raises(TableIsWithdrawn):
            require_inputs_in_service(f"SELECT * FROM {name}", withdrawn)


@pytest.mark.parametrize(
    "field", ["orders", "orders_aggregate", "orders_by_pk", "insert_orders", "update_orders"]
)
def test_a_graphql_field_naming_a_table_in_no_schema_is_unknown(field):
    from graphql import build_schema

    from provisa.compiler.parser import GraphQLValidationError, parse_query

    schema = build_schema("type Query { other: Int } type Mutation { other: Int }")
    operation = "mutation" if field.startswith(("insert_", "update_")) else "query"
    with pytest.raises(GraphQLValidationError):
        parse_query(schema, f"{operation} {{ {field} {{ id }} }}", ctx=_context())


# -- the registry the data plane reads, and where withdrawn is set --------------------------------


@pytest.fixture
def model(control_plane):
    return control_plane.org()


def _table(name: str):
    from provisa.core.models import Table

    return Table.model_validate(
        {
            "source_id": "crm",
            "domain_id": "sales",
            "schema": "public",
            "table": name,
            "columns": [{"name": "id", "visible_to": ["analyst"], "data_type": "integer"}],
        }
    )


async def _seed(conn) -> None:
    from sqlalchemy import insert

    from provisa.core.models import Source
    from provisa.core.repositories import source as source_repo
    from provisa.core.schema_org import domains

    await source_repo.upsert(
        conn,
        Source(id="crm", type="postgresql", host="h", database="d", username="u", password=""),
    )
    await conn.execute_core(insert(domains).values(id="sales"))


async def test_the_data_planes_registry_leaves_withdrawn_tables_out(model):
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.repositories import table as table_repo

    async with model.acquire() as conn:
        await _seed(conn)
        await table_repo.upsert(conn, _table("orders"))
        await table_repo.upsert(conn, _table("staging"), withdrawn=True)
        assert [t["table_name"] for t in await fetch_tables(conn)] == ["orders"]
        assert [t["table_name"] for t in await fetch_tables(conn, withdrawn=True)] == ["staging"]


async def test_a_new_table_is_live_and_a_resave_keeps_what_is_held(model):
    """REQ-1921: a table a registration creates is live; a later save that is not a
    configuration's (an admin edit, a bulk re-sync) keeps a held table's withdrawn as it is."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.repositories import table as table_repo

    async with model.acquire() as conn:
        await _seed(conn)
        await table_repo.upsert(conn, _table("orders"))
        await table_repo.upsert(conn, _table("staging"), withdrawn=True)
        await table_repo.upsert(conn, _table("staging"))  # a re-sync
        assert [t["table_name"] for t in await fetch_tables(conn, withdrawn=True)] == ["staging"]
        await table_repo.upsert(conn, _table("staging"), withdrawn=False)  # a configuration
        assert [t["table_name"] for t in await fetch_tables(conn)] == ["orders", "staging"]


def test_a_config_table_is_withdrawn_only_when_the_file_says_so_and_draft_is_refused():
    from pydantic import ValidationError

    from provisa.core.models import Table

    declared = {
        "source_id": "crm",
        "domain_id": "sales",
        "schema": "public",
        "table": "orders",
        "columns": [{"name": "id", "visible_to": ["analyst"]}],
    }
    assert Table.model_validate(declared).withdrawn is False
    assert Table.model_validate({**declared, "withdrawn": True}).withdrawn is True
    with pytest.raises(ValidationError) as refused:
        Table.model_validate({**declared, "draft": True})
    assert "table 'orders' declares draft" in str(refused.value)
    assert "withdrawn: true" in str(refused.value)


async def test_a_table_withdrawn_takes_its_regions_cached_responses_with_it():
    """REQ-1921: withdrawing removes the cached responses, not only stops new ones."""
    from provisa.cache.tenancy import purge_when_withdrawn

    purged: list[str] = []

    class _Store:
        async def purge_place(self, place: str) -> int:
            purged.append(place)
            return 3

    runtime = SimpleNamespace(withdrawn_table_ids=frozenset())
    state = SimpleNamespace(
        org_id="acme", response_cache_store=_Store(), _active_runtime=lambda: runtime
    )
    assert await purge_when_withdrawn(state, frozenset({7})) == 3  # 7 withdrawn
    assert await purge_when_withdrawn(state, frozenset({7})) == 0  # still: nothing new
    assert await purge_when_withdrawn(state, frozenset()) == 0  # put back: nothing to remove
    assert await purge_when_withdrawn(state, frozenset({7, 9})) == 3
    assert len(purged) == 2


# -- a withdrawn table is in none of the lists a reader is served from ----------------------------


async def test_a_withdrawn_table_is_in_no_list_a_reader_is_served_from(model, monkeypatch):
    """REQ-1921: the lists every reader's surface is made from are the tables in service -- the
    registry reads are planned from, the replica routes, the tables counted for Hot replication,
    the catalog (Flight, pgwire, MCP and its search index), the live polls."""
    from types import SimpleNamespace

    from provisa.api.admin.db_queries import fetch_tables
    from provisa.api.flight.catalog import _build_catalog_tables_async
    from provisa.core.repositories import table as table_repo
    from provisa.federation import registry_view, replica_routing
    from provisa.live import reconcile as live_reconcile

    async with model.acquire() as conn:
        await _seed(conn)
        await table_repo.upsert(conn, _table("orders"))
        await table_repo.upsert(conn, _table("staging"), withdrawn=True)
        # A withdrawn table polled live, were withdrawn tables polled.
        from sqlalchemy import update

        from provisa.core.schema_org import registered_tables as rt

        await conn.execute_core(update(rt).values(live={"strategy": "ttl", "interval": 60}))

    state = SimpleNamespace(
        model_db=model,
        tenant_db=model,
        config=object(),
        schema_boot_id="b",
        schema_version=1,
        tables=[],
        contexts={},
    )
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", lambda _s: "store")

    def names(tables) -> list[str]:
        return sorted(t["table_name"] if isinstance(t, dict) else t.table_name for t in tables)

    # What reads are planned from, what Hot replication counts and promotes
    # (replica_hot.evaluate reads this list), and what the replica routes are made from.
    assert names(await registry_view.registered_tables(state)) == ["orders"]
    async with model.acquire() as conn:
        assert names(await registry_view.registered_tables(state, conn)) == ["orders"]
    assert names((await replica_routing._registry(state)).registered) == ["orders"]
    # The catalog every catalog surface is built from.
    assert sorted(t.table_name for t in await _build_catalog_tables_async(state)) == ["orders"]

    # The live polls.
    class _Live:
        specs: list = []

        def reconcile(self, specs, *a, **kw):
            self.specs = list(specs)

    live = _Live()
    async with model.acquire() as conn:
        await live_reconcile.reconcile_live_engine(conn, live)
    assert [s.query_id for s in live.specs] == ["crm.orders"]

    # It is found only where its steward's preview looks for it.
    async with model.acquire() as conn:
        assert names(await fetch_tables(conn, withdrawn=True)) == ["staging"]
