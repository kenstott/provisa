# Copyright (c) 2026 Kenneth Stott
# Canary: 10f49821-814c-4b25-8f7b-8fce10e1497f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A draft table or view is out of service (REQ-1921, "draft is its own setting"): the data
plane's registry leaves it out, so no schema offers it and nothing copies it; a statement naming
it is refused naming it as draft, on SQL and GraphQL alike; its domain's owners set and clear it.
A table registered through the admin starts as draft; a config table only when the file says so."""

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


def _row(name: str, *, domain="sales", alias=None, visible_to=None) -> dict:
    return {
        "id": 9,
        "source_id": "crm",
        "domain_id": domain,
        "schema_name": "public",
        "table_name": name,
        "alias": alias,
        "columns": [{"column_name": "id", "visible_to": visible_to or []}],
    }


def _context(*drafts: dict, domain_prefix: bool = False):
    from provisa.compiler.context import build_context

    si = SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role=_ROLE,
        domains=[{"id": "sales"}],
        domain_prefix=domain_prefix,
        draft_tables=list(drafts),
    )
    return build_context(si)


def test_a_draft_table_the_role_would_read_is_named_by_every_name_it_could_be_given():
    ctx = _context(_row("orders", alias="order_book"))
    assert {k for k, v in ctx.draft_names.items() if v == "orders"} >= {
        "orders",
        "order_book",
        "public.orders",
        "sales.orders",
        "sales.order_book",
    }
    assert ctx.tables == {}  # offered in no schema


def test_a_draft_table_out_of_the_roles_reach_is_not_named():
    assert _context(_row("payroll", domain="hr")).draft_names == {}
    assert _context(_row("orders", visible_to=["auditor"])).draft_names == {}


def test_a_domain_prefixed_draft_is_named_by_its_prefixed_field():
    """Prefixed as the schema build prefixes a table: by its domain's GraphQL alias."""
    from provisa.compiler.naming import domain_gql_alias

    ctx = _context(_row("orders"), domain_prefix=True)
    assert ctx.draft_names[f"{domain_gql_alias('sales', None)}__orders"] == "orders"


def test_a_sql_statement_naming_a_draft_table_is_refused_as_draft():
    from provisa.compiler.sql_validator import _check_registered_relations

    import sqlglot

    ctx = _context(_row("orders"))
    gov = SimpleNamespace(table_map={}, physical_map={})
    for sql, code in (
        ("SELECT id FROM orders", "V020"),
        ("SELECT id FROM public.orders", "V020"),
        ("SELECT id FROM nowhere", "V006"),
    ):
        violations = _check_registered_relations(sqlglot.parse_one(sql), gov, ctx)
        assert [v.code for v in violations] == [code], sql
    refused = _check_registered_relations(sqlglot.parse_one("SELECT id FROM orders"), gov, ctx)
    assert "'orders' is a draft" in refused[0].message


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
        "columns": [{"column_name": "cik", "visible_to": []}],
    }


@pytest.mark.parametrize(
    ("alias", "said"),
    [
        (None, '"pet_shelter"."financial_facts"'),  # what the admin's Preview writes
        ("facts", '"pet_shelter"."facts"'),  # ... for an aliased table
        ("facts", "pet_shelter.financial_facts"),
        (None, "sec.financial_facts"),
        (None, "financial_facts"),
    ],
)
def test_a_draft_named_through_its_domain_is_refused_as_draft(alias, said):
    """A draft is named by every name the same table answers to once released — its domain's
    SQL name with its semantic name or its table name, as ``build_governance_context`` names a
    served table — so the admin's Preview of a new table is refused as draft, not as unknown."""
    import sqlglot

    from provisa.compiler.context import build_context
    from provisa.compiler.sql_validator import _check_registered_relations

    role = {"id": "org_admin", "capabilities": ["usage"], "domain_access": ["*"]}
    ctx = build_context(
        SchemaInput(
            tables=[],
            relationships=[],
            column_types={},
            naming_rules=[],
            role=role,
            domains=[{"id": "pet-shelter"}],
            draft_tables=[_govdata_row(alias=alias)],
        )
    )
    gov = SimpleNamespace(table_map={}, physical_map={})
    tree = sqlglot.parse_one(f"SELECT * FROM {said} LIMIT 51 OFFSET 0", dialect="postgres")
    violations = _check_registered_relations(tree, gov, ctx)
    assert [v.code for v in violations] == ["V020"], violations
    assert "'financial_facts' is a draft" in violations[0].message


def test_a_draft_and_a_served_table_answer_to_the_same_qualified_names():
    """One naming rule: the qualified names governance resolves for a served table are the
    qualified names a draft of the same registration is refused under."""
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.stage2 import build_governance_context

    role = {"id": "org_admin", "capabilities": ["usage"], "domain_access": ["*"]}
    row = _govdata_row(alias="facts")
    served_row = {**row, "write_ops": [], "write_returns_rows": False, "write_refused_forms": []}

    def _built(*, draft: bool):
        return build_context(
            SchemaInput(
                tables=[] if draft else [served_row],
                relationships=[],
                column_types={41: [ColumnMetadata("cik", "varchar", True)]},
                naming_rules=[],
                role=role,
                domains=[{"id": "pet-shelter"}],
                draft_tables=[row] if draft else [],
            )
        )

    served = _built(draft=False)
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
    assert qualified <= set(_built(draft=True).draft_names)


@pytest.mark.parametrize(
    "field", ["orders", "orders_aggregate", "orders_by_pk", "insert_orders", "update_orders"]
)
def test_a_graphql_field_naming_a_draft_table_is_refused_as_draft(field):
    from graphql import build_schema

    from provisa.compiler.definitions import TableIsDraft
    from provisa.compiler.parser import parse_query

    schema = build_schema("type Query { other: Int } type Mutation { other: Int }")
    ctx = _context(_row("orders"))
    operation = "mutation" if field.startswith(("insert_", "update_")) else "query"
    with pytest.raises(TableIsDraft) as refused:
        parse_query(schema, f"{operation} {{ {field} {{ id }} }}", ctx=ctx)
    assert refused.value.table == "orders"


def test_a_graphql_field_naming_nothing_is_still_unknown():
    from graphql import build_schema

    from provisa.compiler.parser import GraphQLValidationError, parse_query

    schema = build_schema("type Query { other: Int }")
    with pytest.raises(GraphQLValidationError):
        parse_query(schema, "{ nowhere { id } }", ctx=_context(_row("orders")))


# -- the registry the data plane reads, and where draft is set --------------------------------


@pytest.fixture
def model(control_plane):
    return control_plane.org()


def _table(name: str, *, draft: bool):
    from provisa.core.models import Table

    return Table.model_validate(
        {
            "source_id": "crm",
            "domain_id": "sales",
            "schema": "public",
            "table": name,
            "draft": draft,
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


async def test_the_data_planes_registry_leaves_drafts_out(model):
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.repositories import table as table_repo

    async with model.acquire() as conn:
        await _seed(conn)
        await table_repo.upsert(conn, _table("orders", draft=False))
        await table_repo.upsert(conn, _table("staging", draft=True))
        assert [t["table_name"] for t in await fetch_tables(conn)] == ["orders"]
        assert [t["table_name"] for t in await fetch_tables(conn, draft=True)] == ["staging"]


async def test_a_bulk_registration_starts_new_tables_as_draft_and_keeps_a_resyncs(model):
    from provisa.api.admin.region_defaults import admin_registration_draft
    from provisa.core.repositories import table as table_repo

    async with model.acquire() as conn:
        await _seed(conn)
        assert await admin_registration_draft(conn, "crm", "public", "new_one") is True
        await table_repo.upsert(conn, _table("orders", draft=False))
        assert await admin_registration_draft(conn, "crm", "public", "orders") is False


def test_a_config_table_is_draft_only_when_the_file_says_so():
    from provisa.core.models import Table

    declared = {
        "source_id": "crm",
        "domain_id": "sales",
        "schema": "public",
        "table": "orders",
        "columns": [{"name": "id", "visible_to": ["analyst"]}],
    }
    assert Table.model_validate(declared).draft is False
    assert Table.model_validate({**declared, "draft": True}).draft is True


async def test_a_table_gone_draft_takes_its_regions_cached_responses_with_it():
    """REQ-1921: turning draft on removes the cached responses, not only stops new ones."""
    from provisa.cache.tenancy import purge_when_drafted

    purged: list[str] = []

    class _Store:
        async def purge_place(self, place: str) -> int:
            purged.append(place)
            return 3

    runtime = SimpleNamespace(draft_table_ids=frozenset())
    state = SimpleNamespace(
        org_id="acme", response_cache_store=_Store(), _active_runtime=lambda: runtime
    )
    assert await purge_when_drafted(state, frozenset({7})) == 3  # 7 went draft
    assert await purge_when_drafted(state, frozenset({7})) == 0  # still draft: nothing new
    assert await purge_when_drafted(state, frozenset()) == 0  # released: nothing to remove
    assert await purge_when_drafted(state, frozenset({7, 9})) == 3
    assert len(purged) == 2


# -- a draft that holds a replica is still in none of the lists a reader is served from ----------


async def test_a_draft_holding_a_replica_is_in_no_list_a_reader_is_served_from(model, monkeypatch):
    """REQ-1921: a draft may hold a replica, landed for its steward's preview. Holding one
    offers it to nobody: the lists every reader's surface is made from are the tables in
    service -- the registry reads are planned from, the replica routes, the tables counted for
    Hot replication, the catalog (Flight, pgwire, MCP and its search index), the live polls."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from provisa.api.flight.catalog import _build_catalog_tables_async
    from provisa.core.repositories import table as table_repo
    from provisa.federation import registry_view, replica_routing, replica_state
    from provisa.live import reconcile as live_reconcile

    key = ("crm", "public", "staging")
    async with model.acquire() as conn:
        await _seed(conn)
        await table_repo.upsert(conn, _table("orders", draft=False))
        await table_repo.upsert(conn, _table("staging", draft=True))
        # As its steward's preview asks for it (query_residency): the ordinary record.
        assert await replica_state.request_build(
            conn, key, replica_state.REASON_MODEL, model_stamp=1, now=datetime.now(UTC)
        )
        assert await replica_state.read(conn, key) is not None
        # A draft polled live, were drafts polled.
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

    # It is found only where its steward's read and its build look for it.
    assert names(await registry_view.draft_tables(state)) == ["staging"]
