# Copyright (c) 2026 Kenneth Stott
# Canary: 34d4fb24-b010-415e-a0f9-03d67c2170a0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The OData service (REQ-2193), over the real router, schema, compiler and governance.

Each statement the service hands the pipeline is governed by the real ``apply_governance`` for the
acting role and run on DuckDB, as ``test_masked_join_keys`` runs one, so what a test reads is what
the statement returned under the role's masks and row rules.
"""

# Requirements: REQ-2193

from __future__ import annotations

import math
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import duckdb
import httpx
import json

import pytest
import sqlglot
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

from provisa.api.odata import expand as odata_expand
from provisa.api.odata.csdl import metadata_document
from provisa.api.odata.edm import EdmType, edm_type
from provisa.api.odata.expression import (
    Call,
    Comparison,
    In,
    Literal,
    Logical,
    Name,
    Not,
    OrderKey,
    parse_filter,
    parse_key,
    parse_orderby,
)
from provisa.api.odata.headers import ODataResponseHeaders
from provisa.api.odata.model import entity_sets
from provisa.api.odata.query import QueryOptionRefused, parse_query_options
from provisa.api.odata import router as odata_router
from provisa.api.role_surface import RoleSurface
from provisa.auth.middleware import AuthMiddleware
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, build_table_path_map, generate_schema
from provisa.compiler.sql_rewrite import lower_to_catalog_physical
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.core.limits import http_transport_for_path
from provisa.fakes import digest as digest_mod
from provisa.fakes.duckdb_functions import register
from provisa.pgwire import governed_plan
from provisa.security.masking import MaskingRule, MaskType
from tests.role_state import RoleState

pytestmark = [pytest.mark.anyio]

_ROLES = {
    "analyst": {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]},
    "clerk": {"id": "clerk", "capabilities": ["query_development"], "domain_access": ["*"]},
}
_EDM = "{http://docs.oasis-open.org/odata/ns/edm}"
_ORDERS = 12
_EMAIL_MASK = MaskingRule(mask_type=MaskType.constant, value="***")


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _column(name: str, roles: tuple[str, ...], key: bool = False) -> dict:
    return {"column_name": name, "visible_to": list(roles), "is_primary_key": key}


def _table(table_id: int, name: str, columns: list[dict]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": name,
        "columns": columns,
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


_BOTH = ("analyst", "clerk")
TABLES = [
    _table(
        1,
        "orders",
        [
            _column("id", _BOTH, key=True),
            _column("customer_id", _BOTH),
            _column("email", _BOTH),
            _column("amount", _BOTH),
            _column("margin", ("analyst",)),
            _column("placed_at", _BOTH),
        ],
    ),
    _table(2, "customers", [_column("id", _BOTH, key=True), _column("name", _BOTH)]),
    _table(3, "salaries", [_column("id", ("analyst",), key=True), _column("pay", ("analyst",))]),
    _table(4, "events", [_column("kind", _BOTH), _column("note", _BOTH)]),
    _table(
        5,
        "stamps",
        [
            _column("id", ("analyst",), key=True),
            _column("ref", ("analyst",)),
            _column("on_day", ("analyst",)),
            _column("at_time", ("analyst",)),
            _column("at_tz", ("analyst",)),
            _column("flag", ("analyst",)),
            _column("doc", ("analyst",)),
        ],
    ),
]
RELATIONSHIPS = [
    {
        "id": "order-customer",
        "source_table_id": 1,
        "target_table_id": 2,
        "source_column": "customer_id",
        "target_column": "id",
        "cardinality": "many-to-one",
        "alias": "customer",
    },
    {
        "id": "customer-orders",
        "source_table_id": 2,
        "target_table_id": 1,
        "source_column": "id",
        "target_column": "customer_id",
        "cardinality": "one-to-many",
        "alias": "orders",
    },
    {
        "id": "customer-orders-again",
        "source_table_id": 2,
        "target_table_id": 1,
        "source_column": "id",
        "target_column": "customer_id",
        "cardinality": "one-to-many",
        "alias": "ordersAgain",
        "graphql_alias": "ordersAgain",
    },
    {
        "id": "order-salary",
        "source_table_id": 1,
        "target_table_id": 3,
        "source_column": "id",
        "target_column": "id",
        "cardinality": "one-to-one",
        "alias": "salary",
    },
]
COLUMN_TYPES = {
    1: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="customer_id", data_type="integer", is_nullable=True),
        ColumnMetadata(column_name="email", data_type="varchar(200)", is_nullable=True),
        ColumnMetadata(column_name="amount", data_type="decimal(10,2)", is_nullable=True),
        ColumnMetadata(column_name="margin", data_type="double", is_nullable=True),
        ColumnMetadata(column_name="placed_at", data_type="timestamp", is_nullable=True),
    ],
    2: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="name", data_type="varchar(100)", is_nullable=True),
    ],
    3: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="pay", data_type="integer", is_nullable=True),
    ],
    4: [
        ColumnMetadata(column_name="kind", data_type="varchar", is_nullable=True),
        ColumnMetadata(column_name="note", data_type="varchar", is_nullable=True),
    ],
    5: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="ref", data_type="uuid", is_nullable=True),
        ColumnMetadata(column_name="on_day", data_type="date", is_nullable=True),
        ColumnMetadata(column_name="at_time", data_type="time", is_nullable=True),
        ColumnMetadata(column_name="at_tz", data_type="timestamptz", is_nullable=True),
        ColumnMetadata(column_name="flag", data_type="boolean", is_nullable=True),
        ColumnMetadata(column_name="doc", data_type="jsonb", is_nullable=True),
    ],
}


def _visible(role_id: str) -> list[dict]:
    """The registered tables as the role is served them: a table it has no column of is gone."""
    return [t for t in TABLES if any(role_id in c["visible_to"] for c in t["columns"])]


def _surface(role_id: str) -> tuple[RoleSurface, SchemaInput]:
    si = SchemaInput(
        tables=_visible(role_id),
        relationships=RELATIONSHIPS,
        column_types=COLUMN_TYPES,
        naming_rules=[],
        role=_ROLES[role_id],
        domains=[{"id": "sales", "description": "Sales"}],
    )
    surface = RoleSurface(
        schema=generate_schema(si),
        context=build_context(si),
        rls=RLSContext(rules={}),
        table_path_map=build_table_path_map(si),
    )
    return surface, si


class _Pipeline:
    """Stands in for the pipeline's two entry points: the real governance of the acting role, then
    the governed statement on DuckDB."""

    def __init__(self, surfaces: dict[str, RoleSurface], masks: dict, rls: dict) -> None:
        self.surfaces = surfaces
        self.masks = masks
        self.rls = rls
        self.statements: list[str] = []
        self.bound: list[list] = []
        self.con = duckdb.connect()
        self.con.execute("ATTACH ':memory:' AS pg")
        self.con.execute("CREATE SCHEMA pg.public")
        self.con.execute(
            "CREATE TABLE pg.public.orders (id INTEGER, customer_id INTEGER, email VARCHAR, "
            "amount DECIMAL(10,2), margin DOUBLE, placed_at TIMESTAMP)"
        )
        self.con.execute("CREATE TABLE pg.public.customers (id INTEGER, name VARCHAR)")
        self.con.execute("CREATE TABLE pg.public.salaries (id INTEGER, pay INTEGER)")
        self.con.execute("CREATE TABLE pg.public.events (kind VARCHAR, note VARCHAR)")
        for i in range(_ORDERS, 0, -1):  # stored out of key order
            self.con.execute(
                "INSERT INTO pg.public.orders VALUES (?, ?, ?, ?, ?, ?)",
                [
                    i,
                    i % 3,
                    f"user{i}@real.example",
                    Decimal(i) + Decimal("0.25"),
                    i / 2,
                    "2026-01-02",
                ],
            )
        self.con.execute("INSERT INTO pg.public.events VALUES ('a', 'x'), ('a', 'x'), ('b', 'y')")
        self.con.execute("SET TimeZone = 'UTC'")
        self.con.execute(
            "CREATE TABLE pg.public.stamps (id INTEGER, ref UUID, on_day DATE, at_time TIME, "
            "at_tz TIMESTAMPTZ, flag BOOLEAN, doc VARCHAR)"
        )
        self.con.execute(
            "INSERT INTO pg.public.stamps VALUES (1, '12345678-1234-1234-1234-123456789abc', "
            "'2026-03-04', '10:30:00', '2026-03-04 10:30:00+00', TRUE, '{}'), "
            "(2, NULL, NULL, NULL, NULL, FALSE, NULL)"
        )

    async def govern(self, sql, role_id, *, exec_params=None, state=None, **_kwargs):
        ctx = self.surfaces[role_id].context
        gov = build_governance_context(
            role_id,
            RLSContext(rules=self.rls.get(role_id, {})),
            self.masks,
            ctx,
            _visible(role_id),
            _ROLES[role_id],
            RELATIONSHIPS,
        )
        violations = validate_sql(
            sql, ctx, gov, _ROLES[role_id], _visible(role_id), bypass_relationship_guard=True
        )
        if violations:
            raise PermissionError("; ".join(v.message for v in violations))
        lowered = lower_to_catalog_physical(apply_governance(sql, gov), ctx)
        self.statements.append(lowered)
        self.bound.append(list(exec_params or []))
        native = sqlglot.transpile(lowered, read="postgres", write="duckdb")[0]
        return SimpleNamespace(sql=native, params=list(exec_params or []))

    async def execute(self, plan, state):
        # DuckDB's own Arrow, as its engine terminal hands the encoders (REQ-2194).
        from provisa.executor.drivers.duckdb_arrow import arrow_rows
        from provisa.executor.result import ArrowResult

        table = self.con.execute(plan.sql, plan.params).to_arrow_table()
        return ArrowResult(table, to_rows=arrow_rows)


@pytest.fixture
def service(monkeypatch):
    built = {role: _surface(role)[0] for role in _ROLES}
    masks = {(1, "clerk"): {"email": (_EMAIL_MASK, "varchar(200)")}}
    rls = {"clerk": {1: '"customer_id" = 1'}}
    state = RoleState(
        surfaces=dict(built),
        roles=_ROLES,
        schema_boot_id="boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        masking_rules=masks,
        tables=TABLES,
        approval_hook=None,
        kafka_table_configs={},
        source_types={"pg": "postgresql"},
    )
    pipeline = _Pipeline(built, masks, rls)
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", pipeline.govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", pipeline.execute)

    app = FastAPI()

    @app.middleware("http")
    async def _role(request: Request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.role = request.headers.get("x-role")
        return await call_next(request)

    app.include_router(odata_router.create_odata_router(state))
    app.add_middleware(ODataResponseHeaders)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    return SimpleNamespace(client=client, pipeline=pipeline)


def _as(role: str) -> dict[str, str]:
    return {"x-role": role}


async def _metadata(service, role: str) -> ET.Element:
    resp = await service.client.get("/data/odata/$metadata", headers=_as(role))
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("application/xml")
    return ET.fromstring(resp.text)


def _entity_type(root: ET.Element, name: str) -> ET.Element:
    found = [e for e in root.iter(f"{_EDM}EntityType") if e.get("Name") == name]
    assert len(found) == 1, [e.get("Name") for e in root.iter(f"{_EDM}EntityType")]
    return found[0]


def _names(root: ET.Element, tag: str) -> set[str | None]:
    return {e.get("Name") for e in root.iter(f"{_EDM}{tag}")}


# -- $metadata: a role sees only its own -----------------------------------------------------------


async def test_a_role_sees_only_its_tables_in_metadata(service):
    assert "salaries" in _names(await _metadata(service, "analyst"), "EntitySet")
    clerk = await _metadata(service, "clerk")
    assert _names(clerk, "EntitySet") == {"customers", "events", "orders"}
    assert "Salaries" not in _names(clerk, "EntityType")


async def test_a_role_sees_only_its_columns_in_metadata(service):
    def _properties(root: ET.Element) -> set:
        orders = next(
            e
            for e in root.iter(f"{_EDM}EntityType")
            if e.find(f"{_EDM}Key") is not None
            and "email" in {p.get("Name") for p in e.iter(f"{_EDM}Property")}
        )
        return {p.get("Name") for p in orders.iter(f"{_EDM}Property")}

    assert "margin" in _properties(await _metadata(service, "analyst"))
    clerk = await _metadata(service, "clerk")
    assert "margin" not in _properties(clerk)
    assert (
        "margin"
        not in (await service.client.get("/data/odata/$metadata", headers=_as("clerk"))).text
    )


async def test_a_relationship_into_a_table_the_role_cannot_see_is_absent(service):
    analyst = await _metadata(service, "analyst")
    clerk = await _metadata(service, "clerk")
    assert "salary" in _names(analyst, "NavigationProperty")
    assert "salary" not in _names(clerk, "NavigationProperty")


async def test_metadata_declares_keys_types_and_constraints(service):
    root = await _metadata(service, "clerk")
    orders = next(
        e
        for e in root.iter(f"{_EDM}EntityType")
        if "email" in {p.get("Name") for p in e.iter(f"{_EDM}Property")}
    )
    assert [r.get("Name") for r in orders.iter(f"{_EDM}PropertyRef")] == ["id"]
    types = {p.get("Name"): p for p in orders.iter(f"{_EDM}Property")}
    assert types["id"].get("Type") == "Edm.Int32"
    assert types["id"].get("Nullable") == "false"
    assert types["placedAt" if "placedAt" in types else "placed_at"].get("Type") == (
        "Edm.DateTimeOffset"
    )
    assert types["amount"].get("Type") == "Edm.Decimal"
    assert (types["amount"].get("Precision"), types["amount"].get("Scale")) == ("10", "2")

    (customer,) = [n for n in orders.iter(f"{_EDM}NavigationProperty")]
    assert customer.get("Name") == "customer"
    assert customer.get("Type").startswith("Provisa.")
    (constraint,) = customer.iter(f"{_EDM}ReferentialConstraint")
    assert constraint.get("ReferencedProperty") == "id"
    assert constraint.get("Property") in types


async def test_a_to_many_relationship_is_a_collection_with_no_constraint(service):
    root = await _metadata(service, "clerk")
    orders = next(n for n in root.iter(f"{_EDM}NavigationProperty") if n.get("Name") == "orders")
    assert orders.get("Type").startswith("Collection(Provisa.")
    assert list(orders.iter(f"{_EDM}ReferentialConstraint")) == []


def test_a_constraint_on_a_column_the_role_cannot_see_is_not_stated():
    surface, _ = _surface("clerk")
    ctx = surface.context
    del ctx.physical_to_gql[(1, "customer_id")]
    orders = next(e for e in entity_sets_without_key_check(surface) if e.name == "orders")
    assert [n.constraints for n in orders.navigations if n.name == "customer"] == [()]


def entity_sets_without_key_check(surface: RoleSurface):
    """The model of a surface whose context lost one column's name; its property is dropped too."""
    surface.context.aggregate_columns[1] = [
        c for c in surface.context.aggregate_columns[1] if c[0] != "customer_id"
    ]
    return entity_sets(surface)


async def test_each_entity_set_names_its_tables_domain(service):
    """The UI narrows the entity sets to the navbar's domain selection by this annotation; without
    it every set was listed whatever the selection."""
    root = await _metadata(service, "clerk")
    domains = {
        s.get("Name"): [
            a.get("String")
            for a in s.iter(f"{_EDM}Annotation")
            if a.get("Term") == "Provisa.Domain"
        ]
        for s in root.iter(f"{_EDM}EntitySet")
    }
    assert domains == {"customers": ["sales"], "events": ["sales"], "orders": ["sales"]}


async def test_the_domain_term_is_declared(service):
    root = await _metadata(service, "clerk")
    (term,) = [t for t in root.iter(f"{_EDM}Term") if t.get("Name") == "Domain"]
    assert (term.get("Type"), term.get("AppliesTo")) == ("Edm.String", "EntitySet")


def test_the_document_is_escaped_and_well_formed():
    surface, _ = _surface("analyst")
    assert ET.fromstring(metadata_document(entity_sets(surface))).tag.endswith("Edmx")


# -- reading ---------------------------------------------------------------------------------------


async def test_a_masked_column_stays_masked_in_a_read(service):
    resp = await service.client.get("/data/odata/orders", headers=_as("clerk"))
    assert resp.status_code == 200, resp.text
    rows = resp.json()["value"]
    assert rows and {r["email"] for r in rows} == {"***"}
    assert "real.example" not in resp.text


async def test_a_masked_key_stays_masked_and_the_read_still_pages(service, monkeypatch):
    """Pages are read in key order; a key masked for the role is read through its mask and the
    statement is not refused for ordering by it."""
    service.pipeline.masks[(1, "clerk")]["id"] = (
        MaskingRule(mask_type=MaskType.constant, value=0),
        "integer",
    )
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 2)
    first = await service.client.get("/data/odata/orders", headers=_as("clerk"))
    assert first.status_code == 200, first.text
    assert {r["id"] for r in first.json()["value"]} == {0}
    assert "@odata.nextLink" in first.json()


async def test_a_masked_column_of_a_table_with_no_key_stays_masked(service):
    service.pipeline.masks[(4, "clerk")] = {"note": (_EMAIL_MASK, "varchar")}
    resp = await service.client.get("/data/odata/events", headers=_as("clerk"))
    assert resp.status_code == 200, resp.text
    assert {r["note"] for r in resp.json()["value"]} == {"***"}


async def test_the_same_read_shows_the_value_to_a_role_with_no_mask(service):
    resp = await service.client.get("/data/odata/orders", headers=_as("analyst"))
    assert "user1@real.example" in {r["email"] for r in resp.json()["value"]}


async def test_a_masked_column_stays_masked_when_selected_alone(service):
    resp = await service.client.get(
        "/data/odata/orders", params={"$select": "email"}, headers=_as("clerk")
    )
    assert {r["email"] for r in resp.json()["value"]} == {"***"}
    assert all(set(r) == {"email"} for r in resp.json()["value"])


async def test_a_read_and_its_count_keep_to_the_row_rule(service):
    resp = await service.client.get(
        "/data/odata/orders", params={"$count": "true"}, headers=_as("clerk")
    )
    body = resp.json()
    assert len(body["value"]) == body["@odata.count"] == _ORDERS // 3
    bare = await service.client.get("/data/odata/orders/$count", headers=_as("clerk"))
    assert bare.text == str(_ORDERS // 3)
    assert bare.headers["content-type"].startswith("text/plain")


async def test_every_statement_went_through_the_pipeline(service):
    await service.client.get("/data/odata/orders", params={"$count": "true"}, headers=_as("clerk"))
    assert len(service.pipeline.statements) == 2


@pytest.mark.parametrize(
    ("path", "role"),
    [("salaries", "clerk"), ("nothing", "clerk"), ("salaries/$count", "clerk")],
)
async def test_a_table_the_role_cannot_see_is_not_found(service, path, role):
    resp = await service.client.get(f"/data/odata/{path}", headers=_as(role))
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"
    assert service.pipeline.statements == []


async def test_selecting_a_column_the_role_cannot_see_is_refused(service):
    resp = await service.client.get(
        "/data/odata/orders", params={"$select": "id,margin"}, headers=_as("clerk")
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_query_option"
    assert service.pipeline.statements == []


async def test_select_names_the_columns_and_the_context(service):
    resp = await service.client.get(
        "/data/odata/orders", params={"$select": "id,amount"}, headers=_as("analyst")
    )
    body = resp.json()
    assert body["@odata.context"] == "http://test/data/odata/$metadata#orders(id,amount)"
    assert set(body["value"][0]) == {"id", "amount"}
    assert body["value"][0]["amount"] == 1.25


async def test_top_and_skip_page_in_key_order(service):
    resp = await service.client.get(
        "/data/odata/orders", params={"$top": "3", "$skip": "2"}, headers=_as("analyst")
    )
    assert [r["id"] for r in resp.json()["value"]] == [3, 4, 5]
    assert "@odata.nextLink" not in resp.json()


async def test_top_zero_reads_no_row_and_still_counts(service):
    resp = await service.client.get(
        "/data/odata/orders", params={"$top": "0", "$count": "true"}, headers=_as("analyst")
    )
    assert resp.json()["value"] == []
    assert resp.json()["@odata.count"] == _ORDERS


async def test_following_next_links_reads_every_row_once(service, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 5)
    url = "/data/odata/orders?$select=id&$count=true"
    seen, pages = [], 0
    while url:
        body = (await service.client.get(url, headers=_as("analyst"))).json()
        seen += [r["id"] for r in body["value"]]
        assert body["@odata.count"] == _ORDERS
        url = body.get("@odata.nextLink")
        pages += 1
    assert seen == list(range(1, _ORDERS + 1))
    assert pages == 3


async def test_a_full_last_page_has_no_next_link(service, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", _ORDERS)
    body = (await service.client.get("/data/odata/orders", headers=_as("analyst"))).json()
    assert len(body["value"]) == _ORDERS
    assert "@odata.nextLink" not in body


async def test_a_next_link_carries_what_remains_of_top(service, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 5)
    body = (
        await service.client.get(
            "/data/odata/orders", params={"$top": "7", "$select": "id"}, headers=_as("analyst")
        )
    ).json()
    assert len(body["value"]) == 5
    parts = urlsplit(body["@odata.nextLink"])
    assert parts.path == "/data/odata/orders"
    assert parse_qs(parts.query) == {"$select": ["id"], "$top": ["2"], "$skip": ["5"]}
    following = await service.client.get(body["@odata.nextLink"], headers=_as("analyst"))
    assert [r["id"] for r in following.json()["value"]] == [6, 7]
    assert "@odata.nextLink" not in following.json()


async def test_a_next_link_is_the_address_the_client_used(service, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 5)
    headers = {**_as("analyst"), "x-forwarded-proto": "https", "x-forwarded-host": "data.example"}
    body = (await service.client.get("/data/odata/orders", headers=headers)).json()
    assert body["@odata.nextLink"].startswith("https://data.example/data/odata/orders?")


async def test_a_table_with_no_key_keeps_its_repeated_rows_and_a_stable_order(service):
    body = (await service.client.get("/data/odata/events", headers=_as("analyst"))).json()
    assert body["value"] == [
        {"kind": "a", "note": "x"},
        {"kind": "a", "note": "x"},
        {"kind": "b", "note": "y"},
    ]


# -- the service's conventions ---------------------------------------------------------------------


async def test_the_service_document_lists_the_roles_sets(service):
    resp = await service.client.get("/data/odata/", headers=_as("clerk"))
    body = resp.json()
    assert body["@odata.context"] == "http://test/data/odata/$metadata"
    assert body["value"] == [
        {"name": n, "kind": "EntitySet", "url": n} for n in ("customers", "events", "orders")
    ]
    assert (await service.client.get("/data/odata", headers=_as("clerk"))).json() == body


@pytest.mark.parametrize("path", ["", "/$metadata", "/orders", "/orders/$count", "/nothing"])
async def test_every_response_says_its_version(service, path):
    resp = await service.client.get(f"/data/odata{path}", headers=_as("clerk"))
    assert resp.headers["odata-version"] == "4.0"


async def test_a_read_is_minimal_metadata_json(service):
    resp = await service.client.get("/data/odata/orders", headers=_as("clerk"))
    assert resp.headers["content-type"] == "application/json;odata.metadata=minimal"
    assert resp.json()["@odata.context"].endswith("$metadata#orders")


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("$expand", "*"),
        ("$search", "id"),
        ("$apply", "id"),
        ("$compute", "id"),
        ("$levels", "id"),
    ],
)
async def test_an_option_not_served_is_refused_never_ignored(service, option, value):
    resp = await service.client.get(
        "/data/odata/orders", params={option: value}, headers=_as("analyst")
    )
    assert resp.status_code == 501
    assert service.pipeline.statements == []


@pytest.mark.parametrize(
    ("items", "status"),
    [
        ([("$top", "-1")], 400),
        ([("$top", "x")], 400),
        ([("$skip", "1.5")], 400),
        ([("$count", "yes")], 400),
        ([("$select", "id,")], 400),
        ([("$bogus", "1")], 400),
        ([("$top", "1"), ("$top", "2")], 400),
        ([("$format", "atom")], 406),
    ],
)
def test_an_option_it_cannot_read_is_refused(items, status):
    with pytest.raises(QueryOptionRefused) as refused:
        parse_query_options(items)
    assert refused.value.status == status


def test_a_custom_option_and_a_star_select_are_taken():
    options = parse_query_options([("sap-client", "1"), ("$select", "*"), ("$format", "json")])
    assert options.select is None


# -- authentication --------------------------------------------------------------------------------


async def test_no_role_is_a_401_with_a_challenge(service):
    resp = await service.client.get("/data/odata/orders")
    assert resp.status_code == 401
    assert resp.headers.get_list("www-authenticate") == [
        'Basic realm="Provisa"',
        'Bearer realm="Provisa"',
    ]
    assert resp.headers["odata-version"] == "4.0"


async def test_the_auth_middleware_refusal_carries_the_challenge():
    """A request with no credential is answered by the auth middleware, before any router."""

    class _Provider:
        token_validators = {"bearer": lambda token: None}

    app = FastAPI()
    app.add_middleware(AuthMiddleware, provider=_Provider())
    app.add_middleware(ODataResponseHeaders)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    resp = await client.get("/data/odata/orders")
    assert resp.status_code == 401
    assert 'Basic realm="Provisa"' in resp.headers.get_list("www-authenticate")


async def test_the_headers_stay_off_every_other_route():
    async def _inner(scope, receive, send):
        await JSONResponse({}, status_code=401)(scope, receive, send)

    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=ODataResponseHeaders(_inner)), base_url="http://test"
    )
    resp = await client.get("/data/odataish")
    assert "odata-version" not in resp.headers
    assert "www-authenticate" not in resp.headers


# -- Edm types -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        ("integer", "Edm.Int32"),
        ("bigint", "Edm.Int64"),
        ("int64", "Edm.Int64"),
        ("smallint", "Edm.Int16"),
        ("tinyint", "Edm.SByte"),
        ("real", "Edm.Single"),
        ("double precision", "Edm.Double"),
        ("boolean", "Edm.Boolean"),
        ("date", "Edm.Date"),
        ("time", "Edm.TimeOfDay"),
        ("time with time zone", "Edm.String"),
        ("timestamp", "Edm.DateTimeOffset"),
        ("timestamp with time zone", "Edm.DateTimeOffset"),
        ("uuid", "Edm.Guid"),
        ("bytea", "Edm.Binary"),
        ("varchar(20)", "Edm.String"),
        ("jsonb", "Edm.String"),
        ("array(integer)", "Edm.String"),
        ("interval", "Edm.String"),
        ("inet", "Edm.String"),
    ],
)
def test_a_column_type_is_its_edm_type(data_type, expected):
    assert edm_type(data_type).name == expected


def test_a_decimal_keeps_its_precision_and_scale():
    assert edm_type("decimal(10,2)") == EdmType("Edm.Decimal", 10, 2)
    assert edm_type("numeric(7)") == EdmType("Edm.Decimal", 7, 0)
    assert edm_type("numeric") == EdmType("Edm.Decimal", None, "variable")


@pytest.mark.parametrize(
    ("value", "type_name", "expected"),
    [
        (None, "Edm.Int32", None),
        (Decimal("7"), "Edm.Int64", 7),
        (datetime(2026, 1, 2, 3, 4, 5), "Edm.DateTimeOffset", "2026-01-02T03:04:05Z"),
        (
            datetime(2026, 1, 2, 3, tzinfo=timezone(timedelta(hours=2))),
            "Edm.DateTimeOffset",
            "2026-01-02T03:00:00+02:00",
        ),
        ("2026-01-02 03:04:05", "Edm.DateTimeOffset", "2026-01-02T03:04:05Z"),
        (date(2026, 1, 2), "Edm.Date", "2026-01-02"),
        (datetime(2026, 1, 2, 3), "Edm.Date", "2026-01-02"),
        (b"\xfb\xff", "Edm.Binary", "-_8="),
        ("\\xfbff", "Edm.Binary", "-_8="),
        ('{"a": 1}', "Edm.String", '{"a": 1}'),
        ({"a": 1}, "Edm.String", '{"a":1}'),
        (float("nan"), "Edm.Double", "NaN"),
        (float("inf"), "Edm.Double", "INF"),
    ],
)
def test_a_value_is_written_as_its_edm_type(value, type_name, expected):
    assert json.loads(_written(value, type_name), parse_float=Decimal)["value"][0]["v"] == expected


def _written(value, type_name) -> bytes:
    """A one-row read of ``value`` (as a row engine's fetch boundary holds it) written by the
    OData encoder as a property of ``type_name`` (REQ-2194)."""
    from provisa.core.ir_arrow import row_batch
    from provisa.executor import json_encoders as je

    entity = ("T", {"v": type_name}, ["v"], [])
    return je.odata(
        [row_batch([(value,)], ["v"])],
        ["v"],
        entity,
        context="c",
        count=None,
        page=1,
        next_link="",
        root="r",
    )


def test_a_decimal_is_written_exactly():
    exact = Decimal("12345678901234567890.123456789")
    assert b'"v":' + str(exact).encode() + b"}" in _written(exact, "Edm.Decimal")


# -- registration ----------------------------------------------------------------------------------


def test_the_app_mounts_the_service_with_its_headers_outside_auth(monkeypatch, tmp_path):
    import provisa.api.app as app_mod

    config = tmp_path / "provisa.yaml"
    config.write_text("{}\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(config))
    app = app_mod.create_app()

    paths = {getattr(route, "path", None) for route in app.routes}
    assert {
        "/data/odata",
        "/data/odata/",
        "/data/odata/$metadata",
        "/data/odata/{entity_set_name}",
        "/data/odata/{entity_set_name}/$count",
    } <= paths
    # A middleware added later runs outside those added before it; user_middleware is outermost
    # first.
    order = [m.cls.__name__ for m in app.user_middleware]
    assert order.index("ODataResponseHeaders") < order.index("AuthMiddleware")


# -- $filter ---------------------------------------------------------------------------------------


async def _ids(service, expression: str, role: str = "analyst", **options: str) -> list[int]:
    params = {"$filter": expression, "$select": "id", **options}
    resp = await service.client.get("/data/odata/orders", params=params, headers=_as(role))
    assert resp.status_code == 200, resp.text
    return [r["id"] for r in resp.json()["value"]]


async def _refused(service, expression: str, role: str = "analyst") -> httpx.Response:
    return await service.client.get(
        "/data/odata/orders", params={"$filter": expression}, headers=_as(role)
    )


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("id eq 3", [3]),
        ("id ne 3", [i for i in range(1, _ORDERS + 1) if i != 3]),
        ("id gt 10", [11, 12]),
        ("id ge 11", [11, 12]),
        ("id lt 2", [1]),
        ("id le 2", [1, 2]),
        ("3 eq id", [3]),
        ("10 lt id", [11, 12]),
        ("id gt 2 and id lt 5", [3, 4]),
        ("id eq 1 or id eq 12", [1, 12]),
        ("not (id gt 2)", [1, 2]),
        ("not id gt 2", [1, 2]),
        ("id eq 1 or id eq 2 and id eq 3", [1]),  # and binds tighter than or
        ("(id eq 1 or id eq 2) and id eq 2", [2]),
        ("((id eq 1))", [1]),
        ("not not id eq 1", [1]),
        ("id in (2, 4)", [2, 4]),
        ("email in ('user1@real.example', 'nobody')", [1]),
        ("amount eq 1.25", [1]),
        ("amount gt 11.25", [12]),
        ("amount lt 1.5e0", [1]),
        ("margin eq 0.5", [1]),
        ("email eq 'user3@real.example'", [3]),
        ("contains(email, 'r12')", [12]),
        ("startswith(email, 'user12')", [12]),
        ("endswith(email, '12@real.example')", [12]),
        ("tolower(email) eq 'user2@real.example'", [2]),
        ("toupper(email) eq 'USER2@REAL.EXAMPLE'", [2]),
        ("contains(toupper(email), 'R9@')", [9]),
        ("length(email) eq 18 and id eq 1", [1]),
        ("length(email) gt 18 and id lt 11", [10]),
        ("year(placedAt) eq 2027", []),
        ("year(placedAt) eq 2026 and id eq 1", [1]),
        ("month(placedAt) eq 1 and id eq 1", [1]),
        ("day(placedAt) eq 2 and id eq 1", [1]),
        ("placedAt eq 2026-01-02T00:00:00Z and id eq 1", [1]),
        ("placedAt eq 2026-01-02 and id eq 1", [1]),
        ("placedAt lt 2026-01-02T00:00:00Z", []),
        ("placedAt lt 2026-01-02T00:30:00+01:00", []),  # an offset moves the instant
        ("placedAt eq 2026-01-02T01:00:00+01:00 and id eq 1", [1]),
    ],
)
async def test_each_operator_of_a_filter(service, expression, expected):
    assert await _ids(service, expression) == expected


async def _with_nulls(service) -> None:
    service.pipeline.con.execute(
        "UPDATE pg.public.orders SET email = NULL, customer_id = NULL WHERE id = 1"
    )


async def test_null_is_compared_by_eq_and_ne(service):
    await _with_nulls(service)
    assert await _ids(service, "email eq null") == [1]
    assert await _ids(service, "null eq email") == [1]
    assert await _ids(service, "email ne null and id le 2") == [2]


async def test_a_null_is_not_equal_to_a_value(service):
    await _with_nulls(service)
    assert await _ids(service, "email ne 'user2@real.example' and id le 2") == [1]
    assert await _ids(service, "not (email eq 'user2@real.example') and id le 2") == [1]


async def test_two_null_columns_are_equal(service):
    await _with_nulls(service)
    assert await _ids(service, "email eq email and id le 2") == [1, 2]


async def test_in_holds_for_null_when_it_lists_null(service):
    await _with_nulls(service)
    assert await _ids(service, "email in ('user2@real.example', null)") == [1, 2]
    assert await _ids(service, "email in (null)") == [1]


async def _stamp_ids(service, expression: str) -> list[int]:
    resp = await service.client.get(
        "/data/odata/stamps", params={"$filter": expression}, headers=_as("analyst")
    )
    assert resp.status_code == 200, resp.text
    return [r["id"] for r in resp.json()["value"]]


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("ref eq 12345678-1234-1234-1234-123456789abc", [1]),
        ("ref ne 12345678-1234-1234-1234-123456789ABC", [2]),
        ("onDay eq 2026-03-04", [1]),
        ("onDay gt 2026-03-03", [1]),
        ("onDay lt 2026-03-03", []),
        ("year(onDay) eq 2026", [1]),
        ("month(onDay) eq 3", [1]),
        ("day(onDay) eq 4", [1]),
        ("atTime eq 10:30:00", [1]),
        ("atTime lt 11:00", [1]),
        ("atTz eq 2026-03-04T10:30:00Z", [1]),
        ("atTz eq 2026-03-04T12:30:00+02:00", [1]),
        ("atTz gt 2026-03-04T12:30:00+02:00", []),
        ("flag", [1]),
        ("not flag", [2]),
        ("flag eq true", [1]),
        ("flag eq false", [2]),
        ("flag and id eq 1", [1]),
        ("true", [1, 2]),
        ("false", []),
        ("id eq 1 and true", [1]),
        ("ref eq null", [2]),
        ("doc eq null", [2]),
    ],
)
async def test_a_typed_literal_compares_as_its_type(service, expression, expected):
    assert await _stamp_ids(service, expression) == expected


@pytest.mark.parametrize(
    "expression",
    ["ref eq 'x'", "onDay eq 2026-03-04T10:30:00Z", "atTime eq 2026-03-04", "flag eq 1"],
)
async def test_a_literal_of_another_type_is_a_400(service, expression):
    resp = await service.client.get(
        "/data/odata/stamps", params={"$filter": expression}, headers=_as("analyst")
    )
    assert resp.status_code == 400, resp.text


async def test_filtering_on_a_type_with_no_edm_primitive_is_a_501_naming_it(service):
    """A JSON column is served as a string but is no string to the engine that holds it."""
    resp = await service.client.get(
        "/data/odata/stamps", params={"$filter": "doc eq '{}'"}, headers=_as("analyst")
    )
    assert resp.status_code == 501
    assert "doc" in resp.json()["error"]["message"]


async def test_a_doubled_quote_is_one_quote_in_the_value(service):
    service.pipeline.con.execute("UPDATE pg.public.orders SET email = 'o''brien' WHERE id = 1")
    assert await _ids(service, "email eq 'o''brien'") == [1]
    assert await _ids(service, "contains(email, '''b')") == [1]


@pytest.mark.parametrize(
    "payload",
    [
        "x' OR '1'='1",
        "'; DROP TABLE orders; --",
        "' UNION SELECT margin FROM orders --",
        "$1",
        "\\",
    ],
)
async def test_an_injection_in_a_string_literal_stays_a_value(service, payload):
    written = payload.replace("'", "''")
    for expression in (
        f"email eq '{written}'",
        f"contains(email, '{written}')",
        f"startswith(email, '{written}')",
        f"endswith(email, '{written}')",
        f"tolower(email) eq '{written}'",
        f"email in ('{written}')",
    ):
        assert await _ids(service, expression) == []
        statement = service.pipeline.statements[-1]
        assert "DROP" not in statement and "UNION" not in statement
        assert "1'='1" not in statement


async def test_a_value_is_bound_never_in_the_statements_text(service):
    await _ids(service, "email eq 'marker-value' and contains(email, 'other-marker')")
    assert "marker" not in service.pipeline.statements[-1]
    assert sorted(service.pipeline.bound[-1][-2:]) == ["%other-marker%", "marker-value"]


async def test_a_wildcard_in_a_search_is_the_character(service):
    assert await _ids(service, "contains(email, '%')") == []
    assert await _ids(service, "contains(email, '_')") == []
    assert await _ids(service, "startswith(email, 'user_')") == []
    service.pipeline.con.execute("UPDATE pg.public.orders SET email = '50%_!off' WHERE id = 1")
    assert await _ids(service, "contains(email, '0%_!o')") == [1]


@pytest.mark.parametrize(
    ("expression", "named"),
    [
        ("substring(email, 1) eq 'x'", "substring"),
        ("indexof(email, 'x') eq 1", "indexof"),
        ("concat(email, 'x') eq 'x'", "concat"),
        ("trim(email) eq 'x'", "trim"),
        ("hour(placedAt) eq 1", "hour"),
        ("round(amount) eq 1", "round"),
        ("now() gt placedAt", "now"),
        ("isof(id, Edm.Int32)", "isof"),
        ("bogus(id)", "bogus"),
        ("not bogus(id)", "bogus"),
        ("id eq 1 and (bogus(id) or id eq 2)", "bogus"),
        ("tolower(bogus(email)) eq 'x'", "bogus"),
    ],
)
async def test_a_function_not_served_is_a_501_naming_it(service, expression, named):
    resp = await _refused(service, expression)
    assert resp.status_code == 501, resp.text
    assert resp.json()["error"]["code"] == "not_implemented"
    assert named in resp.json()["error"]["message"]
    assert service.pipeline.statements == []


async def test_a_search_for_anything_but_a_literal_is_a_501_naming_it(service):
    resp = await _refused(service, "contains(email, email)")
    assert resp.status_code == 501
    assert "contains" in resp.json()["error"]["message"]


@pytest.mark.parametrize(
    ("expression", "named"),
    [
        ("id add 1 eq 2", "add"),
        ("(id mul 2) eq 2", "mul"),
        ("id mod 2 eq 0", "mod"),
        ("customer/name eq 'x'", "customer"),
        ("orders/any(o: o/id eq 1)", "orders"),
        ("email eq duration'P1D'", "duration"),
        ("Provisa.fn(id)", "Provisa.fn"),
    ],
)
async def test_a_construct_not_served_is_a_501_naming_it(service, expression, named):
    resp = await _refused(service, expression)
    assert resp.status_code == 501, resp.text
    assert named in resp.json()["error"]["message"]


@pytest.mark.parametrize(
    "expression",
    [
        "",
        "id",
        "id eq",
        "eq 1",
        "id == 1",
        "id eq 1 and",
        "(id eq 1",
        "id eq 1)",
        "id eq 'unterminated",
        "ID eq 1",
        "id eq 1 AND id eq 2",
        "id eq 2026-13-45",
        "placedAt eq 2026-01-02T25:00:00Z",
        "id in ()",
        "id in 1",
        "contains(email)",
        "tolower(email, 'x') eq 'x'",
        "id eq 'x'",
        "email eq 1",
        "email gt 2026-01-02",
        "placedAt eq 'x'",
        "contains(id, 'x')",
        "year(email) eq 1",
        "length(id) eq 1",
        "tolower(id) eq 'x'",
        "id gt null",
        "id : 1",
        "id and email",
        "1",
        "'a'",
    ],
)
async def test_text_that_is_no_filter_is_a_400(service, expression):
    resp = await _refused(service, expression)
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"]["code"] == "invalid_query_option"
    assert service.pipeline.statements == []


async def test_a_bound_on_nesting_and_on_length(service):
    assert (await _refused(service, "(" * 200 + "id eq 1" + ")" * 200)).status_code == 400
    assert (await _refused(service, "not " * 200 + "id eq 1")).status_code == 400
    assert (await _refused(service, " or ".join(["id eq 1"] * 1500))).status_code == 400


async def test_the_deepest_and_longest_filter_served_is_answered(service):
    assert await _ids(service, "(" * 60 + "id eq 1" + ")" * 60) == [1]
    assert await _ids(service, " or ".join(f"id eq {i % 2 + 1}" for i in range(900))) == [1, 2]


async def test_filtering_on_a_column_the_role_cannot_see_is_as_for_no_column(service):
    hidden = await _refused(service, "margin gt 0", role="clerk")
    missing = await _refused(service, "nothing gt 0", role="clerk")
    assert hidden.status_code == missing.status_code == 400
    assert hidden.json()["error"]["code"] == missing.json()["error"]["code"]
    assert service.pipeline.statements == []
    assert await _ids(service, "margin gt 5.5") == [12]


async def test_a_filter_keeps_to_the_row_rule(service):
    """The clerk's rule is customer_id = 1: ids 1, 4, 7, 10."""
    assert await _ids(service, "id le 4", role="clerk") == [1, 4]
    assert await _ids(service, "id eq 2", role="clerk") == []


async def test_an_or_cannot_widen_past_the_row_rule(service):
    assert await _ids(service, "id gt 0 or id lt 0", role="clerk") == [1, 4, 7, 10]
    assert await _ids(service, "true or id eq 2", role="clerk") == [1, 4, 7, 10]


async def test_a_filter_is_one_statement(service):
    await _ids(service, "id gt 1")
    assert len(service.pipeline.statements) == 1
    assert "WHERE" in service.pipeline.statements[0]


# -- a masked column in a filter: what REST and GraphQL do, V005 on the compiled path (REQ-531) -----


async def test_a_filter_on_a_masked_column_is_refused_and_runs_nothing(service):
    """A filter on the real value would recover it one comparison at a time; the shown value is
    not what the clause reads, so the statement is refused before it runs."""
    for expression in (
        "email eq 'user1@real.example'",
        "email eq '***'",
        "contains(email, 'user')",
        "tolower(email) eq 'x'",
        "not (email eq 'x')",
        "id gt 0 or email eq 'x'",
    ):
        resp = await _refused(service, expression, role="clerk")
        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["code"] == "forbidden"
        assert "email" in resp.json()["error"]["message"]
    assert service.pipeline.statements == []


async def test_the_same_filter_reads_for_a_role_with_no_mask(service):
    assert await _ids(service, "email eq 'user1@real.example'") == [1]


async def test_a_count_with_a_filter_on_a_masked_column_is_refused(service):
    resp = await service.client.get(
        "/data/odata/orders/$count",
        params={"$filter": "email eq 'x'"},
        headers=_as("clerk"),
    )
    assert resp.status_code == 403


@pytest.fixture
def faked(service, monkeypatch):
    """The clerk reads email through a fake: its filter is on the fake the role is shown."""
    monkeypatch.setattr(digest_mod, "_key", b"k" * 32)
    register(service.pipeline.con)
    rule = MaskingRule(MaskType.fake, fake="email()")
    service.pipeline.masks[(1, "clerk")] = {"email": (rule, "varchar(200)")}
    return service


async def test_a_filter_on_a_faked_column_compares_what_the_role_is_shown(faked):
    shown = await faked.client.get(
        "/data/odata/orders", params={"$select": "id,email"}, headers=_as("clerk")
    )
    assert shown.status_code == 200, shown.text
    row = shown.json()["value"][0]
    assert row["email"] != f"user{row['id']}@real.example"

    assert await _ids(faked, f"email eq '{row['email']}'", role="clerk") == [row["id"]]
    assert await _ids(faked, f"email eq 'user{row['id']}@real.example'", role="clerk") == []


# -- $orderby --------------------------------------------------------------------------------------


async def _ordered(service, order: str, role: str = "analyst", **options: str) -> list[int]:
    params = {"$orderby": order, "$select": "id", **options}
    resp = await service.client.get("/data/odata/orders", params=params, headers=_as(role))
    assert resp.status_code == 200, resp.text
    return [r["id"] for r in resp.json()["value"]]


async def test_orderby_ascending_and_descending(service):
    assert await _ordered(service, "id desc") == list(range(_ORDERS, 0, -1))
    assert await _ordered(service, "id asc") == list(range(1, _ORDERS + 1))
    assert await _ordered(service, "id") == list(range(1, _ORDERS + 1))


async def test_orderby_many_keys_in_the_order_given(service):
    ids = await _ordered(service, "customerId desc,id desc")
    expected = sorted(range(1, _ORDERS + 1), key=lambda i: (-(i % 3), -i))
    assert ids == expected


async def test_orderby_a_tie_is_broken_by_the_key_so_pages_hold(service):
    assert await _ordered(service, "placedAt desc") == list(range(1, _ORDERS + 1))


async def test_orderby_a_null_is_first_ascending_and_last_descending(service):
    await _with_nulls(service)
    assert (await _ordered(service, "customerId"))[0] == 1
    assert (await _ordered(service, "customerId desc"))[-1] == 1


async def test_orderby_on_a_masked_column_sorts_what_the_role_is_shown(service):
    """The email mask is one constant, so no order of the real values shows through."""
    assert await _ordered(service, "email desc", role="clerk") == [1, 4, 7, 10]
    real = await _ordered(service, "email desc")
    assert real != list(range(1, _ORDERS + 1))


async def test_orderby_a_column_the_role_cannot_see_is_as_for_no_column(service):
    for name in ("margin", "nothing"):
        resp = await service.client.get(
            "/data/odata/orders", params={"$orderby": name}, headers=_as("clerk")
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "invalid_query_option"
    assert service.pipeline.statements == []


@pytest.mark.parametrize(
    ("text", "status", "named"),
    [
        ("", 400, ""),
        ("id sideways", 400, ""),
        ("id,", 400, ""),
        ("id id", 400, ""),
        ("id desc,id asc", 400, "id"),
        ("1", 400, ""),
        ("tolower(email) desc", 501, "tolower"),
        ("customer/name", 501, "customer"),
    ],
)
async def test_an_orderby_it_cannot_read_is_refused(service, text, status, named):
    resp = await service.client.get(
        "/data/odata/orders", params={"$orderby": text}, headers=_as("analyst")
    )
    assert resp.status_code == status, resp.text
    assert named in resp.json()["error"]["message"]
    assert service.pipeline.statements == []


# -- paging and counting with a filter -------------------------------------------------------------


async def test_a_count_is_of_the_filtered_rows(service):
    resp = await service.client.get(
        "/data/odata/orders",
        params={"$filter": "id gt 4", "$count": "true", "$top": "2", "$select": "id"},
        headers=_as("analyst"),
    )
    assert resp.json()["@odata.count"] == _ORDERS - 4
    assert [r["id"] for r in resp.json()["value"]] == [5, 6]
    assert len(service.pipeline.statements) == 2


async def test_the_count_path_takes_a_filter(service):
    resp = await service.client.get(
        "/data/odata/orders/$count", params={"$filter": "id gt 4"}, headers=_as("analyst")
    )
    assert resp.text == str(_ORDERS - 4)


async def test_a_count_with_a_filter_keeps_to_the_row_rule(service):
    resp = await service.client.get(
        "/data/odata/orders/$count", params={"$filter": "id gt 4"}, headers=_as("clerk")
    )
    assert resp.text == "2"


async def test_following_next_links_keeps_the_filter_and_the_order(service, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 2)
    expression = "contains(email, '@real') and (id gt 3 or email eq 'o''brien & co+1')"
    resp = await service.client.get(
        "/data/odata/orders",
        params={
            "$filter": expression,
            "$orderby": "id desc",
            "$select": "id",
            "$count": "true",
        },
        headers=_as("analyst"),
    )
    body = resp.json()
    parts = urlsplit(body["@odata.nextLink"])
    query = parse_qs(parts.query)
    assert query["$filter"] == [expression]
    assert query["$orderby"] == ["id desc"]
    assert query["$skip"] == ["2"]

    seen, pages = [r["id"] for r in body["value"]], 1
    url = body["@odata.nextLink"]
    while url:
        body = (await service.client.get(url, headers=_as("analyst"))).json()
        assert body["@odata.count"] == _ORDERS - 3
        seen += [r["id"] for r in body["value"]]
        url = body.get("@odata.nextLink")
        pages += 1
    assert seen == list(range(_ORDERS, 3, -1))
    assert pages == 5


async def test_a_filtered_page_that_ends_the_rows_has_no_next_link(service, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 2)
    body = (
        await service.client.get(
            "/data/odata/orders", params={"$filter": "id gt 10"}, headers=_as("analyst")
        )
    ).json()
    assert [r["id"] for r in body["value"]] == [11, 12]
    assert "@odata.nextLink" not in body


async def test_a_next_link_under_a_row_rule_holds_the_filter(service, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 1)
    url = "/data/odata/orders?$filter=id%20gt%201&$select=id"
    seen = []
    while url:
        body = (await service.client.get(url, headers=_as("clerk"))).json()
        seen += [r["id"] for r in body["value"]]
        url = body.get("@odata.nextLink")
    assert seen == [4, 7, 10]


async def test_top_with_a_filter_carries_what_remains(service, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 2)
    body = (
        await service.client.get(
            "/data/odata/orders",
            params={"$filter": "id gt 2", "$top": "3", "$select": "id"},
            headers=_as("analyst"),
        )
    ).json()
    assert [r["id"] for r in body["value"]] == [3, 4]
    following = await service.client.get(body["@odata.nextLink"], headers=_as("analyst"))
    assert [r["id"] for r in following.json()["value"]] == [5]
    assert "@odata.nextLink" not in following.json()


# -- parsing ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "tree"),
    [
        ("a eq 1", Comparison("eq", Name("a"), Literal("number", 1))),
        ("a eq -1", Comparison("eq", Name("a"), Literal("number", -1))),
        ("a eq 1.50", Comparison("eq", Name("a"), Literal("number", Decimal("1.50")))),
        ("a eq 1e3", Comparison("eq", Name("a"), Literal("number", 1000.0))),
        ("a eq INF", Comparison("eq", Name("a"), Literal("number", float("inf")))),
        ("a eq -INF", Comparison("eq", Name("a"), Literal("number", float("-inf")))),
        ("a eq 'it''s'", Comparison("eq", Name("a"), Literal("string", "it's"))),
        ("a eq ''", Comparison("eq", Name("a"), Literal("string", ""))),
        ("a eq 2026-01-02", Comparison("eq", Name("a"), Literal("date", date(2026, 1, 2)))),
        ("a eq null", Comparison("eq", Name("a"), Literal("null", None))),
        ("a eq true", Comparison("eq", Name("a"), Literal("boolean", True))),
        (
            "a eq 2026-01-02T03:04:05.1234567Z",
            Comparison(
                "eq",
                Name("a"),
                Literal("datetime", datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)),
            ),
        ),
        ("a or b and c", Logical("or", (Name("a"), Logical("and", (Name("b"), Name("c")))))),
        ("a or b or c", Logical("or", (Name("a"), Name("b"), Name("c")))),
        ("(a or b) and c", Logical("and", (Logical("or", (Name("a"), Name("b"))), Name("c")))),
        ("not a and b", Logical("and", (Not(Name("a")), Name("b")))),
        ("a in (1)", In(Name("a"), (Literal("number", 1),))),
        ("tolower(a)", Call("tolower", (Name("a"),))),
        ("  a   eq\t1 ", Comparison("eq", Name("a"), Literal("number", 1))),
    ],
)
def test_the_tree_of_an_expression(text, tree):
    assert parse_filter(text) == tree


def test_the_keys_of_an_orderby():
    assert parse_orderby("a, b desc ,c asc") == (
        OrderKey("a"),
        OrderKey("b", True),
        OrderKey("c"),
    )


def test_nan_is_a_number():
    parsed = parse_filter("a eq NaN")
    assert isinstance(parsed, Comparison)
    assert isinstance(parsed.right, Literal) and math.isnan(parsed.right.value)


# -- $expand ---------------------------------------------------------------------------------------


@pytest.fixture
def expanding(service):
    """Customers 0, 1 and 2, whose orders are the ids with that remainder of 3; customer 9 has
    none; customer 2's name is masked for the clerk, whose row rule keeps customer 1's orders."""
    con = service.pipeline.con
    con.execute("CREATE MACRO json_arrayagg(x) AS to_json(list(x))")
    con.execute(
        "INSERT INTO pg.public.customers VALUES (0, 'zero'), (1, 'one'), (2, 'two'), (9, 'none')"
    )
    con.execute("INSERT INTO pg.public.salaries VALUES (1, 100)")
    return service


async def _expand(service, role: str = "analyst", path: str = "customers", **options: str) -> dict:
    params = {f"${name}": value for name, value in options.items()}
    resp = await service.client.get(f"/data/odata/{path}", params=params, headers=_as(role))
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _expand_refused(service, expansion: str, role: str = "analyst", path: str = "customers"):
    return await service.client.get(
        f"/data/odata/{path}", params={"$expand": expansion}, headers=_as(role)
    )


async def test_a_to_many_expand_nests_each_parents_rows(expanding):
    body = await _expand(expanding, expand="orders($select=id)", select="id,name", orderby="id")
    by_id = {r["id"]: r for r in body["value"]}
    assert [o["id"] for o in by_id[1]["orders"]] == [1, 4, 7, 10]
    assert [o["id"] for o in by_id[2]["orders"]] == [2, 5, 8, 11]
    assert by_id[1]["name"] == "one"
    assert by_id[9]["orders"] == []
    assert set(by_id[1]) == {"id", "name", "orders"}


async def test_a_to_one_expand_nests_one_object_or_null(expanding):
    expanding.pipeline.con.execute("UPDATE pg.public.orders SET customer_id = NULL WHERE id = 3")
    body = await _expand(expanding, path="orders", expand="customer", select="id", filter="id lt 5")
    by_id = {r["id"]: r["customer"] for r in body["value"]}
    assert by_id[1] == {"id": 1, "name": "one"}
    assert by_id[3] is None


async def test_expanded_values_are_written_as_their_types(expanding):
    body = await _expand(expanding, expand="orders($top=1;$select=amount,placedAt,margin)")
    (order,) = next(r for r in body["value"] if r["id"] == 1)["orders"]
    assert order["amount"] == 1.25
    assert order["placedAt"] == "2026-01-02T00:00:00Z"
    assert order["margin"] == 0.5


def _expansion_read(key, key_type: str, orders: str, order_types: dict[str, str]) -> bytes:
    """A one-row read of ``T`` keyed by ``key`` expanding its ``orders`` (the subquery's JSON
    text) a page of one, written by the OData encoder (REQ-2194)."""
    from provisa.core.ir_arrow import row_batch
    from provisa.executor import json_encoders as je

    target = ("O", order_types, list(order_types), [])
    entity = ("T", {"k": key_type}, ["k"], [("orders", True, 1, ["k"], "?$skip=1", target)])
    return je.odata(
        [row_batch([(key, orders)], ["k", "orders"])],
        ["k", "orders"],
        entity,
        context="c",
        count=None,
        page=1,
        next_link="",
        root="r",
    )


def _key_in_next_link(value, type_name: str) -> str:
    """The key literal the encoder addresses a parent by in its expansion's nextLink."""
    from urllib.parse import unquote

    body = json.loads(
        _expansion_read(value, type_name, '[{"id": 1}, {"id": 2}]', {"id": "Edm.Int32"})
    )
    link = body["value"][0]["orders@odata.nextLink"]
    assert link.startswith("r/T(") and link.endswith(")/orders?$skip=1"), link
    return unquote(link[len("r/T(") : -len(")/orders?$skip=1")])


def test_a_decimal_in_an_expansion_stays_exact():
    body = _expansion_read(1, "Edm.Int32", '[{"a": 12345678901234567.25}]', {"a": "Edm.Decimal"})
    assert b'"orders":[{"a":12345678901234567.25}]' in body


async def test_a_nested_select_writes_only_what_it_names(expanding):
    body = await _expand(expanding, expand="orders($select=amount;$top=1)", filter="id eq 1")
    assert list(body["value"][0]["orders"][0]) == ["amount"]


async def test_the_context_states_a_nested_select(expanding):
    narrowed = await _expand(expanding, expand="orders($select=id)", select="name")
    assert narrowed["@odata.context"].endswith("$metadata#customers(name,orders(id))")
    whole = await _expand(expanding, expand="orders($select=id)")
    assert whole["@odata.context"].endswith("$metadata#customers(*,orders(id))")
    plain = await _expand(expanding, expand="orders")
    assert plain["@odata.context"].endswith("$metadata#customers")


async def test_a_nested_filter_order_and_top_are_applied_per_parent(expanding):
    body = await _expand(
        expanding,
        expand="orders($filter=id gt 2 and contains(email, '@real');$orderby=id desc;"
        "$top=2;$select=id)",
        filter="id le 2",
    )
    by_id = {r["id"]: [o["id"] for o in r["orders"]] for r in body["value"]}
    assert by_id == {0: [12, 9], 1: [10, 7], 2: [11, 8]}


async def test_an_expansion_is_one_statement_and_its_filter_is_bound(expanding):
    await _expand(expanding, expand="orders($filter=email eq 'marker-value')")
    assert len(expanding.pipeline.statements) == 1
    assert "marker" not in expanding.pipeline.statements[0]
    assert expanding.pipeline.bound[0][-1] == "marker-value"


async def test_a_quote_or_semicolon_in_a_nested_string_is_the_value(expanding):
    con = expanding.pipeline.con
    con.execute("UPDATE pg.public.orders SET email = 'a;b''c(' WHERE id = 1")
    body = await _expand(
        expanding, expand="orders($filter=email eq 'a;b''c(';$select=id)", filter="id eq 1"
    )
    assert body["value"][0]["orders"] == [{"id": 1}]


async def test_an_or_in_a_nested_filter_cannot_widen_to_another_parent(expanding):
    body = await _expand(expanding, expand="orders($filter=id gt 0 or id lt 0;$select=id)")
    assert [o["id"] for o in next(r for r in body["value"] if r["id"] == 1)["orders"]] == [
        1,
        4,
        7,
        10,
    ]


async def test_two_to_many_siblings_do_not_multiply(expanding):
    """Each is its own subquery of the parent's one row: joined, each row of one would repeat
    every row of the other."""
    body = await _expand(
        expanding,
        expand="orders($select=id;$top=3),ordersAgain($select=id;$orderby=id desc)",
        select="id",
    )
    assert len(body["value"]) == 4
    first = next(r for r in body["value"] if r["id"] == 1)
    assert [o["id"] for o in first["orders"]] == [1, 4, 7]
    assert [o["id"] for o in first["ordersAgain"]] == [10, 7, 4, 1]


async def test_a_to_one_and_a_to_many_nest_together(expanding):
    body = await _expand(
        expanding, path="orders", expand="customer($expand=orders($select=id)),salary"
    )
    assert len(body["value"]) == _ORDERS
    first = next(r for r in body["value"] if r["id"] == 1)
    assert first["salary"] == {"id": 1, "pay": 100}
    assert [o["id"] for o in first["customer"]["orders"]] == [1, 4, 7, 10]


async def test_an_expansion_nests_at_depth(expanding):
    body = await _expand(
        expanding,
        expand="orders($select=id;$top=1;$expand=customer($select=name;"
        "$expand=orders($select=id;$orderby=id desc;$top=1)))",
        filter="id eq 1",
    )
    assert body["value"][0]["orders"] == [
        {"id": 1, "customer": {"name": "one", "orders": [{"id": 10}]}}
    ]


async def test_expansions_past_the_depth_are_refused(expanding):
    deep = "customer"
    for navigation in ("orders", "customer", "orders", "customer"):
        deep = f"{navigation}($expand={deep})"
    resp = await _expand_refused(expanding, deep)
    assert resp.status_code == 400
    assert expanding.pipeline.statements == []


async def test_expand_with_root_filter_order_and_paging(expanding, monkeypatch):
    monkeypatch.setattr(odata_router, "SERVER_PAGE_SIZE", 2)
    body = await _expand(
        expanding, expand="orders($select=id;$top=1)", filter="id lt 9", orderby="id desc"
    )
    assert [r["id"] for r in body["value"]] == [2, 1]
    following = await expanding.client.get(body["@odata.nextLink"], headers=_as("analyst"))
    assert [(r["id"], r["orders"]) for r in following.json()["value"]] == [(0, [{"id": 3}])]


async def test_a_masked_column_of_the_related_table_stays_masked(expanding):
    body = await _expand(
        expanding, role="clerk", expand="orders($select=id,email)", filter="id eq 1"
    )
    assert {o["email"] for o in body["value"][0]["orders"]} == {"***"}
    assert "real.example" not in str(body)


async def test_the_related_tables_row_rule_holds(expanding):
    """The clerk's rule on orders keeps customer 1's: another customer's list is empty."""
    body = await _expand(expanding, role="clerk", expand="orders($select=id)")
    by_id = {r["id"]: [o["id"] for o in r["orders"]] for r in body["value"]}
    assert by_id[1] == [1, 4, 7, 10]
    assert by_id[2] == []


async def test_an_or_in_a_nested_filter_cannot_widen_past_the_row_rule(expanding):
    body = await _expand(
        expanding, role="clerk", expand="orders($filter=true or id eq 2;$select=id)"
    )
    assert next(r for r in body["value"] if r["id"] == 2)["orders"] == []


async def test_a_masked_key_still_joins_and_stays_masked(expanding):
    """REQ-603: the join reads the real key; what the role reads of it is its mask."""
    expanding.pipeline.masks[(2, "clerk")] = {
        "id": (MaskingRule(mask_type=MaskType.constant, value=0), "integer")
    }
    body = await _expand(
        expanding, role="clerk", path="orders", expand="customer", filter="id eq 1"
    )
    assert body["value"][0]["customer"] == {"id": 0, "name": "one"}


async def test_a_nested_filter_on_a_masked_column_is_refused_and_runs_nothing(expanding):
    resp = await _expand_refused(expanding, "orders($filter=email eq 'x')", role="clerk")
    assert resp.status_code == 403, resp.text
    assert "email" in resp.json()["error"]["message"]
    assert expanding.pipeline.statements == []


async def test_a_nested_order_on_a_masked_column_sorts_what_the_role_is_shown(expanding):
    body = await _expand(
        expanding, role="clerk", expand="orders($orderby=email desc;$select=id)", filter="id eq 1"
    )
    assert [o["id"] for o in body["value"][0]["orders"]] == [1, 4, 7, 10]


async def test_a_filter_on_a_faked_column_of_the_related_table_reads_the_fake(
    expanding, monkeypatch
):
    monkeypatch.setattr(digest_mod, "_key", b"k" * 32)
    register(expanding.pipeline.con)
    rule = MaskingRule(MaskType.fake, fake="email()")
    expanding.pipeline.masks[(1, "clerk")] = {"email": (rule, "varchar(200)")}
    body = await _expand(
        expanding, role="clerk", expand="orders($select=id,email;$top=1)", filter="id eq 1"
    )
    (shown,) = body["value"][0]["orders"]
    assert shown["email"] != f"user{shown['id']}@real.example"
    hit = await _expand(
        expanding,
        role="clerk",
        expand=f"orders($filter=email eq '{shown['email']}';$select=id)",
        filter="id eq 1",
    )
    assert hit["value"][0]["orders"] == [{"id": shown["id"]}]


@pytest.mark.parametrize(
    "expansion",
    ["nothing", "salary", "orders($select=margin)", "orders($orderby=margin)"],
)
async def test_expanding_what_the_role_cannot_see_is_as_for_no_name(expanding, expansion):
    """Salary is of orders and hidden from the clerk; margin is a column hidden from it."""
    path = "orders" if expansion == "salary" else "customers"
    resp = await _expand_refused(expanding, expansion, role="clerk", path=path)
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"]["code"] == "invalid_query_option"
    assert expanding.pipeline.statements == []


async def test_a_nested_filter_on_a_hidden_column_is_as_for_no_column(expanding):
    resp = await _expand_refused(expanding, "orders($filter=margin gt 0)", role="clerk")
    assert resp.status_code == 400, resp.text
    assert expanding.pipeline.statements == []


@pytest.mark.parametrize(
    "expansion",
    ["customer($top=1)", "customer($filter=id eq 1)", "customer($orderby=id)", "customer($skip=1)"],
)
async def test_a_single_valued_expansion_takes_no_collection_option(expanding, expansion):
    resp = await _expand_refused(expanding, expansion, path="orders")
    assert resp.status_code == 400, resp.text


@pytest.mark.parametrize(
    ("expansion", "status"),
    [
        ("", 400),
        ("orders(", 400),
        ("orders)", 400),
        ("orders()", 400),
        ("orders($top)", 400),
        ("orders($top=x)", 400),
        ("orders($bogus=1)", 400),
        ("orders($format=json)", 400),
        ("orders(top=1)", 400),
        ("orders($top=1;$top=2)", 400),
        ("orders,orders", 400),
        ("orders($select=id)x", 400),
        ("orders($filter=email eq 'x)", 400),
        ("orders($filter=id eq)", 400),
        ("orders($filter=bogus(id))", 501),
        ("orders($count=true)", 501),
        ("orders($levels=2)", 501),
        ("orders($search=x)", 501),
        ("orders/$ref", 501),
        ("orders/$count", 501),
        ("customer/orders", 501),
        ("*", 501),
    ],
)
async def test_an_expand_it_cannot_read_is_refused(expanding, expansion, status):
    resp = await _expand_refused(expanding, expansion)
    assert resp.status_code == status, resp.text
    assert expanding.pipeline.statements == []


async def test_top_zero_in_an_expansion_reads_no_row_of_it(expanding):
    body = await _expand(expanding, expand="orders($top=0)", filter="id eq 1", select="id")
    assert body["value"] == [{"id": 1, "orders": []}]
    assert "@odata.nextLink" not in str(body)
    assert "orders" not in expanding.pipeline.statements[0].replace("customers", "")


async def test_a_count_takes_no_expand(expanding):
    resp = await expanding.client.get(
        "/data/odata/customers/$count", params={"$expand": "orders"}, headers=_as("analyst")
    )
    assert resp.status_code == 400


async def test_a_row_replicated_table_is_refused_by_name(expanding):
    expanding.pipeline.surfaces["analyst"].context.row_level_tables = frozenset({2})
    resp = await _expand_refused(expanding, "customer", path="orders")
    assert resp.status_code == 501, resp.text
    assert "row by row" in resp.json()["error"]["message"]
    assert expanding.pipeline.statements == []


# -- a bounded to-many expand ----------------------------------------------------------------------


@pytest.fixture
def bounded(expanding, monkeypatch):
    monkeypatch.setattr(odata_expand, "EXPAND_PAGE_SIZE", 3)
    return expanding


async def test_a_to_many_expand_is_bounded_and_links_the_rest(bounded):
    body = await _expand(
        bounded, expand="orders($select=id;$filter=id gt 0;$orderby=id desc)", filter="id eq 1"
    )
    (row,) = body["value"]
    assert [o["id"] for o in row["orders"]] == [10, 7, 4]
    parts = urlsplit(row["orders@odata.nextLink"])
    assert parts.path == "/data/odata/customers(1)/orders"
    assert parse_qs(parts.query) == {
        "$select": ["id"],
        "$filter": ["id gt 0"],
        "$orderby": ["id desc"],
        "$skip": ["3"],
    }


async def test_a_list_that_fits_has_no_link(bounded):
    body = await _expand(bounded, expand="orders($filter=id lt 8)", filter="id eq 1")
    assert [o["id"] for o in body["value"][0]["orders"]] == [1, 4, 7]
    assert "orders@odata.nextLink" not in body["value"][0]


async def test_following_a_nested_link_reads_every_row_once(expanding, monkeypatch):
    monkeypatch.setattr(odata_expand, "EXPAND_PAGE_SIZE", 2)
    first = (await _expand(expanding, expand="orders($select=id)", filter="id eq 1"))["value"][0]
    seen = [o["id"] for o in first["orders"]]
    url, pages = first["orders@odata.nextLink"], 1
    while url:
        page = (await expanding.client.get(url, headers=_as("analyst"))).json()
        assert page["@odata.context"].endswith("$metadata#orders(id)")
        seen += [o["id"] for o in page["value"]]
        url = page.get("@odata.nextLink")
        pages += 1
    assert seen == [1, 4, 7, 10]
    assert pages == 2


async def test_a_nested_link_carries_what_remains_of_top(bounded):
    body = await _expand(bounded, expand="orders($select=id;$top=4)", filter="id eq 1")
    (row,) = body["value"]
    assert [o["id"] for o in row["orders"]] == [1, 4, 7]
    assert parse_qs(urlsplit(row["orders@odata.nextLink"]).query)["$top"] == ["1"]
    following = await bounded.client.get(row["orders@odata.nextLink"], headers=_as("analyst"))
    assert [o["id"] for o in following.json()["value"]] == [10]
    assert "@odata.nextLink" not in following.json()


async def test_a_link_under_a_nested_list_addresses_its_own_parent(bounded):
    body = await _expand(
        bounded,
        path="orders",
        expand="customer($select=name;$expand=orders($select=id))",
        filter="id eq 1",
    )
    customer = body["value"][0]["customer"]
    assert set(customer) == {"name", "orders", "orders@odata.nextLink"}  # the key is only read
    assert "/customers(1)/orders?" in customer["orders@odata.nextLink"]


async def test_a_nested_link_holds_the_row_rule_and_the_mask(bounded, monkeypatch):
    monkeypatch.setattr(odata_expand, "EXPAND_PAGE_SIZE", 2)
    body = await _expand(bounded, role="clerk", expand="orders($select=id,email)", filter="id eq 1")
    link = body["value"][0]["orders@odata.nextLink"]
    page = (await bounded.client.get(link, headers=_as("clerk"))).json()
    assert page["value"] == [{"id": 7, "email": "***"}, {"id": 10, "email": "***"}]


async def test_a_list_past_its_bound_with_no_address_is_refused(bounded):
    """A parent with no key could not say where the rest of its list is."""
    bounded.pipeline.surfaces["analyst"].context.pk_columns.pop(2)
    resp = await _expand_refused(bounded, "orders")
    assert resp.status_code == 501, resp.text
    assert "no key" in resp.json()["error"]["message"]


async def test_a_list_past_its_bound_under_a_masked_key_is_refused(bounded):
    bounded.pipeline.masks[(2, "clerk")] = {
        "id": (MaskingRule(mask_type=MaskType.constant, value=0), "integer")
    }
    resp = await _expand_refused(bounded, "orders", role="clerk")
    assert resp.status_code == 501, resp.text


async def test_a_list_within_its_bound_with_no_address_is_served(bounded):
    bounded.pipeline.surfaces["analyst"].context.pk_columns.pop(2)
    body = await _expand(bounded, expand="orders($top=2;$select=id)", filter="id eq 1")
    assert "orders" in body["value"][0]


async def test_a_ceiling_below_the_page_ends_the_list(bounded, monkeypatch):
    """The role's ceiling holds the list as it holds a read: the probe row is not read, so the
    list ends there with no link."""
    monkeypatch.setitem(_ROLES["analyst"], "max_rows", 2)
    body = await _expand(bounded, expand="orders($select=id;$orderby=id desc)", filter="id eq 1")
    (row,) = body["value"]
    assert [o["id"] for o in row["orders"]] == [10, 7]
    assert "orders@odata.nextLink" not in row


# -- set(key)/navigation ---------------------------------------------------------------------------


async def test_a_navigation_read_takes_a_collections_options(expanding):
    body = await _expand(
        expanding,
        path="customers(1)/orders",
        select="id,amount",
        filter="id gt 1",
        orderby="id desc",
        top="2",
    )
    assert body["@odata.context"].endswith("$metadata#orders(id,amount)")
    assert [o["id"] for o in body["value"]] == [10, 7]
    assert len(expanding.pipeline.statements) == 1


async def test_a_navigation_read_expands(expanding):
    body = await _expand(
        expanding, path="customers(1)/orders", expand="customer($select=name)", top="1"
    )
    assert body["value"][0]["customer"] == {"name": "one"}


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("customers(77)/orders", 404),
        ("customers(1)/nothing", 404),
        ("customers/orders", 404),
        ("nothing(1)/orders", 404),
        ("salaries(1)/orders", 404),
        ("orders(1)/customer", 501),
        ("customers('x')/orders", 400),
        ("customers(a=1)/orders", 400),
        ("customers(id=1,id=2)/orders", 400),
        ("customers(1,2)/orders", 400),
        ("customers()/orders", 400),
    ],
)
async def test_a_navigation_it_cannot_read_is_refused(expanding, path, status):
    resp = await expanding.client.get(f"/data/odata/{path}", headers=_as("clerk"))
    assert resp.status_code == status, resp.text


async def test_a_navigation_takes_no_count(expanding):
    resp = await expanding.client.get(
        "/data/odata/customers(1)/orders", params={"$count": "true"}, headers=_as("analyst")
    )
    assert resp.status_code == 501


async def test_a_navigation_from_a_row_the_rule_hides_is_not_found(expanding):
    expanding.pipeline.rls["clerk"][2] = '"id" = 1'
    hidden = await expanding.client.get("/data/odata/customers(2)/orders", headers=_as("clerk"))
    assert hidden.status_code == 404


async def test_a_navigation_by_a_masked_key_is_refused(expanding):
    expanding.pipeline.masks[(2, "clerk")] = {
        "id": (MaskingRule(mask_type=MaskType.constant, value=0), "integer")
    }
    resp = await expanding.client.get("/data/odata/customers(1)/orders", headers=_as("clerk"))
    assert resp.status_code == 403


@pytest.mark.parametrize(
    ("value", "edm", "literal"),
    [
        (1, EdmType("Edm.Int32"), "1"),
        (Decimal("1.50"), EdmType("Edm.Decimal"), "1.50"),
        ("o'brien/x", EdmType("Edm.String"), "'o''brien/x'"),
        (True, EdmType("Edm.Boolean"), "true"),
        ("2026-01-02", EdmType("Edm.Date"), "2026-01-02"),
        (datetime(2026, 1, 2, 3), EdmType("Edm.DateTimeOffset"), "2026-01-02T03:00:00Z"),
    ],
)
def test_a_key_is_written_as_the_parser_reads_it(value, edm, literal):
    written = _key_in_next_link(value, edm.name)
    assert written == literal
    ((name, read),) = parse_key(written)
    assert name is None and read.value is not None


def test_a_string_key_with_a_slash_or_quote_keeps_its_address():
    ((_, read),) = parse_key("'o''brien/x'")
    assert read == Literal("string", "o'brien/x")


def test_a_composite_key_is_named():
    assert parse_key("a=1,b='x'") == (
        ("a", Literal("number", 1)),
        ("b", Literal("string", "x")),
    )


@pytest.mark.parametrize("text", ["", "a", "a=", "=1", "a=1,", "1,2", "null", "a=b"])
def test_text_that_is_no_key_is_a_400(text):
    with pytest.raises(QueryOptionRefused) as refused:
        parse_key(text)
    assert refused.value.status == 400


def test_the_options_of_an_expansion_are_kept_as_sent():
    (expansion,) = parse_query_options([("$expand", "a($top= 2 ;$select=b,c)")]).expand
    assert expansion.name == "a"
    assert expansion.options.top == 2
    assert expansion.options.select == ("b", "c")
    assert expansion.raw == (("$top", "2"), ("$select", "b,c"))


def test_the_app_times_the_service_by_its_own_transport():
    assert http_transport_for_path("/data/odata/orders") == "odata"
