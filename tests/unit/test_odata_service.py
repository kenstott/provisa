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

import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import duckdb
import httpx
import orjson
import pytest
import sqlglot
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

from provisa.api.odata.csdl import metadata_document
from provisa.api.odata.edm import EdmType, edm_type, edm_value
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
        native = sqlglot.transpile(lowered, read="postgres", write="duckdb")[0]
        return SimpleNamespace(sql=native, params=list(exec_params or []))

    async def execute(self, plan, state):
        rows = self.con.execute(plan.sql, plan.params).fetchall()
        return SimpleNamespace(rows=rows, redirect=None, cache_entry=None)


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


@pytest.mark.parametrize("option", ["$filter", "$orderby", "$expand", "$search"])
async def test_an_option_not_served_is_refused_never_ignored(service, option):
    resp = await service.client.get(
        "/data/odata/orders", params={option: "id"}, headers=_as("analyst")
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
    assert edm_value(value, EdmType(type_name)) == expected


def test_a_decimal_is_written_exactly():
    exact = Decimal("12345678901234567890.123456789")
    assert orjson.dumps(edm_value(exact, EdmType("Edm.Decimal"))) == str(exact).encode()


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
