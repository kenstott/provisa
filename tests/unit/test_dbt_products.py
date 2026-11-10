# Copyright (c) 2026 Kenneth Stott
# Canary: 8d41f6a2-3c07-4be9-a1d5-92e07b6c4f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dbt project's public models with enforced contracts, registered in bulk from Register Table as
draft data products (REQ-2228): one per dbt group, one for a project with no groups. On the real
manifest of ``test_dbt_source`` and a real control-plane database; the fixture's models are all
protected, so each test states what makes one offered.

Tables are registered by the registerTable mutation and products made by the data product
mutations: the tests run those mutations for real, standing in only for the warehouse's column
listing and for the table write behind registerTable's own checks.
"""

# Requirements: REQ-2228

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert, select

from provisa.api.admin.dbt_router import (
    DbtRegisterProducts,
    DbtRegisterRequest,
    dbt_products_preview,
    register_dbt_products,
    register_dbt_source,
)
from provisa.api.admin.types import AvailableColumnType, MutationResult
from provisa.api.errors import ApiError
from provisa.core import schema_org
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import DataContract, Source, SourceType
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import source as source_repo
from provisa.dbt.manifest import from_definitions, parse_manifest, to_definitions
from provisa.dbt.products import planned_products, ungrouped

FIXTURE = Path(__file__).parent.parent / "fixtures" / "dbt" / "manifest.json"
SOURCE = "dbt-jaffle"
WAREHOUSE = "wh-jaffle"
ORDERS = "model.jaffle.customer_orders"
EVENTS = "model.jaffle.order_events"
STAGING = "model.jaffle.stg_orders"
WAREHOUSE_COLUMNS = {
    "customer_orders": ("customer_id", "customer_name", "lifetime_value"),
    "order_events": ("order_id",),
}
REQUEST = SimpleNamespace(state=SimpleNamespace(identity=None))
FINANCE_OWNER = {"name": "Finance Analytics", "email": "finance@example.com"}


@pytest.fixture(name="document")
def _document() -> dict:
    return json.loads(FIXTURE.read_text())


def _offer(
    document: dict,
    unique_id: str = ORDERS,
    *,
    access: str = "public",
    enforced: bool = True,
    version: Any = 2,
    group: str | None = None,
) -> dict:
    node = document["nodes"][unique_id]
    node["access"] = access
    node["contract"] = {"enforced": enforced, "alias_types": True, "checksum": None}
    node["version"] = version
    node["group"] = group
    if unique_id == ORDERS:
        node["columns"]["customer_id"]["data_type"] = "integer"
        node["columns"]["customer_id"]["constraints"] = [{"type": "not_null", "name": None}]
        node["columns"]["lifetime_value"]["data_type"] = "decimal"
    return document


def _group(document: dict, name: str, owner: dict | None = None) -> dict:
    document.setdefault("groups", {})[f"group.jaffle.{name}"] = {
        "name": name,
        "owner": owner or {"name": None, "email": None},
    }
    return document


@pytest.fixture(name="grouped")
def _grouped(document: dict) -> dict:
    """Two public models in two groups, and a protected one."""
    _group(document, "finance", FINANCE_OWNER)
    _group(document, "ops")
    _offer(document, ORDERS, group="finance")
    return _offer(document, EVENTS, version=None, group="ops")


@pytest.fixture(name="ungrouped_project")
def _ungrouped_project(document: dict) -> dict:
    """Two public models in a project with no groups."""
    _offer(document, ORDERS)
    return _offer(document, EVENTS, version=None)


# ---- what the manifest says ----------------------------------------------------------------


def test_the_manifest_says_what_a_public_contract_promises(grouped):
    model = next(m for m in parse_manifest(grouped).models if m.unique_id == ORDERS)
    assert model.is_candidate
    assert model.version == "2"
    assert model.owner is not None
    assert (model.owner.name, model.owner.email) == ("Finance Analytics", "finance@example.com")
    columns = {c.name: c for c in model.columns}
    assert columns["customer_id"].required
    assert columns["customer_id"].data_type == "integer"
    assert not columns["lifetime_value"].required


def test_the_models_tests_are_read_from_the_manifest(grouped):
    manifest = parse_manifest(grouped)
    assert sorted((t.test_name, t.column) for t in manifest.tests_of(ORDERS)) == [
        ("not_null", "customer_id"),
        ("unique", "customer_id"),
    ]


def test_the_groups_are_read_from_the_manifest(grouped):
    groups = {g.name: g.owner for g in parse_manifest(grouped).groups}
    assert set(groups) == {"finance", "ops"}
    assert groups["finance"] is not None
    assert (groups["finance"].name, groups["finance"].email) == (
        "Finance Analytics",
        "finance@example.com",
    )
    assert groups["ops"] is None


def test_a_contract_and_its_groups_survive_being_held(grouped):
    manifest = parse_manifest(grouped)
    assert from_definitions(to_definitions(manifest)) == manifest


def test_what_was_held_before_contracts_and_groups_were_read_still_loads(document):
    held = json.loads(to_definitions(parse_manifest(document)))
    del held["groups"]
    for model in held["models"]:
        for key in ("access", "contract_enforced", "version", "owner"):
            del model[key]
        for column in model["columns"]:
            del column["constraints"]
    for test in held["tests"]:
        del test["attached_to"], test["column"], test["test_name"]
    manifest = from_definitions(json.dumps(held))
    assert manifest.groups == ()
    assert planned_products(manifest) == []


# ---- what is offered, and how it is grouped -----------------------------------------------


@pytest.mark.parametrize(
    "shape",
    [
        pytest.param({"access": "protected"}, id="protected"),
        pytest.param({"access": "private"}, id="private"),
        pytest.param({"enforced": False}, id="contract-not-enforced"),
    ],
)
def test_only_public_models_with_an_enforced_contract_are_offered(document, shape):
    manifest = parse_manifest(_offer(_offer(document, EVENTS), ORDERS, **shape))
    assert [[m.unique_id for m in p.models] for p in planned_products(manifest)] == [[EVENTS]]


def test_the_fixtures_protected_models_offer_no_product(document):
    assert planned_products(parse_manifest(document)) == []


def test_each_dbt_group_is_one_product(grouped):
    plan = planned_products(parse_manifest(grouped))
    assert [(p.id, p.group, [m.unique_id for m in p.models]) for p in plan] == [
        ("finance", "finance", [ORDERS]),
        ("ops", "ops", [EVENTS]),
    ]
    finance = plan[0]
    assert finance.owner is not None and finance.owner.email == "finance@example.com"


def test_a_group_holds_every_public_model_in_it(document):
    _group(document, "finance", FINANCE_OWNER)
    _offer(document, ORDERS, group="finance")
    _offer(document, EVENTS, group="finance")
    plan = planned_products(parse_manifest(document))
    assert [(p.id, [m.unique_id for m in p.models]) for p in plan] == [
        ("finance", [ORDERS, EVENTS])
    ]


def test_a_group_with_no_public_model_makes_no_product(grouped):
    _group(grouped, "marketing")
    assert [p.id for p in planned_products(parse_manifest(grouped))] == ["finance", "ops"]


def test_a_project_with_no_groups_is_one_product(ungrouped_project):
    plan = planned_products(parse_manifest(ungrouped_project))
    assert [(p.id, p.group, p.owner, [m.unique_id for m in p.models]) for p in plan] == [
        ("jaffle", None, None, [ORDERS, EVENTS])
    ]


def test_a_public_model_in_no_group_of_a_grouped_project_is_named_and_left_out(grouped):
    _offer(grouped, STAGING)
    manifest = parse_manifest(grouped)
    assert [m.unique_id for p in planned_products(manifest) for m in p.models] == [ORDERS, EVENTS]
    assert [m.unique_id for m in ungrouped(manifest)] == [STAGING]


# ---- the database, the gates and the mutations ---------------------------------------------


@pytest.fixture(name="db")
def _db() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="dbt-products")
    with db.engine.begin() as conn:
        schema_org.metadata.create_all(
            conn,
            tables=[
                schema_org.domains,
                schema_org.sources,
                schema_org.roles,
                schema_org.registered_tables,
                schema_org.table_columns,
                schema_org.dbt_manifests,
                schema_org.data_products,
                schema_org.data_product_tables,
                schema_org.data_product_commands,
                schema_org.checker_targets,
            ],
        )
    return db


@pytest.fixture(name="registered")
def _registered(db: Database, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Each TableInput the registerTable mutation was given once its own dbt check rewrote it.

    The mutation (its capability gate and its dbt read-through) runs for real; what stands in is
    the table write behind it and the warehouse's column listing.
    """
    import provisa.api.admin.dbt_router as router
    import provisa.api.admin.schema_mutation as schema_mutation
    import provisa.api.admin.schema_mutation_ops as ops
    import provisa.api.admin.schema_query as schema_query
    import provisa.api.app as app
    from provisa.api.admin._dbt_table_registration import read_through_warehouse

    monkeypatch.setattr(app.state, "model_db", db, raising=False)
    monkeypatch.setattr(app.state, "source_types", {SOURCE: "dbt"}, raising=False)
    monkeypatch.setattr(app.state, "roles", {"analyst": {}, "steward": {}}, raising=False)
    monkeypatch.setattr(app, "_rebuild_schemas", AsyncMock())
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", AsyncMock())
    monkeypatch.setattr(router, "require_capability_request", lambda *a, **k: None)

    async def _columns(source_id: str, schema: str, table: str) -> list[AvailableColumnType]:
        assert source_id == SOURCE
        return [
            AvailableColumnType(name=c, data_type="varchar", comment=None)
            for c in WAREHOUSE_COLUMNS[table]
        ]

    monkeypatch.setattr(schema_query, "resolve_available_columns_metadata", _columns)
    seen: list[Any] = []

    async def _register_table(_info: Any, table: Any) -> MutationResult:
        refused = await read_through_warehouse(table)
        if refused is not None:
            return refused
        seen.append(table)
        await _insert_table(
            db, table.schema_name, table.table_name, [c.name for c in table.columns]
        )
        return MutationResult(success=True, message="registered")

    monkeypatch.setattr(ops, "register_table", _register_table)
    return seen


async def _hold(db: Database, document: dict) -> None:
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn, Source(id=WAREHOUSE, type=SourceType.duckdb, path="/data/jaffle.duckdb")
        )
        await conn.execute_core(insert(schema_org.domains).values(id="sales", description=""))
    await register_dbt_source(
        REQUEST,  # pyright: ignore[reportArgumentType]
        DbtRegisterRequest(source_id=SOURCE, spec="upload", content=json.dumps(document)),
    )


async def _insert_table(db: Database, schema: str, table: str, columns: list[str]) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(schema_org.registered_tables).values(
                source_id=WAREHOUSE, domain_id="sales", schema_name=schema, table_name=table
            )
        )
        table_id = (
            await conn.execute_core(
                select(schema_org.registered_tables.c.id).where(
                    schema_org.registered_tables.c.table_name == table
                )
            )
        ).scalar_one()
        for column in columns:
            await conn.execute_core(
                insert(schema_org.table_columns).values(
                    table_id=table_id, column_name=column, data_type="varchar"
                )
            )
    return table_id


async def _table_id(db: Database, table: str) -> int:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(schema_org.registered_tables.c.id).where(
                    schema_org.registered_tables.c.table_name == table
                )
            )
        ).scalar_one()


async def _preview() -> dict:
    return await dbt_products_preview(REQUEST, SOURCE)  # pyright: ignore[reportArgumentType]


async def _register(domain_id: str = "sales") -> dict:
    return await register_dbt_products(
        REQUEST,  # pyright: ignore[reportArgumentType]
        SOURCE,
        DbtRegisterProducts(domain_id=domain_id),
    )


async def _products(db: Database) -> dict[str, dict]:
    async with db.acquire() as conn:
        return {p["id"]: p for p in await data_product_repo.list_all(conn)}


async def _members(db: Database, product_id: str) -> list[int]:
    async with db.acquire() as conn:
        return await data_product_repo.member_table_ids(conn, product_id)


async def _contract(db: Database, table_id: int) -> DataContract:
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(schema_org.registered_tables.c.contract).where(
                    schema_org.registered_tables.c.id == table_id
                )
            )
        ).fetchone()
    assert row is not None
    return DataContract.model_validate(row.contract)


async def _refusal(code: str) -> ApiError:
    with pytest.raises(ApiError) as refused:
        await _register()
    assert refused.value.code == code
    return refused.value


# ---- the preview ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_preview_shows_each_product_and_creates_nothing(db, registered, grouped):
    await _hold(db, grouped)
    preview = await _preview()
    assert [(p["id"], [m["unique_id"] for m in p["models"]]) for p in preview["products"]] == [
        ("finance", [ORDERS]),
        ("ops", [EVENTS]),
    ]
    finance = preview["products"][0]
    assert finance["owner"] == FINANCE_OWNER
    assert finance["exists"] is False
    assert finance["tests"] == [
        "customer_orders.not_null(customer_id)",
        "customer_orders.unique(customer_id)",
    ]
    orders = finance["models"][0]
    assert (orders["schema"], orders["relation"], orders["version"]) == (
        "analytics",
        "customer_orders",
        "2",
    )
    assert orders["table_id"] is None
    assert {"name": "customer_id", "data_type": "integer", "required": True} in orders["columns"]
    assert preview["ungrouped"] == []
    assert await _products(db) == {}
    assert registered == []


@pytest.mark.asyncio
async def test_the_preview_of_a_project_with_no_groups_is_one_product(
    db, registered, ungrouped_project
):
    await _hold(db, ungrouped_project)
    preview = await _preview()
    assert [(p["id"], p["group"], len(p["models"])) for p in preview["products"]] == [
        ("jaffle", None, 2)
    ]


@pytest.mark.asyncio
async def test_the_preview_says_which_models_are_registered_and_which_ids_are_taken(
    db, registered, grouped
):
    await _hold(db, grouped)
    table_id = await _insert_table(db, "analytics", "customer_orders", ["customer_id"])
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(schema_org.data_products).values(
                id="ops", domain_id="sales", name="Taken", status="draft"
            )
        )
    finance, ops = (await _preview())["products"]
    assert finance["models"][0]["table_id"] == table_id
    assert (finance["exists"], ops["exists"]) == (False, True)


@pytest.mark.asyncio
async def test_the_preview_offers_an_owner_named_as_a_role(db, registered, grouped):
    await _hold(db, grouped)
    async with db.acquire() as conn:
        await conn.execute_core(insert(schema_org.roles).values(id="finance", capabilities=[]))
    assert [p["owner_role"] for p in (await _preview())["products"]] == ["finance", None]


@pytest.mark.asyncio
async def test_the_fixtures_protected_models_preview_no_product(db, registered, document):
    await _hold(db, document)
    assert (await _preview())["products"] == []


@pytest.mark.asyncio
async def test_a_source_with_no_manifest_is_refused(db, registered):
    with pytest.raises(ApiError) as refused:
        await _preview()
    assert refused.value.code == "dbt.source_not_registered"


# ---- registering ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_each_model_is_registered_by_the_register_table_mutation(db, registered, grouped):
    await _hold(db, grouped)
    await _register()
    # registerTable's own dbt check read each model through the warehouse source.
    assert [(t.source_id, t.domain_id, t.schema_name, t.table_name) for t in registered] == [
        (WAREHOUSE, "sales", "analytics", "customer_orders"),
        (WAREHOUSE, "sales", "analytics", "order_events"),
    ]
    orders = registered[0]
    assert [c.name for c in orders.columns] == list(WAREHOUSE_COLUMNS["customer_orders"])
    assert all(c.visible_to == ["analyst", "steward"] for c in orders.columns)
    assert orders.description == "One row per customer with lifetime value."


@pytest.mark.asyncio
async def test_each_group_becomes_a_draft_product_of_its_models(db, registered, grouped):
    await _hold(db, grouped)
    said = await _register()
    assert said == {"data_products": ["finance", "ops"], "domain_id": "sales"}
    products = await _products(db)
    assert set(products) == {"finance", "ops"}
    assert {p["status"] for p in products.values()} == {"draft"}
    assert {p["domain_id"] for p in products.values()} == {"sales"}
    assert await _members(db, "finance") == [await _table_id(db, "customer_orders")]
    assert await _members(db, "ops") == [await _table_id(db, "order_events")]


@pytest.mark.asyncio
async def test_a_project_with_no_groups_becomes_one_draft_product(
    db, registered, ungrouped_project
):
    await _hold(db, ungrouped_project)
    await _register()
    products = await _products(db)
    assert list(products) == ["jaffle"]
    assert products["jaffle"]["status"] == "draft"
    assert sorted(await _members(db, "jaffle")) == sorted(
        [await _table_id(db, "customer_orders"), await _table_id(db, "order_events")]
    )


@pytest.mark.asyncio
async def test_a_product_carries_its_owner_and_tests(db, registered, grouped):
    await _hold(db, grouped)
    await _register()
    finance = (await _products(db))["finance"]
    assert finance["support_contact"] == "finance@example.com"
    assert finance["custom_properties"] == {
        "dbt_models": ORDERS,
        "dbt_owner": "Finance Analytics",
        "dbt_tests": "customer_orders.not_null(customer_id); customer_orders.unique(customer_id)",
    }


@pytest.mark.asyncio
async def test_each_member_carries_its_schema_promise_and_version(db, registered, grouped):
    await _hold(db, grouped)
    await _register()
    orders = await _contract(db, await _table_id(db, "customer_orders"))
    assert orders.version == "2"
    assert orders.description == "One row per customer with lifetime value."
    assert [v.contract.version for v in orders.versions] == ["2"]
    promised = {c.name: c.required for c in orders.versions[0].columns}
    assert promised == {"customer_id": True, "customer_name": False, "lifetime_value": False}
    events = await _contract(db, await _table_id(db, "order_events"))
    assert events.version == "0.1.0"


@pytest.mark.asyncio
async def test_a_model_already_registered_is_not_registered_again(db, registered, grouped):
    await _hold(db, grouped)
    table_id = await _insert_table(
        db, "analytics", "customer_orders", list(WAREHOUSE_COLUMNS["customer_orders"])
    )
    await _register()
    assert [t.table_name for t in registered] == ["order_events"]
    assert await _members(db, "finance") == [table_id]


@pytest.mark.asyncio
async def test_a_product_id_in_use_is_refused_before_anything_is_registered(
    db, registered, grouped
):
    await _hold(db, grouped)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(schema_org.data_products).values(
                id="ops", domain_id="sales", name="Taken", status="draft"
            )
        )
    refused = await _refusal("dbt.products.id_taken")
    assert refused.params == {"data_product": "ops"}
    assert registered == []
    assert list(await _products(db)) == ["ops"]


@pytest.mark.asyncio
async def test_a_source_offering_no_public_model_is_refused(db, registered, document):
    await _hold(db, document)
    await _refusal("dbt.products.none_offered")
    assert registered == []


@pytest.mark.asyncio
async def test_a_table_missing_a_promised_column_makes_no_product(db, registered, grouped):
    await _hold(db, grouped)
    await _insert_table(db, "analytics", "customer_orders", ["customer_id"])
    refused = await _refusal("dbt.products.columns_missing")
    assert refused.params == {
        "model": "customer_orders",
        "columns": "customer_name, lifetime_value",
    }
    assert await _products(db) == {}


@pytest.mark.asyncio
async def test_a_registration_the_mutation_refuses_is_passed_on(db, registered, grouped):
    await _hold(db, grouped)
    async with db.acquire() as conn:
        await conn.execute_core(
            schema_org.sources.delete().where(schema_org.sources.c.id == WAREHOUSE)
        )
    refused = await _refusal("dbt.no_warehouse")
    assert refused.params["adapter"] == "duckdb"
    assert await _products(db) == {}
