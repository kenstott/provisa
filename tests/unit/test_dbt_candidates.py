# Copyright (c) 2026 Kenneth Stott
# Canary: 8d41f6a2-3c07-4be9-a1d5-92e07b6c4f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dbt model the manifest marks public with an enforced contract is a candidate data product
(REQ-2228), on the real manifest of ``test_dbt_source`` and a real control-plane database. The
fixture's models are all protected, so each test states what makes one a candidate.
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
    DbtAcceptCandidate,
    DbtRegisterRequest,
    accept_dbt_candidate,
    dbt_candidates,
    register_dbt_source,
)
from provisa.api.errors import ApiError
from provisa.core import schema_org
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import DataContract, Source, SourceType
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import source as source_repo
from provisa.dbt.manifest import from_definitions, parse_manifest, to_definitions

FIXTURE = Path(__file__).parent.parent / "fixtures" / "dbt" / "manifest.json"
SOURCE = "dbt-jaffle"
WAREHOUSE = "wh-jaffle"
MODEL = "model.jaffle.customer_orders"
COLUMNS = ("customer_id", "customer_name", "lifetime_value")
REQUEST = SimpleNamespace()


@pytest.fixture(name="document")
def _document() -> dict:
    return json.loads(FIXTURE.read_text())


def _make_candidate(
    document: dict, *, access: str = "public", enforced: bool = True, version: Any = 2
) -> dict:
    node = document["nodes"][MODEL]
    node["access"] = access
    node["contract"] = {"enforced": enforced, "alias_types": True, "checksum": None}
    node["version"] = version
    node["group"] = "finance"
    node["columns"]["customer_id"]["data_type"] = "integer"
    node["columns"]["customer_id"]["constraints"] = [{"type": "not_null", "name": None}]
    node["columns"]["lifetime_value"]["data_type"] = "decimal"
    document["groups"] = {
        "group.jaffle.finance": {
            "name": "finance",
            "owner": {"name": "Finance Analytics", "email": "finance@example.com"},
        }
    }
    return document


@pytest.fixture(name="candidate")
def _candidate(document: dict) -> dict:
    return _make_candidate(document)


@pytest.fixture(name="db")
def _db() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="dbt-candidates")
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
            ],
        )
    return db


@pytest.fixture(name="gates")
def _gates(db: Database, monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, set[str]]]:
    """What each acceptance asked of the domain gate; the gates themselves stand aside."""
    import provisa.api.admin.dbt_router as router
    import provisa.api.app as app

    monkeypatch.setattr(app.state, "model_db", db, raising=False)
    monkeypatch.setattr(app.state, "source_types", {}, raising=False)
    monkeypatch.setattr(app, "_rebuild_schemas", AsyncMock())
    monkeypatch.setattr(router, "require_capability_request", lambda *a, **k: None)
    asked: list[tuple[str, set[str]]] = []
    monkeypatch.setattr(
        router,
        "require_right_in_domains_request",
        lambda _request, right, domains: asked.append((right, set(domains))),
    )
    return asked


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


async def _register_model(db: Database, columns: tuple[str, ...]) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(schema_org.registered_tables).values(
                source_id=WAREHOUSE,
                domain_id="sales",
                schema_name="analytics",
                table_name="customer_orders",
            )
        )
        table_id = (await conn.execute_core(select(schema_org.registered_tables.c.id))).scalar_one()
        for column in columns:
            await conn.execute_core(
                insert(schema_org.table_columns).values(
                    table_id=table_id, column_name=column, data_type="varchar"
                )
            )
    return table_id


async def _offers(db: Database) -> list[dict]:
    return await dbt_candidates(REQUEST, SOURCE)  # pyright: ignore[reportArgumentType]


async def _accept(unique_id: str = MODEL, **body: Any) -> dict:
    return await accept_dbt_candidate(
        REQUEST,  # pyright: ignore[reportArgumentType]
        SOURCE,
        DbtAcceptCandidate(**{"unique_id": unique_id, "product_id": "orders", **body}),
    )


async def _products(db: Database) -> list[dict]:
    async with db.acquire() as conn:
        return await data_product_repo.list_all(conn)


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


def test_the_manifest_says_what_a_public_contract_promises(candidate):
    model = next(m for m in parse_manifest(candidate).models if m.unique_id == MODEL)
    assert model.is_candidate
    assert model.version == "2"
    assert model.owner is not None
    assert (model.owner.name, model.owner.email) == ("Finance Analytics", "finance@example.com")
    columns = {c.name: c for c in model.columns}
    assert columns["customer_id"].required
    assert columns["customer_id"].data_type == "integer"
    assert not columns["lifetime_value"].required


def test_the_models_tests_are_read_from_the_manifest(candidate):
    manifest = parse_manifest(candidate)
    assert sorted((t.test_name, t.column) for t in manifest.tests_of(MODEL)) == [
        ("not_null", "customer_id"),
        ("unique", "customer_id"),
    ]


def test_a_contract_survives_being_held(candidate):
    manifest = parse_manifest(candidate)
    assert from_definitions(to_definitions(manifest)) == manifest


def test_what_was_held_before_contracts_were_read_still_loads(document):
    held = json.loads(to_definitions(parse_manifest(document)))
    for model in held["models"]:
        for key in ("access", "contract_enforced", "version", "owner"):
            del model[key]
        for column in model["columns"]:
            del column["constraints"]
    for test in held["tests"]:
        del test["attached_to"], test["column"], test["test_name"]
    assert not any(m.is_candidate for m in from_definitions(json.dumps(held)).models)


@pytest.mark.parametrize(
    "shape",
    [
        pytest.param({"access": "protected"}, id="protected"),
        pytest.param({"access": "private"}, id="private"),
        pytest.param({"enforced": False}, id="contract-not-enforced"),
    ],
)
def test_only_public_with_an_enforced_contract_is_a_candidate(document, shape):
    manifest = parse_manifest(_make_candidate(document, **shape))
    assert not any(m.is_candidate for m in manifest.models)


@pytest.mark.asyncio
async def test_a_public_contracted_model_is_offered_and_nothing_is_created(db, gates, candidate):
    await _hold(db, candidate)
    table_id = await _register_model(db, COLUMNS)
    offers = await _offers(db)
    assert [o["unique_id"] for o in offers] == [MODEL]
    offer = offers[0]
    assert offer["name"] == "customer_orders"
    assert offer["version"] == "2"
    assert offer["suggested_id"] == "customer_orders_v2"
    assert offer["owner"] == {"name": "Finance Analytics", "email": "finance@example.com"}
    assert offer["table_id"] == table_id
    assert offer["products"] == []
    assert {"name": "customer_id", "data_type": "integer", "required": True} in [
        {k: c[k] for k in ("name", "data_type", "required")} for c in offer["columns"]
    ]
    assert {"name": "unique", "column": "customer_id"} in offer["tests"]
    assert await _products(db) == []


@pytest.mark.asyncio
async def test_the_fixtures_protected_models_are_not_offered(db, gates, document):
    await _hold(db, document)
    assert await _offers(db) == []


@pytest.mark.asyncio
async def test_an_owner_named_as_a_role_is_offered_as_that_role(db, gates, candidate):
    await _hold(db, candidate)
    async with db.acquire() as conn:
        await conn.execute_core(insert(schema_org.roles).values(id="finance", capabilities=[]))
    assert (await _offers(db))[0]["owner_role"] == "finance"


@pytest.mark.asyncio
async def test_an_owner_no_role_matches_is_offered_with_no_role(db, gates, candidate):
    await _hold(db, candidate)
    assert (await _offers(db))[0]["owner_role"] is None


@pytest.mark.asyncio
async def test_accepting_creates_a_product_with_the_contract_owner_and_version(
    db, gates, candidate
):
    await _hold(db, candidate)
    table_id = await _register_model(db, COLUMNS)
    await _accept(owner_role="finance")

    product = (await _products(db))[0]
    assert product["id"] == "orders"
    assert product["domain_id"] == "sales"
    assert product["name"] == "customer_orders"
    assert product["purpose"] == "One row per customer with lifetime value."
    assert product["status"] == "draft"
    assert product["version"] == "2"
    assert product["owner_role"] == "finance"
    assert product["support_contact"] == "finance@example.com"
    assert product["custom_properties"]["dbt_owner"] == "Finance Analytics"
    assert product["custom_properties"]["dbt_model"] == MODEL
    assert product["custom_properties"]["dbt_tests"] == (
        "not_null(customer_id); unique(customer_id)"
    )

    async with db.acquire() as conn:
        assert await data_product_repo.member_table_ids(conn, "orders") == [table_id]
    contract = await _contract(db, table_id)
    assert contract.version == "2"
    assert contract.description == "One row per customer with lifetime value."
    assert [v.contract.version for v in contract.versions] == ["2"]
    promised = {c.name: c.required for c in contract.versions[0].columns}
    assert promised == {"customer_id": True, "customer_name": False, "lifetime_value": False}
    assert gates == [("data_product_rw", {"sales"})]


@pytest.mark.asyncio
async def test_an_accepted_candidate_says_which_product_it_became(db, gates, candidate):
    await _hold(db, candidate)
    await _register_model(db, COLUMNS)
    await _accept()
    assert (await _offers(db))[0]["products"] == ["orders"]


@pytest.mark.asyncio
async def test_an_unversioned_model_starts_at_the_first_contract_version(db, gates, document):
    await _hold(db, _make_candidate(document, version=None))
    table_id = await _register_model(db, COLUMNS)
    await _accept()
    assert (await _products(db))[0]["version"] is None
    assert (await _contract(db, table_id)).version == "0.1.0"


@pytest.mark.asyncio
async def test_a_warehouse_that_folds_case_still_holds_the_promised_columns(db, gates, candidate):
    await _hold(db, candidate)
    table_id = await _register_model(db, tuple(c.upper() for c in COLUMNS))
    await _accept()
    promised = {c.name: c.required for c in (await _contract(db, table_id)).versions[0].columns}
    assert promised == {"CUSTOMER_ID": True, "CUSTOMER_NAME": False, "LIFETIME_VALUE": False}


async def _refusal(code: str, **body: Any) -> ApiError:
    with pytest.raises(ApiError) as refused:
        await _accept(**body)
    assert refused.value.code == code
    return refused.value


@pytest.mark.asyncio
async def test_a_model_that_is_no_candidate_is_refused(db, gates, document):
    await _hold(db, _make_candidate(document, access="protected"))
    await _register_model(db, COLUMNS)
    await _refusal("dbt.candidate.not_offered")
    assert await _products(db) == []


@pytest.mark.asyncio
async def test_a_candidate_not_yet_registered_is_refused(db, gates, candidate):
    await _hold(db, candidate)
    await _refusal("dbt.candidate.not_registered")
    assert await _products(db) == []


@pytest.mark.asyncio
async def test_a_table_missing_a_promised_column_is_refused(db, gates, candidate):
    await _hold(db, candidate)
    await _register_model(db, ("customer_id",))
    refused = await _refusal("dbt.candidate.columns_missing")
    assert refused.params == {"columns": "customer_name, lifetime_value"}
    assert await _products(db) == []


@pytest.mark.asyncio
async def test_a_product_id_in_use_is_refused(db, gates, candidate):
    await _hold(db, candidate)
    table_id = await _register_model(db, COLUMNS)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(schema_org.data_products).values(
                id="orders", domain_id="sales", name="Taken", status="draft"
            )
        )
    await _refusal("dbt.candidate.id_taken")
    async with db.acquire() as conn:
        assert await data_product_repo.member_table_ids(conn, "orders") == []
        assert await data_product_repo.products_of_table(conn, table_id) == []


@pytest.mark.asyncio
async def test_a_candidate_that_is_already_a_product_is_refused(db, gates, candidate):
    await _hold(db, candidate)
    await _register_model(db, COLUMNS)
    await _accept()
    await _refusal("dbt.candidate.already_a_product", product_id="orders_again")
    assert [p["id"] for p in await _products(db)] == ["orders"]


@pytest.mark.asyncio
async def test_a_source_with_no_manifest_is_refused(db, gates):
    with pytest.raises(ApiError) as refused:
        await _offers(db)
    assert refused.value.code == "dbt.source_not_registered"
