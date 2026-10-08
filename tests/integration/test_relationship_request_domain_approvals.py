# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3b6e2f-1a47-4c95-b0e6-f72a9c5d1e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship request is decided by the domains it touches (REQ-1948), through the admin API.

The org schema is built by the real ``init_schema``; two domains each hold one table. A member
without the right asks for a sales -> finance relationship, which queues a request. The request
is then decided over the REST queue by modelers whose right to create relationships reaches
sales, finance, or a third domain the request does not touch.
"""

# Requirements: REQ-1948, REQ-1944, REQ-1531

from __future__ import annotations

import os
import types
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_common, schema_helpers, schema_mutation
from provisa.api.admin.creation_requests_router import router
from provisa.api.errors import ApiError
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.schema_org import (
    admin_audit_log,
    domains,
    registered_tables,
    relationships,
    roles,
    sources,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORG_ID = "req1948"
_SCHEMA = f"org_{_ORG_ID}"
_SCHEMA_SQL = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "provisa", "core", "schema.sql")
)

# user -> the role they hold. Each modeler role carries create_relationship in one domain.
_USERS = {
    "asker": "sales_reader",
    "sam": "sales_modeler",
    "sue": "sales_modeler",
    "fay": "finance_modeler",
    "fred": "finance_modeler",
    "hal": "hr_modeler",
    "olga": "org_modeler",
}


async def _drop(db: Database) -> None:
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")


def _api() -> FastAPI:
    """The queue router behind a stand-in for sign-in: the caller is named by a header."""
    app = FastAPI()

    @app.middleware("http")
    async def _sign_in(request: Request, call_next):  # pyright: ignore[reportUnusedFunction]
        user = request.headers["x-user"]
        request.state.identity = types.SimpleNamespace(user_id=user, roles=[_USERS[user]])
        return await call_next(request)

    @app.exception_handler(ApiError)
    async def _api_error(_req: Request, exc: ApiError):  # pyright: ignore[reportUnusedFunction]
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail, "code": exc.code, "params": exc.params},
        )

    app.include_router(router)
    return app


@pytest.fixture
async def plane(monkeypatch):
    engine = create_engine_from_url(_ASYNC_URL)
    db = Database(engine, name="tenant", search_path=_SCHEMA)
    await _drop(db)
    with open(_SCHEMA_SQL, encoding="utf-8") as fh:
        await init_schema(db, fh.read(), org_id=_ORG_ID)
    from provisa.core.models import Column, ProvisaConfig, Table
    from provisa.core.repositories import role as role_repo
    from provisa.core.repositories import table as table_repo

    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        for domain_id in ("sales", "finance", "hr"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        for table_name, domain_id in (
            ("orders", "sales"),
            ("customers", "sales"),
            ("invoices", "finance"),
        ):
            await table_repo.upsert(
                conn,
                Table(
                    source_id="pg",
                    domain_id=domain_id,
                    schema_name="public",
                    table_name=table_name,
                    columns=[
                        Column(name="id", visible_to=[], data_type="integer"),
                        Column(name="ref_id", visible_to=[], data_type="integer"),
                    ],
                ),
            )
        for role_id, caps, access in (
            ("sales_reader", ["query_development"], ["sales"]),
            ("sales_modeler", ["create_relationship"], ["sales"]),
            ("finance_modeler", ["create_relationship"], ["finance"]),
            ("hr_modeler", ["create_relationship"], ["hr"]),
            ("org_modeler", ["create_relationship"], ["*"]),
        ):
            await conn.execute_core(
                insert(roles).values(id=role_id, capabilities=caps, domain_access=access)
            )
        loaded = {r["id"]: r for r in await role_repo.list_all(conn)}
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", loaded, raising=False)
    monkeypatch.setattr(
        appmod.state,
        "config",
        ProvisaConfig.model_validate({"sources": [], "domains": [], "tables": [], "roles": []}),
        raising=False,
    )
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _pool():
        return db

    async def _noop() -> None:
        return None

    for module in (schema_mutation, schema_helpers, schema_common):
        monkeypatch.setattr(module, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _noop)
    monkeypatch.setattr(appmod, "_rebuild_schemas", _noop)
    transport = httpx.ASGITransport(app=_api())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield types.SimpleNamespace(db=db, client=client)
    await _drop(db)
    engine.dispose()


def _info(user: str) -> Any:
    identity = types.SimpleNamespace(user_id=user, roles=[_USERS[user]])
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id=_ORG_ID)
    )
    return types.SimpleNamespace(context={"request": request})


async def _ask(user: str, rel_id: str, source: str, target: str) -> int:
    """``user`` asks for a relationship they may not create; the id of the queued request."""
    from provisa.api.admin.types import RelationshipInput

    queued = await schema_mutation._upsert_relationship_impl(
        _info(user),
        RelationshipInput(
            id=rel_id,
            source_table_id=source,
            target_table_id=target,
            source_column="ref_id",
            target_column="id",
            cardinality="many-to-one",
        ),
    )
    assert queued.code == "schema.relationship_request_queued", queued.message
    assert isinstance(queued.params, dict)
    return queued.params["id"]


async def _post(plane, user: str, path: str, **json: Any) -> httpx.Response:
    return await plane.client.post(
        f"/admin/creation-requests/{path}", headers={"x-user": user}, json=json or None
    )


async def _listed(plane, user: str) -> dict[int, dict]:
    resp = await plane.client.get("/admin/creation-requests/", headers={"x-user": user})
    assert resp.status_code == 200, resp.text
    return {r["id"]: r for r in resp.json()}


async def _stored(plane, rel_id: str):
    async with plane.db.acquire() as conn:
        result = await conn.execute_core(
            select(relationships.c.owner, relationships.c.needs_review).where(
                relationships.c.id == rel_id
            )
        )
        return result.fetchone()


async def test_a_queued_relationship_request_needs_two_approvals(plane):
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")
    row = (await _listed(plane, "sam"))[rid]
    assert row["required_approvals"] == 2
    assert row["domains"] == ["finance", "sales"]
    assert row["waiting_on"] == ["finance", "sales"]


async def test_an_approver_outside_the_requests_domains_is_refused(plane):
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")
    resp = await _post(plane, "hal", f"{rid}/approve")
    assert resp.status_code == 403
    assert resp.json()["code"] == "requests.approver_outside_domains"
    assert resp.json()["params"] == {"domains": "finance, sales"}
    assert (await _listed(plane, "sam"))[rid]["approvals"] == []


async def test_two_approvals_from_one_side_do_not_execute_a_cross_domain_request(plane):
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")
    assert (await _post(plane, "sam", f"{rid}/approve")).status_code == 200
    second = await _post(plane, "sue", f"{rid}/approve")
    assert second.status_code == 200
    assert second.json()["status"] == "pending"
    assert second.json()["waiting_on"] == ["finance"]
    assert await _stored(plane, "orders_invoices") is None

    # Execute is not a way around the rule.
    pressed = await _post(plane, "sam", f"{rid}/execute")
    assert pressed.status_code == 409
    assert pressed.json()["code"] == "requests.waiting_on_domains"
    assert pressed.json()["params"] == {"domains": "finance"}
    assert await _stored(plane, "orders_invoices") is None

    # The other side's yes completes it, and the relationship exists.
    third = await _post(plane, "fay", f"{rid}/approve")
    assert third.status_code == 200 and third.json()["status"] == "executed"
    assert await _stored(plane, "orders_invoices") is not None


async def test_a_yes_from_each_side_executes_and_creates_the_relationship(plane):
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")
    first = await _post(plane, "sam", f"{rid}/approve")
    assert first.json()["status"] == "pending" and first.json()["waiting_on"] == ["finance"]
    second = await _post(plane, "fay", f"{rid}/approve")
    assert second.status_code == 200, second.text
    assert second.json()["status"] == "executed"
    stored = await _stored(plane, "orders_invoices")
    # Owned by the approver whose yes completed it; both domains have seen it, so no review flag.
    assert stored is not None and stored.owner == "fay" and stored.needs_review is False
    again = await _post(plane, "sue", f"{rid}/approve")
    assert again.status_code == 409


async def test_one_approver_reaching_both_sides_is_one_approval(plane):
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")
    only = await _post(plane, "olga", f"{rid}/approve")
    assert only.json()["status"] == "pending" and only.json()["waiting_on"] == []
    pressed = await _post(plane, "olga", f"{rid}/execute")
    assert pressed.status_code == 409
    assert pressed.json()["code"] == "requests.approvals_incomplete"
    repeat = await _post(plane, "olga", f"{rid}/approve")
    assert repeat.status_code == 403 and repeat.json()["code"] == "requests.already_approved"
    assert await _stored(plane, "orders_invoices") is None


async def test_a_same_domain_request_takes_two_approvers_of_that_domain(plane):
    rid = await _ask("asker", "orders_customers", "orders", "customers")
    assert (await _listed(plane, "sam"))[rid]["domains"] == ["sales"]
    refused = await _post(plane, "fay", f"{rid}/approve")
    assert refused.status_code == 403
    assert refused.json()["code"] == "requests.approver_outside_domains"
    assert (await _post(plane, "sam", f"{rid}/approve")).json()["status"] == "pending"
    assert (await _post(plane, "sue", f"{rid}/approve")).json()["status"] == "executed"
    assert await _stored(plane, "orders_customers") is not None


async def test_the_requester_cannot_decide_their_own_request(plane):
    # fay holds the right in finance, so asking for a relationship owned by a sales table queues
    # a request -- one her right reaches, and which she still may not decide.
    rid = await _ask("fay", "orders_invoices", "orders", "invoices")
    for path, body in ((f"{rid}/approve", {}), (f"{rid}/reject", {"reason": "duplicate"})):
        resp = await _post(plane, "fay", path, **body)
        assert resp.status_code == 403, resp.text
        assert resp.json()["code"] == "requests.own_request"
    mine = (await _listed(plane, "fay"))[rid]
    assert mine["can_decide"] is False and mine["status"] == "pending"
    assert mine["approve_refusal"]["code"] == "requests.own_request"


async def test_a_rejection_comes_from_any_user_who_could_approve(plane):
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")
    outside = await _post(plane, "hal", f"{rid}/reject", reason="duplicate")
    assert outside.status_code == 403
    assert outside.json()["code"] == "requests.approver_outside_domains"
    rejected = await _post(plane, "fred", f"{rid}/reject", reason="duplicate")
    assert rejected.status_code == 200 and rejected.json()["status"] == "rejected"
    assert await _stored(plane, "orders_invoices") is None


async def test_the_list_shows_what_a_user_can_decide_and_what_they_made(plane):
    cross = await _ask("asker", "orders_invoices", "orders", "invoices")
    inside = await _ask("asker", "orders_customers", "orders", "customers")
    assert set(await _listed(plane, "sam")) == {cross, inside}
    assert set(await _listed(plane, "fay")) == {cross}
    assert set(await _listed(plane, "hal")) == set()
    mine = await _listed(plane, "asker")
    assert set(mine) == {cross, inside}
    assert all(r["can_decide"] is False for r in mine.values())
    assert (await _listed(plane, "sam"))[cross]["can_decide"] is True

    await _post(plane, "sam", f"{cross}/approve")
    assert (await _listed(plane, "fay"))[cross]["waiting_on"] == ["finance"]
    # The page is told beforehand whose approval would be refused, and why.
    assert (await _listed(plane, "fay"))[cross]["approve_refusal"] is None
    again = (await _listed(plane, "sam"))[cross]
    assert again["can_decide"] is True
    assert again["approve_refusal"]["code"] == "requests.already_approved"


async def test_the_graphql_execute_is_held_to_the_same_rule(plane):
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")
    m = schema_mutation.Mutation()
    early = await m.execute_creation_request(_info("sam"), rid)  # pyright: ignore[reportCallIssue]
    assert early.success is False and early.code == "requests.waiting_on_domains"
    outside = await m.reject_creation_request(  # pyright: ignore[reportCallIssue]
        _info("hal"), rid, "duplicate"
    )
    assert outside.success is False and outside.code == "requests.approver_outside_domains"
    assert await _stored(plane, "orders_invoices") is None


async def _trail(plane) -> list[tuple[str, str, dict]]:
    async with plane.db.acquire() as conn:
        result = await conn.execute_core(
            select(
                admin_audit_log.c.action, admin_audit_log.c.actor_id, admin_audit_log.c.detail
            ).order_by(admin_audit_log.c.id)
        )
        return [(r.action, r.actor_id, r.detail) for r in result.fetchall()]


async def test_every_decision_and_every_refusal_is_written_to_the_trail(plane):
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")
    await _post(plane, "hal", f"{rid}/approve")  # refused: outside
    await _post(plane, "sam", f"{rid}/approve")
    await _post(plane, "sam", f"{rid}/execute")  # refused: finance not heard from
    await _post(plane, "fay", f"{rid}/approve")  # completes, creates
    other = await _ask("asker", "orders_customers", "orders", "customers")
    await _post(plane, "sue", f"{other}/reject", reason="duplicate")

    trail = await _trail(plane)
    assert [(action, actor, detail["outcome"]) for action, actor, detail in trail] == [
        ("relationship_request.approve", "hal", "refused"),
        ("relationship_request.approve", "sam", "done"),
        ("relationship_request.execute", "sam", "refused"),
        ("relationship_request.approve", "fay", "done"),
        ("relationship_request.execute", "fay", "done"),
        ("relationship_request.reject", "sue", "done"),
    ]
    refused, approved = trail[0][2], trail[1][2]
    assert refused == {
        "request_id": rid,
        "requested_by": "asker",
        "domains": ["finance", "sales"],
        "domains_reached": [],
        "outcome": "refused",
        "refusal": "requests.approver_outside_domains",
    }
    assert approved["domains_reached"] == ["sales"] and approved["request_id"] == rid
    assert trail[2][2]["refusal"] == "requests.waiting_on_domains"


async def test_no_path_carries_out_a_request_the_rule_has_not_passed(plane):
    m = schema_mutation.Mutation()
    rid = await _ask("asker", "orders_invoices", "orders", "invoices")

    async def _attempts(expected: str) -> None:
        for user in ("sam", "fay", "olga"):
            rest = await _post(plane, user, f"{rid}/execute")
            assert rest.status_code == 409 and rest.json()["code"] == expected, rest.text
            gql = await m.execute_creation_request(  # pyright: ignore[reportCallIssue]
                _info(user), rid
            )
            assert gql.success is False and gql.code == expected
        # Outside the request's domains, and the requester: refused before the count is read.
        for user, code in (
            ("hal", "requests.approver_outside_domains"),
            ("asker", "requests.approver_outside_domains"),
        ):
            assert (await _post(plane, user, f"{rid}/execute")).json()["code"] == code
            gql = await m.execute_creation_request(  # pyright: ignore[reportCallIssue]
                _info(user), rid
            )
            assert gql.success is False and gql.code == code
        assert await _stored(plane, "orders_invoices") is None
        assert (await _listed(plane, "sam"))[rid]["status"] == "pending"

    await _attempts("requests.waiting_on_domains")  # no approvals at all
    await _post(plane, "sam", f"{rid}/approve")
    await _attempts("requests.waiting_on_domains")  # one side
    await _post(plane, "sue", f"{rid}/approve")
    await _attempts("requests.waiting_on_domains")  # two approvals, still one side


async def test_a_request_submitted_over_rest_is_held_to_the_same_count(plane):
    payload = {
        "id": "orders_customers",
        "source_table_id": "orders",
        "target_table_id": "customers",
        "source_column": "ref_id",
        "target_column": "id",
        "cardinality": "many-to-one",
    }
    made = await _post(
        plane,
        "asker",
        "",
        request_type="relationship",
        capability="create_relationship",
        payload=payload,
    )
    assert made.status_code == 200, made.text
    rid = made.json()["id"]
    assert (await _listed(plane, "sam"))[rid]["required_approvals"] == 2
    assert (await _post(plane, "sam", f"{rid}/approve")).json()["status"] == "pending"
    assert await _stored(plane, "orders_customers") is None


async def test_a_creation_that_fails_leaves_the_request_pending_for_a_retry(plane):
    from provisa.api.admin.types import RelationshipInput

    queued = await schema_mutation._upsert_relationship_impl(
        _info("asker"),
        RelationshipInput(
            id="orders_customers",
            source_table_id="orders",
            target_table_id="customers",
            source_column="ref_id",
            target_column="id",
            cardinality="sideways",
        ),
    )
    assert isinstance(queued.params, dict)
    rid = queued.params["id"]
    await _post(plane, "sam", f"{rid}/approve")
    completing = await _post(plane, "sue", f"{rid}/approve")
    assert completing.status_code == 422
    assert completing.json()["code"] == "schema.invalid_cardinality"
    row = (await _listed(plane, "sam"))[rid]
    assert row["status"] == "pending" and len(row["approvals"]) == 2
    retry = await _post(plane, "sam", f"{rid}/execute")
    assert retry.status_code == 422 and retry.json()["code"] == "schema.invalid_cardinality"
    assert await _stored(plane, "orders_customers") is None


async def test_a_request_whose_tables_are_gone_can_only_be_cleared(plane):
    from sqlalchemy import delete

    rid = await _ask("asker", "orders_customers", "orders", "customers")
    async with plane.db.acquire() as conn:
        await conn.execute_core(
            delete(registered_tables).where(
                registered_tables.c.table_name.in_(["orders", "customers"])
            )
        )
    for user in ("sam", "olga"):
        resp = await _post(plane, user, f"{rid}/approve")
        assert resp.status_code == 403
        assert resp.json()["code"] == "requests.tables_not_registered"
        assert resp.json()["params"] == {"tables": "orders, customers"}
    narrow = await _post(plane, "sam", f"{rid}/reject", reason="source_not_registered")
    assert narrow.status_code == 403
    cleared = await _post(plane, "olga", f"{rid}/reject", reason="source_not_registered")
    assert cleared.status_code == 200 and cleared.json()["status"] == "rejected"
