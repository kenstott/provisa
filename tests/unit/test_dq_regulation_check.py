# Copyright (c) 2026 Kenneth Stott
# Canary: e3a92c17-4b6d-4f80-9d25-71c0b8e4f6a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A regulation's check on a column's values is kept as a data quality rule citing its section
(REQ-2245).

``POST /admin/tables/{id}/regulation-checks`` adds a Soda ``failed_rows`` check to the checker that
scans the table and cites the library section, in one transaction. It is refused, storing nothing,
when no checker scans the table, when only a Great Expectations one does, when the column is not
read in plain by the caller, by the checker's own user or by a role that sees the checker's results,
when the section is not there, and without ``dq_rw``. The check's qualifier is its identity, so it
is the same when another check is added before it; a check removed from its contract takes its
citation with it.

The model: a table ``orders`` (column ``email``) in domain ``shop``; a Soda checker table scanning
it, whose results are granted to ``auditor``, signed in to by the user ``dq_bot``; a Great
Expectations source; and a policy document with the section "Personal data".
"""

# Requirements: REQ-2245

from __future__ import annotations

import contextlib
import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

from provisa.api.admin import regulation_checks_router as route
from provisa.api.errors import ApiError
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, reference_citations, registered_tables, sources
from provisa.dq import regulation
from provisa.dq.catalog import build_check_definition
from provisa.dq.contract import build_contract, contract_checks
from provisa.profiler import export
from provisa.reference_library import citations, store
from provisa.security.masking import MaskingRule, MaskType

DATASET = "pg/public/orders"
SECTION = "Personal data"
CONDITION = "email NOT LIKE '%@%'"
_STARS = MaskingRule(mask_type=MaskType.constant, value="***")
_FAKE = MaskingRule(mask_type=MaskType.fake, fake="email()")
_MISSING = {
    "column_name": "email",
    "check_type": "missing",
    "definition": build_check_definition("soda", "missing", column_name="email"),
}


def _role(rid: str) -> dict:
    return {"id": rid, "capabilities": ["query_development"], "domain_access": ["shop"]}


async def _checker(conn, source_id: str, checker: str, checks: list[dict]) -> int:
    table_id = await table_repo.upsert(
        conn,
        Table(
            source_id=source_id,
            domain_id="shop",
            schema_name="dq",
            table_name=f"orders_{source_id}_dq",
            dq_contract=build_contract(checker, DATASET, checks),
            columns=[Column(name="outcome", visible_to=["auditor"], data_type="text")],
        ),
    )
    assert table_id is not None
    return table_id


@pytest.fixture
async def model(control_plane, monkeypatch):
    """The model above, the app state the route reads, and the governance each role reads
    ``email`` through (``masks``: role -> rule, plain where absent)."""
    db = control_plane.org()
    mapping = {"host": "h", "port": 5432, "database": "d", "user": "dq_bot", "password": ""}
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="shop"))
        await conn.execute_core(
            insert(sources).values(
                [
                    {"id": "pg", "type": "postgresql"},
                    {"id": "sd", "type": "soda", "mapping": mapping},
                    {"id": "gx", "type": "great_expectations", "mapping": mapping},
                ]
            )
        )
        orders = await table_repo.upsert(
            conn,
            Table(
                source_id="pg",
                domain_id="shop",
                schema_name="public",
                table_name="orders",
                columns=[Column(name="email", visible_to=["analyst"], data_type="text")],
            ),
        )
        assert orders is not None
        soda = await _checker(conn, "sd", "soda", [_MISSING])
        document = await store.insert_document(
            conn,
            title="Privacy policy",
            source="https://example.com/privacy",
            source_kind=store.URL,
            media_type="text/markdown",
            content_sha256="a" * 64,
            status=store.KEPT,
            embedding_model="m",
            added_by="org_admin",
            domains=["*"],
            passages=[
                store.StoredPassage(
                    ordinal=0, section=SECTION, page=1, text="Emails are kept.", vector=[0.1, 0.9]
                )
            ],
        )
    masks: dict[str, MaskingRule] = {}
    hidden: set[str] = set()

    def _governance(role: str):
        served = {} if role in hidden else {orders: None}
        rules = {(orders, "email"): (masks[role], "text")} if role in masks else {}
        return types.SimpleNamespace(published={}, visible_columns=served, masking_rules=rules)

    roles = {r: _role(r) for r in ("analyst", "dq_bot", "auditor")}
    app_state = types.SimpleNamespace(
        model_db=db,
        contexts={},
        view_context=None,
        roles=roles,
        role_chains={},
        surfaces={r: object() for r in roles},
        meta_roles={},
        meta_resolved_at={},
        auth_config=None,
        auth_middleware_active=False,
        org_id="org1",
    )
    monkeypatch.setattr(route, "state", app_state)
    monkeypatch.setattr(route, "_governance", _governance)

    # Each checker's contract names the dataset orders is published as.
    def _target(_dataset, _contexts):
        return types.SimpleNamespace(table_id=orders)

    monkeypatch.setattr(export, "resolve_contract_target", _target)
    monkeypatch.setattr("provisa.auth.derived_roles.declared_derived_roles", lambda state: [])

    @contextlib.asynccontextmanager
    async def _bound():
        yield

    monkeypatch.setattr("provisa.core.secrets_store.bound_to_request_org", _bound)

    async def _rebuilt(*args, **kwargs):
        return None

    monkeypatch.setattr("provisa.api.app._rebuild_schemas", _rebuilt)
    whole = types.SimpleNamespace(whole=True)
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: whole)
    caps = {"dq_rw"}
    monkeypatch.setattr(
        "provisa.api.admin.capabilities._resolved_capabilities", lambda identity, state: caps
    )
    return types.SimpleNamespace(
        db=db,
        orders=orders,
        soda=soda,
        document=document,
        masks=masks,
        hidden=hidden,
        caps=caps,
        state=app_state,
    )


def _request(role: str | None = "analyst") -> Any:
    identity = types.SimpleNamespace(user_id="steward")
    return types.SimpleNamespace(state=types.SimpleNamespace(role=role, identity=identity))


def _body(m, **over) -> route.RegulationCheckInput:
    fields = {
        "column": "email",
        "condition": CONDITION,
        "documentId": m.document,
        "sectionHeading": SECTION,
    }
    return route.RegulationCheckInput(**{**fields, **over})


async def _contract(m, table_id: int) -> str:
    async with m.db.acquire() as conn:
        return (
            await conn.execute_core(
                select(registered_tables.c.dq_contract).where(registered_tables.c.id == table_id)
            )
        ).scalar_one()


async def _citations(m) -> int:
    async with m.db.acquire() as conn:
        return (
            await conn.execute_core(
                select(func.count())
                .select_from(reference_citations)
                .where(reference_citations.c.citer_kind == citations.DQ_CHECK)
            )
        ).scalar_one()


async def _refused(m, body=None, request: Any = None) -> ApiError:
    """The route's refusal, asserting it stored nothing: the contract as it was, no citation."""
    before = await _contract(m, m.soda)
    with pytest.raises(ApiError) as refused:
        await route.keep_regulation_check(request or _request(), m.orders, body or _body(m))
    assert await _contract(m, m.soda) == before
    assert await _citations(m) == 0
    assert "REQ-" not in str(refused.value.detail)
    return refused.value


# --- the object key -------------------------------------------------------------------------


def test_a_check_object_key_round_trips():
    key = regulation.CheckKey(12, "regulation_0123abcd", 7, "e|mail")
    text = regulation.check_object_key(key)
    assert text == "12|regulation_0123abcd|7|e|mail"
    assert regulation.parse_check_object_key(text) == key


def test_a_key_not_made_by_check_object_key_is_refused():
    with pytest.raises(ValueError):
        regulation.parse_check_object_key("12|regulation_x|7")
    with pytest.raises(ValueError):
        regulation.check_object_key(regulation.CheckKey(1, "a|b", 2, "c"))


def test_the_qualifier_is_the_rule_not_its_position():
    a = regulation.regulation_qualifier(7, "email", CONDITION, "section:1:heading:x")
    assert a == regulation.regulation_qualifier(7, "email", CONDITION, "section:1:heading:x")
    assert a != regulation.regulation_qualifier(7, "email", "email IS NULL", "section:1:heading:x")
    assert a != regulation.regulation_qualifier(7, "email", CONDITION, "section:1:heading:y")


def test_the_qualifier_holds_when_another_check_is_added_before_it():
    check = regulation.regulation_check("regulation_q1", CONDITION)
    contract = build_contract("soda", DATASET, [check])
    other = regulation.regulation_check("regulation_q0", "email IS NULL")
    before = build_contract("soda", DATASET, [other, *contract_checks(contract, "soda")])
    assert regulation.held_qualifiers(contract) == {"regulation_q1"}
    assert regulation.held_qualifiers(before) == {"regulation_q0", "regulation_q1"}
    [kept] = [c for c in contract_checks(before, "soda") if "regulation_q1" in c["definition"]]
    # Its position moved; its qualifier, and so its citation's object key, did not.
    assert contract_checks(before, "soda").index(kept) == 1


# --- keeping a check ------------------------------------------------------------------------


async def test_the_check_and_its_citation_are_kept_together(model):
    m = model
    kept = await route.keep_regulation_check(_request(), m.orders, _body(m))
    qualifier = kept["qualifier"]
    assert qualifier.startswith("regulation_")
    assert kept["objectKey"] == f"{m.soda}|{qualifier}|{m.orders}|email"
    assert kept["checkerTable"] == {
        "id": m.soda,
        "tableName": "orders_sd_dq",
        "sourceId": "sd",
        "checker": "soda",
    }
    checks = contract_checks(await _contract(m, m.soda), "soda")
    assert checks[0]["check_type"] == "missing"
    [failed] = [c for c in checks if c["check_type"] == "failed_rows"]
    assert failed["column_name"] == ""
    assert f"qualifier: {qualifier}" in failed["definition"]
    assert "expression:" in failed["definition"] and "NOT LIKE" in failed["definition"]
    async with m.db.acquire() as conn:
        held = await citations.of_citers(conn, citations.DQ_CHECK)
    [cited] = held[(None, kept["objectKey"])]
    assert (cited.document_title, cited.section_heading, cited.cited_by, cited.state) == (
        "Privacy policy",
        SECTION,
        "steward",
        citations.CURRENT,
    )
    assert kept["citation"]["sectionHeading"] == SECTION
    assert kept["citation"]["state"] == citations.CURRENT


async def test_keeping_the_same_rule_twice_is_refused_and_adds_nothing(model):
    m = model
    await route.keep_regulation_check(_request(), m.orders, _body(m))
    contract = await _contract(m, m.soda)
    with pytest.raises(ApiError) as again:
        await route.keep_regulation_check(_request(), m.orders, _body(m))
    assert again.value.status_code == 409 and "already keeps" in str(again.value.detail)
    assert await _contract(m, m.soda) == contract
    assert await _citations(m) == 1


async def test_no_checker_scanning_the_table_is_refused_by_name(model, monkeypatch):
    m = model

    async def _none(conn, contexts, table_ids):
        return []

    monkeypatch.setattr(export, "checkers_scanning", _none)
    refused = await _refused(m)
    assert refused.status_code == 409
    assert "no Soda or Great Expectations checker source scans table 'orders'" in str(
        refused.detail
    )


async def test_a_table_scanned_only_by_great_expectations_is_refused_by_name(model, monkeypatch):
    m = model
    gx_check = {
        "column_name": "email",
        "check_type": "expect_column_values_to_not_be_null",
        "definition": build_check_definition(
            "great_expectations", "expect_column_values_to_not_be_null", column_name="email"
        ),
    }
    async with m.db.acquire() as conn:
        gx = await _checker(conn, "gx", "great_expectations", [gx_check])
    real = export.checkers_scanning

    async def _gx_only(conn, contexts, table_ids):
        return [c for c in await real(conn, contexts, table_ids) if c.checker != "soda"]

    monkeypatch.setattr(export, "checkers_scanning", _gx_only)
    gx_before = await _contract(m, gx)
    refused = await _refused(m)
    assert refused.status_code == 409
    assert "a Soda checker is needed" in str(refused.detail)
    assert f"orders_gx_dq (id {gx})" in str(refused.detail)
    assert await _contract(m, gx) == gx_before


async def test_naming_a_great_expectations_checker_is_refused(model):
    m = model
    gx_check = {
        "column_name": "email",
        "check_type": "expect_column_values_to_not_be_null",
        "definition": build_check_definition(
            "great_expectations", "expect_column_values_to_not_be_null", column_name="email"
        ),
    }
    async with m.db.acquire() as conn:
        gx = await _checker(conn, "gx", "great_expectations", [gx_check])
    refused = await _refused(m, _body(m, checkerTableId=gx))
    assert "is a Great Expectations checker; a Soda checker is needed" in str(refused.detail)


@pytest.mark.parametrize(
    ("rule", "state"), [(_STARS, "masked"), (_FAKE, "faked"), (None, "hidden")]
)
async def test_a_column_not_plain_for_the_caller_is_refused(model, rule, state):
    m = model
    if rule is None:
        m.hidden.add("analyst")
    else:
        m.masks["analyst"] = rule
    refused = await _refused(m)
    assert refused.status_code == 409
    assert f"{state} for you" in str(refused.detail)
    assert "***" not in str(refused.detail)


async def test_a_column_not_plain_for_the_checker_user_is_refused(model):
    m = model
    m.masks["dq_bot"] = _STARS
    refused = await _refused(m)
    assert "masked for the checker's user 'dq_bot'" in str(refused.detail)
    assert "for you" not in str(refused.detail)


async def test_a_column_not_plain_for_a_role_that_sees_the_results_is_refused(model):
    m = model
    m.masks["auditor"] = _FAKE
    refused = await _refused(m)
    assert "faked for role 'auditor'" in str(refused.detail)


async def test_a_role_not_served_the_results_is_not_asked(model):
    m = model
    m.state.roles["outsider"] = _role("outsider")
    m.state.surfaces["outsider"] = object()
    m.masks["outsider"] = _STARS
    kept = await route.keep_regulation_check(_request(), m.orders, _body(m))
    assert kept["qualifier"]


async def test_a_section_that_is_not_there_is_refused(model):
    m = model
    refused = await _refused(m, _body(m, sectionHeading="Retention"))
    assert refused.code == citations.CitationRefused.code
    assert "has no section at heading 'Retention'" in str(refused.detail)


async def test_a_document_that_is_not_there_is_refused(model):
    m = model
    refused = await _refused(m, _body(m, documentId=m.document + 99))
    assert refused.status_code == 404


async def test_without_dq_rw_it_is_refused(model):
    m = model
    m.caps.clear()
    refused = await _refused(m)
    assert (refused.status_code, refused.code) == (403, "auth.missing_capability")
    assert "dq_rw" in str(refused.detail)


# --- removal ----------------------------------------------------------------------------------


async def test_removing_the_check_from_its_contract_uncites_it(model):
    m = model
    kept = await route.keep_regulation_check(_request(), m.orders, _body(m))
    second = await route.keep_regulation_check(
        _request(), m.orders, _body(m, condition="email IS NULL")
    )
    contract = await _contract(m, m.soda)
    remaining = [
        c
        for c in contract_checks(contract, "soda")
        if f"qualifier: {kept['qualifier']}" not in c["definition"]
    ]
    async with m.db.acquire() as conn:
        stored = await table_repo.get(conn, m.soda)
        assert stored is not None
        await table_repo.upsert(
            conn,
            Table(
                source_id="sd",
                domain_id="shop",
                schema_name="dq",
                table_name=stored["table_name"],
                dq_contract=build_contract("soda", DATASET, remaining),
                columns=[Column(name="outcome", visible_to=["auditor"], data_type="text")],
            ),
        )
        held = await citations.of_citers(conn, citations.DQ_CHECK)
    assert set(held) == {(None, second["objectKey"])}


async def test_deleting_the_checker_table_uncites_its_checks(model):
    m = model
    await route.keep_regulation_check(_request(), m.orders, _body(m))
    async with m.db.acquire() as conn:
        assert await table_repo.delete(conn, m.soda)
    assert await _citations(m) == 0
