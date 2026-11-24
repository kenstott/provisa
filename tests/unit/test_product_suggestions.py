# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4b1f63-7a28-4e05-b3c9-2f6e8a1d5c70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-10-28, SUGGEST A PRODUCT FROM A LISTED TABLE): a reader suggests a data
product from tables the catalog lists to them; the suggestion goes to the domain's data product
owners and data stewards by email, and is counted per table. A table the reader does not see is
refused by name."""

# Requirements: REQ-2180, REQ-2153

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import product_suggestions_router as router
from provisa.api.errors import ApiError
from provisa.core.schema_org import (
    data_products,
    domains,
    product_suggestion_tables,
    product_suggestions,
    registered_tables,
    roles,
    sources,
    table_columns,
    user_directory,
    user_role_assignments,
)
from provisa.entitlements.owners import set_domain_stewards, set_product_owners

pytestmark = pytest.mark.unit

ANA, BEN, EVE = "ana", "ben", "eve"  # ana owns c360, ben stewards sales, eve reads hr only
_OWNER_CAPS = ["data_product_owner", "domain_steward"]

#: id, domain, name, the scope of its one column
_TABLES = (
    (1, "sales", "orders", "listed"),
    (2, "sales", "customers", "public"),
    (3, "sales", "salaries", "domain"),
    (4, "ops2", "jobs", "listed"),
)


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[Any] = []

    def send(self, message: Any) -> None:
        self.sent.append(message)


@pytest.fixture
async def db(monkeypatch, control_plane):
    """Domains sales and ops2; sales holds orders (a listed column), customers (a public column)
    and salaries (a domain column only), and product c360, owned by ana; ben stewards sales.
    ops2 holds jobs (a listed column) and has no owner or steward. eve reaches hr only."""
    org = control_plane.org()
    async with org.acquire() as conn:
        for domain_id in ("sales", "ops2", "hr"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        for tid, domain_id, name, scope in _TABLES:
            await conn.execute_core(
                insert(registered_tables).values(
                    id=tid,
                    source_id="pg",
                    domain_id=domain_id,
                    schema_name="public",
                    table_name=name,
                )
            )
            await conn.execute_core(
                insert(table_columns).values(
                    table_id=tid, column_name="id", data_type="integer", scope=scope
                )
            )
        await conn.execute_core(
            insert(data_products).values(id="c360", domain_id="sales", name="C360")
        )
        await conn.execute_core(
            insert(roles).values(
                id="owners", capabilities=_OWNER_CAPS, domain_access=["*"], parents=[]
            )
        )
        await conn.execute_core(
            insert(roles).values(id="reader", capabilities=[], domain_access=["hr"], parents=[])
        )
        for user, role_id in ((ANA, "owners"), (BEN, "owners"), (EVE, "reader")):
            await conn.execute_core(
                insert(user_role_assignments).values(user_id=user, role_id=role_id, domain_id="*")
            )
            await conn.execute_core(
                insert(user_directory).values(user_id=user, email=f"{user}@example.com")
            )
        async with conn.transaction():
            await set_product_owners(conn, "c360", "sales", [ANA], set_by="root")
            await set_domain_stewards(conn, "sales", [BEN], set_by="root")

    monkeypatch.setattr(appmod.state, "model_db", org, raising=False)
    monkeypatch.setattr(appmod.state, "names_table", None, raising=False)
    monkeypatch.setattr(appmod.state, "held_entitlements", {}, raising=False)
    monkeypatch.setattr(appmod.state, "role_owner_rows", {}, raising=False)
    monkeypatch.setattr(
        appmod.state,
        "config",
        SimpleNamespace(mail=SimpleNamespace(provider="smtp")),
        raising=False,
    )
    monkeypatch.setattr(appmod.state, "admin_db", object(), raising=False)
    monkeypatch.setattr(router, "allowed_domains_request", _reach)
    return org


@pytest.fixture
def mailer(monkeypatch) -> FakeSender:
    """The mail boundary: the sender every notice goes through, and the attempts recorded."""
    sender = FakeSender()
    monkeypatch.setattr("provisa.core.mail.email_sender", lambda _cfg: sender)
    sender.recorded = []  # type: ignore[attr-defined]

    async def record(_db, attempt):
        sender.recorded.append(attempt)  # type: ignore[attr-defined]

    monkeypatch.setattr("provisa.core.mail_stats.record", record)
    return sender


def _reach(request: Any) -> frozenset[str] | None:
    """eve reaches hr, so sales and ops2 are listed to her, names only; ana and ben reach every
    domain."""
    return frozenset({"hr"}) if request.state.identity.user_id == EVE else None


def _request(user_id: str = EVE) -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(
            identity=SimpleNamespace(user_id=user_id), active_org_id="acme", role="reader"
        )
    )


def _body(*tables: str, domain_id: str = "sales", reason: str = "churn study") -> Any:
    return router.SuggestionBody(domain_id=domain_id, tables=list(tables), reason=reason)


async def _counts() -> dict[str, int]:
    rows = await router.suggestion_counts(_request(BEN))
    return {r["table_name"]: r["suggestions"] for r in rows}


async def _stored(db) -> int:
    async with db.acquire() as conn:
        return len((await conn.execute_core(select(product_suggestions.c.id))).fetchall())


async def test_a_suggestion_is_recorded_and_mailed_to_the_domains_owners_and_stewards(db, mailer):
    out = await router.suggest(_body("orders", "customers"), _request())
    assert (out["domain_id"], out["suggested_by"], out["reason"]) == ("sales", EVE, "churn study")
    assert out["tables"] == ["customers", "orders"]
    assert out["recipients"] == [ANA, BEN]
    async with db.acquire() as conn:
        named = (
            await conn.execute_core(
                select(product_suggestion_tables.c.table_id).order_by(
                    product_suggestion_tables.c.table_id
                )
            )
        ).fetchall()
    assert [r.table_id for r in named] == [1, 2]
    assert sorted(m.to for m in mailer.sent) == ["ana@example.com", "ben@example.com"]
    (message, *_) = mailer.sent
    assert "sales.customers, sales.orders" in message.subject
    assert "churn study" in message.body and EVE in message.body
    assert {(a.kind, a.succeeded) for a in mailer.recorded} == {(router.MAIL_KIND, True)}  # type: ignore[attr-defined]


async def test_suggestions_are_counted_per_table(db, mailer):
    await router.suggest(_body("orders"), _request())
    assert await _counts() == {"orders": 1}
    await router.suggest(_body("orders", "customers"), _request())
    assert await _counts() == {"orders": 2, "customers": 1}
    rows = await router.suggestion_counts(_request(BEN))
    assert [r["table_name"] for r in rows] == ["orders", "customers"]  # most wanted first


async def test_counts_answer_only_the_domains_the_caller_reaches(db, mailer):
    await router.suggest(_body("orders"), _request())
    assert await router.suggestion_counts(_request()) == []  # eve reaches hr alone


@pytest.mark.parametrize("table", ["salaries", "no_such_table"])
async def test_a_table_the_reader_does_not_see_is_refused_by_name(db, mailer, table):
    with pytest.raises(ApiError) as refused:
        await router.suggest(_body("orders", table), _request())
    assert refused.value.status_code == 404
    assert refused.value.code == "product_suggestion.table_not_listed"
    assert f"sales.{table}" in str(refused.value.detail)
    assert "sales.orders" not in str(refused.value.detail)
    assert refused.value.params == {"tables": f"sales.{table}"}
    assert await _stored(db) == 0
    assert mailer.sent == []


async def test_a_table_in_a_reached_domain_needs_no_listed_column(db, mailer, monkeypatch):
    monkeypatch.setattr(router, "allowed_domains_request", lambda _request: None)
    out = await router.suggest(_body("salaries"), _request())
    assert out["tables"] == ["salaries"]


async def test_a_domain_with_no_owner_or_steward_is_refused(db, mailer):
    with pytest.raises(ApiError) as refused:
        await router.suggest(_body("jobs", domain_id="ops2"), _request())
    assert (refused.value.status_code, refused.value.code) == (
        409,
        "product_suggestion.no_recipient",
    )
    assert "ops2" in str(refused.value.detail)
    assert refused.value.params == {"domain": "ops2"}
    assert await _stored(db) == 0


async def test_a_suggestion_states_its_reason(db, mailer):
    with pytest.raises(ApiError) as refused:
        await router.suggest(_body("orders", reason="  "), _request())
    assert refused.value.code == "product_suggestion.reason_required"
    assert await _stored(db) == 0
    assert mailer.sent == []


async def test_a_suggestion_names_a_table(db, mailer):
    with pytest.raises(ApiError) as refused:
        await router.suggest(_body(), _request())
    assert refused.value.code == "product_suggestion.tables_required"


async def test_a_suggestion_is_made_signed_in(db, mailer):
    with pytest.raises(ApiError) as refused:
        await router.suggest(_body("orders"), _request("anonymous"))
    assert refused.value.status_code == 401


async def test_the_suggester_is_not_mailed_their_own_suggestion(db, mailer):
    await router.suggest(_body("orders"), _request(BEN))
    assert [m.to for m in mailer.sent] == ["ana@example.com"]
