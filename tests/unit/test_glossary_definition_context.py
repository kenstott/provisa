# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7b2c19-8d3a-4f61-b05e-c2a9d8f1e734
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1387 (amended 2026-11-18): the definition assistant drafts from the bound columns' and
tables' own descriptions, types and sample values, and from related terms' definitions.

A sales table (``orders``, aliased ``Sales Orders``) and an hr table (``staff``) each hold a
``customer_id`` column, so one term ``customer`` is bound to both. The sample values are read as
the curator through the governed pipeline: the read is stubbed at discovery's seams
(:mod:`provisa.discovery.gather`, as :mod:`provisa.api.admin.definition_context` imports them) and
answers masked values to the curator's role and raw ones to any other, so a raw value in the
prompt would mean the read was not the curator's. Handlers are awaited directly with a fake
request over a real control-plane store, as in test_glossary_ai_selected_domains.py.
"""

# Requirements: REQ-1387, REQ-2253

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert

from provisa.api.admin import definition_context, glossary_router, schema_helpers
from provisa.core import org_secrets
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources
from tests.unit.gate_identity import serve_every_column

if TYPE_CHECKING:
    from fastapi import Request

pytestmark = pytest.mark.asyncio

CURATOR = "steward"


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


def _request() -> "Request":
    fake = types.SimpleNamespace(
        headers={},
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="acme",
            role=CURATOR,
        ),
    )
    return cast("Request", fake)


def _column(name: str, description: str | None, data_type: str = "varchar") -> Column:
    return Column(name=name, data_type=data_type, visible_to=[], description=description)


class _Reads:
    """The governed reads issued, by role and SQL, answering masked values to the curator."""

    def __init__(self) -> None:
        self.issued: list[tuple[str, str]] = []

    async def governed_as(self, sql: str, role: str) -> list[tuple]:
        self.issued.append((role, sql))
        if role != CURATOR:
            return [("C-1001",), ("C-1002",)]
        return [("C-****",), ("C-****",), ("X-****",)]


@asynccontextmanager
async def _store(control_plane, monkeypatch, sensitive: frozenset[str] = frozenset()):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        for d in ("sales", "hr"):
            c.execute(insert(domains).values(id=d, description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    db = control_plane.org()

    async def _pool():
        return db

    async def _notify(org_id: str, reason: str) -> None:
        return None

    async def _no_jev(_pool, _vendor):
        return None

    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    monkeypatch.setattr("provisa.api.admin.reference_library_router.embed_terms", AsyncMock())
    monkeypatch.setattr(org_secrets, "org_ai_key_or_none", _no_jev)
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)

    reads = _Reads()

    async def _never_read(state, conn, ids):
        return set()

    def _published_as(state, role, table_id):
        return f'"t{table_id}"', {
            "customer_id": "customer_id",
            "order_dt": "order_dt",
            "card_no": "card_no",
            "hire_dt": "hire_dt",
        }

    async def _profiled(conn, table_id, table_name):
        return {name: {"payment_card"} for name in sensitive}

    async def _decided(conn, ids):
        return {}

    monkeypatch.setattr(definition_context, "tables_never_read", _never_read)
    monkeypatch.setattr(definition_context, "_published_as", _published_as)
    monkeypatch.setattr(definition_context, "_profiled_classes", _profiled)
    monkeypatch.setattr(definition_context, "_decided", _decided)
    monkeypatch.setattr(definition_context, "_governed_as", reads.governed_as)

    async with db.acquire() as conn:
        orders = Table(
            source_id="__derived__",
            domain_id="sales",
            schema_name="s",
            table_name="orders",
            alias="Sales Orders",
            description="One row per order placed by a buyer.",
            columns=[
                _column("customer_id", "FK to cust_mstr.cust_key, NOT NULL"),
                _column("order_dt", None, "date"),
                _column("card_no", "PAN, PCI scope"),
            ],
            view_sql="SELECT 1",
        )
        staff = Table(
            source_id="__derived__",
            domain_id="hr",
            schema_name="s",
            table_name="staff",
            description="Employees and their managers.",
            columns=[
                _column("customer_id", "Customer the employee is assigned to"),
                _column("hire_dt", None, "date"),
            ],
            view_sql="SELECT 1",
        )
        await table_repo.upsert(conn, orders)
        await table_repo.upsert(conn, staff)
        yield conn, reads


def _model(monkeypatch) -> list[str]:
    prompts: list[str] = []

    async def _fake_llm(prompt: str, operation: str, max_tokens: int = 256) -> str:
        prompts.append(prompt)
        return "A drafted definition."

    monkeypatch.setattr(schema_helpers, "_call_llm", _fake_llm)
    return prompts


async def _ids(conn) -> dict[str, int]:
    return {t["name"]: t["id"] for t in await glossary_repo.list_terms(conn)}


async def test_the_prompt_carries_the_bound_columns_descriptions_types_and_tables(
    control_plane, monkeypatch
):
    prompts = _model(monkeypatch)
    async with _store(control_plane, monkeypatch) as (conn, _reads):
        ids = await _ids(conn)
        await glossary_router.generate_definition(_request(), ids["customer"], domains=["sales"])
        prompt = prompts[0]
        assert "Sales Orders.customer_id (varchar)" in prompt
        assert "column description: FK to cust_mstr.cust_key, NOT NULL" in prompt
        assert "table Sales Orders: One row per order placed by a buyer." in prompt


async def test_sample_values_are_read_as_the_curator_and_arrive_masked(control_plane, monkeypatch):
    prompts = _model(monkeypatch)
    async with _store(control_plane, monkeypatch) as (conn, reads):
        ids = await _ids(conn)
        await glossary_router.generate_definition(_request(), ids["customer"], domains=["sales"])
        assert reads.issued, "no sample values were read"
        assert {role for role, _sql in reads.issued} == {CURATOR}
        assert all("SELECT DISTINCT" in sql for _role, sql in reads.issued)
        # Distinct values as the governed read answered them: the masked text, once each.
        assert "sample values: C-****, X-****" in prompts[0]
        assert "C-1001" not in prompts[0]


async def test_a_sensitive_column_sends_no_sample_values(control_plane, monkeypatch):
    prompts = _model(monkeypatch)
    async with _store(control_plane, monkeypatch, sensitive=frozenset({"card_no"})) as (
        conn,
        reads,
    ):
        ids = await _ids(conn)
        await glossary_router.generate_definition(_request(), ids["card no"], domains=["sales"])
        assert reads.issued == []
        prompt = prompts[0]
        assert "Sales Orders.card_no (varchar)" in prompt
        assert "sample values" not in prompt


async def test_related_terms_are_sent_with_their_definitions(control_plane, monkeypatch):
    prompts = _model(monkeypatch)
    async with _store(control_plane, monkeypatch) as (conn, _reads):
        ids = await _ids(conn)
        await glossary_repo.set_definition(conn, ids["order date"], "When the buyer placed it.")
        await glossary_repo.add_edge(conn, ids["order date"], ids["customer"], "PART_OF")
        await glossary_repo.add_edge(conn, ids["customer"], ids["card no"], "RELATED_TO")
        await glossary_router.generate_definition(_request(), ids["customer"], domains=["sales"])
        prompt = prompts[0]
        assert "- PART_OF order date: When the buyer placed it." in prompt
        # A related term with no definition is sent by its name alone.
        assert "- RELATED_TO card no\n" in prompt


async def test_the_prompt_warns_against_copying_technical_wording(control_plane, monkeypatch):
    prompts = _model(monkeypatch)
    async with _store(control_plane, monkeypatch) as (conn, _reads):
        ids = await _ids(conn)
        await glossary_router.generate_definition(_request(), ids["customer"], domains=["sales"])
        prompt = prompts[0]
        assert (
            "Column and table descriptions are often written for engineers: use them only to "
            "understand what the data means. Do not copy their wording, column or table names, "
            "codes, data types, key or nullability notes, or abbreviations."
        ) in prompt
        assert "never describe it as 'this field', 'this column'" in prompt


async def test_nothing_outside_the_selected_domains_is_sent(control_plane, monkeypatch):
    prompts = _model(monkeypatch)
    async with _store(control_plane, monkeypatch) as (conn, reads):
        ids = await _ids(conn)
        await glossary_repo.set_definition(conn, ids["hire date"], "The day employment began.")
        await glossary_repo.add_edge(conn, ids["customer"], ids["hire date"], "RELATED_TO")
        await glossary_router.generate_definition(_request(), ids["customer"], domains=["sales"])
        prompt = prompts[0]
        assert "staff" not in prompt
        assert "Employees and their managers." not in prompt
        assert "Customer the employee is assigned to" not in prompt
        assert "hire date" not in prompt
        assert "The day employment began." not in prompt
        # hr's table is not read either.
        term = await glossary_repo.get_term(conn, ids["customer"])
        assert term is not None
        staff_id = next(r["table_id"] for r in term["refs"] if r["domain_id"] == "hr")
        orders_id = next(r["table_id"] for r in term["refs"] if r["domain_id"] == "sales")
        assert any(f'"t{orders_id}"' in sql for _r, sql in reads.issued)
        assert not any(f'"t{staff_id}"' in sql for _r, sql in reads.issued)


async def test_bulk_definitions_carry_the_same_context(control_plane, monkeypatch):
    prompts = _model(monkeypatch)
    async with _store(control_plane, monkeypatch) as (conn, reads):
        await glossary_router.generate_all_definitions(_request(), domains=["sales"])
        sent = "\n".join(prompts)
        assert "column description: FK to cust_mstr.cust_key, NOT NULL" in sent
        assert "table Sales Orders: One row per order placed by a buyer." in sent
        assert "sample values: C-****, X-****" in sent
        assert "Do not copy their wording" in sent
        assert "staff" not in sent
        assert {role for role, _sql in reads.issued} == {CURATOR}
