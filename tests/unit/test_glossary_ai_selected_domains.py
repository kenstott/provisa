# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e5f21-7c4a-4d9e-a6b2-91f0c3d7e845
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-2253: the ontology's AI assistants work only within the domains selected in the filter.

Two domains share one term (``customer``, a column of a sales table and of an hr table); each also
has a term of its own. With sales selected, nothing of hr -- its term, its table's column, a
relationship to its term -- reaches the model, and nothing of hr is written, whatever the model
proposes. Handlers are awaited directly with a fake request over a real control-plane store, as in
test_glossary_admin_surface.py.
"""

# Requirements: REQ-2253

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert

from provisa.api.admin import glossary_router, schema_helpers
from provisa.core import org_secrets
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources
from provisa.llm.client import Completion
from tests.unit.gate_identity import serve_every_column

if TYPE_CHECKING:
    from fastapi import Request

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


def _request() -> "Request":
    fake = types.SimpleNamespace(
        headers={},
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="acme",
        ),
    )
    return cast("Request", fake)


def _tbl(domain: str, name: str, columns: list[str]) -> Table:
    return Table(
        source_id="__derived__",
        domain_id=domain,
        schema_name="s",
        table_name=name,
        columns=[Column(name=c, data_type="text", visible_to=[]) for c in columns],
        view_sql="SELECT 1",
    )


@asynccontextmanager
async def _two_domains(control_plane, monkeypatch):
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
    # The rights and the role's domain authority are other requirements' tests (REQ-1590,
    # REQ-1591, REQ-1592): an unlimited curator here, so only the filter's selection narrows.
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)
    async with db.acquire() as conn:
        await table_repo.upsert(conn, _tbl("sales", "orders", ["customer_id", "order_dt"]))
        await table_repo.upsert(conn, _tbl("hr", "staff", ["customer_id", "hire_dt"]))
        yield conn


async def _ids(conn) -> dict[str, int]:
    return {t["name"]: t["id"] for t in await glossary_repo.list_terms(conn)}


def _model(monkeypatch, reply: str) -> list[str]:
    prompts: list[str] = []

    async def _fake_llm(prompt: str, operation: str, max_tokens: int = 256) -> str:
        prompts.append(prompt)
        return reply

    monkeypatch.setattr(schema_helpers, "_call_llm", _fake_llm)
    return prompts


def _relationship_model(monkeypatch, *answers: tuple[str, bool]) -> list[str]:
    """The relationship assistant reads each answer with its cut-off flag (J1482): one
    ``(text, cut_off)`` per ask, in order."""
    prompts: list[str] = []
    queue = list(answers)

    async def _fake_llm(prompt: str, operation: str, max_tokens: int = 256) -> Completion:
        prompts.append(prompt)
        text, cut_off = queue.pop(0)
        return Completion(text, "anthropic", "m", None, cut_off)

    monkeypatch.setattr(schema_helpers, "_call_llm_detailed", _fake_llm)
    return prompts


async def test_the_relationship_assistant_sees_and_writes_only_the_selected_domains(
    control_plane, monkeypatch
):
    prompts = _relationship_model(
        monkeypatch,
        (
            '[{"from": "order date", "to": "customer", "rel_type": "PART_OF"},'
            ' {"from": "hire date", "to": "customer", "rel_type": "PART_OF"},'
            ' {"from": "order date", "to": "hire date", "rel_type": "RELATED_TO"}]',
            False,
        ),
    )
    async with _two_domains(control_plane, monkeypatch) as conn:
        result = await glossary_router.generate_relationships(_request(), domains=["sales"])
        assert len(prompts) == 1
        assert "- order date [property]" in prompts[0]
        assert "- customer [property]" in prompts[0]
        assert "hire date" not in prompts[0]
        # The model named hr's term anyway: neither edge touching it is written.
        assert result["added"] == 1
        ids = await _ids(conn)
        edges = await glossary_repo.term_edges(conn)
        assert edges == [(ids["order date"], ids["customer"], "PART_OF")]


async def test_without_a_selection_the_relationship_assistant_reaches_every_domain(
    control_plane, monkeypatch
):
    prompts = _relationship_model(monkeypatch, ("[]", False))
    async with _two_domains(control_plane, monkeypatch):
        await glossary_router.generate_relationships(_request())
        assert "hire date" in prompts[0]
        assert "order date" in prompts[0]


async def test_a_cut_off_answers_re_ask_sends_only_the_selected_domains(control_plane, monkeypatch):
    prompts = _relationship_model(
        monkeypatch,
        # Cut off after one complete relationship: the rest is asked for.
        ('[{"from": "order date", "to": "customer", "rel_type": "PART_OF"}, {"from": "hi', True),
        (
            '[{"from": "hire date", "to": "customer", "rel_type": "PART_OF"},'
            ' {"from": "customer", "to": "order date", "rel_type": "RELATED_TO"}]',
            False,
        ),
    )
    async with _two_domains(control_plane, monkeypatch) as conn:
        result = await glossary_router.generate_relationships(_request(), domains=["sales"])
        assert len(prompts) == 2
        assert "already proposed" in prompts[1]
        for prompt in prompts:
            assert "- order date [property]" in prompt
            assert "- customer [property]" in prompt
            assert "hire date" not in prompt
        # The re-ask's answer named hr's term anyway: that edge is not written.
        assert result["added"] == 2
        ids = await _ids(conn)
        edges = sorted(await glossary_repo.term_edges(conn))
        assert edges == sorted(
            [
                (ids["order date"], ids["customer"], "PART_OF"),
                (ids["customer"], ids["order date"], "RELATED_TO"),
            ]
        )


async def test_bulk_definitions_fill_only_the_selected_domains_terms(control_plane, monkeypatch):
    prompts = _model(monkeypatch, "A drafted definition.")
    async with _two_domains(control_plane, monkeypatch) as conn:
        ids = await _ids(conn)
        await glossary_repo.add_edge(conn, ids["customer"], ids["hire date"], "RELATED_TO")
        result = await glossary_router.generate_all_definitions(_request(), domains=["sales"])
        assert result["generated"] == 2
        sent = "\n".join(prompts)
        assert "'customer'" in sent and "'order date'" in sent
        # Neither hr's term, nor hr's table, nor a relationship to hr's term is sent.
        assert "hire date" not in sent
        assert "staff" not in sent
        assert "orders.customer_id" in sent
        defined = {t["name"]: t["definition"] for t in await glossary_repo.list_terms(conn)}
        assert defined["hire date"] in (None, "")
        assert defined["order date"] == "A drafted definition."


async def test_a_terms_drafted_definition_sends_only_the_selected_domains(
    control_plane, monkeypatch
):
    prompts = _model(monkeypatch, "The buyer.")
    async with _two_domains(control_plane, monkeypatch) as conn:
        ids = await _ids(conn)
        await glossary_repo.add_edge(conn, ids["customer"], ids["hire date"], "RELATED_TO")
        await glossary_repo.add_edge(conn, ids["order date"], ids["customer"], "PART_OF")
        await glossary_router.generate_definition(_request(), ids["customer"], domains=["sales"])
        assert "orders.customer_id" in prompts[0]
        assert "staff.customer_id" not in prompts[0]
        assert "PART_OF order date" in prompts[0]
        assert "hire date" not in prompts[0]
        # No selection: the whole term, both domains' columns and every related term.
        await glossary_router.generate_definition(_request(), ids["customer"])
        assert "staff.customer_id" in prompts[1]
        assert "hire date" in prompts[1]
