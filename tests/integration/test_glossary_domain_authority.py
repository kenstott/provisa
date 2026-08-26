# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7f1c92-4d68-4e05-b1a3-6f92c04d7e18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1591: who may reach a glossary term, asked at the router's own gate.

Real PG, the real config loader, the real derivation. What is exercised here is the boundary
between the router's authority helpers and the repository's domain scope: a term derived from
two domains' tables, and roles that hold one, the other, both, or neither.

The rule this pins is that reaching a term takes ANY of its domains, not all of them —
deliberately unlike REQ-1531's ``require_domains``. That rule guards acts reaching DATA in two
domains; a term is prose about a concept both domains already reference, so requiring all would
leave a shared term curatable by nobody but a holder of every domain it touches.
"""

# Requirements: REQ-1591

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
import pytest_asyncio
from fastapi import Request

from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.core import domain_policy
from provisa.core.config_loader import load_config, parse_config_dict
from provisa.core.repositories import glossary as glossary_repo

pytestmark = [pytest.mark.integration]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()

# Every role holds both glossary rights; what varies between them is only domain reach, which is
# the one variable under test.
_ROLES = {
    "sales_only": ["sales"],
    "store_only": ["petstore"],
    "both": ["sales", "petstore"],
    "hr_only": ["hr"],
    "unlimited": ["*"],
}


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(tenant_db):
    async with tenant_db.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


@pytest_asyncio.fixture(autouse=True)
async def _clean(tenant_db, _init_schema):
    domain_policy.reset()
    async with tenant_db.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE glossary_term_domains, glossary_term_experts, glossary_term_edges,
                     glossary_term_refs, glossary_terms, rls_rules, relationships,
                     relationship_candidates, table_columns, registered_tables,
                     naming_rules, roles, domains, sources CASCADE
            """
        )
    yield
    domain_policy.reset()


def _config() -> dict:
    """One concept, two domains: ``orders`` in sales, ``order_id`` in the pet store.

    ``normalize_term`` sheds the trailing proxy token, so both columns land on the term "order" —
    the cross-table merge the glossary is built on, which a domain boundary is not a reason to
    stop. ``hr.employees.hire_dt`` is the control: a term neither domain under test reaches.
    """
    src = {
        "id": "pg1",
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "d",
        "username": "u",
        "password": "p",
    }
    tables = [
        ("sales", "orders", ["id", "amount"]),
        ("petstore", "pets", ["order_id", "name"]),
        ("hr", "employees", ["hire_dt"]),
    ]
    return {
        "sources": [src],
        "domains": [{"id": d} for d in ("sales", "petstore", "hr")],
        "tables": [
            {
                "source_id": "pg1",
                "domain_id": domain,
                "schema": "public",
                "table": table,
                "columns": [
                    {"name": c, "data_type": "text", "visible_to": ["sales_only"]} for c in columns
                ],
            }
            for domain, table, columns in tables
        ],
        "roles": [
            {"id": rid, "capabilities": ["glossary_read", "glossary_rw"], "domain_access": access}
            for rid, access in _ROLES.items()
        ],
    }


def _request(role_id: str, monkeypatch) -> Request:
    """A caller holding exactly one role, with the app state the authority helper reads.

    The helper resolves domain reach off ``roles.domain_access`` and not off the claim's own
    ``:domain`` suffix (REQ-1530), so the role is the whole input.
    """
    from provisa.api import app as app_module

    monkeypatch.setattr(
        app_module.state,
        "roles",
        {
            rid: {"id": rid, "capabilities": ["glossary_read", "glossary_rw"], "domain_access": acc}
            for rid, acc in _ROLES.items()
        },
        raising=False,
    )
    identity = SimpleNamespace(user_id="u1", roles=[role_id])
    # The helpers read one attribute off the request — ``state.identity`` — so a stub carrying it
    # is the whole input; building a real ASGI scope would add nothing the gate looks at.
    return cast(Request, SimpleNamespace(state=SimpleNamespace(identity=identity)))


async def _term_id(conn, name: str) -> int:
    rows = {t["name"]: t for t in await glossary_repo.list_terms(conn)}
    assert name in rows, f"{name!r} not derived; got {sorted(rows)}"
    return rows[name]["id"]


@pytest_asyncio.fixture
async def loaded(tenant_db):
    async with tenant_db.acquire() as conn:
        await load_config(parse_config_dict(_config()), conn)
        yield conn


@pytest.mark.asyncio(loop_scope="session")
async def test_one_term_carries_both_domains_its_refs_point_at(loaded):
    """The combined model: two domains' columns merge into ONE term scoped to both."""
    scope = await glossary_repo.term_domains(loaded)
    order = await _term_id(loaded, "order")
    assert scope[order] == {"sales", "petstore"}


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("role", ["sales_only", "store_only", "both", "unlimited"])
async def test_any_one_of_a_terms_domains_admits_the_caller(loaded, monkeypatch, role):
    """ANY, not ALL — holding one of the two domains a shared term spans is enough."""
    order = await _term_id(loaded, "order")
    await glossary_router._require_term_in_scope(loaded, order, _request(role, monkeypatch))


@pytest.mark.asyncio(loop_scope="session")
async def test_a_caller_holding_none_of_them_is_refused(loaded, monkeypatch):
    order = await _term_id(loaded, "order")
    with pytest.raises(ApiError) as exc:
        await glossary_router._require_term_in_scope(
            loaded, order, _request("hr_only", monkeypatch)
        )
    assert exc.value.status_code == 403
    assert exc.value.code == "auth.domain_denied"


@pytest.mark.asyncio(loop_scope="session")
async def test_curating_asks_the_same_question_reading_does(loaded, monkeypatch):
    """The gate is one function, so a term a caller can read is a term they can curate.

    Where two domains genuinely mean different things by one phrase the remedy is a split —
    create a term and move the refs onto it — not a narrower gate.
    """
    order = await _term_id(loaded, "order")
    req = _request("store_only", monkeypatch)
    await glossary_router._require_term_in_scope(loaded, order, req)
    await glossary_repo.set_definition(loaded, order, "A request to buy.")
    await glossary_router._require_term_in_scope(loaded, order, req)


@pytest.mark.asyncio(loop_scope="session")
async def test_an_unscoped_term_is_reachable_by_any_glossary_right_holder(loaded, monkeypatch):
    """No domains is NOT no access — it is unscoped, which is why the create path requires one."""
    unscoped = await glossary_repo.create_abstract_term(loaded, "party", domains=set())
    for role in _ROLES:
        await glossary_router._require_term_in_scope(loaded, unscoped, _request(role, monkeypatch))


@pytest.mark.asyncio(loop_scope="session")
async def test_an_abstract_terms_domains_are_declared_and_then_gate_it(loaded, monkeypatch):
    declared = await glossary_repo.create_abstract_term(loaded, "quota", domains={"sales"})
    scope = await glossary_repo.term_domains(loaded)
    assert scope[declared] == {"sales"}

    await glossary_router._require_term_in_scope(loaded, declared, _request("both", monkeypatch))
    with pytest.raises(ApiError):
        await glossary_router._require_term_in_scope(
            loaded, declared, _request("store_only", monkeypatch)
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_a_declaration_cannot_widen_the_declarers_own_reach(loaded, monkeypatch):
    """Declaring into a domain you cannot reach is how a member would grant themselves one."""
    req = _request("sales_only", monkeypatch)
    assert glossary_router._declared_domains(req, ["sales"]) == {"sales"}
    with pytest.raises(ApiError) as exc:
        glossary_router._declared_domains(req, ["sales", "petstore"])
    assert exc.value.status_code == 403


@pytest.mark.asyncio(loop_scope="session")
async def test_the_navbar_filter_narrows_but_never_widens(loaded, monkeypatch):
    """The selection INTERSECTS with authority: unchecking hides, checking cannot reveal."""
    sales = _request("sales_only", monkeypatch)
    # No selection leaves authority exactly as it is — the UI sends none when all boxes are checked.
    assert glossary_router._view_scope(sales, None) == frozenset({"sales"})
    # Naming a domain the role does not hold does not add it.
    assert glossary_router._view_scope(sales, ["sales", "petstore"]) == frozenset({"sales"})
    # An unlimited role narrowed by a selection sees only the selection.
    unlimited = _request("unlimited", monkeypatch)
    assert glossary_router._view_scope(unlimited, None) is None
    assert glossary_router._view_scope(unlimited, ["petstore"]) == frozenset({"petstore"})


@pytest.mark.asyncio(loop_scope="session")
async def test_the_list_is_filtered_by_the_same_scope_the_gate_uses(loaded, monkeypatch):
    """A term the gate would refuse must not appear in the list either."""
    scope = glossary_router._view_scope(_request("hr_only", monkeypatch), None)
    names = {t["name"] for t in await glossary_repo.list_terms(loaded, domains=scope)}
    assert "hire date" in names
    assert "order" not in names


@pytest.mark.asyncio(loop_scope="session")
async def test_a_domain_gates_nothing_in_single_domain_mode(loaded, monkeypatch):
    """Where domains gate nothing, this gate does nothing either — as every domain gate behaves."""
    domain_policy.reset()
    monkeypatch.setattr(domain_policy, "single_domain", lambda: True)
    order = await _term_id(loaded, "order")
    await glossary_router._require_term_in_scope(loaded, order, _request("hr_only", monkeypatch))
    assert glossary_router._declared_domains(_request("hr_only", monkeypatch), None) == set()
