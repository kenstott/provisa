# Copyright (c) 2026 Kenneth Stott
# Canary: 0fc70a3f-9e9b-4d9e-9984-8f2425ab6a7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A domain named in a request narrows what a role is shown; it never adds to it.

``/data/sdl``, ``/data/introspection`` and ``/data/proto/{role}`` build a per-domain schema when
the request names domains. The role's own ``domain_access`` decides what it reaches: a domain the
role does not hold is refused by name, and the schema is built for the role exactly as it is held.

``rights.reaches_all_domains`` is the one place "does this role reach every domain" is decided,
including the single exemption (single-domain mode), so it is tested here in both modes.
"""

from __future__ import annotations

import types
from typing import Any

import pytest
from graphql import GraphQLField, GraphQLObjectType, GraphQLSchema, GraphQLString

import provisa.api.app as appmod
from provisa.api.data import sdl
from provisa.api.errors import ApiError
from provisa.core import domain_policy
from provisa.security.rights import reaches_all_domains, reaches_domain

CACHE: dict[str, Any] = {
    "tables": [
        {"id": 1, "domain_id": "sales", "name": "orders"},
        {"id": 2, "domain_id": "finance", "name": "ledger"},
        {"id": 3, "domain_id": "meta", "name": "registered_tables_meta"},
        {"id": 4, "domain_id": "ops", "name": "queries"},
    ],
    "relationships": [],
    "column_types": {},
    "naming_rules": {},
    "domains": {"sales": {}, "finance": {}},
    "domain_prefix": {},
    "physical_table_map": {},
    "functions": [],
    "webhooks": [],
    "enum_types": {},
    "metrics": [],
}


# The role comes from the authenticated request; the header these endpoints also accept is unset.
_NO_HEADER: Any = None


def _role(domain_access: list[str]) -> dict:
    return {"id": "scoped", "capabilities": ["usage"], "domain_access": domain_access}


def _request(role_id: str = "scoped") -> Any:
    return types.SimpleNamespace(state=types.SimpleNamespace(role=role_id))


@pytest.fixture
def multi_domain(monkeypatch):
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)


@pytest.fixture
def single_domain(monkeypatch):
    monkeypatch.setattr(domain_policy, "single_domain", lambda: True)


@pytest.fixture
def served(monkeypatch):
    """The app state the endpoints read, and a recorder standing in for the schema build."""

    def _install(domain_access: list[str]) -> list[tuple[dict, list[str]]]:
        built: list[tuple[dict, list[str]]] = []
        monkeypatch.setattr(appmod.state, "roles", {"scoped": _role(domain_access)}, raising=False)
        monkeypatch.setattr(appmod.state, "schema_build_cache", dict(CACHE), raising=False)

        def _build(role, domain_ids, _cache):
            built.append((role, list(domain_ids)))
            return GraphQLSchema(
                query=GraphQLObjectType("Query", {"ok": GraphQLField(GraphQLString)})
            )

        monkeypatch.setattr(sdl, "_build_domain_schema", _build)
        return built

    return _install


# --- the one decision ----------------------------------------------------------------------------


def test_only_the_wildcard_reaches_every_domain(multi_domain):
    assert reaches_all_domains(["*"])
    assert reaches_all_domains(["sales", "*"])
    assert not reaches_all_domains([])  # empty is no domains, never "unrestricted"
    assert not reaches_all_domains(["sales"])
    assert reaches_domain(["*"], "finance")
    assert reaches_domain(["sales"], "sales")
    assert not reaches_domain(["sales"], "finance")
    assert not reaches_domain([], "sales")
    assert not reaches_domain([], "meta")
    assert not reaches_domain([], "ops")


def test_single_domain_mode_is_the_one_exemption(single_domain):
    # The deployment has one domain and domains are not a gate: every role reaches it.
    assert reaches_all_domains([])
    assert reaches_all_domains(["sales"])
    assert reaches_domain([], "anything")


@pytest.mark.parametrize("mode", ["multi_domain", "single_domain"])
def test_a_missing_list_is_an_error_in_either_mode(mode, request):
    request.getfixturevalue(mode)
    with pytest.raises(ValueError, match="domain_access is missing"):
        reaches_all_domains(None)
    with pytest.raises(ValueError, match="domain_access is missing"):
        reaches_domain(None, "sales")


# --- /data/sdl and /data/introspection -----------------------------------------------------------


@pytest.mark.parametrize("held", [["sales"], []])
@pytest.mark.parametrize("endpoint", ["get_sdl", "get_introspection"])
async def test_a_domain_the_role_does_not_reach_is_refused_by_name(
    multi_domain, served, endpoint, held
):
    built = served(held)
    with pytest.raises(ApiError) as err:
        await getattr(sdl, endpoint)(_request(), _NO_HEADER, "finance")
    assert (err.value.status_code, err.value.code) == (403, "data.domain_not_accessible")
    assert err.value.params == {"role_id": "scoped", "domain": "finance"}
    assert built == [], "a refused request builds no schema"


@pytest.mark.parametrize("endpoint", ["get_sdl", "get_introspection"])
async def test_one_unreached_domain_refuses_the_whole_request(multi_domain, served, endpoint):
    built = served(["sales"])
    with pytest.raises(ApiError) as err:
        await getattr(sdl, endpoint)(_request(), _NO_HEADER, "sales,finance")
    assert err.value.params["domain"] == "finance"
    assert built == []


@pytest.mark.parametrize(
    "held,asked,domains",
    [
        (["sales"], "sales", ["sales"]),
        (["*"], "finance", ["finance"]),
        (["*"], "sales,finance", ["sales", "finance"]),
    ],
)
async def test_a_reached_domain_is_served_for_the_role_as_it_is_held(
    multi_domain, served, held, asked, domains
):
    built = served(held)
    resp = await sdl.get_introspection(_request(), _NO_HEADER, asked)
    assert resp.status_code == 200
    [(role, asked_domains)] = built
    assert asked_domains == domains
    assert role["domain_access"] == held, "the request added nothing to the role's scope"


async def test_single_domain_mode_serves_any_role_its_one_domain(single_domain, served):
    built = served([])
    resp = await sdl.get_introspection(_request(), _NO_HEADER, "default")
    assert resp.status_code == 200
    assert built[0][0]["domain_access"] == []


# --- the schema build itself takes the role unchanged --------------------------------------------


@pytest.mark.parametrize("held", [["sales"], [], ["*"]])
def test_the_domain_schema_is_generated_with_the_roles_own_scope(monkeypatch, held):
    from provisa.compiler import schema_gen

    seen: list[Any] = []

    def _generate(si):
        seen.append(si)
        return object()

    monkeypatch.setattr(schema_gen, "generate_schema", _generate)
    monkeypatch.setattr(appmod.state, "source_types", {}, raising=False)
    monkeypatch.setattr(appmod.state, "graphql_remote_sources", {}, raising=False)

    role = _role(held)
    sdl._build_domain_schema(role, ["sales"], dict(CACHE))

    [si] = seen
    assert si.role is role
    assert si.role["domain_access"] == held
    # The requested domain chose the roots; nothing else.
    assert si.root_table_ids == {1}
