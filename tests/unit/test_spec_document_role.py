# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0b4f71-3e59-4c28-8a47-b1f9e2c5d803
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A spec link that names a role returns THAT role's spec, or is refused (REQ-273, REQ-1620).

``/data/rest/openapi.json`` and ``/data/jsonapi/openapi.json`` take ``?role=``. They used to read
it only when the auth layer had established no role at all, so on every real deployment a link
naming ``analyst`` was answered with the acting role's spec. The named role (one held role, or a
comma-separated set served as its meta-role) now decides, on the terms of a role named in a path;
with none named the spec is the acting role's.
"""

# Requirements: REQ-273, REQ-1620, REQ-222

from __future__ import annotations

import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import provisa.api.app as appmod
from provisa.api.acting_role import document_role
from provisa.api.errors import ApiError
from provisa.api.jsonapi.generator import create_jsonapi_router
from provisa.api.rest.generator import create_rest_router
from provisa.auth.models import RoleAssignment

A, B = "role_a", "role_b"
META = "meta:role_a+role_b"


def _request(*held: str, acting: str | None = None):
    state = types.SimpleNamespace(
        identity=types.SimpleNamespace(user_id="u1", roles=list(held)),
        assignments=[RoleAssignment(role_id=r, domain_id="*") for r in held],
    )
    if acting is not None:
        state.role = acting
    return types.SimpleNamespace(state=state)


@pytest.fixture(autouse=True)
def _roles(monkeypatch):
    roles = {r: {"id": r, "capabilities": [], "domain_access": ["*"]} for r in (A, B)}
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    monkeypatch.setattr(
        "provisa.security.meta_role.ensure_meta_role",
        lambda state, members: "meta:" + "+".join(sorted(set(members))),
    )


def test_a_named_role_decides_and_the_acting_role_answers_only_when_none_is_named():
    assert document_role(_request(A, B, acting=A), B) == B
    assert document_role(_request(A, B, acting=A), f"{A},{B}") == META
    assert document_role(_request(A, B, acting=A), None) == A
    assert document_role(_request(A, B, acting=A), "") == A
    assert document_role(_request(A, B), None) is None


def test_a_named_role_the_caller_does_not_hold_is_refused_not_answered_as_the_acting_role():
    for named in (B, f"{A},{B}"):
        with pytest.raises(ApiError) as err:
            document_role(_request(A, acting=A), named)
        assert (err.value.status_code, err.value.code) == (403, "auth.role_not_assigned")
        assert err.value.params == {"role_id": B}
    with pytest.raises(ApiError) as err:
        document_role(_request(A, B, acting=A), META)
    assert (err.value.status_code, err.value.code) == (403, "auth.meta_role_named")


# --- the two routes --------------------------------------------------------------------------------


@pytest.fixture
def client(monkeypatch):
    """Both spec routes behind a stand-in for the auth layer: the caller holds A and B, acts as A."""
    asked: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "provisa.api.rest.openapi_spec.generate_rest_openapi_spec",
        lambda state, role_id, domains=None: asked.append(("rest", role_id)) or {"for": role_id},
    )
    monkeypatch.setattr(
        "provisa.api.jsonapi.spec.generate_jsonapi_openapi_spec",
        lambda state, role_id, domains=None: asked.append(("jsonapi", role_id)) or {"for": role_id},
    )
    app = FastAPI()

    @app.middleware("http")
    async def _auth(request, call_next):
        held = request.headers["x-held"].split(",")
        request.state.identity = types.SimpleNamespace(user_id="u1", roles=held)
        request.state.assignments = [RoleAssignment(role_id=r, domain_id="*") for r in held]
        request.state.role = held[0]
        return await call_next(request)

    state = types.SimpleNamespace()
    app.include_router(create_rest_router(state))
    app.include_router(create_jsonapi_router(state))
    test_client = TestClient(app)
    test_client.asked = asked  # type: ignore[attr-defined]
    return test_client


@pytest.mark.parametrize("path", ["/data/rest/openapi.json", "/data/jsonapi/openapi.json"])
def test_the_spec_is_the_named_role_sets_or_the_acting_roles(client, path):
    held = {"x-held": f"{A},{B}"}
    assert client.get(path, headers=held).json() == {"for": A}
    assert client.get(f"{path}?role={B}", headers=held).json() == {"for": B}
    assert client.get(f"{path}?role={A},{B}", headers=held).json() == {"for": META}
    assert client.get(f"{path}?role={B}&download=1", headers=held).json() == {"for": B}


@pytest.mark.parametrize("path", ["/data/rest/openapi.json", "/data/jsonapi/openapi.json"])
def test_a_link_naming_an_unheld_role_is_refused(client, path):
    resp = client.get(f"{path}?role={B}", headers={"x-held": A})
    assert resp.status_code == 403, resp.text
    assert client.asked == []  # no spec was generated for anyone
