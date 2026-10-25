# Copyright (c) 2026 Kenneth Stott
# Canary: 75a65c79-e795-4fbc-acc2-bc9f3965f1e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request naming a set of held roles acts as their meta-role on every route (REQ-1952).

On an unsecured deployment the HTTP middleware resolves a named set -- data roles and declared
derived roles -- to its meta-role. A body naming the same set as the header runs as that role,
and gRPC, Flight and Bolt resolve the set they are named through the same resolver."""

# Requirements: REQ-1952, REQ-2057

from __future__ import annotations

import json
from types import SimpleNamespace

import pyarrow.flight as flight
import pytest

from provisa.api.acting_role import acting_role
from provisa.api.errors import ApiError
from provisa.api.flight.server import ProvisaFlightServer
from provisa.auth.derived_roles import resolve_unsecured_role
from provisa.bolt.session import BoltSession
from provisa.grpc.server import ProvisaServicer
from tests.role_state import RoleState

SET = "org_admin,attr1__v,attr2__v"
META = "meta:attr1+attr1__v+attr2+attr2__v+org_admin"


@pytest.fixture()
def built(monkeypatch):
    """Meta-roles are built by id from what they are made of; each build is recorded."""
    seen: list = []

    def ensure(_state, members, quals=()):
        seen.append((list(members), tuple(quals)))
        return "meta:" + "+".join(sorted({*members, *quals}))

    monkeypatch.setattr("provisa.security.meta_role.ensure_meta_role", ensure)
    return seen


def _state(**extra):
    """An unsecured runtime declaring the derived roles attr1 and attr2."""
    state = RoleState(
        org_id="default",
        auth_config=None,
        auth_middleware_active=False,
        multitenancy=False,
        admin_db=None,
        contexts={"org_admin": object(), "analyst": object()},
        settings_overrides={
            "derived_roles": [{"attribute": a, "description": a} for a in ("attr1", "attr2")]
        },
        user_attributes={},
    )
    for key, value in extra.items():
        setattr(state, key, value)
    return state


def test_the_unsecured_resolver_acts_as_the_meta_role_of_the_set(built):
    assert resolve_unsecured_role(_state(), SET) == META
    assert built == [(["org_admin"], ("attr1__v", "attr1", "attr2__v", "attr2"))]


def test_one_named_role_is_that_role(built):
    assert resolve_unsecured_role(_state(), "analyst") == "analyst"
    assert built == []


class TestBodyRole:
    """The body names the role set the header named: the request runs as their meta-role."""

    def _request(self):
        return SimpleNamespace(state=SimpleNamespace(role=META))

    def test_a_body_naming_the_headers_set_runs_as_its_meta_role(self):
        reordered = "attr2__v,org_admin,attr1__v"
        assert acting_role(self._request(), SET, reordered, "org_admin") == META

    def test_a_body_naming_another_set_is_refused(self):
        with pytest.raises(ApiError) as refused:
            acting_role(self._request(), SET, "org_admin,attr1__v", "org_admin")
        assert refused.value.status_code == 400


class TestGrpc:
    async def test_an_unsecured_rpc_naming_a_set_runs_as_its_meta_role(self, built):
        """Resolved once the RPC's org is bound (ProvisaServicer._acting, at every handler)."""
        servicer = ProvisaServicer.__new__(ProvisaServicer)
        servicer._state = _state()
        assert await servicer._acting(SET, SimpleNamespace()) == META


class TestFlight:
    def test_an_unsecured_ticket_naming_a_set_runs_as_its_meta_role(self, built):
        srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
        srv._state = _state()
        seen: dict = {}
        srv._do_get_inner = lambda request, ticket: seen.update(request) or "ok"
        srv.do_get(None, flight.Ticket(json.dumps({"query": "SELECT 1", "role": SET}).encode()))
        assert seen["role"] == META


class TestBolt:
    async def test_an_unsecured_principal_naming_a_set_runs_as_its_meta_role(
        self, monkeypatch, built
    ):
        import provisa.api.app as app_module

        state = _state()
        monkeypatch.setattr(app_module, "state", state)
        session = BoltSession(SimpleNamespace(), (5, 4))  # type: ignore[arg-type]
        _user, roles = await session._resolve_user("basic", SET, "unused")  # type: ignore[misc]
        session.roles = roles
        session.org_id = "default"
        role_id, _ops = session._resolve_db(None)
        assert role_id == SET
        assert session._meta_role(state, role_id) == META
