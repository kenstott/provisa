# Copyright (c) 2026 Kenneth Stott
# Canary: 3b224be7-137a-4864-ad0e-f231a251526d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A principal's roles are the same on every transport (REQ-120, REQ-551): with
``auth.assignments_source: provisa`` they are the role assignments the control plane holds for it
in its org -- the ones granted in the admin UI, or the configured default_assignments when it has
none -- read by one reader (provisa.auth.held_assignments) that HTTP, Bolt, pgwire and every
bearer sign-in (Flight, gRPC, Airport) use. Before, only HTTP read them: a user granted a role in
the admin UI could not select it over Bolt, pgwire or Flight, which read only the sign-in's own
claims."""

# Requirements: REQ-120, REQ-551

from __future__ import annotations

import asyncio
import types

import pytest

from provisa.auth import held_assignments
from provisa.auth.models import AuthIdentity, RoleAssignment

#: A user whose sign-in carries no role claim, granted ``steward`` in the control plane.
_USER = AuthIdentity(user_id="u-1", email="u@example.com", display_name=None, roles=[])
_GRANTED = [RoleAssignment(role_id="steward", domain_id="*")]


def _state(source: str = "provisa"):
    return types.SimpleNamespace(
        auth_config={
            "provider": "basic",
            "default_role": "analyst",
            "assignments_source": source,
            "default_assignments": [],
        },
        contexts={"analyst": object(), "steward": object()},
        multitenancy=False,
        org_id="default",
    )


@pytest.fixture
def granted(monkeypatch):
    """The control plane holds ``steward`` for the user, in the org it is read in (recorded)."""
    read_in: list[str | None] = []

    async def _read(db_pool, identity, default_assignments):  # noqa: ARG001  # the reader's
        from provisa.core.request_context import current_org

        read_in.append(current_org.get(None))
        return list(_GRANTED) if identity.user_id == _USER.user_id else []

    monkeypatch.setattr(held_assignments, "read_assignments", _read)
    return read_in


def test_the_one_reader_reads_the_control_plane_in_the_principals_org(granted):
    held = asyncio.run(held_assignments.held_assignments(_state(), _USER, "acme"))
    assert held == _GRANTED
    assert granted == ["acme"]


def test_with_claims_as_the_source_the_sign_ins_claims_are_the_assignments(granted):
    claimed = AuthIdentity(user_id="u-1", email=None, display_name=None, roles=["analyst:sales"])
    held = asyncio.run(held_assignments.held_assignments(_state("claims"), claimed, None))
    assert held == [RoleAssignment(role_id="analyst", domain_id="sales")]
    assert granted == []  # the control plane is not read


def test_the_reader_answers_the_default_assignments_to_a_user_with_none():
    class _Conn:
        """A control-plane connection that holds no assignment for anyone."""

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def execute_core(self, statement):  # noqa: ARG002  # the pool's signature
            return types.SimpleNamespace(fetchall=lambda: [])

    class _Pool:
        def acquire(self):
            return _Conn()

    held = asyncio.run(held_assignments.read_assignments(_Pool(), _USER, [{"role_id": "analyst"}]))
    assert held == [RoleAssignment(role_id="analyst", domain_id="*")]


def test_a_bearer_sign_in_selects_a_role_granted_in_the_control_plane(granted, monkeypatch):
    """Flight, gRPC and Airport authorize through authorize_role_or_probe."""
    from provisa.auth.bearer import authorize_role_or_probe
    from provisa.security import role_probe

    async def _not_suspended(identity, org_id, transport):  # noqa: ARG001  # role_probe's
        return False

    monkeypatch.setattr(role_probe, "suspended", _not_suspended)
    role = authorize_role_or_probe(
        _state(), _USER, "steward", transport="flight", endpoint="flight", run=asyncio.run
    )
    assert role == "steward"


def test_a_bolt_sign_in_selects_a_role_granted_in_the_control_plane(granted):
    from provisa.bolt.session import BoltSession

    roles = asyncio.run(BoltSession._selectable_roles(_state(), _USER))
    assert roles == ["analyst", "steward"]


def test_a_pgwire_sign_in_selects_a_role_granted_in_the_control_plane(granted):
    from provisa.pgwire.server import _held_roles

    held = _held_roles(_state(), _USER, "analyst", "acme", run=asyncio.run)
    assert held == {"analyst", "steward"}
    assert granted == ["acme"]


def test_a_flight_sign_in_selects_a_role_granted_in_the_control_plane(granted, monkeypatch):
    from provisa.api.flight.server import ProvisaFlightServer
    from provisa.security import role_probe

    async def _not_suspended(identity, org_id, transport):  # noqa: ARG001  # role_probe's
        return False

    monkeypatch.setattr(role_probe, "suspended", _not_suspended)
    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    server._state = _state()
    server._run_on_loop = asyncio.run  # type: ignore[method-assign]  # the RPC's loop, here
    assert server._authorize_role(_USER, {"role": "steward"}) == "steward"


def test_an_http_sign_in_reads_the_same_assignments(granted):
    """The HTTP middleware's read is the same reader's."""
    from provisa.auth.middleware import AuthMiddleware

    middleware = AuthMiddleware.__new__(AuthMiddleware)
    middleware._db_pool = object()
    middleware._default_assignments = []
    assert asyncio.run(middleware._read_assignments(_USER)) == _GRANTED
