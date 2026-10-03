# Copyright (c) 2026 Kenneth Stott
# Canary: 1b2e2bc2-2766-43e0-801a-2d3573297323
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1235: on every wire surface, an org-scoped credential opens its own org and no other.

The owner here belongs to both acme and beta, and presents a credential issued for acme. Each
surface is asked for beta the way its clients name an org. The resolver's own rule is covered
in test_org_resolve.py; these check that each surface hands it the credential's org and the
requested org as two separate facts.
"""

from __future__ import annotations

import pytest

from provisa.api.org_resolve import OrgResolutionError
from provisa.auth.models import AuthIdentity


class _Row:
    def __init__(self, org_id: str) -> None:
        self._mapping = {"org_id": org_id}


class _Result:
    def __init__(self, org_ids: list[str]) -> None:
        self._org_ids = org_ids

    def fetchall(self):
        return [_Row(o) for o in self._org_ids]


class _Conn:
    def __init__(self, org_ids: list[str]) -> None:
        self._org_ids = org_ids

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def execute_core(self, _stmt):
        return _Result(self._org_ids)


class _AdminDb:
    def acquire(self):
        return _Conn(["acme", "beta"])


class _State:
    multitenancy = True
    admin_db = _AdminDb()
    roles: dict = {}


def _identity() -> AuthIdentity:
    return AuthIdentity(
        user_id="u1",
        email=None,
        display_name=None,
        roles=[],
        raw_claims={"pat": True},
        active_org_id="acme",
    )


@pytest.fixture(autouse=True)
def _no_runtime_build(monkeypatch):
    """Building an org's runtime is an integration concern; these tests stop at the decision."""

    async def _noop(_org_id, _env=None):
        return None

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _noop, raising=False)


class TestPgwire:
    async def test_the_credentials_org_is_bound(self):
        from provisa.pgwire.server import _resolve_and_build_org

        assert await _resolve_and_build_org(_State(), _identity(), None) == "acme"

    async def test_another_org_named_by_sni_is_refused(self):
        from provisa.pgwire.server import _resolve_and_build_org

        with pytest.raises(OrgResolutionError, match="scoped to org 'acme'"):
            await _resolve_and_build_org(_State(), _identity(), "beta")


class TestFlight:
    async def test_the_credentials_org_is_bound(self):
        from provisa.api.flight.server import _resolve_identity_org

        assert await _resolve_identity_org(_State(), _identity(), {}) == "acme"

    async def test_another_org_named_by_the_ticket_is_refused(self):
        from provisa.api.flight.server import _resolve_identity_org

        with pytest.raises(OrgResolutionError, match="scoped to org 'acme'"):
            await _resolve_identity_org(_State(), _identity(), {"org": "beta"})


class TestGrpc:
    def _servicer(self):
        from provisa.grpc.server import ProvisaServicer

        return ProvisaServicer(_State(), None, None)

    async def test_the_credentials_org_is_bound(self, monkeypatch):
        monkeypatch.setattr("provisa.grpc.auth.authenticated_identity", _identity)
        assert await self._servicer()._resolve_org(None) == "acme"

    async def test_another_org_named_by_metadata_is_refused(self, monkeypatch):
        monkeypatch.setattr("provisa.grpc.auth.authenticated_identity", _identity)
        with pytest.raises(OrgResolutionError, match="scoped to org 'acme'"):
            await self._servicer()._resolve_org("beta")


class TestMcp:
    async def test_the_credentials_org_is_bound(self):
        from provisa.api.mcp.server import _org_for_identity

        assert await _org_for_identity(_identity(), _State()) == "acme"


class TestBolt:
    def _session(self, monkeypatch, requested: str | None):
        from provisa.bolt.session import BoltSession

        monkeypatch.setattr("provisa.api.app.state", _State(), raising=False)
        session = BoltSession.__new__(BoltSession)
        session._org_resolved = False
        session.org_id = None
        session.user_id = "u1"
        session.roles = []
        session._credential_org = "acme"
        monkeypatch.setattr(session, "_requested_org", lambda: requested)
        return session

    async def test_the_credentials_org_is_bound(self, monkeypatch):
        session = self._session(monkeypatch, None)
        await session._ensure_org()
        assert session.org_id == "acme"

    async def test_another_org_named_by_sni_is_refused(self, monkeypatch):
        session = self._session(monkeypatch, "beta")
        with pytest.raises(OrgResolutionError, match="scoped to org 'acme'"):
            await session._ensure_org()

    async def test_signing_in_records_the_credentials_org(self, monkeypatch):
        from provisa.bolt.session import BoltSession

        class _AppState:
            auth_config = {"provider": "basic"}
            auth_middleware_active = True
            contexts = {"analyst": object()}

        monkeypatch.setattr("provisa.api.app.state", _AppState(), raising=False)

        async def _authenticated(_state, _scheme, _principal, _credentials):
            return _identity()

        session = BoltSession.__new__(BoltSession)
        session._credential_org = None
        monkeypatch.setattr(BoltSession, "_authenticate", staticmethod(_authenticated))
        monkeypatch.setattr(
            BoltSession, "_selectable_roles", lambda self, _state, _identity: ["analyst"]
        )
        assert await session._resolve_user("bearer", "", "token") == ("u1", ["analyst"])
        assert session._credential_org == "acme"
