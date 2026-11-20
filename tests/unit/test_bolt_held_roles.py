# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4d27e1-5b80-4f3a-a6e2-7d18f0b3c952
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt sign-in acts only as a role its identity holds, as every other transport's does
(REQ-120, amended 2026-11-01; REQ-802), and its cross-org right is judged from what the principal
holds, not from the session's role set (REQ-1337).

Where Provisa holds the assignments, a Bolt HELLO (4.x) or LOGON (5.x) whose identity holds no role
is refused by name, as pgwire, Flight, gRPC and Airport refuse it, not answered "Invalid
credentials": the credential was valid."""

# Requirements: REQ-120, REQ-802, REQ-1337

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.auth import held_assignments
from provisa.auth.models import AuthIdentity, AuthProvider, RoleAssignment
from provisa.bolt.session import BoltSession

_USER = "grace"
_ORG = "default"
_SESSION = "signed-in-session"
_UNAUTHORIZED = "Neo.ClientError.Security.Unauthorized"


class _Provider(AuthProvider):
    """An identity provider whose one bearer token signs the user in with the role claims given."""

    provider_name = "oidc"

    def __init__(self, claims: list[str]) -> None:
        self._claims = claims

    async def validate_token(self, token: str) -> AuthIdentity:
        if token != _SESSION:
            raise ValueError("unknown token")
        return AuthIdentity(user_id=_USER, email=None, display_name=None, roles=list(self._claims))


def _state(source: str, *, multitenancy: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        auth_config={
            "provider": "oidc",
            "role_mapping": [],
            "default_role": "analyst",
            "assignments_source": source,
            "default_assignments": [],
        },
        auth_middleware_active=True,
        multitenancy=multitenancy,
        org_id=_ORG,
        admin_db=None,
        roles={},
        platform_roles={"platform_admin": {"capabilities": ["cross_org"]}},
        contexts={"analyst": object(), "org_admin": object(), "steward": object()},
        user_attributes={},
        settings_overrides={"derived_roles": []},
        meta_roles={},
        meta_qualifiers={},
    )


@pytest.fixture
def holdings(monkeypatch) -> dict[str, list[str]]:
    """The roles the user is assigned in the control plane; the sign-in carries the claims given."""
    held: dict[str, list[str]] = {_USER: []}

    async def _assigned(db_pool, identity, default_assignments):  # noqa: ARG001  # the reader's
        return [RoleAssignment(r, "*") for r in held.get(identity.user_id, [])]

    monkeypatch.setattr(held_assignments, "read_assignments", _assigned)
    monkeypatch.setattr(
        "provisa.auth.wiring.build_auth_provider",
        lambda auth_config, admin_pool=None: _Provider([]),  # noqa: ARG005  # the builder's
    )
    return held


def _serve(monkeypatch, state: Any) -> None:
    import provisa.api.app as app_module

    monkeypatch.setattr(app_module, "state", state)


class _Recorded(BoltSession):
    """A session whose replies are recorded instead of written."""

    def __init__(self, version: tuple[int, int]) -> None:
        super().__init__(SimpleNamespace(), version)  # type: ignore[arg-type]
        self.failures: list[tuple[str, str]] = []
        self.successes: list[dict] = []

    def send_failure(self, code: str, message: str) -> None:
        self.failures.append((code, message))

    def send_success(self, meta: dict | None = None) -> None:
        self.successes.append(meta or {})


_BEARER = {"scheme": "bearer", "principal": "", "credentials": _SESSION}


def _hello(session: _Recorded) -> None:
    asyncio.run(session.handle_hello([dict(_BEARER, user_agent="neo4j-python/4.4")]))


def _logon(session: _Recorded) -> None:
    asyncio.run(session.handle_logon([dict(_BEARER)]))


_SIGN_INS = {"hello-4.4": ((4, 4), _hello), "logon-5.4": ((5, 4), _logon)}


@pytest.mark.parametrize("sign_in", _SIGN_INS)
def test_a_sign_in_holding_no_role_is_refused_by_name(sign_in, monkeypatch, holdings):
    """It was answered "Invalid credentials for principal ''"."""
    _serve(monkeypatch, _state("provisa"))
    version, sign = _SIGN_INS[sign_in]
    session = _Recorded(version)
    sign(session)
    assert session.failures == [(_UNAUTHORIZED, f"'{_USER}' holds no role in this organization")]
    assert session.role_id is None


@pytest.mark.parametrize("sign_in", _SIGN_INS)
def test_a_sign_in_holding_a_role_acts_as_it(sign_in, monkeypatch, holdings):
    holdings[_USER] = ["steward"]
    _serve(monkeypatch, _state("provisa"))
    version, sign = _SIGN_INS[sign_in]
    session = _Recorded(version)
    sign(session)
    assert session.failures == []
    assert (session.role_id, session.roles) == ("steward", ["steward"])


@pytest.mark.parametrize("sign_in", _SIGN_INS)
def test_a_bad_credential_is_still_invalid_credentials(sign_in, monkeypatch, holdings):
    _serve(monkeypatch, _state("provisa"))
    version, _sign = _SIGN_INS[sign_in]
    session = _Recorded(version)
    bad = {"scheme": "bearer", "principal": "", "credentials": "forged"}
    if version[0] < 5:
        asyncio.run(session.handle_hello([bad]))
    else:
        asyncio.run(session.handle_logon([bad]))
    assert session.failures == [(_UNAUTHORIZED, "Invalid credentials for principal ''")]


# --- the cross-org right, judged from what is held ------------------------------------------------


@pytest.fixture
def judged(monkeypatch) -> list[bool]:
    """What the session's org resolution was told about the cross-org right."""
    told: list[bool] = []

    async def _resolve(state, *, can_act_any_org=False, **kw):  # noqa: ARG001  # the resolver's
        told.append(can_act_any_org)
        return _ORG

    async def _built(org_id, env=None):  # noqa: ARG001  # the builder's
        return None

    monkeypatch.setattr("provisa.api.org_resolve.resolve_session_org", _resolve)
    monkeypatch.setattr("provisa.api.app.ensure_serving_runtime", _built)
    return told


def _bind_org(monkeypatch, state: Any, session_roles: list[str], claims: list[str]) -> None:
    _serve(monkeypatch, state)
    session = _Recorded((5, 4))
    session.user_id = _USER
    session.roles = session_roles
    session._identity = AuthIdentity(user_id=_USER, email=None, display_name=None, roles=claims)
    asyncio.run(session._ensure_org())


def test_a_cross_org_role_in_the_session_the_principal_does_not_hold_crosses_no_org(
    monkeypatch, holdings, judged
):
    """The session's role set and the credential name platform_admin; the control plane assigns
    analyst alone."""
    holdings[_USER] = ["analyst"]
    state = _state("provisa", multitenancy=True)
    _bind_org(monkeypatch, state, ["platform_admin"], ["platform_admin"])
    assert judged == [False]


def test_a_held_cross_org_role_crosses_orgs_though_the_session_does_not_name_it(
    monkeypatch, holdings, judged
):
    """platform_admin has no data context, so it is never among a session's selectable roles."""
    holdings[_USER] = ["platform_admin", "analyst"]
    state = _state("provisa", multitenancy=True)
    _bind_org(monkeypatch, state, ["analyst"], [])
    assert judged == [True]


def test_where_roles_are_claims_the_claims_are_what_is_held(monkeypatch, holdings, judged):
    state = _state("claims", multitenancy=True)
    _bind_org(monkeypatch, state, ["analyst"], ["platform_admin"])
    assert judged == [True]
