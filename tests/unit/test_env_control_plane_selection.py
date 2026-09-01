# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1618: who the sandbox ephemeral auto-select applies to.

REQ-1602 derives a sandbox visitor's environment from their user id, and the platform administrator
is not a visitor: they hold an assignment in the org's prod schema and nothing at all in a
visitor's branch, so selecting them into one leaves them with an empty capability set and every
admin surface in that org refuses them (the reported symptom was
``invites.user_management_in_org_required`` on their own deployment).

The fact cannot be recovered where the selection happens. ``identity.roles`` is a list of claim
STRINGS, and the acting set inside a tenant org has had the control-plane roles stripped out of it
by REQ-1327 before any of this runs. So the caller -- AuthMiddleware, which reads the platform
plane -- decides it and passes it in.
"""

import hashlib

import pytest

from provisa.api.env_routing import PROD, resolve_selected_env


class _Result:
    def __init__(self, pin):
        self._pin = pin

    def fetchone(self):
        # The membership pin (REQ-1596): the environment an invitation seated this member in, or
        # None for a member of the org itself, which the platform administrator is.
        return None if self._pin is None else (self._pin,)


class _Conn:
    def __init__(self, pin):
        self._pin = pin

    async def execute_core(self, _stmt):
        return _Result(self._pin)


class _Acquire:
    def __init__(self, pin):
        self._pin = pin

    async def __aenter__(self):
        return _Conn(self._pin)

    async def __aexit__(self, *_exc):
        return False


class _AdminDb:
    def __init__(self, pin=None):
        self._pin = pin

    def acquire(self):
        return _Acquire(self._pin)


class _Identity:
    def __init__(self, user_id, roles):
        self.user_id = user_id
        self.roles = roles


ADMIN_UID = "oLuB7qGCOvZ7o8zgMSGNbiJfHJC2"
VISITOR_UID = "visitor-42"


def _ephemeral_of(user_id: str) -> str:
    return f"ephemeral_{hashlib.md5(user_id.encode(), usedforsecurity=False).hexdigest()[:8]}"


@pytest.fixture
def _env_exists(monkeypatch):
    """Every named environment exists and never expires, so the name itself is the assertion."""
    seen: list[str] = []

    async def _get_env(_admin_db, _org_id, name):
        seen.append(name)
        return {"name": name, "expires_at": None}

    async def _renew(*_a, **_kw):
        return None

    monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
    monkeypatch.setattr("provisa.core.env_store.renew_idle_expiry", _renew)
    return seen


@pytest.mark.asyncio
async def test_the_control_plane_is_served_sandbox_prod(_env_exists):
    identity = _Identity(ADMIN_UID, ["org_admin"])  # stripped set: the right is not visible here

    selected = await resolve_selected_env(
        _AdminDb(), "sandbox", identity, None, {"user_management"}, is_control_plane=True
    )

    assert selected == PROD
    assert _env_exists == []  # nothing but prod was ever looked up


@pytest.mark.asyncio
async def test_a_visitor_is_still_auto_selected_into_their_own_branch(_env_exists):
    identity = _Identity(VISITOR_UID, ["sandbox"])

    selected = await resolve_selected_env(
        _AdminDb(_ephemeral_of(VISITOR_UID)),
        "sandbox",
        identity,
        None,
        {"query_development"},
        is_control_plane=False,
    )

    assert selected == _ephemeral_of(VISITOR_UID)


@pytest.mark.asyncio
async def test_the_claim_strings_do_not_decide_it(_env_exists):
    """An identity naming the control-plane role is still a visitor when the caller says so.

    The old test probed each claim for a ``capabilities`` attribute, which a string never has, so it
    answered "not the control plane" for every caller alive. Pinning the decision to the argument is
    the fix; this holds the argument as the ONLY input.
    """
    identity = _Identity(VISITOR_UID, ["platform_admin"])

    selected = await resolve_selected_env(
        _AdminDb(_ephemeral_of(VISITOR_UID)),
        "sandbox",
        identity,
        None,
        {"cross_org"},
        is_control_plane=False,
    )

    assert selected == _ephemeral_of(VISITOR_UID)


@pytest.mark.asyncio
async def test_other_orgs_are_untouched_by_the_auto_select(_env_exists):
    identity = _Identity(VISITOR_UID, ["analyst"])

    selected = await resolve_selected_env(
        _AdminDb(), "acme", identity, None, {"query_development"}, is_control_plane=False
    )

    assert selected == PROD
