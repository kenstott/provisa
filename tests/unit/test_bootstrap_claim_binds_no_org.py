# Copyright (c) 2026 Kenneth Stott
# Canary: 1ff2cc58-cc94-4c48-8757-3d4112bc8df8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Claiming the bootstrap administrator works on a request bound to no org.

The claim is a platform-plane request, made by someone who belongs to no org yet. Seating the
claimant read the model store of "the org this request is bound to"; on a multi-tenant
deployment there is none, the read raised "No active org bound", and the first administrator's
claim answered 500 (found by the started-server tests of REQ-1285/1298/1306; the in-process
integration tests bind an org for the whole test). The deployment org's store is named."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.security.rights import ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE


class _NoOrgBound:
    """App state as a request bound to no org sees it: the org-routed store refuses."""

    org_id = "root"
    admin_db = object()
    platform_model_db = object()

    @property
    def model_db(self):
        raise RuntimeError("No active org bound (current_org unset).")


async def test_the_claimant_is_seated_in_the_deployment_org_with_no_org_bound(monkeypatch):
    from provisa.api import auth_router
    from provisa.core import org_membership

    state = _NoOrgBound()
    granted: list[tuple[object, str, str]] = []
    joined: list[tuple[str, str]] = []

    async def _membership(admin_db, user_id, org_id, *, joined_via):
        assert admin_db is state.admin_db
        joined.append((user_id, org_id))

    async def _role(model_db, user_id, role_id, *, granter_capabilities):
        granted.append((model_db, user_id, role_id))

    async def _sandbox(_admin_db):
        return None

    monkeypatch.setattr("provisa.api.app.state", state)
    monkeypatch.setattr(org_membership, "grant_membership", _membership)
    monkeypatch.setattr(org_membership, "grant_org_role", _role)
    monkeypatch.setattr("provisa.api.sandbox_org.seat_platform_admins", _sandbox)

    await auth_router._seat_claimant_in_root("founder")  # noqa: SLF001

    assert joined == [("founder", "root")]
    # Both roles land in the deployment org's own store, named, not routed by the request.
    assert granted == [
        (state.platform_model_db, "founder", PLATFORM_ADMIN_ROLE),
        (state.platform_model_db, "founder", ORG_ADMIN_ROLE),
    ]


def test_the_stand_in_refuses_the_org_routed_store_as_the_app_does():
    import pytest

    with pytest.raises(RuntimeError, match="No active org bound"):
        _ = _NoOrgBound().model_db
    assert SimpleNamespace  # the stand-in above is the only state this test needs


# --- the same read elsewhere on routes that need no org named (#187) ----------------------------


async def test_seating_administrators_in_the_sandbox_reads_the_deployment_orgs_store(monkeypatch):
    """Called from the claim, on the same unbound request: the platform_admin assignments are
    read from the deployment org's store, named."""
    from provisa.api import sandbox_org

    class _Reached(Exception):
        pass

    class _DeploymentStore:
        def acquire(self):
            raise _Reached  # as far as this test goes: the right store was asked

    class _State(_NoOrgBound):
        platform_model_db = _DeploymentStore()

    class _Conn:
        async def execute_core(self, _statement):
            return SimpleNamespace(fetchone=lambda: ("ready",))

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *_exc):
            return False

    monkeypatch.setattr("provisa.api.app.state", _State())
    import pytest

    with pytest.raises(_Reached):
        await sandbox_org.seat_platform_admins(SimpleNamespace(acquire=lambda: _Acquire()))


def test_org_provisioning_uses_the_deployment_orgs_store_with_no_org_bound(monkeypatch):
    from provisa.api.admin import orgs_router

    state = _NoOrgBound()
    monkeypatch.setattr("provisa.api.app.state", state)
    assert orgs_router._pool() is state.platform_model_db  # noqa: SLF001


async def test_removing_ones_account_names_the_store_of_the_platform_grants(monkeypatch):
    """DELETE /auth/account is made by a person who may belong to no org; the store that holds
    the platform_admin assignments is the deployment org's, named."""
    from provisa.api import auth_router

    state = _NoOrgBound()
    seen: dict = {}

    async def _remove(admin_db, platform_db, user_id, *, model_db_of, record_db_of):
        seen.update(platform_db=platform_db, user_id=user_id)
        return {"removed": user_id}

    monkeypatch.setattr("provisa.api.app.state", state)
    monkeypatch.setattr("provisa.core.org_membership.remove_account", _remove)
    monkeypatch.setattr("provisa.api.admin.orgs_router._admin_pool", lambda: state.admin_db)
    request = SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id="pat")))
    answer = await auth_router.delete_account(request, confirm="pat")
    assert seen == {"platform_db": state.platform_model_db, "user_id": "pat"}
    assert answer == {"removed": "pat"}
