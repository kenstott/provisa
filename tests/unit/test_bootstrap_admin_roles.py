# Copyright (c) 2026 Kenneth Stott
# Canary: 1e7f4b90-a253-4d68-8c1b-f36d9a0e5c72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A single-tenant deployment's bootstrap administrator holds platform_admin too (REQ-1913).

Deployment settings are the platform administrator's in every deployment. A single-tenant install
used to seat its one administrator as org_admin only — through a default assignment that applies
to every user with no roles of their own — so that rule would have left a default install with
nobody able to edit deployment settings. The administrator setup CREATES is now recorded with
both roles; a user added later is not."""

# Requirements: REQ-1913, REQ-1297, REQ-124

from __future__ import annotations

import types

import pytest

from provisa.api.errors import ApiError


class _Result:
    def __init__(self, scalar=None, row=None):
        self._scalar, self._row = scalar, row

    def scalar(self):
        return self._scalar

    def fetchone(self):
        return self._row


class _Pool:
    """Stands in for the platform control plane: an empty local_users table that records the
    account setup creates."""

    def __init__(self, existing_users: int = 0):
        self.existing_users = existing_users
        self.created: list[dict] = []

    def acquire(self):
        pool = self

        class _Conn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute_core(self, stmt):
                if stmt.is_insert:
                    pool.created.append(stmt.compile().params)
                    return _Result()
                return _Result(scalar=pool.existing_users, row=None)

            async def upsert(self, _table, values, **_kw):
                pool.created.append(values)

        return _Conn()


@pytest.fixture
def setup(tmp_path, monkeypatch):
    """The setup module with its config I/O recorded and the seating door observed."""
    import provisa.api.setup_router as mod

    seated: list[str] = []
    written: dict = {}

    async def _seat(user_id: str) -> None:
        seated.append(user_id)

    async def _noop(*_a, **_kw):
        return None

    monkeypatch.setattr("provisa.api.auth_router._seat_claimant_in_root", _seat)
    monkeypatch.setattr(mod, "write_verifier", _noop)
    monkeypatch.setattr("provisa.api.admin._config_io.config_path", lambda: tmp_path / "p.yaml")
    monkeypatch.setattr("provisa.api.admin._config_io.read_config_for_setup", lambda: {})
    monkeypatch.setattr(
        "provisa.api.admin._config_io.write_config", lambda _p, cfg: written.update(cfg)
    )
    monkeypatch.setattr("provisa.api.app._load_and_build", _noop)
    monkeypatch.delenv("PROVISA_MULTITENANCY", raising=False)
    monkeypatch.delenv("PROVISA_SUPERUSER_USERNAME", raising=False)
    return types.SimpleNamespace(mod=mod, seated=seated, written=written)


# --- the environment-configured basic install ----------------------------------------------------


async def test_the_seeded_admin_of_a_single_tenant_install_is_seated_with_both_roles(setup):
    pool = _Pool()
    await setup.mod._auto_configure_idp("basic", pool)
    assert len(pool.created) == 1 and pool.created[0]["username"] == "admin"
    assert setup.seated == [pool.created[0]["id"]]
    # Everyone else still gets the org's default role only.
    assert setup.written["auth"]["default_assignments"] == [
        {"role_id": "org_admin", "domain_id": "*"}
    ]


async def test_a_multitenant_install_is_unchanged(setup, monkeypatch):
    monkeypatch.setenv("PROVISA_MULTITENANCY", "1")
    pool = _Pool()
    await setup.mod._auto_configure_idp("basic", pool)
    assert len(pool.created) == 1
    assert setup.seated == []


async def test_nobody_is_seated_when_accounts_already_exist(setup):
    pool = _Pool(existing_users=3)
    await setup.mod._auto_configure_idp("basic", pool)
    assert pool.created == [] and setup.seated == []


# --- the setup wizard ----------------------------------------------------------------------------


@pytest.fixture
def wizard(setup, monkeypatch):
    pool = _Pool()
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(admin_db=pool), raising=False
    )
    return types.SimpleNamespace(pool=pool, **vars(setup))


async def test_the_wizards_administrator_is_seated_with_both_roles_in_single_mode(wizard):
    body = wizard.mod.SetupRequest(
        provider="basic", mode="single", admin_username="root", admin_password="pw"
    )
    assert (await wizard.mod.run_setup(body))["success"] is True
    assert len(wizard.pool.created) == 1
    assert wizard.seated == [wizard.pool.created[0]["id"]]


async def test_the_wizard_in_multi_mode_is_unchanged(wizard):
    body = wizard.mod.SetupRequest(
        provider="basic", mode="multi", admin_username="root", admin_password="pw"
    )
    await wizard.mod.run_setup(body)
    assert len(wizard.pool.created) == 1
    assert wizard.seated == []


# --- what the two roles mean at the deployment-settings gate -------------------------------------

# The seeded role capabilities of a SINGLE-TENANT deployment (schema.sql, then
# apply_tenancy_role_grants: org_admin is granted platform_settings, never cross_org).
SINGLE_TENANT_ROLES = {
    "platform_admin": {
        "capabilities": ["admin", "superadmin", "platform_settings", "cross_org"],
        "domain_access": ["*"],
    },
    "org_admin": {
        "capabilities": ["platform_settings", "org_settings", "observability"],
        "domain_access": ["*"],
    },
}


def _gate(roles_held: list[str], monkeypatch) -> None:
    import provisa.api.app as app_mod
    from provisa.api.admin._platform_guard import require_deployment_settings

    monkeypatch.setattr(app_mod, "state", types.SimpleNamespace(roles=SINGLE_TENANT_ROLES))
    identity = types.SimpleNamespace(user_id="u1", roles=roles_held)
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))
    require_deployment_settings(request)  # pyright: ignore[reportArgumentType]


def test_the_bootstrap_administrator_edits_deployment_settings(monkeypatch):
    _gate(["platform_admin", "org_admin"], monkeypatch)


def test_an_org_administrator_added_later_does_not(monkeypatch):
    with pytest.raises(ApiError) as err:
        _gate(["org_admin"], monkeypatch)
    assert (err.value.status_code, err.value.code) == (403, "platform.control_plane_role_required")
