# Copyright (c) 2026 Kenneth Stott
# Canary: 454ad47b-9715-47f7-a817-e36235443796
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The local MCP principal on the installs the first-run setup does not seat an administrator for
(REQ-1105, A NO-SIGN-IN INSTALL SEATS A LOCAL ADMIN; FIRST-RUN SETUP NAMES THE LOCAL PRINCIPAL):
the no-sign-in setup, the first Firebase sign-in, and multi mode, which names nobody."""

# Requirements: REQ-1105

from __future__ import annotations

import types

import bcrypt
import pytest

from provisa.api import auth_router
from provisa.api import setup_router as mod


class _Result:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _Pool:
    """The platform control plane: records the accounts inserted and the id of the one that exists
    under the local admin's username, if any."""

    def __init__(self, existing_id: str | None = None):
        self.existing_id = existing_id
        self.created: list[dict] = []
        self.claimed_by: str | None = None

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
                return _Result((pool.existing_id,) if pool.existing_id else None)

            async def upsert_returning(self, _table, values, **_kw):
                return pool.claimed_by or values["user_id"]

        return _Conn()


@pytest.fixture
def env(tmp_path, monkeypatch):
    seated: list[str] = []
    stored: dict = {}

    async def _seat(user_id: str) -> None:
        seated.append(user_id)

    async def _noop(*_a, **_kw):
        return None

    def _store(_db, given: dict, *, updated_by: str) -> None:
        stored.update(given)
        stored["_by"] = updated_by

    from provisa.core import settings_registry

    monkeypatch.setattr("provisa.api.auth_router._seat_claimant_in_root", _seat)
    monkeypatch.setattr(settings_registry, "store", _store)
    monkeypatch.setattr("provisa.api.admin._config_io.config_path", lambda: tmp_path / "p.yaml")
    monkeypatch.setattr("provisa.api.admin._config_io.read_config_for_setup", lambda: {})
    monkeypatch.setattr("provisa.api.admin._config_io.write_config", lambda _p, _c: None)
    monkeypatch.setattr("provisa.api.app._load_and_build", _noop)
    monkeypatch.setattr(mod, "write_verifier", _noop)
    monkeypatch.delenv("PROVISA_MULTITENANCY", raising=False)
    monkeypatch.setenv("PROVISA_MCP_BRIDGE_COMMAND", "/home/me/.provisa/runtime/bin/python")
    pool = _Pool()
    state = types.SimpleNamespace(
        admin_db=pool,
        multitenancy=False,
        auth_config={"bootstrap_superadmin": True},
    )
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    return types.SimpleNamespace(pool=pool, state=state, seated=seated, stored=stored)


def _none_setup(mode: str):
    return mod.run_setup(mod.SetupRequest(provider="none", mode=mode))


# --- no sign-in ---------------------------------------------------------------------------------


async def test_a_no_sign_in_setup_seats_a_local_admin_nobody_can_sign_in_as(env):
    await _none_setup("single")
    (account,) = env.pool.created
    assert account["username"] == mod.LOCAL_ADMIN_USERNAME and account["is_active"] is True
    # A real bcrypt hash of a secret nobody holds: the well-known passwords do not match it.
    for guess in ("", "admin", mod.LOCAL_ADMIN_USERNAME, "password"):
        assert not bcrypt.checkpw(guess.encode(), account["password_hash"].encode())
    assert env.seated == [account["id"]]


async def test_a_no_sign_in_setup_names_the_local_admin_the_mcp_principal(env):
    await _none_setup("single")
    account_id = env.pool.created[0]["id"]
    assert env.stored["mcp.principal"] == account_id
    assert env.stored["_by"] == account_id


async def test_a_no_sign_in_setup_reuses_a_local_admin_already_seated(env):
    env.pool.existing_id = "already"
    await _none_setup("single")
    assert env.pool.created == []
    assert env.stored["mcp.principal"] == "already"


async def test_a_no_sign_in_setup_names_nobody_off_the_desktop_tier(env, monkeypatch):
    monkeypatch.delenv("PROVISA_MCP_BRIDGE_COMMAND")
    await _none_setup("single")
    assert "mcp.principal" not in env.stored


async def test_a_multi_mode_no_sign_in_setup_seats_and_names_nobody(env):
    await _none_setup("multi")
    assert env.pool.created == [] and env.seated == []
    assert "mcp.principal" not in env.stored


# --- multi mode ---------------------------------------------------------------------------------


async def test_a_multi_mode_wizard_does_not_name_the_platform_admin(env):
    body = mod.SetupRequest(
        provider="basic", mode="multi", admin_username="root", admin_password="pw"
    )
    await mod.run_setup(body)
    assert len(env.pool.created) == 1
    assert "mcp.principal" not in env.stored


async def test_a_multitenant_seeded_admin_is_not_named(env, monkeypatch):
    monkeypatch.setenv("PROVISA_MULTITENANCY", "1")
    pool = _SeedPool()
    await mod._auto_configure_idp("basic", pool)
    assert "mcp.principal" not in env.stored


class _SeedPool(_Pool):
    def acquire(self):
        conn_cm = super().acquire()

        class _Conn:
            async def __aenter__(self):
                self.inner = await conn_cm.__aenter__()
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute_core(self, _stmt):
                return types.SimpleNamespace(scalar=lambda: 0)

            async def upsert(self, _table, _values, **_kw):
                return None

        return _Conn()


# --- the first Firebase sign-in -----------------------------------------------------------------


def _request(user_id: str):
    identity = types.SimpleNamespace(user_id=user_id)
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))


async def test_the_first_firebase_user_is_named_the_mcp_principal_on_desktop(env):
    result = await auth_router.claim_bootstrap(_request("fb-1"))  # pyright: ignore[reportArgumentType]
    assert result["claimed"] is True
    assert env.seated == ["fb-1"]
    assert env.stored["mcp.principal"] == "fb-1" and env.stored["_by"] == "fb-1"


async def test_a_claim_lost_to_another_user_names_nobody(env):
    env.pool.claimed_by = "someone-else"
    result = await auth_router.claim_bootstrap(_request("fb-2"))  # pyright: ignore[reportArgumentType]
    assert result["claimed"] is False
    assert "mcp.principal" not in env.stored


async def test_a_claim_names_nobody_off_the_desktop_tier(env, monkeypatch):
    monkeypatch.delenv("PROVISA_MCP_BRIDGE_COMMAND")
    await auth_router.claim_bootstrap(_request("fb-3"))  # pyright: ignore[reportArgumentType]
    assert "mcp.principal" not in env.stored


async def test_a_multitenant_claim_does_not_name_the_platform_admin(env):
    env.state.multitenancy = True
    result = await auth_router.claim_bootstrap(_request("fb-4"))  # pyright: ignore[reportArgumentType]
    assert result["claimed"] is True and env.seated == ["fb-4"]
    assert "mcp.principal" not in env.stored
