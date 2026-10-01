# Copyright (c) 2026 Kenneth Stott
# Canary: 7a4d1c96-0e3b-4f52-8c7a-1d9b6e2f4a08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A replica refresh binds the vault of the org its sources are registered in (REQ-1695)."""

# Requirements: REQ-1695

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core import secrets_store
from provisa.core.secrets import resolve_secrets
from provisa.federation.source_vault import names_org_secret, org_vault

_REF = "${secret:source_src_password}"


class _NoState:
    """A state any read of fails the test: nothing may be looked up when nothing is to be bound."""

    def __getattr__(self, name: str):
        raise AssertionError(f"state.{name} was read although no vault had to be bound")


@pytest.fixture
def vault(monkeypatch):
    """The org vaults, read through the store's own ``bound`` with the decrypt replaced."""
    reads: list[tuple[object, str]] = []

    async def _decrypted(admin_db, org_id, owner_id):
        reads.append((admin_db, org_id))
        return {"source_src_password": f"pw-of-{org_id}"}

    monkeypatch.setattr(secrets_store, "_decrypted", _decrypted)
    return reads


@pytest.mark.parametrize(
    "source",
    [
        SimpleNamespace(id="s", password=_REF),
        SimpleNamespace(id="s", host=f"{_REF}.example.com"),
        SimpleNamespace(id="s", federation_hints={"s3_secret": _REF}),
        SimpleNamespace(id="s", mapping={"credentials_json": _REF}),
    ],
)
def test_a_source_with_a_vault_reference_in_any_connection_value_names_an_org_secret(source):
    assert names_org_secret(source)


def test_a_source_with_literal_and_environment_values_names_none():
    source = SimpleNamespace(
        id="s", host="db", password="${env:PG_PASSWORD}", federation_hints={"a": 1}, mapping=None
    )
    assert not names_org_secret(source)


async def test_the_refresh_resolves_the_secret_under_the_org_the_sources_are_registered_in(vault):
    admin_db = object()
    state = SimpleNamespace(active_org_id="org-a", admin_db=admin_db)
    with pytest.raises(KeyError, match="no organization is bound"):
        resolve_secrets(_REF)
    async with org_vault(state, [SimpleNamespace(id="s", password=_REF)]):
        assert resolve_secrets(_REF) == "pw-of-org-a"
    assert vault == [(admin_db, "org-a")]
    with pytest.raises(KeyError, match="no organization is bound"):
        resolve_secrets(_REF)


async def test_sources_that_name_no_secret_bind_nothing_and_read_nothing(vault):
    async with org_vault(_NoState(), [SimpleNamespace(id="s", password="literal")]):
        assert secrets_store.bound_org_id() is None
    assert vault == []


async def test_a_vault_already_bound_for_that_org_is_kept(vault):
    state = SimpleNamespace(active_org_id="org-a", admin_db=object())
    async with secrets_store.bound(state.admin_db, "org-a", user_id="u-1"):
        held = secrets_store._bound.get()
        async with org_vault(state, [SimpleNamespace(id="s", password=_REF)]):
            assert secrets_store._bound.get() is held  # the acting person's vault stays bound
    assert len(vault) == 2  # the outer bind's org and personal reads; none from org_vault


async def test_another_orgs_binding_is_replaced_for_the_block(vault):
    state = SimpleNamespace(active_org_id="org-b", admin_db=object())
    async with secrets_store.bound(state.admin_db, "org-a"):
        async with org_vault(state, [SimpleNamespace(id="s", password=_REF)]):
            assert resolve_secrets(_REF) == "pw-of-org-b"
        assert resolve_secrets(_REF) == "pw-of-org-a"


async def test_the_reconcile_runs_inside_the_orgs_vault(vault, monkeypatch):
    from provisa.federation import registry_view
    from provisa.federation.runtime import EngineRuntime

    seen: list[str] = []

    class _Backend:
        async def reconcile_landed_tables(self, state):
            seen.append(resolve_secrets(_REF))
            return [("src", "orders")]

        async def refresh_landed_views(self, state):
            return None

    async def _sources(state, conn=None):
        return [SimpleNamespace(id="src", password=_REF)]

    monkeypatch.setattr(registry_view, "registered_sources", _sources)
    runtime = EngineRuntime.__new__(EngineRuntime)
    runtime._backend = _Backend()
    runtime._state = SimpleNamespace(active_org_id="org-a", admin_db=object())
    assert await runtime.reconcile_landed_tables() == [("src", "orders")]
    assert seen == ["pw-of-org-a"]


def _walking_backend(resolved: list[str]):
    """A native backend whose attach walk resolves the source's secret, as the real one does."""
    from provisa.federation.native_backend import NativeEngineBackend

    class _Backend(NativeEngineBackend):
        def _runtime_for(self, state):
            resolved.append(resolve_secrets(_REF))
            self._runtime = "runtime"
            self._walked = self._registry_of(state)
            return self._runtime

    backend = _Backend.__new__(_Backend)
    backend._runtime = None
    backend._walked = None
    return backend


async def test_a_pending_attach_walk_runs_inside_the_orgs_vault(vault, monkeypatch):
    from provisa.federation import registry_view

    async def _sources(state, conn=None):
        return [SimpleNamespace(id="src", password=_REF)]

    monkeypatch.setattr(registry_view, "registered_sources", _sources)
    resolved: list[str] = []
    backend = _walking_backend(resolved)
    state = SimpleNamespace(
        active_org_id="org-a",
        admin_db=object(),
        config=object(),
        runtime_sources={},
        tables=[],
        tenant_db=None,
    )
    assert await backend._attached_runtime(state) == "runtime"
    assert resolved == ["pw-of-org-a"]
    assert len(vault) == 1


async def test_a_statement_with_no_walk_pending_binds_nothing(vault, monkeypatch):
    from provisa.federation import registry_view
    from provisa.federation.native_backend import NativeEngineBackend

    async def _sources(state, conn=None):
        raise AssertionError("the registry was read although no walk was pending")

    monkeypatch.setattr(registry_view, "registered_sources", _sources)

    class _Backend(NativeEngineBackend):
        def _runtime_for(self, state):
            assert secrets_store.bound_org_id() is None
            return self._runtime

    backend = _Backend.__new__(_Backend)
    backend._runtime = "runtime"
    state = SimpleNamespace(config=object(), runtime_sources={}, tables=[], tenant_db=None)
    backend._walked = backend._registry_of(state)
    assert await backend._attached_runtime(state) == "runtime"
    assert vault == []
