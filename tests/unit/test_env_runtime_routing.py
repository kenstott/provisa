# Copyright (c) 2026 Kenneth Stott
# Canary: 202546c3-214e-4732-b525-2232901dcb2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1487/REQ-1488/REQ-1529: an environment is part of a runtime's identity, and the header
that selects one is checked before anything is bound to it."""

from __future__ import annotations

import pytest

from provisa.api.env_routing import (
    ENV_HEADER,
    EnvironmentSelectionError,
    env_header_value,
    select_environment,
)
from provisa.api.org_runtime import (
    OrgRegistry,
    OrgRuntime,
    active_env,
    reset_current_env,
    runtime_key,
    set_current_env,
)
from provisa.core.environments import PROD, org_schema


class TestRuntimeKey:
    def test_prod_keys_on_the_bare_org_id(self):
        # An org that never created an environment is registered under exactly the key it always
        # was, so every pre-environment call site keeps resolving it.
        assert runtime_key("acme") == "acme"
        assert runtime_key("acme", None) == "acme"
        assert runtime_key("acme", PROD) == "acme"

    def test_a_branch_gets_its_own_key(self):
        assert runtime_key("acme", "feature") == "acme_env_feature"

    def test_key_separator_matches_the_schema_separator(self):
        # Same ``_env_`` boundary the schema name uses, for the same REQ-1309 reason: an org id
        # cannot contain an underscore, so the first ``_env_`` splits either name unambiguously.
        assert org_schema("acme", "feature").endswith("_env_feature")
        assert runtime_key("acme", "feature").endswith("_env_feature")

    def test_two_orgs_never_collide_through_an_environment(self):
        assert runtime_key("acme", "b") != runtime_key("acmeb")


class TestRegistryEnvKeys:
    def _registry(self) -> OrgRegistry:
        reg = OrgRegistry()
        for key, env in (("acme", PROD), ("acme_env_a", "a"), ("acme_env_b", "b"), ("other", PROD)):
            reg.set(key, OrgRuntime(org_id="acme" if key.startswith("acme") else "other", env=env))
        return reg

    def test_env_keys_lists_prod_and_every_branch(self):
        assert sorted(self._registry().env_keys("acme")) == ["acme", "acme_env_a", "acme_env_b"]

    def test_invalidate_alone_reaches_only_prod(self):
        reg = self._registry()
        reg.invalidate("acme")
        assert sorted(reg.env_keys("acme")) == ["acme_env_a", "acme_env_b"]

    def test_invalidate_org_takes_the_branches_with_it(self):
        reg = self._registry()
        reg.invalidate_org("acme")
        assert reg.env_keys("acme") == []
        assert reg.get("other") is not None


class TestActiveEnv:
    def test_unbound_is_prod(self):
        assert active_env() == PROD

    def test_bound_env_is_returned_and_resets(self):
        token = set_current_env("feature")
        try:
            assert active_env() == "feature"
        finally:
            reset_current_env(token)
        assert active_env() == PROD


class TestEnvHeaderValue:
    def test_absent_header_names_none(self):
        assert env_header_value([(b"host", b"x")]) is None

    def test_header_is_read_case_insensitively(self):
        assert env_header_value([(ENV_HEADER.upper().encode(), b"feature")]) == "feature"

    def test_whitespace_only_header_names_none(self):
        assert env_header_value([(ENV_HEADER.encode(), b"  ")]) is None


class _FakeDb:
    """Stands in for the admin plane, recording whether it was asked anything at all."""

    def __init__(self, known: set[str]):
        self.known = known
        self.queried: list[str] = []


class TestSelectEnvironment:
    @pytest.mark.asyncio
    async def test_no_header_is_prod_without_a_lookup(self, monkeypatch):
        db = _FakeDb(set())
        # prod exists for every org from creation and cannot be deleted, so there is nothing a
        # query could tell us — the common path must cost nothing.
        assert await select_environment(db, "acme", None) == PROD
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_explicit_prod_is_prod_without_a_lookup(self):
        db = _FakeDb(set())
        assert await select_environment(db, "acme", PROD) == PROD
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_illegal_name_is_refused_before_any_lookup(self):
        db = _FakeDb(set())
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(db, "acme", "Not A Name")
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_unknown_name_is_refused_and_never_falls_back_to_prod(self, monkeypatch):
        db = _FakeDb(set())

        async def _get_env(_db, org_id, name):
            _db.queried.append(name)
            return None

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(db, "acme", "feature")
        assert db.queried == ["feature"]

    @pytest.mark.asyncio
    async def test_known_name_is_returned(self, monkeypatch):
        db = _FakeDb({"feature"})

        async def _get_env(_db, org_id, name):
            _db.queried.append(name)
            return {"name": name} if name in _db.known else None

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        assert await select_environment(db, "acme", "feature") == "feature"

    @pytest.mark.asyncio
    async def test_no_admin_plane_is_a_server_error_not_a_404(self):
        # Not an EnvironmentSelectionError: the name may be perfectly good, the server simply
        # cannot answer the question, and rendering that as "no such environment" would be a lie.
        with pytest.raises(RuntimeError) as exc:
            await select_environment(None, "acme", "feature")
        assert not isinstance(exc.value, EnvironmentSelectionError)


class _FakeState:
    """The two attributes ``AppState._active_runtime`` reads, and nothing else.

    Called unbound rather than through a constructed AppState: the method's whole subject is the
    registry lookup, and building a real AppState would drag in control planes that have no
    bearing on which key it resolves.
    """

    def __init__(self, registry: OrgRegistry, org_id: str = "acme"):
        self.org_registry = registry
        self.org_id = org_id


class TestActiveRuntimeSelectsTheEnvironment:
    def _state(self) -> _FakeState:
        reg = OrgRegistry()
        reg.set("acme", OrgRuntime(org_id="acme", env=PROD))
        reg.set("acme_env_feature", OrgRuntime(org_id="acme", env="feature"))
        return _FakeState(reg)

    def test_unbound_env_resolves_prod(self):
        from provisa.api.app import AppState

        assert AppState._active_runtime(self._state()).env == PROD

    def test_bound_env_resolves_that_branch(self):
        from provisa.api.app import AppState

        token = set_current_env("feature")
        try:
            rt = AppState._active_runtime(self._state())
        finally:
            reset_current_env(token)
        assert rt.env == "feature"

    def test_unbuilt_branch_raises_rather_than_serving_prod(self):
        from provisa.api.app import AppState

        state = self._state()
        state.org_registry.invalidate("acme_env_feature")
        token = set_current_env("feature")
        try:
            with pytest.raises(RuntimeError, match="no runtime built for environment"):
                AppState._active_runtime(state)
        finally:
            reset_current_env(token)
