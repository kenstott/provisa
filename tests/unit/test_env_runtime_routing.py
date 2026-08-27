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

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from provisa.api.env_routing import (
    ENV_HEADER,
    SWITCH_CAPABILITY,
    EnvironmentRightError,
    EnvironmentSelectionError,
    env_header_value,
    may_switch,
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


def _fake_db(known: set[str]) -> Any:
    """A ``_FakeDb`` in the position ``select_environment`` types as ``Database``.

    The selection never touches the connection itself -- it hands the handle to ``get_env``, which
    every one of these tests replaces -- so the fake is the whole surface the function uses.
    """
    return _FakeDb(known)


def _row(
    name: str, expires_at: datetime | None = None, idle_ttl_seconds: int | None = None
) -> dict:
    """A registry row as ``get_env`` returns one: every column, both expiry columns included."""
    return {
        "org_id": "acme",
        "name": name,
        "expires_at": expires_at,
        # REQ-1600: None is REQ-1523's fixed deadline -- the selection renews nothing.
        "idle_ttl_seconds": idle_ttl_seconds,
    }


class TestSelectEnvironment:
    @pytest.mark.asyncio
    async def test_no_header_is_prod_without_a_lookup(self, monkeypatch):
        db = _fake_db(set())
        # prod exists for every org from creation and cannot be deleted, so there is nothing a
        # query could tell us — the common path must cost nothing.
        assert await select_environment(db, "acme", None) == PROD
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_explicit_prod_is_prod_without_a_lookup(self):
        db = _fake_db(set())
        assert await select_environment(db, "acme", PROD) == PROD
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_illegal_name_is_refused_before_any_lookup(self):
        db = _fake_db(set())
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(db, "acme", "Not A Name")
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_unknown_name_is_refused_and_never_falls_back_to_prod(self, monkeypatch):
        db = _fake_db(set())

        async def _get_env(_db, org_id, name):
            _db.queried.append(name)
            return None

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(db, "acme", "feature")
        assert db.queried == ["feature"]

    @pytest.mark.asyncio
    async def test_known_name_is_returned(self, monkeypatch):
        db = _fake_db({"feature"})

        async def _get_env(_db, org_id, name):
            _db.queried.append(name)
            return _row(name) if name in _db.known else None

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        assert await select_environment(db, "acme", "feature") == "feature"

    @pytest.mark.asyncio
    async def test_a_future_expiry_is_served_normally(self, monkeypatch):
        # REQ-1523: an expiry is a deadline, not a mark. Until it passes the environment is
        # ordinary, and a request naming it is answered like any other.
        db = _fake_db({"feature"})
        later = datetime.now(timezone.utc) + timedelta(hours=1)

        async def _get_env(_db, org_id, name):
            _db.queried.append(name)
            return _row(name, later)

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        assert await select_environment(db, "acme", "feature") == "feature"

    @pytest.mark.asyncio
    async def test_a_passed_expiry_is_refused_before_the_sweep_reaches_it(self, monkeypatch):
        # REQ-1523: the reaper runs on a schedule, so an expired environment still HAS its schemas
        # between two ticks. Serving it because the sweep has not arrived would make the deadline
        # the tick rather than the expiry the org was told.
        db = _fake_db({"feature"})
        past = datetime.now(timezone.utc) - timedelta(seconds=1)

        async def _get_env(_db, org_id, name):
            _db.queried.append(name)
            return _row(name, past)

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        with pytest.raises(EnvironmentSelectionError, match="expired"):
            await select_environment(db, "acme", "feature")

    @pytest.mark.asyncio
    async def test_an_expired_environment_never_falls_back_to_prod(self, monkeypatch):
        # The same rule the unknown-name case states: a caller who believed they were writing to a
        # branch must not silently write to production.
        db = _fake_db({"feature"})
        past = datetime.now(timezone.utc) - timedelta(days=1)

        async def _get_env(_db, org_id, name):
            return _row(name, past)

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(db, "acme", "feature")

    @pytest.mark.asyncio
    async def test_no_admin_plane_is_a_server_error_not_a_404(self):
        # Not an EnvironmentSelectionError: the name may be perfectly good, the server simply
        # cannot answer the question, and rendering that as "no such environment" would be a lie.
        with pytest.raises(RuntimeError) as exc:
            await select_environment(None, "acme", "feature")
        assert not isinstance(exc.value, EnvironmentSelectionError)


class TestSwitchRight:
    """REQ-1573: being served by an environment other than prod is its own right."""

    @pytest.mark.asyncio
    async def test_prod_needs_no_right(self):
        # prod is what a request naming nothing is served, so an analyst reaching it is not a
        # switch and cannot be refused one.
        db = _fake_db(set())
        assert await select_environment(db, "acme", None, set()) == PROD
        assert await select_environment(db, "acme", PROD, set()) == PROD

    @pytest.mark.asyncio
    async def test_a_holder_is_served_the_branch(self, monkeypatch):
        db = _fake_db({"feature"})

        async def _get_env(_db, org_id, name):
            _db.queried.append(name)
            return _row(name) if name in _db.known else None

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        assert await select_environment(db, "acme", "feature", {SWITCH_CAPABILITY}) == "feature"

    @pytest.mark.asyncio
    async def test_without_the_right_it_is_a_403_and_never_a_404(self):
        # Distinct from EnvironmentSelectionError on purpose: telling the caller the name is
        # unknown would be a lie about the org's model, and the lookup is never even reached.
        db = _fake_db({"feature"})
        with pytest.raises(EnvironmentRightError):
            await select_environment(db, "acme", "feature", {"read", "query"})
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_an_illegal_name_is_still_a_404_first(self):
        # The name is refused for what it is before who asked is consulted, so a caller without
        # the right learns nothing about which names exist.
        db = _fake_db(set())
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(db, "acme", "Not A Name", set())

    @pytest.mark.asyncio
    async def test_no_capabilities_argument_is_an_unguarded_call_site(self, monkeypatch):
        # Passing None is not "no capabilities" — it is a caller that has already decided the
        # question (the CLI, an internal rebind), and the gate sits at the HTTP selection point.
        db = _fake_db({"feature"})

        async def _get_env(_db, org_id, name):
            return _row(name) if name in _db.known else None

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        assert await select_environment(db, "acme", "feature") == "feature"

    def test_platform_authority_carries_the_right(self):
        assert may_switch({"superadmin"})
        assert may_switch({"admin"})
        assert may_switch({SWITCH_CAPABILITY})
        assert not may_switch({"read", "query", "create_model"})


class TestPinnedMembership:
    """REQ-1596: a membership confined to one environment is served that one and no other."""

    @pytest.fixture
    def known(self, monkeypatch):
        async def _get_env(_db, org_id, name):
            _db.queried.append(name)
            return _row(name) if name in _db.known else None

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)

    @pytest.mark.asyncio
    async def test_naming_nothing_serves_the_pin_not_prod(self, known):
        # The whole point of the pin: a sandbox visitor who simply omits the header must not be
        # handed the org's production data.
        db = _fake_db({"sandbox_ab12"})
        assert await select_environment(db, "acme", None, set(), pinned="sandbox_ab12") == (
            "sandbox_ab12"
        )

    @pytest.mark.asyncio
    async def test_naming_the_pin_is_the_same_answer(self, known):
        db = _fake_db({"sandbox_ab12"})
        assert (
            await select_environment(db, "acme", "sandbox_ab12", set(), pinned="sandbox_ab12")
            == "sandbox_ab12"
        )

    @pytest.mark.asyncio
    async def test_naming_another_environment_is_refused(self):
        db = _fake_db({"sandbox_ab12", "feature"})
        with pytest.raises(EnvironmentRightError):
            await select_environment(db, "acme", "feature", set(), pinned="sandbox_ab12")
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_naming_prod_is_refused_too(self):
        # prod is the one name that needs no right, which is exactly why a pinned member asking
        # for it has to be refused here rather than falling through to the ordinary rule.
        db = _fake_db({"sandbox_ab12"})
        with pytest.raises(EnvironmentRightError):
            await select_environment(db, "acme", PROD, set(), pinned="sandbox_ab12")
        assert db.queried == []

    @pytest.mark.asyncio
    async def test_the_pin_needs_no_switch_right(self, known):
        # The sandbox role withholds environment_switch so the visitor cannot leave; requiring it
        # for the pin itself would leave them with nowhere to be served at all.
        db = _fake_db({"sandbox_ab12"})
        assert not may_switch(set())
        assert await select_environment(db, "acme", None, set(), pinned="sandbox_ab12") == (
            "sandbox_ab12"
        )

    @pytest.mark.asyncio
    async def test_an_expired_pin_stops_serving(self, monkeypatch):
        past = datetime.now(timezone.utc) - timedelta(minutes=1)

        async def _get_env(_db, org_id, name):
            return _row(name, past)

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        db = _fake_db({"sandbox_ab12"})
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(db, "acme", None, set(), pinned="sandbox_ab12")

    @pytest.mark.asyncio
    async def test_a_retired_pin_is_refused_and_never_falls_back(self, monkeypatch):
        async def _get_env(_db, org_id, name):
            return None

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        db = _fake_db(set())
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(db, "acme", None, set(), pinned="sandbox_ab12")


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
