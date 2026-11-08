# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1327 (Amended 2026-11-08): naming conventions and the processing settings an org owns.

The failure worth pinning is a value carried process-wide: whichever org saved or compiled last
governing every other org in the same process. Each read here runs under an org binding, the way a
request or a background pass does, against the org's own settings rows.
"""

# Requirements: REQ-1327

from __future__ import annotations

import asyncio
import types

import pytest

from provisa.api.errors import ApiError
from provisa.compiler import naming
from provisa.compiler.sampling import get_sample_size
from provisa.core import request_context, settings_registry
from provisa.core.org_settings import graphql_remote_max_rows, write_org_overrides
from provisa.core.request_context import reset_current_org, set_current_org

ALPHA = "alpha"
BETA = "beta"

OVERRIDES = {
    ALPHA: {
        "naming": {"convention": "apollo_graphql", "sql_convention": "snake"},
        "replication": {"hot_threshold": 7, "hot_interval": 11, "hot_max_rows": 13},
        "hot_tables": {"auto_threshold": 17, "max_bytes": 19},
        "sampling": {"default_sample_size": 23},
        "graphql_remote": {"max_rows": 29},
    },
    BETA: {
        "naming": {"convention": "hasura_graphql", "sql_convention": "hasura_graphql"},
        "replication": {"hot_threshold": 31, "hot_interval": 37, "hot_max_rows": 41},
        "hot_tables": {"auto_threshold": 43, "max_bytes": 47},
        "sampling": {"default_sample_size": 53},
        "graphql_remote": {"max_rows": 59},
    },
}


@pytest.fixture
def two_orgs():
    """Two orgs in one process, each with its own settings rows, as the runtime holds them."""
    request_context.register_org_overrides_provider(
        lambda: OVERRIDES.get(request_context.current_org.get(), {})
    )
    yield
    request_context.register_org_overrides_provider(None)


class _Bound:
    def __init__(self, org: str | None):
        self.org = org

    def __enter__(self):
        self.token = request_context.current_org.set(self.org)

    def __exit__(self, *exc):
        reset_current_org(self.token)


def as_org(org: str | None) -> _Bound:
    return _Bound(org)


class TestNamingIsPerOrg:
    def test_each_org_compiles_names_by_its_own_convention(self, two_orgs):
        with as_org(ALPHA):
            assert naming.apply_gql_name("order_items") == "orderItems"
            assert naming.active_gql_convention() == "apollo_graphql"
        with as_org(BETA):
            assert naming.apply_gql_name("order_items") == "order_items"
            assert naming.active_gql_convention() == "hasura_graphql"

    def test_the_sql_plane_is_per_org_too(self, two_orgs):
        with as_org(ALPHA):
            alpha_sql = naming.active_sql_form()
        with as_org(BETA):
            beta_sql = naming.active_sql_form()
        assert alpha_sql == "snake_case"
        assert beta_sql == naming._canonical_convention("hasura_graphql")

    def test_two_orgs_compiling_at_once_do_not_cross(self, two_orgs):
        """Interleaved on one event loop, as two requests in one worker are."""

        async def compile_for(org: str, expected: str) -> list[str]:
            token = set_current_org(org)
            try:
                seen = []
                for _ in range(20):
                    await asyncio.sleep(0)
                    seen.append(naming.apply_gql_name("order_items"))
                assert set(seen) == {expected}
                return seen
            finally:
                reset_current_org(token)

        async def both():
            await asyncio.gather(compile_for(ALPHA, "orderItems"), compile_for(BETA, "order_items"))

        asyncio.run(both())

    def test_saving_one_orgs_convention_moves_no_other(self, two_orgs):
        """The configured process value is the inherited base: it is not where an org's goes."""
        before = (naming._gql_convention, naming._sql_convention)
        OVERRIDES[ALPHA]["naming"]["convention"] = "snake"
        try:
            with as_org(BETA):
                assert naming.apply_gql_name("order_items") == "order_items"
            assert (naming._gql_convention, naming._sql_convention) == before
        finally:
            OVERRIDES[ALPHA]["naming"]["convention"] = "apollo_graphql"

    def test_an_org_with_no_convention_inherits_the_deployments(self, two_orgs):
        naming.configure(gql="hasura_graphql", sql="snake")
        try:
            with as_org("gamma"):  # no rows
                assert naming.active_gql_convention() == "hasura_graphql"
            with as_org(None):
                assert naming.active_gql_convention() == "hasura_graphql"
        finally:
            naming.configure()


class TestRegistryReadsTheActingOrg:
    @pytest.mark.parametrize(
        "key,alpha,beta",
        [
            ("replication.hot_threshold", 7, 31),
            ("replication.hot_interval", 11, 37),
            ("replication.hot_max_rows", 13, 41),
            ("hot_tables.auto_threshold", 17, 43),
            ("hot_tables.max_bytes", 19, 47),
            ("sampling.default_sample_size", 23, 53),
        ],
    )
    def test_each_org_reads_its_own(self, two_orgs, key, alpha, beta):
        with as_org(ALPHA):
            assert settings_registry.value(key) == alpha
        with as_org(BETA):
            assert settings_registry.value(key) == beta

    def test_no_org_bound_reads_the_deployments(self, two_orgs):
        with as_org(None):
            assert (
                settings_registry.value("replication.hot_threshold")
                == settings_registry.resolve("replication.hot_threshold").value
            )

    def test_default_sample_size_is_the_orgs(self, two_orgs):
        with as_org(ALPHA):
            assert get_sample_size() == 23
        with as_org(BETA):
            assert get_sample_size() == 53

    def test_remote_row_limit_is_the_orgs(self, two_orgs):
        with as_org(ALPHA):
            assert graphql_remote_max_rows(10000) == 29
        with as_org(BETA):
            assert graphql_remote_max_rows(10000) == 59
        with as_org("gamma"):
            assert graphql_remote_max_rows(10000) == 10000

    def test_a_platform_setting_is_not_read_from_an_orgs_rows(self):
        """An org row that named a platform setting changes nothing: only what the catalog
        declares org-owned has an org layer."""
        request_context.register_org_overrides_provider(
            lambda: {"replication": {"builds_per_node": 999}, "cache": {"redis_url": "redis://x"}}
        )
        try:
            with as_org(ALPHA):
                assert settings_registry.value("replication.builds_per_node") != 999
                assert settings_registry.value("cache.redis_url") != "redis://x"
        finally:
            request_context.register_org_overrides_provider(None)

    def test_the_org_owned_set_is_exact(self):
        owned = {s.key for s in settings_registry.all_settings() if s.org_owned}
        assert owned == {
            "replication.hot_threshold",
            "replication.hot_interval",
            "replication.hot_max_rows",
            "hot_tables.auto_threshold",
            "hot_tables.max_rows",
            "hot_tables.max_bytes",
            "hot_tables.refresh_interval",
            "sampling.default_sample_size",
        }


class TestTheOrgDoor:
    @pytest.mark.parametrize(
        "updates,bad",
        [
            ({"graphql_remote": {"max_object_depth": 9}}, "graphql_remote.max_object_depth"),
            ({"replication": {"builds_per_node": 9}}, "replication.builds_per_node"),
            ({"hot_tables": {"redis_url": "x"}}, "hot_tables.redis_url"),
            ({"sampling": {"other": 1}}, "sampling.other"),
        ],
    )
    async def test_a_block_is_bounded_to_the_fields_the_org_owns(self, updates, bad):
        with pytest.raises(ValueError, match=bad.replace(".", r"\.")):
            await write_org_overrides(object(), updates, updated_by="alice")


# --- the settings API -----------------------------------------------------------------------------


def _coro(value):
    async def _c():
        return value

    return _c()


class _FakeState:
    def __init__(self):
        self.settings_overrides: dict = {}
        self.config = None
        self.deployment_cache_default_ttl = 300
        self.config_live_export = False
        self.tenant_db = object()
        self.model_db = self.tenant_db
        self.admin_db = object()
        self.response_cache_default_ttl = 300


@pytest.fixture
def app_state(monkeypatch):
    st = _FakeState()
    monkeypatch.setattr("provisa.api.app.state", st, raising=False)

    async def _read(_db):
        return dict(st.settings_overrides)

    async def _write(_db, updates, *, updated_by):
        for k, v in updates.items():
            if v is None:
                st.settings_overrides.pop(k, None)
            else:
                st.settings_overrides[k] = v
        return list(updates)

    async def _no_rebuild(**_kw):
        return None

    monkeypatch.setattr("provisa.core.org_settings.read_org_overrides", _read)
    monkeypatch.setattr("provisa.core.org_settings.write_org_overrides", _write)
    monkeypatch.setattr("provisa.api.app._rebuild_schemas", _no_rebuild, raising=False)
    return st


@pytest.fixture
def caller(monkeypatch):
    import provisa.api.admin.capabilities as capmod

    caps: set[str] = set()
    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: caps)

    def _as(*rights: str):
        caps.clear()
        caps.update(rights)
        return types.SimpleNamespace(
            state=types.SimpleNamespace(
                identity=types.SimpleNamespace(user_id="alice", roles=[]), active_org_id=ALPHA
            )
        )

    return _as


ORG_ADMIN = ("org_settings",)
PLATFORM_ADMIN = ("platform_settings", "cross_org")


class TestAnOrgAdminEditsItsOwn:
    @pytest.mark.parametrize(
        "body,stored",
        [
            (
                {"naming": {"convention": "hasura_graphql", "sql_convention": "snake"}},
                {"naming": {"convention": "hasura_graphql", "sql_convention": "snake"}},
            ),
            ({"naming": {"domain_prefix": True}}, {"naming": {"domain_prefix": True}}),
            ({"sampling": {"default_sample_size": 5}}, {"sampling": {"default_sample_size": 5}}),
            ({"graphql_remote": {"max_rows": 500}}, {"graphql_remote": {"max_rows": 500}}),
            (
                {"replication": {"hot_threshold": 5, "hot_interval": 60, "hot_max_rows": 9}},
                {"replication": {"hot_threshold": 5, "hot_interval": 60, "hot_max_rows": 9}},
            ),
            (
                {"hot_tables": {"auto_threshold": 5, "max_bytes": 1024}},
                {"hot_tables": {"auto_threshold": 5, "max_bytes": 1024}},
            ),
        ],
    )
    async def test_saved_to_the_orgs_rows_with_the_org_right_alone(
        self, caller, app_state, body, stored
    ):
        from provisa.api.admin import settings_router as router

        request = caller(*ORG_ADMIN)
        request.json = lambda: _coro(body)
        result = await router.update_settings(request)
        assert result["success"] is True
        assert app_state.settings_overrides == stored

    async def test_a_value_the_setting_refuses_is_refused(self, caller, app_state):
        from provisa.api.admin import settings_router as router

        request = caller(*ORG_ADMIN)
        request.json = lambda: _coro({"replication": {"hot_threshold": 0}})
        with pytest.raises(ApiError) as exc:
            await router.update_settings(request)
        assert exc.value.status_code == 400
        assert app_state.settings_overrides == {}

    async def test_an_unknown_convention_is_refused(self, caller, app_state):
        from provisa.api.admin import settings_router as router

        request = caller(*ORG_ADMIN)
        request.json = lambda: _coro({"naming": {"convention": "shouty"}})
        with pytest.raises(ApiError) as exc:
            await router.update_settings(request)
        assert exc.value.status_code == 400
        assert app_state.settings_overrides == {}

    async def test_null_clears_back_to_the_deployments(self, caller, app_state):
        from provisa.api.admin import settings_router as router

        app_state.settings_overrides = {"sampling": {"default_sample_size": 5}}
        request = caller(*ORG_ADMIN)
        request.json = lambda: _coro({"sampling": {"default_sample_size": None}})
        await router.update_settings(request)
        assert app_state.settings_overrides == {}

    async def test_saving_a_convention_keeps_the_domain_mode(self, caller, app_state):
        from provisa.api.admin import settings_router as router

        app_state.settings_overrides = {"naming": {"use_domains": False, "default_domain": "core"}}
        request = caller(*ORG_ADMIN)
        request.json = lambda: _coro({"naming": {"convention": "snake"}})
        await router.update_settings(request)
        assert app_state.settings_overrides["naming"] == {
            "use_domains": False,
            "default_domain": "core",
            "convention": "snake",
        }

    async def test_the_domain_mode_stays_off_this_door(self, caller, app_state):
        from provisa.api.admin import settings_router as router

        request = caller(*ORG_ADMIN)
        request.json = lambda: _coro({"naming": {"use_domains": True}})
        with pytest.raises(ApiError) as exc:
            await router.update_settings(request)
        assert exc.value.status_code == 400

    def test_a_domain_policy_change_keeps_the_conventions(self):
        from provisa.api.admin.settings_router import naming_after_domain_policy

        before = {"convention": "snake", "use_domains": False, "default_domain": "core"}
        assert naming_after_domain_policy(before, True, "core") == {
            "convention": "snake",
            "use_domains": True,
        }
        assert naming_after_domain_policy({"convention": "snake"}, None, "x") == {
            "convention": "snake"
        }
        assert naming_after_domain_policy({"use_domains": True}, None, "x") is None

    async def test_an_org_admin_reads_its_own_values(self, caller, app_state, two_orgs):
        from provisa.api.admin import settings_router as router

        with as_org(ALPHA):
            body = await router.get_settings(caller(*ORG_ADMIN))
        assert body["naming"]["convention"] == "apollo_graphql"
        assert body["naming"]["sql_convention"] == "snake"
        assert body["sampling"]["default_sample_size"] == 23
        assert body["graphql_remote"] == {"max_rows": 29}
        assert body["replication"] == {"hot_threshold": 7, "hot_interval": 11, "hot_max_rows": 13}
        assert body["hot_tables"]["auto_threshold"] == 17
        # REQ-230: with no ceiling of its own the row ceiling is the auto threshold.
        assert body["hot_tables"]["max_rows"] == 17
        assert "cdc" not in body


class TestPlatformOnlyStaysPlatformOnly:
    @pytest.mark.parametrize(
        "body",
        [
            {"graphql_remote": {"max_object_depth": 3}},
            {"graphql_remote": {"max_rows": 5, "max_list_items": 3}},
            {"cdc": {"consumer_group_id": "g"}},
            {"engine": {}},
            {"otel": {}},
        ],
    )
    async def test_refused_with_the_org_right_alone(self, caller, app_state, body):
        from provisa.api.admin import settings_router as router

        request = caller(*ORG_ADMIN)
        request.json = lambda: _coro(body)
        with pytest.raises(ApiError) as exc:
            await router.update_settings(request)
        assert exc.value.status_code == 403
        assert app_state.settings_overrides == {}

    async def test_the_store_connections_are_the_platforms(self, caller, app_state):
        from provisa.api.admin import settings_router as router

        with pytest.raises(ApiError) as get:
            await router.get_cache_storage(caller(*ORG_ADMIN))
        assert get.value.status_code == 403
        request = caller(*ORG_ADMIN)
        request.json = lambda: _coro({"cache": {"redis_url": "redis://x"}})
        with pytest.raises(ApiError) as put:
            await router.set_cache_storage(request)
        assert put.value.status_code == 403

    def test_the_platform_door_no_longer_carries_the_orgs_blocks(self):
        from provisa.api.admin.settings_router import _CACHE_STORAGE_SETTINGS

        assert "hot_tables" not in _CACHE_STORAGE_SETTINGS
        assert "replication" not in _CACHE_STORAGE_SETTINGS

    async def test_a_platform_admin_keeps_the_cdc_group(self, caller, app_state, monkeypatch):
        from provisa.api.admin import settings_router as router

        written: dict = {}
        monkeypatch.setattr(router, "read_config", lambda: {})
        monkeypatch.setattr(router, "config_path", lambda: "p")
        monkeypatch.setattr(router, "write_config", lambda _p, cfg: written.update(cfg))
        request = caller(*PLATFORM_ADMIN)
        request.json = lambda: _coro({"cdc": {"consumer_group_id": "receiver"}})
        await router.update_settings(request)
        assert written["cdc_consumer_group_id"] == "receiver"
        assert app_state.settings_overrides == {}


class TestAppWiring:
    def test_the_runtime_of_the_bound_org_supplies_the_rows(self, monkeypatch):
        from provisa.api import app as app_mod

        rts = {
            ALPHA: types.SimpleNamespace(
                settings_overrides={"sampling": {"default_sample_size": 3}}
            )
        }
        monkeypatch.setattr(
            app_mod.state,
            "org_registry",
            types.SimpleNamespace(get=lambda key: rts.get(key)),
        )
        with as_org(ALPHA):
            assert app_mod._acting_org_overrides() == {"sampling": {"default_sample_size": 3}}
        with as_org("unbuilt"):
            assert app_mod._acting_org_overrides() == {}


class TestHotEvaluationCadence:
    async def test_each_org_is_evaluated_on_its_own_interval(self, two_orgs, monkeypatch):
        from provisa.federation import replica_hot

        evaluated: list[str] = []

        async def _evaluate(_state, workers):
            evaluated.append(request_context.current_org.get())

        slept: list[float] = []

        async def _sleep(seconds):
            slept.append(seconds)
            clock[0] += seconds
            if len(slept) > 8:
                raise asyncio.CancelledError

        clock = [0.0]
        monkeypatch.setattr(replica_hot, "evaluate", _evaluate)
        monkeypatch.setattr(replica_hot.asyncio, "sleep", _sleep)
        monkeypatch.setattr(replica_hot.time, "monotonic", lambda: clock[0])
        rts = {
            k: types.SimpleNamespace(org_id=k, env="prod", tenant_db=object())
            for k in (ALPHA, BETA)
        }
        state = types.SimpleNamespace(
            org_registry=types.SimpleNamespace(all_org_ids=lambda: list(rts), get=rts.get)
        )
        with pytest.raises(asyncio.CancelledError):
            await replica_hot.evaluation_loop(state, should_run=lambda: True, workers=1)
        # Alpha every 11 s; beta's 37 s falls due on the ticks at 44 s and 88 s.
        assert evaluated.count(ALPHA) == 8
        assert evaluated.count(BETA) == 2
