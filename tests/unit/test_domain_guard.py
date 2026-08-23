# Copyright (c) 2026 Kenneth Stott
# Canary: 834ec64c-2400-4671-8733-d6fba1f6c183
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1530, REQ-1531: what a mutation may be done TO — the domain half of the gate."""

import pytest

from provisa.api.admin import capabilities as caps_mod
from provisa.api.admin import domain_guard as guard
from provisa.security.rights import domain_access_for_claims


class _Identity:
    def __init__(self, roles, user_id="u1"):
        self.roles = roles
        self.user_id = user_id


class _State:
    def __init__(self, roles):
        self.roles = roles


class _Request:
    def __init__(self, identity):
        self.state = type("S", (), {"identity": identity})()


class _Info:
    def __init__(self, identity):
        self.context = {"request": _Request(identity)}


_SALES_DEV = {
    "sales_developer": {
        "capabilities": ["create_view", "query_development", "table_registration"],
        "domain_access": ["sales"],
    },
    "unlimited_developer": {
        "capabilities": ["create_view", "table_registration"],
        "domain_access": ["*"],
    },
    "platform_admin": {"capabilities": ["admin"], "domain_access": ["sales"]},
}


@pytest.fixture
def multi_domain(monkeypatch):
    """Domains are a gate: single-domain mode exempts everything, which would hide every check."""
    monkeypatch.setattr("provisa.core.domain_policy.single_domain", lambda: False)


@pytest.fixture
def registry(monkeypatch):
    monkeypatch.setattr("provisa.api.app.state", _State(_SALES_DEV), raising=False)
    return _SALES_DEV


class TestTheScopeIsTheRoles:
    def test_the_domain_suffix_on_a_claim_is_not_the_scope(self):
        # REQ-1530: the suffix records which grant was made; the scope is the role's. A claim
        # naming sales_developer reaches sales whether or not the claim says so.
        assert domain_access_for_claims(["sales_developer:finance"], _SALES_DEV) == {"sales"}

    def test_an_unknown_role_reaches_no_domain(self):
        assert domain_access_for_claims(["not_a_role"], _SALES_DEV) == set()

    def test_a_registry_that_dropped_domain_access_is_refused_rather_than_read_as_empty(self):
        with pytest.raises(ValueError, match="NOT NULL"):
            domain_access_for_claims(["r"], {"r": {"capabilities": []}})


class TestRequireDomain:
    def test_a_scoped_member_may_act_in_their_own_domain(self, multi_domain, registry):
        caps_mod.require_domain(_Info(_Identity(["sales_developer"])), "sales")

    def test_a_scoped_member_is_refused_outside_it(self, multi_domain, registry):
        with pytest.raises(PermissionError, match="finance"):
            caps_mod.require_domain(_Info(_Identity(["sales_developer"])), "finance")

    def test_an_unnarrowed_member_may_act_anywhere(self, multi_domain, registry):
        caps_mod.require_domain(_Info(_Identity(["unlimited_developer"])), "finance")

    def test_the_platform_administrator_bypasses_it(self, multi_domain, registry):
        # REQ-1297: the bypass is the same one require_capability honours.
        caps_mod.require_domain(_Info(_Identity(["platform_admin"])), "finance")

    def test_dev_mode_with_no_identity_is_not_gated(self, multi_domain, registry):
        caps_mod.require_domain(_Info(None), "finance")

    def test_anonymous_is_not_gated(self, multi_domain, registry):
        caps_mod.require_domain(_Info(_Identity(["sales_developer"], "anonymous")), "finance")

    def test_single_domain_mode_gates_nothing(self, monkeypatch, registry):
        monkeypatch.setattr("provisa.core.domain_policy.single_domain", lambda: True)
        caps_mod.require_domain(_Info(_Identity(["sales_developer"])), "finance")

    def test_require_capability_still_applies_the_domain_half(self, multi_domain, registry):
        info = _Info(_Identity(["sales_developer"]))
        caps_mod.require_capability(info, "table_registration", domain_id="sales")
        with pytest.raises(PermissionError, match="finance"):
            caps_mod.require_capability(info, "table_registration", domain_id="finance")

    def test_the_right_is_checked_before_the_domain(self, multi_domain, registry):
        with pytest.raises(PermissionError, match="Missing capability"):
            caps_mod.require_capability(
                _Info(_Identity(["unlimited_developer"])), "masking_config", domain_id="sales"
            )


class TestReferencedTableNames:
    def test_it_names_the_tables_a_view_reads(self):
        assert guard.referenced_table_names("SELECT * FROM orders JOIN customers ON 1=1") == {
            "orders",
            "customers",
        }

    def test_a_cte_names_no_governed_object(self):
        # A CTE is defined in the statement, so it is not a table the model governs.
        sql = "WITH recent AS (SELECT * FROM orders) SELECT * FROM recent"
        assert guard.referenced_table_names(sql) == {"orders"}

    def test_unparseable_sql_fails_closed(self):
        # REQ-1531: an authorization question about SQL nobody can parse has no safe yes.
        with pytest.raises(ValueError, match="unknown"):
            guard.referenced_table_names("SELECT FROM WHERE ((")


class _FakeConn:
    def __init__(self, by_name):
        self._by_name = by_name


async def _fake_find_by_table_name(conn, name):
    return conn._by_name.get(name)


class TestViewReadDomains:
    @pytest.fixture(autouse=True)
    def _patch_repo(self, monkeypatch):
        monkeypatch.setattr(
            "provisa.core.repositories.table.find_by_table_name", _fake_find_by_table_name
        )

    @pytest.mark.asyncio
    async def test_it_collects_the_domain_of_every_table_read(self):
        conn = _FakeConn({"orders": {"domain_id": "sales"}, "ledger": {"domain_id": "finance"}})
        assert await guard.view_read_domains(conn, "SELECT * FROM orders JOIN ledger ON 1=1") == {
            "sales",
            "finance",
        }

    @pytest.mark.asyncio
    async def test_an_unregistered_name_contributes_no_domain(self):
        conn = _FakeConn({"orders": {"domain_id": "sales"}})
        assert await guard.view_read_domains(conn, "SELECT * FROM orders, nowhere") == {"sales"}

    @pytest.mark.asyncio
    async def test_a_view_reading_another_domain_is_refused(self, multi_domain, registry):
        # REQ-1531: otherwise a member of one domain publishes another domain's rows into a view
        # its owner does not govern.
        conn = _FakeConn({"orders": {"domain_id": "sales"}, "ledger": {"domain_id": "finance"}})
        with pytest.raises(PermissionError, match="finance"):
            await guard.require_view_within_domains(
                _Info(_Identity(["sales_developer"])),
                conn,
                "SELECT * FROM orders JOIN ledger ON 1=1",
            )

    @pytest.mark.asyncio
    async def test_a_view_within_the_members_domain_passes(self, multi_domain, registry):
        conn = _FakeConn({"orders": {"domain_id": "sales"}})
        await guard.require_view_within_domains(
            _Info(_Identity(["sales_developer"])), conn, "SELECT * FROM orders"
        )


async def _fake_get(conn, table_id):
    return conn._by_name.get(table_id)


class TestTableDomain:
    @pytest.mark.asyncio
    async def test_a_table_that_does_not_exist_is_a_lookup_error_not_a_permission_answer(
        self, monkeypatch
    ):
        monkeypatch.setattr("provisa.core.repositories.table.get", _fake_get)
        with pytest.raises(guard.DomainLookupError):
            await guard.table_domain(_FakeConn({}), 7)

    @pytest.mark.asyncio
    async def test_an_act_on_a_table_asks_about_the_domain_that_holds_it(
        self, monkeypatch, multi_domain, registry
    ):
        monkeypatch.setattr("provisa.core.repositories.table.get", _fake_get)
        conn = _FakeConn({7: {"domain_id": "finance"}, 8: {"domain_id": "sales"}})
        info = _Info(_Identity(["sales_developer"]))
        await guard.require_table_domain(info, conn, 8)
        with pytest.raises(PermissionError, match="finance"):
            await guard.require_table_domain(info, conn, 7)


class _ReqState:
    def __init__(self, identity):
        self.identity = identity


class _RestRequest:
    def __init__(self, identity):
        self.state = _ReqState(identity)


class TestTheRestPathAsksTheSameQuestion:
    """REQ-1531: the check grew up in the GraphQL resolver layer, so an admin REST router reaching
    the same table enforced nothing."""

    def test_a_developer_cannot_mint_a_role_over_rest(self, registry):
        from provisa.api.errors import ApiError

        with pytest.raises(ApiError) as e:
            caps_mod.require_capability_request(
                _RestRequest(_Identity(["sales_developer"])), "user_management"
            )
        assert e.value.status_code == 403

    def test_a_holder_of_the_right_may(self, monkeypatch):
        monkeypatch.setattr(
            "provisa.api.app.state",
            _State({"admin_role": {"capabilities": ["user_management"], "domain_access": ["*"]}}),
            raising=False,
        )
        caps_mod.require_capability_request(
            _RestRequest(_Identity(["admin_role"])), "user_management"
        )

    def test_dev_mode_is_not_gated(self, registry):
        caps_mod.require_capability_request(_RestRequest(None), "user_management")
