# Copyright (c) 2026 Kenneth Stott
# Canary: 6c4f0a93-2d1e-4b7a-9e58-0f3a7b2d8c15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: what the entitlement UI reads and writes beyond setting a list. Each capability scoped
or unscoped (/kinds); effective access per capability over the roles a request acts as; expiry
rules, each a date and what it ends, with a dry run that flags a rule removing nothing; add access,
which appends only what is new; and the timeline of what was added, by whom and until when."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from provisa.auth import role_grants
from provisa.core.schema_org import admin_audit_log, user_role_assignments
from provisa.entitlements import store
from provisa.entitlements.lists import HeldEntitlements
from provisa.security.rights import Capability
from tests.unit.test_entitlements_api import _as, _hold, _idp, _identity, _role, _set
from tests.unit.test_entitlements_api import api as api  # noqa: F401  (fixture)

pytestmark = pytest.mark.asyncio

_TOMORROW = (datetime.now(UTC) + timedelta(days=1)).isoformat()
_NEXT_WEEK = (datetime.now(UTC) + timedelta(days=7)).isoformat()
_AN_HOUR_AGO = (datetime.now(UTC) - timedelta(hours=1)).isoformat()

_REVENUE = ["finance.revenue", "finance.revenue.amount", "finance.revenue.id"]
_SALES = [
    "sales.customers",
    "sales.customers.id",
    "sales.orders",
    "sales.orders.customer_id",
    "sales.orders.id",
]


async def _put(api_, **over):
    response = await api_.client.put("/admin/entitlements/lists", json=_set(**over))
    assert response.status_code == 200, response.text
    return response.json()


def _rule(ends, ends_at=_TOMORROW, principal="alice", role="viewer", **over):
    return {
        "principal": principal,
        "role_id": role,
        "ends_at": ends_at,
        "ends": ends,
        "reason": "contract ends",
        **over,
    }


async def _add_rule(api_, ends, **over):
    response = await api_.client.post("/admin/entitlements/expiries", json=_rule(ends, **over))
    assert response.status_code == 200, response.text
    return response.json()


async def _audit(api_, action):
    async with api_.db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(admin_audit_log).where(admin_audit_log.c.action == action)
            )
        ).fetchall()
    return rows


async def _passed_rule(api_, ends, principal="alice", role="viewer", list_kind=None) -> str:
    """A rule whose date has passed: written to the store, as the API refuses a past date."""
    async with api_.db.acquire() as conn:
        rule_key = await store.add_expiry(
            conn,
            principal,
            role,
            datetime.fromisoformat(_AN_HOUR_AGO),
            ends,
            set_by="admin",
            reason="contract ended",
            list_kind=list_kind,
        )
        api_.state.held_entitlements = await store.load_held(conn)
    return rule_key


def _origin(role_id, lists, from_role=None, ended=()):
    return {
        "role_id": role_id,
        "lists": lists,
        "from_role": from_role or role_id,
        "ended": list(ended),
    }


# -- effective access per capability ----------------------------------------------------------


async def _capabilities(api_, principal="alice", role_ids="viewer"):
    response = await api_.client.get(
        "/admin/entitlements/effective/capabilities",
        params={"principal": principal, "role_ids": role_ids},
    )
    assert response.status_code == 200, response.text
    return {row["capability"]: row for row in response.json()}


async def test_read_expands_to_what_the_persons_lists_select(api):
    await _put(api)
    read = (await _capabilities(api))["read"]
    assert read["kind"] == "columns"
    assert read["identifiers"] == _REVENUE
    assert read["origins"] == [_origin("viewer", {"tables": "provisa"})]
    assert read["model_version"] == ["boot", 1]


async def test_several_roles_combine_by_union_per_capability(api):
    await _put(api)  # viewer: finance.revenue only; analyst: no list, so its whole schema
    caps = await _capabilities(api, role_ids="viewer,analyst")
    assert caps["read"]["identifiers"] == sorted([*_REVENUE, *_SALES])
    assert caps["read"]["origins"] == [
        _origin("viewer", {"tables": "provisa"}),
        _origin("analyst", {}),
    ]
    assert caps["traverse"]["identifiers"] == ["sales.orders_customer"]


async def test_an_unscoped_capability_has_no_kind_and_no_identifiers(api):
    api.state.roles["settings"] = _role([Capability.ORG_SETTINGS.value])
    api.state.role_chains["settings"] = ["settings"]
    caps = await _capabilities(api, role_ids="settings")
    assert caps[Capability.ORG_SETTINGS.value] == {
        "capability": Capability.ORG_SETTINGS.value,
        "kind": None,
        "identifiers": None,
        "origins": [_origin("settings", {})],
        "model_version": ["boot", 1],
    }


async def test_a_list_scoped_capability_reads_its_kinds_list(api):
    assign = Capability.ASSIGN_ENTITLEMENTS.value
    before = (await _capabilities(api, role_ids="granter"))[assign]
    assert (before["kind"], before["identifiers"]) == ("roles", ["*"])
    await _put(api, role="granter", kind="roles", ids=["viewer"])
    after = (await _capabilities(api, role_ids="granter"))[assign]
    assert after["identifiers"] == ["viewer"]
    assert after["origins"] == [_origin("granter", {"roles": "provisa"})]


async def test_the_identity_providers_list_is_named_as_the_origin(api):
    await _idp(api, "alice", {"viewer": {"tables": ("sales.orders",)}})
    read = (await _capabilities(api))["read"]
    assert read["identifiers"] == ["sales.orders", "sales.orders.customer_id", "sales.orders.id"]
    assert read["origins"] == [_origin("viewer", {"tables": "idp"})]


async def test_effective_capabilities_of_an_unknown_role_are_refused_by_name(api):
    response = await api.client.get(
        "/admin/entitlements/effective/capabilities",
        params={"principal": "alice", "role_ids": "viewer,ghost"},
    )
    assert response.status_code == 404
    assert response.json()["code"] == "entitlements.role_not_found"


async def test_reading_someone_elses_capabilities_needs_a_right(api):
    api.caller["identity"] = _identity("bob", "viewer")
    response = await api.client.get(
        "/admin/entitlements/effective/capabilities",
        params={"principal": "alice", "role_ids": "viewer"},
    )
    assert response.status_code == 403
    assert response.json()["code"] == "auth.missing_capability"


# -- expiry rules -----------------------------------------------------------------------------


async def test_a_rule_is_added_with_a_key_and_returns_the_grants_access(api):
    view = await _add_rule(api, {"read": ["finance"]})
    assert view["principal"] == "alice" and view["role_id"] == "viewer"
    [rule] = view["expiries"]
    assert rule["ends"] == {"read": ["finance"]}
    assert rule["rule_key"]
    assert rule["passed"] is False
    assert [r.detail["rule_key"] for r in await _audit(api, "entitlements.expiry_added")] == [
        rule["rule_key"]
    ]


async def test_a_rule_ends_what_it_names_at_its_date(api):
    await _passed_rule(api, {"read": ["finance"]})
    mask = _as(api, "alice")
    assert mask is not None
    assert not mask.has_table(1)
    assert mask.has_table(2) and mask.has_table(3)


@pytest.mark.parametrize(
    "ends",
    [
        {"read": ["hr"]},  # outside the role's schema: nothing of it to end
        {Capability.ENVIRONMENT_MANAGEMENT.value: ["dev"]},  # no bit in the mask
        {"read": []},
    ],
)
async def test_a_rule_that_would_remove_nothing_is_refused_by_name(api, ends):
    response = await api.client.post("/admin/entitlements/expiries", json=_rule(ends))
    assert response.status_code == 422
    assert response.json()["code"] == "entitlements.expiry_removes_nothing"
    assert api.state.held_entitlements == {}


async def test_adding_a_rule_needs_assign_entitlements(api):
    api.caller["identity"] = _identity("bob", "viewer")
    response = await api.client.post(
        "/admin/entitlements/expiries", json=_rule({"read": ["finance"]})
    )
    assert response.status_code == 403
    assert response.json()["code"] == "auth.missing_capability"


async def test_a_rule_is_changed_by_its_key(api):
    [rule] = (await _add_rule(api, {"read": ["finance"]}))["expiries"]
    response = await api.client.put(
        f"/admin/entitlements/expiries/{rule['rule_key']}",
        json={"ends_at": _NEXT_WEEK, "ends": {"*": ["finance"]}, "reason": "extended"},
    )
    assert response.status_code == 200, response.text
    [changed] = response.json()["expiries"]
    assert changed["rule_key"] == rule["rule_key"]
    assert changed["ends"] == {"*": ["finance"]}
    assert datetime.fromisoformat(changed["ends_at"]) == datetime.fromisoformat(_NEXT_WEEK)
    assert changed["reason"] == "extended"


async def test_a_change_that_would_remove_nothing_is_refused_by_name(api):
    [rule] = (await _add_rule(api, {"read": ["finance"]}))["expiries"]
    response = await api.client.put(
        f"/admin/entitlements/expiries/{rule['rule_key']}",
        json={"ends_at": _NEXT_WEEK, "ends": {"read": ["hr"]}, "reason": "x"},
    )
    assert response.status_code == 422
    assert response.json()["code"] == "entitlements.expiry_removes_nothing"


async def test_a_rule_is_withdrawn_by_its_key(api):
    rule = {"rule_key": await _passed_rule(api, {"read": ["finance"]})}
    response = await api.client.request(
        "DELETE", f"/admin/entitlements/expiries/{rule['rule_key']}", json={"reason": "renewed"}
    )
    assert response.status_code == 200, response.text
    assert response.json()["expiries"] == []
    assert _as(api, "alice") is None
    assert len(await _audit(api, "entitlements.expiry_withdrawn")) == 1


@pytest.mark.parametrize("method", ["PUT", "DELETE"])
async def test_an_unknown_rule_is_refused_by_name(api, method):
    body = {"ends_at": _TOMORROW, "ends": {}, "reason": "x"} if method == "PUT" else {"reason": "x"}
    response = await api.client.request(
        method, "/admin/entitlements/expiries/nosuchrule", json=body
    )
    assert response.status_code == 404
    assert response.json()["code"] == "entitlements.expiry_not_found"


# -- the dry run --------------------------------------------------------------------------------


async def _preview(api_, ends, **over):
    response = await api_.client.post(
        "/admin/entitlements/expiries/preview", json=_rule(ends, **over)
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_the_preview_names_what_a_rule_would_end_per_capability(api):
    assert await _preview(api, {"read": ["finance"]}) == {
        "removes": {"read": _REVENUE},
        "removes_nothing": False,
    }


async def test_the_preview_of_a_grant_wide_rule_ends_every_capability(api):
    removes = (await _preview(api, {}))["removes"]
    assert removes["read"] == sorted([*_REVENUE, *_SALES])
    assert removes["traverse"] == ["sales.orders_customer"]
    assert removes["execute"] == ["sales.refund"]


async def test_the_preview_flags_a_rule_that_would_remove_nothing(api):
    assert await _preview(api, {"read": ["hr"]}) == {"removes": {}, "removes_nothing": True}


async def test_the_preview_counts_the_rules_already_in_force(api):
    [rule] = (await _add_rule(api, {"read": ["finance"]}))["expiries"]
    later = (datetime.now(UTC) + timedelta(days=3)).isoformat()
    assert (await _preview(api, {"read": ["finance"]}, ends_at=later))["removes_nothing"] is True
    replacing = await _preview(api, {"read": ["finance"]}, ends_at=later, rule_key=rule["rule_key"])
    assert replacing["removes"] == {"read": _REVENUE}


async def test_the_preview_is_gated_as_a_write(api):
    api.caller["identity"] = _identity("bob", "viewer")
    response = await api.client.post(
        "/admin/entitlements/expiries/preview", json=_rule({"read": ["finance"]})
    )
    assert response.status_code == 403


# -- add access ---------------------------------------------------------------------------------


def _add(ids=("sales.orders",), **over):
    return {
        "principal": "alice",
        "role_id": "viewer",
        "kind": "tables",
        "identifiers": list(ids),
        "reason": "project",
        **over,
    }


async def _post_add(api_, **over):
    response = await api_.client.post("/admin/entitlements/lists/add", json=_add(**over))
    assert response.status_code == 200, response.text
    return response.json()


async def test_add_access_appends_without_restating_the_list(api):
    await _put(api)
    view = await _post_add(api)
    [tables] = [row for row in view["lists"] if row["kind"] == "tables"]
    assert tables["identifiers"] == ["finance.revenue", "sales.orders"]
    mask = _as(api, "alice")
    assert mask is not None
    assert mask.has_table(1) and mask.has_table(2) and not mask.has_table(3)


async def test_add_access_until_a_date_ends_only_what_it_added(api):
    await _put(api)
    view = await _post_add(api, until=_TOMORROW)
    [rule] = view["expiries"]
    assert rule["ends"] == {"*": ["sales.orders"]}
    assert rule["list_kind"] == "tables"


async def test_add_access_limited_to_capabilities_ends_only_those(api):
    await _put(api)
    view = await _post_add(api, until=_TOMORROW, capabilities=["read"])
    assert [r["ends"] for r in view["expiries"]] == [{"read": ["sales.orders"]}]


async def test_add_access_of_what_is_already_listed_adds_nothing_and_no_rule(api):
    await _put(api)
    view = await _post_add(api, ids=["finance.revenue"], until=_TOMORROW)
    assert view["expiries"] == []
    [tables] = [row for row in view["lists"] if row["kind"] == "tables"]
    assert tables["identifiers"] == ["finance.revenue"]


async def test_add_access_with_no_list_held_appends_to_the_roles_default(api):
    api.state.roles["viewer"]["default_entitlements"] = {"tables": ["finance.*"]}
    view = await _post_add(api)
    [tables] = [row for row in view["lists"] if row["kind"] == "tables"]
    assert (tables["identifiers"], tables["origin"]) == (["finance.*", "sales.orders"], "provisa")


async def test_add_access_the_identity_provider_supersedes_is_refused_by_name(api):
    await _idp(api, "alice", {"viewer": {"tables": ("finance.revenue",)}})
    response = await api.client.post("/admin/entitlements/lists/add", json=_add())
    assert response.status_code == 409
    assert response.json()["code"] == "entitlements.idp_supersedes"


async def test_add_access_needs_assign_entitlements(api):
    api.caller["identity"] = _identity("bob", "viewer")
    response = await api.client.post("/admin/entitlements/lists/add", json=_add())
    assert response.status_code == 403
    assert response.json()["code"] == "auth.missing_capability"


async def test_add_access_is_audited(api):
    await _put(api)
    await _post_add(api, until=_TOMORROW)
    [row] = await _audit(api, "entitlements.add")
    assert row.subject_id == "alice"
    assert row.detail["identifiers"] == ["sales.orders"]
    assert row.detail["kind"] == "tables"


# -- the timeline of additions -------------------------------------------------------------------


async def _history(api_, principal="alice", role_id="viewer"):
    response = await api_.client.get(
        "/admin/entitlements/lists/history",
        params={"principal": principal, "role_id": role_id},
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_the_history_lists_what_was_added_by_whom_and_until_when(api):
    await _put(api, expires_at=_TOMORROW)
    await _post_add(api, until=_NEXT_WEEK)
    first, second = await _history(api)
    assert (first["kind"], first["added"], first["set_by"], first["reason"]) == (
        "tables",
        ["finance.revenue"],
        "admin",
        "quarter close",
    )
    assert datetime.fromisoformat(first["until"]) == datetime.fromisoformat(_TOMORROW)
    assert second["added"] == ["sales.orders"]
    assert second["reason"] == "project"
    assert datetime.fromisoformat(second["until"]) == datetime.fromisoformat(_NEXT_WEEK)
    assert first["valid_from"] <= second["valid_from"]


async def test_a_change_that_added_nothing_is_not_an_addition(api):
    await _put(api, ids=["finance.revenue", "sales.orders"])
    await _put(api, ids=["finance.revenue"])
    [only] = await _history(api)
    assert only["added"] == ["finance.revenue", "sales.orders"]
    assert only["until"] is None


async def test_a_withdrawn_rule_leaves_its_addition_open_ended(api):
    view = await _post_add(api, ids=["sales.orders"], until=_TOMORROW)
    [rule] = view["expiries"]
    await api.client.request(
        "DELETE", f"/admin/entitlements/expiries/{rule['rule_key']}", json={"reason": "kept"}
    )
    [addition] = await _history(api)
    assert addition["until"] is None


async def test_reading_someone_elses_history_needs_a_right(api):
    api.caller["identity"] = _identity("bob", "viewer")
    response = await api.client.get(
        "/admin/entitlements/lists/history", params={"principal": "alice", "role_id": "viewer"}
    )
    assert response.status_code == 403
    assert response.json()["code"] == "auth.missing_capability"


# -- expiry rules end every capability they name (amended 2026-11-11) ---------------------------

_ASSIGN = Capability.ASSIGN_ENTITLEMENTS.value
_NONE = HeldEntitlements()


@pytest.mark.parametrize("method", ["POST", "PUT"])
async def test_a_rule_whose_date_has_passed_is_refused_as_a_lists_is(api, method):
    if method == "POST":
        response = await api.client.post(
            "/admin/entitlements/expiries",
            json=_rule({"read": ["finance"]}, ends_at=_AN_HOUR_AGO),
        )
    else:
        [rule] = (await _add_rule(api, {"read": ["finance"]}))["expiries"]
        response = await api.client.put(
            f"/admin/entitlements/expiries/{rule['rule_key']}",
            json={"ends_at": _AN_HOUR_AGO, "ends": {"read": ["finance"]}, "reason": "x"},
        )
    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "entitlements.expiry_in_past"
    assert body["detail"] == "An expiry must be in the future"
    assert [r.ends_at for r in api.state.held_entitlements.get("alice", _NONE).expiries] == (
        [] if method == "POST" else [datetime.fromisoformat(_TOMORROW)]
    )


async def test_a_rule_on_a_list_scoped_capability_removes_what_it_names(api):
    rule = _rule({_ASSIGN: ["viewer"]}, role="granter")
    preview = await api.client.post("/admin/entitlements/expiries/preview", json=rule)
    assert preview.json() == {"removes": {_ASSIGN: ["viewer"]}, "removes_nothing": False}
    view = await _add_rule(api, {_ASSIGN: ["viewer"]}, role="granter")
    assert [r["ends"] for r in view["expiries"]] == [{_ASSIGN: ["viewer"]}]


async def test_a_grant_wide_rule_removes_the_list_scoped_capabilities_too(api):
    removes = (await _preview(api, {}, role="granter"))["removes"]
    assert removes[_ASSIGN] == ["*"]


async def test_a_rule_on_a_list_scoped_capability_outside_its_list_removes_nothing(api):
    await _put(api, principal="alice", role="granter", kind="roles", ids=["analyst"])
    preview = await _preview(api, {_ASSIGN: ["nobody"]}, role="granter")
    assert preview == {"removes": {}, "removes_nothing": True}


async def _write_as(api_, user, role_id, claims=None):
    api_.caller["identity"] = _identity(user, "granter", claims)
    return await api_.client.put(
        "/admin/entitlements/lists", json=_set(principal="bob", role=role_id)
    )


async def test_a_passed_rule_ends_the_list_scoped_reach_it_names(api):
    await _passed_rule(api, {_ASSIGN: ["viewer"]}, role="granter")
    refused = await _write_as(api, "alice", "viewer")
    assert refused.status_code == 403
    assert refused.json()["code"] == "entitlements.role_out_of_reach"
    # analyst inherits from viewer: a list naming a role covers the roles beneath it.
    assert (await _write_as(api, "alice", "analyst")).status_code == 403
    assert (await _write_as(api, "alice", "nobody")).status_code == 200
    assert (await _write_as(api, "carol", "viewer")).status_code == 200


async def test_a_rule_not_yet_passed_ends_nothing(api):
    await _add_rule(api, {_ASSIGN: ["viewer"]}, role="granter")
    assert (await _write_as(api, "alice", "viewer")).status_code == 200


async def test_the_identity_providers_roles_list_is_not_ended_by_a_rule(api):
    # Enforcement reads the token's lists: the flat form of a roles list for granter.
    claims = {"groups": ["provisa:granter:roles:viewer"]}
    await _passed_rule(api, {_ASSIGN: ["viewer"]}, role="granter")
    assert (await _write_as(api, "alice", "viewer", claims)).status_code == 200
    assert (await _write_as(api, "alice", "viewer")).status_code == 403


async def test_effective_access_shows_what_a_passed_rule_ended(api):
    await _passed_rule(api, {_ASSIGN: ["viewer"]}, role="granter")
    assign = (await _capabilities(api, role_ids="granter"))[_ASSIGN]
    assert assign["identifiers"] == ["*"]
    assert assign["origins"] == [_origin("granter", {}, ended=["viewer"])]


async def test_effective_access_drops_an_identifier_a_rule_ended_whole(api):
    await _put(api, principal="alice", role="granter", kind="roles", ids=["viewer", "nobody"])
    await _passed_rule(api, {_ASSIGN: ["viewer"]}, role="granter")
    assign = (await _capabilities(api, role_ids="granter"))[_ASSIGN]
    assert assign["identifiers"] == ["nobody"]


async def test_a_lists_expiry_on_a_list_scoped_kind_is_kept(api):
    view = await _put(
        api, principal="alice", role="granter", kind="roles", ids=["viewer"], expires_at=_TOMORROW
    )
    [rule] = view["expiries"]
    assert (rule["ends"], rule["list_kind"]) == ({"*": ["viewer"]}, "roles")


async def test_a_passed_lists_expiry_ends_the_capabilities_its_kind_scopes(api):
    await _passed_rule(api, {"*": ["viewer"]}, role="granter", list_kind="roles")
    assert (await _write_as(api, "alice", "viewer")).status_code == 403
    assert (await _write_as(api, "alice", "nobody")).status_code == 200


async def test_a_list_scoped_kinds_expiry_ends_nothing_in_the_mask(api):
    await _passed_rule(api, {"*": ["finance"]}, list_kind="roles")
    mask = _as(api, "alice")
    assert mask is not None and mask.full


async def test_add_access_until_a_date_on_a_list_scoped_kind_is_kept(api):
    view = await _post_add(api, role_id="granter", kind="roles", ids=["viewer"], until=_TOMORROW)
    [rule] = view["expiries"]
    assert (rule["ends"], rule["list_kind"]) == ({"*": ["viewer"]}, "roles")


async def test_effective_access_includes_inherited_capabilities_with_their_role(api):
    settings = Capability.ORG_SETTINGS.value
    people = Capability.USER_MANAGEMENT.value
    # As the runtime build holds them: a role's capabilities include what it inherits (REQ-2155).
    api.state.roles["parent"] = _role([settings])
    api.state.roles["child"] = _role([settings, people], parents=["parent"])
    api.state.role_chains["parent"] = ["parent"]
    api.state.role_chains["child"] = ["child", "parent"]
    caps = await _capabilities(api, role_ids="child")
    assert caps[settings]["origins"] == [_origin("child", {}, from_role="parent")]
    assert caps[people]["origins"] == [_origin("child", {}, from_role="child")]


# -- expiry is on the grant and on its entries (amended 2026-11-11) ---------------------------


async def _grant_ends(api_, principal="alice", role="viewer"):
    async with api_.db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(user_role_assignments.c.expires_at).where(
                    user_role_assignments.c.user_id == principal,
                    user_role_assignments.c.role_id == role,
                )
            )
        ).fetchall()
    return [r.expires_at for r in rows]


async def test_a_grant_wide_rule_is_the_grants_own_expiry(api):
    await _hold(api, "alice")
    view = await _add_rule(api, {})
    assert view["expiries"] == [] and view["grant_ended"] is False
    assert api.state.held_entitlements == {}  # no second mechanism: no rule is stored
    assert await _grant_ends(api) == [datetime.fromisoformat(_TOMORROW)]
    [row] = await _audit(api, "role.expiry_set")
    assert (row.subject_id, row.detail["role_id"]) == ("alice", "viewer")


async def test_a_grant_wide_rule_shortens_the_grant_and_never_extends_it(api):
    await _hold(api, "alice", expires_at=datetime.fromisoformat(_NEXT_WEEK))
    await _add_rule(api, {})
    assert await _grant_ends(api) == [datetime.fromisoformat(_TOMORROW)]
    await _add_rule(api, {}, ends_at=_NEXT_WEEK)
    assert await _grant_ends(api) == [datetime.fromisoformat(_TOMORROW)]


async def test_a_grant_wide_rule_ends_the_whole_role_for_that_person(api):
    await _hold(api, "alice", role="granter")
    await _add_rule(api, {}, role="granter")
    async with api.db.acquire() as conn:  # its date comes
        await role_grants.end_grant(conn, "alice", "granter", datetime.fromisoformat(_AN_HOUR_AGO))
        assert await role_grants.held_grants(conn, "alice") == []
    view = (
        await api.client.get("/admin/entitlements/effective?principal=alice&role_id=granter")
    ).json()
    assert view["grant_ended"] is True


async def test_a_grant_wide_rule_on_a_role_provisa_does_not_grant_is_refused_by_name(api):
    response = await api.client.post("/admin/entitlements/expiries", json=_rule({}))
    assert response.status_code == 409
    assert response.json()["code"] == "entitlements.no_grant_to_end"


async def test_an_entry_changed_to_end_the_whole_grant_becomes_the_grants_expiry(api):
    await _hold(api, "alice")
    [rule] = (await _add_rule(api, {"read": ["finance"]}))["expiries"]
    response = await api.client.put(
        f"/admin/entitlements/expiries/{rule['rule_key']}",
        json={"ends_at": _TOMORROW, "ends": {}, "reason": "leaving"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["expiries"] == []
    assert await _grant_ends(api) == [datetime.fromisoformat(_TOMORROW)]


async def test_an_entry_ends_alone_and_the_grant_carries_on(api):
    await _hold(api, "alice")
    await _passed_rule(api, {"read": ["finance"]})
    assert await _grant_ends(api) == [None]
    mask = _as(api, "alice")
    assert mask is not None and not mask.has_table(1) and mask.has_table(2)


async def _refused_past_grant(response):
    assert response.status_code == 422, response.text
    body = response.json()
    assert body["code"] == "entitlements.entry_outlives_grant"
    assert datetime.fromisoformat(body["params"]["grant_expires_at"]) == datetime.fromisoformat(
        _TOMORROW
    )


async def test_an_entry_dated_past_its_grant_is_refused_by_name(api):
    await _hold(api, "alice", expires_at=datetime.fromisoformat(_TOMORROW))
    client = api.client
    await _refused_past_grant(
        await client.post(
            "/admin/entitlements/expiries", json=_rule({"read": ["finance"]}, ends_at=_NEXT_WEEK)
        )
    )
    await _refused_past_grant(
        await client.put("/admin/entitlements/lists", json=_set(expires_at=_NEXT_WEEK))
    )
    await _refused_past_grant(
        await client.post("/admin/entitlements/lists/add", json=_add(until=_NEXT_WEEK))
    )
    assert api.state.held_entitlements == {}


async def test_an_entry_changed_past_its_grant_is_refused_by_name(api):
    await _hold(api, "alice", expires_at=datetime.fromisoformat(_TOMORROW))
    soon = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    [rule] = (await _add_rule(api, {"read": ["finance"]}, ends_at=soon))["expiries"]
    await _refused_past_grant(
        await api.client.put(
            f"/admin/entitlements/expiries/{rule['rule_key']}",
            json={"ends_at": _NEXT_WEEK, "ends": {"read": ["finance"]}, "reason": "x"},
        )
    )


async def test_an_entry_ends_with_its_grant_or_before_it(api):
    await _hold(api, "alice", expires_at=datetime.fromisoformat(_NEXT_WEEK))
    view = await _add_rule(api, {"read": ["finance"]}, ends_at=_NEXT_WEEK)
    assert [r["ends"] for r in view["expiries"]] == [{"read": ["finance"]}]


async def test_an_entry_of_a_role_the_grant_holds_in_two_domains_ends_by_the_later(api):
    await _hold(api, "alice", expires_at=datetime.fromisoformat(_TOMORROW), domain="sales")
    await _hold(api, "alice", expires_at=datetime.fromisoformat(_NEXT_WEEK), domain="finance")
    view = await _add_rule(api, {"read": ["finance"]}, ends_at=_NEXT_WEEK)
    assert len(view["expiries"]) == 1


# -- an entry granted again keeps the latest of its expiries ------------------------------------


def _ends_of(view):
    return sorted(
        (r["ends"], datetime.fromisoformat(r["ends_at"]) if r["ends_at"] else None)
        for r in view["expiries"]
    )


async def test_an_entry_granted_again_later_keeps_the_later_date(api):
    await _put(api)
    await _post_add(api, until=_TOMORROW)
    view = await _post_add(api, until=_NEXT_WEEK)
    assert _ends_of(view) == [({"*": ["sales.orders"]}, datetime.fromisoformat(_NEXT_WEEK))]


async def test_an_entry_granted_again_sooner_keeps_the_later_date(api):
    await _put(api)
    await _post_add(api, until=_NEXT_WEEK)
    view = await _post_add(api, until=_TOMORROW)
    assert _ends_of(view) == [({"*": ["sales.orders"]}, datetime.fromisoformat(_NEXT_WEEK))]


async def test_an_entry_granted_again_with_no_end_has_none(api):
    await _put(api)
    await _post_add(api, until=_TOMORROW)
    view = await _post_add(api)
    assert view["expiries"] == []
    [tables] = [row for row in view["lists"] if row["kind"] == "tables"]
    assert tables["identifiers"] == ["finance.revenue", "sales.orders"]


async def test_an_entry_with_no_end_granted_again_with_a_date_keeps_no_end(api):
    await _put(api)
    await _post_add(api)
    view = await _post_add(api, until=_TOMORROW)
    assert view["expiries"] == []


async def test_granting_again_extends_only_the_entry_granted_again(api):
    await _put(api)
    await _post_add(api, ids=["sales.orders", "sales.customers"], until=_TOMORROW)
    view = await _post_add(api, ids=["sales.orders"], until=_NEXT_WEEK)
    assert _ends_of(view) == [
        ({"*": ["sales.customers"]}, datetime.fromisoformat(_TOMORROW)),
        ({"*": ["sales.orders"]}, datetime.fromisoformat(_NEXT_WEEK)),
    ]


async def test_granting_again_for_one_capability_extends_it_for_that_one(api):
    await _put(api)
    await _post_add(api, until=_TOMORROW, capabilities=["read", "traverse"])
    view = await _post_add(api, until=_NEXT_WEEK, capabilities=["read"])
    assert _ends_of(view) == [
        ({"read": ["sales.orders"]}, datetime.fromisoformat(_NEXT_WEEK)),
        ({"traverse": ["sales.orders"]}, datetime.fromisoformat(_TOMORROW)),
    ]


async def test_withdrawing_the_extension_does_not_bring_the_earlier_date_back(api):
    await _put(api)
    await _post_add(api, until=_TOMORROW)
    [rule] = (await _post_add(api, until=_NEXT_WEEK))["expiries"]
    response = await api.client.request(
        "DELETE", f"/admin/entitlements/expiries/{rule['rule_key']}", json={"reason": "x"}
    )
    assert response.json()["expiries"] == []


# -- the timeline -------------------------------------------------------------------------------


async def _timeline(api_, principal="alice"):
    response = await api_.client.get(
        "/admin/entitlements/timeline", params={"principal": principal}
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_the_timeline_is_each_grant_with_its_dated_entries_under_it(api):
    await _hold(api, "alice", expires_at=datetime.fromisoformat(_NEXT_WEEK))
    await _hold(api, "alice", role="granter")
    await _add_rule(api, {"read": ["finance"]}, ends_at=_NEXT_WEEK)
    await _add_rule(api, {"*": ["sales"]})
    body = await _timeline(api)
    assert body["principal"] == "alice"
    granter, viewer = body["grants"]
    assert (granter["role_id"], granter["expires_at"], granter["entries"]) == ("granter", None, [])
    assert viewer["role_id"] == "viewer" and viewer["domain_id"] == "*"
    assert datetime.fromisoformat(viewer["expires_at"]) == datetime.fromisoformat(_NEXT_WEEK)
    assert [
        (e["ends"], e["ends_at"] and datetime.fromisoformat(e["ends_at"]))
        for e in viewer["entries"]
    ] == [
        ({"*": ["sales"]}, datetime.fromisoformat(_TOMORROW)),
        ({"read": ["finance"]}, datetime.fromisoformat(_NEXT_WEEK)),
    ]


async def test_the_timeline_leaves_out_a_grant_that_ended(api):
    await _hold(api, "alice", expires_at=datetime.fromisoformat(_AN_HOUR_AGO))
    assert (await _timeline(api))["grants"] == []


async def test_reading_someone_elses_timeline_needs_a_right(api):
    await _hold(api, "alice")
    api.caller["identity"] = _identity("bob", "viewer")
    response = await api.client.get("/admin/entitlements/timeline", params={"principal": "alice"})
    assert response.status_code == 403
    assert response.json()["code"] == "auth.missing_capability"
