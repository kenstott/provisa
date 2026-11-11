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

from provisa.core.schema_org import admin_audit_log
from provisa.security.rights import Capability
from tests.unit.test_entitlements_api import _as, _idp, _identity, _role, _set
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
    assert read["origins"] == [{"role_id": "viewer", "lists": {"tables": "provisa"}}]
    assert read["model_version"] == ["boot", 1]


async def test_several_roles_combine_by_union_per_capability(api):
    await _put(api)  # viewer: finance.revenue only; analyst: no list, so its whole schema
    caps = await _capabilities(api, role_ids="viewer,analyst")
    assert caps["read"]["identifiers"] == sorted([*_REVENUE, *_SALES])
    assert caps["read"]["origins"] == [
        {"role_id": "viewer", "lists": {"tables": "provisa"}},
        {"role_id": "analyst", "lists": {}},
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
        "origins": [{"role_id": "settings", "lists": {}}],
        "model_version": ["boot", 1],
    }


async def test_a_list_scoped_capability_reads_its_kinds_list(api):
    assign = Capability.ASSIGN_ENTITLEMENTS.value
    before = (await _capabilities(api, role_ids="granter"))[assign]
    assert (before["kind"], before["identifiers"]) == ("roles", ["*"])
    await _put(api, role="granter", kind="roles", ids=["viewer"])
    after = (await _capabilities(api, role_ids="granter"))[assign]
    assert after["identifiers"] == ["viewer"]
    assert after["origins"] == [{"role_id": "granter", "lists": {"roles": "provisa"}}]


async def test_the_identity_providers_list_is_named_as_the_origin(api):
    await _idp(api, "alice", {"viewer": {"tables": ("sales.orders",)}})
    read = (await _capabilities(api))["read"]
    assert read["identifiers"] == ["sales.orders", "sales.orders.customer_id", "sales.orders.id"]
    assert read["origins"] == [{"role_id": "viewer", "lists": {"tables": "idp"}}]


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
    await _add_rule(api, {"read": ["finance"]}, ends_at=_AN_HOUR_AGO)
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
    [rule] = (await _add_rule(api, {"read": ["finance"]}, ends_at=_AN_HOUR_AGO))["expiries"]
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
