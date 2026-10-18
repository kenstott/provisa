# Copyright (c) 2026 Kenneth Stott
# Canary: 96dd49e1-8cbe-4a56-921a-6903fde3e1c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pseudo-roles: roles held because of what a user's attributes say about a declared claim type
(REQ-2057). Rightless, never granted by hand, named by a request like any role, and entering a
meta-role through its grants only."""

# Requirements: REQ-2057

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.auth.pseudo_roles import (
    declared_claim_roles,
    held_pseudo_roles,
    principal_bindings,
    principal_session_vars,
)
from provisa.security import meta_role
from provisa.security.inheritance import expand_grants


class TestHolding:
    def test_a_declared_claim_gives_its_parent_and_the_sub_role_of_its_value(self):
        held = held_pseudo_roles(["region"], {"region": "eu", "department": "marketing"})
        assert held == {"region": "", "region__eu": "region"}

    def test_no_value_holds_neither_the_parent_nor_a_sub_role(self):
        assert held_pseudo_roles(["region"], {"department": "marketing"}) == {}
        assert held_pseudo_roles(["region"], {"region": ""}) == {}

    def test_the_orgs_record_decides_the_value(self):
        attributes, held = principal_bindings({"region": "us"}, {"region": "eu"}, ["Region"])
        assert attributes == {"region": "eu"}
        assert held == {"region": "", "region__eu": "region"}

    def test_the_orgs_own_list_replaces_the_deployments(self):
        config = SimpleNamespace(claim_roles=["region"])
        assert declared_claim_roles(SimpleNamespace(settings_overrides={}, config=config)) == [
            "region"
        ]
        org = SimpleNamespace(settings_overrides={"claim_roles": []}, config=config)
        assert declared_claim_roles(org) == []

    def test_session_variables_are_the_attributes_then_the_identity(self):
        assert principal_session_vars("u1", {"region": "eu", "role": "x"}, "analyst") == {
            "region": "eu",
            "user_id": "u1",
            "role": "analyst",
        }


class TestNaming:
    def test_a_named_pseudo_role_qualifies_the_set_with_its_parent(self, monkeypatch):
        built: list = []
        monkeypatch.setattr(
            meta_role,
            "ensure_meta_role",
            lambda _s, members, quals=(): built.append((members, quals)) or "meta:x",
        )
        held = {"region": "", "region__eu": "region"}
        assert (
            meta_role.resolve_requested_role(None, {"analyst"}, "analyst,region__eu", lambda: held)
            == "meta:x"
        )
        assert built == [(["analyst"], ("region", "region__eu"))]

    def test_the_pseudo_roles_are_read_only_when_a_named_role_is_not_assigned(self, monkeypatch):
        monkeypatch.setattr(meta_role, "ensure_meta_role", lambda _s, members: "meta:x")
        unread = lambda: pytest.fail("no named role is unassigned")  # noqa: E731
        assert meta_role.resolve_requested_role(None, {"analyst"}, "analyst", unread) == "analyst"

    def test_a_pseudo_role_not_held_is_refused_by_name(self):
        with pytest.raises(PermissionError, match="region__us"):
            meta_role.resolve_requested_role(
                None, {"analyst"}, "analyst,region__us", lambda: {"region__eu": "region"}
            )

    def test_named_with_no_data_role_it_reaches_no_data(self):
        state = SimpleNamespace(surfaces={"analyst": object()})
        with pytest.raises(PermissionError, match="No schema available"):
            meta_role.ensure_meta_role(state, [], ("region__eu",))


class TestTheMetaRole:
    def test_a_grant_to_a_qualifier_reaches_the_meta_role(self):
        meta = "meta:analyst+region+region__eu"
        col = {"column_name": "email", "visible_to": ["analyst", "region__eu"]}
        expand_grants([col], {meta: [meta, "analyst", "region", "region__eu"]})
        assert meta in col["visible_to"]

    def test_a_mask_entry_naming_a_qualifier_applies_to_the_meta_role(self):
        # REQ-2059: the meta-role reads each list with its chain, the qualifiers in it.
        from provisa.security.masking import reader_masks

        meta = "meta:analyst+region__eu"
        everyone = {"to": ["*"], "mask": {"type": "constant", "value": None}}
        masked = [
            (1, "email", [{"to": ["region__eu"], "mask": None}, everyone], "varchar"),
            (1, "phone", [{"to": ["org_admin"], "mask": None}, everyone], "varchar"),
        ]
        out = reader_masks(
            masked,
            meta,
            meta_role.meta_chain(meta, ["analyst"], {"analyst": ["analyst"]}, ("region__eu",)),
        )
        assert set(out[(1, meta)]) == {"phone"}

    def test_a_qualifier_does_not_unmask_past_an_earlier_entry(self):
        # A member's own entry before the qualifier's applies first (deny by order).
        from provisa.security.masking import reader_masks

        meta = "meta:analyst+region__eu"
        masks = [
            {"to": ["analyst"], "mask": {"type": "constant", "value": None}},
            {"to": ["region__eu"], "mask": None},
        ]
        chain = meta_role.meta_chain(meta, ["analyst"], {"analyst": ["analyst"]}, ("region__eu",))
        assert set(reader_masks([(1, "email", masks, "varchar")], meta, chain)[(1, meta)]) == {
            "email"
        }


class _RegionProvider(AuthProvider):
    provider_name = "test"  # a provider of this deployment's own, not IdP-signed

    async def validate_token(self, token: str) -> AuthIdentity:
        if token != "t":
            raise ValueError("Invalid token")
        return AuthIdentity(
            user_id="amelie",
            email=None,
            display_name=None,
            roles=["analyst"],
            raw_claims={"region": "us"},
        )


@pytest.fixture
def client(monkeypatch, serve_deployment_org):
    # The middleware's deployment org is 'root'; the app state serves it (REQ-1266).
    serve_deployment_org("root")
    monkeypatch.setattr("provisa.auth.middleware._loaded_claim_roles", lambda: ["region"])
    monkeypatch.setattr(
        "provisa.auth.middleware._loaded_user_attributes", lambda _uid: {"region": "eu"}
    )
    monkeypatch.setattr(
        "provisa.security.meta_role.ensure_meta_role",
        lambda _s, members, quals=(): "meta:" + "+".join(sorted([*members, *quals])),
    )
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware, provider=_RegionProvider(), default_role="analyst", default_org_id="root"
    )

    @app.get("/test")
    async def _t(request: Request):
        identity = request.state.identity
        return {
            "role": request.state.role,
            "pseudo": identity.pseudo_roles,
            "acting": identity.acting_roles,
        }

    return TestClient(app)


class TestTheRequest:
    def test_the_user_holds_what_the_orgs_record_says_and_acts_as_nothing_of_it_unnamed(
        self, client
    ):
        data = client.get("/test", headers={"Authorization": "Bearer t"}).json()
        assert data["pseudo"] == {"region": "", "region__eu": "region"}
        assert data["role"] == "analyst"

    def test_named_it_qualifies_the_acting_role_and_adds_no_right(self, client):
        resp = client.get(
            "/test", headers={"Authorization": "Bearer t", "X-Provisa-Role": "analyst,region__eu"}
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["role"] == "meta:analyst+region+region__eu"
        assert resp.json()["acting"] == ["analyst"]

    def test_the_providers_value_is_not_held_where_the_org_says_otherwise(self, client):
        resp = client.get(
            "/test", headers={"Authorization": "Bearer t", "X-Provisa-Role": "analyst,region__us"}
        )
        assert resp.status_code == 403
        assert "region__us" in resp.json()["detail"]


@pytest.fixture
def unsecured(monkeypatch, serve_deployment_org):
    serve_deployment_org("root")
    monkeypatch.setattr("provisa.auth.middleware._loaded_claim_roles", lambda: ["region"])
    monkeypatch.setattr(
        "provisa.security.meta_role.ensure_meta_role",
        lambda _s, members, quals=(): "meta:" + "+".join(sorted([*members, *quals])),
    )
    app = FastAPI()
    app.add_middleware(AuthMiddleware, provider=None, default_org_id="root")

    @app.get("/test")
    async def _t(request: Request):
        identity = request.state.identity
        return {
            "role": request.state.role,
            "pseudo": identity.pseudo_roles,
            "acting": identity.acting_roles,
            "attributes": identity.org_attributes,
        }

    return TestClient(app)


class TestUnsecured:
    """Unsecured, a named declared pseudo-role is held as a named role is, with its parent and the
    attribute it implies; a signed-in request still holds one only by its claim (above)."""

    def test_named_it_is_held_with_its_parent_and_binds_its_attribute(self, unsecured):
        data = unsecured.get("/test", headers={"X-Provisa-Role": "reviewer,region__eu"}).json()
        assert data["role"] == "meta:region+region__eu+reviewer"
        assert data["acting"] == ["reviewer"]
        assert data["pseudo"] == {"region__eu": "region", "region": ""}
        assert data["attributes"] == {"region": "eu"}

    def test_a_name_that_is_no_declared_claim_type_is_a_role(self, unsecured):
        data = unsecured.get("/test", headers={"X-Provisa-Role": "dept__ops"}).json()
        assert data["role"] == "dept__ops" and data["pseudo"] == {}

    def test_named_alone_it_reaches_no_data(self, unsecured, monkeypatch):
        def refuse(_s, members, quals=()):
            raise PermissionError("No schema available for the roles named")

        monkeypatch.setattr("provisa.security.meta_role.ensure_meta_role", refuse)
        assert unsecured.get("/test", headers={"X-Provisa-Role": "region__eu"}).status_code == 403


def test_the_unsecured_split_names_each_part():
    from provisa.auth.pseudo_roles import named_unsecured

    assert named_unsecured(["reviewer", "region__eu", "region"], ["region"]) == (
        ["reviewer"],
        {"region__eu": "region", "region": ""},
        {"region": "eu"},
    )


class TestTheNodesOwnWork:
    """A service call a regional node makes on its own behalf carries the node's region
    pseudo-role; a user's request never takes its region from the node (REQ-2057)."""

    def test_on_an_eu_node_a_service_call_acts_with_region__eu(self, monkeypatch):
        from provisa.auth.pseudo_roles import service_acting_role
        from provisa.core import process_region

        built: list = []
        monkeypatch.setattr(process_region, "region", lambda: "eu")
        monkeypatch.setattr(
            meta_role,
            "ensure_meta_role",
            lambda _s, members, quals=(): built.append((members, quals)) or "meta:ops+region",
        )
        assert service_acting_role(None, "ops") == "meta:ops+region"
        assert built == [(["ops"], ("region", "region__eu"))]

    def test_on_a_node_with_no_region_it_acts_as_its_role_alone(self, monkeypatch):
        from provisa.auth.pseudo_roles import service_acting_role
        from provisa.core import process_region
        from provisa.core.regions import DEFAULT_REGION

        monkeypatch.setattr(process_region, "region", lambda: DEFAULT_REGION)
        monkeypatch.setattr(
            meta_role, "ensure_meta_role", lambda *_a: pytest.fail("no region to carry")
        )
        assert service_acting_role(None, "ops") == "ops"

    async def test_a_trigger_command_runs_as_the_role_with_the_nodes_region(self, monkeypatch):
        from provisa.scheduler import trigger_runs

        ran: list[str] = []

        async def _invoke(_name, _args, _state, role):
            ran.append(role)

        monkeypatch.setattr("provisa.api.data.action_exec.invoke_command", _invoke)
        monkeypatch.setattr(
            "provisa.auth.pseudo_roles.service_acting_role", lambda _s, role: f"{role}+region__eu"
        )
        state = SimpleNamespace(tracked_functions={"tidy": {"arguments": []}})
        await trigger_runs._run_command({"command": "tidy", "role": "ops"}, {}, state)
        assert ran == ["ops+region__eu"]

    def test_a_users_request_takes_no_region_from_the_node(self, monkeypatch, client):
        from provisa.core import process_region

        monkeypatch.setattr(process_region, "region", lambda: "us")
        data = client.get("/test", headers={"Authorization": "Bearer t"}).json()
        # The org's record says eu; the node being us adds nothing.
        assert data["pseudo"] == {"region": "", "region__eu": "region"}
