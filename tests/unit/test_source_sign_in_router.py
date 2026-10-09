# Copyright (c) 2026 Kenneth Stott
# Canary: a9572009-6901-42c4-a149-02817776e895
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The calls the Sources form makes to sign a source in (REQ-1923): who may make them, what
they hand the exchange, and that nothing they answer is a credential. The exchange itself is
held to its rules in ``test_source_sign_in.py``."""

# Requirements: REQ-1923
from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import source_sign_in_router as router
from provisa.api.errors import ApiError
from provisa.core import request_context
from provisa.core.source_sign_in import Completed, SignInRefused, Started

pytestmark = pytest.mark.asyncio

ORG = "acme"
CLIENT_SECRET = "made-up-client-secret"
CODE = "made-up-authorization-code"


def _request(user_id: str | None = "uid-ada"):
    identity = None if user_id is None else SimpleNamespace(user_id=user_id)
    # No headers and no url: a call that read the address it was reached on would fail here.
    return SimpleNamespace(state=SimpleNamespace(identity=identity))


@pytest.fixture
def wired(monkeypatch):
    seen: dict = {"asked": [], "stored": [], "swept": [], "started": [], "completed": []}
    token = request_context.set_current_org(ORG)

    def gate(request, capability):
        seen["asked"].append(capability)
        if seen.get("refuse"):
            raise ApiError(403, "auth.missing_capability", f"Missing capability: {capability!r}")

    async def store_source_secret(actor, name, value, description):
        seen["stored"].append((actor, name, value))
        return f"${{secret:{name}}}"

    async def sweep(admin_db, **given):
        seen["swept"].append(given)

    async def start(admin_db, **given):
        seen["started"].append(given)
        await given["store_secret"](given["secret_names"][0], given["client_secret"], "d")
        return Started("https://issuer.test/authorize?state=made-up-state", 600)

    async def complete(admin_db, **given):
        seen["completed"].append(given)
        if seen.get("refusal"):
            raise seen["refusal"]
        return Completed("mail", "ada@example.test", "${secret:a}", "${secret:b}")

    from provisa.api.admin import schema_common

    monkeypatch.setattr(router, "require_capability_request", gate)
    monkeypatch.setattr(router, "_admin_db", lambda: "admin-plane")
    monkeypatch.setattr(router, "_public_address", lambda: "https://acme.provisa.test")
    monkeypatch.setattr(schema_common, "_store_source_secret", store_source_secret)
    monkeypatch.setattr(router.source_sign_in, "sweep", sweep)
    monkeypatch.setattr(router.source_sign_in, "start", start)
    monkeypatch.setattr(router.source_sign_in, "complete", complete)

    from provisa.api_source import oauth_store
    from provisa.core import config_loader, config_location

    async def store_refresh_token(admin_db, platform_url, org_id, **given):
        seen.setdefault("locked", []).append((admin_db, platform_url, org_id, given))

    monkeypatch.setattr(oauth_store, "store_refresh_token", store_refresh_token)
    monkeypatch.setattr(config_location, "config_path_str", lambda: "provisa.yaml")
    monkeypatch.setattr(
        config_loader,
        "load_control_plane",
        lambda path: SimpleNamespace(resolved_platform_url=lambda: f"platform-of:{path}"),
    )
    yield seen
    request_context.reset_current_org(token)


def _start_body(**changed) -> router.StartRequest:
    given = {
        "source_id": "mail",
        "kind": "google_workspace",
        "account": "ada@example.test",
        "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
        "client_id": "client-1",
        "client_secret": CLIENT_SECRET,
    }
    given.update(changed)
    return router.StartRequest(**given)


class TestWhoMayAsk:
    async def test_each_call_needs_the_right_that_adding_a_source_needs(self, wired):
        await router.redirect_address(_request())
        await router.start(_request(), _start_body())
        await router.complete(_request(), router.CompleteRequest(state="s", code=CODE))
        assert wired["asked"] == ["source_registration"] * 3

    async def test_without_it_nothing_is_recorded_stored_or_exchanged(self, wired):
        wired["refuse"] = True
        for call in (
            lambda: router.redirect_address(_request()),
            lambda: router.start(_request(), _start_body()),
            lambda: router.complete(_request(), router.CompleteRequest(state="s", code=CODE)),
        ):
            with pytest.raises(ApiError) as raised:
                await call()
            assert raised.value.status_code == 403
        assert wired["stored"] == wired["swept"] == wired["started"] == wired["completed"] == []


class TestRedirectAddress:
    async def test_it_comes_from_the_configured_public_address(self, wired):
        answer = await router.redirect_address(_request())
        assert answer == {"redirect_address": "https://cloud.provisa.test/source-sign-in.html"}

    async def test_an_unset_public_address_is_refused_by_name(self, wired, monkeypatch):
        monkeypatch.setattr(router, "_public_address", lambda: None)
        with pytest.raises(ApiError) as raised:
            await router.redirect_address(_request())
        assert raised.value.code == "source_sign_in.public_address_not_set"
        assert raised.value.status_code == 400


class TestStart:
    async def test_the_sign_in_is_bound_to_the_caller_their_organisation_and_environment(
        self, wired
    ):
        await router.start(_request(), _start_body())
        (given,) = wired["started"]
        assert (given["org_id"], given["env"], given["user_id"]) == (ORG, "prod", "uid-ada")
        assert given["public_address"] == "https://acme.provisa.test"
        assert given["kind_id"] == "google_workspace"

    async def test_the_vault_names_are_the_sources_own(self, wired):
        from provisa.api.admin.schema_common import source_mapping_secret_name

        await router.start(_request(), _start_body())
        assert wired["started"][0]["secret_names"] == (
            source_mapping_secret_name("mail", "client_secret", "prod"),
            source_mapping_secret_name("mail", "refresh_token", "prod"),
        )

    async def test_the_client_secret_is_stored_as_a_typed_credential_is(self, wired):
        await router.start(_request(), _start_body())
        ((actor, name, value),) = wired["stored"]
        assert (actor, value) == ("uid-ada", CLIENT_SECRET)
        assert name.endswith("__client_secret")

    async def test_the_organisations_abandoned_sign_ins_are_swept_first(self, wired):
        await router.start(_request(), _start_body())
        (swept,) = wired["swept"]
        assert (swept["org_id"], swept["env"]) == (ORG, "prod")

    async def test_the_answer_is_where_to_send_the_browser_and_nothing_else(self, wired):
        answer = await router.start(_request(), _start_body())
        assert answer == {
            "authorization_url": "https://issuer.test/authorize?state=made-up-state",
            "expires_in": 600,
        }
        assert CLIENT_SECRET not in repr(answer)

    async def test_a_deployment_without_sign_in_binds_to_its_one_caller(self, wired):
        await router.start(_request(user_id=None), _start_body())
        assert wired["started"][0]["user_id"] == "anonymous"
        assert wired["stored"][0][0] is None


class TestSettings:
    async def test_what_the_issuers_addresses_depend_on_is_passed_on(self, wired):
        await router.start(_request(), _start_body(settings={"tenant": "contoso"}))
        assert wired["started"][0]["settings"] == {"tenant": "contoso"}

    async def test_a_source_with_none_passes_none(self, wired):
        await router.start(_request(), _start_body())
        assert wired["started"][0]["settings"] == {}


class TestComplete:
    async def test_the_refresh_token_is_written_under_the_lock_its_refreshes_take(self, wired):
        await router.complete(_request(), router.CompleteRequest(state="s", code=CODE))
        writer = wired["completed"][0]["store_refresh_token"]
        reference = await writer("mail", "source_mail__refresh_token", "made-up-refresh-token")
        assert reference == "${secret:source_mail__refresh_token}"
        ((admin_db, platform_url, org_id, given),) = wired["locked"]
        assert (admin_db, platform_url, org_id) == ("admin-plane", "platform-of:provisa.yaml", ORG)
        assert given == {
            "source_id": "mail",
            "secret_name": "source_mail__refresh_token",
            "refresh_token": "made-up-refresh-token",
            "actor": "uid-ada",
        }
        assert "made-up" not in reference

    async def test_the_caller_and_their_organisation_are_what_the_state_is_checked_against(
        self, wired
    ):
        await router.complete(_request(), router.CompleteRequest(state="made-up-state", code=CODE))
        (given,) = wired["completed"]
        assert (given["org_id"], given["user_id"]) == (ORG, "uid-ada")
        assert (given["state"], given["code"], given["error"]) == ("made-up-state", CODE, None)

    async def test_the_answer_is_references_never_a_credential(self, wired):
        answer = await router.complete(_request(), router.CompleteRequest(state="s", code=CODE))
        assert answer == {
            "source_id": "mail",
            "account": "ada@example.test",
            "client_secret": "${secret:a}",
            "refresh_token": "${secret:b}",
        }
        assert CODE not in repr(answer)

    @pytest.mark.parametrize(
        ("code", "status"),
        [
            ("state_used", 400),
            ("state_expired", 400),
            ("unknown_state", 400),
            ("state_not_yours", 403),
        ],
    )
    async def test_a_refusal_keeps_its_name_and_status(self, wired, code, status):
        wired["refusal"] = SignInRefused(code, "refused", status=status)
        with pytest.raises(ApiError) as raised:
            await router.complete(_request(), router.CompleteRequest(state="s", code=CODE))
        assert raised.value.code == f"source_sign_in.{code}"
        assert raised.value.status_code == status
        assert CODE not in str(raised.value.detail)

    async def test_a_refusals_particulars_are_passed_on(self, wired):
        wired["refusal"] = SignInRefused(
            "wrong_account", "bo approved", approved="bo@example.test", account="ada@example.test"
        )
        with pytest.raises(ApiError) as raised:
            await router.complete(_request(), router.CompleteRequest(state="s", code=CODE))
        assert raised.value.params == {"approved": "bo@example.test", "account": "ada@example.test"}
