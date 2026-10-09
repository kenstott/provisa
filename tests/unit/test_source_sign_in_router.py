# Copyright (c) 2026 Kenneth Stott
# Canary: a9572009-6901-42c4-a149-02817776e895
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The calls the Sources form makes to sign a source in (REQ-1923): who may make them, that the
client is the ORGANISATION's and never the form's, what they hand the exchange, and that
nothing they answer is a credential. The exchange itself is held to its rules in
``test_source_sign_in.py``; the organisation's setting in ``test_mail_platforms.py``."""

# Requirements: REQ-1923
from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import source_sign_in_router as router
from provisa.api.errors import ApiError
from provisa.core import mail_platforms, request_context
from provisa.core.mail_platforms import Configured, MailPlatformRefused
from provisa.core.source_sign_in import Completed, SignInRefused, Started

pytestmark = pytest.mark.asyncio

ORG = "acme"
KIND = "google_workspace"
CODE = "made-up-authorization-code"


def _request(user_id: str | None = "uid-ada"):
    identity = None if user_id is None else SimpleNamespace(user_id=user_id)
    # No headers and no url: a call that read the address it was reached on would fail here.
    return SimpleNamespace(state=SimpleNamespace(identity=identity))


@pytest.fixture
def wired(monkeypatch):
    seen: dict = {"asked": [], "read": [], "swept": [], "started": [], "completed": []}
    seen["client"] = Configured(KIND, "org-client-1", {})
    token = request_context.set_current_org(ORG)

    def gate(request, capability):
        seen["asked"].append(capability)
        if seen.get("refuse"):
            raise ApiError(403, "auth.missing_capability", f"Missing capability: {capability!r}")

    async def read(admin_db, org_id, platform_id):
        seen["read"].append((admin_db, org_id, platform_id))
        return seen["client"]

    async def require(admin_db, org_id, platform_id):
        seen["read"].append((admin_db, org_id, platform_id))
        if seen["client"] is None:
            raise MailPlatformRefused("not_configured", "not connected", platform=platform_id)
        return seen["client"]

    async def sweep(admin_db, **given):
        seen["swept"].append(given)

    async def start(admin_db, **given):
        seen["started"].append(given)
        return Started(
            "https://issuer.test/authorize?state=made-up-state", 600, "https://cloud.provisa.test"
        )

    async def complete(admin_db, **given):
        seen["completed"].append(given)
        if seen.get("refusal"):
            raise seen["refusal"]
        return Completed("mail", "ada@example.test", "${secret:b}")

    monkeypatch.setattr(router, "require_capability_request", gate)
    monkeypatch.setattr(router, "_admin_db", lambda: "admin-plane")
    monkeypatch.setattr(router, "_public_address", lambda: "https://acme.provisa.test")
    monkeypatch.setattr(router, "_holds_org_settings", lambda request: seen.get("org_admin", False))
    monkeypatch.setattr(router.mail_platforms, "read", read)
    monkeypatch.setattr(router.mail_platforms, "require", require)
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
        "kind": KIND,
        "account": "ada@example.test",
        "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
    }
    given.update(changed)
    return router.StartRequest(**given)


class TestWhoMayAsk:
    async def test_each_call_needs_the_right_that_adding_a_source_needs(self, wired):
        await router.status(_request(), KIND)
        await router.start(_request(), _start_body())
        await router.complete(_request(), router.CompleteRequest(state="s", code=CODE))
        assert wired["asked"] == ["source_registration"] * 3

    async def test_without_it_nothing_is_read_recorded_or_exchanged(self, wired):
        wired["refuse"] = True
        for call in (
            lambda: router.status(_request(), KIND),
            lambda: router.start(_request(), _start_body()),
            lambda: router.complete(_request(), router.CompleteRequest(state="s", code=CODE)),
        ):
            with pytest.raises(ApiError) as raised:
                await call()
            assert raised.value.status_code == 403
        assert wired["read"] == wired["swept"] == wired["started"] == wired["completed"] == []


class TestStatus:
    async def test_it_says_whether_the_callers_organisation_has_a_client(self, wired):
        assert await router.status(_request(), KIND) == {"configured": True, "may_configure": False}
        assert wired["read"] == [("admin-plane", ORG, KIND)]
        wired["client"] = None
        assert (await router.status(_request(), KIND))["configured"] is False

    async def test_it_says_whether_the_caller_is_one_who_may_enter_it(self, wired):
        wired["client"], wired["org_admin"] = None, True
        assert await router.status(_request(), KIND) == {"configured": False, "may_configure": True}

    async def test_it_carries_nothing_of_the_client(self, wired):
        assert "org-client-1" not in repr(await router.status(_request(), KIND))


class TestStart:
    async def test_the_form_sends_no_client_and_no_address(self, wired):
        assert set(router.StartRequest.model_fields) == {"source_id", "kind", "account", "scopes"}

    async def test_the_client_is_the_callers_organisations(self, wired):
        wired["client"] = Configured(KIND, "org-client-1", {"tenant": "contoso"})
        await router.start(_request(), _start_body())
        (given,) = wired["started"]
        assert wired["read"] == [("admin-plane", ORG, KIND)]
        assert given["client_id"] == "org-client-1"
        assert given["client_secret_name"] == mail_platforms.secret_name(KIND)
        assert given["settings"] == {"tenant": "contoso"}

    async def test_an_organisation_with_no_client_is_refused_by_name_before_anything(self, wired):
        wired["client"] = None
        with pytest.raises(ApiError) as raised:
            await router.start(_request(), _start_body())
        assert raised.value.code == "mail_platform.not_configured"
        assert raised.value.params == {"platform": KIND}
        assert wired["swept"] == wired["started"] == []

    async def test_the_sign_in_is_bound_to_the_caller_their_organisation_and_environment(
        self, wired
    ):
        await router.start(_request(), _start_body())
        (given,) = wired["started"]
        assert (given["org_id"], given["env"], given["user_id"]) == (ORG, "prod", "uid-ada")
        assert given["public_address"] == "https://acme.provisa.test"
        assert given["kind_id"] == KIND

    async def test_the_refresh_token_will_go_under_the_sources_own_name(self, wired):
        from provisa.api.admin.schema_common import source_mapping_secret_name

        await router.start(_request(), _start_body())
        assert wired["started"][0]["refresh_token_name"] == source_mapping_secret_name(
            "mail", "refresh_token", "prod"
        )

    async def test_the_organisations_abandoned_sign_ins_are_swept_first(self, wired):
        await router.start(_request(), _start_body())
        (swept,) = wired["swept"]
        assert (swept["org_id"], swept["env"]) == (ORG, "prod")

    async def test_the_answer_is_where_to_send_the_browser_and_where_it_returns(self, wired):
        answer = await router.start(_request(), _start_body())
        assert answer == {
            "authorization_url": "https://issuer.test/authorize?state=made-up-state",
            "expires_in": 600,
            "return_origin": "https://cloud.provisa.test",
        }

    async def test_a_deployment_without_sign_in_binds_to_its_one_caller(self, wired):
        await router.start(_request(user_id=None), _start_body())
        assert wired["started"][0]["user_id"] == "anonymous"


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

    async def test_the_answer_is_a_reference_never_a_credential(self, wired):
        answer = await router.complete(_request(), router.CompleteRequest(state="s", code=CODE))
        assert answer == {
            "source_id": "mail",
            "account": "ada@example.test",
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
