# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6e3b84-91a7-4f25-b8c2-7e5a19f3c640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Signing a QuickBooks source in with Intuit (REQ-2197): the client is the source's own, the
company is the one Intuit names in its return address (``realmId``), the app authenticates with
HTTP Basic, and the approval is kept for that company as the source's refreshes keep theirs.

The control plane is a real PostgreSQL database; Intuit's token endpoint is a stand-in and every
credential is made up."""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import sqlalchemy as sa

from provisa.api.admin import source_sign_in_router as router
from provisa.api.errors import ApiError
from provisa.core import request_context, schema_admin, secrets_store, source_sign_in
from provisa.core.source_sign_in import Completed, SignInRefused, Started
from provisa.quickbooks.settings import ACCOUNTING_SCOPE, AUTHORIZATION_URL, TOKEN_URL
from provisa.quickbooks.sign_in import QUICKBOOKS

pytestmark = pytest.mark.asyncio

ORG, USER, ENV = "acme", "uid-ada", "prod"
PUBLIC = "https://acme.provisa.test"
REDIRECT = "https://cloud.provisa.test/source-sign-in.html"
REALM = "9341453456789012"
CLIENT_ID = "made-up-intuit-client"
ENTERED_SECRET = "${secret:quickbooks_client_secret}"
CLIENT_SECRET = "made-up-client-secret"
CODE = "made-up-authorization-code"
ACCESS, REFRESH = "made-up-access-token", "made-up-refresh-token"
SECRETS = (CLIENT_SECRET, CODE, ACCESS, REFRESH)


class _Sealer:
    """Stands in for the vault's cipher: reversible, and not the identity."""

    def encrypt(self, value: bytes) -> bytes:
        return b"sealed:" + value[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        assert blob.startswith(b"sealed:")
        return blob[len(b"sealed:") :][::-1]


class Intuit:
    """Intuit's token endpoint: what it was sent, and what it answers."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.answer: httpx.Response | None = None

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        # Intuit's answer: no scope is stated.
        granted = {
            "token_type": "bearer",
            "access_token": ACCESS,
            "refresh_token": REFRESH,
            "expires_in": 3600,
            "x_refresh_token_expires_in": 8726400,
        }
        return self.answer or httpx.Response(200, json=granted)

    def form(self) -> dict[str, str]:
        (request,) = self.requests
        return {k: v[0] for k, v in parse_qs(request.content.decode()).items()}


@pytest.fixture
def plane(control_plane):
    first = control_plane.platform
    with first.engine.begin() as conn:
        conn.execute(schema_admin.orgs.insert(), [{"id": ORG, "name": ORG}])
    return first


@pytest.fixture
def vault(monkeypatch):
    """The source's client secret, entered as a reference, resolved in the organisation's vault;
    no organisation-wide mail-platform entry is read."""
    from provisa.core import mail_platforms

    resolved: list[str] = []

    async def resolve(entered: str) -> str:
        resolved.append(entered)
        return CLIENT_SECRET if entered == ENTERED_SECRET else entered

    async def no_entry(*args, **kwargs):
        raise AssertionError("a QuickBooks sign-in reads no organisation-wide client")

    monkeypatch.setattr(secrets_store, "_cipher", lambda *, mint: _Sealer())
    monkeypatch.setattr(source_sign_in, "_source_client_secret", resolve)
    monkeypatch.setattr(mail_platforms, "require", no_entry)
    monkeypatch.setattr(mail_platforms, "credential", no_entry)
    return resolved


@pytest.fixture
def intuit(monkeypatch):
    intuit = Intuit()
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        source_sign_in.httpx,
        "AsyncClient",
        lambda **kw: real_client(transport=httpx.MockTransport(intuit.handle), **kw),
    )
    return intuit


KEPT: list[tuple[str, str, str, list[str]]] = []


@pytest.fixture(autouse=True)
def _nothing_kept():
    KEPT.clear()
    yield
    KEPT.clear()


async def _keep(source_id: str, account: str, refresh_token: str, scopes: list[str]) -> None:
    KEPT.append((source_id, account, refresh_token, scopes))


async def _start(plane, **changed):
    given = {
        "org_id": ORG,
        "env": ENV,
        "user_id": USER,
        "source_id": "books",
        "kind_id": "quickbooks",
        "account": "",
        "mapping": {"environment": "sandbox"},
        "client_id": CLIENT_ID,
        "client_secret": ENTERED_SECRET,
        "public_address": PUBLIC,
    }
    given.update(changed)
    return await source_sign_in.start(plane, **given)


def _state(started: Started) -> str:
    return parse_qs(urlsplit(started.authorization_url).query)["state"][0]


async def _complete(plane, started: Started, **changed):
    given = {
        "org_id": ORG,
        "user_id": USER,
        "state": _state(started),
        "code": CODE,
        "error": None,
        "keep_grant": _keep,
        "returned": {"realmId": REALM},
    }
    given.update(changed)
    return await source_sign_in.complete(plane, **given)


def _rows(plane) -> list[dict]:
    with plane.engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sa.select(schema_admin.source_sign_ins))]


def _says_no_secret(refused: SignInRefused) -> bool:
    said = " ".join([str(refused), refused.code, *map(str, refused.params.values())])
    return not any(secret in said for secret in SECRETS)


class TestTheKind:
    async def test_it_is_registered_for_the_quickbooks_source_type(self):
        assert source_sign_in.KINDS["quickbooks"] is QUICKBOOKS

    async def test_it_asks_for_the_accounting_api_whatever_the_settings(self):
        assert QUICKBOOKS.scopes({}) == [ACCOUNTING_SCOPE] == ["com.intuit.quickbooks.accounting"]
        assert QUICKBOOKS.scopes({"environment": "production"}) == [ACCOUNTING_SCOPE]


class TestStart:
    async def test_intuit_is_asked_for_the_accounting_api_with_the_sources_own_client(
        self, plane, vault, intuit
    ):
        started = await _start(plane)
        asked = {k: v[0] for k, v in parse_qs(urlsplit(started.authorization_url).query).items()}
        assert started.authorization_url.startswith(f"{AUTHORIZATION_URL}?")
        assert asked["client_id"] == CLIENT_ID
        assert asked["scope"] == "com.intuit.quickbooks.accounting"
        assert asked["response_type"] == "code"
        # The address the existing sign-in routes use.
        assert asked["redirect_uri"] == REDIRECT == source_sign_in.redirect_address(PUBLIC)
        assert started.return_origin == "https://cloud.provisa.test"
        assert intuit.requests == []

    async def test_the_client_secret_is_kept_only_sealed_and_never_in_the_address(
        self, plane, vault, intuit
    ):
        started = await _start(plane, client_secret=CLIENT_SECRET)
        assert CLIENT_SECRET not in started.authorization_url
        (row,) = _rows(plane)
        assert CLIENT_SECRET not in repr(row)
        assert (
            CLIENT_SECRET[::-1]
            in base64.b64decode(json.loads(row["settings"])["client_secret_sealed"]).decode()
        )

    async def test_a_company_need_not_be_named_before_the_sign_in(self, plane, vault, intuit):
        await _start(plane, account="")
        assert _rows(plane)[0]["account"] == ""

    async def test_without_its_client_secret_it_is_refused(self, plane, vault, intuit):
        with pytest.raises(SignInRefused) as raised:
            await _start(plane, client_secret=None)
        assert raised.value.code == "source_sign_in.incomplete"
        assert _rows(plane) == []

    async def test_an_organisation_client_kind_takes_no_client_secret_from_a_source(
        self, plane, vault, intuit, monkeypatch
    ):
        monkeypatch.setitem(
            source_sign_in.KINDS,
            "stand_in",
            source_sign_in.SignInKind(
                id="stand_in",
                authorization_endpoint="https://issuer.test/authorize",
                token_endpoint="https://issuer.test/token",
                authorization_params=lambda account: {},
                scopes=lambda mapping: ["read"],
            ),
        )
        with pytest.raises(SignInRefused) as raised:
            await _start(plane, kind_id="stand_in", account="ada@example.test")
        assert raised.value.code == "source_sign_in.client_not_taken"


class TestComplete:
    async def test_the_code_is_exchanged_with_the_apps_credentials_as_basic_auth(
        self, plane, vault, intuit
    ):
        started = await _start(plane)
        await _complete(plane, started)
        (request,) = intuit.requests
        assert str(request.url) == TOKEN_URL
        basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
        assert request.headers["Authorization"] == f"Basic {basic}"
        form = intuit.form()
        assert form["grant_type"] == "authorization_code"
        assert form["code"] == CODE
        assert form["redirect_uri"] == REDIRECT
        # The secret is in the header alone, resolved from the reference the source holds.
        assert "client_secret" not in form and "client_id" not in form
        assert vault == [ENTERED_SECRET]

    async def test_the_refresh_token_is_kept_for_the_company_intuit_names(
        self, plane, vault, intuit
    ):
        started = await _start(plane)
        done = await _complete(plane, started)
        assert done == Completed(source_id="books", account=REALM)
        # Kept as the source keeps the ones Intuit rotates: under the realm id, with what the
        # sign-in asked for, since Intuit's answer does not say.
        assert KEPT == [("books", REALM, REFRESH, [ACCOUNTING_SCOPE])]

    async def test_the_company_chosen_at_intuit_wins_over_the_one_the_form_named(
        self, plane, vault, intuit
    ):
        started = await _start(plane, account="1234")
        done = await _complete(plane, started)
        assert done.account == REALM
        assert KEPT[0][1] == REALM
        # A sweep of the source, should it not be saved, forgets what was kept.
        assert _rows(plane)[0]["account"] == REALM

    @pytest.mark.parametrize("realm", [None, "", "  ", "<script>", "../../x"])
    async def test_an_answer_that_names_no_company_is_refused_before_the_exchange(
        self, plane, vault, intuit, realm
    ):
        started = await _start(plane)
        with pytest.raises(SignInRefused) as raised:
            await _complete(plane, started, returned={"realmId": realm})
        assert raised.value.code == "source_sign_in.no_account_returned"
        assert raised.value.params == {"parameter": "realmId"}
        assert intuit.requests == [] and KEPT == []

    async def test_intuits_refusal_is_named_and_carries_no_credential(self, plane, vault, intuit):
        intuit.answer = httpx.Response(400, json={"error": "invalid_grant", "code": CODE})
        started = await _start(plane)
        with pytest.raises(SignInRefused) as raised:
            await _complete(plane, started)
        assert raised.value.code == "source_sign_in.refused_by_issuer"
        assert raised.value.params == {"error": "invalid_grant"}
        assert _says_no_secret(raised.value)
        assert KEPT == []

    async def test_an_answer_without_a_refresh_token_is_refused(self, plane, vault, intuit):
        intuit.answer = httpx.Response(200, json={"access_token": ACCESS, "expires_in": 3600})
        started = await _start(plane)
        with pytest.raises(SignInRefused) as raised:
            await _complete(plane, started)
        assert raised.value.code == "source_sign_in.no_refresh_token"
        assert KEPT == []

    async def test_the_state_is_used_once(self, plane, vault, intuit):
        started = await _start(plane)
        await _complete(plane, started)
        with pytest.raises(SignInRefused) as raised:
            await _complete(plane, started)
        assert raised.value.code == "source_sign_in.state_used"
        assert len(intuit.requests) == 1


# -- the calls the form makes --------------------------------------------------------------------


def _request() -> Any:
    return SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id=USER)))


@pytest.fixture
def wired(monkeypatch):
    seen: dict = {"started": [], "completed": [], "read": [], "resolved": []}
    token = request_context.set_current_org(ORG)

    async def require(admin_db, org_id, platform_id):
        seen["read"].append(platform_id)
        raise AssertionError("a QuickBooks sign-in reads no organisation-wide client")

    async def sweep(admin_db, **given):
        return None

    async def start(admin_db, **given):
        seen["started"].append(given)
        return Started(f"{AUTHORIZATION_URL}?state=s", 600, "https://cloud.provisa.test")

    async def complete(admin_db, **given):
        seen["completed"].append(given)
        return Completed("books", REALM)

    async def resolved(entered: str) -> str:
        seen["resolved"].append(entered)
        return {"${secret:qb_client_id}": CLIENT_ID}.get(entered, entered)

    monkeypatch.setattr(router, "require_capability_request", lambda request, capability: None)
    monkeypatch.setattr(router, "_admin_db", lambda: "admin-plane")
    monkeypatch.setattr(router, "_public_address", lambda: PUBLIC)
    monkeypatch.setattr(router, "_resolved", resolved)
    monkeypatch.setattr(router.mail_platforms, "require", require)
    monkeypatch.setattr(router.source_sign_in, "sweep", sweep)
    monkeypatch.setattr(router.source_sign_in, "start", start)
    monkeypatch.setattr(router.source_sign_in, "complete", complete)
    yield seen
    request_context.reset_current_org(token)


def _body(**changed) -> router.StartRequest:
    given = {
        "source_id": "books",
        "kind": "quickbooks",
        "account": "",
        "mapping": {"environment": "sandbox"},
        "client": {"client_id": "${secret:qb_client_id}", "client_secret": ENTERED_SECRET},
    }
    given.update(changed)
    return router.StartRequest(**given)


class TestRouter:
    async def test_the_client_is_the_sources_own_as_the_form_holds_it(self, wired):
        await router.start(_request(), _body())
        (given,) = wired["started"]
        assert wired["read"] == []
        # The id is sent to Intuit, so it is resolved; the secret goes on as entered.
        assert given["client_id"] == CLIENT_ID
        assert given["client_secret"] == ENTERED_SECRET
        assert given["settings"] == {}
        assert wired["resolved"] == ["${secret:qb_client_id}"]

    @pytest.mark.parametrize(
        "client",
        [None, {"client_id": "", "client_secret": "x"}, {"client_id": "x", "client_secret": " "}],
    )
    async def test_a_quickbooks_sign_in_without_the_apps_client_is_refused_by_name(
        self, wired, client
    ):
        with pytest.raises(ApiError) as raised:
            await router.start(_request(), _body(client=client))
        assert raised.value.code == "source_sign_in.client_missing"
        assert wired["started"] == []

    async def test_a_mail_source_is_not_handed_a_client_by_the_form(self, wired):
        with pytest.raises(ApiError) as raised:
            await router.start(
                _request(), _body(kind="google_workspace", account="ada@example.test")
            )
        assert raised.value.code == "source_sign_in.client_not_taken"
        assert wired["started"] == [] and wired["read"] == []

    async def test_the_client_is_never_printed_with_its_secret(self):
        client = router.SourceClient(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        assert CLIENT_SECRET not in repr(client)

    async def test_intuits_realm_id_is_handed_to_the_exchange(self, wired):
        answer = await router.complete(
            _request(), router.CompleteRequest(state="s", code=CODE, realm_id=REALM)
        )
        (given,) = wired["completed"]
        assert given["returned"] == {"realmId": REALM}
        assert answer == {"source_id": "books", "account": REALM, "connected": True}
