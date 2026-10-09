# Copyright (c) 2026 Kenneth Stott
# Canary: 5463ca48-991d-4a8f-bce3-d377d51dbea2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Signing a source in to its issuer by a person's approval (REQ-1923).

The control plane is a real SQLite file, so the single use of a state and its survival across
database handles are the database's doing. The issuer is a stand-in and every credential is
made up; the vault is a dict that records what was put under which name.
"""

# Requirements: REQ-1923
from __future__ import annotations

import base64
import datetime as dt
import hashlib
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import sqlalchemy as sa

from provisa.core import schema_admin, secrets_store, source_sign_in
from provisa.core.database import Database, create_engine_from_url
from provisa.core.source_sign_in import SignInKind, SignInRefused

pytestmark = pytest.mark.asyncio

ORG, USER, ENV = "acme", "uid-ada", "prod"
ACCOUNT = "ada@example.test"
PUBLIC = "https://acme.provisa.test"
REDIRECT = "https://cloud.provisa.test/source-sign-in.html"
SECRET_NAME, TOKEN_NAME = "source_mail__client_secret", "source_mail__refresh_token"
CLIENT_SECRET = "made-up-client-secret"
CODE = "made-up-authorization-code"
ACCESS, REFRESH = "made-up-access-token", "made-up-refresh-token"
SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
SECRETS = (CLIENT_SECRET, CODE, ACCESS, REFRESH)


class _Sealer:
    """Stands in for the vault's cipher: reversible, and not the identity."""

    def encrypt(self, value: bytes) -> bytes:
        return b"sealed:" + value[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        assert blob.startswith(b"sealed:")
        return blob[len(b"sealed:") :][::-1]


class Issuer:
    """The issuer's token endpoint and its account lookup."""

    def __init__(self) -> None:
        self.forms: list[dict] = []
        self.answer: httpx.Response | None = None
        self.approved = ACCOUNT

    def granted(self, **changed) -> dict:
        body = {"access_token": ACCESS, "refresh_token": REFRESH, "expires_in": 3599}
        body.update(changed)
        return {k: v for k, v in body.items() if v is not None}

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.forms.append({k: v[0] for k, v in parse_qs(request.content.decode()).items()})
        return self.answer or httpx.Response(200, json=self.granted())

    async def approved_account(self, access_token: str) -> str:
        assert access_token == ACCESS
        return self.approved


@pytest.fixture
def control_plane(tmp_path):
    def open_handle() -> Database:
        engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'plane.db'}")
        return Database(engine, name="admin")

    first = open_handle()
    with first.engine.begin() as conn:
        schema_admin.metadata.create_all(
            conn, tables=[schema_admin.orgs, schema_admin.source_sign_ins]
        )
    first.open_handle = open_handle  # type: ignore[attr-defined]
    return first


@pytest.fixture
def vault(monkeypatch):
    held: dict[str, str] = {}

    @asynccontextmanager
    async def bound(admin_db, org_id, *, user_id=None):
        yield

    def resolve(reference: str) -> str:
        return held[reference.removeprefix("${secret:").removesuffix("}")]

    monkeypatch.setattr(secrets_store, "_cipher", lambda *, mint: _Sealer())
    monkeypatch.setattr(secrets_store, "bound", bound)
    monkeypatch.setattr(source_sign_in, "resolve_secrets", resolve)
    return held


@pytest.fixture
def issuer(monkeypatch):
    issuer = Issuer()
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        source_sign_in.httpx,
        "AsyncClient",
        lambda **kw: real_client(transport=httpx.MockTransport(issuer.handle), **kw),
    )
    monkeypatch.setitem(
        source_sign_in.KINDS,
        "stand_in",
        SignInKind(
            id="stand_in",
            authorization_endpoint="https://issuer.test/authorize",
            token_endpoint="https://issuer.test/token",
            authorization_params=lambda account: {"login_hint": account},
            approved_account=issuer.approved_account,
        ),
    )
    return issuer


def _store(vault: dict):
    async def store(name: str, value: str, description: str) -> str:
        vault[name] = value
        return f"${{secret:{name}}}"

    return store


async def _start(control_plane, vault, **changed):
    given = {
        "org_id": ORG,
        "env": ENV,
        "user_id": USER,
        "source_id": "mail",
        "kind_id": "stand_in",
        "account": ACCOUNT,
        "scopes": [SCOPE],
        "client_id": "client-1",
        "client_secret": CLIENT_SECRET,
        "public_address": PUBLIC,
        "secret_names": (SECRET_NAME, TOKEN_NAME),
        "store_secret": _store(vault),
    }
    given.update(changed)
    return await source_sign_in.start(control_plane, **given)


def _asked(started) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(started.authorization_url).query).items()}


async def _complete(control_plane, vault, state, **changed):
    given = {
        "org_id": ORG,
        "user_id": USER,
        "state": state,
        "code": CODE,
        "error": None,
        "store_secret": _store(vault),
    }
    given.update(changed)
    return await source_sign_in.complete(control_plane, **given)


def _rows(control_plane) -> list[dict]:
    with control_plane.engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sa.select(schema_admin.source_sign_ins))]


def _says_no_secret(refused: SignInRefused) -> bool:
    said = " ".join([str(refused), refused.code, *map(str, refused.params.values())])
    return not any(secret in said for secret in SECRETS)


# -- the address the issuer returns to --------------------------------------------------------


class TestRedirectAddress:
    async def test_it_is_on_the_sign_in_host_of_the_configured_public_address(self):
        assert source_sign_in.redirect_address(PUBLIC) == REDIRECT

    @pytest.mark.parametrize(
        ("configured", "address"),
        [
            ("https://cloud.provisa.test/", "https://cloud.provisa.test/source-sign-in.html"),
            ("http://localhost:3000", "http://localhost:3000/source-sign-in.html"),
            ("https://provisa.test", "https://provisa.test/source-sign-in.html"),
            ("http://10.1.2.3:8080", "http://10.1.2.3:8080/source-sign-in.html"),
        ],
    )
    async def test_a_host_that_names_no_organisation_is_used_as_configured(
        self, configured, address
    ):
        assert source_sign_in.redirect_address(configured) == address

    @pytest.mark.parametrize("unset", [None, "", "  ", "cloud.provisa.test"])
    async def test_no_public_address_is_refused_by_name(self, unset):
        with pytest.raises(SignInRefused) as raised:
            source_sign_in.redirect_address(unset)
        assert raised.value.code == "source_sign_in.public_address_not_set"


# -- starting ----------------------------------------------------------------------------------


class TestStart:
    async def test_the_issuer_is_asked_with_a_state_and_a_challenge(
        self, control_plane, vault, issuer
    ):
        started = await _start(control_plane, vault)
        asked = _asked(started)
        assert started.authorization_url.startswith("https://issuer.test/authorize?")
        assert started.expires_in == source_sign_in.STATE_LIFETIME == 600
        assert asked["response_type"] == "code"
        assert asked["client_id"] == "client-1"
        assert asked["redirect_uri"] == REDIRECT
        assert asked["scope"] == SCOPE
        assert asked["code_challenge_method"] == "S256"
        assert asked["login_hint"] == ACCOUNT
        assert len(asked["state"]) >= 43  # 32 random bytes

    async def test_each_start_has_its_own_state(self, control_plane, vault, issuer):
        first, second = [_asked(await _start(control_plane, vault))["state"] for _ in range(2)]
        assert first != second

    async def test_only_the_states_digest_is_kept_and_the_verifier_is_sealed(
        self, control_plane, vault, issuer
    ):
        asked = _asked(await _start(control_plane, vault))
        (row,) = _rows(control_plane)
        assert row["state_digest"] == hashlib.sha256(asked["state"].encode()).hexdigest()
        assert asked["state"] not in map(str, row.values())
        assert row["verifier"].startswith(b"sealed:")
        verifier = secrets_store.unseal(row["verifier"])
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        assert asked["code_challenge"] == challenge.rstrip(b"=").decode()
        assert verifier.encode() not in row["verifier"]

    async def test_it_is_bound_to_who_started_it_and_what_for(self, control_plane, vault, issuer):
        await _start(control_plane, vault)
        (row,) = _rows(control_plane)
        assert (row["org_id"], row["env"], row["user_id"]) == (ORG, ENV, USER)
        assert (row["source_id"], row["account"], row["scopes"]) == ("mail", ACCOUNT, SCOPE)
        assert row["used_at"] is None

    async def test_the_client_secret_goes_to_the_vault_and_not_into_what_is_pending(
        self, control_plane, vault, issuer
    ):
        started = await _start(control_plane, vault)
        assert vault == {SECRET_NAME: CLIENT_SECRET}
        (row,) = _rows(control_plane)
        assert CLIENT_SECRET not in map(str, row.values())
        assert CLIENT_SECRET not in started.authorization_url

    async def test_the_redirect_address_is_the_configured_one(self, control_plane, vault, issuer):
        with pytest.raises(SignInRefused) as raised:
            await _start(control_plane, vault, public_address="")
        assert raised.value.code == "source_sign_in.public_address_not_set"
        assert vault == {} and _rows(control_plane) == []

    async def test_an_unknown_kind_is_refused(self, control_plane, vault, issuer):
        with pytest.raises(SignInRefused) as raised:
            await _start(control_plane, vault, kind_id="nobody")
        assert raised.value.code == "source_sign_in.unknown_kind"

    @pytest.mark.parametrize("missing", ["source_id", "account", "client_id", "client_secret"])
    async def test_a_start_without_what_it_needs_is_refused(
        self, control_plane, vault, issuer, missing
    ):
        with pytest.raises(SignInRefused) as raised:
            await _start(control_plane, vault, **{missing: ""})
        assert raised.value.code == "source_sign_in.incomplete"
        assert vault == {}


# -- completing --------------------------------------------------------------------------------


class TestComplete:
    async def test_the_code_is_exchanged_with_the_verifier_and_the_token_goes_to_the_vault(
        self, control_plane, vault, issuer
    ):
        asked = _asked(await _start(control_plane, vault))
        done = await _complete(control_plane, vault, asked["state"])
        (form,) = issuer.forms
        assert form["grant_type"] == "authorization_code"
        assert form["code"] == CODE
        assert form["client_secret"] == CLIENT_SECRET
        assert form["redirect_uri"] == REDIRECT
        sent_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(form["code_verifier"].encode()).digest()
        )
        assert sent_challenge.rstrip(b"=").decode() == asked["code_challenge"]
        assert vault[TOKEN_NAME] == REFRESH
        assert done == source_sign_in.Completed(
            source_id="mail",
            account=ACCOUNT,
            client_secret=f"${{secret:{SECRET_NAME}}}",
            refresh_token=f"${{secret:{TOKEN_NAME}}}",
        )

    async def test_what_it_answers_carries_no_credential(self, control_plane, vault, issuer):
        asked = _asked(await _start(control_plane, vault))
        done = await _complete(control_plane, vault, asked["state"])
        assert not any(secret in repr(done) for secret in SECRETS)

    async def test_a_state_is_good_once(self, control_plane, vault, issuer):
        state = _asked(await _start(control_plane, vault))["state"]
        await _complete(control_plane, vault, state)
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, state)
        assert raised.value.code == "source_sign_in.state_used"
        assert len(issuer.forms) == 1

    async def test_a_state_this_deployment_did_not_issue_is_refused(
        self, control_plane, vault, issuer
    ):
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, "made-up-state")
        assert raised.value.code == "source_sign_in.unknown_state"
        assert issuer.forms == [] and vault == {}

    @pytest.mark.parametrize("caller", [{"org_id": "globex"}, {"user_id": "uid-bo"}])
    async def test_another_organisations_or_persons_state_is_refused_and_not_spent(
        self, control_plane, vault, issuer, caller
    ):
        state = _asked(await _start(control_plane, vault))["state"]
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, state, **caller)
        assert raised.value.code == "source_sign_in.state_not_yours"
        assert raised.value.status == 403
        assert issuer.forms == [] and TOKEN_NAME not in vault
        assert (await _complete(control_plane, vault, state)).account == ACCOUNT

    async def test_a_state_past_its_time_is_refused(
        self, control_plane, vault, issuer, monkeypatch
    ):
        state = _asked(await _start(control_plane, vault))["state"]
        later = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=601)
        monkeypatch.setattr(source_sign_in, "_now", lambda: later)
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, state)
        assert raised.value.code == "source_sign_in.state_expired"
        assert issuer.forms == []

    async def test_a_sign_in_may_be_completed_by_another_process(
        self, control_plane, vault, issuer
    ):
        state = _asked(await _start(control_plane, vault))["state"]
        elsewhere = control_plane.open_handle()
        assert (await _complete(elsewhere, vault, state)).account == ACCOUNT
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, state)
        assert raised.value.code == "source_sign_in.state_used"

    async def test_the_issuers_refusal_at_the_approval_is_named_and_spends_the_state(
        self, control_plane, vault, issuer
    ):
        state = _asked(await _start(control_plane, vault))["state"]
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, state, code=None, error="access_denied")
        assert raised.value.code == "source_sign_in.refused_by_issuer"
        assert raised.value.params == {"error": "access_denied"}
        with pytest.raises(SignInRefused) as again:
            await _complete(control_plane, vault, state)
        assert again.value.code == "source_sign_in.state_used"

    async def test_what_the_browser_says_the_issuer_said_is_not_repeated_unless_it_is_a_name(
        self, control_plane, vault, issuer
    ):
        state = _asked(await _start(control_plane, vault))["state"]
        with pytest.raises(SignInRefused) as raised:
            await _complete(
                control_plane, vault, state, code=None, error="<script>made-up</script>"
            )
        assert raised.value.params == {"error": "no_code"}

    async def test_the_issuers_refusal_of_the_code_is_named_without_what_else_it_said(
        self, control_plane, vault, issuer
    ):
        issuer.answer = httpx.Response(
            400, json={"error": "invalid_grant", "error_description": f"Bad code {CODE}"}
        )
        state = _asked(await _start(control_plane, vault))["state"]
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, state)
        assert raised.value.code == "source_sign_in.refused_by_issuer"
        assert raised.value.params == {"error": "invalid_grant"}
        assert _says_no_secret(raised.value)
        assert TOKEN_NAME not in vault

    async def test_an_approval_without_a_refresh_token_is_refused(
        self, control_plane, vault, issuer
    ):
        issuer.answer = httpx.Response(200, json=issuer.granted(refresh_token=None))
        state = _asked(await _start(control_plane, vault))["state"]
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, state)
        assert raised.value.code == "source_sign_in.no_refresh_token"
        assert _says_no_secret(raised.value) and TOKEN_NAME not in vault

    async def test_an_approval_by_another_account_is_refused_naming_both(
        self, control_plane, vault, issuer
    ):
        issuer.approved = "Bo@Example.test"
        state = _asked(await _start(control_plane, vault))["state"]
        with pytest.raises(SignInRefused) as raised:
            await _complete(control_plane, vault, state)
        assert raised.value.code == "source_sign_in.wrong_account"
        assert raised.value.params == {"approved": "bo@example.test", "account": ACCOUNT}
        assert _says_no_secret(raised.value) and TOKEN_NAME not in vault

    async def test_the_account_is_compared_without_regard_to_case(
        self, control_plane, vault, issuer
    ):
        issuer.approved = "Ada@Example.Test"
        state = _asked(await _start(control_plane, vault))["state"]
        assert (await _complete(control_plane, vault, state)).account == ACCOUNT


# -- sign-ins that came to nothing --------------------------------------------------------------


class TestSweep:
    @pytest.fixture
    def swept(self, control_plane, vault):
        forgotten: list[str] = []
        saved: set[str] = set()

        async def exists(source_id: str) -> bool:
            return source_id in saved

        async def forget(name: str) -> None:
            forgotten.append(name)
            vault.pop(name, None)

        async def run() -> list[str]:
            await source_sign_in.sweep(
                control_plane, org_id=ORG, env=ENV, source_exists=exists, forget_secret=forget
            )
            return forgotten

        run.saved = saved  # type: ignore[attr-defined]
        return run

    @staticmethod
    def _later(monkeypatch, seconds: int) -> None:
        later = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds)
        monkeypatch.setattr(source_sign_in, "_now", lambda: later)

    async def test_a_sign_in_still_in_time_is_left_alone(self, control_plane, vault, issuer, swept):
        await _start(control_plane, vault)
        assert await swept() == []
        assert len(_rows(control_plane)) == 1

    async def test_one_never_completed_loses_the_entries_it_wrote(
        self, control_plane, vault, issuer, swept, monkeypatch
    ):
        vault["someone_elses_secret"] = "made-up-unrelated"
        await _start(control_plane, vault)
        self._later(monkeypatch, 601)
        assert await swept() == [SECRET_NAME, TOKEN_NAME]
        assert _rows(control_plane) == []
        assert vault == {"someone_elses_secret": "made-up-unrelated"}

    async def test_a_saved_sources_entries_survive(
        self, control_plane, vault, issuer, swept, monkeypatch
    ):
        state = _asked(await _start(control_plane, vault))["state"]
        await _complete(control_plane, vault, state)
        swept.saved.add("mail")
        self._later(monkeypatch, 24 * 3600 + 1)
        assert await swept() == []
        assert vault == {SECRET_NAME: CLIENT_SECRET, TOKEN_NAME: REFRESH}
        assert _rows(control_plane) == []

    async def test_one_completed_and_never_saved_loses_its_entries_after_a_day(
        self, control_plane, vault, issuer, swept, monkeypatch
    ):
        state = _asked(await _start(control_plane, vault))["state"]
        await _complete(control_plane, vault, state)
        self._later(monkeypatch, 3600)
        assert await swept() == []
        self._later(monkeypatch, 24 * 3600 + 1)
        assert await swept() == [SECRET_NAME, TOKEN_NAME]
        assert vault == {}

    async def test_a_source_being_signed_in_again_keeps_its_entries(
        self, control_plane, vault, issuer, swept, monkeypatch
    ):
        await _start(control_plane, vault)
        self._later(monkeypatch, 601)
        await _start(control_plane, vault)  # the operator tries again
        assert await swept() == []
        assert vault == {SECRET_NAME: CLIENT_SECRET}
        assert len(_rows(control_plane)) == 1

    async def test_another_organisations_sign_ins_are_not_touched(
        self, control_plane, vault, issuer, swept, monkeypatch
    ):
        await _start(control_plane, vault, org_id="globex")
        self._later(monkeypatch, 601)
        assert await swept() == []
        assert len(_rows(control_plane)) == 1
