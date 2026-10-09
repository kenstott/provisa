# Copyright (c) 2026 Kenneth Stott
# Canary: 9aac4571-6d11-4478-98bb-9d3ada38a7ab
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The delegated credentials of an API source (REQ-320): an OAuth2 refresh token, and a Google
service account reading as a named account. Every credential here is made up for the test and
the issuer is a stand-in; nothing is sent anywhere."""

from __future__ import annotations

import json
from unittest.mock import patch

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import TypeAdapter, ValidationError

from provisa.api_source import oauth_grants
from provisa.api_source.caller import ApiCallError, _apply_auth
from provisa.api_source.oauth_grants import (
    CredentialRefused,
    RefreshGrant,
    ReplacementNotKept,
    ServiceAccountKeyInvalid,
    access_token,
    exchange_refresh_token,
)
from provisa.core.auth_models import (
    ApiAuth,
    ApiAuthGoogleServiceAccount,
    ApiAuthOAuth2RefreshToken,
)

TOKEN_URL = "https://issuer.test/token"
SCOPE = "https://www.googleapis.com/auth/gmail.readonly"


@pytest.fixture(autouse=True)
def _nothing_held():
    oauth_grants._tokens.clear()
    yield
    oauth_grants._tokens.clear()


@pytest.fixture(scope="module")
def signing_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def key_file(signing_key) -> str:
    pem = signing_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return json.dumps(
        {
            "type": "service_account",
            "client_email": "reader@project.iam.test",
            "private_key": pem,
            "token_uri": TOKEN_URL,
        }
    )


def _issuer(status: int = 200, **body) -> httpx.Response:
    return httpx.Response(status, json=body, request=httpx.Request("POST", TOKEN_URL))


def _granted(token: str = "access-1", **extra) -> httpx.Response:
    return _issuer(access_token=token, expires_in=3600, token_type="Bearer", **extra)


def _refresh(**changed) -> ApiAuthOAuth2RefreshToken:
    fields = {
        "client_id": "client-1",
        "client_secret": "made-up-client-secret",
        "refresh_token": "made-up-refresh-token",
        "token_url": TOKEN_URL,
    }
    return ApiAuthOAuth2RefreshToken(**{**fields, **changed})


def _service_account(key_file: str, **changed) -> ApiAuthGoogleServiceAccount:
    fields = {"key": key_file, "subject": "ada@example.test", "scopes": [SCOPE]}
    return ApiAuthGoogleServiceAccount(**{**fields, **changed})


class TestStated:
    def test_both_kinds_are_api_auth(self, key_file):
        adapter = TypeAdapter(ApiAuth)
        assert isinstance(
            adapter.validate_python(_refresh().model_dump()), ApiAuthOAuth2RefreshToken
        )
        assert isinstance(
            adapter.validate_python(_service_account(key_file).model_dump()),
            ApiAuthGoogleServiceAccount,
        )

    def test_a_service_account_states_who_it_reads_as_and_for_what(self, key_file):
        with pytest.raises(ValidationError):
            ApiAuthGoogleServiceAccount(key=key_file, scopes=[SCOPE])  # type: ignore[call-arg]
        with pytest.raises(ValidationError):
            ApiAuthGoogleServiceAccount(key=key_file, subject="ada@example.test", scopes=[])


class TestRefreshToken:
    def test_the_refresh_token_is_exchanged_for_the_token_sent(self):
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()) as post:
            assert access_token(_refresh()) == "access-1"
        assert post.call_args.args == (TOKEN_URL,)
        assert post.call_args.kwargs["data"] == {
            "grant_type": "refresh_token",
            "client_id": "client-1",
            "client_secret": "made-up-client-secret",
            "refresh_token": "made-up-refresh-token",
        }

    def test_a_stated_scope_is_asked_for(self):
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()) as post:
            access_token(_refresh(scope=SCOPE))
        assert post.call_args.kwargs["data"]["scope"] == SCOPE

    def test_secret_references_are_resolved_at_the_exchange(self, monkeypatch):
        monkeypatch.setenv("TEST_GRANT_CLIENT_SECRET", "resolved-client-secret")
        monkeypatch.setenv("TEST_GRANT_REFRESH", "resolved-refresh-token")
        auth = _refresh(
            client_secret="${env:TEST_GRANT_CLIENT_SECRET}",
            refresh_token="${env:TEST_GRANT_REFRESH}",
        )
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()) as post:
            access_token(auth)
        sent = post.call_args.kwargs["data"]
        assert sent["client_secret"] == "resolved-client-secret"
        assert sent["refresh_token"] == "resolved-refresh-token"

    def test_a_token_is_held_until_it_lapses(self):
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()) as post:
            assert access_token(_refresh()) == access_token(_refresh()) == "access-1"
        post.assert_called_once()

    def test_a_token_about_to_lapse_is_asked_for_again(self):
        soon = _issuer(access_token="access-1", expires_in=oauth_grants._EXPIRY_MARGIN - 1)
        with patch.object(oauth_grants.httpx, "post", side_effect=[soon, _granted("access-2")]):
            assert access_token(_refresh()) == "access-1"
            assert access_token(_refresh()) == "access-2"

    def test_a_token_with_no_stated_life_is_not_held(self):
        answers = [_issuer(access_token="access-1"), _issuer(access_token="access-2")]
        with patch.object(oauth_grants.httpx, "post", side_effect=answers):
            assert access_token(_refresh()) == "access-1"
            assert access_token(_refresh()) == "access-2"
        assert oauth_grants._tokens == {}

    def test_another_persons_approval_is_another_token(self):
        answers = [_granted("access-ada"), _granted("access-bo")]
        with patch.object(oauth_grants.httpx, "post", side_effect=answers):
            assert access_token(_refresh()) == "access-ada"
            assert access_token(_refresh(refresh_token="made-up-other-token")) == "access-bo"

    def test_no_credential_is_kept_as_a_key_of_the_hold(self):
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()):
            access_token(_refresh())
        assert not any("made-up" in key for key in oauth_grants._tokens)

    def test_a_lapsed_approval_is_refused_with_the_issuers_reason(self):
        refused = _issuer(
            400, error="invalid_grant", error_description="Token has been expired or revoked."
        )
        with (
            patch.object(oauth_grants.httpx, "post", return_value=refused),
            pytest.raises(CredentialRefused) as raised,
        ):
            access_token(_refresh())
        assert isinstance(raised.value, ApiCallError)
        assert raised.value.reason == "invalid_grant: Token has been expired or revoked."
        assert "client-1" in str(raised.value)
        assert "made-up" not in str(raised.value)

    def test_a_refusal_without_a_reason_names_its_status(self):
        bare = httpx.Response(401, text="no", request=httpx.Request("POST", TOKEN_URL))
        with (
            patch.object(oauth_grants.httpx, "post", return_value=bare),
            pytest.raises(CredentialRefused) as raised,
        ):
            access_token(_refresh())
        assert raised.value.reason == "HTTP 401"

    def test_an_issuer_that_fails_is_not_a_refusal(self):
        with (
            patch.object(oauth_grants.httpx, "post", return_value=_issuer(503)),
            pytest.raises(httpx.HTTPStatusError),
        ):
            access_token(_refresh())
        assert oauth_grants._tokens == {}


class TestPublicClient:
    def test_a_client_with_no_secret_sends_none(self):
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()) as post:
            assert access_token(_refresh(client_secret=None)) == "access-1"
        assert post.call_args.kwargs["data"] == {
            "grant_type": "refresh_token",
            "client_id": "client-1",
            "refresh_token": "made-up-refresh-token",
        }


class TestExchange:
    """One exchange whose answer is the caller's to keep (the writer that stores a replaced
    refresh token calls this)."""

    def test_the_answer_is_returned_with_its_life(self):
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()):
            assert exchange_refresh_token(_refresh()) == RefreshGrant("access-1", 3600.0, None)

    def test_a_replacement_is_returned_to_the_caller(self):
        answer = _granted(refresh_token="made-up-second-token")
        with patch.object(oauth_grants.httpx, "post", return_value=answer):
            grant = exchange_refresh_token(_refresh())
        assert grant.replacement == "made-up-second-token"

    def test_the_same_token_answered_again_is_not_a_replacement(self):
        answer = _granted(refresh_token="made-up-refresh-token")
        with patch.object(oauth_grants.httpx, "post", return_value=answer):
            assert exchange_refresh_token(_refresh()).replacement is None

    def test_exactly_the_stated_refresh_token_is_sent_every_time(self):
        answers = [_granted(refresh_token="made-up-second-token"), _granted("access-2")]
        with patch.object(oauth_grants.httpx, "post", side_effect=answers) as post:
            exchange_refresh_token(_refresh())
            exchange_refresh_token(_refresh())
        sent = [call.kwargs["data"]["refresh_token"] for call in post.call_args_list]
        assert sent == ["made-up-refresh-token", "made-up-refresh-token"]

    def test_nothing_is_held(self):
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()) as post:
            exchange_refresh_token(_refresh())
            exchange_refresh_token(_refresh())
        assert post.call_count == 2
        assert oauth_grants._tokens == {}

    def test_no_stated_life_is_said_so(self):
        with patch.object(oauth_grants.httpx, "post", return_value=_issuer(access_token="a")):
            assert exchange_refresh_token(_refresh()).expires_in is None

    def test_a_refusal_is_the_issuers(self):
        refused = _issuer(400, error="invalid_grant", error_description="AADSTS700082: expired.")
        with (
            patch.object(oauth_grants.httpx, "post", return_value=refused),
            pytest.raises(CredentialRefused) as raised,
        ):
            exchange_refresh_token(_refresh())
        assert raised.value.reason == "invalid_grant: AADSTS700082: expired."


class TestReplacedRefreshTokenOnAPlainCall:
    """A plain API call keeps nothing, so an issuer that replaces the refresh token is refused."""

    def test_it_is_refused_by_name_and_no_token_is_held(self):
        answer = _granted(refresh_token="made-up-second-token")
        with (
            patch.object(oauth_grants.httpx, "post", return_value=answer),
            pytest.raises(ReplacementNotKept) as raised,
        ):
            access_token(_refresh())
        assert "replaces the refresh token at each use" in str(raised.value)
        assert "made-up" not in str(raised.value)
        assert oauth_grants._tokens == {}

    def test_the_generic_caller_is_such_a_call(self):
        answer = _granted(refresh_token="made-up-second-token")
        with (
            patch.object(oauth_grants.httpx, "post", return_value=answer),
            pytest.raises(ReplacementNotKept),
        ):
            _apply_auth(_refresh(), {}, {})


class TestServiceAccount:
    def test_an_assertion_signed_by_the_key_is_exchanged(self, key_file, signing_key):
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()) as post:
            assert access_token(_service_account(key_file)) == "access-1"
        assert post.call_args.args == (TOKEN_URL,)
        sent = post.call_args.kwargs["data"]
        assert sent["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer"
        claims = jwt.decode(
            sent["assertion"], signing_key.public_key(), algorithms=["RS256"], audience=TOKEN_URL
        )
        assert claims["iss"] == "reader@project.iam.test"
        assert claims["sub"] == "ada@example.test"
        assert claims["scope"] == SCOPE
        assert claims["exp"] - claims["iat"] == 3600

    def test_scopes_are_asked_for_together(self, key_file, signing_key):
        scopes = [SCOPE, "https://www.googleapis.com/auth/tasks.readonly"]
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()) as post:
            access_token(_service_account(key_file, scopes=scopes))
        claims = jwt.decode(
            post.call_args.kwargs["data"]["assertion"],
            signing_key.public_key(),
            algorithms=["RS256"],
            audience=TOKEN_URL,
        )
        assert claims["scope"] == " ".join(scopes)

    def test_each_account_read_as_has_its_own_token(self, key_file):
        answers = [_granted("access-ada"), _granted("access-bo")]
        with patch.object(oauth_grants.httpx, "post", side_effect=answers) as post:
            assert access_token(_service_account(key_file)) == "access-ada"
            assert (
                access_token(_service_account(key_file, subject="bo@example.test")) == "access-bo"
            )
            assert access_token(_service_account(key_file)) == "access-ada"
        assert post.call_count == 2

    def test_the_key_is_resolved_from_its_reference(self, key_file, monkeypatch):
        monkeypatch.setenv("TEST_GRANT_KEY_FILE", key_file)
        with patch.object(oauth_grants.httpx, "post", return_value=_granted()):
            assert access_token(_service_account("${env:TEST_GRANT_KEY_FILE}")) == "access-1"

    def test_a_domain_that_has_not_delegated_is_refused_with_googles_reason(self, key_file):
        refused = _issuer(
            401,
            error="unauthorized_client",
            error_description="Client is unauthorized to retrieve access tokens using this method.",
        )
        with (
            patch.object(oauth_grants.httpx, "post", return_value=refused),
            pytest.raises(CredentialRefused) as raised,
        ):
            access_token(_service_account(key_file))
        assert raised.value.reason.startswith("unauthorized_client: ")
        assert "reader@project.iam.test reading as ada@example.test" in str(raised.value)
        assert "PRIVATE KEY" not in str(raised.value)

    @pytest.mark.parametrize(
        ("key", "said"),
        [
            ("-----BEGIN PRIVATE KEY----- made-up", "is not JSON"),
            ('["made-up"]', "is not a JSON object"),
            ('{"client_email": "reader@project.iam.test"}', "has no private_key, token_uri"),
        ],
    )
    def test_a_key_that_is_not_one_is_refused_before_anything_is_sent(self, key, said):
        with (
            patch.object(oauth_grants.httpx, "post") as post,
            pytest.raises(ServiceAccountKeyInvalid, match=said) as raised,
        ):
            access_token(_service_account(key))
        post.assert_not_called()
        assert "made-up" not in str(raised.value)

    def test_a_private_key_that_cannot_sign_is_refused_without_quoting_it(self, key_file):
        broken = json.dumps({**json.loads(key_file), "private_key": "made-up-not-a-key"})
        with (
            patch.object(oauth_grants.httpx, "post") as post,
            pytest.raises(ServiceAccountKeyInvalid) as raised,
        ):
            access_token(_service_account(broken))
        post.assert_not_called()
        assert "made-up" not in str(raised.value)


class TestApplied:
    def test_a_call_is_sent_with_the_exchanged_token(self, key_file):
        for auth in (_refresh(), _service_account(key_file)):
            headers: dict = {}
            params: dict = {}
            with patch.object(oauth_grants.httpx, "post", return_value=_granted()):
                _apply_auth(auth, headers, params)
            assert headers == {"Authorization": "Bearer access-1"}
            assert params == {}
