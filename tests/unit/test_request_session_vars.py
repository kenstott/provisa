# Copyright (c) 2026 Kenneth Stott
# Canary: 1d4f8b2e-7a3c-4e9d-b6a1-3c8e5f0d2b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1682: RLS session variables are bound per request from the acting identity."""

from types import SimpleNamespace

import pytest

from provisa.auth.middleware import request_session_vars
from provisa.core.request_context import (
    reset_session_vars,
    session_vars_for,
    set_session_vars,
)
from provisa.pgwire._pipeline import _resolve_session_settings


def _identity(user_id="u-7", claims=None):
    return SimpleNamespace(user_id=user_id, raw_claims=claims or {})


class TestRequestSessionVars:
    def test_user_id_role_and_hasura_claims(self):
        out = request_session_vars(
            _identity(
                claims={"X-Hasura-User-Id": 3, "x-hasura-org-id": "acme", "nested": {"a": 1}}
            ),
            "analyst",
            {},
            honor_session_headers=False,
        )
        assert out == {"user_id": "u-7", "role": "analyst", "org_id": "acme"}

    def test_claim_user_id_is_overridden_by_identity(self):
        out = request_session_vars(
            _identity(claims={"X-Hasura-User-Id": 3}), None, {}, honor_session_headers=False
        )
        assert out["user_id"] == "u-7"

    def test_anonymous_binds_no_user_id_but_headers_do(self):
        # No auth provider: the deployment has no claims, so the session headers are its context.
        out = request_session_vars(
            _identity(user_id="anonymous"),
            "user",
            {"x-provisa-session-user-id": "2"},
            honor_session_headers=True,
        )
        assert out == {"user_id": "2", "role": "user"}

    def test_with_an_auth_provider_a_header_never_replaces_a_claim(self):
        # A client header must not redirect a row filter that reads the verified identity.
        out = request_session_vars(
            _identity(claims={"tenant_id": "acme"}),
            "analyst",
            {"x-provisa-session-tenant-id": "beta", "x-provisa-session-region": "east"},
            honor_session_headers=False,
        )
        assert out["tenant_id"] == "acme"
        assert "region" not in out

    def test_bool_claim_renders_as_sql_literal_text(self):
        assert (
            request_session_vars(
                _identity(claims={"x-hasura-is-admin": True}), None, {}, honor_session_headers=False
            )["is_admin"]
            == "true"
        )


class TestSessionVarsFor:
    def test_request_overlays_role_constants(self):
        token = set_session_vars({"user_id": "2"})
        try:
            out = session_vars_for({"session_vars": {"region": "east", "user_id": "role-level"}})
        finally:
            reset_session_vars(token)
        assert out == {"region": "east", "user_id": "2"}

    def test_unbound_means_null_in_the_predicate(self):
        sql = _resolve_session_settings(
            "region = current_setting('provisa.region')", session_vars_for(None), "postgres"
        )
        assert sql == "region = NULL"

    def test_bound_value_is_quoted(self):
        token = set_session_vars({"user_id": "o'neil"})
        try:
            sql = _resolve_session_settings(
                "id = current_setting('provisa.user_id')", session_vars_for(None), "postgres"
            )
        finally:
            reset_session_vars(token)
        assert sql == "id = 'o''neil'"


# -- the node's region (REQ-1922) ------------------------------------------------------------------


@pytest.fixture
def _region():
    from provisa.core import process_region

    was = process_region._region
    yield process_region
    process_region._region = was


_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


def test_a_predicate_reads_the_answering_nodes_region(_region):
    from provisa.core.request_context import (
        reset_session_vars,
        session_vars_for,
        set_session_vars,
    )

    _region.bind_launch(_PLATFORM, requested="eu")
    token = set_session_vars({"region": "us", "team": "a"})  # a caller cannot name another
    try:
        assert session_vars_for({"session_vars": {"region": "us"}}) == {"region": "eu", "team": "a"}
    finally:
        reset_session_vars(token)


def test_with_no_platform_regions_the_name_stays_the_deployments(_region):
    from provisa.core.request_context import session_vars_for

    _region.bind_launch({}, requested=None)
    assert "region" not in session_vars_for(None)
    assert session_vars_for({"session_vars": {"region": "east"}}) == {"region": "east"}
