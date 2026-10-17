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
    ORG_VAR,
    current_org,
    reset_session_vars,
    session_vars_for,
    set_session_vars,
)
from provisa.pgwire._pipeline import _resolve_session_settings


def _identity(user_id="u-7", claims=None, org=None):
    return SimpleNamespace(user_id=user_id, raw_claims=claims or {}, org_attributes=org or {})


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
        assert out == {
            "user_id": "u-7",
            "role": "analyst",
            "hasura_user_id": "3",
            "hasura_org_id": "acme",
        }

    def test_a_hasura_user_id_claim_never_replaces_the_identity(self):
        """The app's own user key, which Hasura read from x-hasura-user-id, is its own variable:
        provisa.user_id stays the verified identity."""
        out = request_session_vars(
            _identity(user_id="auth0|abc", claims={"X-Hasura-User-Id": 3}),
            None,
            {},
            honor_session_headers=False,
        )
        assert out["user_id"] == "auth0|abc" and out["hasura_user_id"] == "3"

    def test_hasura_claims_inside_the_hasura_claims_object_are_read(self):
        """Hasura reads its session variables from the claims object it names (by default
        https://hasura.io/jwt/claims); each is read from there, as Hasura read it."""
        claims = {
            "sub": "auth0|abc",
            "https://hasura.io/jwt/claims": {
                "x-hasura-user-id": "7",
                "x-hasura-default-role": "customer",
                "x-hasura-allowed-roles": ["customer"],
            },
        }
        out = request_session_vars(
            _identity(user_id="auth0|abc", claims=claims),
            "customer",
            {},
            honor_session_headers=False,
        )
        assert out["hasura_user_id"] == "7" and out["hasura_default_role"] == "customer"
        assert out["user_id"] == "auth0|abc"
        assert "hasura_allowed_roles" not in out  # a list is no value a predicate compares

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
            )["hasura_is_admin"]
            == "true"
        )


class TestSessionVarsFor:
    def test_request_overlays_role_constants(self):
        token = set_session_vars({"user_id": "2"})
        try:
            out = session_vars_for({"session_vars": {"region": "east", "user_id": "role-level"}})
        finally:
            reset_session_vars(token)
        # The org the test runs in rides along (conftest binds it); a caller cannot set it.
        assert out == {"region": "east", "user_id": "2", ORG_VAR: current_org.get()}

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

    def test_an_imported_rule_reads_the_app_keys_rows_and_the_identity_is_unchanged(self):
        """A token whose subject is not the app's user key: the imported Hasura rule reads the
        rows of the key the token's x-hasura-user-id claim names, and provisa.user_id is the
        subject."""
        import duckdb

        from provisa.hasura_v2.mapper import _predicate

        rule = _predicate({"id": {"_eq": "X-Hasura-User-Id"}})
        claims = {"sub": "auth0|ana", "https://hasura.io/jwt/claims": {"x-hasura-user-id": "1"}}
        bound = request_session_vars(
            _identity(user_id="auth0|ana", claims=claims),
            "customer",
            {},
            honor_session_headers=False,
        )
        token = set_session_vars(bound)
        try:
            vars_ = session_vars_for(None)
            sql = _resolve_session_settings(rule, vars_, "postgres")
            who = _resolve_session_settings("current_setting('provisa.user_id')", vars_, "postgres")
        finally:
            reset_session_vars(token)
        con = duckdb.connect()
        con.execute("CREATE TABLE customers (id INTEGER, name TEXT)")
        con.execute("INSERT INTO customers VALUES (1, 'Ana'), (2, 'Ben')")
        rows = con.execute(f"SELECT name FROM customers WHERE {sql}").fetchall()
        assert rows == [("Ana",)]
        assert who == "'auth0|ana'"


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
        assert session_vars_for({"session_vars": {"region": "us"}}) == {
            "region": "eu",
            "team": "a",
            ORG_VAR: current_org.get(),
        }
    finally:
        reset_session_vars(token)


def test_with_no_platform_regions_the_name_stays_the_deployments(_region):
    from provisa.core.request_context import session_vars_for

    _region.bind_launch({}, requested=None)
    assert "region" not in session_vars_for(None)
    assert session_vars_for({"session_vars": {"region": "east"}}) == {
        "region": "east",
        ORG_VAR: current_org.get(),
    }
