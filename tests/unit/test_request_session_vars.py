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
        )
        assert out == {"user_id": "u-7", "role": "analyst", "org_id": "acme"}

    def test_claim_user_id_is_overridden_by_identity(self):
        out = request_session_vars(_identity(claims={"X-Hasura-User-Id": 3}), None, {})
        assert out["user_id"] == "u-7"

    def test_anonymous_binds_no_user_id_but_headers_do(self):
        out = request_session_vars(
            _identity(user_id="anonymous"), "user", {"x-provisa-session-user-id": "2"}
        )
        assert out == {"user_id": "2", "role": "user"}

    def test_bool_claim_renders_as_sql_literal_text(self):
        assert (
            request_session_vars(_identity(claims={"x-hasura-is-admin": True}), None, {})[
                "is_admin"
            ]
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
            "region = current_setting('provisa.region')", session_vars_for(None)
        )
        assert sql == "region = NULL"

    def test_bound_value_is_quoted(self):
        token = set_session_vars({"user_id": "o'neil"})
        try:
            sql = _resolve_session_settings(
                "id = current_setting('provisa.user_id')", session_vars_for(None)
            )
        finally:
            reset_session_vars(token)
        assert sql == "id = 'o''neil'"
