# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sources are read together on one direct connection only when they are the same database under
the same login (REQ-863, REQ-028): two logins on one database are two grants, and a statement over
both run on one source's connection would read the other's tables under its credentials."""

# Requirements: REQ-863, REQ-028

from provisa.federation.colocation import colocation_key
from provisa.transpiler.router import Route, decide_route


def _route(key_a: str, key_b: str):
    return decide_route(
        sources={"a", "b"},
        source_types={"a": "postgresql", "b": "postgresql"},
        source_dialects={"a": "postgres", "b": "postgres"},
        source_dsns={"a": key_a, "b": key_b},
        operator_floor={},
    )


def _key(user: str = "app", password: str = "s3cret", extras: dict | None = None) -> str:
    return colocation_key("db.local", 5432, "sales", user, password, extras)


def test_one_database_under_one_login_is_one_direct_route():
    decision = _route(_key(), _key())
    assert decision.route == Route.DIRECT
    assert "colocated" in decision.reason


def test_one_database_under_two_users_is_the_engine_s():
    assert _route(_key(user="app"), _key(user="reporting")).route == Route.ENGINE


def test_one_user_with_another_password_is_the_engine_s():
    assert _route(_key(password="s3cret"), _key(password="other")).route == Route.ENGINE


def test_one_user_with_another_key_or_token_is_the_engine_s():
    a = _key(extras={"private_key": "k1"})
    b = _key(extras={"private_key": "k2"})
    assert _route(a, b).route == Route.ENGINE


def test_the_key_never_holds_the_secret():
    key = _key(password="s3cret", extras={"token": "t0ken"})
    assert "s3cret" not in key and "t0ken" not in key
