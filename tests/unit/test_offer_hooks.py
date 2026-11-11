# Copyright (c) 2026 Kenneth Stott
# Canary: 6fa43a0c-bd49-4485-93ae-e7b05ed6863d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, REQUESTS GO THROUGH HOOKS): where a request goes and how it is
signed, when an offer's data is public enough to be open, and a profile sanitized for its reader."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from provisa.core import offer_hooks, offer_profiles, offer_public
from provisa.core.repositories.data_product_offer import OfferRefused
from provisa.entitlements.lists import HeldEntitlements, HeldList
from provisa.profiler.governance import ColumnRule

_HOOK = offer_hooks.HookTarget(url="https://grants.example/requests", secret="${env:OFFER_KEY}")
_PROVISA = {"provider": "basic", "assignments_source": "provisa"}
_CLAIMS = {"provider": "oidc", "assignments_source": "claims"}


# -- where a request goes --------------------------------------------------------------------------


def test_with_no_hook_and_provisa_holding_access_a_request_goes_to_the_local_queue():
    assert offer_hooks.destination(None, _PROVISA) == offer_hooks.LOCAL
    # No one signs in: Provisa holds what there is.
    assert offer_hooks.destination(None, {}) == offer_hooks.LOCAL


def test_a_configured_hook_takes_every_request():
    assert offer_hooks.destination(_HOOK, _PROVISA) == _HOOK.url
    assert offer_hooks.destination(_HOOK, _CLAIMS) == _HOOK.url


def test_where_an_identity_provider_holds_access_a_request_needs_a_hook():
    with pytest.raises(OfferRefused) as refused:
        offer_hooks.destination(None, _CLAIMS)
    assert (refused.value.code, refused.value.status) == ("no_request_hook", 409)
    # The default source is claims.
    assert offer_hooks.held_by_idp({"provider": "oidc"}) is True


def test_the_hook_is_a_url_and_its_secret_as_the_operator_entered_it():
    settings = {"offer_hook": {"url": _HOOK.url, "secret": "${secret:GRANTS_HOOK}"}}
    assert offer_hooks.target_of(settings) == offer_hooks.HookTarget(
        _HOOK.url, "${secret:GRANTS_HOOK}"
    )
    # Kept as entered, never rewritten into a reference: resolved at use like any credential.
    assert offer_hooks.target_of(
        {"offer_hook": {"url": _HOOK.url, "secret": "hunter2"}}
    ) == offer_hooks.HookTarget(_HOOK.url, "hunter2")
    assert offer_hooks.target_of({}) is None
    with pytest.raises(ValueError, match="secret is required"):
        offer_hooks.target_of({"offer_hook": {"url": _HOOK.url, "secret": ""}})
    with pytest.raises(ValueError, match="http"):
        offer_hooks.target_of({"offer_hook": {"url": "ftp://x", "secret": "${env:K}"}})


def test_a_hooks_secret_is_resolved_at_use_as_entered(monkeypatch):
    from provisa.core import secrets_store

    seen = []

    async def resolved(value):
        seen.append(value)
        return "s3cret"

    monkeypatch.setattr(secrets_store, "resolved_credential", resolved)
    target = offer_hooks.HookTarget(_HOOK.url, "${env:HOOK_SECRET}")
    assert asyncio.run(offer_hooks.resolved_secret(target)) == "s3cret"
    assert seen == ["${env:HOOK_SECRET}"]


def test_a_signature_verifies_only_under_its_secret_and_its_body():
    signature = offer_hooks.sign(b'{"a":1}', "k1")
    assert signature.startswith("sha256=")
    assert offer_hooks.verified(b'{"a":1}', signature, "k1") is True
    assert offer_hooks.verified(b'{"a":2}', signature, "k1") is False
    assert offer_hooks.verified(b'{"a":1}', signature, "k2") is False
    assert offer_hooks.verified(b'{"a":1}', None, "k1") is False


def test_a_request_is_posted_signed_with_its_id_as_the_idempotency_key(monkeypatch):
    monkeypatch.setenv("OFFER_KEY", "s3cret")
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(202)

    real = httpx.AsyncClient
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(answer), **kw)
    )
    body = {"id": "r1", "kind": "bid", "requester": "u1"}
    asyncio.run(offer_hooks.deliver(_HOOK, body))
    (posted,) = seen
    assert str(posted.url) == _HOOK.url
    assert posted.headers["Idempotency-Key"] == "r1"
    assert json.loads(posted.content) == body
    assert offer_hooks.verified(
        posted.content, posted.headers[offer_hooks.SIGNATURE_HEADER], "s3cret"
    )


def test_a_hook_that_refuses_the_request_fails_it(monkeypatch):
    monkeypatch.setenv("OFFER_KEY", "s3cret")
    real = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kw: real(
            transport=httpx.MockTransport(lambda _r: httpx.Response(500, text="down")), **kw
        ),
    )
    with pytest.raises(offer_hooks.HookFailed, match="answered 500"):
        asyncio.run(offer_hooks.deliver(_HOOK, {"id": "r1"}))


# -- an open offer is public data ------------------------------------------------------------------


def _public(**more) -> list[str]:
    args = {
        "role_id": "viewer",
        "role_chains": {"viewer": ["viewer"], "analyst": ["analyst", "viewer"]},
        "roles": {"viewer": {}, "analyst": {}},
        "role_defaults": {},
        "held": {},
        "idp": {},
        "datasets": ["sales.orders", "sales.customers"],
        "unread": [],
    }
    return offer_public.not_public(**{**args, **more})


def test_a_role_every_role_inherits_reading_every_dataset_is_public():
    assert _public() == []
    assert _public(role_defaults={"tables": ["*"]}) == []


def test_a_role_not_every_member_holds_is_not_public():
    found = _public(
        roles={"viewer": {}, "analyst": {}, "auditor": {}},
        role_chains={"viewer": ["viewer"], "analyst": ["analyst", "viewer"]},
    )
    assert found == ["not every member holds role 'viewer': auditor does not inherit it"]


def test_a_dataset_the_role_does_not_read_is_named():
    assert _public(unread=["sales.customers"]) == ["role 'viewer' reads nothing of sales.customers"]


def test_a_narrowed_list_anywhere_is_named():
    held = {
        "u1": HeldEntitlements(lists=(HeldList("viewer", "tables", ("sales.orders",)),)),
        "u2": HeldEntitlements(lists=(HeldList("analyst", "tables", ("sales.orders",)),)),
    }
    found = _public(
        role_defaults={"tables": ["sales.orders"]},
        held=held,
        idp={"u3": {"viewer": {"tables": ["sales.orders"]}}},
    )
    assert found == [
        "role 'viewer''s default tables list leaves out sales.customers",
        "u1's grant of 'viewer' leaves out sales.customers",
        "u3's identity provider narrows 'viewer' past sales.customers",
    ]


# -- a profile shown is sanitized ------------------------------------------------------------------


def _rule(name: str, *, masks: tuple = (), pii: bool = False, served=("viewer",)) -> ColumnRule:
    return ColumnRule(
        physical=name, exposed=name, served_to=frozenset(served), masks=masks, pii=pii
    )


def _column(name: str) -> dict:
    return {
        "run_id": "r",
        "column_name": name,
        "data_type": "text",
        "null_share": 0.1,
        "distinct_count": 40,
        "distinct_ratio": 0.4,
        "min_value": "a",
        "max_value": "z",
    }


def test_a_masked_or_pii_column_shows_its_type_null_rate_and_distinct_count_and_no_value():
    masked = ({"to": None, "mask": {"type": "constant", "value": None}},)
    rules = [
        _rule("id"),
        _rule("email", masks=masked),
        _rule("ssn", pii=True),
        _rule("x", served=()),
    ]
    results = {
        "runs": [{"run_id": "r"}],
        "columns": [_column(c) for c in ("id", "email", "ssn", "x")],
        "top_values": [
            {"run_id": "r", "column_name": c, "kind": "frequent", "value": "v"}
            for c in ("id", "email", "ssn")
        ],
    }
    safe = offer_profiles.sanitized(results, rules, frozenset({"viewer"}))
    columns = {r["column_name"]: r for r in safe["columns"]}
    assert set(columns) == {"id", "email", "ssn"}  # x is not served to the reader
    assert columns["id"]["min_value"] == "a"
    for restricted in ("email", "ssn"):
        row = columns[restricted]
        assert (row["data_type"], row["null_share"], row["distinct_count"]) == ("text", 0.1, 40)
        assert (row["min_value"], row["max_value"]) == (None, None)
    assert [r["column_name"] for r in safe["top_values"]] == ["id"]
    assert safe["runs"] == [{"run_id": "r"}]


def test_a_dataset_with_no_profiler_has_no_profile():
    class Conn:
        async def execute_core(self, _query):
            return SimpleNamespace(
                fetchone=lambda: SimpleNamespace(table_name="t", profiler_source_id=None)
            )

    assert asyncio.run(offer_profiles.latest(Conn(), None, 1, frozenset({"viewer"}))) is None
