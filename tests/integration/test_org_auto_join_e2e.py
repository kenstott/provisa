# Copyright (c) 2026 Kenneth Stott
# Canary: 8d8e6c1a-0a99-48fc-89c9-e4db6a717274
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1285, REQ-1261: an org that lets matching emails join admits a newly signed-in person with
its default role, and nobody else -- through a started server's HTTP API.

The org's administrator sets the join policy (an email rule and a default role). A person whose
verified email matches signs in for the first time and is a member holding that role, with no
invitation. A person whose email does not match signs in and belongs to no org, and so does one
whose matching address nothing verified: an address an identity provider has not verified, or one
typed into a local account.

Lands on the TEST instance only: one real server over a database the harness creates."""

from __future__ import annotations

import pytest

from tests.integration import tenancy_e2e_support as tenancy

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def issuer():
    idp = tenancy.Issuer()
    try:
        yield idp
    finally:
        idp.close()


@pytest.fixture(scope="module")
def server(issuer):
    """A server that signs people in with the issuer's tokens."""
    boot = tenancy.boot_server(issuer)
    try:
        yield boot
    finally:
        boot.cleanup()


@pytest.fixture(scope="module")
def local_accounts_server():
    """A server whose people are local accounts (the basic provider)."""
    boot = tenancy.boot_server()
    try:
        yield boot
    finally:
        boot.cleanup()


def test_a_matching_verified_email_joins_with_the_default_role_and_no_other_does(server, issuer):
    # The first platform administrator, seated in the deployment's own org (REQ-1290, REQ-1296).
    founder = issuer.token("founder", "founder@hq.test", email_verified=True)
    claimed = tenancy.claim_platform_admin(server, founder)
    assert claimed["claimed"] is True, claimed
    org = claimed["org_id"]
    assert org == server.org_id

    # Before the policy exists, a signed-in person with a matching email belongs nowhere.
    alice_token = issuer.token("alice", "alice@example.com", email_verified=True)
    assert tenancy.member_of(tenancy.me(server, alice_token)) == []

    tenancy.let_matching_emails_join(server, founder, org, "example.com")

    # Alice's next request finds her a member, holding the org's default role, uninvited.
    alice = tenancy.me(server, alice_token)
    assert alice["user_id"] == "alice"
    assert tenancy.member_of(alice) == [org], alice
    # Acting in the org she now belongs to, she holds its default role.
    in_org = tenancy.me(server, alice_token, org=org)
    assert in_org["active_org_id"] == org, in_org
    assert tenancy.MEMBER_ROLE in tenancy.roles_of(in_org), in_org

    # Mallory's email is not the org's: she signs in and belongs to no org.
    mallory = tenancy.me(
        server, issuer.token("mallory", "mallory@elsewhere.org", email_verified=True)
    )
    assert tenancy.member_of(mallory) == [], mallory
    assert tenancy.MEMBER_ROLE not in tenancy.roles_of(mallory), mallory

    # Eve's token names an address at the org's domain that her identity provider has not
    # verified. It joins her to nothing, offers her nothing, and the org does not admit her.
    eve_token = issuer.token("eve", "eve@example.com", email_verified=False)
    eve = tenancy.me(server, eve_token)
    assert tenancy.member_of(eve) == [], eve
    status, offers = tenancy.call(server, "GET", "/auth/auto-join-offers", token=eve_token)
    assert (status, offers) == (200, {"offers": []}), tenancy.said(server, "offers", status, offers)
    status, body = tenancy.call(server, "GET", "/auth/me", token=eve_token, org=org)
    assert status == 403, tenancy.said(server, "act in the org unjoined", status, body)

    # The org's own member list, read by its administrator, says the same.
    tenancy.grant_org_admin(server, founder, org, "org-admin")
    admin = issuer.token("org-admin", "org-admin@hq.test", email_verified=True)
    members = tenancy.member_ids(server, admin, org)
    assert "alice" in members, members
    assert {"mallory", "eve"}.isdisjoint(members), members


def test_a_local_accounts_typed_address_joins_nothing(local_accounts_server):
    """REQ-1261: a local account's email is whatever was typed when the account was made. An
    org's rule naming its domain does not admit it; that person joins by an invitation link."""
    server = local_accounts_server
    operator = tenancy.operator_token(server)
    tenancy.register(server, operator, "founder", "founder@hq.test")
    typed_id = tenancy.register(server, operator, "typed", "typed@example.com")

    founder = tenancy.sign_in(server, "founder")
    org = tenancy.claim_platform_admin(server, founder)["org_id"]
    tenancy.let_matching_emails_join(server, founder, org, "example.com")

    typed_token = tenancy.sign_in(server, "typed")
    typed = tenancy.me(server, typed_token)
    assert typed["user_id"] == typed_id
    assert typed["email"] == "typed@example.com", typed
    assert tenancy.member_of(typed) == [], typed
    status, offers = tenancy.call(server, "GET", "/auth/auto-join-offers", token=typed_token)
    assert (status, offers) == (200, {"offers": []}), tenancy.said(server, "offers", status, offers)
    status, body = tenancy.call(server, "GET", "/auth/me", token=typed_token, org=org)
    assert status == 403, tenancy.said(server, "act in the org unjoined", status, body)
    # An invitation addressed to the typed address is not shown to its holder either: the
    # answer would carry the invitation's token (REQ-1287).
    status, invite = tenancy.call(
        server,
        "POST",
        "/admin/invites/",
        token=founder,
        org=org,
        body={"org_id": org, "role_id": tenancy.MEMBER_ROLE, "email": "typed@example.com"},
    )
    assert status == 200, tenancy.said(server, "invite the address", status, invite)
    status, mine = tenancy.call(server, "GET", "/auth/my-invites", token=typed_token)
    assert (status, mine) == (200, {"invites": []}), tenancy.said(server, "inbox", status, mine)

    # The link itself still redeems: the token is the proof, not the address.
    status, redeemed = tenancy.call(
        server, "POST", "/auth/redeem-invite", token=typed_token, body={"token": invite["token"]}
    )
    assert status == 200, tenancy.said(server, "redeem the link", status, redeemed)
    assert tenancy.member_of(tenancy.me(server, typed_token)) == [org]
    assert tenancy.MEMBER_ROLE in tenancy.roles_of(tenancy.me(server, typed_token, org=org))


def test_a_signed_in_person_without_user_management_creates_no_account(local_accounts_server):
    """Creating an account for someone else is managing users. A signed-in person who holds no
    such right is refused, and the account does not exist; the operator, who holds it, is not."""
    server = local_accounts_server
    operator = tenancy.operator_token(server)
    tenancy.register(server, operator, "bystander", "bystander@hq.test")
    bystander = tenancy.sign_in(server, "bystander")

    status, body = tenancy.call(
        server,
        "POST",
        "/auth/register",
        token=bystander,
        body={"username": "sock-puppet", "password": tenancy.PASSWORD, "email": "s@example.com"},
    )
    assert status == 403, tenancy.said(server, "register while signed in", status, body)
    assert body["code"] == "auth.missing_capability", body
    status, body = tenancy.call(
        server,
        "POST",
        "/auth/login",
        body={"username": "sock-puppet", "password": tenancy.PASSWORD},
    )
    assert status == 401, tenancy.said(server, "sign in as the refused account", status, body)
