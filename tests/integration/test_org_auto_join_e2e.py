# Copyright (c) 2026 Kenneth Stott
# Canary: 8d8e6c1a-0a99-48fc-89c9-e4db6a717274
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1285: an org that lets matching emails join admits a newly signed-in person with its
default role, and nobody else -- through a started server's HTTP API.

The org's administrator sets the join policy (an email rule and a default role). A person whose
email matches signs in for the first time and is a member holding that role, with no invitation.
A person whose email does not match signs in and belongs to no org.

Lands on the TEST instance only: one real server over a database the harness creates."""

from __future__ import annotations

import pytest

from tests.integration import tenancy_e2e_support as tenancy

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def server():
    boot = tenancy.boot_server()
    try:
        yield boot
    finally:
        boot.cleanup()


def test_a_matching_email_joins_with_the_default_role_and_another_does_not(server):
    operator = tenancy.operator_token(server)
    tenancy.register(server, operator, "founder", "founder@hq.test")
    alice_id = tenancy.register(server, operator, "alice", "alice@example.com")
    tenancy.register(server, operator, "mallory", "mallory@elsewhere.org")

    # The first platform administrator, seated in the deployment's own org (REQ-1290, REQ-1296).
    founder = tenancy.sign_in(server, "founder")
    claimed = tenancy.claim_platform_admin(server, founder)
    assert claimed["claimed"] is True, claimed
    org = claimed["org_id"]
    assert org == server.org_id

    # Before the policy exists, a signed-in person with a matching email belongs nowhere.
    assert tenancy.member_of(tenancy.me(server, tenancy.sign_in(server, "alice"))) == []

    tenancy.let_matching_emails_join(server, founder, org, "example.com")

    # Alice's next request finds her a member, holding the org's default role, uninvited.
    alice = tenancy.me(server, tenancy.sign_in(server, "alice"))
    assert alice["user_id"] == alice_id
    assert tenancy.member_of(alice) == [org], alice
    assert alice["active_org_id"] == org, alice
    assert tenancy.MEMBER_ROLE in tenancy.roles_of(alice), alice

    # The org's own member list, read by its administrator, says the same.
    admin = tenancy.org_admin(server, operator, founder, org)
    assert alice_id in tenancy.member_ids(server, admin, org)

    # Mallory's email is not the org's: she signs in and belongs to no org.
    mallory = tenancy.me(server, tenancy.sign_in(server, "mallory"))
    assert tenancy.member_of(mallory) == [], mallory
    assert tenancy.MEMBER_ROLE not in tenancy.roles_of(mallory), mallory
