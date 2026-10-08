# Copyright (c) 2026 Kenneth Stott
# Canary: e0f8fcc9-92fa-48cf-bb36-a7db9300a444
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1306: a person who leaves an org is gone from it on both planes, and signing in again does
not put them back -- through a started server's HTTP API.

The org admits anyone at its email domain. A person at that domain is a member; they leave. Their
membership and their role are gone, the org's member list no longer has them, and their next
sign-in -- which the org's rule still matches -- does not re-join them.

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


def test_leaving_removes_the_person_and_their_next_sign_in_does_not_rejoin_them(server):
    operator = tenancy.operator_token(server)
    tenancy.register(server, operator, "founder", "founder@hq.test")
    alice_id = tenancy.register(server, operator, "alice", "alice@example.com")

    founder = tenancy.sign_in(server, "founder")
    org = tenancy.claim_platform_admin(server, founder)["org_id"]
    tenancy.let_matching_emails_join(server, founder, org, "example.com")
    admin = tenancy.org_admin(server, operator, founder, org)

    alice = tenancy.sign_in(server, "alice")
    joined = tenancy.me(server, alice)
    assert tenancy.member_of(joined) == [org], joined
    assert tenancy.MEMBER_ROLE in tenancy.roles_of(joined), joined
    assert alice_id in tenancy.member_ids(server, admin, org)

    # She leaves, of her own accord.
    status, left = tenancy.call(server, "POST", f"/admin/orgs/{org}/leave", token=alice)
    assert status == 200, tenancy.said(server, "leave the org", status, left)
    assert left == {"left": org, "user_id": alice_id}, left

    # Gone on both planes: no membership (platform plane), no role (the org's own plane), and
    # the org's member list no longer has her.
    after = tenancy.me(server, alice)
    assert tenancy.member_of(after) == [], after
    assert tenancy.MEMBER_ROLE not in tenancy.roles_of(after), after
    assert alice_id not in tenancy.member_ids(server, admin, org)

    # The org's rule still matches her email. A fresh sign-in does not undo the departure.
    again = tenancy.me(server, tenancy.sign_in(server, "alice"))
    assert tenancy.member_of(again) == [], again
    assert tenancy.MEMBER_ROLE not in tenancy.roles_of(again), again
    assert alice_id not in tenancy.member_ids(server, admin, org)

    # Leaving twice is refused by name: there is no membership to leave.
    status, body = tenancy.call(server, "POST", f"/admin/orgs/{org}/leave", token=alice)
    assert status == 404, tenancy.said(server, "leave again", status, body)
