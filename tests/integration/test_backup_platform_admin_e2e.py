# Copyright (c) 2026 Kenneth Stott
# Canary: 19be9c01-86eb-42ab-9da2-2cce9b33b268
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1298: a backup platform administrator is made by an invitation into the deployment's own
org, never by a second bootstrap claim -- through a started server's HTTP API.

The platform administrator invites a person into the root org with the platform role. The person
redeems the invitation, is a member of root holding that role, and can do what only a platform
administrator can. Claiming the bootstrap slot a second time changes nothing: it stays the first
claimant's.

Lands on the TEST instance only: one real server over a database the harness creates."""

from __future__ import annotations

import pytest

from tests.integration import tenancy_e2e_support as tenancy

pytestmark = [pytest.mark.integration]

_PLATFORM_ADMIN = "platform_admin"


@pytest.fixture(scope="module")
def server():
    boot = tenancy.boot_server()
    try:
        yield boot
    finally:
        boot.cleanup()


def test_a_backup_platform_admin_comes_by_invitation_and_a_second_claim_is_refused(server):
    operator = tenancy.operator_token(server)
    founder_id = tenancy.register(server, operator, "founder", "founder@hq.test")
    tenancy.register(server, operator, "bystander", "bystander@hq.test")

    founder = tenancy.sign_in(server, "founder")
    claimed = tenancy.claim_platform_admin(server, founder)
    assert (claimed["claimed"], claimed["claimed_by"]) == (True, founder_id), claimed
    root = claimed["org_id"]

    # Only a platform administrator lists the deployment's orgs.
    bystander = tenancy.sign_in(server, "bystander")
    status, body = tenancy.call(server, "GET", "/admin/orgs/", token=bystander)
    assert status == 403, tenancy.said(server, "a non-admin lists orgs", status, body)

    # Step one: the platform administrator invites the backup into root with the platform role.
    status, invite = tenancy.call(
        server,
        "POST",
        "/admin/invites/",
        token=founder,
        org=root,  # issuing an invitation is an act in the org it invites into
        body={"org_id": root, "role_id": _PLATFORM_ADMIN, "email": "backup@hq.test"},
    )
    assert status == 200, tenancy.said(server, "invite the backup", status, invite)
    assert invite["role_id"] == _PLATFORM_ADMIN, invite

    # Step two: the person redeems it. With the basic provider that is the registration the
    # invitation link leads to; the account is created and the invitation spent in one call.
    status, registered = tenancy.call(
        server,
        "POST",
        "/auth/register",
        body={
            "username": "backup",
            "password": tenancy.PASSWORD,
            "email": "backup@hq.test",
            "invite_token": invite["token"],
        },
    )
    assert status == 200, tenancy.said(server, "redeem the invitation", status, registered)

    backup = tenancy.sign_in(server, "backup")
    identity = tenancy.me(server, backup, org=root)
    assert identity["user_id"] == registered["user_id"]
    assert tenancy.member_of(identity) == [root], identity
    assert _PLATFORM_ADMIN in tenancy.roles_of(identity), identity

    # The second administrator can act: the same call the bystander was refused.
    status, orgs = tenancy.call(server, "GET", "/admin/orgs/", token=backup)
    assert status == 200, tenancy.said(server, "the backup lists orgs", status, orgs)

    # The invitation is spent: it makes no third administrator.
    status, again = tenancy.call(
        server,
        "POST",
        "/auth/register",
        body={
            "username": "intruder",
            "password": tenancy.PASSWORD,
            "email": "intruder@hq.test",
            "invite_token": invite["token"],
        },
    )
    assert status == 400, tenancy.said(server, "reuse the invitation", status, again)

    # A second bootstrap claim takes nothing: the slot is the first claimant's, whoever asks.
    for who, token in (("the backup", backup), ("a bystander", bystander)):
        second = tenancy.claim_platform_admin(server, token)
        assert (second["claimed"], second["claimed_by"]) == (False, founder_id), (who, second)
    assert _PLATFORM_ADMIN not in tenancy.roles_of(tenancy.me(server, bystander))
