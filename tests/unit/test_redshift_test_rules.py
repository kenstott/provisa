# Copyright (c) 2026 Kenneth Stott
# Canary: 9ea3755c-31c6-4833-90c7-79ee1965b72b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Redshift fixture removes only the inbound rules it added: the ones on its port carrying
its description, never a rule anyone else made."""

from __future__ import annotations

from tests.integration import redshift_cluster as rc


class _Ec2:
    def __init__(self, permissions):
        self.permissions = permissions
        self.revoked: list[dict] = []

    def describe_security_groups(self, GroupIds):  # noqa: N803 - boto3's parameter name
        return {"SecurityGroups": [{"GroupId": GroupIds[0], "IpPermissions": self.permissions}]}

    def revoke_security_group_ingress(self, GroupId, IpPermissions):  # noqa: N803
        self.revoked.append({"GroupId": GroupId, "IpPermissions": IpPermissions})


def _perm(port, *ranges):
    return {"IpProtocol": "tcp", "FromPort": port, "ToPort": port, "IpRanges": list(ranges)}


def test_only_its_own_rules_on_its_port_are_found():
    ec2 = _Ec2(
        [
            _perm(
                5439,
                {"CidrIp": "1.1.1.1/32", "Description": "provisa-e2e"},
                {"CidrIp": "2.2.2.2/32", "Description": "someone else"},
                {"CidrIp": "3.3.3.3/32"},
            ),
            _perm(22, {"CidrIp": "4.4.4.4/32", "Description": "provisa-e2e"}),
        ]
    )
    assert rc._our_rules(ec2, "sg-1", 5439) == ["1.1.1.1/32"]


def test_revoking_names_exactly_the_cidrs_given_and_nothing_when_none():
    ec2 = _Ec2([])
    rc._revoke(ec2, "sg-1", 5439, [])
    assert ec2.revoked == []
    rc._revoke(ec2, "sg-1", 5439, ["1.1.1.1/32"])
    (call,) = ec2.revoked
    assert call["GroupId"] == "sg-1"
    assert call["IpPermissions"] == [_perm(5439, {"CidrIp": "1.1.1.1/32"})]
