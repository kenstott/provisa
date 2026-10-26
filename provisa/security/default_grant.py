# Copyright (c) 2026 Kenneth Stott
# Canary: 591a783d-3e41-4cc4-96e6-2f3f00576d2f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The read a registration grants (REQ-2154): the role the registrant acts as, and the standard
Viewer role, written into each grant list where they can be seen and changed. Registration never
grants write. A grant to Viewer reaches every role built on it (security.inheritance.expand_grants),
which is how a column is granted to every role; removing Viewer is how it is made sensitive."""

# Requirements: REQ-2154, REQ-2155

from __future__ import annotations

from provisa.security.meta_role import META_PREFIX

#: The standard role every role builds on (provisa/core/standard_roles.yaml).
VIEWER_ROLE = "viewer"


def default_read_grant(acting_role: str) -> list[str]:
    """The grant list a registration writes for acting_role: that role and Viewer. A set of
    roles acting together (a meta-role) is not a grantee; its members are."""
    if acting_role.startswith(META_PREFIX):
        members = acting_role[len(META_PREFIX) :].split("+")
    else:
        members = [acting_role]
    return sorted({*members, VIEWER_ROLE})
