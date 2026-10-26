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

from typing import Any

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


def with_read_grant(table: Any, acting_role: str) -> Any:
    """``table`` (a Table model being registered) with every column's grant list also naming the
    registrant's role and Viewer, beside what the registration names (REQ-2154)."""
    grant = default_read_grant(acting_role)
    return table.model_copy(
        update={
            "columns": [
                c.model_copy(
                    update={
                        "visible_to": [*c.visible_to, *(g for g in grant if g not in c.visible_to)]
                    }
                )
                for c in table.columns
            ]
        }
    )
