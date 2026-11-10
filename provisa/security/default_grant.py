# Copyright (c) 2026 Kenneth Stott
# Canary: 591a783d-3e41-4cc4-96e6-2f3f00576d2f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The read a registration grants (REQ-2154): to a column registered with no grant named, the role
the registrant acts as and the standard Viewer role, written into its grant list where they can be
seen and changed; a grant list named is used as given. Registration never grants write. A grant
to Viewer reaches every role built on it (security.inheritance.expand_grants), which is how a
column is granted to every role; removing Viewer is how it is made sensitive."""

# Requirements: REQ-2154, REQ-2155

from __future__ import annotations

from typing import Any

from provisa.security.meta_role import META_PREFIX
from provisa.security.rights import VIEWER_ROLE


def default_read_grant(acting_role: str) -> list[str]:
    """The grant list a registration writes for acting_role: that role and Viewer. A set of
    roles acting together (a meta-role) is not a grantee; its members are."""
    if acting_role.startswith(META_PREFIX):
        members = acting_role[len(META_PREFIX) :].split("+")
    else:
        members = [acting_role]
    return sorted({*members, VIEWER_ROLE})


def with_read_grant(table: Any, acting_role: str) -> Any:
    """``table`` (a Table model being registered) with each column registered with no grant named
    granted the registrant's role and Viewer; a column whose registration names a grant list keeps
    exactly that list (REQ-2154, amended 2026-11-10), as a metric, a command and an MCP tool do.
    Added to a named list, Viewer served every role a column granted to one (REQ-2226)."""
    grant = default_read_grant(acting_role)
    return table.model_copy(
        update={
            "columns": [
                c.model_copy(update={"visible_to": list(c.visible_to) if c.visible_to else grant})
                for c in table.columns
            ]
        }
    )
