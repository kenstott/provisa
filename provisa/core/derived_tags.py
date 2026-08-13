# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1e70cb-53d8-4f9e-a06b-91d2f4e8c7b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a table already IS, read off its registration (REQ-1443).

The registry has three kinds of tag. User tags are whatever an org decides to track. System tags
(``provisa.core.models.SYSTEM_TAGS``) are code-defined vocabulary a steward assigns. Derived tags
are neither assigned nor stored — they are computed here, every time, from the table's own
registration, which is what makes them safe to publish to an external catalog: a consumer reading
``data_quality`` off a table in DataHub is reading the same fact the scan runner reads.

The rules are deliberately narrow. Each one restates a decision already recorded somewhere else in
the model, so there is exactly one place to change it:

* ``fact`` / ``dimension`` — the star-schema role declared at registration (REQ-1320). The Cypher
  plane already turns this into a node label; the catalog gets the same fact under the same name.
* ``data_quality`` — a table on a checker source carrying a contract, which is precisely the shape
  :func:`provisa.dq.registration.apply_dq_registration` demands before it will derive the results
  schema. Its rows are check outcomes, so a consumer that treats them as business records is
  reading a scan log as if it were data.
"""

# Requirements: REQ-1320, REQ-1443

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from provisa.dq.contract import CHECKERS

# The modeling roles that name themselves as tags. Kept as a mapping rather than an identity check
# so a role that should NOT surface in the catalog can be dropped without touching the caller.
_ROLE_TAGS: dict[str, str] = {"fact": "fact", "dimension": "dimension"}


def derived_tags_for_table(table: Mapping[str, Any], source_type: str | None) -> tuple[str, ...]:
    """The derived tag ids that hold for one registered table, in registry order.

    ``table`` is a registered-table row or model dump — whatever the caller already has — and
    ``source_type`` is the type of the source it belongs to. Both are required rather than looked
    up here: the callers (the admin read path and the metadata-export builder) have already loaded
    the estate, and a second fetch would let this answer drift from the rows around it.
    """
    tags: list[str] = []
    role = table.get("modeling_role")
    if role in _ROLE_TAGS:
        tags.append(_ROLE_TAGS[role])
    if source_type in CHECKERS and table.get("dq_contract"):
        tags.append("data_quality")
    return tuple(tags)
