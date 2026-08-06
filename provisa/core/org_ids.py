# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4d17c8-53be-4f20-8e6a-2b0d5fc41e93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an org id may be (REQ-1309).

The id is chosen once, at org creation, and is then spent in two places that impose different
rules: it becomes the PostgreSQL schema name ``org_<id>`` and the DNS label an org is addressed by
(REQ-1276, REQ-1234). Both readings need the same answer, so the rule lives here rather than beside
either one — the creation-time validator rejects an id this pattern refuses, and the hostname
readers refuse to see an org in a label this pattern refuses.
"""

from __future__ import annotations

import re

# Requirements: REQ-1309

# A legal *unquoted* SQL identifier and a legal DNS label at once: a leading lowercase letter
# followed by lowercase letters and digits. The requirement names hyphens as well; they are excluded
# deliberately, because every org-schema DDL site interpolates the name unquoted (``SET search_path
# TO org_<id>``, ``CREATE ROLE role_<id>``, ``org_<id>_mv_cache``, the compiler's
# ``org_<id>__<catalog>`` catalog names) and a hyphen there is a syntax error surfacing during
# background provisioning — the exact failure mode REQ-1309 exists to prevent. 40 chars keeps
# ``org_<id>_mv_cache`` inside PostgreSQL's 63-byte identifier limit.
ORG_ID_PATTERN = re.compile(r"[a-z][a-z0-9]{1,39}")


def is_org_id(value: str | None) -> bool:  # REQ-1309
    """Whether ``value`` could be an org id at all."""
    return bool(value) and ORG_ID_PATTERN.fullmatch(value) is not None  # type: ignore[arg-type]
