# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which columns of one table a role is served, for tests that ask about a single column.

Read by the one rule (``security.visibility.visible_tables``, i.e. ``rights.column_served``,
REQ-2152), for a role reaching every domain, so a test asks only about the column's grant.
"""

from __future__ import annotations

from provisa.security.visibility import visible_tables


def visible_column_names(table: dict, role_id: str) -> set[str]:
    served = visible_tables([table], {"id": role_id, "domain_access": ["*"]})
    return {c["column_name"] for c in served[0]["columns"]} if served else set()


def is_column_visible(table: dict, column: str, role_id: str) -> bool:
    return column in visible_column_names(table, role_id)
