# Copyright (c) 2026 Kenneth Stott
# Canary: 9f4c2e60-7a18-4d53-b6e9-3c0d8a5f1b72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's ``scope``: where it is served (REQ-1959).

- ``domain``: within the domains a role reaches, to whom ``visible_to`` says — an empty list is
  everyone.
- ``public``: the same, and also to a role that does not reach the table's domain.
- ``restricted``: only to whom ``visible_to`` names — an empty list is nobody.

Who is served a column is decided in :func:`provisa.security.rights.column_served`. This module
holds the values and their validation, which every path that stores a column applies: the model
file, the admin API and the repository.
"""

# Requirements: REQ-1959

from __future__ import annotations

SCOPE_DOMAIN = "domain"
SCOPE_PUBLIC = "public"
SCOPE_RESTRICTED = "restricted"
COLUMN_SCOPES: tuple[str, ...] = (SCOPE_DOMAIN, SCOPE_PUBLIC, SCOPE_RESTRICTED)


class ColumnScopeInvalid(ValueError):
    """A column's ``scope`` is not one of :data:`COLUMN_SCOPES`."""

    def __init__(self, column: str, scope: object) -> None:
        self.column = column
        self.scope = scope
        super().__init__(
            f"Column {column!r} has scope {scope!r}; a column's scope is one of "
            f"{', '.join(COLUMN_SCOPES)}"
        )


def require_column_scope(column: str, scope: object) -> str:
    """``scope`` when it is a column scope, else :class:`ColumnScopeInvalid` naming the column."""
    if scope not in COLUMN_SCOPES:
        raise ColumnScopeInvalid(column, scope)
    return str(scope)
