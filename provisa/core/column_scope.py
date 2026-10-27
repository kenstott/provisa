# Copyright (c) 2026 Kenneth Stott
# Canary: 9f4c2e60-7a18-4d53-b6e9-3c0d8a5f1b72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's ``scope``: how far beyond its table's domain it reaches (REQ-1959, REQ-2153).

- ``domain``: outside the domain the column does not exist; within it, roles that reach the
  domain read it as ``visible_to`` grants.
- ``listed``: the same, and every role of the organisation sees the table's and the column's
  names in the catalog, and nothing more.
- ``public``: the same, and also served to a role that does not reach the table's domain.

Who reads a column is decided by ``visible_to`` alone, and an empty list is no role (REQ-2154).

Who is served a column is decided in :func:`provisa.security.rights.column_served`. This module
holds the values and their validation, which every path that stores a column applies: the model
file, the admin API and the repository.
"""

# Requirements: REQ-1959, REQ-2153

from __future__ import annotations

SCOPE_DOMAIN = "domain"
SCOPE_LISTED = "listed"
SCOPE_PUBLIC = "public"
COLUMN_SCOPES: tuple[str, ...] = (SCOPE_DOMAIN, SCOPE_LISTED, SCOPE_PUBLIC)


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
