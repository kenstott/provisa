# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3e9c15-2d68-4b04-9f57-c1e8b6d0a243
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What of the catalog an admin-API caller is answered (REQ-1958).

The admin GraphQL catalog reads (``tables``, ``relationships``, ``domains``, ``sources``,
``metrics``) used to answer every signed-in caller with the whole registered catalog. They answer
by the caller's rights and reach instead:

- A caller holding a catalog-administration right — ``table_registration``, or a right that
  governs how columns are hidden (REQ-1944) — is answered the catalog of the domains its roles
  reach.
- Everyone else is answered what the role the request acts as is SERVED: the tables and columns
  of its compiled context, from the one narrowing the Flight and MCP catalogs use
  (``flight.catalog.role_visibility``). There is no second notion of what a role may see.
- Grant lists and mask settings need ``view_governance`` (REQ-1134), for an administrator too.

All of it is decided by rights and domain reach, never by a role's id (REQ-1337). A role with no
data surface (a control-plane role, REQ-1327) is served nothing. A deployment with no auth
provider narrows nothing, as its other gates do.
"""

# Requirements: REQ-1958, REQ-1134, REQ-1944, REQ-1337, REQ-1327

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# The rights that administer the catalog: registering tables, or governing how a table's columns
# are hidden (REQ-1944). Their holder reads the catalog of the domains it reaches.
CATALOG_ADMIN_RIGHTS = frozenset(
    {"table_registration", "masking_config", "column_grant", "access_config", "sensitive_data"}
)
VIEW_GOVERNANCE = "view_governance"


@dataclass(frozen=True)
class CatalogScope:
    """One caller's view of the catalog."""

    # Nothing is narrowed: a deployment that authenticates nobody.
    whole: bool
    # The caller holds a catalog-administration right.
    admin: bool
    # The domains the roles CARRYING such a right reach; None = every domain.
    admin_reach: frozenset[str] | None
    # The domains any of the caller's roles reaches; None = every domain.
    reach: frozenset[str] | None
    # table id → columns the acting role is served.
    served: dict[int, set[str]]
    # The caller may read grant lists and mask settings (REQ-1134).
    governance: bool
    # The role the request acts as (a meta-role for a set), None when there is none.
    role: str | None

    def lists_table(self, table_id: int, domain_id: str) -> bool:
        if self.whole or table_id in self.served:
            return True
        return self._administers(domain_id)

    def _administers(self, domain_id: str) -> bool:
        return self.admin and (self.admin_reach is None or domain_id in self.admin_reach)

    def columns(self, table_id: int, domain_id: str) -> set[str] | None:
        """The columns of a listed table the caller is answered; None = all of them."""
        if self.whole or self._administers(domain_id):
            return None
        return self.served.get(table_id, set())

    def lists_column(self, table_id: int, domain_id: str, column: str | None) -> bool:
        if not self.lists_table(table_id, domain_id):
            return False
        columns = self.columns(table_id, domain_id)
        return columns is None or column is None or column in columns

    def lists_domain(self, domain_id: str, with_listed_table: bool) -> bool:
        """A domain the caller's roles reach, or one holding a table the caller is answered."""
        if self.whole or with_listed_table:
            return True
        return self.reach is None or domain_id in self.reach


def catalog_scope(info: Any) -> CatalogScope:
    """The scope of the caller behind a GraphQL ``info``."""
    from provisa.api.admin.capabilities import (
        _ANONYMOUS,
        _identity_from_info,
        _resolved_capabilities,
        allowed_domains_request,
    )
    from provisa.api.app import state
    from provisa.api.flight.catalog import role_visibility

    identity = _identity_from_info(info)
    if identity is None or getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        return CatalogScope(
            whole=True,
            admin=True,
            admin_reach=None,
            reach=None,
            served={},
            governance=True,
            role=None,
        )
    request = info.context["request"] if isinstance(info.context, dict) else info.context.request
    caps = _resolved_capabilities(identity, state)
    role = getattr(request.state, "role", None)
    admin = bool(caps & CATALOG_ADMIN_RIGHTS)
    return CatalogScope(
        whole=False,
        admin=admin,
        admin_reach=_admin_reach(identity, state) if admin else frozenset(),
        reach=allowed_domains_request(request),
        served=role_visibility(state, role) if role else {},
        governance=VIEW_GOVERNANCE in caps,
        role=role,
    )


def _admin_reach(identity: Any, state: Any) -> frozenset[str] | None:
    """The domains the caller's roles carrying a catalog-administration right reach (the scope
    REQ-1944 gives those rights); None when domains gate nothing for it."""
    from provisa.api.admin.capabilities import _rights_scope
    from provisa.core import domain_policy

    if domain_policy.single_domain():
        return None
    return _rights_scope(identity, state, tuple(sorted(CATALOG_ADMIN_RIGHTS)))
