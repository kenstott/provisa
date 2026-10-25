# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3e9c15-2d68-4b04-9f57-c1e8b6d0a243
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What of the catalog an admin-API caller is answered (REQ-1958, REQ-2152).

The admin GraphQL catalog reads (``tables``, ``relationships``, ``domains``, ``sources``,
``metrics``) used to answer every signed-in caller with the whole registered catalog. They answer
by the caller's grants instead:

- Every caller is answered what the role the request acts as is SERVED: the tables and columns
  of its compiled context, from the one narrowing the Flight and MCP catalogs use
  (``flight.catalog.role_visibility``). There is no second notion of what a role may see.
- A table in no compiled context -- a draft, or one saved since the last schema build -- has its
  stored columns tested by the rule the compile applies (``security.visibility.visible_tables``,
  i.e. ``rights.column_served``), so a table is answered from the moment it is saved.
- No capability widens it (REQ-2152): ``table_registration`` or a right that governs how columns
  are hidden (REQ-1944) opens a feature, never more of the catalog.
- Grant lists and mask settings need ``view_governance`` (REQ-1134).

All of it is decided by grants and domain reach, never by a role's id (REQ-1337). A role with no
data surface (a control-plane role, REQ-1327) is served nothing. A deployment with no auth
provider enforces no right, as its other gates do, but answers the catalog of the roles the
request names (X-Provisa-Role).
"""

# Requirements: REQ-1958, REQ-1134, REQ-1944, REQ-1337, REQ-1327, REQ-2152

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any

VIEW_GOVERNANCE = "view_governance"


@dataclass(frozen=True)
class CatalogScope:
    """One caller's view of the catalog."""

    # Nothing is narrowed: a deployment that authenticates nobody.
    whole: bool
    # The domains any of the caller's roles reaches; None = every domain.
    reach: frozenset[str] | None
    # table id → columns the acting role is served.
    served: dict[int, set[str]]
    # The caller may read grant lists and mask settings (REQ-1134).
    governance: bool
    # The role the request acts as (a meta-role for a set), None when there is none.
    role: str | None
    # The ids of the tables the last schema build compiled; any other is judged by its stored
    # columns (:meth:`answered_columns`).
    compiled: frozenset[int] = frozenset()
    # The roles the request acts as, each as ``{"id", "domain_access"}`` (a meta-role's members).
    subjects: tuple[dict, ...] = ()

    def stored_columns(self, table: dict) -> set[str]:
        """The stored columns of ``table`` (``id``, ``domain_id``, ``columns`` of
        ``column_name``/``visible_to``/``scope``) the acting role is served, by the rule the
        compile applies -- for a table no context compiled yet (REQ-2152)."""
        from provisa.security.visibility import visible_tables

        served: set[str] = set()
        for subject in self.subjects:
            for visible in visible_tables([table], subject):
                served.update(c["column_name"] for c in visible["columns"])
        return served

    def answered_columns(self, table: dict) -> set[str] | None:
        """The columns of ``table`` (as :meth:`stored_columns` takes it) the caller is answered;
        None = all of them. A table the last build compiled is answered from the acting role's
        context; one it did not -- a draft, or one saved since -- from its stored columns."""
        if self.whole:
            return None
        if table["id"] in self.compiled:
            return self.served.get(table["id"], set())
        return self.stored_columns(table)

    def lists_table(self, table_id: int) -> bool:
        return self.whole or table_id in self.served

    def columns(self, table_id: int) -> set[str] | None:
        """The columns of a listed table the caller is answered; None = all of them."""
        if self.whole:
            return None
        return self.served.get(table_id, set())

    def lists_column(self, table_id: int, column: str | None) -> bool:
        if not self.lists_table(table_id):
            return False
        columns = self.columns(table_id)
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
    from provisa.security.rights import acting_claims, capabilities_for_claims

    context = info.context
    request = context.get("request") if isinstance(context, dict) else context.request
    if request is None:
        # The process's own call (the boot's warmup queries): no request, nobody to narrow for.
        return CatalogScope(
            whole=True,
            reach=None,
            served={},
            governance=True,
            role=None,
        )
    identity = _identity_from_info(info)
    if identity is None:
        # A request the auth middleware resolved nobody for is answered nothing.
        return CatalogScope(
            whole=False,
            reach=frozenset(),
            served={},
            governance=False,
            role=None,
        )
    role = getattr(request.state, "role", None)
    if getattr(identity, "user_id", _ANONYMOUS) == _ANONYMOUS:
        # No auth provider: no right is enforced, but the catalog is the one the roles the request
        # names (X-Provisa-Role, the role picker's selection) carry and reach.
        caps = capabilities_for_claims(acting_claims(identity), getattr(state, "roles", {}))
        reach = _acting_reach(identity, state)
    else:
        caps = _resolved_capabilities(identity, state)
        reach = allowed_domains_request(request)
    scope = CatalogScope(
        whole=False,
        reach=reach,
        served=role_visibility(state, role) if role else {},
        governance=VIEW_GOVERNANCE in caps,
        role=role,
        compiled=frozenset(t["id"] for t in state.tables),
        subjects=_subjects(state, role) if role else (),
    )
    if role is None:
        return scope  # no acting role: served nothing, drafts included
    drafts = {
        table["id"]: served
        for table in state.role_build_inputs["draft_tables"]
        if (served := scope.stored_columns(table))
    }
    return dataclasses.replace(scope, served={**drafts, **scope.served})


def _acting_reach(identity: Any, state: Any) -> frozenset[str] | None:
    """The domains the roles ``identity`` acts as reach; None when domains gate nothing for it."""
    from provisa.core import domain_policy
    from provisa.core.env_authority import domains_within
    from provisa.security.rights import acting_claims, domain_access_for_claims

    if domain_policy.single_domain():
        return None
    reached = domain_access_for_claims(acting_claims(identity), getattr(state, "roles", {}))
    return domains_within(sorted(reached))


def _subjects(state: Any, role: str) -> tuple[dict, ...]:
    """The roles ``role`` acts as (a meta-role's members), as the column rule takes a role."""
    from provisa.security.meta_role import acting_roles

    return tuple(
        {"id": member, "domain_access": state.roles[member]["domain_access"]}
        for member in acting_roles(state, role)
        # A role with no definition has no data surface (REQ-1327).
        if member in state.roles
    )
