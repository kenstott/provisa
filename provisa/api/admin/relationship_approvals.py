# Copyright (c) 2026 Kenneth Stott
# Canary: 5f9a2c41-7d3e-4b68-a0c5-e18b6d2f7a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Who decides a relationship request, and when it may be carried out (REQ-1948).

A relationship request is decided by the domains it touches: the domain of its source table and
the domain of its target table. An approval counts only from a user whose right to create
relationships reaches at least one of them (REQ-1944's reach: the domains of the roles carrying
the right). The request is executable once two different users, neither the requester, have
approved AND every domain involved is reached by at least one of them.

Each approval records the involved domains its approver reached when it was given, so the
question "which domains has this request heard from" is answered from the request alone.

The rule is here once; the REST queue and the GraphQL mutations both ask it.
"""

# Requirements: REQ-1948, REQ-1944, REQ-1531

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The right whose reach decides a relationship request.
RIGHT = "create_relationship"

#: The request type this rule governs.
REQUEST_TYPE = "relationship"

#: Two different users, neither the requester.
REQUIRED_APPROVERS = 2


@dataclass(frozen=True)
class Refusal:
    """Why a decision on a request is refused: a stable code, its params and the English text."""

    code: str
    message: str
    params: dict[str, Any] = field(default_factory=dict)


def _named(domains: "frozenset[str] | set[str] | list[str]") -> str:
    return ", ".join(sorted(domains))


async def domains_involved(conn: "Connection", payload: dict) -> frozenset[str]:
    """The domains a relationship request touches: its source table's and its target table's.

    A table the model no longer registers has no domain and contributes none; a request left
    with no domain at all can be approved by nobody (:func:`approval_refusal`). A relationship to
    a function has no target table, so its source's domain is the only one involved.
    """
    from provisa.core.repositories import table as table_repo

    domains: set[str] = set()
    for name in (payload["source_table_id"], payload["target_table_id"]):
        if not name:
            continue
        row = await table_repo.find_by_table_name(conn, name)
        if row is not None:
            domains.add(row["domain_id"])
    return frozenset(domains)


def reached(reach: frozenset[str] | None, involved: frozenset[str]) -> frozenset[str]:
    """The involved domains a right with ``reach`` reaches. ``None`` is every domain."""
    return involved if reach is None else involved & reach


def _eligibility_refusal(
    *,
    user_id: str | None,
    requested_by: str | None,
    involved: frozenset[str],
    reach: frozenset[str] | None,
) -> Refusal | None:
    if not user_id or user_id == "anonymous":
        # Two DIFFERENT users is the rule; a decision nobody signed cannot be counted toward it.
        return Refusal(
            "requests.approver_unidentified",
            "A relationship request is decided by signed-in users; this decision has no user",
        )
    if not reached(reach, involved):
        names = _named(involved)
        return Refusal(
            "requests.approver_outside_domains",
            f"Your right to create relationships does not reach a domain this request "
            f"touches ({names})",
            {"domains": names},
        )
    if user_id == requested_by:
        return Refusal("requests.own_request", "You cannot decide a request you made")
    return None


def approval_refusal(
    *,
    user_id: str | None,
    requested_by: str | None,
    approvals: list[dict],
    involved: frozenset[str],
    reach: frozenset[str] | None,
) -> Refusal | None:
    """Why ``user_id`` may not approve, or None when the approval counts."""
    refusal = _eligibility_refusal(
        user_id=user_id, requested_by=requested_by, involved=involved, reach=reach
    )
    if refusal is not None:
        return refusal
    if any(a["approver"] == user_id for a in approvals):
        return Refusal("requests.already_approved", "You have already approved this request")
    return None


def rejection_refusal(
    *,
    user_id: str | None,
    requested_by: str | None,
    involved: frozenset[str],
    reach: frozenset[str] | None,
) -> Refusal | None:
    """Why ``user_id`` may not reject: a rejection comes from any user who could approve."""
    return _eligibility_refusal(
        user_id=user_id, requested_by=requested_by, involved=involved, reach=reach
    )


def can_decide(
    *,
    user_id: str | None,
    requested_by: str | None,
    involved: frozenset[str],
    reach: frozenset[str] | None,
) -> bool:
    return (
        _eligibility_refusal(
            user_id=user_id, requested_by=requested_by, involved=involved, reach=reach
        )
        is None
    )


def visible_to(
    *,
    user_id: str | None,
    requested_by: str | None,
    involved: frozenset[str],
    reach: frozenset[str] | None,
) -> bool:
    """A user sees the requests they can decide and the ones they made."""
    if user_id is not None and user_id == requested_by:
        return True
    return can_decide(user_id=user_id, requested_by=requested_by, involved=involved, reach=reach)


def _counted(approvals: list[dict], requested_by: str | None) -> dict[str, set[str]]:
    """approver -> involved domains reached, the requester left out, each user once."""
    by_user: dict[str, set[str]] = {}
    for a in approvals:
        if a["approver"] == requested_by:
            continue
        by_user.setdefault(a["approver"], set()).update(a["domains"])
    return by_user


def waiting_on(
    involved: frozenset[str], approvals: list[dict], requested_by: str | None = None
) -> list[str]:
    """The involved domains no approver has reached yet."""
    covered: set[str] = set()
    for domains in _counted(approvals, requested_by).values():
        covered |= domains
    return sorted(involved - covered)


def executable(involved: frozenset[str], approvals: list[dict], requested_by: str | None) -> bool:
    """Two different approvers, neither the requester, and every domain involved reached."""
    if not involved:
        return False
    counted = _counted(approvals, requested_by)
    return len(counted) >= REQUIRED_APPROVERS and not waiting_on(involved, approvals, requested_by)


def incomplete_refusal(
    involved: frozenset[str], approvals: list[dict], requested_by: str | None
) -> Refusal | None:
    """Why an approved-so-far request may not be executed yet, or None when it may."""
    if executable(involved, approvals, requested_by):
        return None
    waiting = waiting_on(involved, approvals, requested_by)
    if waiting:
        names = _named(waiting)
        return Refusal(
            "requests.waiting_on_domains",
            f"This request still waits on an approval from: {names}",
            {"domains": names},
        )
    return Refusal(
        "requests.approvals_incomplete",
        f"This request needs approvals from {REQUIRED_APPROVERS} different users",
        {"required": REQUIRED_APPROVERS},
    )
