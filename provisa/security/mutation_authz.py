# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1b2c3d-4e5f-4061-9a7b-2c3d4e5f6a7b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Protocol-agnostic mutation authorization core (REQ-867, REQ-868, REQ-869).

Two pure concerns, shared across every remote-schema adapter (GraphQL remote,
OpenAPI, gRPC, Hasura):

1. CLASSIFY a registered operation as READ or WRITE *by contract* — never by caller
   declaration (REQ-869). Each protocol has its own signal; the universal default for
   anything unknown is WRITE, so an unclassifiable operation is treated as a mutation
   and default-denied rather than silently executed.

2. AUTHORIZE a write: a role may invoke a mutation only when it holds the global WRITE
   capability (REQ-868) AND appears in that specific mutation's ``writable_by`` list —
   which is empty by default, i.e. default-deny (REQ-867). ADMIN/SUPERADMIN bypass, the
   same convention as ``check_capability``.

Execute-time enforcement (wiring this into the action executor) lives in the endpoint;
this module is pure and unit-testable with no I/O.
"""

from __future__ import annotations

from enum import Enum

from provisa.security.rights import Capability, has_capability


class MutationKind(str, Enum):  # REQ-869
    READ = "read"
    WRITE = "write"


_OPENAPI_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_GRPC_READONLY = frozenset({"NO_SIDE_EFFECTS"})


def classify_openapi(method: str | None, provisa_kind: str | None = None) -> MutationKind:
    """OpenAPI: HTTP method is the contract; x-provisa-kind overrides. GET = read,
    write methods = write, unknown = write (REQ-869)."""
    if provisa_kind:
        return MutationKind.WRITE if provisa_kind.lower() == "mutation" else MutationKind.READ
    if not method:
        return MutationKind.WRITE
    m = method.upper()
    if m == "GET":
        return MutationKind.READ
    if m in _OPENAPI_WRITE_METHODS:
        return MutationKind.WRITE
    return MutationKind.WRITE  # HEAD/OPTIONS/unknown → default-deny as write


def classify_graphql(operation_type: str | None) -> MutationKind:
    """GraphQL: operation type. ``type Mutation`` = write, else read (REQ-869)."""
    return MutationKind.WRITE if (operation_type or "").lower() == "mutation" else MutationKind.READ


def classify_grpc(idempotency_level: str | None) -> MutationKind:
    """gRPC: MethodOptions.idempotency_level. NO_SIDE_EFFECTS = read; IDEMPOTENT and
    IDEMPOTENCY_UNKNOWN = write; anything unknown = write (REQ-869)."""
    return (
        MutationKind.READ
        if (idempotency_level or "").upper() in _GRPC_READONLY
        else MutationKind.WRITE
    )


def classify_hasura(action_type: str | None) -> MutationKind:
    """Hasura: exposed_as/action_type. ``mutation`` = write, else read (REQ-869)."""
    return MutationKind.WRITE if (action_type or "").lower() == "mutation" else MutationKind.READ


def classify_kind(kind: str | None) -> MutationKind:
    """Classify from a registered operation's stored ``kind`` (``mutation``/``query``).

    This is what execute-time enforcement consults: a ``kind=mutation`` operation is a
    WRITE regardless of which surface invoked it, so a SELECT referencing a mutation UDF
    is tainted to write (REQ-869). Unknown/None → WRITE (default-deny).
    """
    if kind is None:
        return MutationKind.WRITE
    return MutationKind.READ if kind.lower() == "query" else MutationKind.WRITE


def authorize_mutation(
    role: dict[str, object] | None, writable_by: list[str] | None
) -> tuple[bool, str]:  # REQ-867, REQ-868
    """Decide whether ``role`` may invoke a write whose ACL is ``writable_by``.

    Returns ``(allowed, reason)``. Allowed only when the role holds the global WRITE
    capability AND its id is in ``writable_by`` (empty = default-deny). ADMIN/SUPERADMIN
    bypass, consistent with ``check_capability``. A missing role is denied.
    """
    if role is None:
        return False, "no role in context"
    caps = role.get("capabilities", [])
    if not isinstance(caps, (list, tuple, set, frozenset)):
        caps = []
    if Capability.ADMIN.value in caps or Capability.SUPERADMIN.value in caps:
        return True, ""
    if not has_capability(role, Capability.WRITE):
        return False, "role lacks the WRITE capability"
    if role.get("id") not in (writable_by or []):
        return False, "role is not listed in the mutation's writable_by (default-deny)"
    return True, ""


def require_mutation_write(action: dict, role: dict | None, field_name: str) -> None:  # REQ-869
    """Execute-time gate for a tracked function/webhook action.

    A ``kind=mutation`` action (or any unknown kind) is a write and is authorized via
    ``authorize_mutation``; a read (``kind=query``) passes untouched — read visibility is
    enforced elsewhere. Raises HTTP 403 when a write is not permitted (default-deny).
    """
    if classify_kind(action.get("kind")) is MutationKind.READ:
        return
    allowed, reason = authorize_mutation(role, action.get("writable_by") or [])
    if not allowed:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(
            status_code=403, detail=f"Mutation {field_name!r} not permitted: {reason}"
        )
