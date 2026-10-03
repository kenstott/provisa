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

2. ADMIT a command call (``admit_command``), the one admission every surface's call passes:
   the role is assigned the command (its one role list, ``visible_to``; empty assigns it to
   every role), reaches the command's domain, and — for a mutation — holds the global WRITE
   capability (REQ-868). A command the role may not use is not found, the same as one never
   registered. No capability stands above the assignment.

The executor (api/data/action_exec.py) renders these as the API's answers; this module is pure
and unit-testable with no I/O.
"""

from __future__ import annotations

from enum import Enum

from provisa.security.rights import Capability, InsufficientRightsError, has_capability


class MutationNotPermitted(PermissionError):
    """REQ-869: a command call the role's rights do not admit. The API
    layer renders it as a 403 ApiError (REQ-1678: the gate itself never imports the API)."""

    def __init__(self, field_name: str, reason: str) -> None:
        super().__init__(f"Mutation {field_name!r} not permitted: {reason}")
        self.field_name = field_name
        self.reason = reason


class ColumnNotWritable(PermissionError):
    """REQ-663: the role is not in a column's ``writable_by``. Carried with the role and column so
    each surface renders it its own way (the API as a 403 ApiError, Cypher as a status tuple)."""

    def __init__(self, role_id: str, column: str) -> None:
        super().__init__(f"Role {role_id!r} does not have write access to column {column!r}")
        self.role_id = role_id
        self.column = column


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


def reclassify_kind(
    role: dict[str, object] | None, current_kind: str | None, target_kind: str | None
) -> str:  # REQ-870
    """Admin-only reclassification of a mutation to read-safe. Returns the new stored kind.

    Governance — not callers — controls classification (REQ-870). Only a role holding the
    ACCESS_CONFIG capability may reclassify, and
    only the demotion mutation → read is allowed: a write can be declared read-safe by an
    admin, but nothing can promote a read to a write and no caller-supplied ``read_only``
    flag exists. A no-op (target already equals current) is idempotent and returns the
    current stored kind. Any disallowed transition raises ``InsufficientRightsError``.
    """
    current = classify_kind(current_kind)
    target = classify_kind(target_kind)
    if current is target:
        return "query" if target is MutationKind.READ else "mutation"
    if not (current is MutationKind.WRITE and target is MutationKind.READ):
        raise ValueError(
            "only demotion of a mutation to read-safe is permitted; a read cannot be promoted"
        )
    if not has_capability(role or {}, Capability.ACCESS_CONFIG):
        raise InsufficientRightsError(str((role or {}).get("id", "")), Capability.ACCESS_CONFIG)
    return "query"


class CommandNotFound(LookupError):
    """A command that does not exist for the calling role — unregistered, not assigned to it, or
    in a domain it does not reach. One answer for all three, so a call never learns that a
    command it may not use exists."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Unknown command: {name!r}")
        self.name = name


def command_assigned(command: dict, role_id: str) -> bool:
    """Whether ``command`` is assigned to ``role_id``: its one list of assigned roles,
    ``visible_to`` — empty assigns it to every role."""
    assigned = command.get("visible_to") or []
    return not assigned or role_id in assigned


def command_reachable(command: dict, role: dict) -> bool:
    """Whether ``role`` may see ``command`` at all: assigned it, and reaching its domain."""
    from provisa.security.rights import reaches_domain

    return command_assigned(command, str(role.get("id", ""))) and reaches_domain(
        role.get("domain_access"), command.get("domain_id") or ""
    )


def admit_command(command: dict, role: dict | None, name: str) -> None:  # REQ-869, REQ-1758
    """The one command admission, for every surface: the role is assigned the command, reaches
    its domain, and — for a command declared ``mutation`` (or of unknown kind) — holds the write
    right. Approval, where the command declares it, is the caller's next step.

    A command is opaque within these rights: what it does once admitted is not inspected here.
    There is no call without a role (REQ-1758)."""
    if role is None:
        raise MutationNotPermitted(name, "no acting role: a command is called as a role")
    if not command_reachable(command, role):
        raise CommandNotFound(name)
    if classify_kind(command.get("kind")) is MutationKind.WRITE and not has_capability(
        role, Capability.WRITE
    ):
        raise MutationNotPermitted(name, "role lacks the WRITE capability")
