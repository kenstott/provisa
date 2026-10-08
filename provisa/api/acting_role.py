# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1e8f3a-2d47-4b95-a0e6-7f9b3d5c1e82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The role an HTTP request runs as (REQ-273).

The acting role is established before the handler runs: by the auth middleware from the
validated identity, or — on a deployment with no auth provider — from the ``X-Provisa-Role``
header (org_admin when the request names none). The middleware binds the role claims, the session
variables and the audit identity for THAT role, so it is the only role the request can be
governed as.

Some request bodies also carry a ``role`` field (``/data/sql``, ``/data/graphql``,
``/query/nl``). It is not a second way to pick a role. A body role equal to the acting role is
accepted; one that differs is refused, naming both — a request never runs as a different role
than the one it asked for. Before this rule a body role was silently ignored on ``/data/sql`` and
``/data/graphql`` (a request naming ``analyst`` ran as org_admin, uncapped and unmasked), and
``/query/nl`` ran its job as the body's role whatever the acting role was.
"""

# Requirements: REQ-273

from __future__ import annotations

from starlette.requests import Request

from provisa.api.errors import ApiError
from provisa.security.meta_role import META_PREFIX


def acting_role(
    request: Request, header_role: str | None, body_role: str | None, body_default: str
) -> str:
    """The role this request runs as.

    ``body_role`` is the request body's ``role`` only when the client sent one (None otherwise);
    ``body_default`` is that field's default, used when nothing established a role at all — a
    router mounted without the auth middleware, as unit harnesses do.
    """
    established = getattr(request.state, "role", None) or header_role
    if established is None:
        return body_role or body_default
    if body_role is not None and body_role != established:
        raise ApiError(
            400,
            "data.role_mismatch",
            f"The request body names role {body_role!r} but the request runs as role "
            f"{established!r}. The role is carried by the X-Provisa-Role header (or the "
            "authenticated identity); remove `role` from the body or make it match.",
            body_role=body_role,
            acting_role=established,
        )
    return established


def held_role(request: Request, role_id: str) -> str:  # REQ-273
    """A role NAMED BY THE REQUEST — in the path or a query parameter — or a 403.

    Some routes address a role's own artifact (``/data/proto/{role_id}``, the gRPC Explorer's
    ``/data/grpc-commands/{role_id}``). Naming a role there is the same act as naming one in
    ``X-Provisa-Role``, and follows the same rule the middleware applies to that header: a role
    the authenticated caller is assigned is honoured, any other is refused. Without it the path
    is a way to read — or run a command as — a role the caller does not hold.

    A deployment with no auth provider (the anonymous dev identity) takes the role at face value,
    as it does for the header; so does a router mounted without the auth middleware.
    """
    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return role_id
    held = {a.role_id for a in getattr(request.state, "assignments", None) or ()}
    if role_id not in held:
        raise ApiError(
            403,
            "auth.role_not_assigned",
            f"Role {role_id!r} is not assigned to this user",
            role_id=role_id,
        )
    return role_id


def named_role(request: Request, named: str) -> str:  # REQ-273, REQ-1620
    """The role a request acts as from the role, or comma-separated set of roles, it NAMES in its
    path — or a refusal.

    The routes that address a role's own artifact (``/data/proto/{role_id}``, the gRPC Explorer's
    ``/data/grpc-commands/{role_id}``) accept what ``X-Provisa-Role`` accepts: one role the caller
    holds is that role, and several act as their meta-role, made on first use
    (``security.meta_role.resolve_requested_role``). Every member is held (:func:`held_role`, the
    same refusal by name), and a meta-role is never named directly: a client names the roles it
    holds and the server acts as them.

    REQ-1327: a control-plane role confers no data rights, so beside other roles it adds nothing
    to the set — the set acts as its data-plane members, as the middleware resolves the same
    header. Named alone it stays the role named, and the route refuses it for having no surface.
    """
    from provisa.api.app import state
    from provisa.security.meta_role import MetaRoleNamed, resolve_requested_role
    from provisa.security.rights import is_control_plane_role

    names = [r.strip() for r in named.split(",") if r.strip()]
    if not names:
        raise ApiError(400, "data.missing_role_id", "Missing role_id")
    for name in names:
        if not name.startswith(META_PREFIX):
            held_role(request, name)
    if len(set(names)) > 1:
        for name in names:
            if not name.startswith(META_PREFIX) and name not in state.roles:
                raise ApiError(404, "data.no_role", f"No role {name!r}", role_id=name)
        data_plane = [n for n in names if not is_control_plane_role(n, state.roles)]
        names = data_plane or names[:1]
    try:
        return resolve_requested_role(state, set(names), ",".join(names))
    except MetaRoleNamed as exc:
        raise ApiError(403, "auth.meta_role_named", str(exc), role_id=named) from exc


def header_role(request: Request, x_provisa_role: str | None, x_role: str | None) -> str | None:
    """The role a header-addressed data route runs as, or None when the request has none.

    ``X-Provisa-Role`` is the role header: the middleware reads it, checks it against the
    caller's assignments and publishes the result as the acting role. ``X-Role`` is NOT a role
    header — nothing validates it — but clients have sent it, and a route that silently ignored
    it answered as the acting role while the caller believed it had asked for another. So an
    ``X-Role`` that names a different role than the one the request runs as is refused, naming
    both, on the same terms as a differing body ``role``; one that agrees is accepted and does
    nothing. It never establishes a role by itself.
    """
    established = getattr(request.state, "role", None) or x_provisa_role
    if x_role is not None and x_role != established:
        raise ApiError(
            400,
            "data.role_mismatch",
            f"The X-Role header names role {x_role!r} but the request runs as role "
            f"{established!r}. The role is carried by the X-Provisa-Role header (or the "
            "authenticated identity); remove X-Role or make it match.",
            body_role=x_role,
            acting_role=established,
        )
    return established


def sent_role(body: object) -> str | None:
    """The ``role`` a request body carried, or None when the client did not send the field."""
    return getattr(body, "role") if "role" in body.model_fields_set else None  # type: ignore[attr-defined]
