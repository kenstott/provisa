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

The body of ``POST /query/nl`` also carries a ``role`` field. It is not a second way to pick a
role. A body role equal to the acting role is accepted; one that differs is refused, naming both
— a request never runs as a different role than the one it asked for. Before this rule
``/query/nl`` ran its job as the body's role whatever the acting role was.
"""

# Requirements: REQ-273

from __future__ import annotations

from starlette.requests import Request

from provisa.api.errors import ApiError


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


def sent_role(body: object) -> str | None:
    """The ``role`` a request body carried, or None when the client did not send the field."""
    return getattr(body, "role") if "role" in body.model_fields_set else None  # type: ignore[attr-defined]
