# Copyright (c) 2026 Kenneth Stott
# Canary: 215668c8-7ee4-4c3d-81ae-6aa14aa15df0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""gRPC authenticates its callers before any handler runs (REQ-273, REQ-1263).

``x-provisa-role`` used to be the whole story: every handler read it, and a present value was
treated as proof of identity. It is a REQUEST, not a credential. This interceptor puts the
credential check in front of every RPC on the server — the generated service and reflection alike —
validates the bearer token or personal access token in ``authorization``, derives the role from the
validated identity, and publishes that authorized role for the handler to read.

The authorized role travels in a ContextVar rather than in the metadata because gRPC's
``context.invocation_metadata()`` reads from the live call and cannot be rewritten by an
interceptor. The wrapper sets it inside the handler's own task, so each RPC sees its own value.
"""

# Requirements: REQ-273, REQ-1263, REQ-1266, REQ-1337

from __future__ import annotations

import logging
from contextvars import ContextVar

import grpc
import grpc.aio
import jwt

from provisa.audit.context import AuditIdentity, set_audit_identity
from provisa.auth.models import AuthIdentity
from provisa.security.high_security import high_security_wire_reject, kms_key_in_pairs

log = logging.getLogger(__name__)

_authorized_role: ContextVar[str | None] = ContextVar("provisa_grpc_authorized_role", default=None)
_identity: ContextVar[AuthIdentity | None] = ContextVar("provisa_grpc_identity", default=None)


def authorized_role() -> str | None:
    """The role the interceptor derived for the RPC running on this task, or None when auth is off."""
    return _authorized_role.get()


def authenticated_identity() -> AuthIdentity | None:
    """The principal the interceptor validated for this RPC, or None when auth is off."""
    return _identity.get()


def auth_active(state) -> bool:
    """Whether this deployment authenticates gRPC callers.

    The same fail-closed reading pgwire and Flight use: a live auth middleware with no resolved
    ``auth_config`` is a misconfiguration, and a secured server must never degrade to trust mode
    because its config went missing.
    """
    if getattr(state, "auth_config", None) is not None:
        return True
    if getattr(state, "auth_middleware_active", False):
        raise RuntimeError("grpc auth_config not configured")
    return False


async def validate_grpc_credential(state, token: str):
    """Validate a caller's bearer credential and return its identity (REQ-1263).

    gRPC carries exactly one credential presentation — a bearer token in ``authorization`` — so the
    bearer validator is selected by name rather than calling ``validate_token``, whose meaning
    differs per provider (under ``basic`` it expects base64 ``user:password``, and every bearer
    credential, personal access token included, would fail there). The platform pool is passed
    through so a PAT resolves here exactly as it does on every other surface.
    """
    from provisa.auth.models import validator_for_scheme
    from provisa.auth.throttle import throttled
    from provisa.auth.wiring import build_auth_provider

    provider = build_auth_provider(state.auth_config, admin_pool=getattr(state, "admin_db", None))
    validator = validator_for_scheme(provider, "bearer")
    if validator is None:
        raise PermissionError(
            f"auth provider {provider.provider_name!r} accepts no bearer credential, "
            "so it cannot authenticate a gRPC caller"
        )
    # REQ-1393: gRPC names no principal, so the throttle keys on the credential itself — replaying
    # one rejected token is bounded, and a caller cannot lock out an account it does not know.
    return await throttled(validator, token, principal=None)


def authorize_role(state, identity, requested: str | None) -> str:
    """The role this RPC executes as — derived from the validated identity, never asserted.

    ``x-provisa-role`` may REQUEST a role, and it is honored only when the identity's own
    assignments carry it; anything else is a privilege claim by the client and is refused. With no
    request, the identity's claims map to a role through the same rules every other surface uses.
    """
    from provisa.auth.role_mapping import resolve_assignments, resolve_role

    auth_config = state.auth_config
    default_role = auth_config.get("default_role")
    if not default_role:
        # No admin default: an identity matching no mapping rule is refused, not escalated.
        raise PermissionError("identity matched no role and no default_role is configured")
    mapped = resolve_role(identity, auth_config.get("role_mapping", []), default_role)
    if not requested:
        return mapped
    permitted = {a.role_id for a in resolve_assignments(identity)} | {mapped}
    if requested not in permitted:
        raise PermissionError(f"role {requested!r} is not assigned to this identity")
    return requested


def _bearer(metadata) -> str | None:
    """The bearer credential in the call's ``authorization`` metadata, if it carries one."""
    for key, value in metadata or ():
        if key.lower() != "authorization":
            continue
        raw = value.decode() if isinstance(value, bytes) else value
        scheme, _, token = raw.partition(" ")
        if scheme.lower() == "bearer" and token:
            return token
    return None


def _text(metadata, name: str) -> str | None:
    for key, value in metadata or ():
        if key.lower() == name:
            return value.decode() if isinstance(value, bytes) else value
    return None


class AuthInterceptor(grpc.aio.ServerInterceptor):
    """Refuses any RPC that does not present a valid credential, and fixes its role.

    Reflection is intercepted along with the data services: a caller who cannot authenticate must
    not be able to enumerate the schema either. The same reasoning covers the high-security check
    (REQ-693) — it runs ahead of the credential check on every RPC, reflection included, because a
    client that cannot receive plaintext rows has no reason to be reading the descriptor either.
    """

    def __init__(self, state):
        self._state = state

    async def intercept_service(self, continuation, handler_call_details):
        metadata = handler_call_details.invocation_metadata
        # REQ-693: gRPC stays open in high-security mode — it is one of the two transports an
        # encrypting client actually uses — but each call must carry the same client-side
        # decryption key the HTTP data endpoints demand. This runs ahead of the auth check, and
        # ahead of the auth-is-off short circuit: whether the deployment authenticates its callers
        # has no bearing on whether the backend may hand out plaintext rows.
        refusal = high_security_wire_reject(self._state, kms_key_in_pairs(metadata))
        if refusal is not None:
            return _abort_handler(grpc.StatusCode.PERMISSION_DENIED, refusal)

        try:
            active = auth_active(self._state)
        except RuntimeError as exc:
            return _abort_handler(grpc.StatusCode.INTERNAL, str(exc))
        if not active:
            return await continuation(handler_call_details)

        credential = _bearer(metadata)
        if not credential:
            return _abort_handler(
                grpc.StatusCode.UNAUTHENTICATED, "a bearer credential is required"
            )
        try:
            identity = await validate_grpc_credential(self._state, credential)
        except (ValueError, jwt.PyJWTError):
            # Every rejection reads the same on the wire: a caller must not learn from the
            # response whether the credential was unknown, expired or revoked.
            return _abort_handler(grpc.StatusCode.UNAUTHENTICATED, "credential rejected")
        try:
            role = authorize_role(self._state, identity, _text(metadata, "x-provisa-role"))
        except PermissionError as exc:
            return _abort_handler(grpc.StatusCode.PERMISSION_DENIED, str(exc))

        handler = await continuation(handler_call_details)
        if handler is None:
            return None
        return _with_principal(handler, role, identity)


def _with_principal(
    handler: grpc.RpcMethodHandler, role: str, identity: AuthIdentity
) -> grpc.RpcMethodHandler:
    """Rebuild ``handler`` so its behavior runs with the validated principal published on the task."""
    behavior = (
        handler.unary_unary or handler.unary_stream or handler.stream_unary or handler.stream_stream
    )
    assert behavior is not None  # a resolved handler always carries exactly one behavior

    async def streaming(request, context):
        _authorized_role.set(role)
        _identity.set(identity)
        # REQ-074/REQ-1386: the acting principal the pipeline's audit write records. Set on the
        # per-RPC task alongside the role, so every handler's governed statements are attributed.
        set_audit_identity(AuditIdentity(user_id=identity.user_id, surface="grpc"))
        # The handler's declared behavior type is the union of all four RPC shapes; the branch
        # below picks the one that matches this handler's own streaming flags.
        async for message in behavior(request, context):  # pyright: ignore[reportGeneralTypeIssues]
            yield message

    async def unary(request, context):
        _authorized_role.set(role)
        _identity.set(identity)
        # REQ-074/REQ-1386: the acting principal the pipeline's audit write records. Set on the
        # per-RPC task alongside the role, so every handler's governed statements are attributed.
        set_audit_identity(AuditIdentity(user_id=identity.user_id, surface="grpc"))
        return await behavior(request, context)  # pyright: ignore[reportGeneralTypeIssues]

    return _rebuild(handler, streaming if handler.response_streaming else unary)


def _rebuild(handler: grpc.RpcMethodHandler, behavior) -> grpc.RpcMethodHandler:
    kwargs = {
        "request_deserializer": handler.request_deserializer,
        "response_serializer": handler.response_serializer,
    }
    if handler.request_streaming and handler.response_streaming:
        return grpc.stream_stream_rpc_method_handler(behavior, **kwargs)
    if handler.request_streaming:
        return grpc.stream_unary_rpc_method_handler(behavior, **kwargs)
    if handler.response_streaming:
        return grpc.unary_stream_rpc_method_handler(behavior, **kwargs)
    return grpc.unary_unary_rpc_method_handler(behavior, **kwargs)


def _abort_handler(code: grpc.StatusCode, detail: str) -> grpc.RpcMethodHandler:
    """A handler that refuses the call.

    Returned in place of the real handler so the refusal happens before the method is even
    resolved — an unauthenticated caller reaches no servicer code, including reflection's.
    """

    async def abort(request, context):  # noqa: ARG001  # the request is never read
        await context.abort(code, detail)

    return grpc.unary_unary_rpc_method_handler(abort)
