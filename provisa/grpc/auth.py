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
interceptor. The wrapper sets it in the RPC's own context (``provisa.grpc.rpc_scope``), so each RPC
sees its own value.

What a principal may act as is the org's to say, so the wrapper binds the RPC's org once the
credential holds and before it reads the role, the order Flight keeps. The org binding stands for
the whole RPC: the handlers find it bound (:data:`org_bound_for_rpc`).
"""

# Requirements: REQ-273, REQ-1263, REQ-1266, REQ-1337

from __future__ import annotations

import logging
from contextvars import ContextVar

import grpc

from provisa.audit.context import AuditIdentity, set_audit_identity
from provisa.auth import bearer
from provisa.auth.bearer import authorize_role_or_probe
from provisa.auth.models import AuthIdentity
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.security.high_security import high_security_wire_reject, kms_key_in_pairs

log = logging.getLogger(__name__)

_authorized_role: ContextVar[str | None] = ContextVar("provisa_grpc_authorized_role", default=None)
# Whether the RPC running in this context already has its organisation bound: by the interceptor
# from its principal, or where it was received (ProvisaServicer._received). The handlers' own
# binding then has nothing to do.
org_bound_for_rpc: ContextVar[bool] = ContextVar("provisa_grpc_org_bound", default=False)


def authorized_role() -> str | None:
    """The role the interceptor derived for the RPC running on this task, or None when auth is off."""
    return _authorized_role.get()


def auth_active(state) -> bool:
    """Whether this deployment authenticates gRPC callers (``auth.bearer.auth_active``)."""
    return bearer.auth_active(state, "grpc")


async def validate_grpc_credential(state, token: str):
    """Validate a caller's bearer credential and return its identity (REQ-1263): the one bearer
    validation every credential-only transport uses (``auth.bearer``)."""
    return await bearer.validate_bearer_credential(state, token, "a gRPC caller")


async def principal_org(state, identity: AuthIdentity, requested: str | None) -> str:
    """The org a validated principal's RPC binds: its membership decides, not the metadata
    (REQ-1337), by the rule MCP, pgwire and Flight use. ``x-provisa-org`` is a REQUEST."""
    from provisa.api.org_resolve import resolve_session_org
    from provisa.security.rights import can_act_cross_org, capabilities_for_claims

    caps = capabilities_for_claims(identity.roles or [], state.platform_roles)
    return await resolve_session_org(
        state,
        user_id=identity.user_id,
        can_act_any_org=can_act_cross_org(caps),
        requested_org=requested or None,
        credential_org=identity.active_org_id,  # REQ-1235
        named_by="send the x-provisa-org metadata",
    )


async def _bind_principal_org(state, identity: AuthIdentity, requested: str | None) -> str:
    """Resolve+build the RPC's org and bind it in the RPC's context (REQ-1266). A single-org
    deployment binds its one org; nothing names it."""
    org_id = state.org_id
    if getattr(state, "multitenancy", False):
        org_id = await principal_org(state, identity, requested)
        from provisa.api.app import ensure_serving_runtime

        await ensure_serving_runtime(org_id)
    set_current_org(org_id)
    org_bound_for_rpc.set(True)
    return org_id


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


_HEALTH_SERVICE_PREFIX = "/grpc.health.v1.Health/"


class AuthInterceptor(grpc.ServerInterceptor):
    """Refuses any RPC that does not present a valid credential, and fixes its role.

    Reflection is intercepted along with the data services: a caller who cannot authenticate must
    not be able to enumerate the schema either. The same reasoning covers the high-security check
    (REQ-693) — it runs ahead of the credential check on every RPC, reflection included, because a
    client that cannot receive plaintext rows has no reason to be reading the descriptor either.

    ``grpc.health.v1.Health`` (REQ-1904) is the one exception, matching the HTTP surface's own
    ``/health``/``/live``/``/ready`` (``provisa/api/app.py``) — unauthenticated and reachable even
    in high-security mode, because it returns liveness/readiness only, never row data or schema. An
    orchestrator probe that required a bearer credential could kill a live pod on an auth outage
    that has nothing to do with whether the pod is actually healthy.

    REQ-1882 (amended 2026-09-29): the synchronous server calls ``intercept_service`` on its one
    polling thread, so only the checks that read local state run here (high-security, auth on/off,
    a bearer present). Validating the credential can reach the platform database, so it runs on the
    RPC's own handler thread and loop (:func:`provisa.grpc.rpc_scope.rpc_scope`), ahead of the
    method's behavior — an unauthenticated caller still reaches no servicer code. The org is bound
    there too, between the credential and the role.
    """

    def __init__(self, state):
        self._state = state

    def intercept_service(self, continuation, handler_call_details):
        if (handler_call_details.method or "").startswith(_HEALTH_SERVICE_PREFIX):
            return continuation(handler_call_details)
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
            # REQ-074/REQ-1386: the deployment authenticates nobody, but the RPC is still audited —
            # as the anonymous principal.
            return _with_anonymous(continuation(handler_call_details))

        credential = _bearer(metadata)
        if not credential:
            return _abort_handler(
                grpc.StatusCode.UNAUTHENTICATED, "a bearer credential is required"
            )
        requested = _text(metadata, "x-provisa-role")
        requested_org = _text(metadata, "x-provisa-org")
        handler = continuation(handler_call_details)
        if handler is None:
            # An unknown method answers UNIMPLEMENTED only to a caller whose credential holds.
            return _with_principal(
                _abort_handler(grpc.StatusCode.UNIMPLEMENTED, "Method not found!"),
                self._state,
                credential,
                requested,
                requested_org,
                handler_call_details.method,
            )
        return _with_principal(
            handler, self._state, credential, requested, requested_org, handler_call_details.method
        )


def _authenticate(
    rpc,
    context,
    state,
    credential: str,
    requested: str | None,
    requested_org: str | None,
    method: str = "grpc",
) -> None:
    """Validate the credential, bind the RPC's org and publish the principal in the RPC's context,
    or abort."""
    from provisa.api.org_resolve import OrgResolutionError

    try:
        identity = rpc.run(validate_grpc_credential(state, credential))
    except bearer.CREDENTIAL_ERRORS as exc:
        context.abort(grpc.StatusCode.UNAUTHENTICATED, bearer.credential_refusal(exc))
        raise  # abort ends the RPC by raising; should it return, the rejection still propagates
    try:
        org_id = rpc.run(_bind_principal_org(state, identity, requested_org))
    except OrgResolutionError as exc:
        context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
        raise  # as above: never proceed in no org
    # authorize_role_or_probe runs here on the handler thread, outside the RPC's context (only the
    # coroutines it hands ``run`` are inside), and the pool's next RPC gets this thread: the org is
    # bound on the thread for the call and no longer.
    org_token = set_current_org(org_id)
    try:
        # REQ-2076: a suspended principal, or a role named that is not held, ends the RPC.
        role = authorize_role_or_probe(
            state,
            identity,
            requested,
            transport="grpc",
            endpoint=method,
            run=rpc.run,
            client=context.peer(),
        )
    except PermissionError as exc:
        context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
        raise  # as above: never proceed as an unauthorized role
    finally:
        reset_current_org(org_token)

    from provisa.auth.derived_roles import bind_principal_session_vars

    async def _publish() -> None:
        _authorized_role.set(role)
        # REQ-1682/REQ-1382: what a row filter reads for this caller, as on every transport.
        bind_principal_session_vars(state, identity, role)
        # REQ-074/REQ-1386: the acting principal the pipeline's audit write records, published in
        # the RPC's context alongside the role so every governed statement is attributed.
        set_audit_identity(AuditIdentity(user_id=identity.user_id, surface="grpc"))

    rpc.run(_publish())


def _with_principal(
    handler: grpc.RpcMethodHandler,
    state,
    credential: str,
    requested: str | None,
    requested_org: str | None,
    method: str = "grpc",
) -> grpc.RpcMethodHandler:
    """Rebuild ``handler`` so the credential is validated on the RPC's thread before it runs."""
    from provisa.grpc.rpc_scope import rpc_scope

    behavior = (
        handler.unary_unary or handler.unary_stream or handler.stream_unary or handler.stream_stream
    )
    assert behavior is not None  # a resolved handler always carries exactly one behavior

    def streaming(request, context):
        with rpc_scope() as rpc:
            _authenticate(rpc, context, state, credential, requested, requested_org, method)
            # The handler's declared behavior type is the union of all four RPC shapes; this branch
            # is taken only for a response-streaming handler, whose behavior is a generator.
            yield from behavior(request, context)  # pyright: ignore[reportGeneralTypeIssues]

    def unary(request, context):
        with rpc_scope() as rpc:
            _authenticate(rpc, context, state, credential, requested, requested_org, method)
            return behavior(request, context)  # pyright: ignore[reportGeneralTypeIssues]

    return _rebuild(handler, streaming if handler.response_streaming else unary)


def _with_anonymous(handler: grpc.RpcMethodHandler | None) -> grpc.RpcMethodHandler | None:
    """Rebuild ``handler`` so it runs with the anonymous audit identity published in the RPC's
    own context (an unsecured deployment) — the context every coroutine of the RPC runs in, where
    the pipeline's audit write reads it. ``None`` — an unknown method — stays None."""
    if handler is None:
        return None
    from provisa.audit.context import ANONYMOUS_USER
    from provisa.grpc.rpc_scope import rpc_scope

    behavior = (
        handler.unary_unary or handler.unary_stream or handler.stream_unary or handler.stream_stream
    )
    assert behavior is not None  # a resolved handler always carries exactly one behavior

    async def _publish() -> None:
        set_audit_identity(AuditIdentity(user_id=ANONYMOUS_USER, surface="grpc"))

    def streaming(request, context):
        with rpc_scope() as rpc:
            rpc.run(_publish())
            yield from behavior(request, context)  # pyright: ignore[reportGeneralTypeIssues]

    def unary(request, context):
        with rpc_scope() as rpc:
            rpc.run(_publish())
            return behavior(request, context)  # pyright: ignore[reportGeneralTypeIssues]

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

    def abort(request, context):  # noqa: ARG001  # the request is never read
        context.abort(code, detail)

    return grpc.unary_unary_rpc_method_handler(abort)
