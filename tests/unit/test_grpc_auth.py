# Copyright (c) 2026 Kenneth Stott
# Canary: 77f97c6d-ce17-4634-8e65-1bad4b485d43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""gRPC authenticates its callers (REQ-273, REQ-617, REQ-1263).

``x-provisa-role`` used to be both the identity and the authorization: a handler read the header,
found a role name, and ran as that role. These tests pin the replacement — an interceptor validates
a bearer credential ahead of every RPC, and the role a handler sees is the one the validated
identity permits, not the one the metadata named.
"""

from __future__ import annotations

import grpc
import pytest

from provisa.grpc import auth as grpc_auth
from provisa.grpc.auth import AuthInterceptor
from provisa.auth.models import AuthIdentity


class FakeState:
    """Minimal AppState substitute carrying only what the auth path reads."""

    def __init__(self, auth_config=None, auth_middleware_active=False, security_high=False):
        self.auth_config = auth_config
        self.auth_middleware_active = auth_middleware_active
        self.security_high = security_high
        self.multitenancy = False
        self.admin_db = None
        self.roles = {}


_AUTH_CONFIG = {
    "provider": "oidc",
    "default_role": "analyst",
    "role_mapping": [
        {"claim": "groups", "type": "contains", "value": "data-eng", "role": "steward"}
    ],
}


def _identity(**claims) -> AuthIdentity:
    return AuthIdentity(
        user_id="u-1",
        email="alice@acme.test",
        display_name="Alice",
        roles=claims.pop("roles", []),
        raw_claims=claims,
        active_org_id=None,
    )


class FakeContext:
    """Records the abort a refused RPC produces instead of raising through grpc's C core."""

    def __init__(self):
        self.code = None
        self.detail = None

    async def abort(self, code, detail):
        self.code = code
        self.detail = detail
        raise _Aborted(code, detail)


class _Aborted(Exception):
    def __init__(self, code, detail):
        super().__init__(detail)
        self.code = code
        self.detail = detail


class CallDetails:
    def __init__(self, metadata):
        self.method = "/provisa.Provisa/QueryOrders"
        self.invocation_metadata = metadata


def _handler(seen: list) -> grpc.RpcMethodHandler:
    """A unary handler that records the role published on its own task."""

    async def behavior(request, context):  # noqa: ARG001  # neither is read
        seen.append(grpc_auth.authorized_role())
        return "ok"

    return grpc.unary_unary_rpc_method_handler(behavior)


@pytest.fixture()
def secured(monkeypatch):
    async def _validate(state, token):  # noqa: ARG001  # signature mirrors the real validator
        if token == "good-token":
            return _identity(groups=["data-eng"], roles=["steward:trading", "auditor"])
        if token == "plain-token":
            return _identity(groups=["everyone"])
        raise ValueError("no such credential")

    monkeypatch.setattr(grpc_auth, "validate_grpc_credential", _validate)
    return AuthInterceptor(FakeState(auth_config=_AUTH_CONFIG, auth_middleware_active=True))


async def _run(interceptor, metadata, seen):
    async def continuation(_details):
        return _handler(seen)

    handler = await interceptor.intercept_service(continuation, CallDetails(metadata))
    context = FakeContext()
    try:
        result = await handler.unary_unary(None, context)
    except _Aborted:
        return context, None
    return context, result


@pytest.mark.asyncio
class TestCredentialIsRequired:
    async def test_an_rpc_without_a_credential_is_refused(self, secured):
        seen: list = []
        context, result = await _run(secured, [("x-provisa-role", "admin")], seen)
        assert context.code == grpc.StatusCode.UNAUTHENTICATED
        assert result is None
        assert seen == [], "the handler must not run at all"

    async def test_a_rejected_credential_says_nothing_about_why(self, secured):
        context, _ = await _run(secured, [("authorization", "Bearer bogus")], [])
        assert context.code == grpc.StatusCode.UNAUTHENTICATED
        assert context.detail == "credential rejected"

    async def test_a_non_bearer_authorization_is_not_a_credential(self, secured):
        context, _ = await _run(secured, [("authorization", "Basic dXNlcjpwdw==")], [])
        assert context.code == grpc.StatusCode.UNAUTHENTICATED
        assert context.detail == "a bearer credential is required"

    async def test_reflection_is_gated_too(self, secured):
        """A caller who cannot authenticate must not be able to enumerate the schema either."""
        details = CallDetails([])
        details.method = "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"

        async def continuation(_details):
            raise AssertionError("the real reflection handler must never be resolved")

        handler = await secured.intercept_service(continuation, details)
        context = FakeContext()
        with pytest.raises(_Aborted):
            await handler.unary_unary(None, context)
        assert context.code == grpc.StatusCode.UNAUTHENTICATED


@pytest.mark.asyncio
class TestRoleComesFromTheIdentity:
    async def test_an_unmapped_identity_runs_as_the_default_role(self, secured):
        seen: list = []
        _, result = await _run(secured, [("authorization", "Bearer plain-token")], seen)
        assert result == "ok"
        assert seen == ["analyst"]

    async def test_a_mapped_claim_selects_the_role(self, secured):
        seen: list = []
        await _run(secured, [("authorization", "Bearer good-token")], seen)
        assert seen == ["steward"]

    async def test_a_requested_role_the_identity_holds_is_honored(self, secured):
        seen: list = []
        await _run(
            secured,
            [("authorization", "Bearer good-token"), ("x-provisa-role", "auditor")],
            seen,
        )
        assert seen == ["auditor"]

    async def test_a_requested_role_the_identity_lacks_is_refused(self, secured):
        seen: list = []
        context, _ = await _run(
            secured,
            [("authorization", "Bearer good-token"), ("x-provisa-role", "admin")],
            seen,
        )
        assert context.code == grpc.StatusCode.PERMISSION_DENIED
        assert seen == []

    async def test_the_metadata_cannot_assert_a_role_it_merely_names(self, secured):
        """The old behavior: naming a role granted it. It must not, even with a valid credential."""
        context, _ = await _run(
            secured,
            [("authorization", "Bearer plain-token"), ("x-provisa-role", "steward")],
            [],
        )
        assert context.code == grpc.StatusCode.PERMISSION_DENIED


@pytest.mark.asyncio
class TestHighSecurityMode:
    """REQ-693: gRPC keeps serving, but every call must prove the client can decrypt."""

    @pytest.fixture()
    def high(self, secured):
        secured._state.security_high = True
        return secured

    async def test_a_call_without_a_kms_key_is_refused(self, high):
        seen: list = []
        context, _ = await _run(high, [("authorization", "Bearer good-token")], seen)
        assert context.code == grpc.StatusCode.PERMISSION_DENIED
        assert context.detail is not None and "x-provisa-kms-key" in context.detail
        assert seen == [], "the handler must not run at all"

    async def test_a_call_carrying_the_kms_key_is_served(self, high):
        metadata = [("authorization", "Bearer good-token"), ("x-provisa-kms-key", b"arn:kms:key")]
        _, result = await _run(high, metadata, [])
        assert result == "ok"

    async def test_the_gate_precedes_the_credential_check(self, high):
        """An unauthenticated caller in high mode learns nothing about credentials."""
        context, _ = await _run(high, [], [])
        assert context.code == grpc.StatusCode.PERMISSION_DENIED

    async def test_the_gate_holds_when_the_deployment_does_not_authenticate(self):
        """A deployment with auth off still refuses plaintext.

        The gate first sat behind ``if not active: return await continuation(...)``, so an
        unauthenticated high-security deployment served rows over gRPC. Whether callers are
        authenticated has no bearing on whether the backend may hand out plaintext.
        """
        state = FakeState(security_high=True)
        seen: list = []
        context, _ = await _run(AuthInterceptor(state), [], seen)
        assert context.code == grpc.StatusCode.PERMISSION_DENIED
        assert seen == [], "the handler must not run at all"

    async def test_reflection_is_gated_too(self, high):
        details = CallDetails([])
        details.method = "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"

        async def continuation(_details):
            raise AssertionError("the real reflection handler must never be resolved")

        handler = await high.intercept_service(continuation, details)
        context = FakeContext()
        with pytest.raises(_Aborted):
            await handler.unary_unary(None, context)
        assert context.code == grpc.StatusCode.PERMISSION_DENIED


@pytest.mark.asyncio
class TestUnsecuredDeployment:
    async def test_no_auth_config_leaves_the_metadata_role_alone(self):
        interceptor = AuthInterceptor(FakeState())
        seen: list = []
        _, result = await _run(interceptor, [("x-provisa-role", "admin")], seen)
        assert result == "ok"
        assert seen == [None], "no identity to derive from, so the handler falls back to metadata"

    async def test_a_live_middleware_without_config_fails_closed(self):
        """A secured server whose config went missing must refuse, never degrade to trust mode."""
        interceptor = AuthInterceptor(FakeState(auth_middleware_active=True))
        context, _ = await _run(interceptor, [("x-provisa-role", "admin")], [])
        assert context.code == grpc.StatusCode.INTERNAL
        assert context.detail is not None and "auth_config not configured" in context.detail
