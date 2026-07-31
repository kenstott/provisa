# Copyright (c) 2026 Kenneth Stott
# Canary: cab73815-cc53-432c-bda6-42e805ede78d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for tenancy requirements: REQ-593, REQ-594"""

import os
from unittest.mock import MagicMock, patch

import pytest


class TestKMSRegionConfiguration:
    """REQ-593: KMS client reads AWS_KMS_REGION from environment, defaults to us-east-1."""

    def test_kms_client_raises_when_env_var_absent(self):
        # REQ-593
        env = {k: v for k, v in os.environ.items() if k != "AWS_KMS_REGION"}
        with patch.dict(os.environ, env, clear=True):
            with patch("boto3.client") as mock_boto3_client:
                from provisa.api.billing import kms as kms_module
                import importlib

                importlib.reload(kms_module)
                with pytest.raises(RuntimeError, match="AWS_KMS_REGION is required"):
                    kms_module._kms_client()
                mock_boto3_client.assert_not_called()

    def test_kms_client_uses_aws_kms_region_env_var_when_set(self):
        # REQ-593
        with patch.dict(os.environ, {"AWS_KMS_REGION": "eu-west-1"}):
            with patch("boto3.client") as mock_boto3_client:
                from provisa.api.billing import kms as kms_module
                import importlib

                importlib.reload(kms_module)
                kms_module._kms_client()
                mock_boto3_client.assert_called_once_with("kms", region_name="eu-west-1")
                assert mock_boto3_client.call_count == 1

    def test_kms_client_region_changes_with_different_env_var_values(self):
        # REQ-593
        for region in ["ap-southeast-1", "us-west-2", "ca-central-1"]:
            with patch.dict(os.environ, {"AWS_KMS_REGION": region}):
                with patch("boto3.client") as mock_boto3_client:
                    from provisa.api.billing import kms as kms_module
                    import importlib

                    importlib.reload(kms_module)
                    kms_module._kms_client()
                    mock_boto3_client.assert_called_once_with("kms", region_name=region)
                    assert mock_boto3_client.call_count == 1


class TestAuthMiddlewareSkipPaths:
    """REQ-594, REQ-1355: the token gate is bypassed for exactly the paths that cannot carry one.

    These assertions used to target ``TenantMiddleware._SKIP_PATHS``. That middleware was
    registered behind ``if state.multitenancy:`` inside ``create_app``, where the flag is still
    its ``False`` default (it is assigned in ``_load_and_build``, which lifespan runs later), so
    it never installed and the skip set it owned protected nothing — the Lemon Squeezy webhook
    401'd on the bearer gate. The guarantee now lives in the one middleware that does run.
    """

    def test_skip_paths_set_contains_billing_signup(self):
        # REQ-594 — the pre-account entrypoint: no token can exist yet.
        from provisa.auth.middleware import _SKIP_PATHS

        assert "/billing/signup" in _SKIP_PATHS

    def test_skip_paths_set_contains_billing_webhook(self):
        # REQ-594 — Lemon Squeezy authenticates with the HMAC X-Signature header, not a bearer.
        from provisa.auth.middleware import _SKIP_PATHS

        assert "/billing/webhook" in _SKIP_PATHS

    def test_skip_paths_set_contains_health(self):
        # REQ-594
        from provisa.auth.middleware import _SKIP_PATHS

        assert "/health" in _SKIP_PATHS

    def test_skip_paths_set_contains_docs(self):
        # REQ-594 — Swagger relocated under /data/openapi/ so the UI can own /docs
        from provisa.auth.middleware import _SKIP_PATHS

        assert "/data/openapi/docs" in _SKIP_PATHS

    def test_skip_paths_set_contains_openapi_json(self):
        # REQ-594
        from provisa.auth.middleware import _SKIP_PATHS

        assert "/data/openapi/openapi.json" in _SKIP_PATHS

    def test_skip_paths_set_has_exactly_the_required_paths(self):
        # REQ-594, REQ-1355 — an exact set, not a subset: every addition here opens an unauthenticated
        # hole, so a new entry must be a deliberate edit to this list with a stated reason.
        from provisa.auth.middleware import _SKIP_PATHS

        expected = {
            "/billing/signup",
            "/billing/webhook",
            "/health",
            "/live",
            "/ready",
            "/data/openapi/docs",
            "/data/openapi/redoc",
            "/data/openapi/openapi.json",
            "/auth/login",
            "/auth/provider-type",
            "/auth/bootstrap-status",
            "/setup/status",
        }
        assert _SKIP_PATHS == expected

    def test_no_data_or_admin_path_is_skipped(self):
        # REQ-594 — the openapi documents are the only /data/ paths that may bypass the gate, and
        # no /admin/ path ever may. Guards against a skip entry that silently unauthenticates data.
        from provisa.auth.middleware import _SKIP_PATHS

        offenders = {
            p
            for p in _SKIP_PATHS
            if p.startswith("/admin/") or (p.startswith("/data/") and not p.startswith("/data/openapi/"))
        }
        assert offenders == set()

    @pytest.mark.asyncio
    async def test_request_to_skip_path_bypasses_the_token_gate(self):
        # REQ-594 — requests to skip paths must not require a bearer token
        from provisa.auth.middleware import AuthMiddleware

        async def fake_app(*_):
            pass

        middleware = AuthMiddleware(fake_app)

        for skip_path in [
            "/billing/signup",
            "/billing/webhook",
            "/health",
            "/data/openapi/docs",
            "/data/openapi/openapi.json",
        ]:
            request = MagicMock()
            request.url.path = skip_path
            # No identity on request state — auth resolution must be skipped entirely
            request.state = MagicMock(spec=[])

            call_next_called = False

            async def call_next(_):
                nonlocal call_next_called
                call_next_called = True
                return MagicMock(status_code=200)

            await middleware.dispatch(request, call_next)
            assert call_next_called, f"call_next not called for skip path {skip_path}"


class TestOrgRoutingIsRegistered:
    """REQ-1266, REQ-1355: per-request org routing is installed on every app.

    The regression this guards: ``_OrgRoutingMiddleware`` and the control-plane router were both
    registered behind ``if state.multitenancy:`` *inside* ``create_app``. ``state.multitenancy`` is
    assigned in ``_load_and_build``, which ``lifespan`` runs after ``create_app`` returns, and both
    entrypoints use uvicorn factory mode — so the flag was always its ``False`` default and neither
    ever installed. With no request-lifetime binder for ``current_org``, ``AppState._active_runtime``
    fell through to the DEFAULT org's runtime for every authenticated request, meaning a member of
    org B read org A's data plane. Nothing in ``create_app`` may branch on that flag.
    """

    def test_create_app_registers_the_org_routing_middleware(self):
        from provisa.api import app as app_mod
        from provisa.api.app import create_app

        _prev_auth_config = getattr(app_mod.state, "auth_config", None)
        app_mod.state.auth_config = None
        try:
            the_app = create_app()
        finally:
            app_mod.state.auth_config = _prev_auth_config

        names = [m.cls.__name__ for m in the_app.user_middleware]
        assert "_OrgRoutingMiddleware" in names, (
            "per-request org routing is not installed; every request would resolve the default "
            f"org's data plane. Registered middleware: {names}"
        )

    def test_create_app_registers_the_control_plane_router(self):
        from provisa.api import app as app_mod
        from provisa.api.app import create_app

        _prev_auth_config = getattr(app_mod.state, "auth_config", None)
        app_mod.state.auth_config = None
        try:
            the_app = create_app()
        finally:
            app_mod.state.auth_config = _prev_auth_config

        paths = {r.path for r in the_app.routes if hasattr(r, "path")}
        assert any(p.startswith("/control-plane") for p in paths), (
            f"control-plane routes are not mounted: {sorted(p for p in paths if 'control' in p)}"
        )

    def test_create_app_never_branches_on_state_multitenancy(self):
        # The flag is unset at factory time; any read of it here is dead-branch by construction.
        import inspect

        from provisa.api.app import create_app

        # Comments are stripped: the surviving REQ-1355 notes name the flag to explain why the
        # guards were removed, and must not read as a reintroduction of them.
        src = "\n".join(
            line for line in inspect.getsource(create_app).splitlines()
            if not line.lstrip().startswith("#")
        )
        assert "state.multitenancy" not in src, (
            "create_app reads state.multitenancy, which _load_and_build has not assigned yet — "
            "the branch can never be taken. Register unconditionally and enforce per request."
        )
