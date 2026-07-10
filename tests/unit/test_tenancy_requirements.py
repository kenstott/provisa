# Copyright (c) 2026 Kenneth Stott
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


class TestTenantMiddlewareSkipPaths:
    """REQ-594: TenantMiddleware skip-path set bypasses tenant resolution for specific paths."""

    def test_skip_paths_set_contains_billing_signup(self):
        # REQ-594
        from provisa.api.middleware.tenant_middleware import _SKIP_PATHS

        assert "/billing/signup" in _SKIP_PATHS

    def test_skip_paths_set_contains_billing_webhook(self):
        # REQ-594
        from provisa.api.middleware.tenant_middleware import _SKIP_PATHS

        assert "/billing/webhook" in _SKIP_PATHS

    def test_skip_paths_set_contains_health(self):
        # REQ-594
        from provisa.api.middleware.tenant_middleware import _SKIP_PATHS

        assert "/health" in _SKIP_PATHS

    def test_skip_paths_set_contains_docs(self):
        # REQ-594 — Swagger relocated under /data/openapi/ so the UI can own /docs
        from provisa.api.middleware.tenant_middleware import _SKIP_PATHS

        assert "/data/openapi/docs" in _SKIP_PATHS

    def test_skip_paths_set_contains_openapi_json(self):
        # REQ-594
        from provisa.api.middleware.tenant_middleware import _SKIP_PATHS

        assert "/data/openapi/openapi.json" in _SKIP_PATHS

    def test_skip_paths_set_has_exactly_the_required_paths(self):
        # REQ-594
        from provisa.api.middleware.tenant_middleware import _SKIP_PATHS

        expected = {
            "/billing/signup",
            "/billing/webhook",
            "/health",
            "/data/openapi/docs",
            "/data/openapi/redoc",
            "/data/openapi/openapi.json",
        }
        assert _SKIP_PATHS == expected

    @pytest.mark.asyncio
    async def test_request_to_skip_path_bypasses_tenant_resolution(self):
        # REQ-594 — requests to skip paths must not require a JWT with tenant_id
        from provisa.api.middleware.tenant_middleware import TenantMiddleware

        async def fake_app(*_):
            pass

        middleware = TenantMiddleware(fake_app)

        for skip_path in [
            "/billing/signup",
            "/billing/webhook",
            "/health",
            "/data/openapi/docs",
            "/data/openapi/openapi.json",
        ]:
            request = MagicMock()
            request.url.path = skip_path
            # No identity on request state — tenant resolution must be skipped
            request.state = MagicMock(spec=[])

            call_next_called = False

            async def call_next(_):
                nonlocal call_next_called
                call_next_called = True
                return MagicMock(status_code=200)

            await middleware.dispatch(request, call_next)
            assert call_next_called, f"call_next not called for skip path {skip_path}"

    @pytest.mark.asyncio
    async def test_non_skip_path_without_identity_returns_401(self):
        # REQ-594 — paths NOT in the skip set must enforce tenant isolation
        from provisa.api.middleware.tenant_middleware import TenantMiddleware

        async def fake_app(*_):
            pass

        middleware = TenantMiddleware(fake_app)

        request = MagicMock()
        request.url.path = "/api/some-protected-resource"
        # No identity — simulates missing JWT
        state = MagicMock(spec=[])
        object.__setattr__(state, "identity", None)
        request.state = state

        async def call_next(_):
            return MagicMock(status_code=200)

        response = await middleware.dispatch(request, call_next)
        assert response.status_code == 401
