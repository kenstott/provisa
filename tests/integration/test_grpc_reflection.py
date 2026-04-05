# Copyright (c) 2026 Kenneth Stott
# Canary: 7682a87d-449d-4176-8e59-0a132e8cbe03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for gRPC server reflection.

Requires a running gRPC server (Docker Compose stack).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import grpc.aio
import pytest

reflection = pytest.importorskip("grpc_reflection.v1alpha.reflection")

from provisa.grpc.reflection import enable_reflection

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


class TestEnableReflection:
    async def test_reflection_adds_service_names(self):
        """Verify enable_reflection registers services plus the reflection service."""
        server = grpc.aio.server()
        service_names = ["provisa.v1.ProvisaService"]

        enable_reflection(server, service_names)

        # Server should have handlers registered (no error on enable)
        # We verify the function didn't raise and the server is usable
        await server.stop(grace=0)

    async def test_reflection_includes_reflection_service(self):
        """The reflection service itself should be listed."""
        server = grpc.aio.server()
        service_names = ["provisa.v1.ProvisaService"]

        # Capture what gets passed to enable_server_reflection
        captured = {}
        original_enable = reflection.enable_server_reflection

        def spy(names, srv):
            captured["names"] = list(names)
            return original_enable(names, srv)

        with pytest.MonkeyPatch.context() as m:
            m.setattr(reflection, "enable_server_reflection", spy)
            enable_reflection(server, service_names)

        assert reflection.SERVICE_NAME in captured["names"]
        assert "provisa.v1.ProvisaService" in captured["names"]
        await server.stop(grace=0)

    async def test_multiple_services_registered(self):
        """Multiple service names should all be registered."""
        server = grpc.aio.server()
        service_names = [
            "provisa.v1.ProvisaService",
            "provisa.v1.AdminService",
        ]

        captured = {}
        original_enable = reflection.enable_server_reflection

        def spy(names, srv):
            captured["names"] = list(names)
            return original_enable(names, srv)

        with pytest.MonkeyPatch.context() as m:
            m.setattr(reflection, "enable_server_reflection", spy)
            enable_reflection(server, service_names)

        assert len(captured["names"]) == 3  # 2 services + reflection
        assert "provisa.v1.ProvisaService" in captured["names"]
        assert "provisa.v1.AdminService" in captured["names"]
        assert reflection.SERVICE_NAME in captured["names"]
        await server.stop(grace=0)
