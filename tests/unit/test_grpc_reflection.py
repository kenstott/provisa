# Copyright (c) 2026 Kenneth Stott
# Canary: 7682a87d-449d-4176-8e59-0a132e8cbe03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for gRPC server reflection wiring.

Exercises the real ``enable_server_reflection`` aio branch against a
``MagicMock(spec=grpc.aio.Server)`` rather than a live ``grpc.aio.server()``.
Instantiating a real cygrpc server here corrupts the shared C gRPC runtime and
segfaults on teardown once enough of the suite has run; the mock keeps full
coverage of our wrapper (name assembly + forwarding + registration path)
without touching the C server lifecycle.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import grpc.aio
import pytest

reflection = pytest.importorskip("grpc_reflection.v1alpha.reflection")

from provisa.grpc.reflection import enable_reflection  # noqa: E402


def _mock_server() -> MagicMock:
    """A stand-in that satisfies ``isinstance(server, grpc.aio.Server)``."""
    return MagicMock(spec=grpc.aio.Server)


class TestEnableReflection:
    def test_reflection_adds_service_names(self):
        """Verify enable_reflection registers services plus the reflection service."""
        server = _mock_server()
        service_names = ["provisa.v1.ProvisaService"]

        captured: dict[str, list[str]] = {}
        original_enable = reflection.enable_server_reflection

        def spy(names, srv, pool=None):
            captured["names"] = list(names)
            return original_enable(names, srv, pool=pool)

        with pytest.MonkeyPatch.context() as m:
            m.setattr(reflection, "enable_server_reflection", spy)
            result = enable_reflection(server, service_names)

        assert result is None
        assert "provisa.v1.ProvisaService" in captured["names"]
        assert reflection.SERVICE_NAME in captured["names"]
        # Real aio branch ran: the servicer was registered on the server.
        assert server.add_generic_rpc_handlers.called

    def test_reflection_includes_reflection_service(self):
        """The reflection service itself should be listed."""
        server = _mock_server()
        service_names = ["provisa.v1.ProvisaService"]

        captured: dict[str, list[str]] = {}
        original_enable = reflection.enable_server_reflection

        def spy(names, srv, pool=None):
            captured["names"] = list(names)
            return original_enable(names, srv, pool=pool)

        with pytest.MonkeyPatch.context() as m:
            m.setattr(reflection, "enable_server_reflection", spy)
            enable_reflection(server, service_names)

        assert reflection.SERVICE_NAME in captured["names"]
        assert "provisa.v1.ProvisaService" in captured["names"]

    def test_multiple_services_registered(self):
        """Multiple service names should all be registered."""
        server = _mock_server()
        service_names = ["provisa.v1.ProvisaService", "provisa.v1.AdminService"]

        captured: dict[str, list[str]] = {}
        original_enable = reflection.enable_server_reflection

        def spy(names, srv, pool=None):
            captured["names"] = list(names)
            return original_enable(names, srv, pool=pool)

        with pytest.MonkeyPatch.context() as m:
            m.setattr(reflection, "enable_server_reflection", spy)
            enable_reflection(server, service_names)

        assert len(captured["names"]) == 3  # 2 services + reflection
        assert "provisa.v1.ProvisaService" in captured["names"]
        assert "provisa.v1.AdminService" in captured["names"]
        assert reflection.SERVICE_NAME in captured["names"]
