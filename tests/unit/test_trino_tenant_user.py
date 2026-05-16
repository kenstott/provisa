# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for per-tenant Trino user isolation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from provisa.api.trino_setup import get_trino_connection

_BASE_KWARGS = {
    "host": "trino",
    "port": 8080,
    "user": "system",
    "catalog": "iceberg",
}


class TestGetTrinoConnection:
    def test_tenant_id_sets_user(self):
        with patch("trino.dbapi.connect", return_value=MagicMock()) as mock_connect:
            get_trino_connection(_BASE_KWARGS, tenant_id="acme")
            called_kwargs = mock_connect.call_args[1]
            assert called_kwargs["user"] == "acme"

    def test_no_tenant_id_uses_default_user(self):
        with patch("trino.dbapi.connect", return_value=MagicMock()) as mock_connect:
            get_trino_connection(_BASE_KWARGS, tenant_id=None)
            called_kwargs = mock_connect.call_args[1]
            assert called_kwargs["user"] == "system"

    def test_does_not_mutate_input_kwargs(self):
        original = dict(_BASE_KWARGS)
        with patch("trino.dbapi.connect", return_value=MagicMock()):
            get_trino_connection(_BASE_KWARGS, tenant_id="acme")
        assert _BASE_KWARGS == original

    def test_all_base_kwargs_forwarded(self):
        with patch("trino.dbapi.connect", return_value=MagicMock()) as mock_connect:
            get_trino_connection(_BASE_KWARGS, tenant_id="acme")
            called_kwargs = mock_connect.call_args[1]
            assert called_kwargs["host"] == "trino"
            assert called_kwargs["port"] == 8080
            assert called_kwargs["catalog"] == "iceberg"
