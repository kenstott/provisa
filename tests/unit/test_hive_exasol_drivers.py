# Copyright (c) 2026 Kenneth Stott
# Canary: e5f5ded0-043d-4b84-b2d0-4573476ecc45
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for HiveDriver/ExasolDriver (REQ-1731) — mocked at the impyla/pyexasol boundary.

Real-service proof lives in tests/integration/test_hive_direct_driver_e2e.py (requires_hive) and
test_exasol_direct_driver_e2e.py (requires_exasol, amd64-only). These tests verify the driver's own
wiring (connect args, PLAIN-auth default, result-shape mapping) without a live cluster.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from provisa.executor.drivers.exasol import ExasolDriver
from provisa.executor.drivers.hive import HiveDriver
from provisa.executor.drivers.registry import _DRIVER_FACTORIES, create_driver


@pytest.mark.asyncio
async def test_hive_driver_defaults_to_plain_auth() -> None:
    """Stock HiveServer2 drops the connection under NOSASL (verified live against a real
    apache/hive:4.0.0 HS2 service) — the driver must default to PLAIN even with no password."""
    mock_connect = MagicMock()
    with patch("impala.dbapi.connect", mock_connect):
        driver = HiveDriver()
        await driver.connect(host="h", port=10000, database="default", user="hive", password="")
    assert mock_connect.call_args.kwargs["auth_mechanism"] == "PLAIN"
    assert driver.is_connected


@pytest.mark.asyncio
async def test_hive_driver_honors_configured_auth_mechanism() -> None:
    mock_connect = MagicMock()
    with patch("impala.dbapi.connect", mock_connect):
        driver = HiveDriver()
        driver.configure({"auth_mechanism": "GSSAPI"})
        await driver.connect(host="h", port=10000, database="default", user="u", password="p")
    assert mock_connect.call_args.kwargs["auth_mechanism"] == "GSSAPI"


@pytest.mark.asyncio
async def test_hive_driver_execute_maps_result() -> None:
    mock_cursor = MagicMock()
    mock_cursor.description = [("one",), ("two",)]
    mock_cursor.fetchall.return_value = [(1, "x")]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    driver = HiveDriver()
    driver._conn = mock_conn  # bypass connect() — this test targets execute()'s own mapping
    result = await driver.execute("SELECT 1 AS one, 'x' AS two")

    assert result.column_names == ["one", "two"]
    assert result.rows == [(1, "x")]
    mock_cursor.close.assert_called_once()


@pytest.mark.asyncio
async def test_exasol_driver_connect_builds_dsn() -> None:
    mock_connect = MagicMock()
    with patch("pyexasol.connect", mock_connect):
        driver = ExasolDriver()
        await driver.connect(
            host="exa.local", port=8563, database="TEST", user="sys", password="exasol"
        )
    assert mock_connect.call_args.kwargs["dsn"] == "exa.local:8563"
    assert mock_connect.call_args.kwargs["schema"] == "TEST"
    assert driver.is_connected


@pytest.mark.asyncio
async def test_exasol_driver_execute_maps_result() -> None:
    mock_stmt = MagicMock()
    mock_stmt.column_names.return_value = ["ID", "NAME"]
    mock_stmt.fetchall.return_value = [(1, "Widget A")]
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_stmt

    driver = ExasolDriver()
    driver._conn = mock_conn
    result = await driver.execute("SELECT * FROM widgets")

    assert result.column_names == ["ID", "NAME"]
    assert result.rows == [(1, "Widget A")]
    mock_conn.execute.assert_called_once_with("SELECT * FROM widgets")


def test_registry_wires_new_source_types() -> None:
    assert "hiveserver2" in _DRIVER_FACTORIES
    assert "exasol" in _DRIVER_FACTORIES
    assert isinstance(create_driver("hiveserver2"), HiveDriver)
    assert isinstance(create_driver("exasol"), ExasolDriver)
