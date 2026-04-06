# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-cdef-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Zaychik Arrow Flight fallback (REQ-146)."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

import provisa.executor.trino_flight as trino_flight_mod
from provisa.executor.trino import QueryResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reset_zaychik():
    """Reset the module-level _zaychik_available flag to True."""
    trino_flight_mod._zaychik_available = True


def _make_query_result(rows=None, column_names=None) -> QueryResult:
    return QueryResult(rows=rows or [(1,)], column_names=column_names or ["id"])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIsZaychikAvailable:
    def setup_method(self):
        _reset_zaychik()

    def test_is_zaychik_available_initially_true(self):
        """_zaychik_available starts as True."""
        assert trino_flight_mod.is_zaychik_available() is True

    def test_is_zaychik_available_returns_false_after_failure(self):
        """After a simulated failure, is_zaychik_available() returns False."""
        trino_flight_mod._zaychik_available = False
        assert trino_flight_mod.is_zaychik_available() is False


class TestExecuteWithFallback:
    def setup_method(self):
        _reset_zaychik()

    def test_execute_with_fallback_uses_zaychik_when_available(self):
        """When Zaychik is available, execute_trino_flight is called."""
        expected = _make_query_result()
        flight_conn = MagicMock()
        trino_conn = MagicMock()

        with patch.object(
            trino_flight_mod,
            "execute_trino_flight",
            return_value=expected,
        ) as mock_flight:
            result = trino_flight_mod.execute_with_fallback(
                flight_conn, trino_conn, "SELECT 1"
            )

        mock_flight.assert_called_once()
        assert result is expected

    def test_execute_with_fallback_falls_back_on_exception(self):
        """When Zaychik raises, execute_trino (REST) is called."""
        fallback_result = _make_query_result(rows=[(42,)], column_names=["val"])
        flight_conn = MagicMock()
        trino_conn = MagicMock()

        with patch.object(
            trino_flight_mod,
            "execute_trino_flight",
            side_effect=ConnectionError("flight down"),
        ):
            with patch(
                "provisa.executor.trino.execute_trino",
                return_value=fallback_result,
            ) as mock_trino:
                result = trino_flight_mod.execute_with_fallback(
                    flight_conn, trino_conn, "SELECT 42"
                )

        mock_trino.assert_called_once()
        assert result is fallback_result

    def test_zaychik_marked_unavailable_after_failure(self):
        """After one Zaychik failure, _zaychik_available becomes False."""
        flight_conn = MagicMock()
        trino_conn = MagicMock()

        with patch.object(
            trino_flight_mod,
            "execute_trino_flight",
            side_effect=RuntimeError("oops"),
        ):
            with patch("provisa.executor.trino.execute_trino", return_value=_make_query_result()):
                trino_flight_mod.execute_with_fallback(flight_conn, trino_conn, "SELECT 1")

        assert trino_flight_mod.is_zaychik_available() is False

    def test_fallback_stays_disabled_after_first_failure(self):
        """Subsequent calls skip Zaychik entirely once it has failed."""
        flight_conn = MagicMock()
        trino_conn = MagicMock()
        fallback_result = _make_query_result()

        # First call — Zaychik fails
        with patch.object(
            trino_flight_mod,
            "execute_trino_flight",
            side_effect=RuntimeError("down"),
        ) as mock_flight:
            with patch("provisa.executor.trino.execute_trino", return_value=fallback_result):
                trino_flight_mod.execute_with_fallback(flight_conn, trino_conn, "SELECT 1")

        assert not trino_flight_mod.is_zaychik_available()

        # Second call — Zaychik should not be attempted
        with patch.object(
            trino_flight_mod,
            "execute_trino_flight",
        ) as mock_flight2:
            with patch("provisa.executor.trino.execute_trino", return_value=fallback_result):
                trino_flight_mod.execute_with_fallback(flight_conn, trino_conn, "SELECT 2")

        mock_flight2.assert_not_called()

    def test_execute_with_fallback_no_flight_conn_skips_zaychik(self):
        """When flight_conn is None, the REST path is used directly."""
        trino_conn = MagicMock()
        fallback_result = _make_query_result()

        with patch.object(
            trino_flight_mod,
            "execute_trino_flight",
        ) as mock_flight:
            with patch("provisa.executor.trino.execute_trino", return_value=fallback_result) as mock_trino:
                result = trino_flight_mod.execute_with_fallback(
                    None, trino_conn, "SELECT 1"
                )

        mock_flight.assert_not_called()
        mock_trino.assert_called_once()
        assert result is fallback_result
