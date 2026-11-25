# Copyright (c) 2026 Kenneth Stott
# Canary: 476b1350-2b8d-4ee2-9d75-66d8caa927ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for large result redirect logic."""

import json
from decimal import Decimal

from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.redirect import RedirectConfig, _serialize_for_redirect, should_redirect
from provisa.executor.result import QueryResult

# A value whose 18 digits are corrupted to 17 by float64 (REQ-1961).
_EXACT = Decimal("1234567890123456.78")


def _config(enabled=True, threshold=10):
    return RedirectConfig(
        enabled=enabled,
        threshold=threshold,
        bucket="test",
        endpoint_url="http://localhost:9000",
        access_key="key",
        secret_key="secret",
        ttl=3600,
    )


def _result(n_rows):
    return QueryResult(
        rows=[tuple(range(3)) for _ in range(n_rows)],
        column_names=["a", "b", "c"],
    )


class TestShouldRedirect:
    def test_below_threshold_no_redirect(self):
        assert not should_redirect(_result(5), _config(threshold=10))

    def test_at_threshold_no_redirect(self):
        assert not should_redirect(_result(10), _config(threshold=10))

    def test_above_threshold_redirect(self):
        assert should_redirect(_result(11), _config(threshold=10))

    def test_disabled_no_redirect(self):
        assert not should_redirect(_result(100), _config(enabled=False))

    def test_no_governance_info_redirects(self):
        """Without governance info, redirect based on threshold only."""
        assert should_redirect(_result(100), _config())

    def test_empty_result_no_redirect(self):
        assert not should_redirect(_result(0), _config(threshold=0))

    def test_force_redirect_below_threshold(self):
        """Force redirect regardless of row count."""
        assert should_redirect(_result(1), _config(threshold=1000), force=True)

    def test_force_redirect_empty_result(self):
        assert should_redirect(_result(0), _config(), force=True)

    def test_force_redirect_disabled_no_redirect(self):
        """Force cannot override disabled redirect."""
        assert not should_redirect(_result(100), _config(enabled=False), force=True)


class TestRedirectConfig:
    def test_from_env_defaults(self):
        config = RedirectConfig.from_env()
        assert not config.enabled  # default false
        assert config.threshold == 100000
        assert config.ttl == 3600


class TestRedirectNumericExact:
    """REQ-1961: a sized numeric leaves redirect JSON/NDJSON exactly, never as float.

    REQ-1961 requires exact decimals on every read path. The redirect JSON and NDJSON serialisers
    previously used json.JSONEncoder.default → float(Decimal), silently dropping digits past ~17.
    """

    def _result_with_decimal(self):
        return QueryResult(
            rows=[(_EXACT,)],
            column_names=["amount"],
        )

    def _cols(self):
        return [ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None)]

    def test_redirect_json_decimal_exact(self):
        result = self._result_with_decimal()
        body, ct, ext = _serialize_for_redirect(result, self._cols(), "json")
        rows = json.loads(body)
        assert rows[0]["amount"] == "1234567890123456.78", (
            f"float conversion lost digits: got {rows[0]['amount']!r}"
        )

    def test_redirect_ndjson_decimal_exact(self):
        result = self._result_with_decimal()
        body, ct, ext = _serialize_for_redirect(result, self._cols(), "ndjson")
        row = json.loads(body.decode().strip())
        assert row["amount"] == "1234567890123456.78", (
            f"float conversion lost digits: got {row['amount']!r}"
        )
