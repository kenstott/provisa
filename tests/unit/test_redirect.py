# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for large result redirect logic."""

from provisa.executor.redirect import RedirectConfig, should_redirect
from provisa.executor.trino import QueryResult


def _config(enabled=True, threshold=10):
    return RedirectConfig(
        enabled=enabled, threshold=threshold,
        bucket="test", endpoint_url="http://localhost:9000",
        access_key="key", secret_key="secret", ttl=3600,
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

    def test_pre_approved_table_no_redirect(self):
        """REQ-006: pre-approved tables cannot use redirect."""
        assert not should_redirect(
            _result(100), _config(),
            table_governance={1: "pre-approved"},
            target_table_ids=[1],
        )

    def test_registry_required_table_redirects(self):
        assert should_redirect(
            _result(100), _config(),
            table_governance={1: "registry-required"},
            target_table_ids=[1],
        )

    def test_mixed_tables_pre_approved_blocks(self):
        """If any table is pre-approved, no redirect."""
        assert not should_redirect(
            _result(100), _config(),
            table_governance={1: "registry-required", 2: "pre-approved"},
            target_table_ids=[1, 2],
        )

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

    def test_force_redirect_pre_approved_blocked(self):
        """REQ-006 still applies even with force."""
        assert not should_redirect(
            _result(5), _config(),
            table_governance={1: "pre-approved"},
            target_table_ids=[1],
            force=True,
        )


class TestRedirectConfig:
    def test_from_env_defaults(self):
        config = RedirectConfig.from_env()
        assert not config.enabled  # default false
        assert config.threshold == 1000
        assert config.ttl == 3600
