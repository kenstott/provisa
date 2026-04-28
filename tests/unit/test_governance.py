# Copyright (c) 2026 Kenneth Stott
# Canary: 36938b54-ad4d-467a-9510-c4f878eb4ef4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for governance mode routing logic."""

import pytest

from provisa.registry.governance import (
    GovernanceError,
    GovernanceMode,
    check_deprecated,
    check_governance,
    check_output_type,
)


TABLE_GOV = {1: "registry-required", 2: "pre-approved", 3: "registry-required"}


class TestCheckGovernance:
    def test_test_mode_allows_all(self):
        check_governance(GovernanceMode.TEST, [1, 2, 3], TABLE_GOV, stable_id=None)

    def test_production_pre_approved_no_registry(self):
        check_governance(GovernanceMode.PRODUCTION, [2], TABLE_GOV, stable_id=None)

    def test_production_registry_required_with_stable_id(self):
        check_governance(GovernanceMode.PRODUCTION, [1], TABLE_GOV, stable_id="abc-123")

    def test_production_registry_required_without_stable_id_rejected(self):
        with pytest.raises(GovernanceError, match="approved query"):
            check_governance(GovernanceMode.PRODUCTION, [1], TABLE_GOV, stable_id=None)

    def test_production_mixed_tables(self):
        """Pre-approved + registry-required: registry-required needs stable_id."""
        with pytest.raises(GovernanceError):
            check_governance(GovernanceMode.PRODUCTION, [1, 2], TABLE_GOV, stable_id=None)

    def test_production_mixed_with_stable_id(self):
        check_governance(GovernanceMode.PRODUCTION, [1, 2], TABLE_GOV, stable_id="abc")


class TestCheckDeprecated:
    def test_active_query_passes(self):
        check_deprecated({"status": "approved", "stable_id": "abc"})

    def test_deprecated_raises(self):
        with pytest.raises(GovernanceError, match="deprecated"):
            check_deprecated({"status": "deprecated", "stable_id": "old", "deprecated_by": "new"})

    def test_deprecated_with_replacement(self):
        with pytest.raises(GovernanceError, match="new-id"):
            check_deprecated({"status": "deprecated", "stable_id": "old", "deprecated_by": "new-id"})

    def test_deprecated_without_replacement(self):
        with pytest.raises(GovernanceError, match="deprecated"):
            check_deprecated({"status": "deprecated", "stable_id": "old", "deprecated_by": None})


class TestCheckOutputType:
    def test_json_default_allowed(self):
        check_output_type({"permitted_outputs": ["json"]}, "json")

    def test_multiple_permitted(self):
        check_output_type({"permitted_outputs": ["json", "ndjson", "parquet"]}, "ndjson")

    def test_not_permitted_rejected(self):
        with pytest.raises(GovernanceError, match="arrow"):
            check_output_type({"permitted_outputs": ["json"]}, "arrow")
