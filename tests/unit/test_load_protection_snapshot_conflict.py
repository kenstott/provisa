# Copyright (c) 2026 Kenneth Stott
# Canary: a68ea730-9706-4411-8425-9c543f5a3bed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1170: warn when load protection AND periodic snapshotting are both configured on one
table — a snapshot boundary may fall outside the off-peak refresh window, so the snapshot can
miss its intended instant. The check is a WARNING, not a block."""

from __future__ import annotations

import pytest

from provisa.api.admin.schema_mutation import _snapshot_load_protection_conflict


class TestSnapshotLoadProtectionConflict:
    def test_both_load_protected_and_snapshot_conflict(self):
        assert _snapshot_load_protection_conflict(True, None, "snapshot") is True

    def test_off_peak_window_alone_with_snapshot_conflicts(self):
        # An off-peak window implies load protection timing even when load_protected is None.
        assert _snapshot_load_protection_conflict(None, "22:00-06:00", "delta") is True

    def test_snapshot_without_protection_no_conflict(self):
        assert _snapshot_load_protection_conflict(False, None, "snapshot") is False
        assert _snapshot_load_protection_conflict(None, None, "snapshot") is False

    def test_protection_without_snapshot_no_conflict(self):
        assert _snapshot_load_protection_conflict(True, "22:00-06:00", None) is False
        assert _snapshot_load_protection_conflict(True, None, "") is False

    @pytest.mark.parametrize("mode", ["snapshot", "delta"])
    def test_each_bitemporal_mode_conflicts_under_protection(self, mode):
        assert _snapshot_load_protection_conflict(True, "22:00-06:00", mode) is True
