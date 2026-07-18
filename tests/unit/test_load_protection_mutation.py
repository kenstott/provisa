# Copyright (c) 2026 Kenneth Stott
# Canary: 1a5d3c92-2e48-4f17-9b06-7e8c2d4f3f7a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1141: admin mutation guards for load protection — ≥1-gate rule + window validation."""

from __future__ import annotations

from provisa.api.admin.schema_mutation import _parsed_off_peak, _validate_load_protection


def test_not_load_protected_never_fails():
    assert _validate_load_protection(False, None, None, "ttl", "s") is None
    assert _validate_load_protection(None, None, None, "ttl", "s") is None


def test_load_protected_no_gate_fails():
    res = _validate_load_protection(True, None, None, "ttl", "table 5")
    assert res is not None and res.success is False
    assert "at least one refresh gate" in res.message


def test_load_protected_armed_by_window():
    assert _validate_load_protection(True, "01:00-03:00", None, "ttl", "s") is None


def test_load_protected_armed_by_cadence():
    assert _validate_load_protection(True, None, 300, "ttl", "s") is None


def test_load_protected_armed_by_probe_signal():
    assert _validate_load_protection(True, None, None, "ttl_probe", "s") is None
    assert _validate_load_protection(True, None, None, "probe", "s") is None
    # a non-probing signal is not a gate on its own
    res = _validate_load_protection(True, None, None, "ttl", "s")
    assert res is not None and res.success is False


def test_parsed_off_peak_accepts_valid():
    assert _parsed_off_peak(None, "UTC") is None
    assert _parsed_off_peak("01:00-03:00", "UTC") is None
    assert _parsed_off_peak("22:00-02:00", "America/New_York") is None


def test_parsed_off_peak_rejects_malformed_window():
    res = _parsed_off_peak("25:00-03:00", "UTC")
    assert res is not None and res.success is False
    assert "invalid off-peak window" in res.message


def test_parsed_off_peak_rejects_unknown_tz():
    res = _parsed_off_peak("01:00-03:00", "Mars/Olympus")
    assert res is not None and res.success is False
