# Copyright (c) 2026 Kenneth Stott
# Canary: 0a2c4e6f-8b1d-4739-9e5a-1f3d5b7a9c0e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for definition/input version stamping (REQ-862).

Store-independent — no target-engine dependency. Verifies the MV definition hash
and the capability-graded input-version resolution.
"""

from __future__ import annotations

from provisa.lineage import InputVersion, mv_definition_version, resolve_input_version


# --- definition version (store-independent) ------------------------------------


def test_definition_version_is_stable():
    a = mv_definition_version(sql="SELECT 1", source_tables=["t"])
    b = mv_definition_version(sql="SELECT 1", source_tables=["t"])
    assert a == b and a.startswith("sha256:")


def test_definition_version_changes_with_sql():
    a = mv_definition_version(sql="SELECT 1", source_tables=["t"])
    b = mv_definition_version(sql="SELECT 2", source_tables=["t"])
    assert a != b


def test_definition_version_source_order_independent():
    a = mv_definition_version(sql="SELECT 1", source_tables=["a", "b"])
    b = mv_definition_version(sql="SELECT 1", source_tables=["b", "a"])
    assert a == b


def test_definition_version_tracks_aggregate_config():
    a = mv_definition_version(sql="S", serves_aggregates=False)
    b = mv_definition_version(sql="S", serves_aggregates=True)
    assert a != b


# --- input version: capability grading (REQ-862) -------------------------------


def test_no_signals_falls_back_to_refresh_epoch():
    iv = resolve_input_version([], "1720000000")
    assert iv.kind == "refresh_epoch" and iv.value == "1720000000"


def test_iceberg_snapshot_wins_over_watermark():
    signals = [
        InputVersion("100", "watermark"),
        InputVersion("snap-abc", "iceberg_snapshot"),
    ]
    iv = resolve_input_version(signals, "epoch")
    assert iv.kind == "iceberg_snapshot" and iv.value == "snap-abc"


def test_watermark_wins_over_freshness_token():
    signals = [
        InputVersion("tok", "freshness_token"),
        InputVersion("42", "watermark"),
    ]
    assert resolve_input_version(signals, "epoch").kind == "watermark"


def test_freshness_token_used_when_only_signal():
    assert (
        resolve_input_version([InputVersion("t", "freshness_token")], "e").kind == "freshness_token"
    )


def test_empty_valued_signal_ignored():
    # A signal with no value degrades to the epoch fallback, not a blank version.
    iv = resolve_input_version([InputVersion("", "watermark")], "epoch")
    assert iv.kind == "refresh_epoch" and iv.value == "epoch"
