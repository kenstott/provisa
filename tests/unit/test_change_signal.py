# Copyright (c) 2026 Kenneth Stott
# Canary: f5755318-0474-4450-8786-d8024a01386f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-932: the single inbound change-detection axis and its derivations."""

import pytest

from provisa.core.change_signal import (
    APPEND,
    CDC,
    DEFAULT_SIGNAL,
    POLL_SIGNALS,
    PUSH_SIGNALS,
    REPLACE,
    VALID_SIGNALS,
    signal_from_strategy,
    is_poll,
    is_push,
    resolve,
    resolve_effective,
    select_landing_shape,
    to_freshness_mode,
    to_provider,
)


class TestSelectLandingShape:
    def test_push_signal_is_cdc(self):
        for sig in ("debezium", "kafka", "native"):
            assert select_landing_shape(sig, None) == CDC
            assert select_landing_shape(sig, "updated_at") == CDC  # push ignores watermark

    def test_poll_with_watermark_appends(self):
        assert select_landing_shape("ttl_probe", "updated_at") == APPEND
        assert select_landing_shape("probe", "seq") == APPEND

    def test_poll_without_watermark_replaces(self):
        assert select_landing_shape("ttl", None) == REPLACE
        assert select_landing_shape("probe", None) == REPLACE  # a probe has no deltas


class TestResolve:
    def test_table_overrides_source(self):
        assert resolve("probe", "ttl") == "probe"

    def test_none_table_inherits_source(self):
        assert resolve(None, "debezium") == "debezium"

    def test_both_none_falls_to_default(self):
        assert resolve(None, None) == DEFAULT_SIGNAL == "ttl"

    def test_source_none_uses_table(self):
        assert resolve("kafka", None) == "kafka"

    def test_invalid_signal_rejected(self):
        with pytest.raises(ValueError, match="invalid change_signal"):
            resolve("bogus", None)


class TestClassification:
    @pytest.mark.parametrize("sig", ["ttl", "probe", "ttl_probe"])
    def test_poll_signals(self, sig):
        assert is_poll(sig) and not is_push(sig)

    @pytest.mark.parametrize("sig", ["native", "debezium", "kafka"])
    def test_push_signals(self, sig):
        assert is_push(sig) and not is_poll(sig)

    def test_sets_are_disjoint_and_cover_valid(self):
        assert POLL_SIGNALS.isdisjoint(PUSH_SIGNALS)
        assert POLL_SIGNALS | PUSH_SIGNALS == VALID_SIGNALS


class TestToFreshnessMode:
    @pytest.mark.parametrize("sig", ["ttl", "probe", "ttl_probe"])
    def test_poll_passes_through(self, sig):
        assert to_freshness_mode(sig) == sig

    @pytest.mark.parametrize("sig", ["native", "debezium", "kafka"])
    def test_push_has_no_gate(self, sig):
        assert to_freshness_mode(sig) is None


class TestToProvider:
    def test_debezium(self):
        assert to_provider("debezium", "postgresql") == "debezium"

    def test_kafka(self):
        assert to_provider("kafka", "postgresql") == "kafka"

    @pytest.mark.parametrize("sig", ["native", "ttl", "probe", "ttl_probe"])
    def test_native_and_poll_dispatch_on_source_type(self, sig):
        assert to_provider(sig, "postgresql") == "postgresql"
        assert to_provider(sig, "mongodb") == "mongodb"


class TestLegacyStrategyShim:
    def test_poll_maps_to_ttl(self):
        assert signal_from_strategy("poll") == "ttl"

    @pytest.mark.parametrize("s", ["native", "debezium", "kafka"])
    def test_push_strategies_map_identically(self, s):
        assert signal_from_strategy(s) == s

    def test_unknown_and_none(self):
        assert signal_from_strategy(None) is None
        assert signal_from_strategy("bogus") is None


class TestResolveEffective:
    def test_explicit_table_signal_wins_over_legacy(self):
        assert resolve_effective("probe", None, "debezium") == "probe"

    def test_strategy_implied_signal(self):
        assert resolve_effective(None, None, "debezium") == "debezium"
        assert resolve_effective(None, None, "poll") == "ttl"

    def test_source_inherit_when_no_table_or_legacy(self):
        assert resolve_effective(None, "kafka", None) == "kafka"

    def test_all_absent_falls_to_default(self):
        assert resolve_effective(None, None, None) == DEFAULT_SIGNAL
