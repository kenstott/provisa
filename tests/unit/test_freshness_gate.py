# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8c2d40-6a18-4e75-9f02-1c7a0d4f9b91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-855: the centralized materialization_store freshness gate."""

from __future__ import annotations

from provisa.federation.freshness_gate import (
    FreshnessMode,
    evaluate_freshness,
)


def _eval(mode, **kw):
    base = dict(now=100.0, last_refresh_at=None, ttl=10.0, stored_token=None, probe=None)
    base.update(kw)
    return evaluate_freshness(mode, **base)


# ---- TTL mode ---------------------------------------------------------------


def test_ttl_fresh_within_interval():
    d = _eval(FreshnessMode.TTL, now=105.0, last_refresh_at=100.0, ttl=10.0)
    assert d.fresh is True and d.new_token is None


def test_ttl_stale_after_interval():
    d = _eval(FreshnessMode.TTL, now=115.0, last_refresh_at=100.0, ttl=10.0)
    assert d.fresh is False


def test_ttl_never_loaded_is_stale():
    assert _eval(FreshnessMode.TTL, last_refresh_at=None).fresh is False


# ---- PROBE mode (ignores TTL) -----------------------------------------------


def test_probe_unchanged_keeps_rows():
    d = _eval(
        FreshnessMode.PROBE,
        now=999.0,
        last_refresh_at=1.0,  # far past any TTL — but PROBE ignores TTL
        stored_token="v1",
        probe=lambda: "v1",
    )
    assert d.fresh is True and d.new_token == "v1"


def test_probe_changed_invalidates():
    d = _eval(FreshnessMode.PROBE, stored_token="v1", probe=lambda: "v2")
    assert d.fresh is False and d.new_token == "v2"


def test_probe_none_token_degrades_to_ttl():
    # Source cannot produce a token → degrade to TTL (capability signal, not silent stale).
    fresh_in_ttl = _eval(
        FreshnessMode.PROBE, now=105.0, last_refresh_at=100.0, ttl=10.0, probe=lambda: None
    )
    stale_past_ttl = _eval(
        FreshnessMode.PROBE, now=200.0, last_refresh_at=100.0, ttl=10.0, probe=lambda: None
    )
    assert fresh_in_ttl.fresh is True
    assert stale_past_ttl.fresh is False


# ---- TTL_PROBE mode (probe only after the TTL floor) ------------------------


def test_ttl_probe_within_floor_does_not_probe():
    calls = []

    def _probe():
        calls.append(1)
        return "v2"

    d = _eval(FreshnessMode.TTL_PROBE, now=105.0, last_refresh_at=100.0, ttl=10.0, probe=_probe)
    assert d.fresh is True
    assert calls == []  # probe not called within the TTL floor


def test_ttl_probe_past_floor_probes_and_detects_change():
    d = _eval(
        FreshnessMode.TTL_PROBE,
        now=200.0,
        last_refresh_at=100.0,
        ttl=10.0,
        stored_token="v1",
        probe=lambda: "v2",
    )
    assert d.fresh is False and d.new_token == "v2"


def test_ttl_probe_past_floor_unchanged_keeps():
    d = _eval(
        FreshnessMode.TTL_PROBE,
        now=200.0,
        last_refresh_at=100.0,
        ttl=10.0,
        stored_token="v1",
        probe=lambda: "v1",
    )
    assert d.fresh is True and d.new_token == "v1"
