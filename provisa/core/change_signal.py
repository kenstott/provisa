# Copyright (c) 2026 Kenneth Stott
# Canary: 4d7b1e90-3a2c-4f6b-9c1d-5e8a2b7f0c31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The single inbound change-detection axis (REQ-932).

`change_signal` (on Table/Source) subsumes two previously-independent signals whose value
sets it already contains: MV ``freshness_mode`` ({ttl, probe, ttl_probe}) and ``live.strategy``
({poll, native, debezium, kafka}). This module is the one place that resolves the effective
signal and derives the older representations from it.

- Poll signals {ttl, probe, ttl_probe} → the MV/cache freshness gate (poll → same mode).
- Push signals {native, debezium, kafka} → the subscription provider; push skips the freshness
  gate entirely (changes arrive as events, not by polling).

``watermark_column`` (append-vs-replace + subscribability) and materialization/reachability
(whether a copy is landed) are ORTHOGONAL axes, resolved elsewhere.
"""

from __future__ import annotations

POLL_SIGNALS = frozenset({"ttl", "probe", "ttl_probe"})
PUSH_SIGNALS = frozenset({"native", "debezium", "kafka"})
VALID_SIGNALS = POLL_SIGNALS | PUSH_SIGNALS
DEFAULT_SIGNAL = "ttl"


def resolve(
    table_signal: str | None, source_signal: str | None, *, default: str = DEFAULT_SIGNAL
) -> str:
    """Effective change signal: table override → source default → global default.

    A table's None means "inherit the source"; a source with no signal falls to ``default``.
    """
    sig = table_signal or source_signal or default
    if sig not in VALID_SIGNALS:
        raise ValueError(f"invalid change_signal {sig!r}; expected one of {sorted(VALID_SIGNALS)}")
    return sig


def is_poll(sig: str) -> bool:
    return sig in POLL_SIGNALS


def is_push(sig: str) -> bool:
    return sig in PUSH_SIGNALS


def to_freshness_mode(sig: str) -> str | None:
    """Poll signal → its freshness_mode (same value). Push signal → None (event-driven, no gate)."""
    return sig if sig in POLL_SIGNALS else None


def to_provider(sig: str, source_type: str) -> str:
    """Subscription provider for a push/poll signal.

    debezium/kafka name their own providers; native (source push) and poll (watermark) both
    dispatch on the source type — the *decision* was driven by the signal.
    """
    if sig == "debezium":
        return "debezium"
    if sig == "kafka":
        return "kafka"
    return source_type


# REQ-932 migration: legacy live.strategy → change_signal. Read-through only, until live.strategy
# is deleted (Phase 4). "poll" is watermark polling → the ttl poll signal.
_LEGACY_STRATEGY_TO_SIGNAL = {
    "poll": "ttl",
    "native": "native",
    "debezium": "debezium",
    "kafka": "kafka",
}


def from_legacy_strategy(strategy: str | None) -> str | None:
    """Map a legacy live.strategy value to its change_signal equivalent (None if absent/unknown)."""
    return _LEGACY_STRATEGY_TO_SIGNAL.get(strategy or "")


def resolve_effective(
    table_signal: str | None,
    source_signal: str | None,
    legacy_strategy: str | None = None,
) -> str:
    """Effective change signal at runtime: explicit table → legacy live.strategy → source → default.

    The legacy_strategy read-through keeps pre-REQ-932 configs (which set live.strategy but no
    change_signal) working until the field is removed in Phase 4.
    """
    return resolve(table_signal or from_legacy_strategy(legacy_strategy), source_signal)
