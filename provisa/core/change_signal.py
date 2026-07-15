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


# REQ-932: the landing shape a change signal drives when the source is materialized into a store.
# A push signal (debezium/kafka/native) carries per-row change events → CDC (upsert + tombstone by
# PK). A poll signal with a watermark carries a filtered delta → APPEND. A poll signal without a
# watermark has no deltas (a ttl lapse / probe just re-queries the whole result) → REPLACE.
REPLACE = "replace"
APPEND = "append"
CDC = "cdc"


def select_landing_shape(sig: str, watermark_column: str | None) -> str:
    """Landing shape for materializing a source whose effective change_signal is ``sig``.

    push → CDC (delta events, hard deletes); poll+watermark → APPEND (watermark-filtered delta);
    poll without a watermark → REPLACE (no deltas — full refresh). ``sig`` is validated upstream.
    """
    if is_push(sig):
        return CDC
    if watermark_column:
        return APPEND
    return REPLACE


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


# A table's live.strategy implies a change_signal when it declares no explicit one — the two are
# related axes (live.strategy picks the live-data transport; change_signal picks the refresh
# cadence). "poll" is watermark polling → the ttl poll signal.
_STRATEGY_TO_SIGNAL = {
    "poll": "ttl",
    "native": "native",
    "debezium": "debezium",
    "kafka": "kafka",
}


def signal_from_strategy(strategy: str | None) -> str | None:
    """The change_signal implied by a table's live.strategy (None if absent/unknown)."""
    return _STRATEGY_TO_SIGNAL.get(strategy or "")


def resolve_effective(
    table_signal: str | None,
    source_signal: str | None,
    live_strategy: str | None = None,
) -> str:
    """Effective change signal at runtime: explicit table change_signal → the signal implied by the
    table's live.strategy → source default → global default."""
    return resolve(table_signal or signal_from_strategy(live_strategy), source_signal)
