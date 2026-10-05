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
# REQ-1149: a DATA-LESS external trigger ("refresh now" via a Kafka control message or an HTTP
# webhook) that carries no rows. It sits between the axes: push-TRIGGERED timing, but poll-style
# full/delta re-pull from the source of truth (it never lands the message like CDC does). The trigger
# only INVALIDATES the snapshot; under REQ-1141 the load-protected scheduler owns WHEN the heavy pull
# runs, so ``signal`` is treated as a change-detector (a probe verdict), not a CDC carrier.
TRIGGER_SIGNALS = frozenset({"signal"})
VALID_SIGNALS = POLL_SIGNALS | PUSH_SIGNALS | TRIGGER_SIGNALS
DEFAULT_SIGNAL = "ttl"

# REQ-1907/REQ-929: the one canonical set of source types whose rows arrive by PUSH -- a listener
# (kafka/websocket) or an inbound endpoint (ingest) feeds the table, never a poll. Their
# change_signal is INHERENTLY a push signal, so the global ``ttl`` default never applies: a push
# source needs no cache_ttl (REQ-1907) and is served from what its push path keeps, never a replica
# build. push_wiring, role_ttl and source_loader all key off THIS set, never an ad-hoc literal (the
# narrower "runs a background listener" set -- kafka/websocket, not ingest -- is derived from it).
PUSH_SOURCE_TYPES = frozenset({"ingest", "websocket", "kafka"})


def push_signal_for_source_type(source_type: str) -> str:
    """The push change_signal a push source type carries, derived from the push transport each one
    declares in push_wiring and the push vocabulary (:data:`PUSH_SIGNALS`):

    - ``kafka`` -> ``kafka``: Kafka is its own named push provider (push_wiring builds a Kafka
      provider; :func:`to_provider` names ``kafka`` for the ``kafka`` signal).
    - ``websocket`` -> ``native``: push_wiring's websocket listener lands through the REQ-1733
      source-native CDC path (its ``_CHANGE_FEED_SIGNAL`` is ``native``); there is no distinct named
      transport.
    - ``ingest`` -> ``native``: the inbound HTTP endpoint delivers rows natively, again no distinct
      named transport.

    A non-push source type is a programming error (callers gate on :data:`PUSH_SOURCE_TYPES`)."""
    if source_type == "kafka":
        return "kafka"
    if source_type in ("ingest", "websocket"):
        return "native"
    raise ValueError(f"{source_type!r} is not a push source type")


def source_change_signal(source_type: str, configured_signal: str | None) -> str:
    """The effective change_signal for a source of ``source_type`` given its configured signal
    (REQ-929/REQ-1907). For a push source type (:data:`PUSH_SOURCE_TYPES`):

    - an UNSET signal (``None``) resolves to the type's push signal
      (:func:`push_signal_for_source_type`);
    - an EXPLICIT non-push signal is a configuration error, refused by name -- a push source cannot
      be polled, so a ``ttl``/``probe`` on it is never silently overridden.

    A non-push type resolves an unset signal to the global default; an explicit one is returned as
    given (its validity is checked by :func:`resolve`)."""
    if source_type in PUSH_SOURCE_TYPES:
        if configured_signal is None:
            return push_signal_for_source_type(source_type)
        if not is_push(configured_signal):
            raise ValueError(
                f"source type {source_type!r} is push; change_signal {configured_signal!r} is not "
                f"a push signal (expected one of {sorted(PUSH_SIGNALS)}, or leave it unset)"
            )
        return configured_signal
    return configured_signal if configured_signal is not None else DEFAULT_SIGNAL


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


def is_trigger(sig: str) -> bool:
    """A data-less external refresh trigger (REQ-1149) — push-timed but poll-style re-pull."""
    return sig in TRIGGER_SIGNALS


def to_freshness_mode(sig: str) -> str | None:
    """Poll signal → its freshness_mode (same value). Push/trigger → None (event-driven, no poll gate)."""
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
    if sig == "signal":  # REQ-1149: a data-less trigger receiver (kafka control topic / webhook)
        return "signal"
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
