# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source-level freshness gate (REQ-860).

Freshness gating, which existed only on MV (REQ-859), applies to a Source too: when a
Source declares ``freshness_gate=True``, a query reading a table from that source is gated
by a freshness decision before execution. This module builds the Source's freshness
:class:`Strategy` from its ``change_signal`` + ``cache_ttl`` (the same predicate machinery MV
uses, REQ-856/858) and exposes a pure ``gate_source`` decision. Triggering the refresh/produce
path on a stale/failed verdict is the caller's responsibility (REQ-856) — see
``provisa/federation/plan.py`` where the gate feeds the residency prep phase.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.freshness.adapters import StateSubject
from provisa.freshness.decision import Decision
from provisa.freshness.predicate import Probe, Strategy, Ttl, TtlThenProbe, evaluate

if TYPE_CHECKING:
    from provisa.core.models import Source
    from provisa.freshness.subject import FreshnessSubject


def source_strategy(source: Source) -> Strategy:
    """Map a Source's ``change_signal`` + ``cache_ttl`` to a freshness :class:`Strategy` (REQ-860).

    Mirrors how MV maps ``freshness_mode`` + ``refresh_interval``:

    - ``ttl``       → :class:`Ttl` (requires ``cache_ttl``)
    - ``probe``     → :class:`Probe`
    - ``ttl_probe`` → :class:`TtlThenProbe` (requires ``cache_ttl``)

    Gating requires a strategy the predicate can build (the model's ``freshness_gate`` comment):
    a TTL-based signal with no ``cache_ttl`` is unconfigured and raises — never a silent default.
    Push change_signals (native/debezium/kafka) carry no read-time freshness predicate and raise.
    """
    signal = source.change_signal
    ttl = source.cache_ttl
    if signal == "ttl":
        if ttl is None:
            raise ValueError(
                f"source {source.id!r}: freshness_gate with change_signal='ttl' requires cache_ttl"
            )
        return Ttl(float(ttl))
    if signal == "probe":
        return Probe()
    if signal == "ttl_probe":
        if ttl is None:
            raise ValueError(
                f"source {source.id!r}: freshness_gate with change_signal='ttl_probe' requires "
                f"cache_ttl"
            )
        return TtlThenProbe(float(ttl))
    raise ValueError(
        f"source {source.id!r}: change_signal={signal!r} has no read-time freshness predicate "
        f"(freshness_gate supports ttl | probe | ttl_probe)"
    )


def source_subject(
    refreshed_at: float | None,
    *,
    ok: bool = True,
    token: str | None = None,
    baseline: str | None = None,
    upstream: tuple = (),
) -> StateSubject:
    """A Source's observed residency state as a :class:`FreshnessSubject` (REQ-857/859).

    Thin wrapper over :class:`StateSubject`: the Source model holds no runtime residency state
    (that lives in the materialization store), so the caller supplies the observed values —
    ``refreshed_at`` (unix ts, None = never landed), ``ok`` (last land outcome), and the PROBE
    ``token``/``baseline`` when the source produces a change token.
    """
    return StateSubject(
        refreshed_at=refreshed_at, ok=ok, token=token, baseline=baseline, upstream_subjects=upstream
    )


def gate_source(source: Source, subject: FreshnessSubject, now: float) -> Decision:
    """Evaluate a freshness-gated Source's read gate (REQ-860). Pure — no side effects.

    Fails loud if called on a source that has not opted into gating: gating is opt-in
    (``freshness_gate=True``) and the caller must not gate an ungated source.
    """
    if not source.freshness_gate:
        raise ValueError(f"source {source.id!r}: gate_source called but freshness_gate is False")
    return evaluate(subject, source_strategy(source), now)
