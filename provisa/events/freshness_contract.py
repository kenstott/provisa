# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The expected-events FRESHNESS CONTRACT for a periodic MV (REQ-961).

A periodic MV's declared expected-events list is its report's freshness contract: the inputs that
must be fresh-THROUGH ``window.end`` for the output to be trusted. It is verified by a PULL against
per-input freshness state at fire time — NOT by receiving events (there is no NO_CHANGE event type).

- List length is the trust/latency dial. Default (undeclared) = all SQL-lineage inputs (REQ-939
  ``extract_inputs``). Empty = calendar-only (compute the closed period, verify nothing).
- "fresh-through window.end" = the input's source successfully refreshed to cover the window
  (``last_refresh_ok`` AND ``last_refresh_at >= window.end``), with zero or more rows — a fresh input
  with zero rows is a TRUSTWORTHY ZERO.
- A listed input NOT fresh-through window.end at the deadline is an OUTAGE (expected-but-absent) →
  warn/hold, never a silent skip. (A holiday removes the window entirely upstream — REQ-962 — so the
  expectation and the alarm never arise here.)

Pure decision (no I/O): the caller supplies ``freshness_of(input) -> FreshnessSubject`` reading the
per-input freshness state (``provisa/freshness/``). An input with no known freshness state is itself
an outage — fail loud, never assume fresh.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from provisa.freshness.subject import FreshnessSubject


@dataclass(frozen=True)
class ContractResult:
    """The freshness-contract verdict at fire time. ``trusted`` = every listed input is
    fresh-through window.end → seal the output. ``outages`` = the listed inputs that were not
    fresh-through window.end (empty when trusted)."""

    trusted: bool
    outages: tuple[str, ...]

    @property
    def is_outage(self) -> bool:
        return not self.trusted


def _fresh_through(subject: FreshnessSubject, window_end_ts: float) -> bool:
    """An input is fresh-through ``window_end_ts`` when its last refresh succeeded AND covered the
    window boundary (``last_refresh_at >= window_end``). Zero rows on such a refresh is still fresh —
    a trustworthy zero (REQ-961)."""
    if not subject.last_refresh_ok():
        return False
    at = subject.last_refresh_at()
    return at is not None and at >= window_end_ts


def evaluate_contract(
    expected_events: list[str],
    freshness_of: Callable[[str], FreshnessSubject],
    window_end_ts: float,
) -> ContractResult:
    """Verify the freshness contract for a periodic fire (REQ-961). ``expected_events`` is the listed
    input node set (default resolved upstream to all lineage inputs; empty = verify nothing →
    trusted). ``freshness_of`` reads each input's observed freshness state. All fresh-through
    ``window_end_ts`` → trusted; any not → an outage (warn/hold)."""
    outages = tuple(
        inp for inp in expected_events if not _fresh_through(freshness_of(inp), window_end_ts)
    )
    return ContractResult(trusted=not outages, outages=outages)
