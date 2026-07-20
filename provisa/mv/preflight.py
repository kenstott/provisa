# Copyright (c) 2026 Kenneth Stott
# Canary: 8a3f1c6d-92b4-4e07-a1d5-7c0e9b26f483
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Preflight-check verdict vocabulary + runtime (REQ-1165).

REQ-1165 rescopes the REQ-957 hook from a row transform (``rows -> rows``) to a PREFLIGHT
CHECK: ``preflight(rows, ctx)`` inspects the produced rows and returns a VERDICT — one of
continue / abort / quarantine — and NEVER a mutated dataset. The verdict gates landing and
re-posting; the rows themselves pass through untouched (transforms belong in SQL or an
external processor, REQ-940).

Because a gate does not mutate the landed set it does not feed the content hash (REQ-964) —
which removes the reason the old contract had to fully materialize its output. This module
is the verdict vocabulary shared by both evaluation strategies (SQL pushdown in
:mod:`provisa.mv.preflight_sql`, Python+Arrow streaming in :mod:`provisa.processors.arrow`).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Decision(str, Enum):  # REQ-1165
    """The three preflight outcomes.

    - CONTINUE: the dataset passed — land it unchanged and ripple downstream.
    - ABORT: a FATAL data outcome — do not land; emit an ``error`` event + poison the fan-out
      (the REQ-957 fatal-reject path, now expressed as a verdict rather than a raise).
    - QUARANTINE: a non-fatal HOLD — do not land, do not poison; emit a ``quarantine`` event
      and record the node as not-fresh so a downstream contract sees a hold, not fresh data.
    """

    CONTINUE = "continue"
    ABORT = "abort"
    QUARANTINE = "quarantine"


@dataclass(frozen=True)
class Verdict:  # REQ-1165
    """A preflight decision plus an optional operator-facing reason."""

    decision: Decision
    reason: str | None = None

    @property
    def is_continue(self) -> bool:
        return self.decision is Decision.CONTINUE

    @property
    def is_abort(self) -> bool:
        return self.decision is Decision.ABORT

    @property
    def is_quarantine(self) -> bool:
        return self.decision is Decision.QUARANTINE


# Reusable singletons for the common no-reason continue.
CONTINUE = Verdict(Decision.CONTINUE)


class PreflightContractError(TypeError):
    """A preflight hook returned something that is not a coercible verdict (REQ-1165)."""


def to_verdict(result: Any) -> Verdict:
    """Normalize whatever a hook returned into a :class:`Verdict` (REQ-1165).

    Accepted authoring shapes, in addition to a :class:`Verdict` built via the ``ctx`` helpers:
    ``None`` / ``True`` → continue, ``False`` → abort, and the bare decision strings
    ``"continue"`` / ``"abort"`` / ``"quarantine"``. A rescoped hook returns a VERDICT, never
    rows — so a list/dict return is a contract error (fail loud), not a silent transform.
    """
    if isinstance(result, Verdict):
        return result
    if result is None or result is True:
        return CONTINUE
    if result is False:
        return Verdict(Decision.ABORT)
    if isinstance(result, str):
        try:
            return Verdict(Decision(result))
        except ValueError:
            raise PreflightContractError(
                f"preflight returned unknown decision {result!r}; "
                f"expected one of {[d.value for d in Decision]}"
            ) from None
    raise PreflightContractError(
        f"preflight must return a verdict (via ctx.ok/abort/quarantine, a decision string, "
        f"or a bool), got {type(result).__name__} — a preflight CHECK does not return rows"
    )


async def run_preflight(
    fn: Callable[..., Any] | None, rows: Any, ctx: Any
) -> Verdict:
    """Invoke a compiled preflight hook and return its normalized verdict (REQ-1165).

    ``fn`` None → no hook → :data:`CONTINUE`. The hook may be sync or async. Its raw return is
    coerced via :func:`to_verdict`; a hook that ``raise``s is handled by the caller (an
    uncaught raise is the fatal-abort path — see :mod:`provisa.events.handlers`)."""
    if fn is None:
        return CONTINUE
    out = fn(rows, ctx)
    if inspect.isawaitable(out):
        out = await out
    return to_verdict(out)
