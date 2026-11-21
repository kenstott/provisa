# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2e4a71-0f9d-4b36-a5e7-d41b6c93f208
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The steps inside one timed act, each timed as the act's own stages are (REQ-2006).

A demo sandbox's open names its stages (demo_sandbox._stage); what an environment's creation, a
runtime's lookup or a model's write-through spends inside one of them is named here, on the same
line, while the open is under way: ``<label>: <step> took <s>s`` at WARNING, as a hosted tier
logs. Outside such an act a step logs nothing."""

# Requirements: REQ-2006

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Generator
from contextvars import ContextVar

log = logging.getLogger(__name__)

#: The act whose every step is timed, by the label its own stages are logged under.
_label: ContextVar[str | None] = ContextVar("step_clock_label", default=None)


@contextlib.contextmanager
def every_step_of(label: str) -> Generator[None]:
    """Time each :func:`step` taken within, under ``label``."""
    token = _label.set(label)
    try:
        yield
    finally:
        _label.reset(token)


def timing() -> bool:
    """Whether the steps taken now are timed."""
    return _label.get() is not None


def took(step: str, seconds: float) -> None:
    """Log ``step`` as having taken ``seconds``, when steps are timed now."""
    label = _label.get()
    if label is not None:
        log.warning("%s: %s took %.1fs", label, step, seconds)


@contextlib.contextmanager
def step(name: str) -> Generator[None]:
    """Time the step ``name``, when steps are timed now."""
    started = time.monotonic()
    try:
        yield
    finally:
        took(name, time.monotonic() - started)
