# Copyright (c) 2026 Kenneth Stott
# Canary: 80e7ff2d-bcee-4fb0-9953-8bac23759493
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The mode a process was started in (REQ-1916).

A process starts in one of three modes: ``every`` (the default: one process doing all of it),
``control`` (it signs users in, governs, compiles and plans, holds the model store, serves the
admin API and UI and runs the background work; it never touches a row) or ``data`` (it owns the
client-facing listeners, asks a control node for the signed plan of each request, runs that plan
and encodes the answer itself, and reports counters back). The mode is chosen when the process is
launched and does not change while it runs; it is not an operator setting.

The launch entry sets the mode once, before anything asks.
"""

# Requirements: REQ-1916

from __future__ import annotations

EVERY = "every"
CONTROL = "control"
DATA = "data"
MODES = (EVERY, CONTROL, DATA)

_mode = EVERY


def set_mode(mode: str) -> None:
    """Record the mode this process was launched in. Called once by the launch entry."""
    global _mode
    if mode not in MODES:
        raise ValueError(f"unknown process mode {mode!r}; expected one of {MODES}")
    _mode = mode


def mode() -> str:
    """The mode this process was launched in."""
    return _mode


class ControlServesNoData(RuntimeError):
    """A data request reached a control node (REQ-1916), which serves none."""

    code = "node.control_serves_no_data"

    def __init__(self, transport: str) -> None:
        self.transport = transport
        super().__init__(
            f"this node runs in control mode and serves no data requests ({transport}); "
            "send them to a node in data or every mode"
        )


def refuse_data_request(transport: str) -> None:
    """Refuse a data request arriving on ``transport`` when this process is a control node. Called
    at each transport's request boundary (request_deadline.open_request, the Flight request
    budget, the HTTP ``/data`` gate), before anything is read."""
    if _mode == CONTROL:
        raise ControlServesNoData(transport)


def runs_background_work() -> bool:
    """Whether this process does the background work: replica builds, and the scheduled work.
    True for ``every`` and ``control``; a data node runs it only as a command a control node sends
    it."""
    return _mode != DATA


def plans_remotely() -> bool:
    """Whether this process asks a control node for each request's plan (``data``) instead of
    governing and routing it in process (``every``, ``control``)."""
    return _mode == DATA
