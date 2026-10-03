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

A process starts in one of three modes: ``every`` (the default: it serves requests and does the
coordinator's background work), ``query`` (requests only) or ``coordinator`` (background work
only). The mode is chosen when the process is launched and does not change while it runs; it is
not an operator setting.

The launch entry sets the mode once, before anything asks. Until the launch flag exists every
process is ``every``.
"""

# Requirements: REQ-1916

from __future__ import annotations

EVERY = "every"
QUERY = "query"
COORDINATOR = "coordinator"
MODES = (EVERY, QUERY, COORDINATOR)

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


class CoordinatorServesNoData(RuntimeError):
    """A data request reached a coordinator (REQ-1916), which serves none."""

    code = "node.coordinator_serves_no_data"

    def __init__(self, transport: str) -> None:
        self.transport = transport
        super().__init__(
            f"this node runs in coordinator mode and serves no data requests ({transport}); "
            "send them to a node in query or every mode"
        )


def refuse_data_request(transport: str) -> None:
    """Refuse a data request arriving on ``transport`` when this process is a coordinator. Called
    at each transport's request boundary (request_deadline.open_request, the Flight request
    budget, the HTTP ``/data`` gate), before anything is read."""
    if _mode == COORDINATOR:
        raise CoordinatorServesNoData(transport)


def runs_background_work() -> bool:
    """Whether this process does the coordinator's background work: replica builds, and the
    scheduled work a coordinator runs. True for ``every`` and ``coordinator``."""
    return _mode != QUERY
