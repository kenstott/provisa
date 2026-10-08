# Copyright (c) 2026 Kenneth Stott
# Canary: f42a1dbb-f75e-4ecc-bc8e-59df74f3413a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Work bound to an org or environment whose runtime this process no longer holds (REQ-1266,
REQ-1529), and the one place detached work says what it does about it."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


class RuntimeNotBuilt(RuntimeError):
    """Work is bound to an org or environment whose runtime this process does not hold: never
    built here, or dropped (an engine wake, a change of the environment's data, a failed build)
    while work for it was still running. A request is refused with it. Detached work is left to
    the runtime that replaces this one (:func:`left_to_the_next_runtime`)."""

    def __init__(self, org_id: str, env: str | None, message: str) -> None:
        super().__init__(message)
        self.org_id, self.env = org_id, env


def left_to_the_next_runtime(gone: RuntimeNotBuilt, work: str) -> None:
    """THE one place a background pass states what it does when the runtime it runs for is gone:
    nothing. Wiring a runtime's jobs, converging its replicas and the like are done for a runtime
    by its own build; a pass detached from a build that was since dropped or replaced has no
    runtime to do them for, and the build that replaces it runs its own. Said once, at debug --
    it is the ordinary result of an environment's data or an engine changing under a pass, not a
    failure to report with a traceback."""
    log.debug(
        "%s left to the next runtime of org %s%s: %s",
        work,
        gone.org_id,
        f" environment {gone.env}" if gone.env else "",
        gone,
    )
