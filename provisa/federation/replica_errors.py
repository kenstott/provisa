# Copyright (c) 2026 Kenneth Stott
# Canary: 172c92ae-a7a0-4514-a5f3-25e3b1b101b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a replica build tells an operator, as a stable code with params (REQ-1915, REQ-1350).

The server never translates. A build failure whose cause Provisa knows carries a code and
interpolation params beside its English message, and so does the reason a requested build has
not started; the UI renders the code from its own catalog (``serverErrors.replication.*``) and
falls back to the English text for a failure with no code — a driver's or a source's own error.
"""

# Requirements: REQ-1915, REQ-1350

from __future__ import annotations

from typing import Any


class BuildFailure:
    """Mixed into an exception a build can fail with whose cause is known: ``code`` names it
    and ``params`` fill its message. Param values are JSON values; their names match the
    ``{{name}}`` placeholders of the catalog entry for the code."""

    code: str
    params: dict[str, Any]


def coded(exc: BaseException) -> tuple[str | None, dict[str, Any] | None]:
    """The code and params ``exc`` carries, or (None, None) for a failure with none."""
    if isinstance(exc, BuildFailure):
        return exc.code, exc.params
    return None, None


#: Why a requested build did not start on the last pass that tried it: code -> English.
WAITING_ENGINE = "replication.waiting_engine_jobs"
WAITING_SOURCE = "replication.waiting_source_cap"
WAITING = {
    WAITING_ENGINE: "the engine is at its background job cap (replication.engine_jobs)",
    WAITING_SOURCE: "the source is at its live-read cap (max_live_concurrency)",
}
