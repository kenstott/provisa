# Copyright (c) 2026 Kenneth Stott
# Canary: e1fe5620-04ef-4499-9dba-2b04fbe8b09e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Teardown of what a test left in a live external account: every step attempted, none hidden.

REQ-254: a test tears down everything it created in a live account, in teardown that runs even
when the test fails. A teardown of several steps (drop the test's objects, close the connection)
must not stop at the first one that fails -- the rest would be left behind -- and must not log a
failure away either: an object left in the account bills. ``run_every`` runs each step, then
raises every failure together.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable


def run_every(steps: Iterable[tuple[str, Callable[[], object]]]) -> None:
    """Run each ``(what, step)`` in order, even after one fails; then raise every failure in one
    ExceptionGroup, each noted with what it was tearing down."""
    failures: list[Exception] = []
    for what, step in steps:
        try:
            step()
        except Exception as exc:  # noqa: BLE001 -- collected and re-raised below, never dropped
            exc.add_note(f"teardown step: {what}")
            failures.append(exc)
    if failures:
        raise ExceptionGroup(
            f"{len(failures)} teardown step(s) failed; the live account may hold what the test "
            "created",
            failures,
        )
