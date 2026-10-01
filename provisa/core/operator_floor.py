# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1d9e37-4f2b-4c80-b5e6-0d8c3a7f1e92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The operator's settings are the FLOOR of every request (REQ-030, amended 2026-09-30).

Three roles meet at a query. Ideally the upstream source manages its own backpressure. The
operator protects the platform (Provisa itself) from backpressure, and the upstream too when it
cannot protect itself — through source/table settings such as ``load_protected``,
``prefer_materialized`` and the large-result redirect threshold. The end user trades performance
against recency, per request, above that floor: a request may accept staler data or lighter
delivery, never fresher data or more load than the operator allows. A request hint that would go
below the floor is rejected with this error, naming the setting — never silently ignored.
"""

# Requirements: REQ-030


class OperatorFloorError(PermissionError):
    """A request asked for something below the operator's floor.

    A PermissionError, so every transport's existing mapping carries it to the caller (SQLSTATE
    42501 on pgwire, PERMISSION_DENIED on Flight/gRPC); the HTTP app maps it to 403 with the stable
    code ``query.operator_floor``."""


def floor_setting(source: object) -> str | None:  # REQ-030, REQ-826, REQ-1141
    """The operator setting that floors ``source``'s reads to its landed copy, or None.

    ``load_protected`` and ``prefer_materialized`` each block live reads on their own (REQ-826,
    amended 2026-09-30). When the copy refreshes follows the normal rules (REQ-1907): read-triggered
    only when a TTL and/or freshness check says so; with neither it lands once and then refreshes
    only through a change feed or the scheduler. The one definition shared by routing, the engine
    attach, the landing reconcile and the land loader, so they never disagree about a source."""
    if getattr(source, "load_protected", False):
        return "load_protected"
    if getattr(source, "prefer_materialized", False):
        return "prefer_materialized"
    return None
