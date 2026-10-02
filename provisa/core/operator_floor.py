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
cannot protect itself — through source/table settings such as ``load_protected``, ``replicate``
and the large-result redirect threshold. The end user trades performance
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
    """The operator setting that floors EVERY read of ``source`` to its replicas, or None.

    ``load_protected`` and ``replicate: 0`` (always) on the source each do: the source then has no
    live attach on any engine, so every table of it is served from its replica, whatever a table
    says for itself. A table's own setting on a source that is not floored is judged per table
    (``core.replicate.floor_of``). When a replica refreshes follows the normal rules (REQ-1907).
    The one definition shared by the engine attach, admin discovery and the replica reconcile, so
    they never disagree about a source."""
    from provisa.core.replicate import floor_of

    return floor_of(
        getattr(source, "replicate", None),
        bool(getattr(source, "load_protected", False)),
        promoted=False,
    )
