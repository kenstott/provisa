# Copyright (c) 2026 Kenneth Stott
# Canary: 6f915d4b-6495-444e-8aa9-5436357ddb99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Trino engine read's retry policy and the shape of a dial that reached no coordinator.

Reads run through the Zaychik Arrow Flight SQL proxy (``executor/trino_flight.py``, REQ-2194);
this module says which of their failures are retried and which mean the engine is unreachable.
"""

# Requirements: REQ-028, REQ-054, REQ-277, REQ-278, REQ-279, REQ-302, REQ-303

from __future__ import annotations

import random

import trino

# Error names that indicate coordinator loss — safe to retry on read-only SQL.
_RETRYABLE_ERROR_NAMES: frozenset[str] = frozenset(
    {
        "COORDINATOR_NOT_AVAILABLE",
        "SERVER_SHUTTING_DOWN",
        "SERVER_STARTING_UP",  # coordinator restarted mid-query — comes back through this state
        "NO_NODES_AVAILABLE",
        "TOO_MANY_REQUESTS_FAILED",
        "REMOTE_TASK_FAILED",
    }
)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, ConnectionError):
        return True
    from provisa.executor.errors import FederationError, SystemCatalogUnavailable

    if isinstance(exc, SystemCatalogUnavailable):
        # One of Provisa's own catalogs is between its drop and its create: it is back in moments.
        return True
    if isinstance(exc, FederationError):
        return exc.error_name in _RETRYABLE_ERROR_NAMES
    return False


def is_connection_error(exc: BaseException) -> bool:
    """Whether ``exc`` is the driver's own "never reached a coordinator" error (REQ-1448).

    Lives here because this module owns the trino driver: a caller that needs to recognise the
    shape of a failed dial asks the owner what that shape is rather than importing the driver to
    look at it itself.
    """
    from provisa.executor.trino_flight import EngineUnreachable

    return isinstance(exc, (trino.exceptions.TrinoConnectionError, EngineUnreachable))


def _backoff_secs(attempt: int, cap: float = 30.0) -> float:
    """Full-jitter exponential backoff — spreads retries to avoid thundering herd."""
    return random.uniform(0, min(cap, 1.0 * (2**attempt)))


def _trino_query_timeout() -> int:
    # REQ-1913: an operator setting, resolved through the registry on every query.
    from provisa.core import settings_registry

    return settings_registry.value("limits.engine_query_timeout")


def _retry_budget() -> float:
    from provisa.core import settings_registry  # REQ-1913

    return settings_registry.value("limits.retry_budget_secs")
