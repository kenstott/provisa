# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The single sanctioned best-effort boundary for startup wiring.

Setting up one external resource — a source connection pool, an OpenAPI spec, a
per-table ingest DDL — can fail for reasons entirely outside our control: an
unreachable host, a malformed third-party spec, a transient DB error. Such a
failure must not abort whole-server startup; the server should come up serving
every healthy resource and log what it skipped.

`tolerate_startup_failure` is the ONE place allowed to swallow a broad exception,
and only around startup wiring. Everywhere else, let errors propagate.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager

log = logging.getLogger(__name__)


@contextmanager
def tolerate_startup_failure(what: str, *, exc_info: bool = False):
    """Log-and-skip any failure setting up one external resource during startup.

    ``what`` describes the resource (e.g. ``"direct pool for 'sales' (db:5432)"``)
    so the skip is diagnosable. Wraps a block that may ``await`` — the awaits run
    inside the ``with`` body, so their exceptions reach this boundary.
    """
    try:
        yield
    # complexity-gate: allow-ble=1 reason="THE one sanctioned startup-resilience boundary: an individual external-resource setup failing (unreachable host / malformed 3rd-party spec / transient DB error) must not abort whole-server startup — it is logged and skipped so healthy resources still come up. Every scattered startup try/except funnels through here instead of swallowing broadly in place."
    except Exception as exc:
        if exc_info:
            log.warning("[startup] %s failed — skipped", what, exc_info=True)
        else:
            log.warning("[startup] %s failed — skipped: %s", what, exc)


@contextmanager
def tolerate_shutdown_failure(what: str):
    """Log-and-skip any failure tearing down one subsystem during shutdown.

    The mirror of :func:`tolerate_startup_failure` for the teardown path: closing a
    pool, stopping the live engine, or shutting the scheduler can fail on an
    already-broken resource, but one failure must not abort the rest of shutdown.
    ``what`` describes the subsystem so the skip is diagnosable.
    """
    try:
        yield
    # complexity-gate: allow-ble=2 reason="THE one sanctioned shutdown-resilience boundary: tearing down one subsystem (live engine / APQ cache / scheduler) failing on an already-broken resource must not abort the remaining shutdown steps — it is logged and skipped. Mirrors tolerate_startup_failure for the teardown path so scattered shutdown try/excepts funnel through here instead of swallowing broadly in place. File ceiling is 2: this boundary plus tolerate_startup_failure above."
    except Exception as exc:
        log.warning("[shutdown] %s failed — skipped: %s", what, exc)
