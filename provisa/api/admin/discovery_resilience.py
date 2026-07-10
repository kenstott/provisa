# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1d0f2a-3b47-4e58-9a6d-71c2e8f04b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The single sanctioned best-effort boundary for admin-UI discovery reads.

Introspecting an external source — listing its schemas, tables, columns, or
pulling live cache/scheduler stats — can fail for reasons entirely outside our
control: an unreachable host, a source that no longer exposes an
``information_schema``, a transient engine error. Such a failure must not blank
out the admin UI; the dropdown should show what it can and fall back to a safe
default (an empty list, a noop stats object) for the parts it could not reach.

`discovery_fallback` is the ONE place allowed to swallow a broad exception on the
read side, and only around optional introspection. The caller seeds its default
before the block and keeps it when the block fails. Everywhere else, let errors
propagate.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager

log = logging.getLogger(__name__)


@contextmanager
def discovery_fallback(what: str):
    """Log-and-skip any failure of one best-effort introspection read.

    ``what`` describes the read (e.g. ``"engine schemata for 'sales'"``) so the
    skip is diagnosable. Wraps a block that may ``await`` — the awaits run inside
    the ``with`` body, so their exceptions reach this boundary. The caller keeps
    whatever default it assigned before entering the block.
    """
    try:
        yield
    # complexity-gate: allow-ble=1 reason="THE one sanctioned discovery-read boundary: an optional source introspection (list schemas/tables/columns) or live-stats read failing (unreachable source / no information_schema / transient engine error) must not blank the admin UI — it is logged and the caller keeps its safe default. Every scattered read-side try/except funnels through here instead of swallowing broadly in place. Mirrors startup_resilience.tolerate_startup_failure for the read side."
    except Exception as exc:
        log.warning("[discovery] %s failed — skipped: %s", what, exc)
