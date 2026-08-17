# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1b6d24-98ac-4c0e-a5f7-1d0e93b45c72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bring-your-own materialization store: the org's bytes on the org's bill (REQ-1048).

An org that registers its own store owns the disk its materializations land on. That settles the
storage-cost question rather than rationing it: the platform stops being the party that pays for
whatever the org decides to materialize, so there is nothing to meter and no ceiling to enforce
(:mod:`provisa.storage.quota` returns early for these orgs). It is the intended arrangement for
isolated orgs, whose whole premise is dedicated infrastructure, and the exit named in the
quota-exceeded error for everyone else.

The DSN is stored encrypted at rest on ``orgs.storage_url_enc``, decrypted ONCE when the org's
runtime is built, and never returned by any read surface — the admin API reports only whether one
is set. This mirrors ``orgs.engine_url_enc`` exactly, and for the same reason: the value is a
credential to a system the platform does not own.

Resolution is a PRECEDENCE, not a fallback: BYO wins over the deployment's configured store, which
wins over the engine's default. An org with no BYO store set has not failed to resolve one — it is
on the platform store by design, and that is where the quota applies.
"""

# Requirements: REQ-1046, REQ-1048

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def org_store_dsn(org_id: str) -> str | None:
    """The org's own materialization-store DSN, or None when it uses the platform's.

    Read off the built ``OrgRuntime`` rather than the control-plane row: the DSN is decrypted once
    at runtime-build time, so a per-write lookup here would decrypt on every landing. An org with
    no runtime built yet returns None — not because the value is unknown, but because no query has
    run for that org, and the runtime is built before the first one does.
    """
    from provisa.api.app import state

    runtime = state.org_registry.get(org_id)
    if runtime is None:
        return None
    return runtime.storage_url


def org_has_byo_store(org_id: str) -> bool:
    """Whether ``org_id``'s materializations land outside the platform's store (REQ-1048)."""
    return org_store_dsn(org_id) is not None
