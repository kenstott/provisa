# Copyright (c) 2026 Kenneth Stott
# Canary: b2c6e1f4-7d39-4a18-8e05-3f9a1c7d2e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A connector's lifecycle (REQ-2221): the setup values its @on-start function is given, and its
health, polled and kept for Sources to show.

A connector's code runs on a host its operator runs, and nothing gives that host its setup values
but Provisa: at join, and each time the host has started again and Provisa dials it. Each value the
code declares with @requires is read when it is given -- a secret through the audited read every
service read of a secret goes through, never kept -- and goes over the paired, pinned connection.

Each Provisa process polls the built sources whose code tags @on-start or @health, every
``codesource.health_poll_interval`` seconds, and keeps what it found for the status it serves, as
its own view of its own connections (:func:`provisa.codesource.remote_sources.status` is one per
process too). A poll that finds a host restarted is what runs @on-start again when no call has.

A status is healthy when it says ``ok`` or ``healthy``; any other text says what is wrong, and is
shown as it is. A function that raises, or a host that does not answer, is unhealthy by its reason.
"""

# Requirements: REQ-2221
from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import HEALTH, LIFECYCLE_TAGS, ON_START, Manifest
from provisa.codesource.worker import WorkerFailed

log = logging.getLogger(__name__)

HEALTHY, UNHEALTHY = "healthy", "unhealthy"
_SAYS_HEALTHY = frozenset({"ok", "healthy"})
#: How many sources one sweep polls at once.
_AT_ONCE = 8
SETTING = "codesource.health_poll_interval"

_kept: dict[tuple[str, str], dict[str, Any]] = {}  # runtime key, source
_lock = threading.Lock()


def verdict(answer: dict[str, Any]) -> dict[str, Any]:
    """What a host's answer to a health call says: its state and the words to show with it."""
    failed = answer.get("failed")
    if failed is not None:
        return {"state": UNHEALTHY, "detail": failed}
    status = answer.get("status")
    if status is None:  # no @health function: its host answers, and its on-start did not fail
        return {"state": HEALTHY, "detail": None}
    return {
        "state": HEALTHY if status.strip().lower() in _SAYS_HEALTHY else UNHEALTHY,
        "detail": status,
    }


def is_polled(manifest: Manifest) -> bool:
    """Whether a source's health is worth polling: its code has @on-start or @health."""
    return manifest.lifecycle(ON_START) is not None or manifest.lifecycle(HEALTH) is not None


def refuse_in_dev(source_id: str, manifest: Manifest) -> None:
    """A lifecycle function runs only on a host an operator runs (REQ-2221): on a source whose
    code runs on this server nothing would run it, so it is refused by name, not ignored."""
    tagged = [e for e in manifest.exports if e.kind in LIFECYCLE_TAGS]
    if tagged:
        named = ", ".join(f"{e.name} (@{e.kind})" for e in tagged)
        raise BundleRefused(
            "lifecycle_needs_host",
            f"{named}: a lifecycle function runs on a connector's host, and this source runs "
            "on this server",
            source=source_id,
        )


def setup_values(
    org_id: str, source_id: str, manifest: Manifest, mapping: dict, admin_db: Any
) -> Callable[[], dict[str, Any]] | None:
    """What gives a host its setup values, read each time it is called; None when the code has no
    @on-start to give them to and it is not a script connector, whose script reads them at any
    call (REQ-2225)."""
    if manifest.lifecycle(ON_START) is None and not mapping.get("script_hash"):
        return None
    from provisa.codesource import services

    grant = services.ServiceGrant(
        org_id=org_id,
        source_id=source_id,
        secrets=services.declared(mapping.get("secrets")),
        settings=services.declared_settings(mapping.get("settings")),
        admin_db=admin_db,
    )
    requires = manifest.requires

    def values() -> dict[str, Any]:
        out: dict[str, Any] = {}
        for requirement in requires:
            if requirement.secret:
                out[requirement.name] = services.read_secret(grant, requirement.name)
            elif requirement.name in grant.settings:
                out[requirement.name] = grant.settings[requirement.name]
            else:
                raise ValueError(
                    f"the source has no value for {requirement.name!r}, which its code requires"
                )
        return out

    return values


# -- kept -------------------------------------------------------------------------------------------


def health(runtime: str, source_id: str) -> dict[str, Any] | None:
    """What this process last found of the source, or None when it has not polled it yet."""
    with _lock:
        return _kept.get((runtime, source_id))


def forget(runtime: str, source_id: str) -> None:
    with _lock:
        _kept.pop((runtime, source_id), None)


def _keep(runtime: str, source_id: str, answer: dict[str, Any]) -> None:
    found = {**verdict(answer), "checked_at": datetime.now(UTC).isoformat()}
    with _lock:
        _kept[(runtime, source_id)] = found


def poll_source(runtime: str, source_id: str, prepared: Any) -> None:
    """Ask one source's host for its health and keep the verdict. A host that restarted is dialled
    here, which runs its @on-start again; a failure of that is the source's verdict."""
    try:
        worker = prepared.worker()
        worker.ready()
        answer = worker.health()
    except WorkerFailed as failed:
        answer = {"failed": str(failed)}
    _keep(runtime, source_id, answer)


# -- the sweep --------------------------------------------------------------------------------------


async def _sweep_runtime(state: Any, runtime: Any, slots: asyncio.Semaphore) -> None:
    from provisa.api.org_runtime import runtime_key, serving
    from provisa.codesource.remote_sources import BUILT, mode_of
    from provisa.codesource.source import CODE_SOURCE_TYPES, context_for, prepare
    from provisa.federation.registry_view import registered_sources

    key = runtime_key(runtime.org_id, runtime.env)
    with serving(runtime):
        sources = await registered_sources(state)
        built = [
            s for s in sources if s.type.value in CODE_SOURCE_TYPES and mode_of(s.mapping) == BUILT
        ]
        for source in built:
            prepared = await prepare(
                source.id, source.type.value, source.mapping, await context_for(state)
            )
            if is_polled(prepared.manifest):
                async with slots:
                    await asyncio.to_thread(poll_source, key, source.id, prepared)


async def sweep(state: Any) -> None:
    """Poll every built source this process serves that has a lifecycle to poll."""
    slots = asyncio.Semaphore(_AT_ONCE)

    async def one(runtime: Any) -> None:
        try:
            await _sweep_runtime(state, runtime, slots)
        except Exception:  # allow-ble: one org's failure must not stop the sweep of the others
            log.exception("code source health sweep failed for org %s", runtime.org_id)

    await asyncio.gather(*(one(r) for r in state.org_registry.runtimes()))


async def run(state: Any) -> None:
    """The process's poll, every ``codesource.health_poll_interval`` seconds (read each time)."""
    from provisa.core import settings_registry

    while True:
        await asyncio.sleep(int(settings_registry.value(SETTING)))
        await sweep(state)
