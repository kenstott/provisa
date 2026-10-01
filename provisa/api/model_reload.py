# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3a6d14-2f7b-4c95-b0e8-7d1c5a9f3e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a worker process reloads when a config stamp changes (REQ-1914).

A model or governance change made through one worker is written to the control plane, and the
control plane advances the ``model`` stamp of that org's plane in the same transaction
(``provisa/core/config_stamp.py``). Every other worker of every instance holds a compiled copy of
that model in an :class:`OrgRuntime`; its config watcher (``provisa/core/config_watch.py``) finds
the stored stamp ahead of the one the copy was built at and calls :func:`reload_model` — the same
``_rebuild_schemas`` the worker that made the change runs for itself.

The same watcher carries the operator settings (the platform plane's ``settings`` stamp) and each
org's own settings (its plane's ``settings`` stamp), each a separate stamp so a change of one kind
does not reload another.
"""

# Requirements: REQ-1914

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from provisa.core import config_stamp, deployment_settings, domain_policy
from provisa.core.config_watch import Target
from provisa.core.environments import PROD
from provisa.core.request_context import (
    reset_current_env,
    reset_current_org,
    set_current_env,
    set_current_org,
)

if TYPE_CHECKING:
    from provisa.api.org_runtime import OrgRuntime
    from provisa.core.models import ProvisaConfig

log = logging.getLogger(__name__)

# The columns of a ``sources`` row a worker's per-source state is built from.
_CONNECTION_COLUMNS = (
    "type",
    "host",
    "port",
    "database",
    "username",
    "password_ref",
    "path",
    "federation_hints",
    "bound",
)

# The per-source maps a worker holds, dropped for a source whose row is gone.
_SOURCE_MAPS = (
    "source_types",
    "source_dialects",
    "source_catalogs",
    "source_dsns",
    "source_cache",
    "source_allowed_domains",
    "source_federation_hints",
)


def _connection(row: dict) -> tuple:
    return tuple(json.dumps(row[c], sort_keys=True, default=str) for c in _CONNECTION_COLUMNS)


async def reconcile_sources(rows: dict[str, dict]) -> None:
    """Bring this worker's per-source state (direct pool, dialect, catalog name) in line with the
    ``sources`` rows just read by the schema build.

    The worker that registers, changes or deletes a source updates its own state in the mutation;
    every other worker reaches the same state here, when its model reload reads the changed rows.
    A source is built from the config's own entry where the config declares it (that is where its
    credentials are), else from its row.

    A failure here does not stop the schema build it runs in (REQ-1914): the build is what puts a
    hidden column, a row filter or a mask in force on this worker, and that must not wait on a
    source's credentials or reachability. The failure is logged with its traceback, the recorded
    rows are left as they were, and the next build tries the same sources again.
    """
    try:
        await _reconcile_sources(rows)
    except Exception:  # allow-ble: governance reload must not depend on source connection state
        log.exception("source state could not be brought in line with the registered sources")


async def _reconcile_sources(rows: dict[str, dict]) -> None:
    from provisa.api.app import state
    from provisa.api.app_loaders import _build_source_pools_and_enums
    from provisa.core.models import BUILT_IN_SOURCE_IDS
    from provisa.core.repositories.source import source_from_row

    rt = state._active_runtime()
    seen = {sid: _connection(row) for sid, row in rows.items() if sid not in BUILT_IN_SOURCE_IDS}
    known = rt.source_rows
    if known is None:
        # This runtime's first schema build: its per-source state was built by the boot or the
        # org build just before, from these rows. Recorded as the state to compare against.
        rt.source_rows = seen
        return
    changed = [sid for sid in seen if sid in known and known[sid] != seen[sid]]
    added = [sid for sid in seen if sid not in known]
    removed = [sid for sid in known if sid not in seen]
    for sid in (*changed, *removed):
        await state.source_pools.remove(sid)
    for sid in removed:
        for name in _SOURCE_MAPS:
            getattr(rt, name).pop(sid, None)
        state.graphql_remote_sources.pop(sid, None)
    if added or changed:
        declared = {s.id: s for s in state.config.sources} if state.config is not None else {}
        models = [declared[sid] if sid in declared else source_from_row(rows[sid]) for sid in seen]
        # Every registered source is passed, not only the new ones: the builder also derives the
        # process's enum-type registry from the sources it is given. Pools that exist are kept
        # (SourcePool.add returns for a source it already holds).
        shim = SimpleNamespace(
            sources=[],
            # A first-run install has no config file, so no domain declares a write target yet.
            domains=state.config.domains if state.config is not None else [],
        )
        await _build_source_pools_and_enums(cast("ProvisaConfig", shim), extra_sources=models)
        log.info("source state rebuilt: added=%s changed=%s", added, changed)
    if removed:
        log.info("source state dropped: %s", removed)
    rt.source_rows = seen


def _held(rt: "OrgRuntime") -> bool:
    """Whether ``rt`` is still the runtime its org and environment are served by."""
    from provisa.api.app import state
    from provisa.api.org_runtime import runtime_key

    return state.org_registry.get(runtime_key(rt.org_id, rt.env)) is rt


async def _bound(rt: "OrgRuntime", work: Any) -> None:
    """Run ``work()`` with ``rt``'s org and environment bound, as a request to it would be."""
    org_token = set_current_org(rt.org_id)
    env_token = set_current_env(None if rt.env == PROD else rt.env)
    try:
        await work()
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)


async def reload_model(rt: "OrgRuntime") -> None:
    """Rebuild ``rt``'s compiled model from the control plane. The build records the ``model``
    stamp it read before reading the model, and bumps the process's schema generation, which is
    what every kept plan, compiled query and routing decision is keyed on."""
    from provisa.api.app import _rebuild_schemas

    if not _held(rt):
        return  # dropped since the watcher listed it: there is no copy left to reload

    async def _rebuild() -> None:
        # The worker that made the change announced it (REQ-1072); a reload is not another change.
        await _rebuild_schemas(announce=False)

    await _bound(rt, _rebuild)


async def load_org_settings(rt: "OrgRuntime") -> None:
    """Read ``rt``'s org settings rows, recording the ``settings`` stamp they were read at."""
    from provisa.core.org_settings import read_org_overrides_stamped

    assert rt.tenant_db is not None, "an org's settings live in its tenant plane"
    rt.settings_stamp, rt.settings_overrides = await read_org_overrides_stamped(rt.tenant_db)


async def reload_org_settings(rt: "OrgRuntime") -> None:
    """Read ``rt``'s org settings again. A changed domain mode (REQ-1266) is applied to the org's
    policy scope and the schemas are rebuilt under it, as the worker that saved it did."""
    from provisa.api.admin._config_io import read_config
    from provisa.api.app import _rebuild_schemas

    if not _held(rt):
        return
    before = rt.settings_overrides.get("naming")
    await load_org_settings(rt)
    naming = rt.settings_overrides.get("naming")
    if naming == before:
        return

    async def _apply_domain_mode() -> None:
        # An org with no override inherits the deployment's naming block.
        deployment = read_config().get("naming", {}) or {}
        override = naming or {}
        use_domains = override.get("use_domains", deployment.get("use_domains"))
        default_domain = (
            override["default_domain"]
            if use_domains is False and "default_domain" in override
            else deployment.get("default_domain", "default")
        )
        domain_policy.configure(use_domains, default_domain)
        await _rebuild_schemas(announce=False)

    await _bound(rt, _apply_domain_mode)


def targets() -> list[Target]:
    """Every copy this process holds of something a control plane stores, for its watcher."""
    from provisa.api.app import state
    from provisa.core import settings_registry

    out: list[Target] = []
    if state.admin_db is not None:
        out.append(
            Target(
                name="operator settings",
                db=state.admin_db,
                kind=config_stamp.SETTINGS,
                loaded=deployment_settings.loaded_stamp,
                reload=settings_registry.reload_and_apply,
            )
        )
    for key in state.org_registry.all_org_ids():
        rt = state.org_registry.get(key)
        if rt is None or rt.tenant_db is None:
            continue  # dropped since listed, or registered and still being built
        out.append(
            Target(
                name=f"org {key}: model",
                db=rt.tenant_db,
                kind=config_stamp.MODEL,
                loaded=lambda rt=rt: rt.model_stamp,
                reload=lambda rt=rt: reload_model(rt),
            )
        )
        out.append(
            Target(
                name=f"org {key}: settings",
                db=rt.tenant_db,
                kind=config_stamp.SETTINGS,
                loaded=lambda rt=rt: rt.settings_stamp,
                reload=lambda rt=rt: reload_org_settings(rt),
            )
        )
    return out


async def health() -> dict[str, dict[str, int | None]]:
    """For the worker answering and the org bound to the request: the stamp each of its copies
    was loaded at, and the stamp the control plane stores now. They differ only while a change
    is on its way to this worker."""
    from provisa.api.app import state

    rt = state._active_runtime()
    out: dict[str, dict[str, int | None]] = {}
    if rt.tenant_db is not None:
        stored = await config_stamp.read(rt.tenant_db)
        out["model"] = {"loaded": rt.model_stamp, "stored": stored[config_stamp.MODEL]}
        out["org_settings"] = {
            "loaded": rt.settings_stamp,
            "stored": stored[config_stamp.SETTINGS],
        }
    if state.admin_db is not None:
        out["settings"] = {
            "loaded": deployment_settings.current_stamp(),
            "stored": (await config_stamp.read(state.admin_db))[config_stamp.SETTINGS],
        }
    return out
