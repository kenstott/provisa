# Copyright (c) 2026 Kenneth Stott
# Canary: 219dd772-5a21-4f45-ade3-aaa960aeb834
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Opening, keeping and reaping demo sandboxes (REQ-2006).

A demo sandbox is an org of its own (``sbx<hex>``), opened from the live demos page as a copy of
its demo's template org (demo_templates.py), on the sandbox tier (sandbox_tier.py):

- its model is the template's, cloned schema to schema (env_copy.clone_model), with every source's
  connection carried as it is, so it reads the demo's shared sources, which take no writes;
- its data is the template's replicas, copied from the template's DuckDB file into its own
  (materialize_broker.copy_schema) and recorded as built for its own store, so nothing of the demo
  is seeded at a visitor's request;
- its visitor is a user minted for it, holding the ``sandbox`` role (REQ-1597: org_admin's rights
  but environments and user management) and a personal access token (REQ-1263) the browser keeps
  (REQ-1035 rejected anonymous identity-provider sign-in: the visitor needs no account).

How many sandboxes may be open at once is ``sandbox.max_concurrent``; past it, a visitor is asked
to try again shortly. A sandbox no request has reached for ``sandbox.idle_ttl`` seconds is deleted
whole -- its org, its folder and its file, its engine, and its visitor's token. A visitor's
sandbox whose tab said it was closing, and that no request has reached since for
``CLOSE_GRACE_S``, is deleted the same way (REQ-2065).
"""

# Requirements: REQ-2006, REQ-1597, REQ-1263, REQ-1035, REQ-1600, REQ-2049, REQ-2065

from __future__ import annotations

import asyncio
import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, insert, select, update

from provisa.api.demo_templates import PublishedDemo, follow_latest, published_demo
from provisa.api.errors import ApiError
from provisa.api.sandbox_tier import new_sandbox_org_id
from provisa.core import model_change, settings_registry
from provisa.core.schema_admin import demo_sandboxes, orgs, user_profiles

log = logging.getLogger(__name__)

VISITOR = "visitor"
NAMED = "named"
#: The role a sandbox's person holds (provisa/core/db.py, REQ-1597).
SANDBOX_ROLE = "sandbox"
_TOKEN_NAME = "Demo sandbox"

# One process serves every sandbox (sandbox_tier.py), so this lock is the whole of the count's
# protection: two visitors arriving at once cannot both take the last place.
_opening = asyncio.Lock()


@dataclass(frozen=True)
class OpenedSandbox:
    org_id: str
    demo: str
    user_id: str
    #: The visitor's personal access token; None for a named sandbox, whose person signs in.
    token: str | None


def visitor_user_id(org_id: str) -> str:
    """The user minted for a visitor's sandbox: its own, and nobody else's."""
    return f"sandbox-visitor-{org_id}"


async def _template_ready(demo: PublishedDemo) -> None:
    """Refuse by name when the demo's template is not built, or its replicas are not all landed."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.api.app import ensure_org_runtime

    async with _admin_pool().acquire() as conn:
        row = (
            await conn.execute_core(
                select(orgs.c.provisioning_state).where(orgs.c.id == demo.template_org)
            )
        ).fetchone()
    ready = row is not None and row[0] == "ready"
    if ready:
        from provisa.api.demo_templates import _replica_progress

        rt = await ensure_org_runtime(demo.template_org)
        landed, failed, declared = await _replica_progress(rt)
        ready = landed >= declared and not failed
    if not ready:
        raise ApiError(
            503,
            "sandbox.demo_not_ready",
            f"{demo.title} is still being prepared; try again shortly",
            demo=demo.name,
        )


async def _open_count(conn: Any) -> int:
    return int(
        (await conn.execute_core(select(func.count()).select_from(demo_sandboxes))).scalar_one()
    )


async def named_sandbox_of(user_id: str, demo: str) -> str | None:
    """The named sandbox ``user_id`` already has open of ``demo``, if any."""
    from provisa.api.admin.orgs_router import _admin_pool

    async with _admin_pool().acquire() as conn:
        row = (
            await conn.execute_core(
                select(demo_sandboxes.c.org_id).where(
                    demo_sandboxes.c.user_id == user_id,
                    demo_sandboxes.c.demo == demo,
                    demo_sandboxes.c.kind == NAMED,
                )
            )
        ).fetchone()
    return row[0] if row is not None else None


async def open_sandbox(name: str, *, kind: str, user_id: str | None = None) -> OpenedSandbox:
    """Open a sandbox of the published demo ``name``: a visitor's (``user_id`` None, minted here)
    or a named one for the signed-in ``user_id``."""
    from provisa.api.admin.orgs_router import _admin_pool

    if kind not in (VISITOR, NAMED):
        raise ValueError(f"unknown sandbox kind {kind!r}")
    if (kind == NAMED) != (user_id is not None):
        raise ValueError("a named sandbox is opened for a signed-in user, a visitor's for none")
    # REQ-2049: a demo published since is served first, its template rebuilt from it.
    await follow_latest(name)
    demo = published_demo(name)
    await _template_ready(demo)
    admin = _admin_pool()
    org_id = new_sandbox_org_id()
    person = user_id if user_id is not None else visitor_user_id(org_id)
    async with _opening:
        async with admin.acquire() as conn:
            cap = int(settings_registry.value("sandbox.max_concurrent"))
            if await _open_count(conn) >= cap:
                raise ApiError(
                    503,
                    "sandbox.at_capacity",
                    "Every demo sandbox is in use; try again shortly",
                    limit=cap,
                )
            await conn.execute_core(
                orgs.insert().values(
                    id=org_id,
                    name=f"{demo.title} sandbox",
                    created_by=person,
                    provisioning_state="provisioning",
                    seeded_demo=False,
                )
            )
            await conn.execute_core(
                insert(demo_sandboxes).values(
                    org_id=org_id, demo=demo.name, kind=kind, user_id=person
                )
            )
    try:
        token = await _build(demo, org_id, person, kind)
    except BaseException:
        log.exception("demo sandbox %s of %s could not be opened; removing it", org_id, demo.name)
        await reap(org_id)
        raise
    log.info("demo sandbox %s of %s opened (%s)", org_id, demo.name, kind)
    await note_in_use(demo.name)
    return OpenedSandbox(org_id=org_id, demo=demo.name, user_id=person, token=token)


@model_change.commits_itself  # REQ-1524: its first commit is written below, as org creation's is
async def _build(demo: PublishedDemo, org_id: str, person: str, kind: str) -> str | None:
    from provisa.api.admin.orgs_router import _admin_pool, _pool, _refresh_root_registry_view
    from provisa.api.app import _stop_org_servers, build_org_runtime, state
    from provisa.auth.pat import PersonalAccessTokenStore
    from provisa.core import org_files
    from provisa.core.env_copy import clone_model
    from provisa.core.env_classes import COPIED
    from provisa.core.env_repo import ensure_repo, write_through
    from provisa.core.env_store import ensure_prod
    from provisa.core.environments import PROD, org_schema
    from provisa.core.org_membership import JOINED_VIA_ADMIN, grant_membership, grant_org_role
    from provisa.core.org_provisioning import provision_org
    from provisa.core.redis_location import redis_url
    from provisa.security.rights import DEPLOYMENT_GRANTER

    schema_sql = (Path(__file__).parent.parent / "core" / "schema.sql").read_text()
    await provision_org(
        _pool(),
        schema_sql,
        org_id=org_id,
        redis_url=redis_url(),
        redis_password=settings_registry.value("cache.redis_org_password"),
    )
    await ensure_prod(_admin_pool(), org_id, person)
    # The template's model, then its landed replicas -- both before the sandbox's runtime is built,
    # so its first build finds every replica already recorded as built for its own store and asks
    # for none.
    await clone_model(_pool(), org_schema(demo.template_org), org_schema(org_id), landing=COPIED)
    await org_files.prepare(org_id, state.admin_db, _stop_org_servers)
    # The demo version it is opened from: its examples and tour are that version's, whatever is
    # published after (demo_directory_of).
    from provisa.core.demo_bundles import SANDBOX_DEMO_DIRECTORY

    (org_files.folder(org_id) / SANDBOX_DEMO_DIRECTORY).write_text(str(demo.directory))
    from provisa.api.demo_replicas import copy_to_sandbox
    from provisa.api.demo_templates import replicated_tables, template_seed

    # Only what the demo serves from replicas is copied (REQ-2049): its own sources are read live.
    if replicated_tables(template_seed(demo.template_org)):
        await copy_to_sandbox(demo.template_org, org_id)

    rt = await state.org_registry.rebuild(org_id, lambda oid: build_org_runtime(oid))
    assert rt.model_db is not None
    if kind == VISITOR:
        async with _admin_pool().acquire() as conn:
            await conn.execute_core(
                insert(user_profiles).values(
                    user_id=person, display_name="Demo visitor", provider="sandbox"
                )
            )
    await grant_membership(_admin_pool(), person, org_id, joined_via=JOINED_VIA_ADMIN)
    await grant_org_role(rt.model_db, person, SANDBOX_ROLE, granter_capabilities=DEPLOYMENT_GRANTER)
    # And the demo's own roles (an analyst, say), so its tour can run as each of them; never a
    # built-in role, which would carry what the sandbox role withholds (REQ-1597).
    from provisa.api.demo_templates import demo_roles

    for role_id in demo_roles(demo.template_org):
        await grant_org_role(rt.model_db, person, role_id, granter_capabilities=DEPLOYMENT_GRANTER)
    ensure_repo(org_id)
    async with rt.model_db.acquire() as conn:
        await write_through(
            conn, _admin_pool(), org_id, PROD, org_schema(org_id), "opened from its demo", person
        )
    # REQ-2053: the template's environments (a test environment of fakes, say), each created from
    # the sandbox's prod after its person's roles are granted, so each carries them.
    from provisa.api.demo_templates import create_environments, template_environments

    await create_environments(org_id, await template_environments(demo.template_org), person)
    await _refresh_root_registry_view()
    async with _admin_pool().acquire() as conn:
        await conn.execute_core(
            update(orgs)
            .where(orgs.c.id == org_id)
            .values(provisioning_state="ready", provisioning_error=None)
        )
    if kind != VISITOR:
        return None
    secret, _row = await PersonalAccessTokenStore(_admin_pool()).issue(
        user_id=person,
        org_id=org_id,
        name=_TOKEN_NAME,
        role_id=SANDBOX_ROLE,
        env_name=PROD,  # REQ-1503: a sandbox is served from its one environment
    )
    return secret


async def demo_directory_of(org_id: str) -> Path | None:
    """The directory of the demo version a sandbox-tier org serves: a template's, the version its
    demo is served at; a sandbox's, the one it was opened from. None for an org that is neither
    (the demos' examples and tour are shown to nobody else), and for a sandbox no longer open."""
    from provisa.api.demo_templates import demo_of_template
    from provisa.core.demo_bundles import own_demo_directory
    from provisa.core.org_ids import is_template_org

    if is_template_org(org_id):
        return demo_of_template(org_id).directory
    return own_demo_directory(org_id)


async def note_in_use(name: str) -> None:
    """Tell the node which versions of bundled demo ``name`` are still read: the ready one (its
    template's) and each its open sandboxes were opened from (REQ-2049)."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.core.demo_bundles import (
        bundled_demos,
        own_demo_directory,
        ready_version,
        record_in_use,
    )

    if name not in bundled_demos():
        return
    async with _admin_pool().acquire() as conn:
        orgs_open = [
            row[0]
            for row in (
                await conn.execute_core(
                    select(demo_sandboxes.c.org_id).where(demo_sandboxes.c.demo == name)
                )
            ).fetchall()
        ]
    versions = {ready_version(name)} - {None}
    for org_id in orgs_open:
        directory = own_demo_directory(org_id)
        if directory is not None:
            versions.add(directory.name)
    await asyncio.to_thread(record_in_use, name, versions)  # type: ignore[arg-type]


_touched: dict[str, float] = {}
_TOUCH_EVERY_S = 60.0
#: REQ-2065: "a couple of minutes" -- how long a sandbox whose tab said it was closing waits for a
#: request (a reload, another tab of it) before it is deleted.
CLOSE_GRACE_S = 120


async def touch(org_id: str) -> None:
    """A request reached the sandbox ``org_id``: its idle expiry starts again (REQ-1600). Written
    at most once a minute per sandbox, so a busy sandbox does not write the control plane on every
    request; a minute is nothing against an expiry of ``sandbox.idle_ttl``."""
    import time

    from provisa.api.admin.orgs_router import _admin_pool

    clock = time.monotonic()
    if clock - _touched.get(org_id, float("-inf")) < _TOUCH_EVERY_S:
        return
    _touched[org_id] = clock
    async with _admin_pool().acquire() as conn:
        await conn.execute_core(
            update(demo_sandboxes)
            .where(demo_sandboxes.c.org_id == org_id)
            .values(last_active_at=datetime.now(UTC), closing_at=None)  # REQ-2065: still in use
        )


async def mark_closing(org_id: str) -> None:
    """The visitor's tab of sandbox ``org_id`` is closing (REQ-2065): it is deleted unless a
    request reaches it within ``CLOSE_GRACE_S``. That request's touch is written at once, not
    held back by the once-a-minute throttle, so it clears the mark."""
    from provisa.api.admin.orgs_router import _admin_pool

    _touched.pop(org_id, None)
    async with _admin_pool().acquire() as conn:
        await conn.execute_core(
            update(demo_sandboxes)
            .where(demo_sandboxes.c.org_id == org_id, demo_sandboxes.c.kind == VISITOR)
            .values(closing_at=datetime.now(UTC))
        )


async def expired(now: datetime | None = None) -> list[str]:
    """The sandboxes no request has reached for ``sandbox.idle_ttl`` seconds, and those whose tab
    said it was closing ``CLOSE_GRACE_S`` ago with no request since (REQ-2065), oldest first."""
    from provisa.api.admin.orgs_router import _admin_pool

    at = now if now is not None else datetime.now(UTC)
    idle = timedelta(seconds=int(settings_registry.value("sandbox.idle_ttl")))
    async with _admin_pool().acquire() as conn:
        rows = (
            await conn.execute_core(
                select(demo_sandboxes.c.org_id)
                .where(
                    (demo_sandboxes.c.last_active_at <= at - idle)
                    | (demo_sandboxes.c.closing_at <= at - timedelta(seconds=CLOSE_GRACE_S))
                )
                .order_by(demo_sandboxes.c.last_active_at)
            )
        ).fetchall()
    return [r[0] for r in rows]


async def last_active() -> datetime | None:
    """When a request last reached any open sandbox; None when none is open."""
    from provisa.api.admin.orgs_router import _admin_pool

    async with _admin_pool().acquire() as conn:
        return (
            await conn.execute_core(select(func.max(demo_sandboxes.c.last_active_at)))
        ).scalar_one()


async def reap(org_id: str) -> None:
    """Delete the sandbox ``org_id`` whole: its org (schemas, rows, its visitor's token), its
    engine, and its folder with its DuckDB file and uploads."""
    from provisa.api.admin.orgs_router import _admin_pool, destroy_org
    from provisa.core import org_files

    async with _admin_pool().acquire() as conn:
        row = (
            await conn.execute_core(
                select(
                    demo_sandboxes.c.kind, demo_sandboxes.c.user_id, demo_sandboxes.c.demo
                ).where(demo_sandboxes.c.org_id == org_id)
            )
        ).fetchone()
    # The deployment's registry view names every org, and dropping this one is a change to it,
    # committed as a deleted org's is (REQ-1524).
    async with model_change.scope(f"reap demo sandbox {org_id}"):
        await destroy_org(org_id)
    if row is not None and row[0] == VISITOR:
        async with _admin_pool().acquire() as conn:
            await conn.execute_core(delete(user_profiles).where(user_profiles.c.user_id == row[1]))
    _touched.pop(org_id, None)
    from provisa.security.sandbox_egress import forget

    forget(org_id)
    folder = org_files.folder(org_id)
    if folder.exists():
        await asyncio.to_thread(shutil.rmtree, folder)
    log.info("demo sandbox %s reaped", org_id)
    if row is not None:
        await note_in_use(row[2])


async def reap_expired() -> list[str]:
    """Reap every sandbox past its idle expiry; the ones reaped."""
    reaped = []
    for org_id in await expired():
        try:
            await reap(org_id)
        except Exception:  # allow-ble: one sandbox that cannot be removed must not keep the others; logged with its cause, and retried at the next pass
            log.exception("demo sandbox %s could not be reaped; retried at the next pass", org_id)
            continue
        reaped.append(org_id)
    return reaped
