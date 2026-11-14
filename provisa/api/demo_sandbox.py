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
import concurrent.futures
import contextlib
import logging
from functools import partial
import threading
import time
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, insert, select, update

from provisa.api.demo_templates import PublishedDemo, follow_latest, published_demo
from provisa.api.errors import ApiError
from provisa.api.sandbox_tier import new_sandbox_org_id
from provisa.core import model_change, settings_registry, step_clock
from provisa.core.schema_admin import demo_sandboxes, orgs, user_profiles

log = logging.getLogger(__name__)

VISITOR = "visitor"
NAMED = "named"
#: The role a sandbox's person holds (provisa/core/db.py, REQ-1597).
SANDBOX_ROLE = "sandbox"
_TOKEN_NAME = "Demo sandbox"

# The count's protection: two visitors arriving at once cannot both take the last place. Every
# request runs on a thread and event loop of its own, so an asyncio lock (bound to one loop) cannot
# serialize them; the control plane's advisory lock does, across threads and processes alike.
_OPENING_LOCK_KEY = 7351
#: How long an open waits for its demo's stores and template, in place of refusing it (33 s for a
#: demo's stores, measured 2026-10-31); past it the visitor is told it is still being prepared, and
#: the page keeps waiting. Under the UI server's proxy timeout (ui.proxy_timeout, REQ-1448).
OPEN_WAIT_S = 120.0
_OPEN_POLL_S = 2.0

# What the tier's VM idle stop counts as use besides an open sandbox's last request (last_active):
# an open under way, and the last request to any sandbox, kept after the sandbox is reaped. On
# alpha.498 the VM stopped itself while an open waited for its demo's stores, the sandbox before it
# closed and reaped, its window counted from when the tier started. Requests run on threads of
# their own, so a thread lock guards both.
_use = threading.Lock()
_opening = 0
_last_used: datetime | None = None


def used() -> None:
    """A request reached a sandbox, or an open began or ended: the tier is in use now."""
    global _last_used
    with _use:
        _last_used = datetime.now(UTC)


@contextlib.contextmanager
def an_open():
    """An open under way: the tier counts as in use for as long as it lasts, however long its
    demo's stores and template take."""
    global _opening
    used()
    with _use:
        _opening += 1
    try:
        yield
    finally:
        with _use:
            _opening -= 1
        used()


@contextlib.asynccontextmanager
async def _stage(org_id: str, demo: str, name: str):
    """One stage of opening sandbox ``org_id`` of ``demo``, named as it starts and timed as it ends.
    At WARNING: the hosted demo tier logs at WARNING (as demo_warmup's "tour warmed" line). An open
    that stalls is named by its last stage started and not ended."""
    started = time.monotonic()
    log.warning("demo sandbox %s of %s: %s ...", org_id, demo, name)
    try:
        yield
    finally:
        log.warning(
            "demo sandbox %s of %s: %s took %.1fs", org_id, demo, name, time.monotonic() - started
        )


@dataclass(frozen=True)
class OpenedSandbox:
    org_id: str
    demo: str
    user_id: str
    #: The visitor's personal access token; None for a named sandbox, whose person signs in.
    token: str | None


async def _sandbox_runtime(org_id: str, *, cloned: bool = False) -> Any:
    """Build a sandbox's runtime: on the regions sandbox tier, in its region's store (REQ-2068),
    where its replicas and views were copied (demo_region_seed.copy_to_sandbox); elsewhere in the
    store of its own folder. ``cloned``: its first build, its schema provisioned and its model
    cloned from its template a moment ago (app.build_org_runtime's ``model_cloned``)."""
    from provisa.api.app import _region_lane_of, build_org_runtime
    from provisa.core.environments import PROD

    bound = await _region_lane_of(org_id, PROD)
    return await build_org_runtime(
        org_id, storage_url=None if bound is None else bound.materialize_url, model_cloned=cloned
    )


def visitor_user_id(org_id: str) -> str:
    """The user minted for a visitor's sandbox: its own, and nobody else's."""
    return f"sandbox-visitor-{org_id}"


async def _template_ready(demo: PublishedDemo) -> None:
    """Refuse by name when the demo's template is not built, or its replicas are not all landed."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.api.app import ensure_org_runtime
    from provisa.api.demo_templates import template_builds

    _refuse_failed_template(demo)
    building = template_builds.state_of(demo.name)
    if building == "ready":
        # Built and landed (or served as built at this start, demo_templates.warm_ready): its
        # runtime is built when the sandbox is copied from it, not to count its replicas here.
        return

    # Still being prepared here: its preparation (its profile runs among it, REQ-2052) is what a
    # sandbox is cloned from. A demo with no replica read as ready from its provisioning alone,
    # and its first sandbox was cloned before the template had profiled anything.
    ready = building not in ("queued", "building")
    async with _admin_pool().acquire() as conn:
        row = (
            await conn.execute_core(
                select(orgs.c.provisioning_state).where(orgs.c.id == demo.template_org)
            )
        ).fetchone()
    ready = ready and row is not None and row[0] == "ready"
    if ready:
        from provisa.api.demo_templates import _replica_progress

        rt = await ensure_org_runtime(demo.template_org)
        landed, failed, declared, _waiting = await _replica_progress(rt)
        ready = landed >= declared and not failed
    if not ready:
        from provisa.api.demo_templates import template_builds

        # The demo a visitor asks for is built next, when its template is still waiting.
        template_builds.prefer(demo.name)
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


def _refuse_failed_stores(demo: PublishedDemo) -> None:
    """A demo whose stores the node could not bring up since it was last asked for is refused,
    not left starting: the visitor waiting on it is told, and trying again (an open) asks the node
    again. Why is the operator's (logged, and in the node's own log): docker's own words name
    networks and containers no visitor needs."""
    if demo.stores_ready:  # up, or a demo served from its directory, which runs its own
        return
    from provisa.core.demo_bundles import stores_failed

    why = stores_failed(demo.name)
    if why is None:
        return
    logging.getLogger(__name__).error(
        "demo %s: its stores could not be started: %s", demo.name, why
    )
    raise ApiError(
        503,
        "sandbox.demo_stores_failed",
        f"{demo.title} could not be started; try again in a few minutes",
        demo=demo.name,
    )


def _refuse_failed_template(demo: PublishedDemo) -> None:
    """A demo whose template's preparation failed (TemplateBuilds.run) is refused by name: its Test
    (synthetic) environments or profile runs are not there to copy, opening it failed on the copy,
    and a visitor told it was being prepared would wait on nothing. Trying again, an open, builds
    it again (follow_latest)."""
    from provisa.api.demo_templates import template_builds

    if template_builds.state_of(demo.name) == "failed":
        raise _demo_failed(demo)


def _demo_failed(demo: PublishedDemo) -> ApiError:
    return ApiError(
        503,
        "sandbox.demo_failed",
        f"{demo.title} could not be prepared; try again in a few minutes",
        demo=demo.name,
    )


def _refuse_unhealthy_stores(demo: PublishedDemo) -> None:
    """A bundled demo whose store is down or red now is refused at its open: marked ready, it
    would fail partway through its tour (the reviews index red past Elasticsearch's disk
    watermark, 2026-10-31). What is wrong is the operator's, logged. A demo served from its
    directory runs its stores itself."""
    from provisa.core.demo_bundles import bundled_demos, unhealthy_stores

    if not bundled_demos():
        return
    wrong = unhealthy_stores(demo.directory)
    if wrong:
        log.error("demo %s: its stores are not healthy: %s", demo.name, "; ".join(wrong))
        raise _demo_failed(demo)


async def _ready_demo(name: str, org_id: str) -> PublishedDemo:
    """Published demo ``name``, its stores up and its template ready: an open waits for them
    (``OPEN_WAIT_S``) in place of being refused while the node brings them up. One that cannot be
    readied is refused by name at once. Each part of the wait is timed (``_stage``): the ask for
    a newer version, the wait for the demo's stores, the wait for its template."""
    from provisa.api.demo_templates import template_builds

    deadline = time.monotonic() + OPEN_WAIT_S
    while True:
        try:
            # REQ-2049: a demo published since is served first, its template rebuilt from it.
            async with _stage(org_id, name, "its newest version"):
                await follow_latest(name)
            demo = published_demo(name)
            async with _stage(org_id, name, "its template's readiness"):
                await _template_ready(demo)
            async with _stage(org_id, name, "its stores' health"):
                await asyncio.to_thread(_refuse_unhealthy_stores, demo)
            return demo
        except ApiError as refused:
            if refused.code != "sandbox.demo_not_ready":
                raise
            waited = {"its stores": 0.0, "its template": 0.0}
            try:
                while True:
                    demo = published_demo(name)
                    _refuse_failed_stores(demo)
                    _refuse_failed_template(demo)
                    building = template_builds.state_of(name) in ("queued", "building")
                    if demo.stores_ready and not building:
                        break
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(_OPEN_POLL_S)
                    waited["its template" if demo.stores_ready else "its stores"] += _OPEN_POLL_S
            finally:
                log.warning(
                    "demo sandbox %s of %s: waited %.0fs for its stores, %.0fs for its template",
                    org_id,
                    name,
                    waited["its stores"],
                    waited["its template"],
                )
            if time.monotonic() >= deadline:
                raise


async def demo_status(name: str) -> dict:
    """Whether published demo ``name`` can be opened now, and how many places are free (REQ-2006):
    its template's place in the tier's builds (demo_templates.TemplateBuilds.state_of) and the cap
    less the sandboxes open. Read only: it opens nothing, builds nothing, follows no newer version
    and marks no sandbox in use. An unknown or unpublished demo, and one that failed, is refused by
    name."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.api.demo_templates import template_builds

    # A visitor's page asks this while it waits for the demo: that wait is use of the tier.
    used()
    demo = published_demo(name)
    _refuse_failed_stores(demo)
    _refuse_failed_template(demo)
    state = template_builds.state_of(demo.name)
    cap = int(settings_registry.value("sandbox.max_concurrent"))
    async with _admin_pool().acquire() as conn:
        open_now = await _open_count(conn)
    return {
        "demo": demo.name,
        # An open of a built template waits for its stores, if they are not up (_ready_demo).
        "ready": state == "ready",
        "state": state,
        "places_free": max(cap - open_now, 0),
    }


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


async def record_visitor_attributes(conn: Any, person: str, directory: Path) -> dict:
    """Record the attributes demo ``directory`` declares for its visitor as the org's attributes for
    ``person`` (REQ-2151), as an administrator records any member's, and return the org's
    attributes by user -- what the sandbox's runtime resolves requests against. A derived role the
    tour acts as (REQ-2057) is then held from an attribute, as for any member."""
    from provisa.api.demo_templates import demo_visitor_attributes
    from provisa.core.repositories import user_attribute as attr_repo

    for attr_name, value in demo_visitor_attributes(directory).items():
        await attr_repo.upsert(conn, person, attr_name, value)
    return await attr_repo.org_by_user(conn)


async def open_sandbox(name: str, *, kind: str, user_id: str | None = None) -> OpenedSandbox:
    """Open a sandbox of the published demo ``name``: a visitor's (``user_id`` None, minted here)
    or a named one for the signed-in ``user_id``."""
    if kind not in (VISITOR, NAMED):
        raise ValueError(f"unknown sandbox kind {kind!r}")
    if (kind == NAMED) != (user_id is not None):
        raise ValueError("a named sandbox is opened for a signed-in user, a visitor's for none")
    with an_open():
        return await _open(name, kind=kind, user_id=user_id)


async def _open(name: str, *, kind: str, user_id: str | None) -> OpenedSandbox:
    from provisa.api.admin.orgs_router import _admin_pool

    org_id = new_sandbox_org_id()
    async with _stage(org_id, name, "its demo's readiness"):
        demo = await _ready_demo(name, org_id)
    admin = _admin_pool()
    person = user_id if user_id is not None else visitor_user_id(org_id)
    async with _stage(org_id, demo.name, "a place"), admin.acquire() as conn:
        async with conn.advisory_lock(_OPENING_LOCK_KEY):
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
        # Each step inside a stage (an environment's creation, a model's history, a runtime's
        # lookup) on the stages' own line, so a slow open names what inside a stage was slow.
        with step_clock.every_step_of(f"demo sandbox {org_id} of {demo.name}"):
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
    from provisa.api.app import _stop_org_servers, state
    from provisa.codesource import uploads
    from provisa.auth.pat import PersonalAccessTokenStore
    from provisa.core import org_files
    from provisa.core.env_copy import clone_model
    from provisa.core.env_classes import COPIED
    from provisa.core.env_store import ensure_prod
    from provisa.core.environments import PROD, org_schema
    from provisa.core.org_membership import JOINED_VIA_ADMIN, grant_org_roles, record_membership
    from provisa.core.org_provisioning import provision_org
    from provisa.core.redis_location import redis_url
    from provisa.security.rights import DEPLOYMENT_GRANTER

    schema_sql = (Path(__file__).parent.parent / "core" / "schema.sql").read_text()
    async with _stage(org_id, demo.name, "its schema"):
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
    async with _stage(org_id, demo.name, "the template's model"):
        await clone_model(
            _pool(), org_schema(demo.template_org), org_schema(org_id), landing=COPIED
        )
    async with _stage(org_id, demo.name, "its uploads, secrets and files"):
        # Uploads are the organisation's, outside its schema: its code sources name the template's.
        await uploads.copy(_admin_pool(), demo.template_org, org_id)
        # REQ-2095: the secrets its cloned model names, in its own vault before its runtime is built.
        from provisa.core.demo_secrets import seed as seed_secrets

        await seed_secrets(_admin_pool(), org_id, demo.directory)
        await org_files.prepare(org_id, state.admin_db, _stop_org_servers)
    # The demo version it is opened from: its examples and tour are that version's, whatever is
    # published after (demo_directory_of).
    from provisa.core.demo_bundles import SANDBOX_DEMO_DIRECTORY

    (org_files.folder(org_id) / SANDBOX_DEMO_DIRECTORY).write_text(str(demo.directory))
    from provisa.api.demo_replicas import bitemporal_view_tables, copy_views_to_sandbox
    from provisa.api.demo_templates import template_seed

    seed = template_seed(demo.template_org)
    await carry_landed_data(demo.template_org, org_id, seed, demo.name)
    # REQ-1162: and its bitemporal views' history, which no refresh of those sources gives back.
    if views := bitemporal_view_tables(seed):
        async with _stage(org_id, demo.name, "the template's views"):
            await copy_views_to_sandbox(demo.template_org, org_id, views, demo.directory)

    async with _stage(org_id, demo.name, "its runtime"):
        rt = await state.org_registry.rebuild(org_id, partial(_sandbox_runtime, cloned=True))
    assert rt.model_db is not None
    async with _stage(org_id, demo.name, "its person's membership and roles"):
        # Each plane written once, in one checkout and one transaction: every statement is a round
        # trip to a control plane on another machine.
        async with _admin_pool().acquire() as conn, conn.transaction():
            if kind == VISITOR:
                await conn.execute_core(
                    insert(user_profiles).values(
                        user_id=person, display_name="Demo visitor", provider="sandbox"
                    )
                )
            await record_membership(conn, person, org_id, joined_via=JOINED_VIA_ADMIN)
        # The sandbox role, and the demo's own roles (an analyst, say), so its tour can run as each
        # of them; never a built-in role, which would carry what the sandbox role withholds
        # (REQ-1597).
        from provisa.api.demo_templates import demo_roles

        granted = [SANDBOX_ROLE, *demo_roles(demo.template_org)]
        async with rt.model_db.acquire() as conn:
            await grant_org_roles(conn, person, granted, granter_capabilities=DEPLOYMENT_GRANTER)
            if kind == VISITOR:
                # REQ-2151: the attributes the demo's visitor carries, so a derived role its tour
                # acts as is held from an attribute; the runtime resolves requests against them at
                # once.
                async with conn.transaction():
                    rt.user_attributes = await record_visitor_attributes(
                        conn, person, demo.directory
                    )
    async with _stage(org_id, demo.name, "its record, environments and ready mark"):
        async with _stage(org_id, demo.name, "its record"):
            await _record(rt.model_db, demo, org_id, person)
        await _environments(demo, org_id, person)
        async with _stage(org_id, demo.name, "the registry view"):
            await _refresh_root_registry_view()
        async with _stage(org_id, demo.name, "its ready mark"), _admin_pool().acquire() as conn:
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


async def _record(model_db: Any, demo: PublishedDemo, org_id: str, person: str) -> None:
    """Sandbox ``org_id``'s first commit of its prod (REQ-1524). Its model was cloned from its
    template's prod a moment ago, so its rendering starts from the template's (REQ-2006): only an
    entity that differs is rendered again."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.core.env_repo import ensure_repo, write_through
    from provisa.core.environments import PROD, org_schema

    ensure_repo(org_id)
    async with model_db.acquire() as conn:
        await write_through(
            conn,
            _admin_pool(),
            org_id,
            PROD,
            org_schema(org_id),
            "opened from its demo",
            person,
            like=(demo.template_org, PROD),
        )


async def _environments(demo: PublishedDemo, org_id: str, person: str) -> None:
    """Sandbox ``org_id``'s environments: its template's, each created from its prod, given its
    rows, and its runtime built."""
    from provisa.api import demo_synthetic
    from provisa.api.app import ensure_org_runtime
    from provisa.api.demo_templates import create_environments, template_environments

    # REQ-2053: the template's environments (a test environment of fakes, say), each created from
    # the sandbox's prod after its person's roles are granted, so each carries them.
    async with _stage(org_id, demo.name, "its environments' list"):
        wanted = await template_environments(demo.template_org)
    # The org's redaction rules and secrets, loaded once (redaction_load.ensure_loaded) by the
    # lookup of its prod runtime: before any environment is created, as the first creation's
    # lookup loaded them when the environments were created one after another. Loaded inside the
    # fan-out below, the second creation's lookup found the load under way and went on without it.
    async with _stage(org_id, demo.name, "its redaction rules and secrets"):
        await ensure_org_runtime(org_id)
    synthetic = set(demo_synthetic.synthetic_envs(wanted))
    # The environments are made side by side, each its own line: created from prod, given its
    # rows, its runtime built. What two lines share is the org's, and each piece of it is either
    # keyed by the environment (its registry row, schema, branch, rows and runtime) or written by
    # a runtime build alone: the domain policy (domain_policy, keyed by the org), which a build
    # sets to the deployment's naming and then to the org's own, reading it between
    # (app._build_org_runtime). So one runtime is built at a time: the builds' turn below.
    # Each line runs on a thread and loop of its own (run_on_own_thread): the control plane blocks
    # the thread that calls it (core.database), so two lines on one loop would run one after
    # another. The builds' turn is taken across those threads.
    from provisa.core.connection_loop import CrossLoopLock, run_on_own_thread

    building = CrossLoopLock()
    # The phase each line failed in (its creation, its rows, its runtime), so the failure raised is
    # the one an open making them one after another would have met first.
    failed_in: dict[str, int] = {}

    async def _line(env: dict[str, str]) -> None:
        name = env["name"]
        failed_in[name] = 0
        async with _stage(org_id, demo.name, f"its environment {name}"):
            await create_environments(org_id, [env], person)
        # REQ-1942: a Test (synthetic) one is given the template's generated rows; nothing is
        # generated at a visitor's request. REQ-2006: its rows, records and bindings are copied
        # from the template's prepared environment, through its schema.
        failed_in[name] = 1
        if name in synthetic:
            async with _stage(org_id, demo.name, f"its environment {name}'s rows"):
                await demo_synthetic.copy_to_sandbox(demo.template_org, org_id, [name])
        # REQ-2006 (amended 2026-11-04): then its runtime, built once and cloned, as its prod's
        # is: its model was copied from prod a moment ago. Left to the first request, a tour step
        # waited 15-20 s on an ordinary build that seeded the whole model again.
        failed_in[name] = 2
        waiting = time.monotonic()
        async with building:
            # The other line's build ahead of this one's: the time a line waits for its turn.
            step_clock.took(f"environment {name}: its turn to build", time.monotonic() - waiting)
            async with _stage(org_id, demo.name, f"its environment {name}'s runtime"):
                await ensure_org_runtime(org_id, name, model_cloned=True)

    async with _stage(org_id, demo.name, "its environments"):
        lines = [
            run_on_own_thread(partial(_line, env), name=f"sandbox-environment-{env['name']}")
            for env in wanted
        ]
        # Every line is waited for, failed or not, and however this wait ends: the open removes a
        # sandbox that failed (reap), and a line still writing would write into what is being
        # removed.
        try:
            await asyncio.gather(
                *(asyncio.wrap_future(line) for line in lines), return_exceptions=True
            )
        finally:
            concurrent.futures.wait(lines)
    failures = [
        (failed_in[env["name"]], index, failure)
        for index, (env, line) in enumerate(zip(wanted, lines, strict=True))
        if not line.cancelled() and (failure := line.exception()) is not None
    ]
    if failures:
        raise min(failures, key=lambda failure: failure[:2])[2]


async def carry_landed_data(template_org: str, org_id: str, seed: Any, demo: str) -> None:
    """The template's landed data, into sandbox ``org_id``'s own store, before its runtime is
    built. Only what the demo serves from replicas is copied (REQ-2049): its own sources are read
    live. A regions demo's other regions' views are copied with them (REQ-2068). And what its
    listeners land from Kafka topics (REQ-2049, amended 2026-11-03): a sandbox landing those itself
    started from the topic's first message, so its first read, and its views' first refresh, found
    them empty. Where the template's listeners stopped is recorded as where the sandbox's landing
    stopped first, then the rows are copied: its listener continues from there."""
    from provisa.api import demo_replicas
    from provisa.api.demo_region_seed import regional
    from provisa.api.demo_templates import replicated_tables

    # A regions tier copies each region's data through its seeds (demo_region_seed).
    live = [] if regional() else demo_replicas.live_landed_tables(seed)
    if live:
        async with _stage(org_id, demo, "where its template's landing stopped"):
            await demo_replicas.continue_live_landing(template_org, org_id, seed)
    if live or regional() or replicated_tables(seed):
        async with _stage(org_id, demo, "the template's store"):
            await demo_replicas.copy_to_sandbox(template_org, org_id)


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
    template's) and each its open sandboxes were opened from (REQ-2049). With none open, its stores
    linger ``sandbox.demo_stores_linger`` seconds before the node takes them down; with one open,
    they stay up."""
    import time

    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.core.demo_bundles import (
        bundled_demos,
        hold,
        linger,
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
    if orgs_open:
        await asyncio.to_thread(hold, name)
    else:
        until = time.time() + int(settings_registry.value("sandbox.demo_stores_linger"))
        await asyncio.to_thread(linger, name, until)


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

    used()
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
    said it was closing ``CLOSE_GRACE_S`` ago with no request since (REQ-2065), oldest first --
    this tier's: the regions sandbox tier reaps its own (REQ-2068), whose stores the other cannot
    reach, and the other tier the rest."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.api.sandbox_tier import is_this_nodes_sandbox

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
    return [r[0] for r in rows if is_this_nodes_sandbox(r[0])]


async def last_active() -> datetime | None:
    """When the tier was last in use, which is what its VM's idle stop counts from: when a request
    last reached any open sandbox, None when none is open; now while it is building templates ahead
    (REQ-2006), which it does only while it is up: stopped at the end of its idle window, a pass over
    the demos never reached the end of them; now while a sandbox is being opened; and never before
    the last request this process saw to a sandbox, reaped since or not."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.api import demo_templates

    if demo_templates.building_ahead():
        return datetime.now(UTC)
    with _use:
        if _opening:
            return datetime.now(UTC)
        last_used = _last_used
    async with _admin_pool().acquire() as conn:
        latest = (
            await conn.execute_core(select(func.max(demo_sandboxes.c.last_active_at)))
        ).scalar_one()
    return max((t for t in (latest, last_used) if t is not None), default=None)


async def any_open() -> bool:
    """Whether any demo sandbox is open, on whichever tier: every tier runs on the one sandbox VM,
    and a sandbox is open until it is reaped (``sandbox.idle_ttl`` with no request, or its tab
    closed), however long since its visitor's last request."""
    from provisa.api.admin.orgs_router import _admin_pool

    async with _admin_pool().acquire() as conn:
        latest = (
            await conn.execute_core(select(func.max(demo_sandboxes.c.last_active_at)))
        ).scalar_one()
    return latest is not None


async def vm_last_active(since: datetime) -> datetime:
    """When any tier on the sandbox VM was last in use: this tier's own last use (``last_active``,
    or ``since``, when this process started) is recorded for the others, and the latest every
    tier has recorded returned. Each tier's use is in its own process; one that counted only its
    own stopped the VM under a visitor of another (2026-11-03 20:40, 2026-11-04 02:27)."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.api.demo_region_seed import here
    from provisa.core.schema_admin import sandbox_tier_use

    mine = max(t for t in (await last_active(), since) if t is not None)
    async with _admin_pool().acquire() as conn:
        await conn.upsert(
            sandbox_tier_use, {"tier": here(), "last_used_at": mine}, index_elements=["tier"]
        )
        latest = (
            await conn.execute_core(select(func.max(sandbox_tier_use.c.last_used_at)))
        ).scalar_one()
    return max(t for t in (mine, latest) if t is not None)


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


def _release_mark() -> Path:
    """Where this tier records the Provisa release its open sandboxes were provisioned by: one
    mark for the sandbox tier, one for each node of the regions tier, which reaps its own (REQ-2068;
    each node has a data directory of its own)."""
    from provisa.api.demo_region_seed import regional
    from provisa.core.org_files import data_dir

    return data_dir() / ("sandbox-release-regions" if regional() else "sandbox-release")


async def reap_visitors_of_another_release() -> list[str]:
    """At the tier's start: once the release has changed since its last start, every open
    visitor's sandbox has the old release's schema (no migrations), and a column added since fails
    its reads. Each is reaped; its tab opens a fresh one from the rebuilt template. A named
    sandbox, a signed-in person's, is kept. This release is then recorded. The sandboxes reaped."""
    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.api.sandbox_tier import is_this_nodes_sandbox
    from provisa.core.demo_bundles import running_provisa

    mark = _release_mark()
    running = running_provisa()
    if mark.is_file() and mark.read_text().strip() == running:
        return []
    async with _admin_pool().acquire() as conn:
        rows = (
            await conn.execute_core(
                select(demo_sandboxes.c.org_id).where(demo_sandboxes.c.kind == VISITOR)
            )
        ).fetchall()
    reaped = []
    for (org_id,) in rows:
        if not is_this_nodes_sandbox(org_id):
            continue  # another node's
        try:
            await reap(org_id)
        except Exception:  # allow-ble: one sandbox that cannot be removed must not keep the others; logged with its cause, and its idle expiry reaps it
            log.exception("demo sandbox %s of an earlier release could not be reaped", org_id)
            continue
        reaped.append(org_id)
    mark.parent.mkdir(parents=True, exist_ok=True)
    mark.write_text(running + "\n")
    return reaped


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
