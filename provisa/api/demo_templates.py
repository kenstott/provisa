# Copyright (c) 2026 Kenneth Stott
# Canary: dddd1887-185b-47b0-b4bd-a80f7ddf6894
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The published named demos, and the template org each one's sandboxes are cloned from (REQ-2006).

A named demo is ``demo/named/<name>/``: its ``config.yaml`` (the whole config, REQ-1858) and its
``demo.yaml`` (title, description, screenshot, ``published``). Only a published demo is offered as a
sandbox; an unknown or unpublished name is refused by name.

Each published demo has a template org, ``sbxtpl<name>`` (sandbox_tier.template_org_id), on the
sandbox tier. It is a demo organisation (REQ-1919): every build of its runtime rebuilds its model
from the demo's configuration, and its replicas are landed into its own DuckDB file when the tier
starts -- once, at deploy time. A visitor's sandbox is a copy of it (demo_sandbox.py).

The configuration names its own directory as ``${env:PROVISA_NAMED_DEMO_DIR}`` (the weblogs demo's
log globs). One process serves every published demo, so that reference is replaced here by the
demo's directory instead of read from one process-wide variable.

Where the demos come from (REQ-2049). A deployment names the demos it offers in
``PROVISA_DEMO_BUNDLES``; the node pulls each one's published bundle and runs the bundle's own
stores (provisa/core/demo_bundles.py), and the tier serves the version the node marks ready. When a
sandbox is opened the tier looks for a newer published version and asks the node for it; the
visitor is served the ready one meanwhile, and once a newer one is ready the demo's template is
replaced from it before the next sandbox is cloned. A sandbox already open keeps what it was cloned
from. A demo's own sources are read live, as its configuration says, at the addresses the node
gives (the bundle's stores.env). Without ``PROVISA_DEMO_BUNDLES`` the demos are the directories
under ``named_demos_dir()`` -- a source checkout's, for development, and the bundle build's own.
"""

# Requirements: REQ-2006, REQ-1858, REQ-1919, REQ-2049

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Awaitable, Callable
from typing import Any

import yaml

from provisa.api.errors import ApiError
from provisa.api.sandbox_tier import TEMPLATE_PREFIX, is_template_org, template_org_id
from provisa.core.replicate import ALWAYS
from provisa.core.connection_loop import CrossLoopLock

_DEMO_DIR_REF = "${env:PROVISA_NAMED_DEMO_DIR}"


def named_demos_dir() -> Path:
    from provisa.core.demo_bundles import named_demos_dir as directory

    return directory()


def bundled_demos() -> tuple[str, ...]:
    from provisa.core.demo_bundles import bundled_demos as names

    return names()


def bundles_root() -> Path:
    from provisa.core.demo_bundles import bundles_root as root

    return root()


def _registry() -> Any:
    from provisa.core.demo_bundles import Registry

    return Registry(os.environ["PROVISA_DEMO_REGISTRY"])


def latest_published(name: str) -> str:
    """The version of demo ``name`` published as its latest. Blocking: run in a thread."""
    from provisa.core.demo_bundles import LATEST

    registry = _registry()
    _digest, oci_manifest = registry.resolve(name, LATEST)
    return registry.manifest_of(name, oci_manifest).version


def _demo_directories() -> list[Path]:
    """Each named demo's directory: the ready version of each bundled demo the node has one of,
    else every one under ``named_demos_dir()``."""
    from provisa.core.demo_bundles import ready_version

    names = bundled_demos()
    if not names:
        return [spec.parent for spec in sorted(named_demos_dir().glob("*/demo.yaml"))]
    from provisa.core.demo_bundles import check_directory

    found = []
    for name in names:
        version = ready_version(name)
        if version is not None:
            check_directory(bundles_root() / name / version)
            found.append(bundles_root() / name / version)
    return found


@dataclass(frozen=True)
class PublishedDemo:
    name: str
    title: str
    directory: Path

    @property
    def template_org(self) -> str:
        return template_org_id(self.name)


def published_demos() -> dict[str, PublishedDemo]:
    """Every named demo whose demo.yaml says ``published: true``, by name."""
    found: dict[str, PublishedDemo] = {}
    bundled = bool(bundled_demos())
    for directory in _demo_directories():
        spec_file = directory / "demo.yaml"
        spec = yaml.safe_load(spec_file.read_text())
        published = spec.get("published")
        if not isinstance(published, bool):
            raise ValueError(f"{spec_file}: published must be true or false")
        if published:
            # A bundle's directory is its version; the demo is the directory above it.
            name = directory.parent.name if bundled else directory.name
            found[name] = PublishedDemo(name=name, title=spec["title"], directory=directory)
    return found


def published_demo(name: str) -> PublishedDemo:
    """The published demo ``name``; refused by name when there is none."""
    demo = published_demos().get(name)
    if demo is None:
        raise ApiError(
            404,
            "sandbox.unknown_demo",
            f"No published demo named {name!r}",
            demo=name,
        )
    return demo


def demo_environments(directory: Path) -> list[dict[str, str]]:
    """The environments demo ``directory``'s setup.yaml creates (REQ-2053): each ``environment``
    step's ``{name, data_mode}``, in order; none when it has no setup.yaml."""
    path = directory / "setup.yaml"
    if not path.is_file():
        return []
    steps = (yaml.safe_load(path.read_text()) or {}).get("steps") or []
    return [step["environment"] for step in steps if "environment" in step]


async def create_environments(org_id: str, wanted: list[dict[str, str]], created_by: str | None):
    """Create in ``org_id`` each of ``wanted``'s environments it does not have, from its prod:
    a demo's template its demo's, a sandbox its template's (REQ-2053)."""
    from sqlalchemy import select

    from provisa.api.admin.orgs_router import _admin_pool, _org_model_db, _pool
    from provisa.api.app import state
    from provisa.core.env_classes import REFUSED
    from provisa.core.env_create import create_environment
    from provisa.core.environments import PROD
    from provisa.core.schema_admin import environments

    async with _admin_pool().acquire() as conn:
        held = {
            row[0]
            for row in (
                await conn.execute_core(
                    select(environments.c.name).where(environments.c.org_id == org_id)
                )
            ).fetchall()
        }
    for env in wanted:
        if env["name"] in held:
            continue
        await create_environment(
            state,
            _admin_pool(),
            _pool(),
            await _org_model_db(org_id),
            org_id,
            env["name"],
            from_env=PROD,
            created_by=created_by,
            expires_at=None,
            data_mode=env["data_mode"],
            mutation_handling=env.get("mutation_handling", REFUSED),
            note="created from prod, as its demo declares",
        )


async def template_environments(template_org: str) -> list[dict[str, str]]:
    """The template org's environments other than prod, each ``{name, data_mode,
    mutation_handling}``: what a sandbox cloned from it carries (REQ-2053)."""
    from sqlalchemy import select

    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.core.environments import PROD
    from provisa.core.schema_admin import environments

    async with _admin_pool().acquire() as conn:
        rows = (
            await conn.execute_core(
                select(
                    environments.c.name,
                    environments.c.data_mode,
                    environments.c.mutation_handling,
                )
                .where(environments.c.org_id == template_org, environments.c.name != PROD)
                .order_by(environments.c.created_at)
            )
        ).fetchall()
    return [{"name": r[0], "data_mode": r[1], "mutation_handling": r[2]} for r in rows]


def demo_of_template(org_id: str) -> PublishedDemo:
    """The published demo whose template org ``org_id`` is."""
    assert is_template_org(org_id), org_id
    return published_demo(org_id[len(TEMPLATE_PREFIX) :])


def _with_directory(node: Any, directory: str) -> Any:
    if isinstance(node, str):
        return node.replace(_DEMO_DIR_REF, directory)
    if isinstance(node, list):
        return [_with_directory(item, directory) for item in node]
    if isinstance(node, dict):
        return {key: _with_directory(value, directory) for key, value in node.items()}
    return node


def template_seed(org_id: str) -> Any:  # a ProvisaConfig
    """The configuration template org ``org_id`` is rebuilt from: its demo's config.yaml, with the
    demo's directory written in."""
    from provisa.core.config_loader import parse_config_dict, read_config_with_includes

    demo = demo_of_template(org_id)
    raw = _with_directory(
        read_config_with_includes(demo.directory / "config.yaml"), str(demo.directory)
    )
    if bundled_demos():
        # The demo's own stores, where the node runs them (its sources are read live there).
        from provisa.core.demo_bundles import stores_env

        raw = _with_stores(raw, stores_env(demo.directory))
    _sandbox_sees_what_its_rights_cover(raw, _role_rights(raw))
    return parse_config_dict(raw)


async def write_declared_vector_models(model_db: Any, seed: Any) -> None:  # REQ-2006
    """The embedding models a demo's configuration declares (``vector_models``) are its template
    org's own settings, and so, copied with the template's settings (``clone_model``), each of its
    sandboxes': a demo whose queries embed text (the sonnets demo's ``lines_like``) is refused by
    the model allowlist without them. Written at every build of the template, as the rest of its
    model is rebuilt from its configuration (REQ-1919); a configuration that declares none clears
    the setting."""
    from provisa.core.org_settings import write_org_overrides

    models = [m.model_dump() for m in seed.vector_models]
    await write_org_overrides(model_db, {"vector_models": models or None}, updated_by="platform")


def connector_bundles() -> list[Any]:  # list[BundleSpec]
    """The connector bundle of every source the published demos read through one (the files
    demo's file connector), once each."""
    from provisa.federation.pgwire_replica import _source_type
    from provisa.runtime_deps.pgwire_bundles import BUNDLE_CONNECTOR, bundle_spec_for

    types = {
        _source_type(source)
        for demo in published_demos().values()
        for source in template_seed(demo.template_org).sources
    }
    return [bundle_spec_for(t) for t in sorted(types) if t in BUNDLE_CONNECTOR]


def prefetch_connector_bundles() -> None:
    """Download the demos' connector bundles into the runtime-deps cache now, outside any request,
    so a visitor's first query finds them. A bundle is hundreds of megabytes: fetched inside a
    request it was cut off by the request's deadline every time. A bundle that cannot be fetched
    is logged by name; the request that needs it then starts its download again
    (``BundleResolver.resolve``)."""
    import logging

    from provisa.runtime_deps.pgwire_bundles import BundleResolver, BundleUnavailable

    log = logging.getLogger(__name__)
    resolver = BundleResolver()
    for spec in connector_bundles():
        try:
            log.info(
                "connector bundle %s %s: %s",
                spec.artifact_name,
                spec.version,
                resolver.resolve(spec),
            )
        except BundleUnavailable:
            log.exception(
                "connector bundle %s %s was not fetched", spec.artifact_name, spec.version
            )


def embedding_models() -> list[Any]:  # list[VectorModelConfig]
    """The local (huggingface) embedding models the published demos declare and enable, once
    each: the sonnets demo's, which its search tables embed a phrase with."""
    seen: dict[str, Any] = {}
    for demo in published_demos().values():
        for model in template_seed(demo.template_org).vector_models:
            if model.provider == "huggingface" and model.enabled:
                seen.setdefault(model.id, model)
    return list(seen.values())


def load_embedding_models() -> None:
    """Load the demos' local embedding models into this process now, outside any request, so a
    visitor's first search finds them loaded. Importing sentence-transformers and loading a model
    took a hosted tier's first search past its 60s request timeout, every time the node started.
    A model that cannot be loaded is logged by name; the request that needs it then loads it
    (``HuggingFaceLocalProvider.embed``)."""
    import logging

    from provisa.vector.providers import HuggingFaceLocalProvider

    log = logging.getLogger(__name__)
    provider = HuggingFaceLocalProvider()
    for model in embedding_models():
        try:
            provider.load(model)
            log.info("embedding model %s: loaded", model.id)
        except Exception:  # allow-ble: a model the deploy cannot load leaves the tier serving; the request that needs it loads it, and fails, by name
            log.exception("embedding model %s was not loaded", model.id)


_ENV_REF = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)(?::-[^}]*)?\}")


def _with_stores(node: Any, values: dict[str, str]) -> Any:
    """``node`` with each ``${env:NAME}`` / ``${env:NAME:-default}`` whose NAME the bundle's
    stores.env sets replaced by its value there; any other reference is left to the loader."""
    if isinstance(node, str):
        return _ENV_REF.sub(lambda m: values.get(m.group(1), m.group(0)), node)
    if isinstance(node, list):
        return [_with_stores(item, values) for item in node]
    if isinstance(node, dict):
        return {key: _with_stores(value, values) for key, value in node.items()}
    return node


def _role_rights(raw: dict) -> dict[str, frozenset[str]]:
    """Each role a demo's configuration can name, with its rights: the seeded roles, and the
    demo's own (flattened, inheritance included), which stand in for a seeded one of the same id."""
    from provisa.core.db import SEEDED_ROLE_RIGHTS
    from provisa.core.models import Role, flatten_roles

    rights = dict(SEEDED_ROLE_RIGHTS)
    own = flatten_roles([Role(**role) for role in raw.get("roles", [])])
    rights.update({role.id: frozenset(role.capabilities) for role in own})
    return rights


def _sandbox_sees_what_its_rights_cover(node: Any, rights: dict[str, frozenset[str]]) -> None:
    """A demo names who sees each column and table as it would anywhere (its admin, its own
    roles). Its sandbox's person holds the sandbox role (REQ-1597): wherever a role holding every
    right the sandbox role holds is named, the sandbox role is named with it."""
    from provisa.api.demo_sandbox import SANDBOX_ROLE

    covered = rights[SANDBOX_ROLE]
    if isinstance(node, list):
        for item in node:
            _sandbox_sees_what_its_rights_cover(item, rights)
    elif isinstance(node, dict):
        visible = node.get("visible_to")
        if (
            isinstance(visible, list)
            and SANDBOX_ROLE not in visible
            and any(covered <= rights.get(role_id, frozenset()) for role_id in visible)
        ):
            visible.append(SANDBOX_ROLE)
        for value in node.values():
            _sandbox_sees_what_its_rights_cover(value, rights)


def demo_roles(template_org: str) -> list[str]:
    """The roles a demo's configuration defines for itself (its analyst, say), which a sandbox's
    person also holds so the demo's tour can run as each. One that would carry a right the sandbox
    role withholds (REQ-1597), on its own or by inheritance, is refused by name rather than
    granted."""
    from provisa.core.db import SANDBOX_DENIED
    from provisa.core.models import flatten_roles

    out = []
    for role in flatten_roles(list(template_seed(template_org).roles)):
        withheld = sorted(set(role.capabilities) & SANDBOX_DENIED)
        if withheld:
            raise ValueError(
                f"demo role {role.id!r} of {template_org} holds {', '.join(withheld)}, which a "
                "sandbox's person may not"
            )  # REQ-1597
        out.append(role.id)
    return out


def replicated_tables(config: Any) -> list[Any]:
    """The tables of a demo's configuration that are always read from a replica -- replicated
    always (``replicate: 0``) or load-protected, on the table or its source: a template is landed
    when each of these is. A demo's own sources are read live and land nothing; its views have
    no replica either."""
    from provisa.core.models import DERIVED_SOURCE_ID
    from provisa.core.replicate import resolved_load_protected, resolved_replicate

    sources = {source.id: source for source in config.sources}
    return [
        table
        for table in config.tables
        if table.source_id != DERIVED_SOURCE_ID
        and (
            resolved_replicate(sources[table.source_id], table) == ALWAYS
            or resolved_load_protected(sources[table.source_id], table)
        )
    ]


#: What runs once a demo's template is built and every one of its replicas is landed, at each
#: start of the sandbox tier: ``hook(demo, template_org)``, with the template's runtime built and the
#: template org bound. Registered by what derives something of its own from a template at deploy
#: time (the tour answers). A hook that raises is logged and the next one still runs; the template
#: stays usable.
TemplateHook = Callable[[PublishedDemo, str], Awaitable[None]]
_ready_hooks: list[TemplateHook] = []

#: How often a template being prepared is asked whether its replicas have landed.
_LANDED_POLL_S = 2.0


def on_template_ready(hook: TemplateHook) -> TemplateHook:
    """Register ``hook`` to run when a template is ready (see ``_ready_hooks``)."""
    _ready_hooks.append(hook)
    return hook


def seeding_release() -> bool:
    """Whether this process is a bundle build's seeding run (``PROVISA_DEMO_SEED_EXPORT=true``):
    each template is landed from its demo's own sources and written out as the demo's seed
    (demo_replicas.export_seed). Otherwise a template is landed from the seed its bundle carries."""
    from provisa.core.demo_bundles import seeding_run

    return seeding_run()


async def land_from_seed(org_id: str) -> None:
    """In the template org ``org_id``'s runtime build, with its model loaded and the org bound,
    before its replica convergence is wired: land its replicas from the demo's seed when this
    deployment's store does not hold them (demo_replicas.adopt_seed). A bundle build's seeding
    run lands them from the demo's sources instead, and writes the seed. Its bitemporal views'
    history, when its bundle carries it, likewise (demo_replicas.adopt_views; REQ-1162)."""
    import logging

    from provisa.api.demo_replicas import adopt_seed, adopt_views, landed_here

    if seeding_release():
        return
    demo = demo_of_template(org_id)
    if await asyncio.to_thread(adopt_views, org_id, demo.directory):
        logging.getLogger(__name__).info(
            "demo template %s: views' history from its seed", demo.name
        )
    from provisa.api.demo_region_seed import kept_here

    if await landed_here(org_id) >= len(kept_here(replicated_tables(template_seed(org_id)))):
        return
    if await adopt_seed(org_id, demo.directory):
        logging.getLogger(__name__).info("demo template %s: landed from its seed", demo.name)


async def ensure_demo_templates(pool) -> dict[str, str]:
    """Create and build the template org of every published demo that does not have a ready one,
    then wait for each template's replicas to land and run the ready hooks.

    Run by the sandbox tier at startup -- deploy time -- and never on a visitor's request. The
    build is the create path's own (orgs_router._provision_org_task), with the demo seeded: the
    template is a demo organisation (``seeded_demo``), so its model is the demo's configuration.
    A bundled demo's template built from another version than the ready one is replaced. What
    each demo's template did: ``"ready"`` (built before; its runtime is built now) or
    ``"building"``."""
    from provisa.core.connection_loop import spawn_background

    outcome: dict[str, str] = {}
    for demo in published_demos().values():
        if bundled_demos() and _templated(demo.name) != demo.directory.name:
            await _retire_template(demo.name)
        build = await _template_row(pool, demo)
        _mark_templated(demo)
        task = spawn_background(_prepare(demo, build), name=f"demo-template-{demo.name}")
        _build_tasks.add(task)
        task.add_done_callback(_build_tasks.discard)
        outcome[demo.name] = "building" if build else "ready"
    return outcome


def _templated(name: str) -> str | None:
    """The version of bundled demo ``name`` its template was built from."""
    path = bundles_root() / name / "templated"
    return path.read_text().strip() if path.is_file() else None


def _mark_templated(demo: PublishedDemo) -> None:
    if bundled_demos():
        (bundles_root() / demo.name / "templated").write_text(demo.directory.name + "\n")


# The lock that keeps two openers of one demo from replacing its template at once, one a demo.
# Each request runs on a thread and event loop of its own (REQ-1882): an asyncio.Lock binds to the
# first loop that waits on it, and an opener on another loop then waited for good.
_following: dict[str, CrossLoopLock] = {}


async def follow_latest(name: str) -> None:
    """Before a sandbox of bundled demo ``name`` is opened: ask the node for a newer published
    version, if there is one, and serve the ready one -- its template replaced first when it was
    built from another version. The visitor never waits for a newer version's stores: they are
    served it from the next sandbox opened after the node has it ready."""
    import logging

    import httpx

    from provisa.core.demo_bundles import BundleError, ready_version, want

    if name not in bundled_demos():
        return
    try:
        latest = await asyncio.to_thread(latest_published, name)
    except (BundleError, httpx.HTTPError):
        # REQ-2049 (approved hosting design): the ready version is served whatever the registry
        # answers; asking for a newer one is retried at the next sandbox opened. Logged with its
        # cause, never silent.
        logging.getLogger(__name__).warning(
            "demo %s: the registry could not be asked for a newer version", name, exc_info=True
        )
    else:
        if latest != ready_version(name):
            await asyncio.to_thread(want, name, latest)
    ready = ready_version(name)
    if ready is None:
        raise ApiError(
            503,
            "sandbox.demo_not_ready",
            f"The {name} demo is still being prepared; try again shortly",
            demo=name,
        )
    lock = _following.setdefault(name, CrossLoopLock())
    async with lock:
        if _templated(name) == ready:
            return
        await _retire_template(name)
        from provisa.api.admin.orgs_router import _admin_pool

        demo = published_demo(name)
        build = await _template_row(_admin_pool(), demo)
        _mark_templated(demo)
        await _prepare(demo, build)


async def _retire_template(name: str) -> None:
    """Remove demo ``name``'s template org whole, when there is one: its bundle was replaced."""
    import shutil

    from sqlalchemy import select

    from provisa.api.admin.orgs_router import _admin_pool, destroy_org
    from provisa.core import model_change, org_files
    from provisa.core.schema_admin import orgs

    org_id = template_org_id(name)
    async with _admin_pool().acquire() as conn:
        row = (await conn.execute_core(select(orgs.c.id).where(orgs.c.id == org_id))).fetchone()
    if row is None:
        return
    async with model_change.scope(f"replace demo template {org_id}"):
        await destroy_org(org_id)
    from provisa.security.sandbox_egress import forget

    forget(org_id)
    folder = org_files.folder(org_id)
    if folder.exists():
        await asyncio.to_thread(shutil.rmtree, folder)
    import logging

    logging.getLogger(__name__).info("demo template %s removed: its bundle was replaced", org_id)


async def _template_row(pool, demo: PublishedDemo) -> bool:
    """Make sure the demo's template org has a row, marked for building unless it is ready;
    whether it is to be built."""
    from sqlalchemy import select, update

    from provisa.core.schema_admin import orgs

    org_id = demo.template_org
    build = True
    async with pool.acquire() as conn:
        row = (
            await conn.execute_core(select(orgs.c.provisioning_state).where(orgs.c.id == org_id))
        ).fetchone()
        if row is None:
            await conn.execute_core(
                orgs.insert().values(
                    id=org_id,
                    name=f"Demo template: {demo.title}",
                    created_by=None,
                    provisioning_state="provisioning",
                    seeded_demo=True,
                )
            )
        elif row[0] == "ready":
            build = False
        else:
            await conn.execute_core(
                update(orgs)
                .where(orgs.c.id == org_id)
                .values(provisioning_state="provisioning", provisioning_error=None)
            )
    return build


async def _prepare(demo: PublishedDemo, build: bool) -> None:
    """Build (or only load) the template, wait until every replica it declares has landed, then
    run the ready hooks. A replica whose build failed stops the wait: the template is not ready,
    and a visitor is told so by name (demo_sandbox._template_ready)."""
    import logging

    from provisa.api.admin.orgs_router import _provision_org_task
    from provisa.api.app import ensure_org_runtime
    from provisa.core.request_context import reset_current_org, set_current_org

    log = logging.getLogger(__name__)
    org_id = demo.template_org
    if build:
        await _provision_org_task(org_id, True, None, False)
    rt = await ensure_org_runtime(org_id)
    # REQ-2053: the environments its setup.yaml declares, which every sandbox of it carries.
    await create_environments(org_id, demo_environments(demo.directory), None)
    from provisa.api.demo_replicas import export_seed

    exporting = seeding_release()
    while True:
        landed, failed, declared = await _replica_progress(rt)
        if failed:
            log.error("demo template %s: replicas failed to land: %s", demo.name, failed)
            return
        # Every table of a demo is replicated always (template_seed): its template is ready when
        # each one has a landed replica, whether or not convergence has asked for all yet.
        if landed >= declared:
            break
        await asyncio.sleep(_LANDED_POLL_S)
    log.info("demo template %s: every replica landed", demo.name)
    from provisa.api.demo_region_seed import regional, views_built_here, views_kept_here

    if exporting and regional():
        # REQ-2068: a region's seed carries its views too; no node of it builds them after.
        while await views_built_here(org_id) < views_kept_here(template_seed(org_id)):
            await asyncio.sleep(_LANDED_POLL_S)
        await export_seed(org_id, demo.directory)
        log.info("demo template %s: region seed written to %s", demo.name, demo.directory)
    elif exporting and replicated_tables(template_seed(org_id)):
        # Only what the demo serves from replicas has a seed; its own sources are read live.
        await export_seed(org_id, demo.directory)
        log.info("demo template %s: seed written to %s", demo.name, demo.directory)
    token = set_current_org(org_id)
    try:
        for hook in _ready_hooks:
            try:
                await hook(demo, org_id)
            except Exception:  # allow-ble: one deploy-time derivation failing leaves the template usable and the next hook still runs; logged with its cause
                log.exception("demo template %s: ready hook %r failed", demo.name, hook)
    finally:
        reset_current_org(token)


async def _replica_progress(rt) -> tuple[int, list[str], int]:
    """How many of the template's replicas have landed, the tables whose last build failed, and
    how many replicated tables its demo's configuration declares (replicated_tables: not its
    views, and not the built-in tables every org registers)."""
    from provisa.federation import replica_state

    from provisa.api.demo_replicas import landed_here

    from provisa.api.demo_region_seed import kept_here

    declared = len(kept_here(replicated_tables(template_seed(rt.org_id))))
    async with rt.tenant_db.acquire() as conn:
        _recorded, failed = await replica_state.progress(conn)
    # Landed: recorded as built for this store AND held by it, rows and all.
    landed = await landed_here(rt.org_id)
    return landed, failed, declared


_build_tasks: set = set()


async def record_demo_destinations(org_id: str) -> None:
    """Record, for the sandbox org ``org_id``, the destinations its demo's configuration declares
    (provisa.security.sandbox_egress): a demo template's and a demo sandbox's are the named demo's
    sources, and the hosted sandbox org's (REQ-1598) are the deployment's demo configuration's."""
    from sqlalchemy import select

    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.api.app import state
    from provisa.core.org_ids import is_sandbox_tier_org
    from provisa.core.schema_admin import demo_sandboxes
    from provisa.security.sandbox_egress import set_demo_destinations, source_fields

    if is_template_org(org_id):
        sources = template_seed(org_id).sources
    elif is_sandbox_tier_org(org_id):
        async with _admin_pool().acquire() as conn:
            row = (
                await conn.execute_core(
                    select(demo_sandboxes.c.demo).where(demo_sandboxes.c.org_id == org_id)
                )
            ).fetchone()
        if row is None:
            raise RuntimeError(f"sandbox org {org_id!r} has no demo_sandboxes row")
        sources = template_seed(template_org_id(row[0])).sources
    else:
        sources = state.seed_config.sources
    set_demo_destinations(org_id, [source_fields(s) for s in sources])
