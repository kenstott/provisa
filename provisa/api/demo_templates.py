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
import threading
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Awaitable, Callable
from typing import Any

import yaml

from provisa.api.errors import ApiError
from provisa.api.sandbox_tier import TEMPLATE_PREFIX, is_template_org, template_org_id
from provisa.core.replicate import ALWAYS
from provisa.core.connection_loop import CrossLoopLock

# reference-convention: REQ-1858 -- a demo config's path to its own directory, not a secret
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
    """Each named demo's directory: of each bundled demo the node has pulled, the version whose
    stores are up (ready), else the version pulled -- offered, its stores started at its first
    open; else every one under ``named_demos_dir()``."""
    from provisa.core.demo_bundles import pulled_version, ready_version

    names = bundled_demos()
    if not names:
        return [spec.parent for spec in sorted(named_demos_dir().glob("*/demo.yaml"))]
    from provisa.core.demo_bundles import check_directory

    found = []
    for name in names:
        version = ready_version(name) or pulled_version(name)
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

    @property
    def stores_ready(self) -> bool:
        """Whether its stores are up where it is served: a bundled demo's once the node has brought
        this version's up since it started (its stores.env written); a demo served from its
        directory runs its stores itself."""
        if not bundled_demos():
            return True
        from provisa.core.demo_bundles import ready_version

        return ready_version(self.name) == self.directory.name


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


def demo_visitor_attributes(directory: Path) -> dict[str, str]:
    """The attributes demo ``directory``'s sandbox visitor carries (REQ-2151): demo.yaml's
    ``visitor: attributes:``, each a name and its text value; none when it declares none. A
    declaration that is not names to text refuses the demo by name."""
    spec_file = directory / "demo.yaml"
    declared = (yaml.safe_load(spec_file.read_text()).get("visitor") or {}).get("attributes")
    if declared is None:
        return {}
    if not isinstance(declared, dict) or not all(
        isinstance(name, str) and isinstance(value, str) for name, value in declared.items()
    ):
        raise ValueError(f"{spec_file}: visitor.attributes must map each name to a text value")
    return dict(declared)


def demo_profilers(directory: Path) -> list[str]:
    """The Data Profiler sources demo ``directory``'s setup.yaml profiles (REQ-2052): each
    ``profile`` step's profiler, in order; none when it has no setup.yaml."""
    path = directory / "setup.yaml"
    if not path.is_file():
        return []
    steps = (yaml.safe_load(path.read_text()) or {}).get("steps") or []
    return [step["profile"] for step in steps if "profile" in step]


async def profile_template(demo: "PublishedDemo", org_id: str) -> list[str]:
    """Load the demo's profile runs into its template (REQ-2052): a profile run's results are
    tables of the org's own schema, which a sandbox is cloned with (env_copy.copy_profile_results).
    They are the bundle build's runs, carried in the bundle (provisa.profiler.carry): the build ran
    them where the demo's setup ran, between its other steps, so what changed between two runs is
    measured; the template's sources hold the data as setup left it. Returns what kept them from
    loading; empty when the demo profiles nothing or they loaded."""
    import logging

    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.profiler.carry import load_runs, results_path

    log = logging.getLogger(__name__)
    if not demo_profilers(demo.directory):
        return []
    path = results_path(demo.directory)
    if not path.is_file():
        return [f"the bundle carries no profile runs ({path.name}): rebuild it"]
    token = set_current_org(org_id)
    try:
        # The template's runtime is built before it is prepared (ensure_org_runtime).
        model_db = state.model_db
        assert model_db is not None
        async with model_db.acquire() as conn:
            loaded = await load_runs(conn, path)
    except ValueError as exc:
        return [str(exc)]
    finally:
        reset_current_org(token)
    log.info("demo template %s: %d profile result relations loaded", demo.name, len(loaded))
    return []


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
        # The demo's own stores, where the node runs them (its sources are read live there), and
        # the names of their data in the node's shared stores.
        from provisa.core.demo_bundles import stores_env

        raw = _with_stores(raw, stores_env(demo.directory))
    else:
        # Served from its directory, the demo runs its stores as its own: its data has the names
        # its configuration gives it, with no suffix.
        from provisa.core.demo_bundles import store_namespaces

        raw = _with_stores(raw, store_namespaces(demo.directory))
    _sandbox_sees_what_its_rights_cover(raw, _role_rights(raw))
    from provisa.core.demo_secrets import resolving

    # REQ-2095: the secrets it names, from its own secrets.yaml, wherever it is parsed.
    with resolving(demo.directory, org_id):
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
        if demo.stores_ready
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
    each: the sonnets and cloudpolicy demos', which their search tables embed a phrase with.

    Read from each demo's own configuration file, whether or not its stores are up: a bundled
    tier starts no demo's stores at boot, and a model left to the first search that needs it
    took that search past its 60s request timeout. A model's declaration names no store."""
    from provisa.core.config_loader import read_config_with_includes
    from provisa.core.models import VectorModelConfig

    seen: dict[str, Any] = {}
    for demo in published_demos().values():
        raw = read_config_with_includes(demo.directory / "config.yaml")
        for declared in raw.get("vector_models", []):
            model = VectorModelConfig(**declared)
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


def declared_rows_directory(org_id: str) -> Path | None:
    """REQ-2089: the directory whose ``replica_rows`` the org ``org_id`` lands its declared
    replicas from: a demo template's demo directory, or, for the deployment org, the directory
    of the config it was launched with. Any other org has none."""
    from provisa.api.app import state
    from provisa.api.sandbox_tier import is_template_org
    from provisa.core.config_location import config_path_str

    if is_template_org(org_id):
        return demo_of_template(org_id).directory
    if org_id == state.org_id:
        return Path(config_path_str()).parent
    return None


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
    from provisa.api.demo_region_seed import kept_here, landed_elsewhere, regional

    landed = await landed_here(org_id) >= len(kept_here(replicated_tables(template_seed(org_id))))
    # REQ-2068: on a regions tier, the other regions' seeds too -- a node whose own region keeps
    # nothing of the demo once took that for the whole template landed, and no other region's
    # seed was ever adopted (every sandbox of it failed reading that region's record).
    if landed and (not regional() or await landed_elsewhere(org_id, demo.directory)):
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
    ``"building"``.

    A bundled demo whose stores the node has not brought up is not built here: the node starts
    no demo's stores at boot, and a template is built from its stores. One served as built is
    served so; any other is built at the demo's first open (follow_latest)."""
    outcome = {}
    for demo in published_demos().values():
        if demo.stores_ready:
            outcome[demo.name] = await _queue_template(pool, demo)
        elif await _served_as_built(pool, demo):
            outcome[demo.name] = "ready (as built)"
        else:
            outcome[demo.name] = "built at its first open"
    _start_builds()
    _startup_queued.set()
    return outcome


async def queue_late_templates(pool) -> list[str]:
    """Queue the template of each published demo this process has not been asked to build -- a
    bundled demo the node marked ready after the tier started (its ``ready`` file, read by
    published_demos) -- as ensure_demo_templates queued the others at startup. Run by the tier's
    scheduler, never on a visitor's request; nothing before the startup has queued its own. The
    demos queued."""
    if not _startup_queued.is_set():
        return []
    queued = []
    for demo in published_demos().values():
        lock = _following.setdefault(demo.name, CrossLoopLock())
        async with lock:
            # Its stores not up: its first open asks for them and builds it (follow_latest).
            if not demo.stores_ready:
                continue
            # A visitor's open builds a newer version itself (follow_latest), marked first.
            if template_builds.state_of(demo.name) != "unknown" or (
                await _templated(pool, demo.name) == demo.directory.name
            ):
                continue
            await _queue_template(pool, demo)
        queued.append(demo.name)
    if queued:
        _start_builds()
    return queued


async def _served_as_built(pool, demo: PublishedDemo) -> bool:
    """Whether ``demo``'s template is served as it is, recorded ready: a template built from this
    demo version by this release, its store on disk as recorded, is served at once -- no build, no
    runtime, no wait for its replicas. Its runtime is built when a sandbox of it is first opened.
    A regions tier's stores are its regions' own."""
    from provisa.api.demo_region_seed import regional

    if regional() or seeding_release() or not await warm_ready(pool, demo):
        return False
    template_builds.record(demo.name, True)
    return True


async def _queue_template(pool, demo: PublishedDemo) -> str:
    """Queue ``demo``'s template, or serve it as built; what its template did."""
    if await _served_as_built(pool, demo):
        return "ready (as built)"
    # A template another demo version or another Provisa release built is replaced, not loaded: its
    # schema is that release's (no migrations), and a column added since fails every read of it
    # (sources.download_from on the sandbox tier).
    # Another demo version or release is the record's, on the control plane (readiness_version):
    # never told by a node's own files, which a fresh disk does not have.
    stale = await _built_for(pool, demo) != readiness_version(demo)
    if stale:
        await _retire_template(demo.name)
    build = await _template_row(pool, demo)
    template_builds.add(demo, build=build)
    return "building" if build else "ready"


def _start_builds() -> None:
    """Start the workers that build the queued templates, unless some are working: they take what
    was queued since."""
    from provisa.core import settings_registry
    from provisa.core.connection_loop import spawn_background

    if not template_builds.claim():
        return
    # A few at a time (sandbox.template_builds_at_once): all at once, a small tier's machine was
    # too busy to serve the visitors it was preparing for.
    at_once = int(settings_registry.value("sandbox.template_builds_at_once"))
    task = spawn_background(template_builds.run(at_once, _prepare), name="demo-templates")
    _build_tasks.add(task)
    task.add_done_callback(_build_tasks.discard)


class TemplateBuilds:
    """The templates waiting to be built (or loaded), in order, and the workers that take them:
    ``at_once`` of them, each building the next waiting template until none is left. A visitor's
    open of a demo still waiting moves it to the front (:meth:`prefer`). Visitors' requests run on
    threads of their own, so the waiting list is guarded by a thread lock."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._waiting: list[tuple[Any, bool]] = []
        #: The demos whose templates are being built now, and each finished one's outcome.
        self._building: set[str] = set()
        self._finished: dict[str, str] = {}
        #: The workers taking templates now, and whether ones claimed (``claim``) are starting.
        self._workers = 0
        self._starting = False

    def state_of(self, name: str) -> str:
        """Where demo ``name``'s template is: ``queued``, ``building``, ``ready``, ``failed``, or
        ``unknown`` when this process has not been asked to build it (the tier has not started its
        builds). Read without building or loading anything."""
        with self._lock:
            if any(demo.name == name for demo, _build in self._waiting):
                return "queued"
            if name in self._building:
                return "building"
            return self._finished.get(name, "unknown")

    def record(self, name: str, ready: bool) -> None:
        """A template built (or replaced) outside the queue: its outcome (``follow_latest``)."""
        with self._lock:
            self._finished[name] = "ready" if ready else "failed"

    def add(self, demo: Any, *, build: bool) -> None:
        with self._lock:
            self._waiting.append((demo, build))

    def prefer(self, name: str) -> bool:
        """Move demo ``name`` to the front when it is still waiting; whether it was."""
        with self._lock:
            for i, (demo, _build) in enumerate(self._waiting):
                if demo.name == name:
                    self._waiting.insert(0, self._waiting.pop(i))
                    return True
        return False

    def claim(self) -> bool:
        """Whether workers are to be started for the waiting templates: none is working or
        starting. The claim lasts until :meth:`run` starts them."""
        with self._lock:
            if self._workers or self._starting:
                return False
            self._starting = True
            return True

    def _next(self) -> tuple[Any, bool] | None:
        with self._lock:
            if not self._waiting:
                # A worker stops where it found nothing waiting: a template queued after is
                # taken by the workers then started (claim).
                self._workers -= 1
                return None
            demo, build = self._waiting.pop(0)
            self._building.add(demo.name)
            self._finished.pop(demo.name, None)
            return demo, build

    def _done(self, name: str, state: str) -> None:
        with self._lock:
            self._building.discard(name)
            self._finished[name] = state

    async def run(self, at_once: int, prepare: Callable[[Any, bool], Awaitable[bool]]) -> None:
        """Build every waiting template, ``at_once`` at a time."""
        import logging

        log = logging.getLogger(__name__)
        workers = max(at_once, 1)
        with self._lock:
            self._starting = False
            self._workers += workers

        async def _work() -> None:
            while (waiting := self._next()) is not None:
                demo, build = waiting
                try:
                    ready = await prepare(demo, build)
                except Exception:  # allow-ble: one template that cannot be built leaves the others to build; logged with its cause, and its demo is refused as not ready by name
                    log.exception("demo template %s could not be prepared", demo.name)
                    ready = False
                self._done(demo.name, "ready" if ready else "failed")

        await asyncio.gather(*(_work() for _ in range(workers)))


#: The tier's template builds (ensure_demo_templates); a visitor's open moves its demo forward.
template_builds = TemplateBuilds()


def readiness_version(demo: PublishedDemo) -> str:
    """What a template's readiness record is for: its demo's version (a bundle's directory, else
    the demo's own) and the Provisa release that built it -- a replica's definition is computed
    by the release, so another release builds the template again."""
    from provisa.core.demo_bundles import running_provisa

    return f"{demo.directory.name}@{running_provisa()}"


async def record_ready(pool, demo: PublishedDemo) -> None:
    """Record that ``demo``'s template is ready, with the rows its store holds now (REQ-2006)."""
    from sqlalchemy import func

    from provisa.api.demo_replicas import store_counts
    from provisa.core.schema_admin import demo_template_readiness

    counts = await asyncio.to_thread(store_counts, demo.template_org)
    async with pool.acquire() as conn:
        await conn.upsert(
            demo_template_readiness,
            {
                "template_org": demo.template_org,
                "version": readiness_version(demo),
                "replicas": counts["replicas"],
                "views": counts["views"],
            },
            index_elements=["template_org"],
            set_extra={"ready_at": func.now()},
        )


async def warm_ready(pool, demo: PublishedDemo) -> bool:
    """Whether ``demo``'s template can be served as it is, right after a start, with no build and
    no wait for its replicas: its org is ready, its readiness record is for this demo version and
    this Provisa release, and its store on disk holds exactly the rows recorded."""
    from sqlalchemy import select

    from provisa.api.demo_replicas import store_counts
    from provisa.core.schema_admin import demo_template_readiness, orgs

    async with pool.acquire() as conn:
        org = (
            await conn.execute_core(
                select(orgs.c.provisioning_state).where(orgs.c.id == demo.template_org)
            )
        ).fetchone()
        record = (
            await conn.execute_core(
                select(
                    demo_template_readiness.c.version,
                    demo_template_readiness.c.replicas,
                    demo_template_readiness.c.views,
                ).where(demo_template_readiness.c.template_org == demo.template_org)
            )
        ).fetchone()
    if org is None or org[0] != "ready" or record is None:
        return False
    if record.version != readiness_version(demo):
        return False
    held = await asyncio.to_thread(store_counts, demo.template_org)
    return held == {"replicas": dict(record.replicas), "views": dict(record.views)}


async def _built_for(pool, demo: PublishedDemo) -> str | None:
    """The demo version and Provisa release ``demo``'s template was recorded ready for
    (readiness_version); None when it was never recorded ready."""
    from sqlalchemy import select

    from provisa.core.schema_admin import demo_template_readiness

    async with pool.acquire() as conn:
        row = (
            await conn.execute_core(
                select(demo_template_readiness.c.version).where(
                    demo_template_readiness.c.template_org == demo.template_org
                )
            )
        ).fetchone()
    return row[0] if row is not None else None


async def _templated(pool, name: str) -> str | None:
    """The version of bundled demo ``name`` its template was recorded ready from, on the control
    plane beside its ready record (readiness_version: ``<version>@<release>``); None when it was
    never recorded ready. Never a node's own file: a fresh disk under a shared plane would read
    no version and retire the template the other nodes serve."""
    from sqlalchemy import select

    from provisa.core.schema_admin import demo_template_readiness

    async with pool.acquire() as conn:
        version = (
            await conn.execute_core(
                select(demo_template_readiness.c.version).where(
                    demo_template_readiness.c.template_org == template_org_id(name)
                )
            )
        ).scalar_one_or_none()
    return None if version is None else version.partition("@")[0]


# The lock that keeps two openers of one demo from replacing its template at once, one a demo.
# Each request runs on a thread and event loop of its own (REQ-1882): an asyncio.Lock binds to the
# first loop that waits on it, and an opener on another loop then waited for good.
_following: dict[str, CrossLoopLock] = {}


async def follow_latest(name: str) -> None:
    """Before a sandbox of bundled demo ``name`` is opened: ask the node for a newer published
    version, if there is one, and serve the ready one -- its template replaced first when it was
    built from another version. The visitor never waits for a newer version's stores: they are
    served it from the next sandbox opened after the node has it ready.

    The node starts no demo's stores until the demo is asked for: the first open since it started
    asks for them, and is told the demo is being prepared until they are up. A template not built
    from the version served is then queued to be built (template_builds), not built while the
    visitor waits; the open is told the same until it is ready (demo_sandbox._template_ready)."""
    import logging

    import httpx

    from provisa.core.demo_bundles import BundleError, hold, ready_version, request

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
            # The node's sync pulls the latest published and serves it once its stores are up.
            await asyncio.to_thread(request, name)
    ready = ready_version(name)
    if ready is None:
        # Its stores are not up since the node started: asked for.
        await asyncio.to_thread(request, name)
        raise ApiError(
            503,
            "sandbox.demo_not_ready",
            f"The {name} demo is still being prepared; try again shortly",
            demo=name,
        )
    # Opened while its stores linger after its last sandbox closed: they are kept up.
    await asyncio.to_thread(hold, name)
    lock = _following.setdefault(name, CrossLoopLock())
    async with lock:
        from provisa.api.admin.orgs_router import _admin_pool

        # Being prepared here already: the open waits for that preparation
        # (demo_sandbox._template_ready). Its ready record is written only when it finishes, so
        # it read as built from another version, and queueing it again built it twice at once --
        # one build's rollback dropping the schema the other was creating its tables in.
        if template_builds.state_of(name) in ("queued", "building"):
            return
        if (
            await _templated(_admin_pool(), name) == ready
            and template_builds.state_of(name) != "unknown"
        ):
            return

        # _queue_template replaces a template built from another version.
        await _queue_template(_admin_pool(), published_demo(name))
        _start_builds()


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


async def _prepare(demo: PublishedDemo, build: bool) -> bool:
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
    said = None
    while True:
        landed, failed, declared, waiting = await _replica_progress(rt)
        if failed:
            log.error("demo template %s: replicas failed to land: %s", demo.name, failed)
            return False
        # Every table of a demo is replicated always (template_seed): its template is ready when
        # each one has a landed replica, whether or not convergence has asked for all yet.
        if landed >= declared:
            break
        if waiting != said:
            said = waiting
            log.warning(
                "demo template %s: %d of %d replicas landed, waiting on %s",
                demo.name,
                landed,
                declared,
                waiting,
            )
        await asyncio.sleep(_LANDED_POLL_S)
    log.info("demo template %s: every replica landed", demo.name)
    # REQ-1942, REQ-2053: its Test (synthetic) environments, generated once here (or adopted from
    # the seed its bundle carries), which every sandbox of it is given (demo_synthetic).
    from provisa.api import demo_synthetic

    token = set_current_org(org_id)
    try:
        await demo_synthetic.prepare(
            org_id,
            demo.directory,
            demo_synthetic.synthetic_envs(demo_environments(demo.directory)),
            exporting=exporting,
        )
    finally:
        reset_current_org(token)
    from provisa.api.demo_region_seed import (
        regional,
        views_built_here,
        views_kept_here,
        views_waiting_here,
    )

    if exporting and regional():
        # REQ-2068: a region's seed carries its views too; no node of it builds them after.
        said = None
        while (built := await views_built_here(org_id)) < (
            kept := views_kept_here(template_seed(org_id))
        ):
            waiting = await views_waiting_here(org_id)
            if waiting != said:
                said = waiting
                log.warning(
                    "demo template %s: %d of %d views built in this region, waiting on %s",
                    demo.name,
                    built,
                    kept,
                    waiting,
                )
            await asyncio.sleep(_LANDED_POLL_S)
        await export_seed(org_id, demo.directory)
        log.info("demo template %s: region seed written to %s", demo.name, demo.directory)
    elif exporting and replicated_tables(template_seed(org_id)):
        # Only what the demo serves from replicas has a seed; its own sources are read live.
        await export_seed(org_id, demo.directory)
        log.info("demo template %s: seed written to %s", demo.name, demo.directory)
    if not exporting:
        # REQ-2052: its profile runs, carried in its bundle, which every sandbox of it is cloned
        # with. Runs that do not load leave the template not ready, as a replica that fails to
        # land does.
        failed_runs = await profile_template(demo, org_id)
        if failed_runs:
            log.error("demo template %s: profile runs not loaded: %s", demo.name, failed_runs)
            return False
    token = set_current_org(org_id)
    try:
        for hook in _ready_hooks:
            try:
                await hook(demo, org_id)
            except Exception:  # allow-ble: one deploy-time derivation failing leaves the template usable and the next hook still runs; logged with its cause
                log.exception("demo template %s: ready hook %r failed", demo.name, hook)
    finally:
        reset_current_org(token)
    if not exporting:
        # REQ-2006: the record says which demo version the template is (_templated, _built_for),
        # on every tier: a regions tier's unrecorded template read as another version's, and the
        # next open retired it and built it again. The next start serves it as it is built
        # (warm_ready) only off a regions tier (_served_as_built).
        from provisa.api.admin.orgs_router import _admin_pool

        await record_ready(_admin_pool(), demo)
    return True


async def _replica_progress(rt) -> tuple[int, list[str], int, str]:
    """How many of the template's replicas have landed, the tables whose last build failed, how
    many replicated tables its demo's configuration declares (replicated_tables: not its views,
    and not the built-in tables every org registers), and what it waits on, table by table."""
    from provisa.federation import replica_state

    from provisa.api.demo_replicas import LANDED, landing_here

    from provisa.api.demo_region_seed import kept_here

    tables = kept_here(replicated_tables(template_seed(rt.org_id)))
    async with rt.tenant_db.acquire() as conn:
        _recorded, failed = await replica_state.progress(conn)
    # Landed: recorded as built for this store AND held by it, rows and all.
    landing = await landing_here(rt.org_id)
    landed = sum(1 for state in landing.values() if state == LANDED)
    return landed, failed, len(tables), waiting_on(tables, landing)


def waiting_on(tables: list[Any], landing: dict[str, str]) -> str:
    """What a template's replicas of ``tables`` wait on, by table: each one's landing
    (demo_replicas.landing) that is not landed, or that no build of it is recorded."""
    from provisa.api.demo_replicas import LANDED
    from provisa.federation.replica_address import replica_table_name

    out = []
    for t in tables:
        state = landing.get(
            replica_table_name(t.source_id, t.schema_name, t.table_name), "no build recorded"
        )
        if state != LANDED:
            out.append(f"{t.source_id}.{t.schema_name}.{t.table_name}: {state}")
    return "; ".join(out)


_build_tasks: set = set()
#: Set once the startup has queued every published demo's template (ensure_demo_templates).
_startup_queued = threading.Event()


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
