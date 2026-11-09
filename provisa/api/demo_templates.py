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

# Requirements: REQ-2006, REQ-1858, REQ-1919, REQ-2049, REQ-2201

from __future__ import annotations

import asyncio
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
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


def demo_scans(directory: Path) -> list[str]:
    """The documents demo ``directory``'s setup.yaml scans for terms (REQ-2201): each ``scan``
    step's URL (the step's value, or its ``url``), in order; none when it has no setup.yaml."""
    path = directory / "setup.yaml"
    if not path.is_file():
        return []
    steps = (yaml.safe_load(path.read_text()) or {}).get("steps") or []
    scans = [step["scan"] for step in steps if "scan" in step]
    return [scan["url"] if isinstance(scan, dict) else scan for scan in scans]


async def scan_template(demo: "PublishedDemo", org_id: str) -> list[str]:
    """Load the demo's scans for terms into its template (REQ-2201): the library documents its
    setup scanned and the scans of them with their drafts, which a sandbox is cloned with
    (env_copy.copy_term_scans). They are the bundle build's, carried in the bundle
    (provisa.term_finding.carry): a scan asks the org's AI model for minutes, and the demo's
    tour opens the one done ahead. Returns what kept them from loading; empty when the demo scans
    nothing or they loaded."""
    import logging

    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.term_finding.carry import load_scans, scans_path

    log = logging.getLogger(__name__)
    if not demo_scans(demo.directory):
        return []
    path = scans_path(demo.directory)
    if not path.is_file():
        return [f"the bundle carries no scans ({path.name}): rebuild it"]
    token = set_current_org(org_id)
    try:
        model_db = state.model_db
        assert model_db is not None
        async with model_db.acquire() as conn, conn.transaction():
            loaded = await load_scans(conn, path)
    except ValueError as exc:
        return [str(exc)]
    finally:
        reset_current_org(token)
    log.info("demo template %s: scans of %s loaded", demo.name, ", ".join(loaded))
    return []


def demo_environments(directory: Path) -> list[dict[str, str]]:
    """The environments demo ``directory``'s setup.yaml creates (REQ-2053): each ``environment``
    step's ``{name, data_mode}``, in order; none when it has no setup.yaml."""
    path = directory / "setup.yaml"
    if not path.is_file():
        return []
    steps = (yaml.safe_load(path.read_text()) or {}).get("steps") or []
    return [step["environment"] for step in steps if "environment" in step]


async def create_environments(
    org_id: str, wanted: list[dict[str, str]], created_by: str | None
) -> list[str]:
    """Create in ``org_id`` each of ``wanted``'s environments it does not have, from its prod:
    a demo's template its demo's, a sandbox its template's (REQ-2053). The names created."""
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
    created: list[str] = []
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
        created.append(env["name"])
    return created


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
    from provisa.core.demo_secrets import resolving

    # REQ-2095: the secrets it names, from its own secrets.yaml, wherever it is parsed.
    with resolving(demo.directory, org_id):
        return parse_config_dict(raw)


async def rebuild_template(directory: Path, seed: Any, conn: Any, engine: Any) -> list[str]:
    """Rebuild a template org's model from its demo's configuration ``seed`` (REQ-1919, every
    build of its runtime), with the profile runs the bundle at ``directory`` carries (REQ-2052);
    the source ids whose change feed could not be reached. The rebuild gives every table a new
    id, and its result tables name relations under those ids: a template served as built, holding
    its runs, lost them at its runtime's first build, and every sandbox cloned from it after that
    had none. A bundle of a demo that profiles and carries no runs is refused by name."""
    from provisa.core.config_loader import rebuild_from_config
    from provisa.profiler.carry import results_path
    from provisa.term_finding.carry import scans_path

    carried = None
    if demo_profilers(directory):
        carried = results_path(directory)
        if not carried.is_file():
            raise ValueError(f"the bundle carries no profile runs ({carried.name}): rebuild it")
    # REQ-2201: and its scans for terms, whose drafts name the glossary terms the rebuild makes.
    scans = None
    if demo_scans(directory):
        scans = scans_path(directory)
        if not scans.is_file():
            raise ValueError(f"the bundle carries no scans ({scans.name}): rebuild it")
    return await rebuild_from_config(seed, conn, engine, carried_runs=carried, carried_scans=scans)


async def write_declared_settings(model_db: Any, seed: Any) -> None:  # REQ-2006, REQ-2057
    """Every org setting a sandbox is cloned with (``carries_setting``, by ``clone_model``) that the
    demo's configuration declares, as its template org's own settings, in one write at every build
    of the template: its embedding models (the sonnets demo's ``lines_like`` is refused by the model
    allowlist without them), its derived roles (retail's region__eu, REQ-2151). A carried setting
    the configuration does not declare is cleared, as the rest of the template's model is rebuilt
    from its configuration (REQ-1919); a setting the configuration cannot declare is cleared too."""
    from provisa.core.env_classes import carries_setting
    from provisa.core.org_settings import (
        ORG_BLOCK_FIELDS,
        ORG_OVERRIDABLE_KEYS,
        write_org_overrides,
    )

    updates: dict[str, Any] = {}
    for key in sorted(k for k in ORG_OVERRIDABLE_KEYS if carries_setting(k)):
        declared = key in seed.model_fields_set
        value = getattr(seed, key) if declared else None
        if key in ORG_BLOCK_FIELDS and value is not None:
            # Only the fields an org owns; the rest of the block is the platform's (org_settings).
            value = {k: getattr(value, k) for k in value.model_fields_set & ORG_BLOCK_FIELDS[key]}
        elif isinstance(value, list):
            value = [v.model_dump() for v in value]
        elif value is not None:
            value = value.model_dump()
        updates[key] = value or None
    await write_org_overrides(model_db, updates, updated_by="platform")


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
    """Locate the demos' connector bundles in the runtime-deps cache at startup, outside any
    request. They were staged at build or packaging time (``python -m provisa.runtime_deps.stage_assets``):
    a runtime never fetches one (REQ-294). A bundle that is not staged is logged by name with the
    fix; the request that needs it is refused the same way (``BundleResolver.resolve``)."""
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
            log.exception("connector bundle %s %s is not staged", spec.artifact_name, spec.version)


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


def tour_standard_roles(directory: Path) -> list[str]:  # REQ-2155
    """The standard roles the demo's tour acts as (its steps' ``role``), in tour order: a role is
    acted as only when it is held, so a sandbox's person is granted them with the demo's own. One
    the sandbox withholds (what org_admin holds and sandbox does not) refuses the demo by name."""
    from provisa.core.db import SANDBOX_WITHHELD, STANDARD_ROLE_RIGHTS

    tour = directory / "tour.yaml"
    if not tour.is_file():
        return []
    named = [s["role"] for s in yaml.safe_load(tour.read_text())["steps"] if "role" in s]
    out = []
    for role_id in dict.fromkeys(named):
        if role_id not in STANDARD_ROLE_RIGHTS:
            continue
        withheld = sorted(STANDARD_ROLE_RIGHTS[role_id] & SANDBOX_WITHHELD)
        if withheld:
            raise ValueError(
                f"{directory.name}: its tour acts as {role_id!r}, which holds "
                f"{', '.join(withheld)}, which a sandbox's person may not"
            )
        out.append(role_id)
    return out


def demo_roles(template_org: str) -> list[str]:
    """The roles a demo's configuration defines for itself (its shop analyst, say), and the standard
    roles its tour acts as (:func:`tour_standard_roles`), which a sandbox's person also holds so the
    demo's tour can run as each. One that would carry a right the sandbox role withholds (REQ-2155:
    what org_admin holds and sandbox does not), on its own or through a standard role it inherits
    from, is refused by name rather than granted."""
    from provisa.core.db import SANDBOX_WITHHELD, standard_role_dicts
    from provisa.security.inheritance import flatten_role_dicts

    own = [role.model_dump() for role in template_seed(template_org).roles]
    flattened = {r["id"]: r for r in flatten_role_dicts([*standard_role_dicts(), *own])}
    out = []
    for role in own:
        withheld = sorted(set(flattened[role["id"]]["capabilities"]) & SANDBOX_WITHHELD)
        if withheld:
            raise ValueError(
                f"demo role {role['id']!r} of {template_org} holds {', '.join(withheld)}, which a "
                "sandbox's person may not"
            )
        out.append(role["id"])
    return [*out, *tour_standard_roles(demo_of_template(template_org).directory)]


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
    served so; any other is built ahead (:func:`build_ahead`). Every demo's stores are asked of
    the node there, at once, served as built or not."""
    outcome = {}
    for demo in published_demos().values():
        if demo.stores_ready:
            outcome[demo.name] = await _queue_template(pool, demo)
        elif await _served_as_built(pool, demo):
            outcome[demo.name] = "ready (as built)"
        else:
            outcome[demo.name] = "built ahead"
    _start_builds()
    _startup_queued.set()
    start_building_ahead(pool)
    return outcome


#: How long the node may take to bring a demo's stores up for a template built ahead: its pull,
#: its images and its data.
_STORES_LIMIT_S = 900.0
#: Each demo version a template was built ahead of, once each: one that could not be is the log's,
#: and an open asks again.
_tried_ahead: set[tuple[str, str]] = set()
_ahead_running = threading.Lock()


def start_building_ahead(pool) -> bool:
    """Start :func:`build_ahead`, unless it is running or this is a bundle build's seeding run,
    which lands every template itself; whether it started."""
    from provisa.core.connection_loop import spawn_background

    if seeding_release() or not bundled_demos() or not _ahead_running.acquire(blocking=False):
        return False

    async def _run() -> None:
        try:
            await build_ahead(pool)
        finally:
            _ahead_running.release()

    _build_tasks.add(spawn_background(_run(), name="demo-templates-ahead"))
    return True


def building_ahead() -> bool:
    """Whether the tier is at work on templates: a pass of :func:`build_ahead`, or a template
    waiting or being built."""
    return _ahead_running.locked() or template_builds.at_work()


async def build_ahead(pool) -> None:
    """REQ-2006: prepare every published demo at the tier's start, all at once, so no visitor waits
    on it: the node starts a demo's stores only when asked, and a template is built from its
    stores, so left to a visitor's open each first open after a wake waited for its demo's stores,
    and took a build. Every demo's stores are asked of the node together -- a template served as
    built included, whose stores are down after a wake as any other's -- and each template not
    built for the version served is queued once its stores are up, the queue building
    sandbox.template_builds_at_once at a time. A visitor's open during it waits on that same
    preparation (follow_latest). Each demo's stores are let go when it is prepared, to linger
    (sandbox.demo_stores_linger). What it finds is the log's: a demo that cannot be prepared is
    said by name, and does not keep the others."""
    ahead = []
    for demo in published_demos().values():
        if (demo.name, demo.directory.name) in _tried_ahead:
            continue
        if template_builds.state_of(demo.name) in ("unknown", "ready"):
            _tried_ahead.add((demo.name, demo.directory.name))
            ahead.append(demo.name)
    await asyncio.gather(*(_build_one_ahead(pool, name) for name in ahead))


async def _build_one_ahead(pool, name: str) -> None:
    import logging

    from provisa.api.demo_sandbox import note_in_use
    from provisa.core.demo_bundles import request, stores_failed

    log = logging.getLogger(__name__)
    await asyncio.to_thread(request, name)
    deadline = time.monotonic() + _STORES_LIMIT_S
    while True:
        demo = published_demos().get(name)
        if demo is not None and demo.stores_ready:
            break
        why = stores_failed(name)
        if why is not None:
            log.error("demo template %s cannot be built: its stores failed: %s", name, why)
            return
        if time.monotonic() >= deadline:
            log.error("demo template %s cannot be built: its stores did not come up", name)
            return
        await asyncio.sleep(_LANDED_POLL_S)
    async with _following.setdefault(name, CrossLoopLock()):
        # A visitor's open may have got here first (follow_latest).
        state = template_builds.state_of(name)
        if state not in ("queued", "building") and (
            state != "ready" or await _templated(pool, name) != demo.directory.name
        ):
            await _queue_template(pool, demo)
            _start_builds()
    while template_builds.state_of(name) in ("queued", "building"):
        await asyncio.sleep(_LANDED_POLL_S)
    if template_builds.state_of(name) != "ready":
        log.error("demo template %s could not be built ahead", name)
    await note_in_use(name)


def retire_templates_without_stores() -> list[str]:
    """Retire the runtime of each template whose stores the node has taken down; those retired. A
    template is built from its stores and they are let go after it, and its runtime kept running
    its views, polls, listeners and replicas against stores no longer there: failing every few
    seconds (PoolTimeout on the provisioning demo's warehouse), and on the one worker a visitor's
    open needs. An open asks for the stores again and builds the runtime it needs."""
    import logging

    from provisa.api.app import state
    from provisa.scheduler.jobs import unschedule_org

    retired = []
    for demo in published_demos().values():
        org_id = demo.template_org
        if demo.stores_ready or not state.org_registry.env_keys(org_id):
            continue
        unschedule_org(getattr(state, "_scheduler", None), org_id)
        state.org_registry.invalidate_org(org_id)
        logging.getLogger(__name__).warning(
            "demo template %s: its stores are down; its runtime is retired", demo.name
        )
        retired.append(demo.name)
    return retired


async def queue_late_templates(pool) -> list[str]:
    """Queue the template of each published demo this process has not been asked to build -- a
    bundled demo the node marked ready after the tier started (its ``ready`` file, read by
    published_demos) -- as ensure_demo_templates queued the others at startup. Run by the tier's
    scheduler, never on a visitor's request; nothing before the startup has queued its own. The
    demos queued."""
    if not _startup_queued.is_set():
        return []
    retire_templates_without_stores()
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
    # A version pulled since the start: the startup's pass served the one before it.
    start_building_ahead(pool)
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
    prepare = _prepare_seeding if seeding_release() else _prepare
    task = spawn_background(template_builds.run(at_once, prepare), name="demo-templates")
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

    def at_work(self) -> bool:
        """Whether a template is waiting or being built."""
        with self._lock:
            return bool(self._waiting or self._building)

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
    the demo's own), the Provisa release that built it -- a replica's definition is computed
    by the release, so another release builds the template again -- and the DDL fingerprint of the
    tables that code creates in an org's schema: a cloud patch keeps the release, and a template
    built by code of another shape keeps that code's tables (no migrations)."""
    from provisa.core.demo_bundles import running_provisa
    from provisa.core.schema_org import ddl_fingerprint

    return f"{demo.directory.name}@{running_provisa()}@{ddl_fingerprint()}"


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
    if held != {"replicas": dict(record.replicas), "views": dict(record.views)}:
        return False
    # REQ-2052: and the profile runs its bundle carries, which every sandbox of it is cloned with.
    # A template served as built without them gave every sandbox an empty run history; one that
    # does not hold them is prepared again, which loads them (profile_template).
    unheld = await unheld_profile_runs(demo)
    if unheld:
        import logging

        logging.getLogger(__name__).warning(
            "demo template %s: not served as built, its profile runs are not held: %s",
            demo.name,
            "; ".join(unheld),
        )
        return False
    # REQ-2201: and the scans for terms its bundle carries, as its profile runs.
    unheld = await unheld_term_scans(demo)
    if unheld:
        import logging

        logging.getLogger(__name__).warning(
            "demo template %s: not served as built, its scans are not held: %s",
            demo.name,
            "; ".join(unheld),
        )
        return False
    return True


async def unheld_term_scans(demo: PublishedDemo) -> list[str]:
    """The scans for terms ``demo``'s bundle carries that its template does not hold, passages
    and drafts all (provisa.term_finding.carry.unheld_scans); empty when it holds every one or
    scans nothing."""
    from provisa.api.admin.orgs_router import _pool
    from provisa.core.environments import org_schema
    from provisa.term_finding.carry import scans_path, unheld_scans

    if not demo_scans(demo.directory):
        return []
    path = scans_path(demo.directory)
    if not path.is_file():
        return [f"the bundle carries no scans ({path.name}): rebuild it"]
    async with _pool().acquire() as conn:
        return await unheld_scans(conn, path, org_schema(demo.template_org))


async def unheld_profile_runs(demo: PublishedDemo) -> list[str]:
    """The profile runs ``demo``'s bundle carries that its template does not hold, rows and all
    (provisa.profiler.carry.unheld_runs); empty when it holds every one or profiles nothing."""
    from provisa.api.admin.orgs_router import _pool
    from provisa.core.environments import org_schema
    from provisa.profiler.carry import results_path, unheld_runs

    if not demo_profilers(demo.directory):
        return []
    path = results_path(demo.directory)
    if not path.is_file():
        return [f"the bundle carries no profile runs ({path.name}): rebuild it"]
    async with _pool().acquire() as conn:
        return await unheld_runs(conn, path, org_schema(demo.template_org))


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
    plane beside its ready record (readiness_version: ``<version>@<release>@<ddl>``); None when it was
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
    from the version served is then queued to be built (template_builds); the open is told the same
    until it is ready (demo_sandbox._template_ready). The open waits on both
    (demo_sandbox._ready_demo)."""
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
        # One that failed is built again: an open is what asks for it again.
        if await _templated(_admin_pool(), name) == ready and template_builds.state_of(
            name
        ) not in ("unknown", "failed"):
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


#: How long another region's node may go unanswered while a seeding region waits on its views
#: (its nodes start together, and one may still be starting).
_REGION_ANSWER_GRACE_S = 60.0


async def _build_view(org_id: str, mv_id: str) -> None:
    """Build the template's materialized view ``mv_id`` now (a seeding run's view nothing else
    builds); a failed build is recorded on the view (mv.refresh.refresh_mv), where the wait reads
    it."""
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.mv.refresh import refresh_mv

    token = set_current_org(org_id)
    try:
        assert state.federation_engine is not None
        mv = state.mv_registry.get(mv_id)
        if mv is None:
            raise LookupError(f"view {mv_id!r} is kept in this region and not registered")
        await refresh_mv(
            state.federation_engine,
            mv,
            state.mv_registry,
            store=state.model_db,
            ledger=state.tenant_db,
        )
    finally:
        reset_current_org(token)


def silent_regions(
    down: dict[str, str], since: dict[str, float], now: float
) -> tuple[list[tuple[str, str]], dict[str, float]]:
    """Of the regions not answering now (``down``: region -> address), those that have not answered
    for _REGION_ANSWER_GRACE_S, given when each was first found not answering (``since``); and the
    record to carry to the next poll. A region that answers again starts over."""
    since = {region: since.get(region, now) for region in down}
    return [
        (region, down[region])
        for region, first in sorted(since.items())
        if now - first >= _REGION_ANSWER_GRACE_S
    ], since


#: Why a seeding run's template was not ready, by demo (_prepare_seeding writes it out).
_seeding_failures: dict[str, str] = {}


async def _prepare_seeding(demo: PublishedDemo, build: bool) -> bool:
    """:func:`_prepare` in a bundle build's seeding run: a template that is not ready, or cannot be
    prepared, says why beside the record the build waits for (demo_replicas.seed_failure_path),
    which ends the build at once by name instead of at the end of its budget."""
    from provisa.api.demo_replicas import seed_failure_path

    try:
        ready = await _prepare(demo, build)
    except Exception as exc:  # allow-ble: re-raised; its reason is written out first
        _write_seeding_failure(seed_failure_path(demo.directory), f"{type(exc).__name__}: {exc}")
        raise
    if not ready:
        _write_seeding_failure(
            seed_failure_path(demo.directory),
            _seeding_failures.get(demo.name, "the template was not ready"),
        )
    return ready


def _write_seeding_failure(path: Path, reason: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(reason)


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
    # REQ-2053: the environments its setup.yaml declares, which every sandbox of it carries. Each
    # created here is a copy of the prod just built, and is built cloned (demo_synthetic.prepare).
    created = await create_environments(org_id, demo_environments(demo.directory), None)
    from provisa.api.demo_region_seed import regional
    from provisa.api.demo_replicas import export_seed, live_landing_waits, live_topic_ends

    # REQ-2049 (amended 2026-11-03): where each topic its listeners land ends now, with its
    # runtime (and so its listeners) built: the template is ready once it has landed to there.
    # A regions tier's sandboxes are copied from its region seeds (demo_region_seed).
    live = not regional()
    ends = await live_topic_ends(template_seed(org_id)) if live else {}

    exporting = seeding_release()
    said = None
    while True:
        landed, failed, declared, waiting = await _replica_progress(rt)
        if failed:
            log.error("demo template %s: replicas failed to land: %s", demo.name, failed)
            _seeding_failures[demo.name] = f"replicas failed to land: {failed}"
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
    # And every table its listeners land from a Kafka topic, which neither its replica record nor
    # its views count: a sandbox is opened holding what its template landed (carry_landed_data).
    said = None
    while live and (waiting := await live_landing_waits(org_id, template_seed(org_id), ends)):
        if waiting != said:
            said = waiting
            log.warning("demo template %s: waiting on its live landing: %s", demo.name, waiting)
        await asyncio.sleep(_LANDED_POLL_S)
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
            created=created,
        )
    finally:
        reset_current_org(token)
    from provisa.api.demo_region_seed import (
        regional,
        seeding_node_addresses,
        unanswered_regions,
        views_built_here,
        views_kept_here,
        views_kept_here_ids,
        views_waiting_here,
    )

    if exporting and regional():
        # REQ-2068: a region's seed carries its views too; no node of it builds them after. Each
        # is built from the replicas that have now landed: one built before then (as the node
        # started, from replicas not yet there) would be carried as it read them, short.
        landed_at = datetime.now(UTC)
        for mv_id in views_kept_here_ids(template_seed(org_id)):
            # As an operator's refresh asks (schema_mutation.refresh_mv): a view nothing else
            # builds -- its inputs all read in place, its refresh periodic -- is built here too.
            await _build_view(org_id, mv_id)
        said = None
        unanswered_since: dict[str, float] = {}
        while (built := await views_built_here(org_id, landed_at)) < (
            kept := views_kept_here(template_seed(org_id))
        ):
            waiting = await views_waiting_here(org_id, landed_at)
            # A view may read a table another region keeps in place: that region's seeding node
            # answers where the build says it listens, or the view never builds.
            from provisa.api.app import state

            down = dict(
                await unanswered_regions(
                    seeding_node_addresses([r.id for r in state.config.platform.regions])
                )
            )
            dead, unanswered_since = silent_regions(down, unanswered_since, time.monotonic())
            if dead:
                reason = "views wait on " + ", ".join(
                    f"region {region}, whose node at {address} does not answer"
                    for region, address in dead
                )
                log.error("demo template %s: %s", demo.name, reason)
                _seeding_failures[demo.name] = reason
                return False
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
        # REQ-2201: and its scans for terms, as its profile runs.
        failed_scans = await scan_template(demo, org_id)
        if failed_scans:
            log.error("demo template %s: scans not loaded: %s", demo.name, failed_scans)
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
