# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7d5e91-6f24-4b08-9c13-e85b2f4d7a60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dev-mode code source's image builds (REQ-2098): started from its form, run in the background,
and recorded in ``code_source_builds`` so any process answers for them.

A build is pushed to one of the organisation's container registries (provisa/core/
container_registries.py) or kept as a ``docker load`` archive, which is downloaded from the node
that built it. Either way it ends with the image's reference and the compose service to run it
(:func:`provisa.codesource.image_build.compose_snippet`).
"""

# Requirements: REQ-2098
from __future__ import annotations

import asyncio
import socket
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from provisa.codesource import image_build, oci
from provisa.codesource.bundle import BundleRefused

BUILDING = "building"
DONE = "done"
FAILED = "failed"


class BuildRefused(BundleRefused):
    """A build that cannot start, named with why."""


def _row(row: Any) -> dict:
    data = dict(row._mapping)
    for key in ("started_at", "finished_at"):
        if data.get(key) is not None:
            data[key] = data[key].isoformat()
    ref = data.get("image_ref")
    data["compose"] = (
        image_build.compose_snippet(ref, image_build.repository_of(data["source_id"]))
        if data["status"] == DONE and ref
        else None
    )
    data.pop("archive_path", None)  # a path on the node that built it: not the caller's business
    return data


async def _pool() -> Any:
    from provisa.api.admin.schema_mutation import _get_pool

    return await _get_pool()


async def read(source_id: str, build_id: str) -> dict:
    from sqlalchemy import select

    from provisa.core.schema_org import code_source_builds as t

    async with (await _pool()).acquire() as conn:
        row = (
            await conn.execute_core(
                select(t).where((t.c.id == build_id) & (t.c.source_id == source_id))
            )
        ).fetchone()
    if row is None:
        raise BuildRefused("build_not_found", f"There is no build {build_id!r} of {source_id!r}")
    return _row(row)


async def listed(source_id: str, limit: int = 10) -> list[dict]:
    from sqlalchemy import select

    from provisa.core.schema_org import code_source_builds as t

    async with (await _pool()).acquire() as conn:
        rows = (
            await conn.execute_core(
                select(t)
                .where(t.c.source_id == source_id)
                .order_by(t.c.started_at.desc())
                .limit(limit)
            )
        ).fetchall()
    return [_row(r) for r in rows]


async def archive_path(source_id: str, build_id: str) -> Path:
    """Where a downloaded build's archive is, on this node; refused by name when it is not."""
    from sqlalchemy import select

    from provisa.core.schema_org import code_source_builds as t

    async with (await _pool()).acquire() as conn:
        row = (
            await conn.execute_core(
                select(t.c.archive_path, t.c.node, t.c.status).where(
                    (t.c.id == build_id) & (t.c.source_id == source_id)
                )
            )
        ).fetchone()
    if row is None or row[2] != DONE or not row[0]:
        raise BuildRefused("no_archive", f"build {build_id!r} has no archive to download")
    path = Path(row[0])
    if row[1] != socket.gethostname() or not path.is_file():
        raise BuildRefused(
            "archive_elsewhere",
            f"build {build_id!r}'s archive is kept on {row[1]}, not on this node: download it there "
            "or build it again",
        )
    return path


async def start(
    state: Any,
    source_id: str,
    arch: str,
    registry_id: str | None,
    download: bool,
    actor: str | None,
) -> dict:
    """Start a build of the dev-mode source ``source_id`` for ``arch``: pushed to the registry
    ``registry_id`` (the default when None), or kept for download. Checked before it starts."""
    from sqlalchemy import insert

    from provisa.codesource import remote_sources
    from provisa.codesource.source import config_of, context_for
    from provisa.core import container_registries
    from provisa.core.connection_loop import spawn_background
    from provisa.core.request_context import require_current_org
    from provisa.core.schema_org import code_source_builds as t

    if arch not in image_build.ARCHITECTURES:
        raise BuildRefused(
            "image_arch",
            f"an image is built for {', '.join(image_build.ARCHITECTURES)}, not {arch!r}",
        )
    source = await remote_sources._source(state, source_id)  # noqa: SLF001 -- one lookup
    config = config_of(source.type.value, source.mapping)
    if config.mode != remote_sources.DEV or config.content_hash is None:
        raise BuildRefused(
            "not_dev", "an image is built from a saved dev-mode source: save its code first"
        )
    org_id = require_current_org()
    registry = None
    if not download:
        registry = await container_registries.resolved(state.admin_db, org_id, registry_id)
    ctx = await context_for(state)
    build_id = uuid.uuid4().hex
    async with (await _pool()).acquire() as conn:
        await conn.execute_core(
            insert(t).values(
                id=build_id,
                source_id=source_id,
                code_hash=config.content_hash,
                arch=arch,
                registry_id=None if registry is None else registry.id,
                status=BUILDING,
                started_by=actor,
            )
        )
    spawn_background(
        _run(source, config, ctx, arch, registry, build_id),
        name=f"code-image:{source_id}:{build_id}",
    )
    return await read(source_id, build_id)


async def _finish(build_id: str, **values: Any) -> None:
    from sqlalchemy import update

    from provisa.core.schema_org import code_source_builds as t

    async with (await _pool()).acquire() as conn:
        await conn.execute_core(
            update(t).where(t.c.id == build_id).values(finished_at=datetime.now(UTC), **values)
        )


async def _run(source: Any, config: Any, ctx: Any, arch: str, registry: Any, build_id: str) -> None:
    """The build itself, on a background worker: its row says how it ended."""
    from provisa.codesource.source import _code_reference, _lockfile, prepare
    from provisa.core.demo_bundles import running_provisa

    workdir = Path(tempfile.mkdtemp(prefix="provisa-image-", dir=_work_root(ctx)))
    try:
        prepared = await prepare(source.id, source.type.value, source.mapping, ctx)
        plan = image_build.Plan(
            source_id=source.id,
            language=config.language,
            runtime=config.runtime,
            code_hash=config.content_hash,
            entry=config.entry,
            exports=[e.name for e in prepared.manifest.exports],
            registries=list(ctx.registries),
            cache_root=ctx.cache_root,
            version=running_provisa(),
        )
        code = await _code_reference(config, ctx)
        lock = await _lockfile(config, ctx)
        built = await asyncio.to_thread(image_build.assemble, plan, code, lock, arch, workdir)
        if registry is not None:
            repository = f"{registry.namespace}/{built.repository}"
            verify = _ca_file(registry, workdir)
            pushed = oci.Registry(
                registry.host, registry.username, registry.credential, verify=verify
            )
            digest = await asyncio.to_thread(pushed.push, repository, built.tag, built.image)
            await _finish(
                build_id,
                status=DONE,
                image_ref=f"{registry.host}/{repository}:{built.tag}",
                digest=digest,
            )
        else:
            out = _work_root(ctx) / "archives" / f"{build_id}.tar"
            out.parent.mkdir(parents=True, exist_ok=True)
            ref = f"{built.repository}:{built.tag}"
            await asyncio.to_thread(oci.docker_archive, built.image, ref, out)
            await _finish(
                build_id,
                status=DONE,
                image_ref=ref,
                archive_path=str(out),
                node=socket.gethostname(),
            )
    except Exception as exc:  # allow-ble: any failure of the build is its row's failure, named with its type and message; nothing else is waiting on it
        await _finish(build_id, status=FAILED, error=f"{type(exc).__name__}: {exc}")
    finally:
        image_build.clean(workdir)


def _work_root(ctx: Any) -> Path:
    root = Path(ctx.cache_root) / "oci" / "work"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _ca_file(registry: Any, workdir: Path) -> str | bool:
    """The registry's certificate authority, trusted beside the public ones, as a file."""
    if not registry.ca_bundle:
        return True
    import certifi

    path = workdir / "registry-ca.pem"
    path.write_text(Path(certifi.where()).read_text() + "\n" + registry.ca_bundle)
    return str(path)
