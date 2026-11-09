# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2c8b47-1d93-4a06-b7f4-0a9d3c61e258
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's two modes (REQ-2098): dev, its code on the Provisa server, and built, its image
on a remote host the operator runs.

CONNECT. The operator deploys a source's image their own way and enters, on the source, the host
and the published port and the one-time pairing code the container printed. Provisa pairs with it
(provisa/codesource/remote.py), dials it, checks its hello -- the protocol, the content hash the
source is pinned to, and the exports the source has -- and switches the source to built mode,
keeping with it the host, the port, the host's certificate pin, and the manifest the source was
read with in dev mode (a built source is not read on the server). Its tables and grants stay: its
exports and code hash are the same.

Back to DEV: the source's mode alone changes; its pin is forgotten, and the host is paired anew
to come back.

RE-PIN. A host that comes back with other code is refused naming the pinned and reported hashes
(the worker's hello check) until a steward re-pins: the source then takes the host's code hash,
once its exports are the ones the source has.

A CONNECTOR KNOWN ONLY BY ITS IMAGE (REQ-2221, :func:`join_connector`). No dev-mode source comes
first: the paired host hands over its manifest in the handshake, and Provisa holds it to the
model's ``connectors`` block. The image the block's digest names, read from its registry, carries
the code hash the block names and the hash of the manifest the host sent; the host's hello reports
that code. The server builds nothing and reads no build setting.

Provisa presents one identity -- a certificate and key, made once per organisation -- to every
remote host of the organisation, kept encrypted in the organisation's own store.
"""

# Requirements: REQ-2098, REQ-2221
from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.remote import Identity, RemoteWorker, new_identity, pair
from provisa.codesource.worker import duckdb_bounds
from provisa.core.org_forget import on_forget

DEV = "dev"
BUILT = "built"
MODES = (DEV, BUILT)

_identities: dict[str, Identity] = {}


@on_forget(size=lambda: len(_identities))
def _forget_identity(org_id: str) -> None:
    _identities.pop(org_id, None)


_identities_lock = threading.Lock()


class RemoteRefused(BundleRefused):
    """A connect, mode switch or re-pin that cannot be made, named with why."""


# -- the organisation's identity ----------------------------------------------------------------


async def identity(org_id: str) -> Identity:
    """The certificate and key ``org_id`` presents to its remote hosts: made on first use, kept
    encrypted in the organisation's prod store (``org_secrets``), the same for every process. Two
    processes making it at once keep the one written first."""
    with _identities_lock:
        held = _identities.get(org_id)
    if held is not None:
        return held
    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import insert

    from provisa.api.admin.orgs_router import _org_model_db
    from provisa.core.org_secrets import CODEHOST_IDENTITY_KEY
    from provisa.core.schema_org import org_secrets
    from provisa.encryption.runtime import encryption_service

    model_db = await _org_model_db(org_id)  # prod: one identity across the org's environments
    service = encryption_service()
    made = new_identity()
    sealed = service.encrypt(
        json.dumps({"certificate": made.certificate, "key": made.key}).encode()
    )
    async with model_db.acquire() as conn:
        await conn.execute_core(
            insert(org_secrets)
            .values(
                key=CODEHOST_IDENTITY_KEY,
                value_enc=sealed,
                updated_at=datetime.now(UTC),
                updated_by="provisa",
            )
            .on_conflict_do_nothing(index_elements=[org_secrets.c.key])
        )
        row = (
            await conn.execute_core(
                select(org_secrets.c.value_enc).where(org_secrets.c.key == CODEHOST_IDENTITY_KEY)
            )
        ).fetchone()
    kept = json.loads(service.decrypt(bytes(row[0])).decode())
    found = Identity(certificate=kept["certificate"], key=kept["key"])
    with _identities_lock:
        _identities[org_id] = found
    return found


# -- the source's mapping -----------------------------------------------------------------------


def mode_of(mapping: dict) -> str:
    mode = mapping.get("mode") or DEV
    if mode not in MODES:
        raise RemoteRefused("mode_invalid", f"a code source's mode is dev or built, not {mode!r}")
    return mode


def remote_of(mapping: dict) -> dict:
    """A built source's remote host: host, port, pin and the manifest it was read with. Refused
    by name when any is missing."""
    remote = mapping.get("remote") or {}
    missing = [k for k in ("host", "port", "pin", "manifest") if not remote.get(k)]
    if missing:
        raise RemoteRefused(
            "remote_incomplete",
            f"a built source names its remote host; this one has no {', '.join(missing)}: "
            "connect it again",
        )
    return remote


async def _save_mapping(source_id: str, mapping: dict) -> None:
    """Keep ``mapping`` as the source's, and rebuild what serves it."""
    from sqlalchemy import update

    from provisa.api.admin.schema_helpers import _rebuild_schemas
    from provisa.api.admin.schema_mutation import _get_pool
    from provisa.codesource.source import forget
    from provisa.core.schema_org import sources

    pool = await _get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            update(sources).where(sources.c.id == source_id).values(mapping=mapping)
        )
    if (result.rowcount or 0) == 0:
        raise RemoteRefused("not_found", f"There is no source {source_id!r}", source=source_id)
    forget(source_id)  # its workers and what was prepared for it go
    await _rebuild_schemas()


async def _source(state: Any, source_id: str) -> Any:
    from provisa.codesource.source import CODE_SOURCE_TYPES
    from provisa.federation.registry_view import registered_sources

    found = [s for s in await registered_sources(state) if s.id == source_id]
    if not found:
        raise RemoteRefused("not_found", f"There is no source {source_id!r}", source=source_id)
    source = found[0]
    if source.type.value not in CODE_SOURCE_TYPES:
        raise RemoteRefused("not_code", f"{source_id!r} is not a code source", source=source_id)
    return source


# -- connect, back to dev, re-pin ---------------------------------------------------------------


async def connect(state: Any, source_id: str, host: str, port: int, code: str) -> dict:
    """Pair with the source's remote host, check it serves the source's code, and switch the source
    to built mode. Its new mapping."""
    from provisa.codesource.manifest import manifest_json
    from provisa.codesource.source import context_for, prepare
    from provisa.core.request_context import require_current_org

    source = await _source(state, source_id)
    mapping = dict(source.mapping)
    if mode_of(mapping) != DEV or not mapping.get("content_hash"):
        raise RemoteRefused(
            "not_dev",
            "a remote host is connected to a saved dev-mode source: its code is read there first",
        )
    host = host.strip()
    if not host or not 0 < int(port) < 65536:
        raise RemoteRefused("address", "a remote host is a host and the port it is published on")
    prepared = await prepare(source.id, source.type.value, mapping, await context_for(state))
    me = await identity(require_current_org())
    try:
        paired = await asyncio.to_thread(pair, host, int(port), code, me, duckdb_bounds())
    except ValueError as refused:  # PairingRefused: the code, or the host's answer, named
        raise RemoteRefused("pairing", str(refused)) from None
    pin = paired.pin
    exports = sorted(e.name for e in prepared.manifest.exports)
    await asyncio.to_thread(
        _check, source_id, prepared.content_hash, host, int(port), pin, me, exports
    )
    built = {
        **mapping,
        "mode": BUILT,
        "remote": {
            "host": host,
            "port": int(port),
            "pin": pin,
            # REQ-2100: what an engine attaching the host verifies it by, read when it paired.
            "certificate": paired.certificate,
            "exports": exports,
            "manifest": manifest_json(prepared.manifest),
        },
    }
    await _save_mapping(source_id, built)
    return built


def _check(
    source_id: str,
    code_hash: str,
    host: str,
    port: int,
    pin: str,
    me: Identity,
    exports: list[str],
    setup: Any = None,
) -> None:
    """Dial the host and check its hello: refused by name when it is not the source's code. With
    ``setup`` (REQ-2221), give it its setup values and wait for its @on-start:
    :class:`~provisa.codesource.worker.OnStartFailed` when that fails."""
    from provisa.codesource.worker import OnStartFailed, WorkerFailed

    worker = RemoteWorker(
        source_id,
        code_hash,
        address=host,
        port=port,
        pin=pin,
        identity=me,
        exports=exports,
        setup=setup,
    )
    try:
        worker.start()
        worker.ready()
    except OnStartFailed:
        worker.stop()
        raise
    except WorkerFailed as failed:
        raise RemoteRefused("remote_refused", str(failed)) from None
    worker.stop()


async def to_dev(state: Any, source_id: str) -> dict:
    """Switch the source back to dev mode; its remote host is forgotten."""
    source = await _source(state, source_id)
    if source.mapping.get("connector"):
        raise RemoteRefused(
            "connector_built_only",
            "a connector's code runs only on the host its operator runs: it has no dev mode",
            source=source_id,
        )
    mapping = {k: v for k, v in source.mapping.items() if k not in ("mode", "remote")}
    await _save_mapping(source_id, mapping)
    return mapping


async def status(state: Any, source_id: str) -> dict:
    """The source's mode and, built, whether its host answers: what it reports (its code hash and
    exports) or why it does not, as this process dials it."""
    from provisa.api.org_runtime import runtime_key
    from provisa.codesource import lifecycle
    from provisa.codesource.remote import RemoteWorker as _Remote
    from provisa.codesource.worker import WorkerFailed, pool
    from provisa.core.request_context import active_env, require_current_org

    source = await _source(state, source_id)
    mapping = source.mapping
    if mode_of(mapping) == DEV:
        return {"mode": DEV}
    remote = remote_of(mapping)
    org_id = require_current_org()
    me = await identity(org_id)
    where = f"{remote['host']}:{remote['port']}"

    def _probe() -> dict:
        # The hello is read with no code hash to check, so a host with other code reports it.
        probe = _Remote(
            source_id,
            "",
            address=remote["host"],
            port=remote["port"],
            pin=remote["pin"],
            identity=me,
        )
        try:
            probe.start()
        except WorkerFailed as failed:
            said = str(failed)
            reported = _reported_hash(said)
            if reported is not None:
                return {"answering": True, "code_hash": reported}
            return {"answering": False, "error": said}
        probe.stop()
        return {
            "answering": True,
            "code_hash": mapping.get("content_hash"),
            "exports": probe.exports,
        }

    probed = await asyncio.to_thread(_probe)
    return {
        "mode": BUILT,
        "host": where,
        "pinned_hash": mapping.get("content_hash"),
        "pool": pool.remote_status(org_id, source_id),
        # REQ-2221: what this process last found polling its @on-start and @health; None when the
        # code has neither, or no poll has run yet.
        "health": lifecycle.health(runtime_key(org_id, active_env()), source_id),
        **probed,
    }


def _reported_hash(message: str) -> str | None:
    """The code hash a host reported, from its refusal by hash (worker.refusal)."""
    import re

    found = re.search(r"runs code ([0-9a-f]{64})", message)
    return None if found is None else found.group(1)


async def repin(state: Any, source_id: str, code_hash: str) -> dict:
    """Pin the built source to the code hash its host now reports, once the steward has reviewed
    it: the host must report that hash and the exports the source has."""
    source = await _source(state, source_id)
    mapping = dict(source.mapping)
    if mode_of(mapping) != BUILT:
        raise RemoteRefused("not_built", "only a built source is re-pinned to its host's code")
    remote = remote_of(mapping)
    connector = mapping.get("connector")
    if connector:
        await _hold_to_block(connector, code_hash, source_id)
    me = await identity(_org())
    await asyncio.to_thread(
        _check,
        source_id,
        code_hash,
        remote["host"],
        int(remote["port"]),
        remote["pin"],
        me,
        remote["exports"],
    )
    mapping["content_hash"] = code_hash
    await _save_mapping(source_id, mapping)
    return mapping


def _org() -> str:
    from provisa.core.request_context import require_current_org

    return require_current_org()


# -- a connector known only by its image (REQ-2221) ------------------------------------------------


async def _connector(name: str) -> dict:
    from provisa.api.admin.schema_mutation import _get_pool
    from provisa.core.repositories import connector as connector_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await connector_repo.get(conn, name)
    if row is None:
        raise RemoteRefused(
            "connector_not_found", f"This model offers no connector {name!r}", connector=name
        )
    return row


async def _hold_to_block(name: str, code_hash: str, source_id: str) -> None:
    """A connector's source runs the code its block names, and no other: a new version comes with
    a new block, promoted with the model."""
    row = await _connector(name)
    if row["code_hash"] != code_hash:
        raise RemoteRefused(
            "connector_other_code",
            f"connector {name} names code {row['code_hash']}, not {code_hash}: "
            "update its block in the model",
            source=source_id,
        )


async def _registry_for(admin_db: Any, org_id: str, image: str, workdir: Path) -> tuple[Any, str]:
    """The registry holding ``image`` and the repository in it, signed in with the organisation's
    entry for it (REQ-2098) where it has one: anonymous otherwise, and the registry says whether
    that will do. An entry whose address is a reference is not matched, since matching it would
    resolve every such entry to read this one."""
    from provisa.codesource.builds import _ca_file
    from provisa.codesource.oci import Registry
    from provisa.core import container_registries

    host, _, repository = image.partition("/")
    for entry in await container_registries.entries(admin_db, org_id):
        kept_host, _, namespace = entry.address.partition("/")
        if kept_host == host and repository.startswith(f"{namespace}/"):
            found = await container_registries.resolved(admin_db, org_id, entry.id)
            verify = _ca_file(found, workdir)
            return (
                Registry(found.host, found.username, found.credential, verify=verify),
                repository,
            )
    return Registry(host), repository


def _labels_at_digest(registry: Any, repository: str, digest: str, workdir: Path) -> dict[str, str]:
    """The labels of the image ``digest`` names: every byte read is checked against the digest that
    names it, so what comes back is what that digest says."""
    from provisa.codesource.oci import ImageRefused

    _, raw = registry.manifest(repository, digest)
    if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise ImageRefused(
            f"{registry.host}/{repository}@{digest}: its manifest is not that digest"
        )
    config = json.loads(raw).get("config") or {}
    if not config.get("digest"):
        raise ImageRefused(f"{registry.host}/{repository}@{digest} names no image configuration")
    out = workdir / "config.json"
    registry.blob(repository, config["digest"], out)
    return (json.loads(out.read_text()).get("config") or {}).get("Labels") or {}


async def _image_labels(state: Any, org_id: str, name: str, row: dict) -> dict[str, str]:
    from provisa.codesource.image_build import LABEL
    from provisa.codesource.oci import ImageRefused
    from provisa.core.container_registries import ContainerRegistryRefused

    with tempfile.TemporaryDirectory(prefix="provisa-connector-") as scratch:
        workdir = Path(scratch)
        try:
            registry, repository = await _registry_for(
                state.admin_db, org_id, row["image"], workdir
            )
            labels = await asyncio.to_thread(
                _labels_at_digest, registry, repository, row["digest"], workdir
            )
        except (ImageRefused, ContainerRegistryRefused, httpx.HTTPError, ValueError) as failed:
            raise RemoteRefused(
                "connector_image",
                f"connector {name}: the image {row['image']}@{row['digest']} could not be read: "
                f"{failed}",
                connector=name,
            ) from None
    return {k.removeprefix(LABEL): v for k, v in labels.items() if k.startswith(LABEL)}


def _held(code_hash: str, labels: dict[str, str], name: str) -> None:
    """The image the block's digest names is the block's code."""
    found = labels.get("code_hash")
    if found != code_hash:
        raise RemoteRefused(
            "connector_digest_code",
            f"connector {name}: its digest names an image of code {found}, not the "
            f"{code_hash} its block names",
            connector=name,
        )


def _manifest_of(paired: Any, labels: dict[str, str], name: str) -> Any:
    """The manifest the host handed over, held to the image's own; the manifest, read."""
    from provisa.codesource.manifest import manifest_from_json, manifest_hash

    raw = paired.manifest
    if raw is None:
        raise RemoteRefused(
            "connector_no_manifest",
            f"connector {name}: its host sent no manifest as it paired: run an image built by "
            "this Provisa version",
            connector=name,
        )
    expected = labels.get("manifest_hash")
    found = manifest_hash(raw)
    if found != expected:
        raise RemoteRefused(
            "connector_manifest",
            f"connector {name}: its host sent manifest {found}, not the {expected} its image "
            "carries",
            connector=name,
        )
    try:
        return manifest_from_json(raw)
    except (KeyError, TypeError, ValueError) as unread:
        # The image's label names a manifest this Provisa cannot read: one built by another.
        raise RemoteRefused(
            "connector_manifest",
            f"connector {name}: its host's manifest cannot be read ({type(unread).__name__}): "
            "run an image built by this Provisa version",
            connector=name,
        ) from None


def _requires_of(row: dict) -> set[tuple[str, bool]]:
    return {(r["name"], bool(r.get("secret"))) for r in row["requires"]}


def _given_requires(row: dict, mapping: dict, name: str) -> None:
    """Every value the connector requires has been given: a secret as a reference (never a value),
    any other as a setting. Refused naming what is missing, before anything is dialled."""
    from provisa.codesource.services import declared, declared_settings

    secrets = declared(mapping.get("secrets"))
    settings = declared_settings(mapping.get("settings"))
    missing = sorted(
        n for n, secret in _requires_of(row) if n not in (secrets if secret else settings)
    )
    if missing:
        raise RemoteRefused(
            "connector_requires_missing",
            f"connector {name} requires {', '.join(missing)}",
            connector=name,
            missing=", ".join(missing),
        )


async def join_connector(state: Any, source_id: str, source_type: str, mapping: dict) -> None:
    """A connector known only by its image becomes a source: ``mapping["connector_join"]`` names
    the connector and the host running it. Pair with the host, take the manifest it hands over,
    hold it and the host to the block (its digest and code hash), and fill in ``mapping`` as a
    built source's, ``connector_join`` and its pairing code gone. The code is not fetched or read
    here, and no build setting of the block is read."""
    from provisa.codesource.languages import LANGUAGES
    from provisa.codesource.lifecycle import setup_values
    from provisa.codesource.manifest import ON_START
    from provisa.codesource.worker import OnStartFailed
    from provisa.core.request_context import require_current_org

    join = mapping.pop("connector_join")
    name, host, port, code = (join.get(k) for k in ("connector", "host", "port", "pairing_code"))
    host = (host or "").strip()
    if not name or not host or not code or not isinstance(port, int) or not 0 < port < 65536:
        raise RemoteRefused(
            "address", "a connector is joined by its name, host, port and pairing code"
        )
    row = await _connector(name)
    if row["language"] != source_type:
        raise RemoteRefused(
            "connector_type",
            f"connector {name} is a {row['language']} source, not a {source_type} one",
            connector=name,
        )
    _given_requires(row, mapping, name)
    org_id = require_current_org()
    labels = await _image_labels(state, org_id, name, row)
    _held(row["code_hash"], labels, name)
    runtime = labels.get("runtime")
    if runtime not in LANGUAGES[row["language"]].runtimes:
        raise RemoteRefused(
            "connector_image",
            f"connector {name}: its image names runtime {runtime!r}, not one of "
            f"{', '.join(LANGUAGES[row['language']].runtimes)}",
            connector=name,
        )
    me = await identity(org_id)
    try:
        paired = await asyncio.to_thread(pair, host, port, code, me, duckdb_bounds())
    except ValueError as refused:  # PairingRefused: the code, or the host's answer, named
        raise RemoteRefused("pairing", str(refused)) from None
    manifest = _manifest_of(paired, labels, name)
    declares = {(r.name, r.secret) for r in manifest.requires}
    if declares != _requires_of(row):
        raise RemoteRefused(
            "connector_requires",
            f"connector {name}: its host's code requires {sorted(n for n, _ in declares)}, not "
            f"the {sorted(n for n, _ in _requires_of(row))} its block names",
            connector=name,
        )
    exports = sorted(e.name for e in manifest.exports)
    setup = None
    if manifest.lifecycle(ON_START) is not None:
        setup = setup_values(org_id, source_id, manifest, mapping, state.admin_db)
    try:
        await asyncio.to_thread(
            _check, source_id, row["code_hash"], host, port, paired.pin, me, exports, setup
        )
    except OnStartFailed as failed:
        raise RemoteRefused(
            "connector_on_start",
            f"connector {name}: {failed}",
            connector=name,
        ) from None
    mapping.update(
        mode=BUILT,
        runtime=runtime,
        code={"kind": "git", "repository": row["git"], "commit": row["commit"]},
        content_hash=row["code_hash"],
        connector=name,
        remote={
            "host": host,
            "port": port,
            "pin": paired.pin,
            "certificate": paired.certificate,
            "exports": exports,
            "manifest": paired.manifest,
        },
    )


def worker_for(
    org_id: str,
    source_id: str,
    code_hash: str,
    remote: dict,
    me: Identity,
    *,
    setup: Any = None,
) -> RemoteWorker:
    """A built source's worker: its remote host, dialled, its hello checked against the code hash
    and exports the source is pinned to. ``setup`` (REQ-2221) gives a host that has just started
    its setup values."""
    return RemoteWorker(
        source_id,
        code_hash,
        address=remote["host"],
        port=int(remote["port"]),
        pin=remote["pin"],
        identity=me,
        exports=remote.get("exports"),
        setup=setup,
    )


__all__ = [
    "BUILT",
    "DEV",
    "RemoteRefused",
    "connect",
    "identity",
    "join_connector",
    "mode_of",
    "remote_of",
    "repin",
    "status",
    "to_dev",
    "worker_for",
]
