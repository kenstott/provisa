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

Provisa presents one identity -- a certificate and key, made once per organisation -- to every
remote host of the organisation, kept encrypted in the organisation's own store.
"""

# Requirements: REQ-2098
from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime
from typing import Any

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.remote import Identity, RemoteWorker, host_certificate, new_identity, pair
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
        raise RemoteRefused(
            "not_code", f"{source_id!r} is not a Node or Python source", source=source_id
        )
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
        pin = await asyncio.to_thread(pair, host, int(port), code, me)
    except ValueError as refused:  # PairingRefused: the code, or the host's answer, named
        raise RemoteRefused("pairing", str(refused)) from None
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
            # REQ-2100: what an engine attaching the host verifies it by.
            "certificate": await asyncio.to_thread(host_certificate, host, int(port), pin, me),
            "exports": exports,
            "manifest": manifest_json(prepared.manifest),
        },
    }
    await _save_mapping(source_id, built)
    return built


def _check(
    source_id: str, code_hash: str, host: str, port: int, pin: str, me: Identity, exports: list[str]
) -> None:
    """Dial the host and check its hello: refused by name when it is not the source's code."""
    from provisa.codesource.worker import WorkerFailed

    worker = RemoteWorker(
        source_id, code_hash, address=host, port=port, pin=pin, identity=me, exports=exports
    )
    try:
        worker.start()
    except WorkerFailed as failed:
        raise RemoteRefused("remote_refused", str(failed)) from None
    worker.stop()


async def to_dev(state: Any, source_id: str) -> dict:
    """Switch the source back to dev mode; its remote host is forgotten."""
    source = await _source(state, source_id)
    mapping = {k: v for k, v in source.mapping.items() if k not in ("mode", "remote")}
    await _save_mapping(source_id, mapping)
    return mapping


async def status(state: Any, source_id: str) -> dict:
    """The source's mode and, built, whether its host answers: what it reports (its code hash and
    exports) or why it does not, as this process dials it."""
    from provisa.codesource.remote import RemoteWorker as _Remote
    from provisa.codesource.worker import WorkerFailed, pool
    from provisa.core.request_context import require_current_org

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
    # REQ-2100: the host's certificate, again, for the engines that attach it.
    mapping["remote"] = {
        **remote,
        "certificate": await asyncio.to_thread(
            host_certificate, remote["host"], int(remote["port"]), remote["pin"], me
        ),
    }
    await _save_mapping(source_id, mapping)
    return mapping


def _org() -> str:
    from provisa.core.request_context import require_current_org

    return require_current_org()


def worker_for(
    org_id: str, source_id: str, code_hash: str, remote: dict, me: Identity
) -> RemoteWorker:
    """A built source's worker: its remote host, dialled, its hello checked against the code hash
    and exports the source is pinned to."""
    return RemoteWorker(
        source_id,
        code_hash,
        address=remote["host"],
        port=int(remote["port"]),
        pin=remote["pin"],
        identity=me,
        exports=remote.get("exports"),
    )


__all__ = [
    "BUILT",
    "DEV",
    "RemoteRefused",
    "connect",
    "identity",
    "mode_of",
    "remote_of",
    "repin",
    "status",
    "to_dev",
    "worker_for",
]
