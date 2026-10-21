# Copyright (c) 2026 Kenneth Stott
# Canary: 65880035-42ce-48a4-b0fd-c2aef4280c83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's remote-host identity, as files an engine presents when it attaches a built host
(REQ-2100).

A built host's front end accepts only the certificate of the Provisa it is paired with
(provisa/codehost/front.py), and an engine that attaches the host over pgwire is Provisa's: it
presents the organisation's identity (provisa/codesource/remote_sources.py ``identity``). Every
engine's PostgreSQL client reads a client certificate and key from files, so the identity is
written where the engine's process reads it, readable by that process alone
(``trino_catalog_files.write_engine_secret_file``):

- Trino: ``codehost-client/`` of the engine tree the app shares with the engine
  (``trino_etc_dir``), read by the engine at ``/etc/trino/codehost-client`` -- a tmpfs in the
  containerized stack, as the Kafka client files are; the key also as PKCS-8 DER, which the JDBC
  driver reads.
- a Postgres engine: the directory the app shares with the engine's server, ``$PROVISA_PG_ENGINE_FILES``
  on the app's side and ``$PROVISA_PG_ENGINE_FILES_AT`` on the server's (the same directory where
  both run on one machine); none shared, and no built host is attached on it.
- an engine in the app's own process (DuckDB): a private directory of this process.

Beside it, each built source's host certificate, pinned when it was paired: the root an engine
verifies the host by (sslmode=verify-ca), as Provisa's own dials check the pin. A certificate, the
organisation's or a host's, is public, so it is written mode 0644 (``write_engine_public_file``) and the engine reads it whatever
user wrote it. Written again when the source is re-pinned.

Each file is named by the organisation, so two organisations' identities never share one.
"""

# Requirements: REQ-2100
from __future__ import annotations

import os
import re
import tempfile
import threading
from collections.abc import Mapping
from pathlib import Path

from provisa.codesource.remote import Identity
from provisa.federation.codehost_connectors import (
    PG_FILES_AT_ENV,
    PG_FILES_ENV,
    Prepared,
    postgres_shares_files,
)

#: The engine's own view of the Trino directory the identity is written to.
TRINO_AT = "/etc/trino/codehost-client"
TRINO_DIR = "codehost-client"
#: The user the postgres image runs as, the reader of a Postgres engine's identity files.
POSTGRES_UID = 999


_private: dict[str, Path] = {}
_lock = threading.Lock()


def _der(identity: Identity) -> bytes:
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(identity.key.encode(), None)
    return key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _root_name(org_id: str, source_id: str) -> str:
    return f"{org_id}.{re.sub(r'[^A-Za-z0-9_.-]', '_', source_id)}.root.crt"


def _contents(
    org_id: str, identity: Identity, roots: Mapping[str, str]
) -> tuple[dict[str, tuple[str, bytes]], dict[str, tuple[str, bytes]]]:
    """Each file of the identity, by its field, and each host's certificate, by source: its name
    and what it holds."""
    return (
        {
            "certificate": (f"{org_id}.crt", identity.certificate.encode()),
            "key": (f"{org_id}.key", identity.key.encode()),
            "key_der": (f"{org_id}.pk8", _der(identity)),
        },
        {sid: (_root_name(org_id, sid), pem.encode()) for sid, pem in roots.items()},
    )


def _at(at: str, contents: tuple[dict, dict]) -> Prepared:
    own, roots = contents
    return Prepared(
        certificate=f"{at}/{own['certificate'][0]}",
        key=f"{at}/{own['key'][0]}",
        key_der=f"{at}/{own['key_der'][0]}",
        roots={sid: f"{at}/{name}" for sid, (name, _data) in roots.items()},
    )


def _all(contents: tuple[dict, dict]) -> dict[str, bytes]:
    own, roots = contents
    return {name: data for name, data in [*own.values(), *roots.values()]}


def _write(
    directory: Path, at: str, org_id: str, identity: Identity, roots: Mapping[str, str], uid: int
) -> Prepared:
    from provisa.core.trino_catalog_files import write_engine_public_file, write_engine_secret_file

    contents = _contents(org_id, identity, roots)
    own, pinned = contents
    # A certificate, the organisation's or a host's, is public: written for any reader, so the
    # engine reads it whoever wrote it. The keys are the engine's alone.
    for field, (name, data) in own.items():
        if field == "certificate":
            write_engine_public_file(directory / name, data)
        else:
            write_engine_secret_file(directory / name, data, uid, directory_mode=0o755)
    for name, data in pinned.values():
        write_engine_public_file(directory / name, data)
    return _at(at, contents)


async def for_trino(
    state: object, org_id: str, identity: Identity, roots: Mapping[str, str]
) -> Prepared:
    """The identity written for Trino; the paths Trino reads it at. A coordinator the control plane
    provisions shares no directory with it: there the identity goes into the shard's Secret
    (``k8s_provisioner.put_codehost_files``), mounted at the same place."""
    from provisa.core.trino_catalog_files import TRINO_UID, trino_etc_dir
    from provisa.federation import k8s_provisioner

    if k8s_provisioner.provisioning_available():
        from provisa.federation.engine_wake import active_shard

        shard = active_shard(state)
        if shard is not None:
            contents = _contents(org_id, identity, roots)
            await k8s_provisioner.put_codehost_files(shard, _all(contents))
            return _at(TRINO_AT, contents)
    return _write(trino_etc_dir() / TRINO_DIR, TRINO_AT, org_id, identity, roots, TRINO_UID)


def for_postgres(org_id: str, identity: Identity, roots: Mapping[str, str]) -> Prepared:
    """The identity written for a Postgres engine's server; the paths the server reads it at."""
    if not postgres_shares_files():
        raise RuntimeError(
            f"no directory is shared with the Postgres engine's server ({PG_FILES_ENV}, "
            f"{PG_FILES_AT_ENV}): a built host is not attached on it"
        )
    return _write(
        Path(os.environ[PG_FILES_ENV]),
        os.environ[PG_FILES_AT_ENV],
        org_id,
        identity,
        roots,
        POSTGRES_UID,
    )


def for_this_process(org_id: str, identity: Identity, roots: Mapping[str, str]) -> Prepared:
    """The identity written for an engine in this process, in a directory of its own."""
    with _lock:
        directory = _private.get(org_id)
        if directory is None:
            directory = _private[org_id] = Path(tempfile.mkdtemp(prefix="provisa-codehost-"))
    return _write(directory, str(directory), org_id, identity, roots, os.getuid())
