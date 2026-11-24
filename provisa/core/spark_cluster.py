# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e8a47-93b2-4d6f-a0e8-7f2b9d14c3a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The organisation's default Spark cluster (REQ-2225).

An organisation setting entered by its administrator: a Spark Connect address and, when the
cluster takes one, a credential kept as a ``${secret:NAME}`` reference, resolved at use in that
organisation's vault. A Spark source's own cluster, when set, is used for its runs and for
validating it; this default applies only where a source names none. With neither, the cluster is
reported as not configured.

A demo or local install has a one-node Spark started on demand, the local test cluster, as its
default; a production install has none until an administrator sets one.

:func:`probe` is the connection test: one ``AnalyzePlan`` call asking the server's Spark version,
over the same gRPC Spark Connect clients speak, so it needs no Spark client here.
"""

# Requirements: REQ-2225
from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from sqlalchemy import delete, func, select

from provisa.core import secrets_store
from provisa.core.schema_admin import org_spark_clusters as _t

if TYPE_CHECKING:
    from provisa.core.database import Database

#: Where an address with no port is served (Spark Connect's own default).
DEFAULT_PORT = 15002
#: The demo's local test cluster (REQ-2225), where its one-node Spark listens.
LOCAL_TEST_CLUSTER_ENV = "PROVISA_DEMO_SPARK_CONNECT"
LOCAL_TEST_LABEL = "local test cluster"

ORIGIN_SOURCE, ORIGIN_ORG, ORIGIN_LOCAL = "source", "organisation", "local_test"

_REFERENCE = re.compile(r"^\$\{secret:[A-Za-z0-9_.\-/]+\}$")
_PARAM = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=[^;]*$")


class SparkClusterRefused(ValueError):
    """A cluster that cannot be kept, or none to run on."""

    def __init__(self, code: str, message: str, *, status: int = 400, **params: str) -> None:
        super().__init__(message)
        self.code = f"spark_cluster.{code}"
        self.status = status
        self.params = params


@dataclass(frozen=True)
class Cluster:
    """The organisation's default, each field as its administrator entered it."""

    address: str
    credential: str | None


@dataclass(frozen=True)
class Endpoint:
    """A Spark Connect address split into what a channel needs."""

    host: str
    port: int
    params: dict[str, str]

    @property
    def token(self) -> str | None:
        return self.params.get("token")

    @property
    def tls(self) -> bool:
        # Spark Connect's own rule: a token is only sent over TLS, so a token turns it on.
        return self.params.get("use_ssl", "").lower() == "true" or self.token is not None


def parse(address: str) -> Endpoint:
    """``sc://host[:port][/;name=value;...]`` split up; refused by name when it is not one."""
    if not address.startswith("sc://"):
        raise SparkClusterRefused(
            "address",
            f"{address!r} is not a Spark Connect address, such as sc://spark.acme.com:15002",
            address=address,
        )
    head, _, tail = address[len("sc://") :].partition("/")
    try:
        split = urlsplit(f"//{head}")
        host, port, userinfo = split.hostname, split.port, split.username or split.password
    except ValueError:
        host, port, userinfo = None, None, None
    if not host or userinfo:
        raise SparkClusterRefused(
            "address",
            f"{address!r} is not a Spark Connect address, such as sc://spark.acme.com:15002",
            address=address,
        )
    params: dict[str, str] = {}
    for part in (p for p in tail.split(";") if p):
        if not _PARAM.match(part):
            raise SparkClusterRefused(
                "address",
                f"{address!r}: {part!r} is not a name=value parameter",
                address=address,
            )
        name, _, value = part.partition("=")
        params[name] = value
    return Endpoint(host, port or DEFAULT_PORT, params)


def with_token(address: str, token: str) -> str:
    """``address`` carrying ``token`` as its Spark Connect ``token`` parameter."""
    endpoint = parse(address)
    if endpoint.token is not None:
        raise SparkClusterRefused(
            "token_twice",
            "The address carries a token of its own: keep the credential as a secret reference "
            "and leave the token out of the address",
        )
    joiner = "" if address.endswith(";") else (";" if "/;" in address else "/;")
    return f"{address}{joiner}token={token}"


def _row(row) -> Cluster:
    return Cluster(row.address, row.credential)


async def read(admin_db: "Database", org_id: str) -> Cluster | None:
    """``org_id``'s default as entered, or None when its administrator has set none."""
    async with admin_db.acquire() as conn:
        row = (await conn.execute_core(select(_t).where(_t.c.org_id == org_id))).fetchone()
    return None if row is None else _row(row)


async def put(
    admin_db: "Database", org_id: str, *, address: str, credential: str | None, actor: str | None
) -> Cluster:
    """Keep ``org_id``'s default, replacing any it had. Refused by name: an address that is not a
    Spark Connect address, one that carries its own token, and a credential that is not a
    ``${secret:NAME}`` reference (a credential typed as a value is never kept)."""
    address = address.strip()
    credential = (credential or "").strip() or None
    endpoint = parse(address)
    if endpoint.token is not None:
        raise SparkClusterRefused(
            "token_in_address",
            "The address carries a token: keep it as a secret and name it as the credential",
        )
    if credential is not None and not _REFERENCE.match(credential):
        raise SparkClusterRefused(
            "credential_not_reference",
            "The credential is a secret reference, such as ${secret:NAME}; a value is "
            "never kept here",
        )
    values = {
        "org_id": org_id,
        "address": address,
        "credential": credential,
        "updated_by": actor,
    }
    async with admin_db.acquire() as conn:
        await conn.upsert(
            _t,
            values,
            index_elements=["org_id"],
            update_columns=["address", "credential", "updated_by"],
            set_extra={"updated_at": func.now()},
        )
    kept = await read(admin_db, org_id)
    if kept is None:
        raise RuntimeError(f"the Spark cluster of {org_id} was kept but cannot be read back")
    return kept


async def forget(admin_db: "Database", org_id: str) -> bool:
    """Remove ``org_id``'s default: True when there was one. A secret it named is left as is."""
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(delete(_t).where(_t.c.org_id == org_id))
    return result.rowcount > 0


def local_test_cluster() -> str | None:
    """The demo's one-node Spark (REQ-2225): its address in a demo or local install, None in a
    production one, which has no default until an administrator sets one."""
    from provisa.core.demo import is_demo

    if not is_demo():
        return None
    address = os.environ.get(LOCAL_TEST_CLUSTER_ENV)
    if not address:
        raise SparkClusterRefused(
            "local_test_unset",
            f"This demo install has no local test cluster: {LOCAL_TEST_CLUSTER_ENV} is not set",
            status=503,
        )
    return address


@dataclass(frozen=True)
class Resolved:
    """The cluster a run or a validation goes to: its address (a token in it when the cluster
    takes one) and where it came from."""

    address: str
    origin: str


async def _resolved_default(admin_db: "Database", org_id: str) -> Resolved:
    from provisa.core.secrets import resolve_secrets

    kept = await read(admin_db, org_id)
    if kept is None:
        local = local_test_cluster()
        if local is None:
            raise SparkClusterRefused(
                "not_configured",
                "No Spark cluster is configured: the source names none and the organisation "
                "has no default Spark cluster",
                status=409,
            )
        return Resolved(local, ORIGIN_LOCAL)
    if kept.credential is None:
        return Resolved(kept.address, ORIGIN_ORG)
    async with secrets_store.bound(admin_db, org_id):
        try:
            token = resolve_secrets(kept.credential)
        except (KeyError, ValueError) as unresolved:
            reason = unresolved.args[0] if unresolved.args else type(unresolved).__name__
            raise SparkClusterRefused(
                "unresolved",
                f"The credential of the default Spark cluster could not be resolved: {reason}",
                status=409,
            ) from None
    return Resolved(with_token(kept.address, token), ORIGIN_ORG)


async def resolved(admin_db: "Database", org_id: str, own: str | None) -> Resolved:
    """The cluster of a source whose own cluster is ``own``: that one when it is set, else the
    organisation's default, else (demo and local installs) the local test cluster. Refused as
    not configured when there is none."""
    mine = own_cluster(own)
    return Resolved(mine, ORIGIN_SOURCE) if mine else await _resolved_default(admin_db, org_id)


def own_cluster(own: str | None) -> str | None:
    """A source's own cluster when it is set, else None."""
    # REQ-2225: "a source's own cluster, when set": an empty value is a cluster left unset.
    if own is None or not own.strip():
        return None
    parse(own.strip())
    return own.strip()


# -- the connection test ----------------------------------------------------------------------------

_SERVICE_METHOD = "/spark.connect.SparkConnectService/AnalyzePlan"
#: How long the connection test waits for the server's answer.
PROBE_TIMEOUT_S = 20.0


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _field(number: int, payload: bytes) -> bytes:
    """One length-delimited protobuf field."""
    return _varint(number << 3 | 2) + _varint(len(payload)) + payload


def version_request(session_id: str, user_id: str) -> bytes:
    """An ``AnalyzePlanRequest`` asking the server's Spark version (spark/connect/base.proto:
    session_id = 1, user_context = 2 {user_id = 1}, spark_version = 10)."""
    return _field(1, session_id.encode()) + _field(2, _field(1, user_id.encode())) + _field(10, b"")


def _fields(data: bytes):
    """Each (number, wire type, value) of a protobuf message, values of length-delimited fields as
    bytes."""
    i = 0
    while i < len(data):
        key, i = _read_varint(data, i)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, i = _read_varint(data, i)
            yield number, wire, value
        elif wire == 2:
            size, i = _read_varint(data, i)
            yield number, wire, data[i : i + size]
            i += size
        elif wire == 1:
            yield number, wire, data[i : i + 8]
            i += 8
        elif wire == 5:
            yield number, wire, data[i : i + 4]
            i += 4
        else:
            raise ValueError(f"wire type {wire} in an AnalyzePlanResponse")


def _read_varint(data: bytes, i: int) -> tuple[int, int]:
    shift = value = 0
    while True:
        byte = data[i]
        i += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, i
        shift += 7


def version_of(response: bytes) -> str:
    """The Spark version an ``AnalyzePlanResponse`` answers (spark_version = 8 {version = 1})."""
    for number, wire, value in _fields(response):
        if number == 8 and isinstance(value, bytes):
            for inner, _wire, text in _fields(value):
                if inner == 1 and isinstance(text, bytes):
                    return text.decode()
    raise ValueError("the server's answer carries no Spark version")


def probe(address: str) -> str:
    """Call the cluster at ``address`` (a token in it when it takes one) and return its Spark
    version. Raises :class:`SparkClusterRefused` ``unreachable`` with the server's own words."""
    from provisa.grpc_remote.executor import UnaryCallFailed, call_unary_raw

    endpoint = parse(address)
    target = f"{endpoint.host}:{endpoint.port}"
    metadata: tuple[tuple[str, str], ...] = (
        () if endpoint.token is None else (("authorization", f"Bearer {endpoint.token}"),)
    )
    try:
        answer = call_unary_raw(
            target,
            tls=endpoint.tls,
            method=_SERVICE_METHOD,
            request=version_request(str(uuid.uuid4()), endpoint.params.get("user_id", "provisa")),
            timeout=PROBE_TIMEOUT_S,
            metadata=metadata,
        )
    except UnaryCallFailed as failed:
        raise SparkClusterRefused(
            "unreachable",
            f"The Spark cluster at {target} did not answer: {failed}",
            status=502,
            target=target,
        ) from None
    try:
        return version_of(answer)
    except (ValueError, IndexError) as unread:
        raise SparkClusterRefused(
            "not_spark_connect",
            f"The server at {target} answered, but not as Spark Connect: {unread}",
            status=502,
            target=target,
        ) from None
