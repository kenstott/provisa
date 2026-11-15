# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The customer360 demo's entity-resolution code source: its entry module (REQ-2034).

Two ``@query`` functions do the matching inside Provisa, so the rerun can be triggered from
Provisa: ``match_organizations`` answers the rows of er.entity_org_bridge and ``match_people``
those of er.entity_person_bridge, for a new match_run_id. Each is given every registry table as a
``TableRef``, which it reads through Provisa as the calling role, and runs the same job
(er_job.run) the shell does over the rows the demo declares. Neither writes anything: two
registered statement commands, one per bridge, each MERGE the bridge from one of them, replacing
its rows in one statement (config.yaml, ``rerun_org_bridge`` and ``rerun_person_bridge``).

The tables a function is given are the registry's own (entity_resolution.ENTITY_SOURCES), one
parameter per table, named as the demo registers it, and each parameter's row type is the columns
the matching reads of that table (entity_resolution.columns_read). The demo's tests hold the three
to each other.

A table is read over PostgreSQL's wire protocol with the minted token its reference carries; the
code needs no driver, so the package has no dependencies to install.
"""

# Requirements: REQ-2034

from __future__ import annotations

import socket
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone

from er_job import Bridges, run
from provisa_services import TableRef  # pyright: ignore[reportMissingImports] -- the host provides it

# --------------------------------------------------------------------------------------------- #
# The tables the matching reads: one row type per registry table, its fields the columns read.  #
# --------------------------------------------------------------------------------------------- #


@dataclass
class Account:
    CreatedDate: datetime | None
    Id: str | None
    Name: str | None
    Website: str | None


@dataclass
class Contact:
    AccountId: str | None
    Email: str | None
    FirstName: str | None
    Id: str | None
    LastName: str | None


@dataclass
class Company:
    name: str | None
    sys_id: str | None
    website: str | None


@dataclass
class ServiceDeskUser:
    company: str | None
    email: str | None
    first_name: str | None
    last_name: str | None
    sys_id: str | None


@dataclass
class Bug:
    customer: str | None
    reporter_email: str | None
    reporter_name: str | None


@dataclass
class StripeCustomer:
    billing_contact_name: str | None
    email: str | None
    id: str | None
    name: str | None


#: Each parameter of the two functions, and the registry table, (source, table), it is given.
TABLES = {
    "accounts": ("salesforce", "Account"),
    "contacts": ("salesforce", "Contact"),
    "companies": ("servicenow", "core_company"),
    "service_desk_users": ("servicenow", "sys_user"),
    "bugs": ("jira", "bug"),
    "stripe_customers": ("stripe", "get_customers"),
}

# --------------------------------------------------------------------------------------------- #
# The rows each function answers: a bridge's columns.                                           #
# --------------------------------------------------------------------------------------------- #


@dataclass
class OrgBridgeRow:
    source_schema: str
    source_table: str
    source_column: str
    source_key: str
    name_raw: str
    name_norm: str
    match_text: str
    label: str
    canonical_id: str
    golden_source: str | None
    match_method: str | None
    match_confidence: str
    match_score: float | None
    unresolved_reason: str | None
    candidate_id: str | None
    candidate_score: float | None
    match_run_id: str


@dataclass
class PersonBridgeRow:
    source_schema: str
    source_table: str
    source_column: str
    source_key: str
    name_raw: str
    name_norm: str
    match_text: str
    label: str
    canonical_id: str
    golden_source: str | None
    org_canonical_id: str | None
    match_method: str | None
    match_confidence: str
    match_score: float | None
    unresolved_reason: str | None
    candidate_id: str | None
    candidate_score: float | None
    match_run_id: str


# --------------------------------------------------------------------------------------------- #
# Reading a table through Provisa.                                                              #
# --------------------------------------------------------------------------------------------- #

_PROTOCOL = 196608  # 3.0
_AUTH_OK, _AUTH_CLEARTEXT = 0, 3


class TableReadFailed(RuntimeError):
    """A table this code could not read, said naming the table and Provisa's answer."""

    def __init__(self, table: str, reason: str) -> None:
        super().__init__(f"reading {table}: {reason}")


def _messages(sock: socket.socket) -> Iterator[tuple[bytes, bytes]]:
    buffer = b""

    def take(n: int) -> bytes:
        nonlocal buffer
        while len(buffer) < n:
            chunk = sock.recv(65536)
            if not chunk:
                raise ConnectionError("the connection ended")
            buffer += chunk
        out, buffer = buffer[:n], buffer[n:]
        return out

    while True:
        kind = take(1)
        (length,) = struct.unpack("!i", take(4))
        yield kind, take(length - 4)


def _error_text(body: bytes) -> str:
    fields = {f[:1]: f[1:].decode("utf-8", "replace") for f in body.split(b"\0") if f}
    return fields.get(b"M", "an error with no message")


def _message(kind: bytes, body: bytes) -> bytes:
    return kind + struct.pack("!i", 4 + len(body)) + body


def _columns(body: bytes) -> list[str]:
    (count,) = struct.unpack("!h", body[:2])
    names, at = [], 2
    for _ in range(count):
        end = body.index(b"\0", at)
        names.append(body[at:end].decode("utf-8"))
        at = end + 1 + 18  # table oid, column number, type oid, size, modifier, format
    return names


def _row(body: bytes, names: list[str]) -> dict[str, str | None]:
    (count,) = struct.unpack("!h", body[:2])
    row: dict[str, str | None] = {}
    at = 2
    for name in names[:count]:
        (size,) = struct.unpack("!i", body[at : at + 4])
        at += 4
        if size < 0:
            row[name] = None
            continue
        row[name] = body[at : at + size].decode("utf-8")
        at += size
    return row


def read_table(table: TableRef) -> list[dict[str, str | None]]:
    """Every row ``table``'s statement answers, each value as text or None, read as the calling
    role with the token minted for this call."""
    target = table.connection
    if target.sslmode != "disable":
        raise TableReadFailed(table.ref, f"the listener requires TLS ({target.sslmode})")
    with socket.create_connection((target.host, target.port), timeout=300) as sock:
        params = b"user\0" + target.user.encode() + b"\0database\0" + target.database.encode()
        start = params + b"\0\0"
        sock.sendall(struct.pack("!ii", 8 + len(start), _PROTOCOL) + start)
        frames = _messages(sock)
        for kind, body in frames:
            if kind == b"E":
                raise TableReadFailed(table.ref, _error_text(body))
            if kind == b"R":
                (code,) = struct.unpack("!i", body[:4])
                if code == _AUTH_CLEARTEXT:
                    sock.sendall(_message(b"p", target.password.encode() + b"\0"))
                elif code != _AUTH_OK:
                    raise TableReadFailed(table.ref, f"authentication method {code} is not offered")
            elif kind == b"Z":
                break
        sock.sendall(_message(b"Q", table.sql.encode() + b"\0"))
        names: list[str] = []
        rows: list[dict[str, str | None]] = []
        failed: str | None = None
        for kind, body in frames:
            if kind == b"E":
                failed = _error_text(body)
            elif kind == b"T":
                names = _columns(body)
            elif kind == b"D":
                rows.append(_row(body, names))
            elif kind == b"Z":
                break
        sock.sendall(_message(b"X", b""))
    if failed is not None:
        raise TableReadFailed(table.ref, failed)
    return rows


# --------------------------------------------------------------------------------------------- #
# The matching.                                                                                 #
# --------------------------------------------------------------------------------------------- #


def _bridges(given: dict[str, TableRef]) -> Bridges:
    """One run of the job over the tables given, for a run id of the time it ran."""
    rows = {TABLES[name]: read_table(table) for name, table in given.items()}
    columns = {TABLES[name]: list(table.columns) for name, table in given.items()}
    run_id = "er-" + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return run(rows, columns, run_id)


def match_organizations(
    accounts: TableRef[Account],
    contacts: TableRef[Contact],
    companies: TableRef[Company],
    service_desk_users: TableRef[ServiceDeskUser],
    bugs: TableRef[Bug],
    stripe_customers: TableRef[StripeCustomer],
) -> list[OrgBridgeRow]:
    """Each mention of an organization in the four systems, matched to its Salesforce account:
    the rows of er.entity_org_bridge for a new run.

    @query
    """
    given = {
        "accounts": accounts,
        "contacts": contacts,
        "companies": companies,
        "service_desk_users": service_desk_users,
        "bugs": bugs,
        "stripe_customers": stripe_customers,
    }
    return [OrgBridgeRow(**row) for row in _bridges(given).org]


def match_people(
    accounts: TableRef[Account],
    contacts: TableRef[Contact],
    companies: TableRef[Company],
    service_desk_users: TableRef[ServiceDeskUser],
    bugs: TableRef[Bug],
    stripe_customers: TableRef[StripeCustomer],
) -> list[PersonBridgeRow]:
    """Each mention of a person in the four systems, matched within its organization to its
    Salesforce contact: the rows of er.entity_person_bridge for a new run.

    @query
    """
    given = {
        "accounts": accounts,
        "contacts": contacts,
        "companies": companies,
        "service_desk_users": service_desk_users,
        "bugs": bugs,
        "stripe_customers": stripe_customers,
    }
    return [PersonBridgeRow(**row) for row in _bridges(given).person]
