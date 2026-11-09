# Copyright (c) 2026 Kenneth Stott
# Canary: a0c3e17b-6d52-4f88-b9a4-2c1d70f6e835
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2226's scenario on a real server: a real, sandboxed worker reads the table it was given over
the minted-token listener, as the calling role, through the one governed pipeline. A column the
role cannot see is not available to the code; a role that can see it reads it."""

# Requirements: REQ-2226

from __future__ import annotations

import json
import socket
import struct

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_sources_e2e import _ROLE, _admin, _archive, _config
from tests.port_lease import lease_ports

pytestmark = [pytest.mark.integration]

_ANALYST = "analyst"

_CODE = {
    "ledger/ledger.py": r'''
import json
import socket
import struct
from dataclasses import dataclass

from provisa_services import TableRef


@dataclass
class Order:
    id: int
    region: str
    secret: str


@dataclass
class Lean:
    id: int
    region: str


@dataclass
class Secret:
    secret: str


@dataclass
class Outcome:
    status: str
    body: str


def orders() -> list[Order]:
    """@table"""
    return [Order(1, "emea", "s1"), Order(2, "amer", "s2")]


def _messages(sock):
    def read(n):
        out = b""
        while len(out) < n:
            out += sock.recv(n - len(out))
        return out

    while True:
        kind = read(1)
        length = struct.unpack("!i", read(4))[0]
        yield kind, read(length - 4)


def _error_of(body: bytes) -> str:
    return dict((f[:1], f[1:].decode()) for f in body.split(b"\0") if f).get(b"M", "")


def _read(table: TableRef) -> Outcome:
    """A PostgreSQL client just far enough to run the one statement the reference names."""
    target = table.connection
    sock = socket.create_connection((target.host, target.port), timeout=30)
    start = b"user\0" + target.user.encode() + b"\0database\0" + target.database.encode() + b"\0\0"
    sock.sendall(struct.pack("!ii", 8 + len(start), 196608) + start)
    frames = _messages(sock)
    kind, body = next(frames)
    if kind != b"R":
        return Outcome("refused", _error_of(body))
    secret = target.password.encode() + b"\0"
    sock.sendall(b"p" + struct.pack("!i", 4 + len(secret)) + secret)
    for kind, body in frames:
        if kind == b"E":
            return Outcome("refused", _error_of(body))
        if kind == b"Z":
            break
    query = table.sql.encode() + b"\0"
    sock.sendall(b"Q" + struct.pack("!i", 4 + len(query)) + query)
    names: list[str] = []
    rows: list[dict] = []
    for kind, body in frames:
        if kind == b"E":
            return Outcome("refused", _error_of(body))
        if kind == b"T":
            count, at = struct.unpack("!h", body[:2])[0], 2
            for _ in range(count):
                end = body.index(b"\0", at)
                names.append(body[at:end].decode())
                at = end + 19
        elif kind == b"D":
            count, at = struct.unpack("!h", body[:2])[0], 2
            row = {}
            for name in names[:count]:
                size = struct.unpack("!i", body[at:at + 4])[0]
                at += 4
                row[name] = body[at:at + size].decode()
                at += size
            rows.append(row)
        elif kind == b"Z":
            return Outcome("ok", json.dumps(rows))
    return Outcome("refused", "the connection ended")


def read_lean(orders: TableRef[Lean]) -> list[Outcome]:
    """@query"""
    return [_read(orders)]


def read_secret(orders: TableRef[Secret]) -> list[Outcome]:
    """@query"""
    return [_read(orders)]
''',
}


def _column(name: str, data_type: str, *roles: str) -> str:
    visible = ", ".join(f'"{r}"' for r in roles)
    return f'{{name: "{name}", visibleTo: [{visible}], dataType: "{data_type}"}}'


@pytest.fixture(scope="module")
def minted_port():
    return lease_ports(1)[0]


@pytest.fixture(scope="module")
def server(tmp_path_factory, minted_port):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("code_table_read")
    config = _config()
    config["roles"] = [
        {
            "id": _ANALYST,
            "description": "Test role.",
            "capabilities": ["query_development", "full_results"],
            "domain_access": ["*"],
        }
    ]
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        "code_table_read",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={
            "PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps")),
            "PROVISA_MINTED_PGWIRE_PORT": str(minted_port),
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _command(srv, function: str) -> None:
    response = httpx.post(
        f"{srv.base_url}/admin/actions/functions",
        json={
            "name": function,
            "sourceId": "ledger",
            "functionName": function,
            "domainId": "fx",
            "visibleTo": [_ROLE, _ANALYST],
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text


@pytest.fixture(scope="module")
def ledger(server):
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "ledger.zip"},
        content=_archive(_CODE),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        server,
        f'createSource(input: {{id: "ledger", type: "python", host: "", port: 0, database: "", '
        f"cacheTtl: 300, mappingJson: {json.dumps(json.dumps(mapping))}}})",
    )
    columns = [
        _column("id", "bigint", _ROLE, _ANALYST),
        _column("region", "text", _ROLE, _ANALYST),
        _column("secret", "text", _ROLE),
    ]
    registered = _admin(
        server,
        'registerTable(input: {sourceId: "ledger", domainId: "fx", schemaName: "default", '
        f'tableName: "orders", cacheTtl: 300, columns: [{", ".join(columns)}]}})',
    )
    _admin(server, release_field(registered_id(registered["message"])))
    for function in ("read_lean", "read_secret"):
        _command(server, function)


def _call(srv, role: str, sql: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    (row,) = response.json()["data"]["sql"]
    return row


def test_a_worker_reads_its_table_as_the_calling_role(server, ledger):
    row = _call(server, _ANALYST, "SELECT * FROM read_lean('fx.orders')")
    assert row["status"] == "ok", row
    rows = json.loads(row["body"])
    assert rows == [{"id": "1", "region": "emea"}, {"id": "2", "region": "amer"}]


def test_a_column_the_role_cannot_see_is_not_available_to_the_code(server, ledger):
    row = _call(server, _ANALYST, "SELECT * FROM read_secret('fx.orders')")
    assert row["status"] == "refused", row
    assert "secret" in row["body"]


def test_a_role_that_can_see_the_column_reads_it(server, ledger):
    row = _call(server, _ROLE, "SELECT * FROM read_secret('fx.orders')")
    assert row["status"] == "ok", row
    assert json.loads(row["body"]) == [{"secret": "s1"}, {"secret": "s2"}]


def test_the_read_composes_inline_with_a_where_clause(server, ledger):
    row = _call(
        server, _ANALYST, "SELECT r.status FROM read_lean('fx.orders') AS r WHERE r.status = 'ok'"
    )
    assert row == {"status": "ok"}


def test_the_listener_refuses_a_wrong_token(server, ledger, minted_port):
    start = b"user\0call\0\0"
    with socket.create_connection(("127.0.0.1", minted_port), timeout=10) as sock:
        sock.sendall(struct.pack("!ii", 8 + len(start), 196608) + start)
        assert sock.recv(1) == b"R"  # a password is asked for
        (length,) = struct.unpack("!i", sock.recv(4))
        sock.recv(length - 4)
        secret = b"provisa_call_wrong\0"
        sock.sendall(b"p" + struct.pack("!i", 4 + len(secret)) + secret)
        assert sock.recv(1) == b"E"
        assert b"28P01" in sock.recv(4096)
