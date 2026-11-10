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
import time

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_sources_e2e import _ROLE, _admin, _archive, _config
from tests.port_lease import lease_ports

pytestmark = [pytest.mark.integration]

# "analyst" itself is a standard role, which a config file cannot define (REQ-2155): this role is
# defined here, and is not an ancestor of org_admin.
_ANALYST = "ledger_analyst"

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
    tag: str


@dataclass
class Lean:
    id: int
    region: str


@dataclass
class Secret:
    secret: str


@dataclass
class Tag:
    tag: str


@dataclass
class Clip:
    id: int
    region: str


@dataclass
class Outcome:
    status: str
    body: str


def orders() -> list[Order]:
    """@table"""
    return [Order(1, "emea", "s1", "t1"), Order(2, "amer", "s2", "t2")]


def vault() -> list[Order]:
    """@table"""
    return [Order(7, "apac", "v7", "tv")]


def clips() -> list[Clip]:
    """@table"""
    return [Clip(1, "emea"), Clip(2, "amer")]


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


def _read_sql(table: TableRef, sql: str) -> Outcome:
    """Like _read but runs a caller-supplied SQL statement over the same minted connection."""
    target = table.connection
    sock = socket.create_connection((target.host, target.port), timeout=30)
    start = b"user\0" + target.user.encode() + b"\0database\0" + target.database.encode() + b"\0\0"
    sock.sendall(struct.pack("!ii", 8 + len(start), 196608) + start)
    frames = _messages(sock)
    kind, body = next(frames)
    if kind != b"R":
        return Outcome("refused", _error_of(body))
    cred = target.password.encode() + b"\0"
    sock.sendall(b"p" + struct.pack("!i", 4 + len(cred)) + cred)
    for kind, body in frames:
        if kind == b"E":
            return Outcome("refused", _error_of(body))
        if kind == b"Z":
            break
    query = sql.encode() + b"\0"
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


def read_whole(orders: TableRef[Order]) -> list[Outcome]:
    """@query"""
    return [_read(orders)]


def read_tag(orders: TableRef[Tag]) -> list[Outcome]:
    """@query"""
    return [_read(orders)]


def read_clips(clips: TableRef[Clip]) -> list[Outcome]:
    """@query"""
    return [_read(clips)]


def read_star(orders: TableRef[Lean]) -> list[Outcome]:
    """Sends SELECT * so the governed pipeline determines the visible columns.

    @query
    """
    return [_read_sql(orders, "SELECT * FROM fx.orders")]


def read_qualified_secret(orders: TableRef[Secret]) -> list[Outcome]:
    """Uses a table alias so the qualified-column visibility check fires.

    @query
    """
    return [_read_sql(orders, "SELECT o.secret FROM fx.orders AS o")]
''',
}


def _column(name: str, data_type: str, *roles: str) -> str:
    visible = ", ".join(f'"{r}"' for r in roles)
    return f'{{name: "{name}", visibleTo: [{visible}], dataType: "{data_type}"}}'


def _column_masked(
    name: str, data_type: str, mask_value: str, unmasked_role: str, *other_roles: str
) -> str:
    """Column visible to all named roles; ``unmasked_role`` sees the real value, the rest see
    ``mask_value`` (a constant mask applied by the governed pipeline, REQ-2226)."""
    all_visible = ", ".join(f'"{r}"' for r in [unmasked_role, *other_roles])
    return (
        f'{{name: "{name}", visibleTo: [{all_visible}], dataType: "{data_type}", '
        f'masks: [{{to: ["{unmasked_role}"]}}, {{mask: {{type: "constant", value: "{mask_value}"}}}}]}}'
    )


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
            # A child holds only what it is given of what its parents hold (REQ-2155). /data/sql
            # needs query_development, a command needs command_execution, and of the standard
            # roles developer is the one holding those with full_results.
            "parents": ["developer"],
            "capabilities": ["usage", "query_development", "command_execution", "full_results"],
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
    ids: dict[str, int] = {}
    # A view is built as the organisation's administrator (mv.governed_build), so "vault" grants
    # its secret to a role that is not that administrator's ancestor.
    # For "orders": _ROLE is the unmasked role for "tag" (analyst sees ***).
    # For "vault": _ANALYST is the unmasked role for "tag" (analyst sees the real value; org_admin
    #   sees *** — the MV build runs as org_admin, so vault_digest captures the masked value).
    for table, secret_reader, tag_unmasked in (
        ("orders", _ROLE, _ROLE),
        ("vault", _ANALYST, _ANALYST),
    ):
        # "tag" is visible to both roles but masked for one of them (REQ-2226: masking runs
        # through the same governed pipeline as column visibility).
        tag_masked = _ANALYST if tag_unmasked == _ROLE else _ROLE
        columns = [
            _column("id", "bigint", _ROLE, _ANALYST),
            _column("region", "text", _ROLE, _ANALYST),
            _column("secret", "text", secret_reader),
            _column_masked("tag", "text", "***", tag_unmasked, tag_masked),
        ]
        registered = _admin(
            server,
            'registerTable(input: {sourceId: "ledger", domainId: "fx", schemaName: "default", '
            f'tableName: "{table}", cacheTtl: 300, columns: [{", ".join(columns)}]}})',
        )
        ids[table] = registered_id(registered["message"])
        _admin(server, release_field(ids[table]))
    # "clips" carries a row-level security rule: analyst may only read the row whose id = 1,
    # so the governed pipeline's RLS enforcement is covered on the minted-token listener (REQ-2226).
    clips_cols = [
        _column("id", "bigint", _ROLE, _ANALYST),
        _column("region", "text", _ROLE, _ANALYST),
    ]
    registered_clips = _admin(
        server,
        'registerTable(input: {sourceId: "ledger", domainId: "fx", schemaName: "default", '
        f'tableName: "clips", cacheTtl: 300, columns: [{", ".join(clips_cols)}]}})',
    )
    _admin(server, release_field(registered_id(registered_clips["message"])))
    _admin(
        server,
        f'upsertRlsRule(input: {{tableId: "clips", roleId: "{_ANALYST}", filterExpr: "id = 1", '
        'description: "Analyst reads only clip 1."})',
    )
    for function in (
        "read_lean",
        "read_secret",
        "read_whole",
        "read_tag",
        "read_clips",
        "read_star",
        "read_qualified_secret",
    ):
        _command(server, function)
    return ids


@pytest.fixture(scope="module")
def vault_digest(server, ledger):
    """A materialized view over a @query whose input is the governed table "vault", built."""
    sql = "SELECT r.status, r.body FROM read_whole('fx.vault') AS r"
    columns = [_column("status", "text", _ROLE, _ANALYST), _column("body", "text", _ROLE, _ANALYST)]
    registered = _admin(
        server,
        'registerTable(input: {sourceId: "__derived__", domainId: "fx", schemaName: "views", '
        f'tableName: "vault_digest", alias: "vault_digest", viewSql: {json.dumps(sql)}, '
        f"materialize: true, mvRefreshInterval: 3600, columns: [{', '.join(columns)}]}})",
    )
    _admin(server, release_field(registered_id(registered["message"])))
    refreshed = _admin(server, 'refreshMv(mvId: "view-vault_digest")')
    assert refreshed["success"], refreshed


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


def test_the_input_is_readable_by_a_role_granted_its_every_column(server, ledger):
    row = _call(server, _ANALYST, "SELECT * FROM read_whole('fx.vault')")
    assert row["status"] == "ok", row
    # vault grants every column (id, region, secret, tag) to _ANALYST (REQ-2226).
    assert json.loads(row["body"]) == [{"id": "7", "region": "apac", "secret": "v7", "tag": "tv"}]


def test_a_view_built_as_a_role_that_cannot_see_a_column_reads_as_that_role(server, vault_digest):
    """The reader here holds the column, so a refusal can only be what the build stored: the code
    ran as the administrator, which does not."""
    deadline = time.monotonic() + 300
    while True:
        response = httpx.post(
            f"{server.base_url}/data/sql",
            json={"sql": "SELECT status, body FROM vault_digest"},
            headers={"X-Provisa-Role": _ANALYST},
            timeout=server.request_timeout + 120,
        )
        rows = response.json()["data"]["sql"] if response.status_code == 200 else []
        if rows and rows[0]["status"] == "refused":
            break
        assert time.monotonic() < deadline, f"{response.text}\n{server.dump_stderr_debug()[-6000:]}"
        time.sleep(2)
    assert len(rows) == 1, rows
    assert "secret" in rows[0]["body"]


def test_the_views_lineage_runs_from_the_input_through_the_command(server, vault_digest):
    response = httpx.get(
        f"{server.base_url}/admin/lineage/federation",
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    graph = response.json()
    outputs = {
        n["column"]: n["id"]
        for n in graph["nodes"]
        if (n["relation"] or "").endswith("vault_digest")
    }
    assert set(outputs) == {"status", "body"}, graph["nodes"]
    for output in outputs.values():
        reached, commands, frontier = set(), set(), [output]
        while frontier:
            at = frontier.pop()
            for edge in graph["edges"]:
                if edge["target"] == at and edge["source"] not in reached:
                    reached.add(edge["source"])
                    frontier.append(edge["source"])
                    commands |= {o["name"] for o in edge["ops"] if o["kind"] == "command"}
        assert "fx.vault.id" in reached, reached
        assert "read_whole" in commands, commands


def test_a_select_star_excludes_a_column_the_role_cannot_see(server, ledger):
    """SELECT * through the minted listener expands only to the visible columns (REQ-2226):
    ``secret`` is hidden from ``ledger_analyst``, so it must not appear in the result."""
    row = _call(server, _ANALYST, "SELECT * FROM read_star('fx.orders')")
    assert row["status"] == "ok", row
    rows = json.loads(row["body"])
    assert len(rows) == 2, rows
    assert all("secret" not in r for r in rows), rows
    assert [r["id"] for r in rows] == ["1", "2"]


def test_a_qualified_column_the_role_cannot_see_is_refused(server, ledger):
    """A qualified reference ``o.secret`` is refused just like an unqualified one (REQ-2226):
    the governed pipeline's column-visibility check covers the alias/qualified path."""
    row = _call(server, _ANALYST, "SELECT * FROM read_qualified_secret('fx.orders')")
    assert row["status"] == "refused", row
    assert "secret" in row["body"]


def test_a_masked_column_returns_the_mask_value(server, ledger):
    """A column the calling role can read but that carries a mask delivers the mask value,
    not the raw value, through the minted-token listener (REQ-2226)."""
    # _ANALYST: visible but masked — must receive the constant "***".
    row = _call(server, _ANALYST, "SELECT * FROM read_tag('fx.orders')")
    assert row["status"] == "ok", row
    assert json.loads(row["body"]) == [{"tag": "***"}, {"tag": "***"}]
    # _ROLE: exempt from the mask — must receive the real values.
    row = _call(server, _ROLE, "SELECT * FROM read_tag('fx.orders')")
    assert row["status"] == "ok", row
    assert json.loads(row["body"]) == [{"tag": "t1"}, {"tag": "t2"}]


def test_a_row_rule_narrows_the_rows_the_calling_role_reads(server, ledger):
    """A domain row rule scoped to a role restricts what that role reads through the minted
    listener, while a role without that rule reads all rows (REQ-2226)."""
    # _ANALYST has an RLS rule on "clips": only id = 1 passes.
    row = _call(server, _ANALYST, "SELECT * FROM read_clips('fx.clips')")
    assert row["status"] == "ok", row
    assert json.loads(row["body"]) == [{"id": "1", "region": "emea"}]
    # _ROLE has no RLS rule — reads both rows.
    row = _call(server, _ROLE, "SELECT * FROM read_clips('fx.clips')")
    assert row["status"] == "ok", row
    assert json.loads(row["body"]) == [{"id": "1", "region": "emea"}, {"id": "2", "region": "amer"}]


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


@pytest.mark.parametrize(("reader", "table"), [(_ANALYST, "orders"), (_ROLE, "vault")])
def test_a_direct_read_of_a_column_the_role_is_not_granted_is_refused(
    server, ledger, reader, table
):
    """REQ-2154, REQ-2226: a column registered with a grant list named is granted exactly that
    list, so a role it does not name is refused the column on a direct read too, not only through
    the minted-token listener. ``orders`` grants ``secret`` to org_admin only and ``vault`` to the
    analyst only: neither registration adds the registrant or Viewer to a named list."""
    response = httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": f"SELECT secret FROM fx.{table}"},
        headers={"X-Provisa-Role": reader},
        timeout=server.request_timeout + 120,
    )
    assert response.status_code != 200, response.text
    assert "Column 'secret' is not visible to this role" in response.text


@pytest.mark.parametrize(("reader", "table"), [(_ROLE, "orders"), (_ANALYST, "vault")])
def test_a_direct_read_of_a_granted_column_is_served(server, ledger, reader, table):
    response = httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": f"SELECT secret FROM fx.{table}"},
        headers={"X-Provisa-Role": reader},
        timeout=server.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    assert response.json()["data"]["sql"], response.text


def test_the_served_grant_of_a_named_column_is_the_named_list(server, ledger):
    """The admin's access view of ``secret`` names only the role its registration named (and
    that role's ancestors' grants never widen it to a sibling): no Viewer grant on it."""
    for table, reader, other in (("orders", _ROLE, _ANALYST), ("vault", _ANALYST, _ROLE)):
        response = httpx.get(
            f"{server.base_url}/admin/tables/{ledger[table]}/access",
            headers={"X-Provisa-Role": _ROLE},
            timeout=server.request_timeout + 120,
        )
        assert response.status_code == 200, response.text
        (secret,) = [c for c in response.json()["columns"] if c["name"] == "secret"]
        readers = {a["role"] for a in secret["access"] if a["read"]}
        assert reader in readers, secret
        assert other not in readers, secret
        assert "viewer" not in readers, secret
