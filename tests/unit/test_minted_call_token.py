# Copyright (c) 2026 Kenneth Stott
# Canary: 6f4b8e21-97c3-4a0d-bd12-3ae5f7c06194
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2226, A LISTENER FOR MINTED TOKENS: the token a code source's function reads its table
with. The listener runs here on a real socket; that the rows come through the governed pipeline
as the calling role, from a real sandboxed worker, is tests/integration/test_code_table_read_e2e."""

# Requirements: REQ-2226

from __future__ import annotations

import asyncio
import socket
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codesource import linux_sandbox, macos_sandbox, sandbox, table_reads
from provisa.core import settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import minted_call_tokens
from provisa.otel_compat import MintedSession
from provisa.pgwire import minted, server as pgwire
from provisa.pgwire.minted import CallTokenRefused, MintedCall, MintedTokenStore
from provisa.pgwire.minted_server import start_minted_pgwire_server
from provisa.pgwire.server import ProvisaSession, stop_pgwire_server

pytestmark = pytest.mark.unit

ORDERS = "sales.orders"


def _call(call_id: str = "call-1", **changes) -> MintedCall:
    fields = {
        "call_id": call_id,
        "role_id": "analyst",
        "members": ("analyst",),
        "qualifiers": (),
        "user_id": "alice",
        "org_id": "org1",
        "tables": (ORDERS,),
        "session_vars": {"provisa.region": "emea"},
        "entitlements": None,
    }
    return MintedCall(**{**fields, **changes})


@pytest.fixture
def db(tmp_path) -> Database:
    engine = create_engine_from_url(f"sqlite:///{tmp_path / 'cp.db'}", pool_size=5, max_overflow=0)
    minted_call_tokens.create(engine.sync_engine if hasattr(engine, "sync_engine") else engine)
    return Database(engine, name="cp")


def _run(coro):
    return asyncio.run(coro)


# -- the token ---------------------------------------------------------------------------------


def test_a_token_stands_for_its_call_and_role(db):
    store = MintedTokenStore(db)

    async def go():
        token = await store.mint(_call())
        return token, await store.resolve(token, "call-1")

    token, call = _run(go())
    assert token.startswith(minted.TOKEN_PREFIX)
    assert (call.role_id, call.members, call.tables) == ("analyst", ("analyst",), (ORDERS,))
    assert call.session_vars == {"provisa.region": "emea"}


def test_only_the_tokens_hash_is_kept(db):
    from sqlalchemy import select

    store = MintedTokenStore(db)

    async def go():
        token = await store.mint(_call())
        async with db.acquire() as conn:
            rows = (await conn.execute_core(select(minted_call_tokens))).fetchall()
        return token, rows

    token, rows = _run(go())
    assert len(rows) == 1
    assert token not in [str(v) for v in rows[0]]
    assert rows[0]._mapping["token_hash"] == minted.hash_token(token)


@pytest.mark.parametrize(
    ("presented", "call_id"),
    [
        ("provisa_call_wrong", "call-1"),  # a wrong token
        ("not-a-minted-token", "call-1"),  # not even its form
    ],
)
def test_a_wrong_token_is_refused(db, presented, call_id):
    store = MintedTokenStore(db)

    async def go():
        await store.mint(_call())
        return await store.resolve(presented, call_id)

    assert _run(go()) is None


def test_another_calls_token_is_refused(db):
    store = MintedTokenStore(db)

    async def go():
        token = await store.mint(_call("call-1"))
        return await store.resolve(token, "call-2")

    assert _run(go()) is None


def test_an_expired_token_is_refused(db):
    store = MintedTokenStore(db)

    async def go():
        token = await store.mint(_call(), ttl_s=-1.0)
        return await store.resolve(token, "call-1")

    assert _run(go()) is None


def test_a_token_ends_with_its_call(db):
    store = MintedTokenStore(db)

    async def go():
        token = await store.mint(_call())
        await store.retire(token)
        return await store.resolve(token, "call-1")

    assert _run(go()) is None


# -- what a minted session may run -------------------------------------------------------------


_STATE = SimpleNamespace(tracked_functions={"score": {}}, tracked_webhooks={})


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id, region FROM sales.orders",
        'SELECT "id" FROM "sales"."orders" WHERE region = \'emea\'',
        "WITH w AS (SELECT id FROM sales.orders) SELECT * FROM w",
        "SELECT oid FROM pg_catalog.pg_type",
        "SELECT * FROM information_schema.columns",
        "SELECT * FROM pg_type",
        "SET extra_float_digits = 3",
        "BEGIN",
        "COMMIT",
        "SHOW server_version",
        "-- only a comment",
    ],
)
def test_a_read_of_the_calls_table_and_what_a_driver_asks_are_admitted(sql):
    minted.refuse_outside_call(sql, (ORDERS,), _STATE)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM sales.customers",  # a table the call was not given
        "SELECT * FROM sales.orders JOIN sales.customers USING (id)",
        "SELECT * FROM sales.orders WHERE id IN (SELECT id FROM sales.customers)",
        # the inner table of a non-recursive CTE is a real one, whatever the CTE is called
        "WITH customers AS (SELECT * FROM customers) SELECT * FROM customers",
        "SELECT * FROM score('sales.orders')",  # running code from a sandbox
        "SELECT score(id) FROM sales.orders",
        "SELECT * FROM generate_series(1, 2)",
        "INSERT INTO sales.orders VALUES (1)",
        "WITH d AS (DELETE FROM sales.orders RETURNING id) SELECT * FROM d",
        "UPDATE sales.orders SET region = 'x'",
        "DELETE FROM sales.orders",
        "DROP TABLE sales.orders",
        "CREATE TABLE sales.t (id INT)",
        "COPY sales.orders TO STDOUT",
        "DO $$ BEGIN NULL; END $$",
        "SET ROLE admin",
        "SET SESSION AUTHORIZATION admin",
        "SET provisa.role = 'admin'",
        "SELECT 1 FROM",  # what does not parse is not admitted
    ],
)
def test_anything_else_is_refused(sql):
    with pytest.raises(CallTokenRefused):
        minted.refuse_outside_call(sql, (ORDERS,), _STATE)


def test_a_quoted_name_is_the_name_it_quotes():
    minted.refuse_outside_call('SELECT 1 FROM "sales"."Orders"', ('"sales"."Orders"',), _STATE)
    with pytest.raises(CallTokenRefused):
        minted.refuse_outside_call('SELECT 1 FROM "sales"."Orders"', ("sales.orders",), _STATE)


def test_the_read_statement_is_built_from_the_declared_columns():
    assert minted.read_statement(ORDERS, ["id", "we ird"]) == (
        'SELECT "id", "we ird" FROM sales.orders'
    )
    assert minted.read_statement(ORDERS, []) == "SELECT * FROM sales.orders"


# -- column-level visibility (REQ-2226) ------------------------------------------------------
# The listener holds no column check of its own: every statement is governed by the one pipeline
# as the calling role (tests/unit/test_minted_pipeline_column_visibility.py covers a hidden column
# refused, a granted one read, a star narrowed and an ancestor's grant reaching the child).


# -- the listener, on a socket -----------------------------------------------------------------


class _Wire:
    """A PostgreSQL client just far enough to authenticate and send one Query."""

    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)

    def _read(self, n: int) -> bytes:
        out = b""
        while len(out) < n:
            chunk = self.sock.recv(n - len(out))
            if not chunk:
                raise ConnectionError("closed")
            out += chunk
        return out

    def message(self) -> tuple[bytes, bytes]:
        kind = self._read(1)
        (length,) = struct.unpack("!i", self._read(4))
        return kind, self._read(length - 4)

    def start(self, user: str) -> tuple[bytes, bytes]:
        body = b"user\0" + user.encode() + b"\0\0"
        self.sock.sendall(struct.pack("!ii", 8 + len(body), 196608) + body)
        return self.message()

    def password(self, secret: str) -> tuple[bytes, bytes]:
        body = secret.encode() + b"\0"
        self.sock.sendall(b"p" + struct.pack("!i", 4 + len(body)) + body)
        return self.message()

    def until_ready(self) -> None:
        while self.message()[0] != b"Z":
            pass

    def query(self, sql: str) -> list[tuple[bytes, bytes]]:
        body = sql.encode() + b"\0"
        self.sock.sendall(b"Q" + struct.pack("!i", 4 + len(body)) + body)
        seen = []
        while True:
            kind, payload = self.message()
            if kind == b"Z":
                return seen
            seen.append((kind, payload))

    def close(self) -> None:
        self.sock.close()


def _code_of(error: bytes) -> str:
    fields = dict((f[:1], f[1:]) for f in error.split(b"\0") if f)
    return fields[b"C"].decode()


@pytest.fixture
def listener(db, monkeypatch):
    # Trust mode, the deployment under which the main listener admits whoever is named.
    state = SimpleNamespace(
        auth_config=None,
        auth_middleware_active=False,
        org_id="org1",
        admin_db=db,
        tracked_functions={},
        tracked_webhooks={},
        meta_qualifiers={},
    )
    monkeypatch.setattr(pgwire, "state", state)
    server = start_minted_pgwire_server("127.0.0.1", 0, None)
    yield server.server_address[1], MintedTokenStore(db)
    stop_pgwire_server(server)


def _authenticated(port: int, token: str, user: str = "call-1") -> _Wire:
    wire = _Wire(port)
    assert wire.start(user)[0] == b"R"  # a password is asked for, whatever the deployment's auth
    assert wire.password(token) == (b"R", struct.pack("!i", 0))  # AuthenticationOk
    wire.until_ready()
    return wire


def test_the_listener_never_admits_a_named_role_on_trust(listener):
    port, _ = listener
    wire = _Wire(port)
    kind, payload = wire.start("admin")
    assert kind == b"R" and struct.unpack("!i", payload[:4])[0] == 3  # cleartext requested
    kind, payload = wire.password("anything")
    assert kind == b"E" and _code_of(payload) == "28P01"
    wire.close()


@pytest.mark.parametrize("as_user", ["call-2", "call-1"])
def test_the_listener_refuses_a_wrong_or_other_calls_token(listener, as_user):
    port, store = listener
    token = _run(store.mint(_call("call-1")))
    wire = _Wire(port)
    wire.start(as_user)
    kind, payload = wire.password(token if as_user == "call-2" else "provisa_call_wrong")
    assert kind == b"E" and _code_of(payload) == "28P01"
    wire.close()


def test_the_listener_refuses_an_expired_token(listener):
    port, store = listener
    token = _run(store.mint(_call(), ttl_s=-1.0))
    wire = _Wire(port)
    wire.start("call-1")
    kind, payload = wire.password(token)
    assert kind == b"E" and _code_of(payload) == "28P01"
    wire.close()


def test_a_session_is_the_calls_role_and_reaches_the_pipeline_as_it(listener, monkeypatch):
    port, store = listener
    token = _run(store.mint(_call()))
    seen: dict = {}

    def pipeline(self, sql, *_args, **_kwargs):
        seen.update(role=self.role_id, user=self.user_id, org=self.org_id, sql=sql)
        raise PermissionError("stop here")

    monkeypatch.setattr(ProvisaSession, "_execute_sql_bound", pipeline)
    wire = _authenticated(port, token)
    answers = wire.query("SELECT id FROM sales.orders")
    wire.close()
    assert seen == {
        "role": "analyst",
        "user": "alice",
        "org": "org1",
        "sql": "SELECT id FROM sales.orders",
    }
    assert any(kind == b"E" for kind, _ in answers)


def test_a_statement_outside_the_call_is_refused_on_the_wire(listener, monkeypatch):
    port, store = listener
    token = _run(store.mint(_call()))
    monkeypatch.setattr(
        ProvisaSession,
        "_execute_sql_bound",
        lambda *_a, **_k: pytest.fail("reached the pipeline"),
    )
    wire = _authenticated(port, token)
    answers = wire.query("SELECT * FROM sales.customers")
    assert [_code_of(p) for k, p in answers if k == b"E"] == ["42501"]
    copy = wire.query("COPY sales.orders TO STDOUT")  # the path that skips execute_sql
    assert [_code_of(p) for k, p in copy if k == b"E"] == ["42501"]
    wire.close()


def test_an_open_session_ends_with_its_call(listener, monkeypatch):
    port, store = listener
    token = _run(store.mint(_call()))
    monkeypatch.setattr(
        ProvisaSession,
        "_execute_sql_bound",
        lambda *_a, **_k: pytest.fail("reached the pipeline"),
    )
    wire = _authenticated(port, token)
    _run(store.retire(token))
    answers = wire.query("SELECT id FROM sales.orders")
    assert [_code_of(p) for k, p in answers if k == b"E"] == ["42501"]
    wire.close()


# -- a call's table references -----------------------------------------------------------------


def _state(db, *, tls: bool = False, listening: bool = True) -> SimpleNamespace:
    server = SimpleNamespace(ssl_ctx=object() if tls else None) if listening else None
    return SimpleNamespace(
        admin_db=db, _minted_pgwire_server=server, meta_qualifiers={}, org_id="org1"
    )


_SESSION = MintedSession(correlation_id="call-9", identity="invoker", role_id="analyst", token="x")


def _payload() -> dict:
    return {"orders": {"kind": "table_ref", "ref": ORDERS, "columns": ["id"]}, "floor": 1}


@pytest.fixture
def port_set(monkeypatch):
    monkeypatch.setattr(
        settings_registry,
        "value",
        {"server.minted_pgwire_port": 6543, "server.hostname": "provisa.example"}.__getitem__,
    )


def test_a_call_hands_its_code_what_it_reads_with_and_the_token_ends_with_it(db, port_set):
    payload = _payload()
    store = MintedTokenStore(db)

    async def go():
        async with table_reads.readable(_state(db), _SESSION, payload, remote=False):
            connection = payload["orders"]["connection"]
            assert await store.resolve(connection["password"], "call-9") is not None
        return connection, await store.resolve(connection["password"], "call-9")

    connection, after = _run(go())
    assert (connection["host"], connection["port"]) == ("127.0.0.1", 6543)
    assert connection["user"] == "call-9"
    assert payload["orders"]["sql"] == 'SELECT "id" FROM sales.orders'
    assert after is None


def test_a_call_with_no_table_mints_nothing(db, port_set):
    async def go():
        async with table_reads.readable(_state(db), _SESSION, {"floor": 1}, remote=False):
            pass

    _run(go())


def test_a_remote_host_is_given_the_deployments_address(db, port_set):
    payload = _payload()

    async def go():
        async with table_reads.readable(_state(db, tls=True), _SESSION, payload, remote=True):
            return dict(payload["orders"]["connection"])

    connection = _run(go())
    assert (connection["host"], connection["port"]) == ("provisa.example", 6543)
    assert connection["sslmode"] == "require"


def test_a_remote_hosts_token_never_crosses_the_network_in_the_clear(db, port_set):
    async def go():
        async with table_reads.readable(_state(db, tls=False), _SESSION, _payload(), remote=True):
            pass

    with pytest.raises(table_reads.TableNotReadable, match="tls.pgwire_cert"):
        _run(go())


def test_no_listener_refuses_by_the_setting(db, monkeypatch):
    monkeypatch.setattr(settings_registry, "value", {"server.minted_pgwire_port": 0}.__getitem__)

    async def go():
        async with table_reads.readable(_state(db), _SESSION, _payload(), remote=False):
            pass

    with pytest.raises(table_reads.TableNotReadable, match="server.minted_pgwire_port"):
        _run(go())


def test_a_reference_that_is_no_table_name_is_refused(db, port_set):
    payload = {"orders": {"kind": "table_ref", "ref": "not a table;", "columns": []}}

    async def go():
        async with table_reads.readable(_state(db), _SESSION, payload, remote=False):
            pass

    with pytest.raises(CallTokenRefused):
        _run(go())


# -- the sandboxes allow that one port ---------------------------------------------------------


def test_the_macos_profile_allows_the_listeners_port_and_no_other():
    text = macos_sandbox.profile(
        executable=Path("/bin/python"),
        read=[],
        socket_path=None,
        proxy_port=1111,
        connect_ports=(6543,),
    )
    assert '(remote ip "localhost:1111"))' in text
    assert '(remote ip "localhost:6543")' in text
    assert text.count("(remote ip") == 2


def test_the_linux_launcher_names_the_port():
    argv = linux_sandbox.launcher_argv(
        "/py", read=["/code"], proxy_port=1111, argv=["/w"], connect_ports=(6543,)
    )
    assert argv[argv.index("--connect-port") + 1] == "6543"


@pytest.mark.parametrize("port", [6543, 0])
def test_a_workers_sandbox_gets_the_port_of_the_setting_and_a_reader_never(monkeypatch, port):
    monkeypatch.setattr(settings_registry, "value", {"server.minted_pgwire_port": port}.__getitem__)
    assert sandbox._minted_ports() == ((port,) if port else ())
    called: dict = {}

    def linux(_python, *, read, proxy_port, argv, connect_ports=()):
        called["ports"] = connect_ports
        return list(argv)

    monkeypatch.setattr(sandbox.platform, "system", lambda: "Linux")
    monkeypatch.setattr(sandbox, "launcher_argv", linux)
    monkeypatch.setattr(
        sandbox, "egress_proxy", lambda _allow: SimpleNamespace(port=1111, environment=dict)
    )
    command = sandbox.sandboxed(
        lambda address: ["/w", address],
        executable=Path("/py"),
        read=[],
        env={},
        cwd=Path("/"),
        egress=[],
    )
    command.argv("sock")
    assert called["ports"] == ((port,) if port else ())
    called.clear()
    sandbox.sandboxed_once(["/r"], executable=Path("/py"), read=[], env={}, egress=[])
    assert called["ports"] == ()
