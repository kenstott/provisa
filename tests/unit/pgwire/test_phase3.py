# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-cdef-012345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 3 pgwire hardening tests.

Covers:
- New catalog tables: pg_extension, pg_enum, pg_stat_activity
- information_schema: key_column_usage, table_constraints, referential_constraints
- pg_constraint populated with PK/FK rows
- COPY command rejection (SQLSTATE 0A000)
- TLS connection with self-signed cert
- SQLAlchemy psycopg2 compatibility
"""

from __future__ import annotations

import asyncio
import socket
import ssl
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio

from provisa.pgwire.catalog import _build_catalog_db, classify
from provisa.pgwire.server import ProvisaConnection, ProvisaServer  # noqa: F401


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_server(port: int, ssl_ctx=None) -> ProvisaServer:
    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn, ssl_ctx=ssl_ctx)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.1)
    return server


def _stub_auth_provider(valid_user: str, valid_password: str):
    provider = MagicMock()

    def _login(username, password):
        if username == valid_user and password == valid_password:
            return username
        raise ValueError("Invalid credentials")

    provider.login.side_effect = _login
    return provider


def _make_ctx_with_fk():
    """Return a CompilationContext mock with one PK table and one FK join."""
    from provisa.compiler.sql_gen import TableMeta, JoinMeta

    dogs_tm = MagicMock(spec=TableMeta)
    dogs_tm.table_name = "dogs"
    dogs_tm.schema_name = "public"
    dogs_tm.catalog_name = "provisa"
    dogs_tm.domain_id = "public"
    dogs_tm.table_id = 1
    dogs_tm.type_name = "Dog"

    breeds_tm = MagicMock(spec=TableMeta)
    breeds_tm.table_name = "breeds"
    breeds_tm.schema_name = "public"
    breeds_tm.catalog_name = "provisa"
    breeds_tm.domain_id = "public"
    breeds_tm.table_id = 2
    breeds_tm.type_name = "Breed"

    jm = MagicMock(spec=JoinMeta)
    jm.source_column = "breed_id"
    jm.target_column = "id"
    jm.target = breeds_tm

    ctx = MagicMock()
    ctx.tables = {"dog": dogs_tm, "breed": breeds_tm}
    ctx.pk_columns = {1: ["id"], 2: ["id"]}
    ctx.joins = {("Dog", "breed"): jm}
    return ctx


def _make_col(name, dtype, nullable, _ordinal=0):
    c = MagicMock()
    c.column_name = name
    c.data_type = dtype
    c.is_nullable = nullable
    return c


def _make_state(ctx=None):
    state = MagicMock()
    if ctx is None:
        mc = MagicMock()
        mc.tables = {}
        state.contexts = {"alice": mc}
    else:
        state.contexts = {"alice": ctx}
    state.schema_build_cache = {
        "column_types": {
            1: [
                _make_col("id", "integer", False, 1),
                _make_col("breed_id", "integer", True, 2),
                _make_col("name", "varchar", True, 3),
            ],
            2: [
                _make_col("id", "integer", False, 1),
                _make_col("breed_name", "varchar", True, 2),
            ],
        }
    }
    state.auth_config = {"provider": "simple"}
    state.auth_middleware_active = True
    return state


# ── catalog unit tests ──────────────────────────────────────────────────────


def test_pg_extension_table_exists():
    state = _make_state()
    db = _build_catalog_db("alice", state)
    rows = db.execute("SELECT * FROM _pg_extension").fetchall()
    db.close()
    assert rows == []


def test_pg_enum_table_exists():
    state = _make_state()
    db = _build_catalog_db("alice", state)
    rows = db.execute("SELECT * FROM _pg_enum").fetchall()
    db.close()
    assert rows == []


def test_pg_stat_activity_table_exists():
    state = _make_state()
    db = _build_catalog_db("alice", state)
    rows = db.execute("SELECT * FROM _pg_stat_activity").fetchall()
    db.close()
    assert rows == []


def test_is_table_constraints_empty_without_ctx():
    state = _make_state()
    db = _build_catalog_db("alice", state)
    rows = db.execute("SELECT * FROM _is_table_constraints").fetchall()
    db.close()
    assert rows == []


def test_is_key_column_usage_empty_without_ctx():
    state = _make_state()
    db = _build_catalog_db("alice", state)
    rows = db.execute("SELECT * FROM _is_key_column_usage").fetchall()
    db.close()
    assert rows == []


def test_is_referential_constraints_exists():
    state = _make_state()
    db = _build_catalog_db("alice", state)
    rows = db.execute("SELECT * FROM _is_referential_constraints").fetchall()
    db.close()
    assert rows == []


def test_pg_constraint_pk_populated():
    ctx = _make_ctx_with_fk()
    state = _make_state(ctx)
    db = _build_catalog_db("alice", state)
    rows = db.execute("SELECT conname, contype FROM _pg_constraint WHERE contype='p'").fetchall()
    db.close()
    names = [r[0] for r in rows]
    assert "pk_dogs" in names
    assert "pk_breeds" in names


def test_pg_constraint_fk_populated():
    ctx = _make_ctx_with_fk()
    state = _make_state(ctx)
    db = _build_catalog_db("alice", state)
    rows = db.execute("SELECT conname, contype FROM _pg_constraint WHERE contype='f'").fetchall()
    db.close()
    names = [r[0] for r in rows]
    assert any("breed_id" in n for n in names)


def test_is_table_constraints_pk_rows():
    ctx = _make_ctx_with_fk()
    state = _make_state(ctx)
    db = _build_catalog_db("alice", state)
    rows = db.execute(
        "SELECT constraint_name, constraint_type FROM _is_table_constraints WHERE constraint_type='PRIMARY KEY'"
    ).fetchall()
    db.close()
    names = [r[0] for r in rows]
    assert "pk_dogs" in names
    assert "pk_breeds" in names


def test_is_key_column_usage_pk_columns():
    ctx = _make_ctx_with_fk()
    state = _make_state(ctx)
    db = _build_catalog_db("alice", state)
    rows = db.execute(
        "SELECT column_name FROM _is_key_column_usage WHERE constraint_name='pk_dogs'"
    ).fetchall()
    db.close()
    assert len(rows) >= 1
    assert rows[0][0] == "id"


def test_classify_copy_is_passthrough():
    assert classify("COPY dogs FROM STDIN") == "PASS_THROUGH"


# ── wire-level COPY rejection ────────────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def pgwire_server_p3():
    import provisa.pgwire.server as _srv

    loop = asyncio.get_running_loop()
    with _srv._loop_lock:
        _srv._loop = loop
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()
    with _srv._loop_lock:
        _srv._loop = None


@pytest.fixture(scope="module")
def mock_state_p3():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.contexts = {"alice": ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": "simple"}
    state.auth_middleware_active = True
    return state


@pytest.mark.asyncio
async def test_copy_rejected(pgwire_server_p3, mock_state_p3):
    port = pgwire_server_p3
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p3),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="alice",
            password="secret",
            database="provisa",
        )
        with pytest.raises(asyncpg.PostgresError):
            await conn.execute("COPY dogs FROM STDIN")
        await conn.close()


@pytest.mark.asyncio
async def test_new_catalog_tables_queryable(pgwire_server_p3, mock_state_p3):
    port = pgwire_server_p3
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p3),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="alice",
            password="secret",
            database="provisa",
        )
        rows = await conn.fetch("SELECT * FROM pg_catalog.pg_extension")
        assert rows == []
        rows = await conn.fetch("SELECT * FROM information_schema.table_constraints")
        assert rows == []
        await conn.close()


# ── TLS test ─────────────────────────────────────────────────────────────────


def _make_self_signed_cert():
    """Generate a minimal self-signed cert+key using cryptography library."""
    try:
        import datetime
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(
                datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
            )
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        key_pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        return cert_pem, key_pem
    except ImportError:
        return None, None


@pytest.mark.asyncio
async def test_tls_connection():
    cert_pem, key_pem = _make_self_signed_cert()
    if cert_pem is None or key_pem is None:
        pytest.skip("cryptography library not available")

    import provisa.pgwire.server as _srv

    loop = asyncio.get_running_loop()
    with _srv._loop_lock:
        _srv._loop = loop

    with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as cf:
        cf.write(cert_pem)
        cert_path = cf.name
    with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as kf:
        kf.write(key_pem)
        key_path = kf.name

    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(cert_path, key_path)

    port = _free_port()
    server = _make_server(port, ssl_ctx=ssl_ctx)
    provider = _stub_auth_provider("alice", "secret")

    client_ctx = ssl.create_default_context()
    client_ctx.check_hostname = False
    client_ctx.verify_mode = ssl.CERT_NONE

    mock_state = MagicMock()
    mc = MagicMock()
    mc.tables = {}
    mock_state.contexts = {"alice": mc}
    mock_state.schema_build_cache = {"column_types": {}}
    mock_state.auth_config = {"provider": "simple"}
    mock_state.auth_middleware_active = True

    try:
        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", mock_state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="alice",
                password="secret",
                database="provisa",
                ssl=client_ctx,
            )
            row = await conn.fetchrow("SHOW server_version")
            await conn.close()
        assert row is not None
        assert "provisa" in str(row[0])
    finally:
        server.shutdown()
        with _srv._loop_lock:
            _srv._loop = None


# ── SQLAlchemy psycopg2 compatibility ─────────────────────────────────────────


def test_sqlalchemy_reflect(pgwire_server_p3, mock_state_p3):
    """SQLAlchemy table reflection must not raise against the catalog."""
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("psycopg2")
    import sqlalchemy as sa

    port = pgwire_server_p3
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state_p3),
    ):
        url = f"postgresql+psycopg2://alice:secret@127.0.0.1:{port}/provisa"
        engine = sa.create_engine(url)
        with engine.connect():
            insp = sa.inspect(engine)
            table_names = insp.get_table_names()
        engine.dispose()
    assert isinstance(table_names, list)
