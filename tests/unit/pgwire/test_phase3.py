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

from provisa.pgwire.catalog import classify
from provisa.pgwire.catalog_populate import _build_catalog_db
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
    dogs_tm.field_name = "public__dogs"
    dogs_tm.display_name = "dogs"
    dogs_tm.schema_name = "public"
    dogs_tm.catalog_name = "provisa"
    dogs_tm.domain_id = "public"
    dogs_tm.table_id = 1
    dogs_tm.type_name = "Dog"

    breeds_tm = MagicMock(spec=TableMeta)
    breeds_tm.table_name = "breeds"
    breeds_tm.field_name = "public__breeds"
    breeds_tm.display_name = "breeds"
    breeds_tm.schema_name = "public"
    breeds_tm.catalog_name = "provisa"
    breeds_tm.domain_id = "public"
    breeds_tm.table_id = 2
    breeds_tm.type_name = "Breed"

    jm = MagicMock(spec=JoinMeta)
    jm.source_column = "breed_id"
    jm.target_column = "id"
    jm.target = breeds_tm
    jm.cardinality = "many-to-one"
    jm.source_constant = None
    jm.source_expr = None

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


def test_is_referential_constraints_populated():
    ctx = _make_ctx_with_fk()
    state = _make_state(ctx)
    db = _build_catalog_db("alice", state)
    rows = db.execute(
        "SELECT constraint_name, unique_constraint_name, match_option, update_rule, delete_rule "
        "FROM _is_referential_constraints"
    ).fetchall()
    db.close()
    assert len(rows) == 1
    con_name, uniq_name, match_opt, upd, dele = rows[0]
    assert "breed_id" in con_name
    assert uniq_name == "pk_breeds"
    assert match_opt == "NONE"
    assert upd == "NO ACTION"
    assert dele == "NO ACTION"


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


# ── DBeaver ER diagram mock ───────────────────────────────────────────────────


def _make_ctx_er():
    """pet_store-like context: meta.pets FK→ meta.registered_tables."""
    from provisa.compiler.sql_gen import TableMeta, JoinMeta

    pets_tm = MagicMock(spec=TableMeta)
    pets_tm.table_id = 10
    pets_tm.field_name = "meta__pets"
    pets_tm.display_name = "pets"
    pets_tm.table_name = "pets"
    pets_tm.schema_name = "meta"
    pets_tm.catalog_name = "provisa"
    pets_tm.domain_id = "meta"
    pets_tm.type_name = "Pet"

    reg_tm = MagicMock(spec=TableMeta)
    reg_tm.table_id = 11
    reg_tm.field_name = "meta__registeredTables"
    reg_tm.display_name = "registeredTables"
    reg_tm.table_name = "registered_tables"
    reg_tm.schema_name = "meta"
    reg_tm.catalog_name = "provisa"
    reg_tm.domain_id = "meta"
    reg_tm.type_name = "RegisteredTable"

    fk_jm = MagicMock(spec=JoinMeta)
    fk_jm.source_column = "registered_table_id"
    fk_jm.target_column = "id"
    fk_jm.target = reg_tm
    fk_jm.cardinality = "many-to-one"
    fk_jm.source_constant = None
    fk_jm.source_expr = None

    ctx = MagicMock()
    ctx.tables = {"pets": pets_tm, "registered_tables": reg_tm}
    ctx.pk_columns = {10: ["id"], 11: ["id"]}
    ctx.joins = {("Pet", "registered_table"): fk_jm}
    return ctx


def _make_state_er():
    ctx = _make_ctx_er()
    state = MagicMock()
    state.contexts = {"alice": ctx}
    state.schema_build_cache = {
        "column_types": {
            10: [
                _make_col("id", "integer", False, 1),
                _make_col("name", "varchar", True, 2),
                _make_col("species", "varchar", True, 3),
                _make_col("registered_table_id", "integer", True, 4),
            ],
            11: [
                _make_col("id", "integer", False, 1),
                _make_col("name", "varchar", True, 2),
            ],
        },
        "tables": [],
    }
    state.auth_config = {"provider": "simple"}
    state.auth_middleware_active = True
    return state


@pytest_asyncio.fixture(scope="module")
async def pgwire_server_er():
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


@pytest.mark.asyncio
async def test_er_meta_schema_visible(pgwire_server_er):
    """Wire: pg_namespace must include 'meta' schema when context has meta tables."""
    port = pgwire_server_er
    provider = _stub_auth_provider("alice", "secret")
    state = _make_state_er()
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT oid, nspname FROM pg_catalog.pg_namespace")
        await conn.close()
    names = [r["nspname"] for r in rows]
    assert "meta" in names, f"meta missing from pg_namespace: {names}"


@pytest.mark.asyncio
async def test_er_pg_class_snake_case(pgwire_server_er):
    """Wire: pg_class.relname must use snake_case (central naming authority)."""
    port = pgwire_server_er
    provider = _stub_auth_provider("alice", "secret")
    state = _make_state_er()
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        rows = await conn.fetch("SELECT relname FROM pg_catalog.pg_class WHERE relkind='r'")
        await conn.close()
    names = [r["relname"] for r in rows]
    assert "pets" in names, f"pets missing: {names}"
    assert "registered_tables" in names, f"registered_tables missing: {names}"
    assert "registeredTables" not in names, f"camelCase in relname: {names}"


@pytest.mark.asyncio
async def test_er_bulk_attribute_query(pgwire_server_er):
    """Wire: DBeaver bulk ER diagram query returns deserialized column rows."""
    port = pgwire_server_er
    provider = _stub_auth_provider("alice", "secret")
    state = _make_state_er()
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        ns_rows = await conn.fetch("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='meta'")
        assert len(ns_rows) == 1, "meta namespace not found"

        # Use sub-select to avoid asyncpg parameter-type-inference limitations.
        # JDBC (DBeaver) specifies param OIDs explicitly; asyncpg does not.
        rows = await conn.fetch(
            """SELECT c.oid, a.attname, a.atttypid, a.attnum
               FROM pg_catalog.pg_attribute a, pg_catalog.pg_class c
               WHERE a.attrelid=c.oid
               AND c.relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='meta')
               AND a.attnum>0 AND NOT a.attisdropped
               ORDER BY c.oid, a.attnum"""
        )
        await conn.close()

    assert len(rows) == 6, (
        f"expected 6 (4 pets + 2 reg), got {len(rows)}: {[r['attname'] for r in rows]}"
    )
    col_names = [r["attname"] for r in rows]
    assert "id" in col_names
    assert "registered_table_id" in col_names
    attnums = [r["attnum"] for r in rows]
    assert all(n > 0 for n in attnums), f"non-positive attnum: {attnums}"
    atttypids = [r["atttypid"] for r in rows]
    assert all(t > 0 for t in atttypids), f"zero atttypid: {atttypids}"


@pytest.mark.asyncio
async def test_er_fk_constraint_deserialized(pgwire_server_er):
    """Wire: pg_constraint FK row must have non-empty integer-array conkey/confkey after deserialization."""
    port = pgwire_server_er
    provider = _stub_auth_provider("alice", "secret")
    state = _make_state_er()
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        con_rows = await conn.fetch(
            "SELECT oid, conname, contype, conrelid, confrelid, conkey, confkey"
            " FROM pg_catalog.pg_constraint"
            " WHERE conrelid=(SELECT oid FROM pg_catalog.pg_class WHERE relname='pets' AND relkind='r')"
        )
        await conn.close()

    fk_rows = [r for r in con_rows if r["contype"] == "f"]
    assert len(fk_rows) >= 1, f"no FK rows for pets: {con_rows}"
    fk = fk_rows[0]
    assert fk["confrelid"] is not None and fk["confrelid"] > 0, "confrelid missing"
    conkey = fk["conkey"]
    confkey = fk["confkey"]
    assert conkey is not None and len(conkey) > 0, f"conkey empty: {conkey}"
    assert confkey is not None and len(confkey) > 0, f"confkey empty: {confkey}"
    assert conkey[0] == 4, f"conkey[0] must be 4 (registered_table_id is 4th col), got {conkey[0]}"
    assert confkey[0] == 1, (
        f"confkey[0] must be 1 (id is 1st col of registered_tables), got {confkey[0]}"
    )


@pytest.mark.asyncio
async def test_er_mermaid_diagram(pgwire_server_er):
    """Wire: catalog queries produce a valid Mermaid ER diagram with FK relationships."""
    _PG_OID_TO_TYPE = {
        23: "int",
        1043: "varchar",
        20: "bigint",
        16: "bool",
        25: "text",
        114: "json",
        3802: "jsonb",
        21: "smallint",
        700: "float4",
        701: "float8",
        1082: "date",
        1114: "timestamp",
        1184: "timestamptz",
        1700: "numeric",
    }
    port = pgwire_server_er
    provider = _stub_auth_provider("alice", "secret")
    state = _make_state_er()
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user="alice", password="secret", database="provisa"
        )
        tables = await conn.fetch(
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c"
            " JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace"
            " WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema')"
        )
        attrs = await conn.fetch(
            "SELECT a.attrelid, a.attname, a.atttypid, a.attnum"
            " FROM pg_catalog.pg_attribute a"
            " JOIN pg_catalog.pg_class c ON c.oid=a.attrelid"
            " JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace"
            " WHERE a.attnum>0 AND NOT a.attisdropped"
            " AND n.nspname NOT IN ('pg_catalog','information_schema')"
            " ORDER BY a.attrelid, a.attnum"
        )
        cons = await conn.fetch(
            "SELECT contype, conrelid, confrelid, conkey, confkey, conname"
            " FROM pg_catalog.pg_constraint WHERE contype IN ('f','p')"
        )
        await conn.close()

    oid_to_name = {r["oid"]: r["relname"] for r in tables}
    by_table: dict = {}
    for a in attrs:
        by_table.setdefault(a["attrelid"], []).append(a)
    pk_by_table: dict = {}
    for con in cons:
        if con["contype"] == "p" and con["conkey"]:
            col = next(
                (
                    a["attname"]
                    for a in by_table.get(con["conrelid"], [])
                    if a["attnum"] == con["conkey"][0]
                ),
                None,
            )
            if col:
                pk_by_table[con["conrelid"]] = col

    lines = ["erDiagram"]
    for t in tables:
        oid, tname = t["oid"], t["relname"]
        lines.append(f"    {tname} {{")
        pk = pk_by_table.get(oid)
        for a in by_table.get(oid, []):
            dtype = _PG_OID_TO_TYPE.get(a["atttypid"], f"oid{a['atttypid']}")
            tag = " PK" if a["attname"] == pk else ""
            lines.append(f"        {dtype} {a['attname']}{tag}")
        lines.append("    }")
    for con in cons:
        if con["contype"] == "f":
            src = oid_to_name.get(con["conrelid"])
            tgt = oid_to_name.get(con["confrelid"])
            if src and tgt:
                lines.append(f'    {src} }}o--|| {tgt} : "{con["conname"]}"')
    diagram = "\n".join(lines)

    assert "erDiagram" in diagram
    assert "pets {" in diagram
    assert "registered_tables {" in diagram
    assert "int id PK" in diagram
    assert "int registered_table_id" in diagram
    assert "}o--||" in diagram, "FK relationship line missing"
    assert "registered_tables" in diagram.split("}o--||")[1], "FK must point to registered_tables"
