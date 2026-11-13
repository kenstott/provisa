# Copyright (c) 2026 Kenneth Stott
# Canary: cc8f7124-debd-4c3b-a5b0-8eab400411c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Real JDBC (pgjdbc) introspection against the Provisa pgwire server (REQ-883).

DBeaver and DataGrip both introspect and diagram a database through the pgjdbc
driver's ``java.sql.DatabaseMetaData`` API — ``getTables`` / ``getColumns`` /
``getPrimaryKeys`` / ``getImportedKeys`` — NOT through hand-written catalog SQL.
The existing ER tests replicate DBeaver's *queries* over asyncpg, which cannot
reproduce the real JDBC wire behaviour (pgjdbc pins param OIDs, uses the extended
protocol, and drives DatabaseMetaData). This test drives the ACTUAL pgjdbc driver
in-process via jpype, so it exercises exactly the calls DataGrip and DBeaver make.

Introspection runs against a real Provisa server (``IsolatedServer``, pgwire enabled): a
pgwire catalog statement is addressed to the meta domain and governed as an ordinary read of it
(REQ-2190), which a hand-built state with no meta domain cannot answer. The data read runs on the
shared un-stubbed, real-Postgres pgwire server (``pgwire_pg_backend`` fixture in conftest) and the
real DIRECT pipeline.

The pgjdbc jar is fetched once from Maven Central into ~/.cache/provisa-test-jars;
the test skips if it cannot be provisioned or the JVM cannot start.
"""

from __future__ import annotations

import asyncio
import urllib.request
from pathlib import Path

import pytest
import yaml

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

# `isolated`: pgjdbc runs in a jpype JVM, which is process-global and cannot be
# restarted. Deselected from the default lane; the isolated lane runs it in one process
# with the other isolated modules, so pgjdbc has a class loader of its own (jvm()).
pytestmark = [pytest.mark.integration, pytest.mark.isolated]

jpype = pytest.importorskip("jpype", reason="jpype required to drive the real pgjdbc driver")

_PGJDBC_VERSION = "42.7.4"
_PGJDBC_URL = (
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/"
    f"{_PGJDBC_VERSION}/postgresql-{_PGJDBC_VERSION}.jar"
)
_JAR_DIR = Path.home() / ".cache" / "provisa-test-jars"
_JAR_PATH = _JAR_DIR / f"postgresql-{_PGJDBC_VERSION}.jar"


def _ensure_pgjdbc_jar() -> str:
    if _JAR_PATH.exists():
        return str(_JAR_PATH)
    _JAR_DIR.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(_PGJDBC_URL, _JAR_PATH)  # noqa: S310  # fixed Maven Central URL
    return str(_JAR_PATH)


_PGJDBC: dict[str, object] = {}


@pytest.fixture(scope="session", autouse=True)
def jvm():
    """pgjdbc, in the process's one JVM. The isolated lane runs every isolated module in one
    process, and test_jdbc_source_jar_e2e may start the JVM first, without pgjdbc on its
    classpath; jpype cannot restart it. So pgjdbc is loaded by a class loader of its own, as
    Provisa loads a customer's driver (provisa.jdbc_source.jdbc), and connected to directly."""
    jar = _ensure_pgjdbc_jar()
    # jpype.dbapi2 (the JDBC source's) makes java.sql.SQLException's Python class derive from its
    # Error, which it can only do before that class is first made -- and a PSQLException this
    # module's driver raises makes it. Register dbapi2 first, whichever module runs first.
    import jpype.dbapi2  # noqa: F401

    if not jpype.isJVMStarted():
        jpype.startJVM()
    url = jpype.JClass("java.io.File")(jar).toURI().toURL()
    parent = jpype.JClass("java.lang.ClassLoader").getSystemClassLoader()
    loader = jpype.JClass("java.net.URLClassLoader")([url], parent)
    driver = jpype.JClass("java.lang.Class").forName("org.postgresql.Driver", True, loader)
    _PGJDBC["driver"] = driver.getDeclaredConstructor().newInstance()
    yield


def _pgjdbc_connect(port: int, user: str, password: str):
    url = f"jdbc:postgresql://127.0.0.1:{port}/provisa"
    props = jpype.JClass("java.util.Properties")()
    props.setProperty("user", user)
    props.setProperty("password", password)
    return _PGJDBC["driver"].connect(url, props)  # type: ignore[attr-defined]


def _connect(port: int):
    return _pgjdbc_connect(port, "admin", "x")


_ORG = "jdbc_introspection"
_REPO_ROOT = Path(__file__).parents[2]
_ROLE = "jdbc_introspector"
_SCHEMA = "sales_analytics"  # domain sales-analytics, as the catalog names it
_TABLE = "orders"


def _config(tmp_dir: Path) -> str:
    """sample_config.yaml plus a role reaching every domain and every column."""
    with open(_REPO_ROOT / "tests" / "fixtures" / "sample_config.yaml") as f:
        cfg = yaml.safe_load(f)
    cfg["roles"].append(
        {
            "id": _ROLE,
            "parents": ["developer"],
            "description": "Test role.",
            "capabilities": ["catalog_read", "data_product_read", "query_development", "usage"],
            "domain_access": ["*"],
        }
    )
    for table in cfg["tables"]:
        for column in table["columns"]:
            column["visible_to"] = [*column["visible_to"], _ROLE]
    path = tmp_dir / "jdbc-introspection.yaml"
    path.write_text(yaml.safe_dump(cfg, sort_keys=False))
    return str(path)


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        enable_pgwire=True,
        config=_config(tmp_path_factory.mktemp("jdbc-introspection")),
        control_plane="own",
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _connect_as(port: int, user: str):
    return _pgjdbc_connect(port, user, "provisa")


def test_jdbc_getTables_lists_governed_table(server):
    """DatabaseMetaData.getTables — the first call DataGrip/DBeaver make per schema."""
    conn = _connect_as(server.pgwire_port, _ROLE)
    try:
        md = conn.getMetaData()
        rs = md.getTables(None, _SCHEMA, _TABLE, jpype.JArray(jpype.JString)(["TABLE"]))
        found = []
        while rs.next():
            found.append(str(rs.getString("TABLE_NAME")))
        rs.close()
    finally:
        conn.close()
    assert _TABLE in found


def test_jdbc_getColumns_reports_column_types(server):
    """DatabaseMetaData.getColumns — drives the ER-diagram / column-tree introspection."""
    conn = _connect_as(server.pgwire_port, _ROLE)
    try:
        md = conn.getMetaData()
        rs = md.getColumns(None, _SCHEMA, _TABLE, None)
        cols = {}
        while rs.next():
            cols[str(rs.getString("COLUMN_NAME"))] = str(rs.getString("TYPE_NAME"))
        rs.close()
    finally:
        conn.close()
    # The catalog also advertises the governance virtual columns _domain_/_name_ (by design,
    # consumed by the relationship-guard and Cypher join paths); assert the real table columns
    # are all introspected with non-empty types.
    assert {"id", "amount", "region"} <= set(cols)
    assert all(cols[c] for c in ("id", "amount", "region"))


def test_jdbc_query_returns_real_rows(pgwire_pg_backend):
    """A real pgjdbc extended-protocol query reads the real rows through the DIRECT pipeline."""
    be = pgwire_pg_backend
    conn = _connect(be["port"])
    try:
        st = conn.createStatement()
        rs = st.executeQuery(
            f"SELECT id, amount, region FROM {be['schema']}.{be['table']} ORDER BY id"
        )
        rows = []
        while rs.next():
            region = rs.getString("region")
            region = None if rs.wasNull() else str(region)
            rows.append((int(rs.getInt("id")), float(rs.getDouble("amount")), region))
        rs.close()
        st.close()
    finally:
        conn.close()
    assert rows == [(r[0], r[1], r[2]) for r in be["rows"]]
