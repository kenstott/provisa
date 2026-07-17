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

The backend is the shared un-stubbed, real-Postgres pgwire server (``pgwire_pg_backend``
fixture in conftest) — introspection answers come from the real catalog and the data
read runs the real DIRECT pipeline.

The pgjdbc jar is fetched once from Maven Central into ~/.cache/provisa-test-jars;
the test skips if it cannot be provisioned or the JVM cannot start.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from pathlib import Path

import pytest

# `isolated`: pgjdbc runs in a jpype JVM, which is process-global and cannot be
# restarted with a new classpath. Another jpype test (e.g. govdata) starting the
# JVM first with a different classpath makes pgjdbc classes unresolvable here.
# Deselected from the default lane and run in its own pytest process by
# scripts/test-all so the JVM is always fresh for this module.
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
    try:
        urllib.request.urlretrieve(_PGJDBC_URL, _JAR_PATH)  # noqa: S310  # fixed Maven Central URL
    except (urllib.error.URLError, OSError) as exc:
        pytest.skip(f"pgjdbc jar unavailable (offline?): {exc}")
    return str(_JAR_PATH)


@pytest.fixture(scope="session", autouse=True)
def jvm():
    """Start the JVM once with pgjdbc on the classpath. jpype cannot restart a JVM,
    so this is session-scoped and never shut down within the run."""
    jar = _ensure_pgjdbc_jar()
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[jar])
    # Register the driver explicitly (JDBC 4 autoloading may not fire under jpype).
    jpype.JClass("java.lang.Class").forName("org.postgresql.Driver")
    yield


def _connect(port: int):
    DriverManager = jpype.JClass("java.sql.DriverManager")
    url = f"jdbc:postgresql://127.0.0.1:{port}/provisa"
    props = jpype.JClass("java.util.Properties")()
    props.setProperty("user", "admin")
    props.setProperty("password", "x")
    return DriverManager.getConnection(url, props)


def test_jdbc_getTables_lists_governed_table(pgwire_pg_backend):
    """DatabaseMetaData.getTables — the first call DataGrip/DBeaver make per schema."""
    be = pgwire_pg_backend
    conn = _connect(be["port"])
    try:
        md = conn.getMetaData()
        rs = md.getTables(None, be["schema"], be["table"], jpype.JArray(jpype.JString)(["TABLE"]))
        found = []
        while rs.next():
            found.append(str(rs.getString("TABLE_NAME")))
        rs.close()
    finally:
        conn.close()
    assert be["table"] in found


def test_jdbc_getColumns_reports_column_types(pgwire_pg_backend):
    """DatabaseMetaData.getColumns — drives the ER-diagram / column-tree introspection."""
    be = pgwire_pg_backend
    conn = _connect(be["port"])
    try:
        md = conn.getMetaData()
        rs = md.getColumns(None, be["schema"], be["table"], None)
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
