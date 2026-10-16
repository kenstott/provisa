# Copyright (c) 2026 Kenneth Stott
# Canary: 1808a691-74db-40fc-ad5d-06d7cea8acd7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""AskAmerica on Trino, LIVE, through Trino's built-in PostgreSQL connector (REQ-540).

This is the test ``trino_connectors.TRINO_READS_PGWIRE_ADAPTER_LIVE`` waits for. It points a
``postgresql`` catalog at the source's bundled ``pgwire-govdata`` server -- the endpoint DuckDB,
Postgres and ClickHouse attach -- and settles what reading the adapter's source cannot:

- that the PostgreSQL JDBC driver's metadata queries (getSchemas / getTables / getColumns) parse
  on the adapter's pg_catalog emulation and return the source's schemas, tables and columns;
- that rows come back over the extended protocol in the formats the driver asks for;
- how Trino's lower-cased names resolve against the adapter (``case-insensitive-name-matching``);
- that a table reads alone and joined with a table of another catalog in one statement.

Each step runs in order and names the statement it failed on; the adapter's own log is
attached to the failure, since the adapter is where the cause will be.

Reachability
------------
Trino runs in a container here and the adapter on the test host. For the life of its endpoint
this module sets ``PROVISA_PGWIRE_BIND_HOST`` (the address the adapter listens on) and
``PROVISA_PGWIRE_ENGINE_HOST`` (the name Trino reaches it by): on Linux (CI) both are the default
bridge's gateway, a host address a container reaches; Docker Desktop's gateway is inside its VM,
so there the adapter stays on loopback and Trino reaches it as ``host.docker.internal``. Only this
module's endpoint listens off loopback; every other test's endpoint is left where it was.

Credentials: ``ASKAMERICA_API_KEY`` (a secret of the warehouse lane).
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import as_deployment_org, deployment_org_on_this_node

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_SOURCE_ID = "askamerica-trino-itest"
_CATALOG = "askamerica_trino_itest"
_SCHEMAS = "fec,ref,geo"
_SCHEMA = "fec"
_TABLE = "candidates"
_COLD_START_SECONDS = 600


def _cold_start(source, pr) -> None:
    """Start the source's server and wait for its first catalog query, failing with the end of
    the server's own log when the wait runs out: the report otherwise says only "STARTING" (warehouse
    run 38886673581: the catalog was not answered in 600 s)."""
    try:
        pr.ensure_endpoint(source, timeout=_COLD_START_SECONDS)
    except pr.SourceStillStartingError as exc:
        server = pr._endpoint_replica(source)._server
        tail = server.log_tail() if server is not None else "(no server was started)"
        raise AssertionError(f"{exc}\nthe server's log, its end:\n{tail}") from exc


_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


def _required(name: str, why: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set: {why}")
    return value


def _source():
    from provisa.core.models import Source, SourceType

    return Source(
        id=_SOURCE_ID,
        type=SourceType.govdata,
        username=_required("ASKAMERICA_API_KEY", "the AskAmerica source needs a real key"),
        database=_SCHEMAS,
    )


def _container_reach() -> dict[str, str]:
    """Where the adapter listens and the name the Trino container reaches it by (the module
    docstring's Reachability)."""
    from provisa.federation import pgwire_replica as pr

    if sys.platform == "darwin":
        return {pr.ENGINE_HOST_ENV: "host.docker.internal"}
    gateway = subprocess.run(
        [
            "docker",
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {pr.BIND_HOST_ENV: gateway, pr.ENGINE_HOST_ENV: gateway}


@pytest.fixture(scope="module")
def served(tmp_path_factory):
    """The source's server, bound where the Trino container reaches it; yields its state dir."""
    from provisa.federation import pgwire_replica as pr

    data_dir = tmp_path_factory.mktemp("askamerica-trino-state")
    reached = _container_reach()
    previous = {
        name: os.environ.get(name)
        for name in ("PROVISA_DATA_DIR", pr.BIND_HOST_ENV, pr.ENGINE_HOST_ENV)
    }
    os.environ["PROVISA_DATA_DIR"] = str(data_dir)
    os.environ.update(reached)
    source = _source()
    try:
        with deployment_org_on_this_node():
            _cold_start(source, pr)
        yield source, data_dir
    finally:
        with as_deployment_org():
            pr.stop_endpoint(source.id)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _adapter_log(data_dir: Path) -> str:
    logs = sorted(Path(data_dir).rglob("pgwire-server.log"))
    if not logs:
        return "(the adapter wrote no log)"
    return "\n".join(logs[-1].read_text(errors="replace").splitlines()[-80:])


@contextlib.contextmanager
def _step(what: str, sql: str, data_dir: Path):
    """Name the statement a failure happened on and attach the end of the adapter's log."""
    try:
        yield
    except Exception as exc:  # noqa: BLE001 - re-raised with the statement and the adapter's log
        raise AssertionError(
            f"{what} failed on:\n  {sql}\n{type(exc).__name__}: {exc}\n"
            f"--- adapter log (end) ---\n{_adapter_log(data_dir)}"
        ) from exc


@pytest.fixture(scope="module")
def catalog(served):
    """A Trino ``postgresql`` catalog over the adapter, built from the product's own connector."""
    import trino.dbapi

    from provisa.federation.trino_connectors import TrinoGovdataConnector

    source, data_dir = served
    props = TrinoGovdataConnector().details(source)
    with_clause = ", ".join(f"\"{k}\" = '{v}'" for k, v in props.items())
    create = f"CREATE CATALOG {_CATALOG} USING postgresql WITH ({with_clause})"
    conn = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="provisa")
    cur = conn.cursor()
    cur.execute(f"DROP CATALOG IF EXISTS {_CATALOG}")
    cur.fetchall()
    with _step("creating the catalog", create, data_dir):
        cur.execute(create)
        cur.fetchall()
    try:
        yield cur, data_dir
    finally:
        cur.execute(f"DROP CATALOG IF EXISTS {_CATALOG}")
        cur.fetchall()
        conn.close()


def _run(cur, what: str, sql: str, data_dir: Path) -> list:
    with _step(what, sql, data_dir):
        cur.execute(sql)
        return cur.fetchall()


def test_trino_reads_an_askamerica_table_live_and_joins_it(catalog):
    cur, data_dir = catalog

    schemas = {
        r[0] for r in _run(cur, "listing schemas", f"SHOW SCHEMAS FROM {_CATALOG}", data_dir)
    }
    assert _SCHEMA in schemas, schemas
    assert schemas - {"information_schema", "pg_catalog"} <= set(_SCHEMAS.split(",")), schemas

    tables = {
        r[0]
        for r in _run(cur, "listing tables", f"SHOW TABLES FROM {_CATALOG}.{_SCHEMA}", data_dir)
    }
    assert _TABLE in tables, tables

    columns = _run(cur, "describing the table", f"DESCRIBE {_CATALOG}.{_SCHEMA}.{_TABLE}", data_dir)
    assert columns, "the table has no columns Trino could map"

    rows = _run(
        cur,
        "reading the table alone",
        f"SELECT * FROM {_CATALOG}.{_SCHEMA}.{_TABLE} LIMIT 5",
        data_dir,
    )
    assert len(rows) == 5

    first = columns[0][0]
    filtered = _run(
        cur,
        "a pushed-down predicate",
        f'SELECT count(*) FROM {_CATALOG}.{_SCHEMA}.{_TABLE} WHERE "{first}" IS NOT NULL',
        data_dir,
    )
    assert filtered[0][0] >= 0

    joined = _run(
        cur,
        "joining with a table of another catalog",
        f"SELECT count(*) FROM (SELECT * FROM {_CATALOG}.{_SCHEMA}.{_TABLE} LIMIT 5) c "
        "CROSS JOIN (SELECT node_id FROM system.runtime.nodes LIMIT 1) n",
        data_dir,
    )
    assert joined[0][0] == 5
