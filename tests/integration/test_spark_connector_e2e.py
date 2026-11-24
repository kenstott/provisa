# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0b3a97-52fe-4c1e-8f44-7ac9e2d10b53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Spark connector against real Spark (REQ-2225): a Spark Connect server in a container, the
connector's own project and the Spark Connect client from its lock, joined with an annotated
script. A table read, a command and a @stream each run on the cluster.

REQ-2270: a Provisa server joins the connector as a remote host, and a command given a governed
table with a row rule reads it over Arrow Flight SQL as the calling role, so only the rows the rule
allows reach the cluster. The server runs no minted-token pgwire listener: a read that took that
path would be refused naming server.minted_pgwire_port.

The server matches the client the connector's uv.lock pins, so a client/server version difference
cannot pass or fail this test.
"""

# Requirements: REQ-2225, REQ-2270

from __future__ import annotations

import datetime
import ipaddress
import json
import os
import shutil
import socket
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml

from provisa.codehost import front
from provisa.codesource import remote, remote_sources
from provisa.codesource.bundle import content_hash
from provisa.codesource.lifecycle import setup_values
from provisa.codesource.manifest import (
    Export,
    Manifest,
    Requirement,
    manifest_from_json,
    manifest_hash,
    manifest_json,
)
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.python.source import HOST

pytestmark = [pytest.mark.integration]

_PROJECT = Path(__file__).resolve().parents[2] / "connectors" / "spark"
#: The cluster, at the version of the client in connectors/spark/uv.lock.
_SPARK_IMAGE = "apache/spark:4.3.0"
_CONNECT_PORT = 15002
_SERVER_START_S = 240

_SCRIPT = textwrap.dedent(
    '''
    import time
    from collections.abc import Iterator
    from dataclasses import dataclass

    from provisa_spark import session


    @dataclass
    class Number:
        id: int
        label: str


    @dataclass
    class Stamp:
        day: str


    @dataclass
    class Tick:
        value: int


    def numbers() -> list[Number]:
        """Three numbers, computed by the cluster.

        @table
        """
        frame = session().sql("SELECT id, concat('n', id) AS label FROM range(3) ORDER BY id")
        return [Number(**row.asDict()) for row in frame.collect()]


    def stamp(day: str) -> str:
        """Leaves a view on the cluster, read back by stamped().

        @command
        """
        session().sql(f"CREATE OR REPLACE TEMP VIEW stamped_day AS SELECT '{day}' AS day")
        return day


    def stamped() -> list[Stamp]:
        """What the last stamp left on the cluster.

        @table
        """
        return [Stamp(**row.asDict()) for row in session().sql("SELECT day FROM stamped_day").collect()]


    def ticks() -> Iterator[Tick]:
        """One streaming query on the cluster for this subscription, stopped when it ends.

        @stream
        """
        query = (
            session()
            .readStream.format("rate")
            .option("rowsPerSecond", 10)
            .load()
            .selectExpr("value")
            .writeStream.format("memory")
            .queryName("ticks")
            .start()
        )
        seen = set()
        try:
            while True:
                for row in session().sql("SELECT value FROM ticks ORDER BY value").collect():
                    if row.value not in seen:
                        seen.add(row.value)
                        yield Tick(value=row.value)
                time.sleep(0.2)
        finally:
            query.stop()
    '''
)

#: Reads one row through the client, so the wait below ends only when the server answers a query.
_PROBE = "from pyspark.sql import SparkSession; import sys; " \
    "s = SparkSession.builder.remote(sys.argv[1]).getOrCreate(); print(s.sql('SELECT 1').collect())"  # fmt: skip

_PROJECT_MANIFEST = manifest_json(
    Manifest(
        exports=(Export("declared", "on-start", "", (), None),),
        types={},
        requires=(Requirement("script", False), Requirement("cluster", False)),
    )
)


def _free_port() -> int:
    from tests.port_lease import lease_ports

    (port,) = lease_ports(1)
    return port


def _pyspark_home(python: Path) -> Path | None:
    """Return the pyspark package directory inside the given venv, or None if absent.

    ``pyspark-client`` is a Connect-only wheel that ships without a full Spark
    distribution.  ``pyspark/__init__.py`` calls ``find_spark_home()`` at import
    time, which looks for a ``spark-distribution`` subdirectory inside the package
    or for a ``SPARK_HOME`` env-var pointing at a real install.  Neither is
    present by default after an sdist install, so every ``import pyspark.sql``
    fails with "Could not find valid SPARK_HOME" — even though the Connect server
    is already running and no JVM is ever needed.

    Callers must:
    1. Create ``<pyspark_home>/spark-distribution/`` as an empty placeholder so
       that ``find_spark_home()`` succeeds when SPARK_HOME is not set.
    2. Pass ``SPARK_HOME=<pyspark_home>`` in subprocess environments so that any
       pre-existing (and potentially wrong) SPARK_HOME on the runner is overridden.
    """
    dirs = sorted((python.parent.parent / "lib").glob("python*/site-packages/pyspark"))
    return dirs[0] if dirs else None


@pytest.fixture(scope="module")
def client_python(tmp_path_factory) -> Path:
    """A Python holding the connector's locked dependencies: the real Spark Connect client."""
    uv = shutil.which("uv")
    assert uv, "uv is on the PATH of a test run"
    work = tmp_path_factory.mktemp("spark-client")
    requirements = work / "requirements.txt"
    subprocess.run(
        [uv, "export", "--project", str(_PROJECT), "--frozen", "--no-emit-project", "--format",
         "requirements-txt", "--output-file", str(requirements)],
        check=True,
    )  # fmt: skip
    venv = work / "venv"
    subprocess.run([uv, "venv", "--python", "3.12", str(venv)], check=True)
    python = venv / "bin" / "python"
    subprocess.run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(python),
            "--require-hashes",
            "-r",
            str(requirements),
        ],
        check=True,
    )
    # pyspark-client installed from sdist does not create spark-distribution/.
    # Create the empty placeholder so pyspark's find_spark_home() does not raise
    # even when SPARK_HOME is unset (belt) — callers also pass SPARK_HOME
    # explicitly to override any wrong value already in the runner env (suspenders).
    home = _pyspark_home(python)
    if home is not None:
        (home / "spark-distribution").mkdir(exist_ok=True)
    return python


@pytest.fixture(scope="module")
def cluster(client_python):
    """A single-node Spark Connect server on a port of this run; its Spark Connect address."""
    port = _free_port()
    name = f"provisa-itest-spark-{os.getpid()}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--memory", "3g", "--name", name,
         "-e", "SPARK_NO_DAEMONIZE=1", "-p", f"127.0.0.1:{port}:{_CONNECT_PORT}",
         _SPARK_IMAGE, "/opt/spark/sbin/start-connect-server.sh"],
        check=True,
        capture_output=True,
    )  # fmt: skip
    address = f"sc://127.0.0.1:{port}"
    # pyspark-client needs SPARK_HOME even for Connect-only sessions (see _pyspark_home).
    # Explicitly set it here so any wrong SPARK_HOME already in the runner env is overridden.
    _phome = _pyspark_home(client_python)
    _probe_env = {**os.environ, "SPARK_HOME": str(_phome)} if _phome is not None else None
    try:
        deadline = time.monotonic() + _SERVER_START_S
        while True:
            probe = subprocess.run(
                [str(client_python), "-c", _PROBE, address],
                capture_output=True,
                text=True,
                env=_probe_env,
            )
            if probe.returncode == 0:
                break
            if time.monotonic() > deadline:
                logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
                raise AssertionError(
                    f"Spark Connect did not answer within {_SERVER_START_S}s\n{probe.stderr}\n"
                    f"{logs.stdout[-4000:]}{logs.stderr[-4000:]}"
                )
            time.sleep(3)
        yield address
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


class _Host:
    """The connector's front end and host, running its own code on the client's Python."""

    def __init__(self, work: Path, python: Path, extra_env: dict[str, str] | None = None) -> None:
        self.work = work
        code = work / "code"
        code.mkdir()
        shutil.copytree(_PROJECT / "provisa_spark", code / "provisa_spark")
        self.hashed = content_hash(code)
        (work / "names.json").write_text("{}")
        (work / "manifest.json").write_text(json.dumps(_PROJECT_MANIFEST))
        # pyspark-client needs SPARK_HOME even for Connect-only sessions (see _pyspark_home).
        # Explicitly set it so any wrong SPARK_HOME in the runner env is overridden and the
        # user script's `import pyspark.sql` succeeds when the front spawns it via `python`.
        _phome = _pyspark_home(python)
        env = {
            **os.environ,
            "PROVISA_CODE_HASH": self.hashed,
            "PROVISA_DISCOVERED": "{}",
            "PROVISA_MANIFEST_FILE": str(work / "manifest.json"),
            "PROVISA_CONNECTOR_SCRIPT": "script",
            **({"SPARK_HOME": str(_phome)} if _phome is not None else {}),
            **(extra_env or {}),
        }
        self.process = subprocess.Popen(  # noqa: S603 -- the test's interpreter and Provisa's front
            [
                sys.executable,
                front.__file__,
                "0",
                str(work / "state"),
                str(work / "spill"),
                str(work / "names.json"),
                *[str(python), "-I", str(HOST), "{fd}", str(code), "provisa_spark"],
            ],
            env=env,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert self.process.stderr is not None
        self.pairing, self.port = "", 0
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not self.port:
            line = self.process.stderr.readline()
            if line.startswith("pairing code: "):
                self.pairing = line.removeprefix("pairing code: ").strip()
            elif line.startswith("remote host listening on "):
                self.port = int(line.rsplit(" ", 1)[1])
        assert self.port, "the host did not start"

    def stop(self) -> None:
        self.process.terminate()
        self.process.wait(timeout=15)


@pytest.fixture
def host(tmp_path, client_python):
    started = _Host(tmp_path, client_python)
    yield started
    started.stop()


@pytest.fixture
def joined(host, cluster, monkeypatch):
    """The source's mapping and identity, once the host has joined with the script. The connector's
    block and the image's labels are stood in for: an image in a registry is not this test's point."""
    identity = remote.new_identity()
    row = {
        "name": "spark",
        "git": "https://git.example/spark.git",
        "ref": "main",
        "commit": "a" * 40,
        "language": "python",
        "image": "registry.example/connectors/spark",
        "digest": "sha256:" + "b" * 64,
        "code_hash": host.hashed,
        "arch": "amd64",
        "requires": [
            {"name": "script", "multiline": True, "script": True},
            {"name": "cluster"},
        ],
    }
    labels = {
        "code_hash": host.hashed,
        "manifest_hash": manifest_hash(_PROJECT_MANIFEST),
        "runtime": "python",
    }

    async def connector(_name):
        return row

    async def image_labels(_state, _org_id, _name, _row):
        return labels

    async def org_identity(_org_id):
        return identity

    monkeypatch.setattr(remote_sources, "_connector", connector)
    monkeypatch.setattr(remote_sources, "_image_labels", image_labels)
    monkeypatch.setattr(remote_sources, "identity", org_identity)
    token = set_current_org("acme")
    yield (
        {
            "connector_join": {
                "connector": "spark",
                "host": "127.0.0.1",
                "port": host.port,
                "pairing_code": host.pairing,
            },
            "settings": {"script": _SCRIPT, "cluster": cluster},
        },
        identity,
    )
    reset_current_org(token)


async def _started_worker(state: tuple) -> tuple:
    mapping, identity = state
    await remote_sources.join_connector(
        SimpleNamespace(admin_db=None), "spark_prod", "spark", mapping
    )
    manifest = manifest_from_json(mapping["remote"]["manifest"])
    worker = remote_sources.worker_for(
        "acme",
        "spark_prod",
        mapping["content_hash"],
        remote_sources.remote_of(mapping),
        identity,
        setup=setup_values("acme", "spark_prod", manifest, mapping, None),
    )
    worker.start()
    worker.ready()
    return worker, mapping["remote"]["names"]


async def test_a_table_is_read_from_the_cluster(joined):
    worker, names = await _started_worker(joined)
    try:
        rows = [
            row
            for batch in worker.rows("numbers", "table", [], [], schema=names["numbers"]["schema"])
            for row in batch
        ]
    finally:
        worker.stop()
    assert rows == [
        {"id": 0, "label": "n0"},
        {"id": 1, "label": "n1"},
        {"id": 2, "label": "n2"},
    ]


async def test_a_command_runs_on_the_cluster(joined):
    worker, names = await _started_worker(joined)
    try:
        assert worker.command("stamp", ["2026-11-09"], ["day"]) == "2026-11-09"
        rows = [
            row
            for batch in worker.rows("stamped", "table", [], [], schema=names["stamped"]["schema"])
            for row in batch
        ]
    finally:
        worker.stop()
    assert rows == [{"day": "2026-11-09"}]


async def test_a_stream_is_a_streaming_query_on_the_cluster(joined):
    worker, names = await _started_worker(joined)
    seen: list[int] = []
    deadline = time.monotonic() + 120
    stream = worker.rows("ticks", "stream", [], [], schema=names["ticks"]["schema"])
    try:
        for batch in stream:
            seen.extend(row["value"] for row in batch)
            if len(seen) >= 5 or time.monotonic() > deadline:
                break
    finally:
        stream.close()
        worker.stop()
    assert len(seen) >= 5, seen
    assert seen == sorted(seen) and len(set(seen)) == len(seen)


# -- REQ-2270: a command's governed table argument, read over Arrow Flight SQL --------------------

#: A role the row rule narrows; org_admin, which the rule does not name, reads every row.
_ANALYST = "clip_analyst"
_ADMIN = "org_admin"

_LEDGER = {
    "ledger/ledger.py": '''
from dataclasses import dataclass


@dataclass
class Clip:
    id: int
    region: str


def clips() -> list[Clip]:
    """@table"""
    return [Clip(1, "emea"), Clip(2, "amer"), Clip(3, "apac")]
''',
}

_TABLE_SCRIPT = textwrap.dedent(
    '''
    from dataclasses import dataclass

    from provisa_services import TableRef
    from provisa_spark import dataframe


    @dataclass
    class Clip:
        id: int
        region: str


    def clips_on_the_cluster(clips: TableRef[Clip]) -> list[Clip]:
        """The clips the caller may read, as a DataFrame on the cluster.

        @query
        """
        frame = dataframe(clips).orderBy("id")
        return [Clip(id=row.id, region=row.region) for row in frame.collect()]
    '''
)


def _lan_address() -> str:
    """This machine's address on its network: a remote host reaches Provisa by it, and a host name
    on loopback is refused for one (table_reads)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect(("192.0.2.1", 9))  # TEST-NET-1: nothing is sent; the route names the address
        address = probe.getsockname()[0]
    assert not address.startswith("127."), f"no network address to serve Flight on: {address}"
    return address


def _certificate(directory: Path, address: str) -> tuple[Path, Path]:
    """A self-signed certificate for ``address``, and its key: the Flight server's TLS, trusted by
    the remote host as its root."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, address)])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(address))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = directory / "flight.crt", directory / "flight.key"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


def _column(name: str, data_type: str) -> str:
    return f'{{name: "{name}", visibleTo: ["{_ADMIN}", "{_ANALYST}"], dataType: "{data_type}"}}'


@pytest.fixture(scope="module")
def flight_tls(tmp_path_factory):
    address = _lan_address()
    cert, key = _certificate(tmp_path_factory.mktemp("flight-tls"), address)
    return address, cert, key


@pytest.fixture(scope="module")
def spark_host(tmp_path_factory, client_python, flight_tls):
    """The connector's host, trusting the Flight server's certificate as its root."""
    _, cert, _ = flight_tls
    started = _Host(
        tmp_path_factory.mktemp("spark-host"),
        client_python,
        {"GRPC_DEFAULT_SSL_ROOTS_FILE_PATH": str(cert)},
    )
    yield started
    started.stop()


@pytest.fixture(scope="module")
def provisa_server(tmp_path_factory, spark_host, flight_tls):
    """A Provisa server serving Arrow Flight over TLS at this machine's address, the connector's
    block and image labels stood in for (tests/integration/spark_connector_app.py)."""
    from tests.integration.isolated_server import IsolatedServer
    from tests.integration.test_code_sources_e2e import _config

    address, cert, key = flight_tls
    work = tmp_path_factory.mktemp("spark-flight-server")
    config = _config()
    config["roles"] = [
        {
            "id": _ANALYST,
            "description": "Reads the clips its row rule allows.",
            "parents": ["data_writer"],
            "capabilities": ["usage", "query_development", "command_execution", "full_results"],
            "domain_access": ["*"],
        }
    ]
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    row = {
        "name": "spark",
        "git": "https://git.example/spark.git",
        "ref": "main",
        "commit": "a" * 40,
        "language": "python",
        "image": "registry.example/connectors/spark",
        "digest": "sha256:" + "b" * 64,
        "code_hash": spark_host.hashed,
        "arch": "amd64",
        "requires": [{"name": "script", "multiline": True, "script": True}, {"name": "cluster"}],
    }
    labels = {
        "code_hash": spark_host.hashed,
        "manifest_hash": manifest_hash(_PROJECT_MANIFEST),
        "runtime": "python",
    }
    srv = IsolatedServer(
        "spark_flight",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        await_flight=True,
        app="tests.integration.spark_connector_app:app",
        env={
            "PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps")),
            "PROVISA_HOSTNAME": address,
            "PROVISA_FLIGHT_CERT": str(cert),
            "PROVISA_FLIGHT_KEY": str(key),
            "PROVISA_TEST_SPARK_CONNECTOR": json.dumps(row),
            "PROVISA_TEST_SPARK_LABELS": json.dumps(labels),
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


@pytest.fixture(scope="module")
def clips_command(provisa_server, spark_host, cluster):
    """The governed table ``fx.clips`` (a Python source; the analyst's row rule allows clip 1) and
    the Spark command taking it, the connector joined with the server."""
    from tests.integration.test_code_sources_e2e import _admin, _archive

    srv = provisa_server
    upload = httpx.post(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-uploads/archive",
        params={"filename": "ledger.zip"},
        content=_archive(_LEDGER),
        headers={"X-Provisa-Role": _ADMIN, "Content-Type": "application/octet-stream"},
        timeout=srv.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    ledger = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        srv,
        f'createSource(input: {{id: "ledger", type: "python", host: "", port: 0, database: "", '
        f"cacheTtl: 300, mappingJson: {json.dumps(json.dumps(ledger))}}})",
    )
    columns = ", ".join([_column("id", "bigint"), _column("region", "text")])
    _admin(
        srv,
        'registerTable(input: {sourceId: "ledger", domainId: "fx", schemaName: "default", '
        f'tableName: "clips", cacheTtl: 300, columns: [{columns}]}})',
    )
    _admin(
        srv,
        f'upsertRlsRule(input: {{tableId: "clips", roleId: "{_ANALYST}", filterExpr: "id = 1", '
        'description: "The analyst reads clip 1 only."})',
    )
    joining = {
        "connector_join": {
            "connector": "spark",
            "host": "127.0.0.1",
            "port": spark_host.port,
            "pairing_code": spark_host.pairing,
        },
        "settings": {"script": _TABLE_SCRIPT, "cluster": cluster},
    }
    _admin(
        srv,
        f'createSource(input: {{id: "spark_jobs", type: "spark", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(joining))}}})',
    )
    response = httpx.post(
        f"{srv.base_url}/admin/actions/functions",
        json={
            "name": "clips_on_the_cluster",
            "sourceId": "spark_jobs",
            "functionName": "clips_on_the_cluster",
            "domainId": "fx",
            "visibleTo": [_ADMIN, _ANALYST],
        },
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    return srv


def _clips_as(srv, role: str) -> list[dict]:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={
            "sql": "SELECT c.id, c.region FROM clips_on_the_cluster('fx.clips') AS c ORDER BY c.id"
        },
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 300,
    )
    assert response.status_code == 200, response.text
    return [{"id": int(r["id"]), "region": r["region"]} for r in response.json()["data"]["sql"]]


def test_a_spark_command_reads_its_governed_table_over_flight_as_the_calling_role(clips_command):
    assert _clips_as(clips_command, _ANALYST) == [{"id": 1, "region": "emea"}]
    assert _clips_as(clips_command, _ADMIN) == [
        {"id": 1, "region": "emea"},
        {"id": 2, "region": "amer"},
        {"id": 3, "region": "apac"},
    ]
