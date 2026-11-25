# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0b3a97-52fe-4c1e-8f44-7ac9e2d10b53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python code source reads through a Spark source's session on real Spark (REQ-2225, amended
2026-11-25; REQ-2270).

A Spark source has no script: it is a connection to the cluster, joined to the Spark connector's
host with its cluster and nothing else. Logic the cluster does not hold is an ordinary Python code
source, which opens the Spark source's session with ``provisa_services.spark.session(<source>)``
and reads a governed table on the cluster with ``provisa_services.spark.dataframe``.

- The Python source's @table computes its rows on the cluster through the session.
- Its @query is given the governed table ``fx.clips`` (another Python source; the analyst's row
  rule allows clip 1), read over Arrow Flight SQL as the calling role and handed to the cluster:
  the analyst's call sees one row, org_admin's all three.

The cluster is a one-node Spark Connect server in a container, on this machine's network address:
the Python source's worker reaches it through its egress proxy, which never connects to loopback,
and the server's egress allow-list names it. The Spark Connect client is the one the connector's
uv.lock pins, built as its wheel (connectors/spark/_build_client.py) and shipped in the Python
source's archive with a hashed requirements.txt, so client and server versions match.

Lands on the TEST instance only: a server of this run with its own control plane, and a Spark
container of this run on a leased port."""

# Requirements: REQ-2225, REQ-2270

from __future__ import annotations

import importlib.util
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import textwrap
import time
import zipfile
from pathlib import Path

import httpx
import pytest
import yaml

from provisa.codehost import front
from provisa.codesource.bundle import content_hash
from provisa.codesource.manifest import PYTHON_RULES, manifest_hash, manifest_json, read_manifest
from provisa.python.source import HOST, READER

pytestmark = [pytest.mark.integration]

_PROJECT = Path(__file__).resolve().parents[2] / "connectors" / "spark"
#: The cluster, at the version of the client in connectors/spark/uv.lock.
_SPARK_IMAGE = "apache/spark:4.3.0"
_CONNECT_PORT = 15002
_SERVER_START_S = 240

#: Reads one row through the client, so the wait below ends only when the server answers a query.
_PROBE = "from pyspark.sql import SparkSession; import sys; " \
    "s = SparkSession.builder.remote(sys.argv[1]).getOrCreate(); print(s.sql('SELECT 1').collect())"  # fmt: skip

#: A role the row rule narrows; org_admin, which the rule does not name, reads every row.
_ANALYST = "clip_analyst"
_ADMIN = "org_admin"
_SPARK_SOURCE = "spark_jobs"

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

_LAKEHOUSE = textwrap.dedent(
    f'''
    from dataclasses import dataclass

    import provisa_services as ps
    from provisa_services import TableRef


    @dataclass
    class Number:
        id: int
        label: str


    @dataclass
    class Clip:
        id: int
        region: str


    def numbers() -> list[Number]:
        """Three numbers, computed by the Spark source's cluster.

        @table
        """
        spark = ps.spark.session("{_SPARK_SOURCE}")
        frame = spark.sql("SELECT id, concat('n', id) AS label FROM range(3) ORDER BY id")
        return [Number(**row.asDict()) for row in frame.collect()]


    def clips_on_the_cluster(clips: TableRef[Clip]) -> list[Clip]:
        """The clips the caller may read, as a DataFrame on the Spark source's cluster.

        @query
        """
        spark = ps.spark.session("{_SPARK_SOURCE}")
        frame = ps.spark.dataframe(spark, clips).orderBy("id")
        return [Clip(id=row.id, region=row.region) for row in frame.collect()]
    '''
)


def _free_port() -> int:
    from tests.port_lease import lease_ports

    (port,) = lease_ports(1)
    return port


def _lan_address() -> str:
    """This machine's address on its network: a code source's egress proxy never connects to
    loopback, so the cluster is published here."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect(("192.0.2.1", 9))  # TEST-NET-1: nothing is sent; the route names the address
        address = probe.getsockname()[0]
    assert not address.startswith("127."), f"no network address to publish Spark on: {address}"
    return address


def _pyspark_home(python: Path) -> Path | None:
    """The pyspark package directory inside the given venv, or None if absent. pyspark-client
    installed from its sdist has no ``spark-distribution`` directory, which pyspark's
    find_spark_home() looks for when SPARK_HOME is unset; the probe below is given SPARK_HOME so a
    wrong one already in the runner's environment is overridden."""
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
        [uv, "pip", "install", "--python", str(python), "--require-hashes", "-r",
         str(requirements)],
        check=True,
    )  # fmt: skip
    home = _pyspark_home(python)
    if home is not None:
        (home / "spark-distribution").mkdir(exist_ok=True)
    return python


@pytest.fixture(scope="module")
def cluster(client_python):
    """A single-node Spark Connect server on this machine's network address; its address."""
    lan = _lan_address()
    port = _free_port()
    name = f"provisa-itest-spark-{os.getpid()}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--memory", "3g", "--name", name,
         "-e", "SPARK_NO_DAEMONIZE=1", "-p", f"{lan}:{port}:{_CONNECT_PORT}",
         _SPARK_IMAGE, "/opt/spark/sbin/start-connect-server.sh"],
        check=True,
        capture_output=True,
    )  # fmt: skip
    address = f"sc://{lan}:{port}"
    home = _pyspark_home(client_python)
    probe_env = {**os.environ, "SPARK_HOME": str(home)} if home is not None else None
    try:
        deadline = time.monotonic() + _SERVER_START_S
        while True:
            probe = subprocess.run(
                [str(client_python), "-c", _PROBE, address],
                capture_output=True,
                text=True,
                env=probe_env,
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
        yield lan, address
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def _project_manifest() -> dict:
    """The manifest the connector's image carries: its own code's, read as a build reads it."""
    done = subprocess.run(  # noqa: S603 -- Provisa's own reader, on the connector's own project
        [sys.executable, "-I", str(READER), str(_PROJECT), "provisa_spark"],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONPATH": ""},
    )
    return manifest_json(read_manifest(json.loads(done.stdout), PYTHON_RULES))


class _Host:
    """The connector's front end and host, running its own code."""

    def __init__(self, work: Path, python: Path, manifest: dict) -> None:
        code = work / "code"
        code.mkdir()
        shutil.copytree(_PROJECT / "provisa_spark", code / "provisa_spark")
        self.hashed = content_hash(code)
        (work / "names.json").write_text("{}")
        (work / "manifest.json").write_text(json.dumps(manifest))
        env = {
            **os.environ,
            "PROVISA_CODE_HASH": self.hashed,
            "PROVISA_DISCOVERED": "{}",
            "PROVISA_MANIFEST_FILE": str(work / "manifest.json"),
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


@pytest.fixture(scope="module")
def project_manifest() -> dict:
    return _project_manifest()


@pytest.fixture(scope="module")
def spark_host(tmp_path_factory, client_python, project_manifest):
    started = _Host(tmp_path_factory.mktemp("spark-host"), client_python, project_manifest)
    yield started
    started.stop()


@pytest.fixture(scope="module")
def client_wheel(tmp_path_factory) -> tuple[str, dict[str, bytes]]:
    """The Spark Connect client the connector pins, as a Python source ships it: its wheel and a
    requirements.txt naming it and every dependency by hash (connectors/spark/_build_client.py)."""
    project = tmp_path_factory.mktemp("spark-client-project") / "spark"
    shutil.copytree(_PROJECT, project, ignore=shutil.ignore_patterns("wheels", ".venv"))
    spec = importlib.util.spec_from_file_location("_build_client", _PROJECT / "_build_client.py")
    assert spec is not None and spec.loader is not None
    build_client = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(build_client)
    wheel = build_client.build(project, None)
    return (project / "requirements.txt").read_text(), {f"wheels/{wheel.name}": wheel.read_bytes()}


def _archive(files: dict[str, str | bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    return buf.getvalue()


def _admin(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ADMIN},
        # Saving a code source fetches its runtime and installs and reads its code.
        timeout=srv.request_timeout + 900,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _upload(srv, filename: str, data: bytes) -> str:
    upload = httpx.post(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-uploads/archive",
        params={"filename": filename},
        content=data,
        headers={"X-Provisa-Role": _ADMIN, "Content-Type": "application/octet-stream"},
        timeout=srv.request_timeout + 60,
    )
    assert upload.status_code == 200, upload.text
    return upload.json()["content_hash"]


def _python_source(srv, source_id: str, data: bytes, filename: str) -> None:
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": _upload(srv, filename, data)},
    }
    _admin(
        srv,
        f'createSource(input: {{id: "{source_id}", type: "python", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )


def _column(name: str, data_type: str) -> str:
    return f'{{name: "{name}", visibleTo: ["{_ADMIN}", "{_ANALYST}"], dataType: "{data_type}"}}'


def _register(srv, source_id: str, table: str, columns: list[tuple[str, str]]) -> None:
    listed = ", ".join(_column(n, t) for n, t in columns)
    _admin(
        srv,
        f'registerTable(input: {{sourceId: "{source_id}", domainId: "fx", schemaName: "default", '
        f'tableName: "{table}", cacheTtl: 300, columns: [{listed}]}})',
    )


@pytest.fixture(scope="module")
def provisa_server(tmp_path_factory, spark_host, project_manifest, cluster):
    """A Provisa server whose code sources' egress allow-list names the cluster's host, the
    connector's block and image labels stood in for (tests/integration/spark_connector_app.py)."""
    from tests.integration.isolated_server import IsolatedServer
    from tests.integration.test_code_sources_e2e import _config

    lan, _ = cluster
    work = tmp_path_factory.mktemp("spark-session-server")
    config = _config()
    config["server"] = {"udf_egress_allowlist": [lan]}
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
        # As connectors/spark/connector.yaml states it: left unset, the org's default cluster.
        "requires": [{"name": "cluster", "org_default": "spark_cluster"}],
    }
    labels = {
        "code_hash": spark_host.hashed,
        "manifest_hash": manifest_hash(project_manifest),
        "runtime": "python",
    }
    srv = IsolatedServer(
        "spark_session",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        await_flight=True,
        app="tests.integration.spark_connector_app:app",
        env={
            "PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps")),
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
def lakehouse(provisa_server, spark_host, cluster, client_wheel):
    """The Spark source ``spark_jobs`` (its cluster, no script), the governed table ``fx.clips``
    with the analyst's row rule, and the Python source ``lakehouse`` that opens the Spark source's
    session: its table ``numbers`` registered and its @query saved as a function."""
    srv = provisa_server
    _, address = cluster
    joining = {
        "connector_join": {
            "connector": "spark",
            "host": "127.0.0.1",
            "port": spark_host.port,
            "pairing_code": spark_host.pairing,
        },
        "settings": {"cluster": address},
    }
    _admin(
        srv,
        f'createSource(input: {{id: "{_SPARK_SOURCE}", type: "spark", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(joining))}}})',
    )
    _python_source(srv, "ledger", _archive(dict(_LEDGER)), "ledger.zip")
    _register(srv, "ledger", "clips", [("id", "bigint"), ("region", "text")])
    _admin(
        srv,
        f'upsertRlsRule(input: {{tableId: "clips", roleId: "{_ANALYST}", filterExpr: "id = 1", '
        'description: "The analyst reads clip 1 only."})',
    )
    requirements, wheels = client_wheel
    files: dict[str, str | bytes] = {
        "lakehouse.py": _LAKEHOUSE,
        "requirements.txt": requirements,
        **wheels,
    }
    _python_source(srv, "lakehouse", _archive(files), "lakehouse.zip")
    _register(srv, "lakehouse", "numbers", [("id", "bigint"), ("label", "text")])
    response = httpx.post(
        f"{srv.base_url}/admin/actions/functions",
        json={
            "name": "clips_on_the_cluster",
            "sourceId": "lakehouse",
            "functionName": "clips_on_the_cluster",
            "domainId": "fx",
            "visibleTo": [_ADMIN, _ANALYST],
        },
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    return srv


def _sql(srv, role: str, sql: str) -> list[dict]:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 300,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "errors" not in body, body
    return body["data"]["sql"]


def test_a_python_source_reads_through_the_spark_sources_session(lakehouse):
    rows = _sql(lakehouse, _ADMIN, 'SELECT id, label FROM "fx"."numbers" ORDER BY id')
    assert [{"id": int(r["id"]), "label": r["label"]} for r in rows] == [
        {"id": 0, "label": "n0"},
        {"id": 1, "label": "n1"},
        {"id": 2, "label": "n2"},
    ]


def _clips_as(srv, role: str) -> list[dict]:
    rows = _sql(
        srv,
        role,
        "SELECT c.id, c.region FROM clips_on_the_cluster('fx.clips') AS c ORDER BY c.id",
    )
    return [{"id": int(r["id"]), "region": r["region"]} for r in rows]


def test_a_governed_table_reaches_the_cluster_as_the_calling_role(lakehouse):
    assert _clips_as(lakehouse, _ANALYST) == [{"id": 1, "region": "emea"}]
    assert _clips_as(lakehouse, _ADMIN) == [
        {"id": 1, "region": "emea"},
        {"id": 2, "region": "amer"},
        {"id": 3, "region": "apac"},
    ]
