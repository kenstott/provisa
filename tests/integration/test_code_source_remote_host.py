# Copyright (c) 2026 Kenneth Stott
# Canary: d87b44ad-8747-48f2-b23f-c3e1e3ff8079
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a code source built into an image, run on a remote host, connected and read
(REQ-2098).

The ``test`` instance: an isolated server on DuckDB with its own control plane, as the code
sources e2e runs it. For each language: the dev-mode source's image is built through the API for
this machine's architecture and downloaded; the archive is loaded into Docker and run, publishing
its 7443 on a free loopback port; the pairing code it printed is entered with that host and port,
which switches the source to built mode; its table is then read from the container. The host's
status says it answers, and the source goes back to dev and is read on the server again."""

# Requirements: REQ-2098

from __future__ import annotations

import json
import platform
import re
import socket
import subprocess
import time
from pathlib import Path

import httpx
import pytest

from tests.integration.test_code_sources_e2e import (
    _ROLE,
    _rows,
    _sql,
    code_source,  # noqa: F401 -- the fixture: the source, its tables registered and released
    runtime_cache,  # noqa: F401 -- the fixture
    server,  # noqa: F401 -- the fixture: an isolated server per language
)

pytestmark = [pytest.mark.integration]

_ARCH = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[
    platform.machine().lower()
]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _api(srv, method: str, path: str, **kw) -> httpx.Response:
    return httpx.request(
        method,
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-sources/fx-{srv.language}{path}",
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 900,
        **kw,
    )


@pytest.fixture(scope="module")
def remote_host(server, code_source, tmp_path_factory):  # noqa: F811
    """The source's image, built through the API, loaded and run; its port, its pairing code, its
    image and its container's name."""
    started = _api(server, "POST", "/builds", json={"arch": _ARCH, "download": True})
    assert started.status_code == 200, started.text
    build = started.json()
    deadline = time.monotonic() + 1500
    while build["status"] == "building":
        assert time.monotonic() < deadline, "the build did not finish"
        time.sleep(5)
        build = _api(server, "GET", f"/builds/{build['id']}").json()
    assert build["status"] == "done", build["error"]
    assert "PROVISA_HOST_PORT:?" in build["compose"]
    archive = tmp_path_factory.mktemp("image") / "image.tar"
    downloaded = _api(server, "GET", f"/builds/{build['id']}/archive")
    assert downloaded.status_code == 200, downloaded.text
    archive.write_bytes(downloaded.content)
    loaded = subprocess.run(
        ["docker", "load", "-i", str(archive)], capture_output=True, text=True, check=True
    )
    assert build["image_ref"] in loaded.stdout, loaded.stdout
    labels = json.loads(
        subprocess.run(
            ["docker", "inspect", "--format", "{{json .Config.Labels}}", build["image_ref"]],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )
    assert labels["dev.provisa.codehost"] == "1"
    assert labels["dev.provisa.runtime"] == ("python" if server.language == "python" else "node")
    port = _free_port()
    name = f"provisa-remote-{server.language}-{port}"
    subprocess.run(
        ["docker", "run", "-d", "--name", name, "-p", f"127.0.0.1:{port}:7443", build["image_ref"]],
        capture_output=True,
        text=True,
        check=True,
    )
    try:
        pairing = None
        deadline = time.monotonic() + 120
        while pairing is None and time.monotonic() < deadline:
            logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True).stderr
            found = re.search(r"pairing code: (\S+)", logs)
            if found and "remote host listening" in logs:
                pairing = found.group(1)
            else:
                time.sleep(1)
        assert pairing is not None, subprocess.run(
            ["docker", "logs", name], capture_output=True, text=True
        )
        yield port, pairing, build["image_ref"], name
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "rmi", "-f", build["image_ref"]], capture_output=True)


def _container_log(name: str) -> str:
    shown = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
    state = subprocess.run(
        ["docker", "inspect", "--format", "{{json .State}}", name], capture_output=True, text=True
    )
    return f"{shown.stdout}{shown.stderr}\nstate: {state.stdout or state.stderr}"


def _usd(srv) -> list[dict]:
    return _rows(_sql(srv, "SELECT code, rate FROM rates WHERE _nf_currency = 'USD' ORDER BY code"))


def test_a_built_source_is_read_from_its_remote_host_and_back_on_the_server_in_dev(
    server,  # noqa: F811
    code_source,  # noqa: F811
    remote_host,
):
    port, pairing, _image, name = remote_host
    on_server = _usd(server)
    connected = _api(
        server,
        "POST",
        "/connect",
        json={"host": "127.0.0.1", "port": port, "pairing_code": pairing},
    )
    # The host's own log beside the server's answer: CI keeps neither the isolated server's log
    # nor the container's.
    assert connected.status_code == 200, (connected.text, _container_log(name))
    assert connected.json()["mode"] == "built"
    assert _usd(server) == on_server  # the same rows, served by the container
    status = _api(server, "GET", "/host").json()
    assert status["mode"] == "built" and status["answering"] is True
    # A built source is not connected again: it goes back to dev first.
    again = _api(
        server,
        "POST",
        "/connect",
        json={"host": "127.0.0.1", "port": port, "pairing_code": pairing},
    )
    assert again.status_code == 400
    assert _api(server, "POST", "/dev").json() == {"mode": "dev"}
    assert _usd(server) == on_server


# REQ-2100: what the image's DuckDB is given to spill to, and the statement that fills it.
_SPILL_FILLS = """
import os, sys
sys.path.insert(0, sys.argv[1])
sys.path.insert(1, os.environ["PROVISA_FRONT_LIB"])  # the image's own DuckDB, sqlglot and Arrow
import pyarrow as pa
import residual
residual.configure(memory_mb=128, threads=1, spill_cap_mb=10240, spill_dir="/var/lib/provisa-spill")
schema = pa.schema([("s", pa.utf8())])
def batches():
    for _ in range(400):
        yield pa.RecordBatch.from_arrays([pa.array([os.urandom(48).hex() for _ in range(10000)])], schema=schema)
try:
    for _ in residual.answer("SELECT s FROM t ORDER BY s DESC", "duckdb", "t", pa.RecordBatchReader.from_batches(schema, batches())):
        pass
except residual.SpillCapped as refused:
    print("REFUSED", refused)
    sys.exit(0)
print("ANSWERED")
sys.exit(1)
"""


def test_a_statement_that_fills_the_images_spill_volume_is_refused_naming_the_cap(remote_host):
    """REQ-2100: the image spills to a volume of its own the operator sizes to the cap. Run with
    that volume a 16 MiB tmpfs (docker's own --tmpfs, no privilege), a sort that needs more is
    refused naming codesource.duckdb_spill_cap_mb, never answered short."""
    _port, _pairing, image, _name = remote_host
    config = json.loads(
        subprocess.run(
            ["docker", "inspect", "--format", "{{json .Config}}", image],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )
    python, front = config["Entrypoint"][0], config["Entrypoint"][2]
    run = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--tmpfs",
            "/var/lib/provisa-spill:size=16m,uid=65532,gid=65532",
            "--entrypoint",
            python,
            image,
            "-c",
            _SPILL_FILLS,
            str(Path(front).parent),
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert run.returncode == 0, run.stdout + run.stderr
    assert "REFUSED" in run.stdout and "codesource.duckdb_spill_cap_mb" in run.stdout, run.stdout
