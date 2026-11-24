# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2d9e14-8f37-4a05-c1e9-2d7f4b80a3c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A checkout's demo offers the Spark connector, run natively (REQ-2225, amended 2026-11-24): the
platform read offers it in a checkout's demo, never outside a demo, and a released package reads
its own block unchanged. A native connector is joined by its name alone, and a host that fails to
start fails the source by name."""

# Requirements: REQ-2225

from __future__ import annotations

import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import yaml

from provisa.codehost import front
from provisa.codesource import native_connector, remote_sources
from provisa.codesource.native_connector import NativeHostFailed
from provisa.core.models import ConnectorConfig, ConnectorRequirement
from provisa.core.repositories import connector as connector_repo
from provisa.core.request_context import reset_current_org, set_current_org

pytestmark = pytest.mark.unit

_PROJECT = Path(__file__).resolve().parents[2] / "connectors" / "spark"


@pytest.fixture
def demo(monkeypatch):
    monkeypatch.setenv("PROVISA_DEMO", "true")


@pytest.fixture
def not_demo(monkeypatch):
    monkeypatch.delenv("PROVISA_DEMO", raising=False)


def _released_spark() -> ConnectorConfig:
    return ConnectorConfig(
        name="spark",
        git="https://github.com/kenstott/provisa.git",
        ref="v0.1.0",
        commit="a" * 40,
        language="python",
        image="ghcr.io/kenstott/provisa-spark-connector",
        digest="sha256:" + "b" * 64,
        code_hash="c" * 64,
        arch="amd64",
        requires=[ConnectorRequirement(name="script", multiline=True, script=True)],
    )


async def test_a_checkout_demo_offers_the_spark_connector_run_natively(demo, control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        offered = await connector_repo.offered(conn)
        found = await connector_repo.get(conn, "spark")
    assert [row["name"] for row in offered] == ["spark"]
    (spark,) = offered
    assert spark["native"] is True
    assert spark["language"] == "python"
    assert spark["code_hash"] == native_connector.code_hash(_PROJECT)
    assert spark["requires"] == [
        {"name": "script", "multiline": True, "script": True},
        {"name": "cluster", "org_default": "spark_cluster"},
    ]
    assert spark["settings"] == [{"name": "replicate", "default_value": 0}]
    assert spark["description"].startswith("Tables and commands from one annotated Spark script")
    assert spark["icon"].startswith("data:image/svg+xml;base64,")
    assert spark["image"] == "" and spark["digest"] == ""
    assert "path" not in spark and "build_command" not in spark
    assert found is not None and found["native"] is True


async def test_a_checkout_outside_a_demo_offers_none(not_demo, control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        assert await connector_repo.offered(conn) == []
        assert await connector_repo.get(conn, "spark") is None


async def test_a_released_packages_block_is_read_unchanged(
    demo, control_plane, tmp_path, monkeypatch
):
    block = tmp_path / "platform_connectors.yaml"
    block.write_text(
        yaml.safe_dump({"connectors": [_released_spark().model_dump(exclude_defaults=True)]})
    )
    monkeypatch.setattr(connector_repo, "_PLATFORM_FILE", block)
    # An installed package: no checkout around it.
    monkeypatch.setattr(native_connector, "_ROOT", tmp_path / "site-packages")
    db = control_plane.org()
    async with db.acquire() as conn:
        (spark,) = await connector_repo.offered(conn)
    assert spark["image"] == "ghcr.io/kenstott/provisa-spark-connector"
    assert spark["digest"] == "sha256:" + "b" * 64
    assert "native" not in spark


async def test_a_checkouts_native_spark_never_shadows_a_filled_block(
    demo, control_plane, tmp_path, monkeypatch
):
    block = tmp_path / "platform_connectors.yaml"
    block.write_text(
        yaml.safe_dump({"connectors": [_released_spark().model_dump(exclude_defaults=True)]})
    )
    monkeypatch.setattr(connector_repo, "_PLATFORM_FILE", block)
    db = control_plane.org()
    async with db.acquire() as conn:
        (spark,) = await connector_repo.offered(conn)
    assert "native" not in spark and spark["digest"] == "sha256:" + "b" * 64


# -- the join --------------------------------------------------------------------------------------


@pytest.fixture
def native_row(monkeypatch):
    row = {
        "name": "spark",
        "language": "python",
        "code_hash": "d" * 64,
        "requires": [
            {"name": "script", "multiline": True, "script": True},
            {"name": "cluster", "org_default": "spark_cluster"},
        ],
        "native": True,
    }

    async def connector(_name):
        return row

    monkeypatch.setattr(remote_sources, "_connector", connector)
    token = set_current_org("acme")
    yield row
    reset_current_org(token)


def _joining(**join) -> dict:
    return {
        "connector_join": {"connector": "spark", **join},
        "settings": {"script": "def f():\n    pass\n"},
    }


async def test_a_native_connector_is_joined_by_its_name_alone(native_row):
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await remote_sources.join_connector(
            SimpleNamespace(admin_db=None),
            "spark_jobs",
            "python",
            _joining(host="10.0.0.9", port=7443, pairing_code="fp.secret"),
        )
    assert refused.value.code == "codesource.connector_native"
    assert "host, pairing_code, port" in str(refused.value)


async def test_a_native_host_that_fails_to_start_fails_the_source_by_name(native_row, monkeypatch):
    def start(_org, source_id, _row):
        raise NativeHostFailed(f"the Spark connector's host for {source_id} exited with 3")

    monkeypatch.setattr(native_connector, "start_for_join", start)
    with pytest.raises(NativeHostFailed) as failed:
        await remote_sources.join_connector(
            SimpleNamespace(admin_db=None), "spark_jobs", "python", _joining()
        )
    assert failed.value.code == "codesource.connector_native_host"
    assert failed.value.params == {"connector": "spark"}
    assert "spark_jobs exited with 3" in str(failed.value)


def test_a_native_sources_worker_starts_its_host_first(monkeypatch):
    started = []
    monkeypatch.setattr(native_connector, "ensure_running", lambda *args: started.append(args))
    remote = {"host": "127.0.0.1", "port": 40123, "pin": "p", "native": "script"}
    remote_sources.worker_for("acme", "spark_jobs", "d" * 64, remote, cast(Any, object()))
    assert started == [("acme", "spark_jobs", "d" * 64, "script", 40123)]


def test_an_image_sources_worker_starts_no_host(monkeypatch):
    def refuse(*_args):
        raise AssertionError("an image source's host is its operator's")

    monkeypatch.setattr(native_connector, "ensure_running", refuse)
    remote = {"host": "10.0.0.9", "port": 7443, "pin": "p"}
    remote_sources.worker_for("acme", "spark_jobs", "d" * 64, remote, cast(Any, object()))


def test_a_host_without_its_state_is_refused_by_name(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(native_connector, "state_dir", lambda o, s: tmp_path / o / s)
    with pytest.raises(NativeHostFailed, match="has no state"):
        native_connector.ensure_running("acme", "spark_jobs", "d" * 64, "script", 40123)


def test_a_host_that_exits_before_it_listens_is_named_with_its_log(tmp_path):
    log = tmp_path / "host.log"
    with open(log, "w") as out:
        process = subprocess.Popen(
            [sys.executable, "-c", "import sys; print('no cluster here'); sys.exit(3)"],
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    with pytest.raises(NativeHostFailed) as failed:
        native_connector._await_listening(process, log, 0)  # pyright: ignore[reportPrivateUsage]
    assert "exited with 3" in str(failed.value)
    assert "no cluster here" in str(failed.value)


def test_the_host_reports_its_port_and_pairing_code(tmp_path):
    log = tmp_path / "host.log"
    log.write_text("earlier start\nremote host listening on 1\n")
    offset = log.stat().st_size
    script = (
        "import sys, time\n"
        "sys.stdout.write('pairing code: fp.secret\\nremote host listening on 40999\\n')\n"
        "sys.stdout.flush()\n"
        "time.sleep(30)\n"
    )
    with open(log, "a") as out:
        process = subprocess.Popen([sys.executable, "-c", script], stdout=out)
    try:
        found = native_connector._await_listening(process, log, offset)  # pyright: ignore[reportPrivateUsage]
    finally:
        process.kill()
        process.wait()
    assert found == (40999, "fp.secret")


def test_the_script_value_is_the_one_the_standard_file_marks():
    row = {"requires": [{"name": "cluster"}, {"name": "job", "script": True}]}
    assert native_connector.script_value(row) == "job"
    with pytest.raises(NativeHostFailed, match="2 script values"):
        native_connector.script_value(
            {"requires": [{"name": "a", "script": True}, {"name": "b", "script": True}]}
        )


# -- REQ-2225: a native host listens on loopback alone -------------------------------------------


def _non_loopback_address() -> str:
    """An address of this machine that is not loopback: the one its default route leaves by."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect(("192.0.2.1", 9))  # TEST-NET-1: a UDP connect picks a route, sends nothing
        address = probe.getsockname()[0]
    assert not address.startswith("127."), f"this machine has no non-loopback address: {address}"
    return address


def _host_files(work: Path) -> SimpleNamespace:
    """A native host's work directory and its install, as _launch leaves them."""
    work.mkdir(parents=True)
    code = work / "code"
    code.mkdir()
    (work / "manifest.json").write_text("{}")
    (work / "names.json").write_text("{}")
    return SimpleNamespace(python=Path(sys.executable), code=code, entry="spark_connector")


def _start_front(work: Path, env: dict[str, str]) -> tuple[subprocess.Popen, int]:
    built = cast(Any, _host_files(work))
    log = work / "host.log"
    with open(log, "w") as out:
        process = subprocess.Popen(  # noqa: S603 -- the test's interpreter and Provisa's front end
            native_connector._command(work, built, 0),  # pyright: ignore[reportPrivateUsage]
            cwd=str(work),
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    port, _ = native_connector._await_listening(process, log, 0)  # pyright: ignore[reportPrivateUsage]
    return process, port


def _connects(address: str, port: int) -> bool:
    try:
        with socket.create_connection((address, port), timeout=5):
            return True
    except ConnectionRefusedError:
        return False


def test_a_native_hosts_front_binds_loopback_alone(tmp_path):
    env = native_connector._environment(tmp_path, "d" * 64, "job")  # pyright: ignore[reportPrivateUsage]
    state = tmp_path / "state"
    server = front.Front(
        0,
        front.Identity(state),
        None,
        {},
        bounds=front.Bounds(state, str(tmp_path / "spill")),
        script=front.Script(state, [sys.executable, "-I", "host.py", "{fd}", "code", "entry"]),
        worker_argv=[],
        bind=env[front.BIND_ENV],
    )
    try:
        assert server.socket.getsockname()[0] == "127.0.0.1"
    finally:
        server.server_close()


def test_a_native_host_refuses_a_connection_from_the_network(tmp_path):
    work = tmp_path / "host"
    env = native_connector._environment(work, "d" * 64, "job")  # pyright: ignore[reportPrivateUsage]
    process, port = _start_front(work, env)
    try:
        assert _connects("127.0.0.1", port)
        assert not _connects(_non_loopback_address(), port)
    finally:
        process.terminate()
        process.wait(timeout=10)


def test_an_image_hosts_front_keeps_listening_on_every_interface(tmp_path):
    state = tmp_path / "state"
    server = front.Front(
        0,
        front.Identity(state),
        None,
        {},
        bounds=front.Bounds(state, str(tmp_path / "spill")),
        script=front.Script(state, [sys.executable, "-I", "host.py", "{fd}", "code", "entry"]),
        worker_argv=[],
    )
    try:
        assert server.socket.getsockname()[0] == "0.0.0.0"
    finally:
        server.server_close()
    # The front started as an image starts it: no bind given, so the network reaches it.
    work = tmp_path / "host"
    env = native_connector._environment(work, "d" * 64, "job")  # pyright: ignore[reportPrivateUsage]
    del env[front.BIND_ENV]
    process, port = _start_front(work, env)
    try:
        assert _connects("127.0.0.1", port)
        assert _connects(_non_loopback_address(), port)
    finally:
        process.terminate()
        process.wait(timeout=10)
