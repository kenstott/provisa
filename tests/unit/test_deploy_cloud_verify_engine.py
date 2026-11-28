# Copyright (c) 2026 Kenneth Stott
# Canary: 404dccef-ca1b-4dd2-b684-07ced7192522
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""verify_engine proves the cloud node's engine path with no account and no token.

A release deploy (deploy-cloud-release.sh) and the hot-patch's reset arm run reset_state before
verify_engine, which leaves zero accounts: a check that asks the public site as an org_admin by a
personal access token can never pass after it. verify_engine instead sends
scripts/cloud_engine_probe.py to the API container, which runs the app's own engine resolution:
the selected engine must be the pin, the boot shard is woken as a query wakes it, and the statement
must be answered by the coordinator at the address the app dials (its query id carries that
coordinator's id).

The probe runs here against the app's real resolution with the cluster calls stubbed; the shell
function runs as the deploy runs it, with ssh_node replaced by a local shell and `sudo docker exec`
by a stub that records what reached the container."""

from __future__ import annotations

import importlib.util
import re
import stat
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_PROBE = _REPO / "scripts" / "cloud_engine_probe.py"
_LIB = _REPO / "scripts" / "cloud-node-lib.sh"
_RELEASE = _REPO / "scripts" / "deploy-cloud-release.sh"
_HOTPATCH = _REPO / "scripts" / "deploy-cloud.sh"

_COORDINATOR = "abcde"
_ADDRESS = ("10.8.0.7", 8080)


def _probe_module():
    spec = importlib.util.spec_from_file_location("cloud_engine_probe", _PROBE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Cursor:
    def __init__(self, rows: list, query_id: str) -> None:
        self._rows = rows
        self.query_id = query_id
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchall(self) -> list:
        return self._rows


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor


@pytest.fixture
def engine(monkeypatch):
    """The node's environment with the cluster stubbed: what the probe woke, dialled and ran."""
    import trino

    from provisa.federation import engine as engine_mod
    from provisa.federation import engine_wake, trino_lifecycle

    monkeypatch.delenv("PROVISA_CLOUD_ORG_ADMIN_PAT", raising=False)
    monkeypatch.setenv("PROVISA_ENGINE", "trino")
    monkeypatch.setenv("PROVISA_ENGINE_SHARD", "shared_1")
    monkeypatch.setenv("ORG_ID", "acme")

    seen: dict = {"events": [], "rows": [[1]], "query_id": f"20261128_120000_00001_{_COORDINATOR}"}

    async def wake(shard, **_kw):
        seen["events"].append(("wake", shard))
        return False

    def endpoint():
        seen["events"].append(("endpoint",))
        return _ADDRESS

    def connect(**kwargs):
        seen["events"].append(("connect",))
        seen["kwargs"] = kwargs
        seen["cursor"] = _Cursor(seen["rows"], seen["query_id"])
        return _Connection(seen["cursor"])

    def coordinator_start(kwargs, timeout, *, fresh=False):
        assert fresh
        return f"{kwargs['host']}:{kwargs['port']}/{_COORDINATOR}"

    monkeypatch.setattr(engine_wake, "ensure_shard_awake", wake)
    monkeypatch.setattr(engine_mod, "configured_engine_endpoint", endpoint)
    monkeypatch.setattr(trino.dbapi, "connect", connect)
    monkeypatch.setattr(trino_lifecycle, "coordinator_start", coordinator_start)
    return seen


def test_the_probe_needs_no_account_and_names_the_engine_that_answered(engine, capsys):
    assert _probe_module().main(["trino"]) == 0
    out = capsys.readouterr().out.strip()
    assert out == (
        f"engine=trino shard=shared_1 coordinator={_ADDRESS[0]}:{_ADDRESS[1]}/{_COORDINATOR} "
        f"query=20261128_120000_00001_{_COORDINATOR} ok=1"
    )
    # Woken as a query wakes it, then dialled at the address that wake recorded.
    assert engine["events"] == [("wake", "shared_1"), ("endpoint",), ("connect",)]
    assert engine["cursor"].executed == ["SELECT 1 AS ok"]
    kwargs = engine["kwargs"]
    assert (kwargs["host"], kwargs["port"]) == _ADDRESS
    # The app's engine principal for the bootstrap org; no credential of any account.
    assert kwargs["user"] == "provisa"
    assert kwargs["source"] == "provisa/acme"
    assert not {"auth", "password", "access_token", "http_headers"} & set(kwargs)


def test_the_probe_refuses_an_engine_other_than_the_pin(engine, monkeypatch, capsys):
    monkeypatch.setenv("PROVISA_ENGINE", "duckdb")
    assert _probe_module().main(["trino"]) == 1
    assert "not the pinned 'trino'" in capsys.readouterr().err
    assert engine["events"] == []


def test_an_answer_from_another_coordinator_is_not_the_pinned_engine(engine, capsys):
    engine["query_id"] = "20261128_120000_00001_zzzzz"
    assert _probe_module().main(["trino"]) == 1
    assert "was not issued by the coordinator" in capsys.readouterr().err


def test_a_wrong_answer_fails(engine, capsys):
    engine["rows"] = [[0]]
    assert _probe_module().main(["trino"]) == 1
    assert "answered SELECT 1 AS ok with [[0]]" in capsys.readouterr().err


def _run_verify_engine(tmp_path: Path, *, answer: str, code: int) -> subprocess.CompletedProcess:
    """verify_engine with ssh_node a local shell; `sudo` records what reached the container."""
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / "scripts" / "cloud_engine_probe.py").write_bytes(_PROBE.read_bytes())
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    sudo = bin_dir / "sudo"
    sudo.write_text(
        "#!/bin/bash\n"
        'printf "%s\\n" "$*" >"$CAPTURE/args"\n'
        'cat >"$CAPTURE/stdin"\n'
        f"printf '%s\\n' {answer!r}\n"
        f"exit {code}\n"
    )
    curl = bin_dir / "curl"
    curl.write_text('#!/bin/bash\ntouch "$CAPTURE/curl"\nexit 1\n')
    for f in (sudo, curl):
        f.chmod(f.stat().st_mode | stat.S_IXUSR)
    script = f'set -euo pipefail\n. {_LIB}\nssh_node() {{ bash -c "$1"; }}\nverify_engine\n'
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "REPO": str(repo), "CAPTURE": str(tmp_path)},
    )


def test_verify_engine_runs_the_probe_in_the_api_container_with_no_token(tmp_path):
    answer = "engine=trino shard=shared_1 coordinator=10.8.0.7:8080/abcde query=q_abcde ok=1"
    done = _run_verify_engine(tmp_path, answer=answer, code=0)
    assert done.returncode == 0, done.stderr
    assert answer in done.stdout
    assert (
        tmp_path / "args"
    ).read_text().strip() == "docker exec -i compose-provisa-1 python - trino"
    assert (tmp_path / "stdin").read_bytes() == _PROBE.read_bytes()
    assert not (tmp_path / "curl").exists()


def test_verify_engine_fails_the_deploy_when_the_probe_fails(tmp_path):
    done = _run_verify_engine(tmp_path, answer="", code=1)
    assert done.returncode == 1
    assert "the engine probe in compose-provisa-1 failed" in done.stderr


@pytest.mark.parametrize("path", [_LIB, _RELEASE, _HOTPATCH], ids=lambda p: p.name)
def test_no_deploy_reads_an_account_token(path):
    assert "PROVISA_CLOUD_ORG_ADMIN_PAT" not in path.read_text()


def test_every_hot_patch_arm_runs_the_one_engine_check():
    arms = [line for line in _HOTPATCH.read_text().splitlines() if "verify_api;" in line]
    assert arms
    for arm in arms:
        assert re.search(r"\bverify_engine;", arm), arm
