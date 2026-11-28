# Copyright (c) 2026 Kenneth Stott
# Canary: 7c4e1d90-3b52-4f68-a1d7-5e90c2b84f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Spark connector's host (REQ-2225): a Spark source joins it with its cluster and no script
(amended 2026-11-25). The host here is a real front end over TLS running the connector project's
code; only the registry and the model's block are stood in for. The cluster a source leaves unset
is the organisation's default Spark cluster, given to the host's @on-start."""

# Requirements: REQ-2225

from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codehost import front
from provisa.codesource import remote, remote_sources
from provisa.codesource.bundle import content_hash
from provisa.codesource.lifecycle import setup_values
from provisa.codesource.manifest import (
    PYTHON_RULES,
    manifest_from_json,
    manifest_hash,
    manifest_json,
    read_manifest,
)
from provisa.core.models import ConnectorRequirement
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.python.source import HOST, READER

pytestmark = pytest.mark.unit

_PROJECT = Path(__file__).resolve().parents[2] / "connectors" / "spark"


def _read_project() -> dict:
    """The reader's document for the connector's own project, as Provisa reads it."""
    done = subprocess.run(  # noqa: S603 -- Provisa's own reader, on the connector's own project
        [sys.executable, "-I", str(READER), str(_PROJECT), "provisa_spark"],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONPATH": ""},
    )
    return json.loads(done.stdout)


#: The manifest the connector's image carries: its own code's, read as a build reads it.
_PROJECT_MANIFEST = manifest_json(read_manifest(_read_project(), PYTHON_RULES))


class _Host:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        code = tmp_path / "code"
        code.mkdir()
        shutil.copytree(_PROJECT / "provisa_spark", code / "provisa_spark")
        self.hashed = content_hash(code)
        self.code = code
        (tmp_path / "names.json").write_text("{}")
        baked = tmp_path / "manifest.json"
        baked.write_text(json.dumps(_PROJECT_MANIFEST))
        self.env = {
            **os.environ,
            "PROVISA_CODE_HASH": self.hashed,
            "PROVISA_DISCOVERED": "{}",
            "PROVISA_MANIFEST_FILE": str(baked),
        }
        self.process: subprocess.Popen | None = None
        self.port = 0
        self.pairing: str | None = None

    def start(self) -> None:
        self.process = subprocess.Popen(  # noqa: S603 -- the test's interpreter and Provisa's front
            [
                sys.executable,
                front.__file__,
                "0",
                str(self.tmp_path / "state"),
                str(self.tmp_path / "spill"),
                str(self.tmp_path / "names.json"),
                *[sys.executable, "-I", str(HOST), "{fd}", str(self.code), "provisa_spark"],
            ],
            env=self.env,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert self.process.stderr is not None
        self.pairing, self.port = None, 0
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            line = self.process.stderr.readline()
            if line.startswith("pairing code: "):
                self.pairing = line.removeprefix("pairing code: ").strip()
            if line.startswith("remote host listening on "):
                self.port = int(line.rsplit(" ", 1)[1])
                return
        raise AssertionError("the host did not start")

    def stop(self) -> None:
        assert self.process is not None
        self.process.terminate()
        self.process.wait(timeout=15)


@pytest.fixture
def host(tmp_path):
    started = _Host(tmp_path)
    started.start()
    yield started
    started.stop()


def _row(code_hash: str, cluster: dict) -> dict:
    return {
        "name": "spark",
        "git": "https://git.example/spark.git",
        "ref": "main",
        "commit": "a" * 40,
        "language": "python",
        "image": "registry.example/connectors/spark",
        "digest": "sha256:" + "b" * 64,
        "code_hash": code_hash,
        "arch": "amd64",
        "requires": [cluster],
    }


@pytest.fixture
def wired(monkeypatch):
    held: dict = {"identity": remote.new_identity(), "row": None, "labels": None}

    async def connector(name):
        return held["row"]

    async def labels(state, org_id, name, row):
        return held["labels"]

    async def identity(org_id):
        return held["identity"]

    monkeypatch.setattr(remote_sources, "_connector", connector)
    monkeypatch.setattr(remote_sources, "_image_labels", labels)
    monkeypatch.setattr(remote_sources, "identity", identity)
    token = set_current_org("acme")
    yield held
    reset_current_org(token)


def _joined(wired: dict, host: _Host, cluster: dict) -> None:
    wired["row"] = _row(host.hashed, cluster)
    wired["labels"] = {
        "code_hash": host.hashed,
        "manifest_hash": manifest_hash(_PROJECT_MANIFEST),
        "runtime": "python",
    }


def _mapping(host: _Host, settings: dict) -> dict:
    return {
        "connector_join": {
            "connector": "spark",
            "host": "127.0.0.1",
            "port": host.port,
            "pairing_code": host.pairing,
        },
        "settings": settings,
    }


async def _join(mapping: dict) -> dict:
    await remote_sources.join_connector(
        SimpleNamespace(admin_db=None), "spark_prod", "spark", mapping
    )
    return mapping


async def _values(mapping: dict) -> dict:
    """The setup values the host's @on-start is given, read off the loop as the join reads them."""
    manifest = manifest_from_json(mapping["remote"]["manifest"])
    give = setup_values("acme", "spark_prod", manifest, mapping, None)
    assert give is not None, "the host's @on-start is given the setup values"
    return await asyncio.to_thread(give)


_OWN = {"name": "cluster"}
_ORG_DEFAULT = {"name": "cluster", "org_default": "spark_cluster"}


# -- the join ---------------------------------------------------------------------------------------


async def test_a_spark_source_joins_its_host_with_its_cluster_and_no_script(host, wired):
    _joined(wired, host, _OWN)
    mapping = await _join(_mapping(host, {"cluster": "sc://spark.example:15002"}))
    assert mapping["mode"] == "built"
    assert mapping["content_hash"] == host.hashed
    assert mapping["connector"] == "spark"
    assert "script_hash" not in mapping and "names" not in mapping["remote"]
    assert mapping["remote"]["exports"] == ["declared"]
    kept = manifest_from_json(mapping["remote"]["manifest"])
    assert [r.name for r in kept.requires] == ["cluster"]
    assert "connector_join" not in mapping and host.pairing not in json.dumps(mapping)
    assert await _values(mapping) == {"cluster": "sc://spark.example:15002"}


async def test_a_source_naming_no_cluster_runs_on_the_organisations_default(
    host, wired, monkeypatch
):
    """REQ-2225: the cluster left unset, the join is not refused and the host's @on-start is given
    the organisation's default Spark cluster."""
    from provisa.core import spark_cluster

    asked: list = []

    async def default(admin_db, org_id):
        asked.append(org_id)
        return spark_cluster.Resolved("sc://default.acme.test:15002", spark_cluster.ORIGIN_ORG)

    monkeypatch.setattr(spark_cluster, "_resolved_default", default)
    _joined(wired, host, _ORG_DEFAULT)
    mapping = await _join(_mapping(host, {}))
    assert mapping["org_defaults"] == {"cluster": "spark_cluster"}
    assert await _values(mapping) == {"cluster": "sc://default.acme.test:15002"}
    assert asked and set(asked) == {"acme"}


async def test_a_sources_own_cluster_is_used_over_the_organisations_default(
    host, wired, monkeypatch
):
    from provisa.core import spark_cluster

    async def default(admin_db, org_id):
        raise AssertionError("the default is not read when the source names its own cluster")

    monkeypatch.setattr(spark_cluster, "_resolved_default", default)
    _joined(wired, host, _ORG_DEFAULT)
    mapping = await _join(_mapping(host, {"cluster": "sc://spark.example:15002"}))
    assert await _values(mapping) == {"cluster": "sc://spark.example:15002"}


async def test_a_cluster_with_no_org_default_is_still_required(host, wired):
    _joined(wired, host, _OWN)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(host, {}))
    assert refused.value.code == "codesource.connector_requires_missing"


# -- the project ------------------------------------------------------------------------------------


def test_the_project_declares_the_cluster_and_no_script():
    manifest = read_manifest(_read_project(), PYTHON_RULES)
    assert {r.name for r in manifest.requires} == {"cluster"}
    assert [(e.name, e.kind) for e in manifest.exports] == [("declared", "on-start")]


def test_the_standard_file_turns_replication_on():
    from provisa.cli_connector import _read_standard_file

    standard = _read_standard_file(_PROJECT)
    assert standard is not None
    assert set(standard.requires) == {"cluster"}
    assert [(s.name, s.default_value) for s in standard.settings] == [("replicate", 0)]
    assert standard.icon.startswith("data:image/svg+xml;base64,")
    assert base64.b64decode(standard.icon.split(",", 1)[1]).startswith(b"<svg")
    assert standard.description


def test_the_standard_file_lets_the_organisations_default_cluster_stand_in():
    """REQ-2225: the Spark connector's cluster is met by the organisation's default when unset."""
    from provisa.cli_connector import _read_standard_file

    standard = _read_standard_file(_PROJECT)
    assert standard is not None
    assert standard.requires["cluster"].org_default == "spark_cluster"


def test_an_org_default_is_not_a_secret_and_has_no_value_of_its_own():
    assert ConnectorRequirement(name="cluster", org_default="spark_cluster").org_default
    for given in ({"secret": True}, {"default_value": "sc://x"}, {"fixed_value": "sc://x"}):
        with pytest.raises(ValueError, match="organisation's spark_cluster"):
            ConnectorRequirement(name="cluster", org_default="spark_cluster", **given)


def test_a_standard_file_naming_a_script_is_refused(tmp_path):
    """REQ-2225 (amended 2026-11-25): a connector takes no script."""
    from provisa.cli_connector import _read_standard_file, _Refused

    (tmp_path / "connector.yaml").write_text(
        "requires:\n  a:\n    multiline: true\n    script: true\n"
    )
    with pytest.raises(_Refused, match="script"):
        _read_standard_file(tmp_path)


def test_a_requirement_takes_no_script_flag():
    with pytest.raises(ValueError, match="script"):
        ConnectorRequirement(name="job", multiline=True, script=True)  # pyright: ignore[reportCallIssue]
