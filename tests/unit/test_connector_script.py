# Copyright (c) 2026 Kenneth Stott
# Canary: 7c4e1d90-3b52-4f68-a1d7-5e90c2b84f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Spark connector (REQ-2225): a connector whose tables and commands come from one annotated
script given at setup, checked by its own hash. The host here is a real front end over TLS running
the connector project's code; only Spark, the registry and the model's block are stood in for."""

# Requirements: REQ-2225

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import textwrap
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
    Export,
    Manifest,
    Requirement,
    manifest_from_json,
    manifest_hash,
    manifest_json,
    read_manifest,
)
from provisa.codesource.worker import WorkerFailed
from provisa.core.models import ConnectorRequirement
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.python.source import HOST, READER

pytestmark = pytest.mark.unit

_PROJECT = Path(__file__).resolve().parents[2] / "connectors" / "spark"
_SCRIPT_REQUIRE = "script"
BOUNDS = {"memory_mb": 64, "threads": 1, "spill_cap_mb": 64}

_SCRIPT = textwrap.dedent(
    '''
    from dataclasses import dataclass

    from provisa_spark import session


    @dataclass
    class Order:
        id: int
        amount: float


    @dataclass
    class Customer:
        id: int
        name: str


    def orders() -> list[Order]:
        """Orders, from the cluster.

        @table
        """
        return [Order(**row.asDict()) for row in session().sql("SELECT orders").collect()]


    def customers() -> list[Customer]:
        """Customers, from the cluster.

        @table
        """
        return [Customer(**row.asDict()) for row in session().sql("SELECT customers").collect()]


    def run_nightly(day: str) -> str:
        """Runs the nightly job.

        @command
        """
        session().sql(f"RUN {day}")
        return day
    '''
)

#: Spark, stood in for: a session on the cluster it is given, answering what a read asks.
_STUB_SPARK = textwrap.dedent(
    """
    class _Row:
        def __init__(self, **fields):
            self._fields = fields

        def asDict(self):
            return dict(self._fields)


    class _Frame:
        def __init__(self, rows):
            self._rows = rows

        def collect(self):
            return self._rows


    class _Session:
        def __init__(self, cluster):
            self.cluster = cluster

        def sql(self, text):
            if text == "SELECT orders":
                return _Frame([_Row(id=1, amount=9.5), _Row(id=2, amount=12.25)])
            if text == "SELECT customers":
                return _Frame([_Row(id=1, name=self.cluster)])
            return _Frame([])


    class _Builder:
        def remote(self, url):
            self._url = url
            return self

        def getOrCreate(self):
            return _Session(self._url)


    class SparkSession:
        builder = _Builder()
    """
)

_PROJECT_MANIFEST = manifest_json(
    Manifest(
        exports=(Export("declared", "on-start", "", (), None),),
        types={},
        requires=(Requirement("script", False), Requirement("cluster", False)),
    )
)


def _script_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class _Host:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        code = tmp_path / "code"
        code.mkdir()
        shutil.copytree(_PROJECT / "provisa_spark", code / "provisa_spark")
        (code / "pyspark" / "sql").mkdir(parents=True)
        (code / "pyspark" / "__init__.py").write_text("")
        (code / "pyspark" / "sql" / "__init__.py").write_text(_STUB_SPARK)
        self.hashed = content_hash(code)
        self.code = code
        (tmp_path / "names.json").write_text("{}")
        baked = tmp_path / "manifest.json"
        baked.write_text(json.dumps(_PROJECT_MANIFEST))
        self.env = {
            **os.environ,
            "PROVISA_CODE_HASH": self.hashed,
            "PROVISA_MANIFEST_FILE": str(baked),
            "PROVISA_CONNECTOR_SCRIPT": _SCRIPT_REQUIRE,
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


def _row(code_hash: str, **overrides) -> dict:
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
        "requires": [
            {"name": "script", "multiline": True, "script": True},
            {"name": "cluster"},
        ],
        **overrides,
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


def _labels(code_hash: str) -> dict:
    return {
        "code_hash": code_hash,
        "manifest_hash": manifest_hash(_PROJECT_MANIFEST),
        "runtime": "python",
    }


def _mapping(host: _Host, script: str = _SCRIPT, **overrides) -> dict:
    return {
        "connector_join": {
            "connector": "spark",
            "host": "127.0.0.1",
            "port": host.port,
            "pairing_code": host.pairing,
        },
        "settings": {"script": script, "cluster": "sc://spark.example:15002"},
        **overrides,
    }


async def _join(mapping: dict) -> dict:
    await remote_sources.join_connector(
        SimpleNamespace(admin_db=None), "spark_prod", "python", mapping
    )
    return mapping


def _joined(wired: dict, host: _Host) -> None:
    wired["row"] = _row(host.hashed)
    wired["labels"] = _labels(host.hashed)


def _worker(mapping: dict, wired: dict):
    manifest = manifest_from_json(mapping["remote"]["manifest"])
    return remote_sources.worker_for(
        "acme",
        "spark_prod",
        mapping["content_hash"],
        remote_sources.remote_of(mapping),
        wired["identity"],
        setup=setup_values("acme", "spark_prod", manifest, mapping, None),
    )


# -- the scenario -----------------------------------------------------------------------------------


async def test_the_script_tags_are_the_sources_tables_and_command(host, wired):
    _joined(wired, host)
    mapping = await _join(_mapping(host))
    assert mapping["mode"] == "built"
    assert mapping["content_hash"] == host.hashed
    assert mapping["connector"] == "spark"
    assert mapping["script_hash"] == _script_hash(_SCRIPT)
    assert mapping["remote"]["exports"] == ["customers", "orders", "run_nightly"]
    kept = manifest_from_json(mapping["remote"]["manifest"])
    assert {e.name: e.kind for e in kept.exports} == {
        "orders": "table",
        "customers": "table",
        "run_nightly": "command",
    }
    assert kept.types["Order"].fields[1].name == "amount"
    assert sorted(r.name for r in kept.requires) == ["cluster", "script"]
    assert mapping["remote"]["names"]["orders"]["columns"][0]["name"] == "id"
    assert "connector_join" not in mapping and host.pairing not in json.dumps(mapping)


async def test_a_read_runs_the_function_on_the_configured_cluster(host, wired):
    _joined(wired, host)
    mapping = await _join(_mapping(host))
    worker = _worker(mapping, wired)
    worker.start()
    worker.ready()
    try:
        names = mapping["remote"]["names"]
        rows = [
            row
            for batch in worker.rows(
                "customers", "table", [], [], schema=names["customers"]["schema"]
            )
            for row in batch
        ]
    finally:
        worker.stop()
    assert rows == [{"id": 1, "name": "sc://spark.example:15002"}]


async def test_a_script_hash_in_the_config_that_differs_is_refused_by_name(host, wired):
    _joined(wired, host)
    mapping = _mapping(host, script_hash=_script_hash("something else"))
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(mapping)
    assert refused.value.code == "codesource.connector_script_hash"
    # Refused before pairing: the one-time code is not spent.
    paired = remote.pair(
        "127.0.0.1", host.port, host.pairing, wired["identity"], BOUNDS, script=_SCRIPT
    )
    assert paired.script is not None


async def test_a_host_reporting_another_script_is_refused_by_name(host, wired, monkeypatch):
    _joined(wired, host)
    real = remote_sources.pair

    def lying(*args, **kwargs):
        paired = real(*args, **kwargs)
        assert paired.script is not None
        return remote.Paired(
            paired.pin,
            paired.certificate,
            paired.manifest,
            {**paired.script, "hash": _script_hash("another script")},
        )

    monkeypatch.setattr(remote_sources, "pair", lying)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(host))
    assert refused.value.code == "codesource.connector_script_hash"
    assert _script_hash("another script") in str(refused.value)


async def test_every_later_dial_is_held_to_the_scripts_hash(host, wired):
    _joined(wired, host)
    mapping = await _join(_mapping(host))
    other = remote.RemoteWorker(
        "spark_prod",
        mapping["content_hash"],
        address="127.0.0.1",
        port=host.port,
        pin=mapping["remote"]["pin"],
        identity=wired["identity"],
        script_hash=_script_hash("another script"),
    )
    with pytest.raises(WorkerFailed) as refused:
        other.start()
    assert "script" in str(refused.value)


async def test_a_restarted_host_serves_its_script_again(host, wired):
    _joined(wired, host)
    mapping = await _join(_mapping(host))
    state = host.tmp_path / "state"
    host.stop()
    assert any(p.name == "connector_script.py" for p in state.iterdir())
    host.start()
    mapping["remote"]["port"] = host.port  # the test's host listens on a port of its own choosing
    worker = _worker(mapping, wired)
    worker.start()
    worker.ready()
    try:
        names = mapping["remote"]["names"]
        rows = [
            row
            for batch in worker.rows("orders", "table", [], [], schema=names["orders"]["schema"])
            for row in batch
        ]
    finally:
        worker.stop()
    assert rows == [{"id": 1, "amount": 9.5}, {"id": 2, "amount": 12.25}]


async def test_a_host_of_a_script_connector_needs_a_script_to_pair(host, wired):
    with pytest.raises(remote.PairingRefused) as refused:
        remote.pair("127.0.0.1", host.port, host.pairing, wired["identity"], BOUNDS)
    assert "script" in str(refused.value)


async def test_a_script_needing_a_value_the_connector_does_not_offer_is_refused(host, wired):
    _joined(wired, host)
    script = (
        'def t() -> list[dict]:\n    """T.\n\n    @table\n    @requires other_key\n    """\n'
        "    return []\n"
    )
    # A row of @table with a typed row: the script is read, then held to the block's requires.
    script = script.replace("list[dict]", "list[R]")
    script = "from dataclasses import dataclass\n@dataclass\nclass R:\n    a: int\n" + script
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(host, script=script))
    assert refused.value.code == "codesource.connector_script_requires"
    assert "other_key" in str(refused.value)


# -- the project ------------------------------------------------------------------------------------


def test_the_project_declares_the_script_and_the_cluster():
    done = subprocess.run(  # noqa: S603 -- Provisa's own reader, on the connector's own project
        [sys.executable, "-I", str(READER), str(_PROJECT), "provisa_spark"],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONPATH": ""},
    )
    manifest = read_manifest(json.loads(done.stdout), PYTHON_RULES)
    assert {r.name for r in manifest.requires} == {"script", "cluster"}


def test_the_standard_file_marks_the_script_and_replication_on():
    from provisa.cli_connector import _read_standard_file

    standard = _read_standard_file(_PROJECT)
    assert standard is not None
    script = standard.requires["script"]
    assert script.multiline and script.script
    assert [(s.name, s.default_value) for s in standard.settings] == [("replicate", 0)]
    assert standard.icon.startswith("data:image/svg+xml;base64,")
    assert base64.b64decode(standard.icon.split(",", 1)[1]).startswith(b"<svg")
    assert standard.description


def test_a_script_value_is_multi_line_text_and_not_a_secret():
    with pytest.raises(ValueError, match="multi-line"):
        ConnectorRequirement(name="script", script=True)
    with pytest.raises(ValueError, match="secret"):
        ConnectorRequirement(name="script", secret=True, multiline=False, script=True)
