# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1f8a02-3b6e-4d77-9f41-8e2a6c0b7d13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A connector's lifecycle tags (REQ-2221, LIFECYCLE TAGS and WHEN ON-START FAILS): @on-start runs
once when the host has its setup values, at join and on each later restart; @on-stop when the host
shuts down; @health returns a status Provisa calls for periodically. The host here is a real front
end running the Python host program, over TLS."""

# Requirements: REQ-2221

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codehost import front
from provisa.codesource import remote, remote_sources, worker as worker_module
from provisa.codesource.bundle import BundleRefused, content_hash
from provisa.codesource.manifest import (
    HEALTH,
    ON_START,
    ON_STOP,
    Export,
    Manifest,
    ManifestRefused,
    Parameter,
    Requirement,
    Scalar,
    manifest_from_json,
    manifest_json,
    manifest_hash,
    read_manifest,
)
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.python.source import HOST

pytestmark = pytest.mark.unit

BOUNDS = {"memory_mb": 256, "threads": 2, "spill_cap_mb": 1024}
RECORD = {"kind": "record"}
TEXT = {"kind": "string"}
NOTHING = {"kind": "undefined"}


def _raw(*exports) -> dict:
    return {"exports": list(exports), "types": {}, "diagnostics": []}


def _fn(name: str, tags: list[str], params: list[dict], returns: dict) -> dict:
    return {
        "name": name,
        "tags": tags,
        "tag_arguments": [],
        "description": "",
        "signatures": [{"parameters": params, "returns": returns}],
    }


def _param(name: str, kind: dict) -> dict:
    return {"name": name, "optional": False, "rest": False, "type": kind}


def _refused(*exports) -> str:
    with pytest.raises(ManifestRefused) as caught:
        read_manifest(_raw(*exports))
    return "; ".join(caught.value.problems)


# -- the tags -----------------------------------------------------------------------------------


def test_each_lifecycle_tag_is_read_as_a_function_of_that_kind():
    manifest = read_manifest(
        _raw(
            _fn("connect", ["on-start"], [_param("values", RECORD)], NOTHING),
            _fn("close", ["on-stop"], [], NOTHING),
            _fn("check", ["health"], [], TEXT),
        )
    )
    assert manifest.lifecycle(ON_START).name == "connect"
    assert manifest.lifecycle(ON_STOP).name == "close"
    assert manifest.lifecycle(HEALTH).name == "check"


def test_a_lifecycle_tag_is_optional():
    assert read_manifest(_raw(_fn("orders", ["command"], [], TEXT))).lifecycle(ON_START) is None


@pytest.mark.parametrize("tag", ["on-start", "on-stop", "health"])
def test_a_second_use_of_a_lifecycle_tag_is_refused_naming_both(tag):
    returns = TEXT if tag == "health" else NOTHING
    said = _refused(_fn("first", [tag], [], returns), _fn("second", [tag], [], returns))
    assert f"@{tag}" in said and "first" in said and "second" in said


def test_a_function_is_one_thing():
    assert "an export is one" in _refused(_fn("both", ["table", "health"], [], TEXT))


def test_on_start_takes_nothing_or_the_setup_values_as_a_mapping():
    assert read_manifest(_raw(_fn("go", ["on-start"], [], NOTHING))).lifecycle(ON_START)
    said = _refused(_fn("go", ["on-start"], [_param("n", {"kind": "number"})], NOTHING))
    assert "go" in said and "@on-start" in said and "mapping" in said
    said = _refused(_fn("go", ["on-start"], [_param("a", RECORD), _param("b", RECORD)], NOTHING))
    assert "go" in said and "@on-start" in said


def test_on_stop_takes_nothing():
    said = _refused(_fn("end", ["on-stop"], [_param("why", TEXT)], NOTHING))
    assert "end" in said and "@on-stop" in said


def test_health_takes_nothing_and_returns_a_text_status():
    said = _refused(_fn("check", ["health"], [_param("x", TEXT)], TEXT))
    assert "check" in said and "@health" in said
    said = _refused(_fn("check", ["health"], [], {"kind": "number"}))
    assert "check" in said and "@health" in said


def test_a_lifecycle_export_survives_being_kept_with_a_built_source():
    manifest = read_manifest(
        _raw(
            _fn("connect", ["on-start"], [_param("values", RECORD)], NOTHING),
            _fn("check", ["health"], [], TEXT),
        )
    )
    kept = manifest_from_json(json.loads(json.dumps(manifest_json(manifest))))
    assert kept.lifecycle(ON_START).parameters == manifest.lifecycle(ON_START).parameters
    assert kept.lifecycle(HEALTH).name == "check"


def test_the_python_reader_reads_a_hyphenated_tag(tmp_path):
    from provisa.python.source import READER

    (tmp_path / "rows.py").write_text(
        'def connect(values: dict) -> None:\n    """Connect.\n\n    @on-start\n    """\n'
    )
    done = subprocess.run(  # noqa: S603 -- the test's interpreter and Provisa's reader
        [sys.executable, "-I", str(READER), str(tmp_path), "rows"],
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(Path(READER).parent / "services")},
    )
    assert done.returncode == 0, done.stderr
    assert json.loads(done.stdout)["exports"][0]["tags"] == ["on-start"]


# -- on a real host -------------------------------------------------------------------------------

_CODE = textwrap.dedent(
    '''
    import json
    import os
    from pathlib import Path

    TRAIL = Path(os.environ["LIFECYCLE_TRAIL"])

    def _note(what):
        with TRAIL.open("a") as f:
            f.write(what + "\\n")


    def connect(values: dict) -> None:
        """Connects.

        @on-start
        """
        _note("start " + json.dumps(values, sort_keys=True))
        if values.get("fail"):
            raise ValueError("the database refused the login")


    def close() -> None:
        """Closes.

        @on-stop
        """
        _note("stop")


    def check() -> str:
        """Checks.

        @health
        """
        _note("health")
        return (TRAIL.parent / "health.txt").read_text().strip()


    def orders() -> list[dict]:
        """Orders.

        @command
        """
        return []
    '''
)


_LIFECYCLE = manifest_json(
    Manifest(
        exports=(
            Export(
                "connect",
                ON_START,
                "Connects.",
                (Parameter("values", Scalar("json"), False, True),),
                None,
            ),
            Export("close", ON_STOP, "Closes.", (), None),
            Export("check", HEALTH, "Checks.", (), Scalar("text")),
            Export("orders", "command", "Orders.", (), None),
        ),
        types={},
        requires=(Requirement("db_uri", True), Requirement("region", False)),
    )
)
_EXPORTS = sorted(e["name"] for e in _LIFECYCLE["exports"])


class Host:
    """A host process over a state directory that survives its restarts."""

    def __init__(self, tmp_path: Path, manifest: dict | None = _LIFECYCLE, code: str = _CODE):
        self.dir = tmp_path
        self.trail = tmp_path / "trail.txt"
        self.trail.write_text("")
        (tmp_path / "health.txt").write_text("ok")
        src = tmp_path / "code"
        src.mkdir()
        (src / "shop.py").write_text(code)
        self.hashed = content_hash(src)
        names = tmp_path / "names.json"
        names.write_text("{}")
        self.names = names
        self.src = src
        self.manifest = tmp_path / "manifest.json"
        self.has_manifest = manifest is not None
        if manifest is not None:
            self.manifest.write_text(json.dumps(manifest))
        self.process: subprocess.Popen | None = None
        self.port = 0
        self.pairing = ""
        self.start()

    def start(self) -> None:
        env = {
            **os.environ,
            "PROVISA_CODE_HASH": self.hashed,
            "LIFECYCLE_TRAIL": str(self.trail),
        }
        if self.has_manifest:
            env["PROVISA_MANIFEST_FILE"] = str(self.manifest)
        self.process = subprocess.Popen(  # noqa: S603 -- the test's interpreter, Provisa's front end
            [
                sys.executable,
                front.__file__,
                str(self.port),
                str(self.dir / "state"),
                str(self.dir / "spill"),
                str(self.names),
                *[sys.executable, "-I", str(HOST), "{fd}", str(self.src), "shop"],
            ],
            env=env,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert self.process.stderr is not None
        deadline = time.monotonic() + 20
        listening = False
        while time.monotonic() < deadline:
            line = self.process.stderr.readline()
            if line.startswith("pairing code: "):
                self.pairing = line.removeprefix("pairing code: ").strip()
            if line.startswith("remote host listening on "):
                self.port = int(line.rsplit(" ", 1)[1])
                listening = True
                break
        assert listening, "the host did not start"
        threading.Thread(target=self.process.stderr.read, daemon=True).start()

    def stop(self) -> None:
        assert self.process is not None
        self.process.terminate()
        self.process.wait(timeout=20)

    def said(self) -> list[str]:
        return [line for line in self.trail.read_text().splitlines() if line]


@pytest.fixture
def host(tmp_path):
    made = Host(tmp_path)
    yield made
    if made.process is not None and made.process.poll() is None:
        made.stop()


@pytest.fixture
def paired(host):
    identity = remote.new_identity()
    done = remote.pair("127.0.0.1", host.port, host.pairing, identity, BOUNDS)
    return host, identity, done.pin


def _worker(host: Host, identity, pin, *, setup=None) -> remote.RemoteWorker:
    return remote.RemoteWorker(
        "crm",
        host.hashed,
        address="127.0.0.1",
        port=host.port,
        pin=pin,
        identity=identity,
        exports=_EXPORTS,
        setup=setup,
    )


GIVEN = {"db_uri": "postgres://x", "region": "eu"}


def test_on_start_runs_once_with_the_setup_values_for_every_worker(paired):
    host, identity, pin = paired
    first = _worker(host, identity, pin, setup=lambda: GIVEN)
    second = _worker(host, identity, pin, setup=lambda: GIVEN)
    first.start()
    first.ready()
    second.start()
    second.ready()
    try:
        assert host.said() == ['start {"db_uri": "postgres://x", "region": "eu"}']
    finally:
        first.stop()
        second.stop()


def test_a_call_waits_for_on_start_to_finish(paired):
    host, identity, pin = paired
    held = threading.Event()

    def slow():
        held.wait(timeout=10)
        return GIVEN

    worker = _worker(host, identity, pin, setup=slow)
    worker.start()
    worker_answered: list = []
    call = threading.Thread(
        target=lambda: worker_answered.append(worker.command("orders", [], [], schema=None)),
        daemon=True,
    )
    call.start()
    time.sleep(0.3)
    assert worker_answered == []  # on-start has not run: the call is held
    assert host.said() == []
    held.set()
    call.join(timeout=10)
    assert worker_answered == [[]] or worker_answered == [None]
    assert host.said() == ['start {"db_uri": "postgres://x", "region": "eu"}']
    worker.stop()


def test_a_failed_on_start_is_named_and_not_run_again(paired):
    host, identity, pin = paired
    bad = {"fail": True}
    first = _worker(host, identity, pin, setup=lambda: bad)
    first.start()
    with pytest.raises(worker_module.OnStartFailed) as caught:
        first.ready()
    assert "connect" in str(caught.value) and "the database refused the login" in str(caught.value)
    # Another dial, another process's: the same failure, the function not run again.
    second = _worker(host, identity, pin, setup=lambda: bad)
    second.start()
    with pytest.raises(worker_module.OnStartFailed):
        second.ready()
    first.stop()
    second.stop()
    assert len([s for s in host.said() if s.startswith("start")]) == 1


def test_a_call_after_a_failed_on_start_fails_naming_it(paired):
    host, identity, pin = paired
    worker = _worker(host, identity, pin, setup=lambda: {"fail": True})
    worker.start()
    with pytest.raises(worker_module.OnStartFailed) as caught:
        worker.command("orders", [], [], schema=None)
    assert "the database refused the login" in str(caught.value)
    worker.stop()


def test_a_value_that_cannot_be_given_is_named():
    from provisa.codesource.services import ServiceRefused

    worker = remote.RemoteWorker(
        "crm",
        "h",
        address="127.0.0.1",
        port=1,
        pin="p",
        identity=None,  # pyright: ignore[reportArgumentType]
        setup=lambda: (_ for _ in ()).throw(ServiceRefused(404, "the secret 'db_uri' resolves")),
    )
    worker._begin_setup()  # the dial is another test's: on-start's values alone
    with pytest.raises(worker_module.OnStartFailed) as caught:
        worker.ready()
    assert "db_uri" in str(caught.value)


def test_on_start_runs_again_on_a_restart_of_the_host(paired):
    host, identity, pin = paired
    worker = _worker(host, identity, pin, setup=lambda: GIVEN)
    worker.start()
    worker.ready()
    worker.stop()
    host.stop()
    host.start()  # the same state directory: still paired
    again = _worker(host, identity, pin, setup=lambda: GIVEN)
    again.start()
    again.ready()
    again.stop()
    starts = [s for s in host.said() if s.startswith("start")]
    assert len(starts) == 2


def test_a_dial_with_no_values_to_give_leaves_on_start_for_the_one_that_has_them(paired):
    host, identity, pin = paired
    probe = _worker(host, identity, pin)  # a status probe, a hello check
    probe.start()
    probe.ready()
    probe.stop()
    assert host.said() == []


def test_on_stop_runs_when_the_host_shuts_down(paired):
    host, _identity, _pin = paired
    assert host.process is not None
    host.process.send_signal(signal.SIGTERM)
    host.process.wait(timeout=20)
    assert host.said() == ["stop"]


def test_health_returns_the_status_the_function_gives(paired):
    host, identity, pin = paired
    worker = _worker(host, identity, pin, setup=lambda: GIVEN)
    worker.start()
    worker.ready()
    assert worker.health() == {"status": "ok"}
    (host.dir / "health.txt").write_text("the database is read-only")
    assert worker.health() == {"status": "the database is read-only"}
    worker.stop()


def test_a_health_function_that_raises_is_named(tmp_path):
    host = Host(
        tmp_path,
        code=_CODE.replace(
            'return (TRAIL.parent / "health.txt").read_text().strip()',
            'raise RuntimeError("cannot reach the database")',
        ),
    )
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", host.port, host.pairing, identity, BOUNDS).pin
    worker = _worker(host, identity, pin, setup=lambda: GIVEN)
    worker.start()
    worker.ready()
    answer = worker.health()
    assert "cannot reach the database" in answer["failed"]
    worker.stop()
    host.stop()


# -- the verdict shown on the source ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("answer", "expected", "text"),
    [
        ({"status": "ok"}, "healthy", "ok"),
        ({"status": "Healthy"}, "healthy", "Healthy"),
        ({"status": "the database is read-only"}, "unhealthy", "the database is read-only"),
        ({"status": None}, "healthy", None),
        ({"failed": "check: boom"}, "unhealthy", "check: boom"),
    ],
)
def test_a_status_is_healthy_or_says_what_is_wrong(answer, expected, text):
    from provisa.codesource.lifecycle import verdict

    found = verdict(answer)
    assert found["state"] == expected
    assert found["detail"] == text


# -- join and restart --------------------------------------------------------------------------------


_ROW_REQUIRES = [{"name": "db_uri", "secret": True}, {"name": "region", "secret": False}]


def _row(code_hash: str, requires=None) -> dict:
    return {
        "name": "crm",
        "git": "https://git.example/crm.git",
        "ref": "main",
        "commit": "a" * 40,
        "path": "",
        "build_command": "",
        "language": "python",
        "image": "registry.example/data/crm",
        "digest": "sha256:" + "b" * 64,
        "code_hash": code_hash,
        "arch": "amd64",
        "requires": _ROW_REQUIRES if requires is None else requires,
    }


@pytest.fixture
def wired(monkeypatch):
    held: dict = {"identity": remote.new_identity(), "values": dict(GIVEN), "row": None}

    async def connector(name):
        return held["row"]

    async def labels(state, org_id, name, row):
        return {
            "code_hash": row["code_hash"],
            "manifest_hash": manifest_hash(_LIFECYCLE),
            "runtime": "python",
        }

    async def identity(org_id):
        return held["identity"]

    def read_secret(grant, name):
        return held["values"][name]

    monkeypatch.setattr(remote_sources, "_connector", connector)
    monkeypatch.setattr(remote_sources, "_image_labels", labels)
    monkeypatch.setattr(remote_sources, "identity", identity)
    monkeypatch.setattr("provisa.codesource.services.read_secret", read_secret)
    token = set_current_org("acme")
    yield held
    reset_current_org(token)


def _mapping(host: Host, **overrides) -> dict:
    return {
        "connector_join": {
            "connector": "crm",
            "host": "127.0.0.1",
            "port": host.port,
            "pairing_code": host.pairing,
        },
        "secrets": {"db_uri": None},
        "settings": {"region": "eu"},
        **overrides,
    }


async def test_a_join_runs_on_start_with_the_setup_values(host, wired):
    wired["row"] = _row(host.hashed)
    mapping = _mapping(host)
    await remote_sources.join_connector(SimpleNamespace(admin_db=None), "crm", "python", mapping)
    assert mapping["mode"] == "built"
    assert host.said() == ['start {"db_uri": "postgres://x", "region": "eu"}']
    assert mapping["remote"]["exports"] == _EXPORTS


async def test_a_failed_on_start_at_first_join_refuses_the_join_by_name(tmp_path, wired):
    failing = Host(
        tmp_path,
        code=_CODE.replace(
            'if values.get("fail"):\n        raise',
            "if True:\n        raise",
        ),
    )
    wired["row"] = _row(failing.hashed)
    try:
        with pytest.raises(BundleRefused) as caught:
            await remote_sources.join_connector(
                SimpleNamespace(admin_db=None), "crm", "python", _mapping(failing)
            )
    finally:
        failing.stop()
    assert caught.value.code == "codesource.connector_on_start"
    assert "crm" in str(caught.value) and "the database refused the login" in str(caught.value)


async def test_a_host_restarted_after_its_join_runs_on_start_again_when_dialled(host, wired):
    wired["row"] = _row(host.hashed)
    mapping = _mapping(host)
    await remote_sources.join_connector(SimpleNamespace(admin_db=None), "crm", "python", mapping)
    host.stop()
    host.start()
    from provisa.codesource.lifecycle import setup_values
    from provisa.codesource.manifest import manifest_from_json

    manifest = manifest_from_json(mapping["remote"]["manifest"])
    setup = setup_values("acme", "crm", manifest, mapping, admin_db=None)
    worker = remote_sources.worker_for(
        "acme", "crm", mapping["content_hash"], mapping["remote"], wired["identity"], setup=setup
    )
    worker.start()
    worker.ready()
    worker.stop()
    assert len([s for s in host.said() if s.startswith("start")]) == 2


def test_setup_values_are_given_only_for_the_code_that_has_on_start():
    from provisa.codesource.lifecycle import setup_values

    bare = Manifest(exports=(), types={}, requires=(Requirement("x", False),))
    assert setup_values("o", "s", bare, {"settings": {"x": "1"}}, admin_db=None) is None


def test_a_setting_a_saved_source_no_longer_has_is_named():
    from provisa.codesource.lifecycle import setup_values

    manifest = Manifest(
        exports=(Export("go", ON_START, "", (), None),),
        types={},
        requires=(Requirement("region", False),),
    )
    setup = setup_values("o", "s", manifest, {"settings": {}}, admin_db=None)
    assert setup is not None
    with pytest.raises(ValueError, match="region"):
        setup()


# -- health polled and kept -------------------------------------------------------------------------


class _Polled:
    def __init__(self, answer):
        self._answer = answer

    def ready(self):
        if isinstance(self._answer, Exception):
            raise self._answer

    def health(self):
        return self._answer


def _prepared(manifest: Manifest, worker) -> SimpleNamespace:
    return SimpleNamespace(manifest=manifest, remote={"host": "h"}, worker=lambda: worker)


_HEALTHY = Manifest(exports=(Export("check", HEALTH, "", (), Scalar("text")),), types={})


def test_a_poll_keeps_the_verdict_for_the_status_to_show():
    from provisa.codesource import lifecycle

    lifecycle.forget("acme", "crm")
    lifecycle.poll_source("acme", "crm", _prepared(_HEALTHY, _Polled({"status": "ok"})))
    kept = lifecycle.health("acme", "crm")
    assert kept is not None and kept["state"] == "healthy" and kept["detail"] == "ok"
    assert isinstance(kept["checked_at"], str)


def test_an_on_start_failure_on_restart_leaves_the_source_unhealthy_by_name():
    from provisa.codesource import lifecycle

    lifecycle.forget("acme", "crm")
    failure = worker_module.OnStartFailed("crm", "its @on-start function connect failed: no db")
    lifecycle.poll_source("acme", "crm", _prepared(_HEALTHY, _Polled(failure)))
    kept = lifecycle.health("acme", "crm")
    assert kept is not None and kept["state"] == "unhealthy"
    assert "no db" in kept["detail"]


def test_a_host_that_cannot_be_reached_is_unhealthy():
    from provisa.codesource import lifecycle

    lifecycle.forget("acme", "crm")

    def worker():
        raise worker_module.WorkerFailed("crm", "the remote host could not be reached")

    prepared = SimpleNamespace(manifest=_HEALTHY, remote={"host": "h"}, worker=worker)
    lifecycle.poll_source("acme", "crm", prepared)
    kept = lifecycle.health("acme", "crm")
    assert kept is not None and kept["state"] == "unhealthy"
    assert "could not be reached" in kept["detail"]


def test_a_code_source_with_no_lifecycle_is_not_polled():
    from provisa.codesource import lifecycle

    bare = Manifest(exports=(), types={})
    assert not lifecycle.is_polled(bare)
    assert lifecycle.is_polled(_HEALTHY)


def test_the_operators_interval_is_a_registered_setting():
    from provisa.core import settings_registry

    assert int(settings_registry.value("codesource.health_poll_interval")) >= 5


# -- a dev-mode source -------------------------------------------------------------------------------


def test_a_dev_mode_source_with_lifecycle_tags_is_refused_by_name():
    from provisa.codesource.lifecycle import refuse_in_dev

    manifest = read_manifest(_raw(_fn("go", ["on-start"], [], NOTHING)))
    with pytest.raises(BundleRefused) as caught:
        refuse_in_dev("crm", manifest)
    assert caught.value.code == "codesource.lifecycle_needs_host"
    assert "go" in str(caught.value)
    refuse_in_dev("crm", Manifest(exports=(), types={}))
