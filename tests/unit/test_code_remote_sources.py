# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4c090a-26f1-476c-a5ad-4406c75ece2b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's dev and built modes (REQ-2098): connecting the remote host running its image --
paired, dialled, its hello checked against the source's code -- switches it to built mode and keeps
the manifest it was read with; a built source is prepared from that, never read here, and its
worker is the remote host, dialled again with backoff when it stops answering."""

# Requirements: REQ-2098

from __future__ import annotations

import os
import socket
import subprocess
import sys
import textwrap
import time
from types import SimpleNamespace

import pytest

from provisa.codesource import remote, remote_sources
from provisa.codesource.bundle import content_hash
from provisa.codesource.manifest import (
    Anonymous,
    EnumOf,
    Export,
    Field,
    ListOf,
    Manifest,
    Named,
    ObjectType,
    Parameter,
    Scalar,
    manifest_from_json,
    manifest_json,
)
from provisa.codesource.worker import WorkerFailed, WorkerPool
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.python.source import HOST

pytestmark = pytest.mark.unit

_CODE = textwrap.dedent(
    '''
    def orders(n: int) -> list[dict]:
        """Orders.

        @table
        """
        return [{"n": i} for i in range(n)]


    def summary() -> dict:
        """@command"""
        return {"ok": True}


    def tags() -> list[str]:
        """@command"""
        return ["a"]
    '''
)

_MANIFEST = Manifest(
    exports=(
        Export(
            "orders",
            "table",
            "Orders.",
            (Parameter("n", Scalar("bigint"), False, True),),
            Named("Order"),
        ),
        Export("summary", "command", "", (), Anonymous((Field("ok", Scalar("boolean"), False),))),
        Export("tags", "command", "", (), ListOf(EnumOf("Tag", ("a", "b"))), many=True),
    ),
    types={"Order": ObjectType("Order", (Field("n", Scalar("bigint"), True),))},
)


def test_a_manifest_is_kept_as_json_and_read_back_the_same():
    assert manifest_from_json(manifest_json(_MANIFEST)) == _MANIFEST


def test_a_mode_is_dev_or_built_and_a_built_source_names_its_host():
    assert remote_sources.mode_of({}) == "dev"
    with pytest.raises(remote_sources.RemoteRefused):
        remote_sources.mode_of({"mode": "cloud"})
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        remote_sources.remote_of({"mode": "built", "remote": {"host": "h", "port": 1}})
    assert "pin" in str(refused.value) and "manifest" in str(refused.value)


# -- the pool's remote workers ------------------------------------------------------------------


class _Flaky:
    def __init__(self, answers: list[bool]) -> None:
        self.answers = answers
        self.dialled = 0

    def make(self):
        outer = self

        class Worker:
            def start(self):
                outer.dialled += 1
                if not outer.answers.pop(0):
                    raise WorkerFailed("shop", "remote host gpu:9443 could not be reached")

            def alive(self):
                return True

            def stop(self):
                pass

        return Worker()


def test_a_host_that_failed_is_dialled_again_only_after_its_wait(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("provisa.codesource.worker.time.monotonic", lambda: now[0])
    pool = WorkerPool()
    flaky = _Flaky([False, True])
    with pytest.raises(WorkerFailed):
        pool.remote("shop", "h1", "acme", flaky.make)
    with pytest.raises(WorkerFailed) as waiting:
        pool.remote("shop", "h1", "acme", flaky.make)
    assert "gpu:9443" in str(waiting.value) and "next try" in str(waiting.value)
    assert flaky.dialled == 1
    assert pool.remote_status("acme", "shop")["connected"] is False
    now[0] += 1.5
    assert pool.remote("shop", "h1", "acme", flaky.make) is not None
    assert flaky.dialled == 2
    assert pool.remote_status("acme", "shop") == {"connected": True}


# -- connect ------------------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def host(tmp_path):
    """A Python remote host serving the shop code; its port, pairing code and code hash."""
    code = tmp_path / "code"
    code.mkdir()
    (code / "shop.py").write_text(_CODE)
    hashed = content_hash(code)
    port = _free_port()
    process = subprocess.Popen(  # noqa: S603 -- the test's own interpreter and Provisa's host
        [
            sys.executable,
            str(HOST.with_name("remote_host.py")),
            str(port),
            str(tmp_path / "state"),
            str(code),
            "shop",
        ],
        env={**os.environ, "PROVISA_CODE_HASH": hashed},
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing = None
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening"):
            break
    assert pairing is not None
    yield port, pairing, hashed
    process.terminate()
    process.wait(timeout=10)


@pytest.fixture
def wired(monkeypatch):
    """A dev-mode source, the org's identity, and the mapping the connect saves."""
    from provisa.codesource import source as source_module

    held: dict = {"identity": remote.new_identity(), "saved": None}

    def give(code_hash: str) -> None:
        held["source"] = SimpleNamespace(
            id="shop",
            type=SimpleNamespace(value="python"),
            mapping={
                "runtime": "python",
                "code": {"kind": "upload", "upload": "u"},
                "content_hash": code_hash,
            },
        )

    async def source(state, source_id):
        return held["source"]

    async def prepare(source_id, source_type, mapping, ctx):
        return SimpleNamespace(content_hash=mapping["content_hash"], manifest=_MANIFEST)

    async def context_for(state):
        return None

    async def identity(org_id):
        return held["identity"]

    async def save(source_id, mapping):
        held["saved"] = mapping

    monkeypatch.setattr(remote_sources, "_source", source)
    monkeypatch.setattr(source_module, "prepare", prepare)
    monkeypatch.setattr(source_module, "context_for", context_for)
    monkeypatch.setattr(remote_sources, "identity", identity)
    monkeypatch.setattr(remote_sources, "_save_mapping", save)
    held["give"] = give
    token = set_current_org("acme")
    yield held
    reset_current_org(token)


async def test_connecting_the_host_switches_the_source_to_built_mode(host, wired):
    port, pairing, hashed = host
    wired["give"](hashed)
    built = await remote_sources.connect(None, "shop", "127.0.0.1", port, pairing)
    assert built["mode"] == "built" and built["content_hash"] == hashed
    remote_host = built["remote"]
    assert (remote_host["host"], remote_host["port"]) == ("127.0.0.1", port)
    assert remote_host["pin"] == pairing.split(".")[0]
    assert remote_host["exports"] == ["orders", "summary", "tags"]
    assert manifest_from_json(remote_host["manifest"]) == _MANIFEST
    assert wired["saved"] == built


async def test_a_host_running_other_code_is_refused_naming_both_hashes(host, wired):
    port, pairing, hashed = host
    wired["give"]("0" * 64)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await remote_sources.connect(None, "shop", "127.0.0.1", port, pairing)
    assert hashed in str(refused.value) and "0" * 64 in str(refused.value)
    assert wired["saved"] is None


async def test_a_pairing_code_that_is_not_the_hosts_is_refused(host, wired):
    port, pairing, hashed = host
    wired["give"](hashed)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await remote_sources.connect(
            None, "shop", "127.0.0.1", port, pairing.split(".")[0] + ".wrong"
        )
    assert refused.value.code == "codesource.pairing"
    assert wired["saved"] is None


async def test_only_a_saved_dev_source_is_connected(wired):
    wired["give"]("")
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await remote_sources.connect(None, "shop", "127.0.0.1", 1, "x.y")
    assert refused.value.code == "codesource.not_dev"


async def test_back_to_dev_forgets_the_host(wired):
    wired["give"]("h1")
    wired["source"].mapping.update({"mode": "built", "remote": {"host": "h"}})
    kept = await remote_sources.to_dev(None, "shop")
    assert "mode" not in kept and "remote" not in kept and kept["content_hash"] == "h1"


async def test_a_built_source_is_prepared_from_its_kept_manifest(monkeypatch):
    from provisa.codesource import source as source_module

    me = remote.new_identity()

    async def identity(org_id):
        return me

    monkeypatch.setattr(remote_sources, "identity", identity)
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": "u"},
        "content_hash": "h1",
        "mode": "built",
        "remote": {
            "host": "gpu",
            "port": 9443,
            "pin": "f" * 64,
            "exports": ["orders", "summary", "tags"],
            "manifest": manifest_json(_MANIFEST),
        },
    }
    ctx = SimpleNamespace(org_id="acme", cache_root=None, registries=[], egress=[], admin_db=None)
    prepared = await source_module.prepare("shop", "python", mapping, ctx)
    assert prepared.manifest == _MANIFEST
    assert prepared.remote["host"] == "gpu" and prepared.identity is me
    source_module.forget("shop")
