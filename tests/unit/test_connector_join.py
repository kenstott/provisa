# Copyright (c) 2026 Kenneth Stott
# Canary: 1a9e4f7c-82b6-4d03-a5f2-6c7e0b13d984
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A connector known only by its image becomes a source through a handshake (REQ-2221): the paired
host sends its manifest, and Provisa holds it to the block's digest and code hash. The host here is
a real front end pairing over TLS; only the registry and the model's block are stood in for."""

# Requirements: REQ-2221

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codehost import front
from provisa.codesource import oci, remote, remote_sources
from provisa.codesource.bundle import BundleRefused, content_hash
from provisa.codesource.manifest import (
    Export,
    Field,
    Manifest,
    Named,
    ObjectType,
    Parameter,
    Requirement,
    Scalar,
    manifest_hash,
    manifest_json,
)
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
    '''
)
_MANIFEST = manifest_json(
    Manifest(
        exports=(
            Export(
                "orders",
                "table",
                "Orders.",
                (Parameter("n", Scalar("bigint"), False, True),),
                Named("Order"),
            ),
        ),
        types={"Order": ObjectType("Order", (Field("n", Scalar("bigint"), True, "How many."),))},
        requires=(Requirement("db_uri", True), Requirement("region", False)),
    )
)


def _start_host(tmp_path: Path, manifest: dict | None):
    code = tmp_path / "code"
    code.mkdir()
    (code / "shop.py").write_text(_CODE)
    hashed = content_hash(code)
    names = tmp_path / "names.json"
    names.write_text("{}")
    env = {**os.environ, "PROVISA_CODE_HASH": hashed}
    if manifest is not None:
        baked = tmp_path / "manifest.json"
        baked.write_text(json.dumps(manifest))
        env["PROVISA_MANIFEST_FILE"] = str(baked)
    process = subprocess.Popen(  # noqa: S603 -- the test's own interpreter and Provisa's front end
        [
            sys.executable,
            front.__file__,
            "0",
            str(tmp_path / "state"),
            str(tmp_path / "spill"),
            str(names),
            *[sys.executable, "-I", str(HOST), "{fd}", str(code), "shop"],
        ],
        env=env,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing, port = None, None
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening on "):
            port = int(line.rsplit(" ", 1)[1])
            break
    assert pairing is not None and port is not None, "the host did not start"
    return process, port, pairing, hashed


@pytest.fixture
def host(tmp_path):
    process, port, pairing, hashed = _start_host(tmp_path, _MANIFEST)
    yield port, pairing, hashed
    process.terminate()
    process.wait(timeout=10)


@pytest.fixture
def no_manifest_host(tmp_path):
    process, port, pairing, hashed = _start_host(tmp_path, None)
    yield port, pairing, hashed
    process.terminate()
    process.wait(timeout=10)


def _row(code_hash: str, **overrides) -> dict:
    return {
        "name": "crm",
        "git": "https://git.example/crm.git",
        "ref": "main",
        "commit": "a" * 40,
        "path": "connectors/crm",
        "build_command": "make dist",
        "language": "python",
        "image": "registry.example/data/crm",
        "digest": "sha256:" + "b" * 64,
        "code_hash": code_hash,
        "arch": "amd64",
        "requires": [{"name": "db_uri", "secret": True}, {"name": "region", "secret": False}],
        **overrides,
    }


@pytest.fixture
def wired(monkeypatch):
    """The model's block and the image's labels, stood in for: set ``held["row"]``/``["labels"]``."""
    held: dict = {"identity": remote.new_identity(), "row": None, "labels": None}

    async def connector(name):
        if held["row"] is None:
            raise remote_sources.RemoteRefused("connector_not_found", f"no connector {name}")
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


def _labels(code_hash: str, **overrides) -> dict:
    return {
        "code_hash": code_hash,
        "manifest_hash": manifest_hash(_MANIFEST),
        "runtime": "python",
        **overrides,
    }


def _mapping(port: int, pairing: str, **overrides) -> dict:
    return {
        "connector_join": {
            "connector": "crm",
            "host": "127.0.0.1",
            "port": port,
            "pairing_code": pairing,
        },
        "secrets": {"db_uri": None},
        "settings": {"region": "eu-west-1"},
        **overrides,
    }


async def _join(mapping: dict, source_type: str = "python") -> dict:
    await remote_sources.join_connector(None, "crm_prod", source_type, mapping)
    return mapping


# -- matching ---------------------------------------------------------------------------------------


async def test_a_host_that_matches_its_block_becomes_a_built_source(host, wired):
    port, pairing, hashed = host
    wired["row"] = _row(hashed)
    wired["labels"] = _labels(hashed)
    mapping = await _join(_mapping(port, pairing))
    assert mapping["mode"] == "built"
    assert mapping["content_hash"] == hashed
    assert mapping["connector"] == "crm"
    assert mapping["runtime"] == "python"
    assert mapping["code"] == {"kind": "git", "repository": wired["row"]["git"], "commit": "a" * 40}
    assert mapping["remote"]["manifest"] == _MANIFEST
    assert mapping["remote"]["exports"] == ["orders"]
    assert mapping["remote"]["pin"] == pairing.split(".")[0]


async def test_the_pairing_code_and_the_build_settings_are_not_kept(host, wired):
    port, pairing, hashed = host
    wired["row"] = _row(hashed)
    wired["labels"] = _labels(hashed)
    mapping = await _join(_mapping(port, pairing))
    kept = json.dumps(mapping)
    assert "connector_join" not in mapping
    assert pairing not in kept
    assert "connectors/crm" not in kept and "make dist" not in kept


# -- refused ----------------------------------------------------------------------------------------


async def test_an_image_whose_digest_names_other_code_is_refused_before_pairing(host, wired):
    port, pairing, hashed = host
    wired["row"] = _row(hashed)
    wired["labels"] = _labels("c" * 64)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(port, pairing))
    assert refused.value.code == "codesource.connector_digest_code"
    assert hashed in str(refused.value) and "c" * 64 in str(refused.value)
    # The one-time code was not spent: the host pairs still.
    me = wired["identity"]
    paired = remote.pair(
        "127.0.0.1", port, pairing, me, {"memory_mb": 64, "threads": 1, "spill_cap_mb": 64}
    )
    assert paired.manifest == _MANIFEST


async def test_a_manifest_this_provisa_cannot_read_is_refused(tmp_path, wired):
    broken = {"exports": [{"name": "orders"}], "types": {}, "requires": []}
    process, port, pairing, hashed = _start_host(tmp_path, broken)
    try:
        wired["row"] = _row(hashed, requires=[])
        wired["labels"] = _labels(hashed, manifest_hash=manifest_hash(broken))
        with pytest.raises(remote_sources.RemoteRefused) as refused:
            await _join(_mapping(port, pairing, secrets={}, settings={}))
        assert refused.value.code == "codesource.connector_manifest"
    finally:
        process.terminate()
        process.wait(timeout=10)


async def test_a_manifest_that_is_not_the_images_is_refused_naming_both(host, wired):
    port, pairing, hashed = host
    wired["row"] = _row(hashed)
    wired["labels"] = _labels(hashed, manifest_hash="d" * 64)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(port, pairing))
    assert refused.value.code == "codesource.connector_manifest"
    assert "d" * 64 in str(refused.value) and manifest_hash(_MANIFEST) in str(refused.value)


async def test_a_host_of_other_code_is_refused_by_its_hello(host, wired):
    port, pairing, hashed = host
    other = "9" * 64
    wired["row"] = _row(other)
    wired["labels"] = _labels(other)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(port, pairing))
    assert refused.value.code == "codesource.remote_refused"
    assert hashed in str(refused.value) and other in str(refused.value)


async def test_an_image_built_before_the_manifest_was_baked_is_refused(no_manifest_host, wired):
    port, pairing, hashed = no_manifest_host
    wired["row"] = _row(hashed)
    wired["labels"] = _labels(hashed)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(port, pairing))
    assert refused.value.code == "codesource.connector_no_manifest"


async def test_requires_that_differ_from_the_block_are_refused(host, wired):
    port, pairing, hashed = host
    wired["row"] = _row(hashed, requires=[{"name": "db_uri", "secret": False}])
    wired["labels"] = _labels(hashed)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(port, pairing, settings={"db_uri": "x"}, secrets={}))
    assert refused.value.code == "codesource.connector_requires"


async def test_a_secret_cannot_be_given_as_a_plain_setting(host, wired):
    port, pairing, hashed = host
    wired["row"] = _row(hashed)
    wired["labels"] = _labels(hashed)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(
            _mapping(port, pairing, secrets={}, settings={"db_uri": "postgres://x", "region": "r"})
        )
    assert refused.value.code == "codesource.connector_requires_missing"
    assert "db_uri" in str(refused.value)
    assert "postgres://x" not in str(refused.value)


async def test_a_missing_required_value_is_refused_before_anything_is_dialled(wired):
    wired["row"] = _row("e" * 64)
    wired["labels"] = _labels("e" * 64)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(1, "x.y", settings={}))
    assert refused.value.code == "codesource.connector_requires_missing"
    assert "region" in str(refused.value)


async def test_the_wrong_source_type_is_refused(wired):
    wired["row"] = _row("e" * 64, language="go")
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(1, "x.y"), source_type="python")
    assert refused.value.code == "codesource.connector_type"


async def test_an_unknown_connector_is_refused(wired):
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(_mapping(1, "x.y"))
    assert refused.value.code == "codesource.connector_not_found"


@pytest.mark.parametrize(
    "broken",
    [
        {"host": ""},
        {"port": "7443"},
        {"port": 70000},
        {"pairing_code": ""},
    ],
)
async def test_an_incomplete_join_is_refused(wired, broken):
    wired["row"] = _row("e" * 64)
    mapping = _mapping(1, "x.y")
    mapping["connector_join"].update(broken)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await _join(mapping)
    assert refused.value.code == "codesource.address"


# -- the image at its digest ------------------------------------------------------------------------


class _Registry:
    host = "registry.example"

    def __init__(self, config: dict, config_digest: str | None = None) -> None:
        self.config_bytes = json.dumps(config).encode()
        digest = "sha256:" + hashlib.sha256(self.config_bytes).hexdigest()
        self.manifest_bytes = json.dumps({"config": {"digest": config_digest or digest}}).encode()
        self.digest = "sha256:" + hashlib.sha256(self.manifest_bytes).hexdigest()

    def manifest(self, repository, reference):
        return json.loads(self.manifest_bytes), self.manifest_bytes

    def blob(self, repository, digest, out):
        out.write_bytes(self.config_bytes)


def test_the_labels_are_read_from_the_image_the_digest_names(tmp_path):
    registry = _Registry({"config": {"Labels": {"dev.provisa.code_hash": "abc"}}})
    found = remote_sources._labels_at_digest(registry, "data/crm", registry.digest, tmp_path)
    assert found == {"dev.provisa.code_hash": "abc"}


def test_a_manifest_that_is_not_its_digest_is_refused(tmp_path):
    registry = _Registry({"config": {"Labels": {}}})
    with pytest.raises(oci.ImageRefused):
        remote_sources._labels_at_digest(registry, "data/crm", "sha256:" + "0" * 64, tmp_path)


async def test_an_unreadable_image_is_refused_naming_the_connector(monkeypatch):
    def unreachable(*_args, **_kwargs):
        raise oci.ImageRefused("registry.example/data/crm: HTTP 503")

    async def registry_for(*_args, **_kwargs):
        return SimpleNamespace(), "data/crm"

    monkeypatch.setattr(remote_sources, "_labels_at_digest", unreachable)
    monkeypatch.setattr(remote_sources, "_registry_for", registry_for)
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await remote_sources._image_labels(
            SimpleNamespace(admin_db=None), "acme", "crm", _row("e" * 64)
        )
    assert refused.value.code == "codesource.connector_image"
    assert "crm" in str(refused.value)


# -- once a source ----------------------------------------------------------------------------------


@pytest.fixture
def built_source(monkeypatch):
    held: dict = {"row": _row("e" * 64)}
    source = SimpleNamespace(
        id="crm_prod",
        type=SimpleNamespace(value="python"),
        mapping={
            "mode": "built",
            "connector": "crm",
            "runtime": "python",
            "code": {"kind": "git", "repository": "r", "commit": "a" * 40},
            "content_hash": "e" * 64,
            "remote": {
                "host": "h",
                "port": 1,
                "pin": "p",
                "exports": ["orders"],
                "manifest": _MANIFEST,
            },
        },
    )

    async def found(state, source_id):
        return source

    async def connector(name):
        return held["row"]

    monkeypatch.setattr(remote_sources, "_source", found)
    monkeypatch.setattr(remote_sources, "_connector", connector)
    return held


async def test_a_connector_source_has_no_dev_mode(built_source):
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await remote_sources.to_dev(None, "crm_prod")
    assert refused.value.code == "codesource.connector_built_only"


async def test_a_connector_source_repins_only_to_the_code_its_block_names(built_source):
    with pytest.raises(remote_sources.RemoteRefused) as refused:
        await remote_sources.repin(None, "crm_prod", "f" * 64)
    assert refused.value.code == "codesource.connector_other_code"


async def test_a_join_outside_create_is_refused():
    from provisa.api.admin._code_source_registration import record_code

    refused = await record_code("crm_prod", "python", {"connector_join": {}})
    assert refused is not None and refused.code == "codesource.connector_join_on_create"


def test_refusals_are_bundle_refusals_the_mutation_reports():
    assert issubclass(remote_sources.RemoteRefused, BundleRefused)
