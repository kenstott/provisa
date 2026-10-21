# Copyright (c) 2026 Kenneth Stott
# Canary: 2c8838dd-35d2-4d9e-a2f7-d49e9ed30ed2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's image builds (REQ-2098): refused by name before they start when the source is
not a saved dev-mode source or the architecture is not one an image is built for; run in the
background, ending as a pushed image or a kept archive, with the compose service to run it, or as a
failure that says why."""

# Requirements: REQ-2098

from __future__ import annotations

import json
import socket
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.codesource import builds, image_build, oci, remote_sources

pytestmark = pytest.mark.unit


def _source(mapping: dict) -> SimpleNamespace:
    return SimpleNamespace(id="rates", type=SimpleNamespace(value="python"), mapping=mapping)


_DEV = {"runtime": "python", "code": {"kind": "upload", "upload": "u"}, "content_hash": "h" * 64}


@pytest.fixture
def finished(monkeypatch) -> list[dict]:
    seen: list[dict] = []

    async def finish(build_id, **values):
        seen.append({"id": build_id, **values})

    monkeypatch.setattr(builds, "_finish", finish)
    return seen


async def test_a_build_is_refused_before_it_starts(monkeypatch):
    async def source(state, source_id):
        return _source({**_DEV, "content_hash": None})

    monkeypatch.setattr(remote_sources, "_source", source)
    with pytest.raises(builds.BuildRefused) as refused:
        await builds.start(None, "rates", "s390x", None, True, None)
    assert "s390x" in str(refused.value)
    with pytest.raises(builds.BuildRefused) as refused:
        await builds.start(None, "rates", "amd64", None, True, None)
    assert refused.value.code == "codesource.not_dev"


def _built(tmp_path: Path) -> image_build.Built:
    layer = oci.write_layer([oci.Entry("/opt/provisa/code/rates.py", data=b"x")], tmp_path / "l")
    image = oci.Image("amd64", [layer], ["/opt/provisa/venv/bin/python"], exposed=[7443])
    return image_build.Built(image, "provisa-code-rates", "hhhhhhhhhhhh-0.1.0", ["rates"])


@pytest.fixture
def run_parts(tmp_path, monkeypatch):
    """What a build run reads, here without fetching, installing or pulling."""
    from provisa.codesource import source as source_module

    async def prepare(*a):
        return SimpleNamespace(manifest=SimpleNamespace(exports=[SimpleNamespace(name="rates")]))

    async def nothing(*a):
        return None

    monkeypatch.setattr(source_module, "prepare", prepare)
    monkeypatch.setattr(source_module, "_code_reference", nothing)
    monkeypatch.setattr(source_module, "_lockfile", nothing)
    monkeypatch.setattr("provisa.core.demo_bundles.running_provisa", lambda: "0.1.0")
    monkeypatch.setattr(image_build, "assemble", lambda *a, **k: _built(tmp_path))
    config = source_module.config_of("python", _DEV)
    ctx = SimpleNamespace(cache_root=tmp_path / "cache", registries=[])
    return config, ctx


async def test_a_build_kept_for_download_ends_with_its_archive_and_compose(
    run_parts, finished, tmp_path
):
    config, ctx = run_parts
    await builds._run(_source(_DEV), config, ctx, "amd64", None, "b1")
    (done,) = finished
    assert done["status"] == "done" and done["node"] == socket.gethostname()
    assert done["image_ref"] == "provisa-code-rates:hhhhhhhhhhhh-0.1.0"
    with tarfile.open(done["archive_path"]) as tar:
        manifest = json.loads(tar.extractfile("manifest.json").read())  # type: ignore[union-attr]
    assert manifest[0]["RepoTags"] == ["provisa-code-rates:hhhhhhhhhhhh-0.1.0"]


async def test_a_build_pushed_to_a_registry_names_where_it_went(run_parts, finished, monkeypatch):
    config, ctx = run_parts
    pushed: list = []

    def push(self, repository, tag, image):
        pushed.append((self.host, self.username, repository, tag))
        return "sha256:" + "d" * 64

    monkeypatch.setattr(oci.Registry, "push", push)
    registry = SimpleNamespace(
        id="r1",
        host="ghcr.io",
        namespace="acme",
        username="bot",
        credential="made-up-token",
        ca_bundle=None,
    )
    await builds._run(_source(_DEV), config, ctx, "amd64", registry, "b2")
    assert pushed == [("ghcr.io", "bot", "acme/provisa-code-rates", "hhhhhhhhhhhh-0.1.0")]
    (done,) = finished
    assert done["image_ref"] == "ghcr.io/acme/provisa-code-rates:hhhhhhhhhhhh-0.1.0"
    assert done["digest"] == "sha256:" + "d" * 64


async def test_a_build_that_fails_says_why(run_parts, finished, monkeypatch):
    config, ctx = run_parts

    def refuse(*a, **k):
        raise image_build.ImageBuildRefused("deno_platform", "the lockfile holds esbuild for linux")

    monkeypatch.setattr(image_build, "assemble", refuse)
    await builds._run(_source(_DEV), config, ctx, "arm64", None, "b3")
    (failed,) = finished
    assert failed["status"] == "failed" and "esbuild" in failed["error"]


def test_a_finished_build_answers_with_the_compose_to_run_it():
    row = SimpleNamespace(
        _mapping={
            "id": "b1",
            "source_id": "rates",
            "status": "done",
            "image_ref": "ghcr.io/acme/provisa-code-rates:abc",
            "archive_path": "/var/cache/x.tar",
            "started_at": None,
            "finished_at": None,
        }
    )
    answered = builds._row(row)
    assert "archive_path" not in answered
    assert "image: ghcr.io/acme/provisa-code-rates:abc" in answered["compose"]
    assert "PROVISA_HOST_PORT:?" in answered["compose"]
