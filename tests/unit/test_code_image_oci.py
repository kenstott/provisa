# Copyright (c) 2026 Kenneth Stott
# Canary: f1f6f47a-124f-4b3e-bef4-e2e3fe07f892
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OCI image Provisa assembles itself (REQ-2098): reproducible layers, the configuration and
manifest that name them, a ``docker load`` archive, layers read straight from another platform's
archives, and a push to a registry signing in through its token challenge -- answered here by an
in-process registry."""

# Requirements: REQ-2098

from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
import zipfile
from pathlib import Path

import httpx
import pytest

from provisa.codesource import oci

pytestmark = pytest.mark.unit


def _members(layer: oci.Layer) -> dict[str, tarfile.TarInfo]:
    with gzip.open(layer.blob) as plain, tarfile.open(fileobj=io.BytesIO(plain.read())) as tar:
        return {m.name: m for m in tar.getmembers()}


# -- layers -------------------------------------------------------------------------------------


def test_the_same_files_make_the_same_layer(tmp_path: Path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text("print(1)\n")
    first = oci.write_layer(oci.tree(tmp_path / "src", "/opt/x"), tmp_path / "one")
    second = oci.write_layer(oci.tree(tmp_path / "src", "/opt/x"), tmp_path / "two")
    assert (first.digest, first.diff_id) == (second.digest, second.diff_id)


def test_a_layer_holds_its_parents_with_time_zero_and_the_owner_named(tmp_path: Path):
    layer = oci.write_layer(
        [
            oci.Entry("/var/lib/provisa", directory=True, mode=0o700, owner=65532),
            oci.Entry("/opt/p/bin/python", link="/opt/p/runtime/python3.12"),
            oci.Entry("/opt/p/run.sh", data=b"#!/bin/sh\n", mode=0o755),
        ],
        tmp_path,
    )
    members = _members(layer)
    assert {"opt", "opt/p", "opt/p/bin", "var", "var/lib"} <= set(members)
    assert members["var/lib/provisa"].uid == 65532 and members["opt/p/run.sh"].uid == 0
    assert members["opt/p/bin/python"].issym()
    assert members["opt/p/run.sh"].mode == 0o755
    assert all(m.mtime == 0 for m in members.values())
    with gzip.open(layer.blob) as plain:
        assert layer.diff_id == "sha256:" + hashlib.sha256(plain.read()).hexdigest()


def test_another_platforms_archive_is_read_without_extracting_it(tmp_path: Path):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:gz") as tar:

        def add(name: str, data: bytes, **kw) -> tarfile.TarInfo:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            for k, v in kw.items():
                setattr(info, k, v)
            tar.addfile(info, io.BytesIO(data))
            return info

        add("python/share/terminfo/n/ncr260vt300wpp", b"one")
        add("python/share/terminfo/N/NCR260VT300WPP", b"two")  # differs only in case
        add("python/bin/python3.12", b"elf", mode=0o755)
        link = tarfile.TarInfo("python/bin/python3")
        link.type, link.linkname = tarfile.SYMTYPE, "python3.12"
        tar.addfile(link)
        hard = tarfile.TarInfo("python/bin/python3-hard")
        hard.type, hard.linkname = tarfile.LNKTYPE, "python/bin/python3.12"
        tar.addfile(hard)
    entries = {e.path: e for e in oci.archive_entries("x.tar.gz", raw.getvalue(), "/opt/p/runtime")}
    assert entries["/opt/p/runtime/python/share/terminfo/n/ncr260vt300wpp"].data == b"one"
    assert entries["/opt/p/runtime/python/share/terminfo/N/NCR260VT300WPP"].data == b"two"
    assert entries["/opt/p/runtime/python/bin/python3.12"].mode == 0o755
    assert entries["/opt/p/runtime/python/bin/python3"].link == "python3.12"
    assert entries["/opt/p/runtime/python/bin/python3-hard"].data == b"elf"


def test_a_wheel_is_read_as_the_zip_it_is_and_nothing_escapes(tmp_path: Path):
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as z:
        z.writestr("pyarrow/__init__.py", "x = 1\n")
    entries = {e.path for e in oci.archive_entries("p.whl", raw.getvalue(), "/opt/p/hostlib")}
    assert "/opt/p/hostlib/pyarrow/__init__.py" in entries
    bad = io.BytesIO()
    with zipfile.ZipFile(bad, "w") as z:
        z.writestr("../../etc/passwd", "root\n")
    with pytest.raises(oci.ImageRefused):
        oci.archive_entries("bad.zip", bad.getvalue(), "/opt/p")


# -- the image ----------------------------------------------------------------------------------


def _image(tmp_path: Path) -> oci.Image:
    layer = oci.write_layer([oci.Entry("/opt/p/a", data=b"a")], tmp_path)
    return oci.Image(
        architecture="arm64",
        layers=[layer],
        entrypoint=["/opt/p/node", "/opt/p/host.mjs"],
        env={"PROVISA_CODE_HASH": "h1"},
        labels={"dev.provisa.codehost": "1"},
        exposed=[7443],
        user="65532:65532",
        base_config={"Env": ["PATH=/usr/bin", "PROVISA_CODE_HASH=old"]},
    )


def test_the_configuration_and_manifest_name_what_the_image_is(tmp_path: Path):
    image = _image(tmp_path)
    config = json.loads(image.config())
    assert config["architecture"] == "arm64" and config["os"] == "linux"
    assert config["config"]["Entrypoint"] == ["/opt/p/node", "/opt/p/host.mjs"]
    assert config["config"]["Env"] == ["PATH=/usr/bin", "PROVISA_CODE_HASH=h1"]
    assert config["config"]["ExposedPorts"] == {"7443/tcp": {}}
    assert config["config"]["User"] == "65532:65532"
    assert config["rootfs"]["diff_ids"] == [image.layers[0].diff_id]
    manifest = json.loads(image.manifest())
    assert manifest["config"]["digest"] == "sha256:" + hashlib.sha256(image.config()).hexdigest()
    assert [layer["digest"] for layer in manifest["layers"]] == [image.layers[0].digest]
    assert manifest["annotations"] == {"dev.provisa.codehost": "1"}


def test_a_docker_load_archive_holds_its_layers_configuration_and_tag(tmp_path: Path):
    image = _image(tmp_path)
    out = tmp_path / "image.tar"
    oci.docker_archive(image, "provisa-code-rates:abc", out)
    with tarfile.open(out) as tar:
        manifest = json.loads(tar.extractfile("manifest.json").read())  # type: ignore[union-attr]
        (entry,) = manifest
        assert entry["RepoTags"] == ["provisa-code-rates:abc"]
        layer = tar.extractfile(entry["Layers"][0]).read()  # type: ignore[union-attr]
        assert "sha256:" + hashlib.sha256(layer).hexdigest() == image.layers[0].diff_id
        assert json.loads(tar.extractfile(entry["Config"]).read())["architecture"] == "arm64"  # type: ignore[union-attr]


# -- a registry ---------------------------------------------------------------------------------


class _Registry:
    """A registry answering the distribution API in-process, behind a bearer-token challenge for
    the user ``bot``."""

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self.manifests: dict[str, bytes] = {}
        self.tokens_for: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/token":
            assert request.headers["authorization"].startswith("Basic ")
            self.tokens_for.append(request.url.params["scope"])
            return httpx.Response(200, json={"token": "t"})
        if request.headers.get("authorization") != "Bearer t":
            return httpx.Response(
                401,
                headers={
                    "www-authenticate": 'Bearer realm="https://reg.test/token",service="reg.test"'
                },
            )
        if request.method == "HEAD" and "/blobs/" in path:
            return httpx.Response(200 if path.rsplit("/", 1)[1] in self.blobs else 404)
        if request.method == "POST" and path.endswith("/blobs/uploads/"):
            return httpx.Response(202, headers={"location": "/v2/upload/1"})
        if request.method == "PUT" and path == "/v2/upload/1":
            digest = request.url.params["digest"]
            assert digest == "sha256:" + hashlib.sha256(request.content).hexdigest()
            self.blobs[digest] = request.content
            return httpx.Response(201)
        if request.method == "PUT" and "/manifests/" in path:
            self.manifests[path.rsplit("/", 1)[1]] = request.content
            return httpx.Response(201)
        if request.method == "GET" and "/manifests/" in path:
            ref = path.rsplit("/", 1)[1]
            return httpx.Response(200, content=self.manifests[ref])
        if request.method == "GET" and "/blobs/" in path:
            return httpx.Response(200, content=self.blobs[path.rsplit("/", 1)[1]])
        return httpx.Response(404)


def test_an_image_is_pushed_signing_in_on_the_registrys_challenge(tmp_path: Path):
    served = _Registry()
    registry = oci.Registry(
        "reg.test", "bot", "made-up-token", transport=httpx.MockTransport(served)
    )
    image = _image(tmp_path)
    digest = registry.push("acme/provisa-code-rates", "abc", image)
    assert served.tokens_for == ["repository:acme/provisa-code-rates:pull,push"]
    assert image.layers[0].digest in served.blobs
    assert served.manifests["abc"] == image.manifest()
    assert digest == "sha256:" + hashlib.sha256(image.manifest()).hexdigest()
    # Pushed again, a blob the registry holds is not sent again.
    served.blobs = {k: v for k, v in served.blobs.items()}
    held = dict(served.blobs)
    registry.push("acme/provisa-code-rates", "abc", image)
    assert served.blobs == held


def test_a_base_image_is_pulled_by_its_digest_and_checked(tmp_path: Path):
    served = _Registry()
    pusher = oci.Registry("reg.test", "bot", "made-up-token", transport=httpx.MockTransport(served))
    image = _image(tmp_path / "made")
    digest = pusher.push("distroless/cc", "latest", image)
    served.manifests[digest] = served.manifests["latest"]
    puller = oci.Registry("reg.test", "bot", "made-up-token", transport=httpx.MockTransport(served))
    layers, config = oci.pull_base(puller, "distroless/cc", digest, tmp_path / "cache")
    assert [(layer.digest, layer.diff_id) for layer in layers] == [
        (image.layers[0].digest, image.layers[0].diff_id)
    ]
    assert config["User"] == "65532:65532"
    served.manifests["sha256:" + "0" * 64] = served.manifests["latest"]
    with pytest.raises(oci.ImageRefused):
        oci.pull_base(puller, "distroless/cc", "sha256:" + "0" * 64, tmp_path / "cache")
